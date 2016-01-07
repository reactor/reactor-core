/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.support;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import reactor.core.error.AlertException;
import reactor.core.support.rb.disruptor.Sequence;
import reactor.fn.LongSupplier;

/**
 * Strategy employed for making ringbuffer consumers wait on a cursor {@link Sequence}.
 */
public interface WaitStrategy
{
    /**
     * Wait for the given sequence to be available.  It is possible for this method to return a value
     * less than the sequence number supplied depending on the implementation of the WaitStrategy.  A common
     * use for this is to signal a timeout.  Any EventProcessor that is using a WaitStragegy to get notifications
     * about message becoming available should remember to handle this case.
     *
     * @param sequence to be waited on.
     * @param cursor the main sequence from ringbuffer. Wait/notify strategies will
     *    need this as is notified upon update.
     * @param spinObserver Spin observer
     * @return the sequence that is available which may be greater than the requested sequence.
     * @throws AlertException if the status of the Disruptor has changed.
     * @throws InterruptedException if the thread is interrupted.
     */
    long waitFor(long sequence, LongSupplier cursor, Runnable spinObserver)
        throws AlertException, InterruptedException;

    /**
     * Implementations should signal the waiting ringbuffer consumers that the cursor has advanced.
     */
    void signalAllWhenBlocking();

    /**
     * Blocking strategy that uses a lock and condition variable for ringbuffer consumer waiting on a barrier.
     *
     * This strategy can be used when throughput and low-latency are not as important as CPU resource.
     */
    final class Blocking implements WaitStrategy
    {
        private final Lock      lock                     = new ReentrantLock();
        private final Condition processorNotifyCondition = lock.newCondition();

        @Override
        public long waitFor(long sequence, LongSupplier cursorSequence, Runnable barrier)
            throws AlertException, InterruptedException
        {
            long availableSequence;
            if ((availableSequence = cursorSequence.get()) < sequence)
            {
                lock.lock();
                try
                {
                    while ((availableSequence = cursorSequence.get()) < sequence)
                    {
                        barrier.run();
                        processorNotifyCondition.await();
                    }
                }
                finally
                {
                    lock.unlock();
                }
            }

            while ((availableSequence = cursorSequence.get()) < sequence)
            {
                barrier.run();
            }

            return availableSequence;
        }

        @Override
        public void signalAllWhenBlocking()
        {
            lock.lock();
            try
            {
                processorNotifyCondition.signalAll();
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    /**
     * Busy Spin strategy that uses a busy spin loop for ringbuffer consumers waiting on a barrier.
     *
     * This strategy will use CPU resource to avoid syscalls which can introduce latency jitter.  It is best
     * used when threads can be bound to specific CPU cores.
     */
    final class BusySpin implements WaitStrategy
    {
        @Override
        public long waitFor(final long sequence, LongSupplier cursor, final Runnable barrier)
            throws AlertException, InterruptedException
        {
            long availableSequence;

            while ((availableSequence = cursor.get()) < sequence)
            {
                barrier.run();
            }

            return availableSequence;
        }

        @Override
        public void signalAllWhenBlocking()
        {
        }
    }

    /**
     * Variation of the {@link Blocking} that attempts to elide conditional wake-ups when
     * the lock is uncontended.  Shows performance improvements on microbenchmarks.  However this
     * wait strategy should be considered experimental as I have not full proved the correctness of
     * the lock elision code.
     */
    final class LiteBlocking implements WaitStrategy
    {
        private final Lock          lock                     = new ReentrantLock();
        private final Condition     processorNotifyCondition = lock.newCondition();
        private final AtomicBoolean signalNeeded             = new AtomicBoolean(false);

        @Override
        public long waitFor(long sequence, LongSupplier cursorSequence, Runnable barrier)
            throws AlertException, InterruptedException
        {
            long availableSequence;
            if ((availableSequence = cursorSequence.get()) < sequence)
            {
                lock.lock();

                try
                {
                    do
                    {
                        signalNeeded.getAndSet(true);

                        if ((availableSequence = cursorSequence.get()) >= sequence)
                        {
                            break;
                        }

                        barrier.run();
                        processorNotifyCondition.await();
                    }
                    while ((availableSequence = cursorSequence.get()) < sequence);
                }
                finally
                {
                    lock.unlock();
                }
            }

            while ((availableSequence = cursorSequence.get()) < sequence)
            {
                barrier.run();
            }

            return availableSequence;
        }

        @Override
        public void signalAllWhenBlocking()
        {
            if (signalNeeded.getAndSet(false))
            {
                lock.lock();
                try
                {
                    processorNotifyCondition.signalAll();
                }
                finally
                {
                    lock.unlock();
                }
            }
        }
    }

    /**
     * <p>Phased wait strategy for waiting ringbuffer consumers on a barrier.</p>
     *
     * <p>This strategy can be used when throughput and low-latency are not as important as CPU resource.
     * Spins, then yields, then waits using the configured fallback WaitStrategy.</p>
     */
    final class PhasedOff implements WaitStrategy
    {
        private static final int SPIN_TRIES = 10000;
        private final long spinTimeoutNanos;
        private final long yieldTimeoutNanos;
        private final WaitStrategy fallbackStrategy;

        public PhasedOff(long spinTimeout,
                                         long yieldTimeout,
                                         TimeUnit units,
                                         WaitStrategy fallbackStrategy)
        {
            this.spinTimeoutNanos = units.toNanos(spinTimeout);
            this.yieldTimeoutNanos = spinTimeoutNanos + units.toNanos(yieldTimeout);
            this.fallbackStrategy = fallbackStrategy;
        }

        /**
         * Block with wait/notifyAll semantics
         */
        public static PhasedOff withLock(long spinTimeout,
                                                         long yieldTimeout,
                                                         TimeUnit units)
        {
            return new PhasedOff(spinTimeout, yieldTimeout,
                                                 units, new Blocking());
        }

        /**
         * Block with wait/notifyAll semantics
         */
        public static PhasedOff withLiteLock(long spinTimeout,
                                                             long yieldTimeout,
                                                             TimeUnit units)
        {
            return new PhasedOff(spinTimeout, yieldTimeout,
                                                 units, new LiteBlocking());
        }

        /**
         * Block by sleeping in a loop
         */
        public static PhasedOff withSleep(long spinTimeout,
                                                          long yieldTimeout,
                                                          TimeUnit units)
        {
            return new PhasedOff(spinTimeout, yieldTimeout,
                                                 units, new Sleeping(0));
        }

        @Override
        public long waitFor(long sequence, LongSupplier cursor, Runnable barrier)
            throws AlertException, InterruptedException
        {
            long availableSequence;
            long startTime = 0;
            int counter = SPIN_TRIES;

            do
            {
                if ((availableSequence = cursor.get()) >= sequence)
                {
                    return availableSequence;
                }

                if (0 == --counter)
                {
                    if (0 == startTime)
                    {
                        startTime = System.nanoTime();
                    }
                    else
                    {
                        long timeDelta = System.nanoTime() - startTime;
                        if (timeDelta > yieldTimeoutNanos)
                        {
                            return fallbackStrategy.waitFor(sequence, cursor, barrier);
                        }
                        else if (timeDelta > spinTimeoutNanos)
                        {
                            Thread.yield();
                        }
                    }
                    counter = SPIN_TRIES;
                }
                barrier.run();
            }
            while (true);
        }

        @Override
        public void signalAllWhenBlocking()
        {
            fallbackStrategy.signalAllWhenBlocking();
        }
    }

    /**
     * Sleeping strategy that initially spins, then uses a Thread.yield(), and
     * eventually sleep (<code>LockSupport.parkNanos(1)</code>) for the minimum
     * number of nanos the OS and JVM will allow while the
     * ringbuffer consumers are waiting on a barrier.
     *
     * This strategy is a good compromise between performance and CPU resource.
     * Latency spikes can occur after quiet periods.
     */
    final class Sleeping implements WaitStrategy
    {
        private static final int DEFAULT_RETRIES = 200;

        private final int retries;

        public Sleeping()
        {
            this(DEFAULT_RETRIES);
        }

        public Sleeping(int retries)
        {
            this.retries = retries;
        }

        @Override
        public long waitFor(final long sequence, LongSupplier cursor, final Runnable barrier)
            throws AlertException, InterruptedException
        {
            long availableSequence;
            int counter = retries;

            while ((availableSequence = cursor.get()) < sequence)
            {
                counter = applyWaitMethod(barrier, counter);
            }

            return availableSequence;
        }

        @Override
        public void signalAllWhenBlocking()
        {
        }

        private int applyWaitMethod(final Runnable barrier, int counter)
            throws AlertException
        {
            barrier.run();

            if (counter > 100)
            {
                --counter;
            }
            else if (counter > 0)
            {
                --counter;
                Thread.yield();
            }
            else
            {
                LockSupport.parkNanos(1L);
            }

            return counter;
        }
    }

    /**
     * Yielding strategy that uses a Thread.yield() for ringbuffer consumers waiting on a barrier
     * after an initially spinning.
     *
     * This strategy is a good compromise between performance and CPU resource without incurring significant latency spikes.
     */
    final class Yielding implements WaitStrategy
    {
        private static final int SPIN_TRIES = 100;

        @Override
        public long waitFor(final long sequence, LongSupplier cursor, final Runnable barrier)
            throws AlertException, InterruptedException
        {
            long availableSequence;
            int counter = SPIN_TRIES;

            while ((availableSequence = cursor.get()) < sequence)
            {
                counter = applyWaitMethod(barrier, counter);
            }

            return availableSequence;
        }

        @Override
        public void signalAllWhenBlocking()
        {
        }

        private int applyWaitMethod(final Runnable barrier, int counter)
            throws AlertException
        {
            barrier.run();

            if (0 == counter)
            {
                Thread.yield();
            }
            else
            {
                --counter;
            }

            return counter;
        }
    }
}
