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
package reactor.core.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Strategy employed to wait for specific {@link LongSupplier} values with various spinning strategies.
 */
public abstract class WaitStrategy
{

    final static WaitStrategy YIELDING  = new Yielding();
    final static WaitStrategy PARKING   = new Parking();
    final static WaitStrategy SLEEPING  = new Sleeping();
    final static WaitStrategy BUSY_SPIN = new BusySpin();

    /**
     * Blocking strategy that uses a lock and condition variable for consumer waiting on a barrier.
     *
     * This strategy can be used when throughput and low-latency are not as important as CPU resource.
     */
    public static WaitStrategy blocking() {
        return new Blocking();
    }

    /**
     * Busy Spin strategy that uses a busy spin loop for consumers waiting on a barrier.
     *
     * This strategy will use CPU resource to avoid syscalls which can introduce latency jitter.  It is best
     * used when threads can be bound to specific CPU cores.
     */
    public static WaitStrategy busySpin() {
        return BUSY_SPIN;
    }

    /**
     * Variation of the {@link #blocking()} that attempts to elide conditional wake-ups when the lock is uncontended.
     * Shows performance improvements on microbenchmarks.  However this wait strategy should be considered experimental
     * as I have not full proved the correctness of the lock elision code.
     */
    public static WaitStrategy liteBlocking() {
        return new LiteBlocking();
    }

    /**
     * Parking strategy that initially spins, then uses a Thread.yield(), and eventually sleep
     * (<code>LockSupport.parkNanos(1)</code>) for the minimum number of nanos the OS and JVM will allow while the
     * consumers are waiting on a barrier.
     * <p>
     * This strategy is a good compromise between performance and CPU resource. Latency spikes can occur after quiet
     * periods.
     */
    public static WaitStrategy parking() {
        return PARKING;
    }

    /**
     * Parking strategy that initially spins, then uses a Thread.yield(), and eventually
     * sleep (<code>LockSupport.parkNanos(1)</code>) for the minimum number of nanos the
     * OS and JVM will allow while the consumers are waiting on a barrier.
     * <p>
     * This strategy is a good compromise between performance and CPU resource. Latency
     * spikes can occur after quiet periods.
     *
     * @param retries the spin cycle count before parking
     */
    public static WaitStrategy parking(int retries) {
        return new Parking(retries);
    }

    /**
     * <p>Phased wait strategy for waiting consumers on a barrier.</p>
     * <p>
     * <p>This strategy can be used when throughput and low-latency are not as important as CPU resource. Spins, then
     * yields, then waits using the configured fallback WaitStrategy.</p>
     *
     * @param delegate the target wait strategy to fallback on
     */
    public static WaitStrategy phasedOff(long spinTimeout, long yieldTimeout, TimeUnit units, WaitStrategy delegate) {
        return new PhasedOff(spinTimeout, yieldTimeout, units, delegate);
    }

    /**
     * Block with wait/notifyAll semantics
     */
    public static WaitStrategy phasedOffLiteLock(long spinTimeout, long yieldTimeout, TimeUnit units) {
        return phasedOff(spinTimeout, yieldTimeout, units, liteBlocking());
    }

    /**
     * Block with wait/notifyAll semantics
     */
    public static WaitStrategy phasedOffLock(long spinTimeout, long yieldTimeout, TimeUnit units) {
        return phasedOff(spinTimeout, yieldTimeout, units, blocking());
    }

    /**
     * Block by parking in a loop
     */
    public static WaitStrategy phasedOffSleep(long spinTimeout, long yieldTimeout, TimeUnit units) {
        return phasedOff(spinTimeout, yieldTimeout, units, parking(0));
    }

    /**
     * Yielding strategy that uses a Thread.yield() for consumers waiting on a 
     * barrier
     * after an initially spinning.
     *
     * This strategy is a good compromise between performance and CPU resource without incurring significant latency spikes.
     */
    public static WaitStrategy yielding() {
        return YIELDING;
    }

    /**
     * Yielding strategy that uses a Thread.sleep(1) for consumers waiting on a
     * barrier
     * after an initially spinning.
     *
     * This strategy will incur up a latency of 1ms and save a maximum CPU resources.
     */
    public static WaitStrategy sleeping() {
        return SLEEPING;
    }

    /**
     * Implementations should signal the waiting consumers that the cursor has advanced.
     */
    public void signalAllWhenBlocking() {
    }

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
     * @throws Exceptions.AlertException if the status of the Disruptor has changed.
     * @throws InterruptedException if the thread is interrupted.
     */
    public abstract long waitFor(long sequence, LongSupplier cursor, Runnable spinObserver)
            throws Exceptions.AlertException, InterruptedException;



    final static class Blocking extends WaitStrategy {

        private final Lock      lock                     = new ReentrantLock();
        private final Condition processorNotifyCondition = lock.newCondition();

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

        @Override
        public long waitFor(long sequence, LongSupplier cursorSequence, Runnable barrier)
                throws Exceptions.AlertException, InterruptedException
        {
            long availableSequence;
            if ((availableSequence = cursorSequence.getAsLong()) < sequence)
            {
                lock.lock();
                try
                {
                    while ((availableSequence = cursorSequence.getAsLong()) < sequence)
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

            while ((availableSequence = cursorSequence.getAsLong()) < sequence)
            {
                barrier.run();
            }

            return availableSequence;
        }
    }


    final static class BusySpin extends WaitStrategy {

        @Override
        public long waitFor(final long sequence, LongSupplier cursor, final Runnable barrier)
                throws Exceptions.AlertException, InterruptedException
        {
            long availableSequence;

            while ((availableSequence = cursor.getAsLong()) < sequence)
            {
                barrier.run();
            }

            return availableSequence;
        }
    }

    final static class Sleeping extends WaitStrategy {

        @Override
        public long waitFor(final long sequence,
                LongSupplier cursor,
                final Runnable barrier)
                throws Exceptions.AlertException, InterruptedException {
            long availableSequence;

            while ((availableSequence = cursor.getAsLong()) < sequence) {
                barrier.run();
                Thread.sleep(1);
            }

            return availableSequence;
        }
    }

    final static class LiteBlocking extends WaitStrategy {

        private final Lock          lock                     = new ReentrantLock();
        private final Condition     processorNotifyCondition = lock.newCondition();
        private final AtomicBoolean signalNeeded             = new AtomicBoolean(false);

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

        @Override
        public long waitFor(long sequence, LongSupplier cursorSequence, Runnable barrier)
                throws Exceptions.AlertException, InterruptedException
        {
            long availableSequence;
            if ((availableSequence = cursorSequence.getAsLong()) < sequence)
            {
                lock.lock();

                try
                {
                    do
                    {
                        signalNeeded.getAndSet(true);

                        if ((availableSequence = cursorSequence.getAsLong()) >= sequence)
                        {
                            break;
                        }

                        barrier.run();
                        processorNotifyCondition.await();
                    }
                    while ((availableSequence = cursorSequence.getAsLong()) < sequence);
                }
                finally
                {
                    lock.unlock();
                }
            }

            while ((availableSequence = cursorSequence.getAsLong()) < sequence)
            {
                barrier.run();
            }

            return availableSequence;
        }
    }


    final static class PhasedOff extends WaitStrategy {

        private static final int SPIN_TRIES = 10000;
        private final long spinTimeoutNanos;
        private final long yieldTimeoutNanos;
        private final WaitStrategy fallbackStrategy;

        PhasedOff(long spinTimeout, long yieldTimeout,
                                         TimeUnit units,
                                         WaitStrategy fallbackStrategy)
        {
            this.spinTimeoutNanos = units.toNanos(spinTimeout);
            this.yieldTimeoutNanos = spinTimeoutNanos + units.toNanos(yieldTimeout);
            this.fallbackStrategy = fallbackStrategy;
        }

        @Override
        public void signalAllWhenBlocking()
        {
            fallbackStrategy.signalAllWhenBlocking();
        }

        @Override
        public long waitFor(long sequence, LongSupplier cursor, Runnable barrier)
                throws Exceptions.AlertException, InterruptedException
        {
            long availableSequence;
            long startTime = 0;
            int counter = SPIN_TRIES;

            do
            {
                if ((availableSequence = cursor.getAsLong()) >= sequence)
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
    }

    final static class Parking extends WaitStrategy {

        private static final int DEFAULT_RETRIES = 200;

        private final int retries;

        Parking() {
            this(DEFAULT_RETRIES);
        }

        Parking(int retries) {
            this.retries = retries;
        }

        private int applyWaitMethod(final Runnable barrier, int counter)
                throws Exceptions.AlertException
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

        @Override
        public long waitFor(final long sequence, LongSupplier cursor, final Runnable barrier)
                throws Exceptions.AlertException, InterruptedException
        {
            long availableSequence;
            int counter = retries;

            while ((availableSequence = cursor.getAsLong()) < sequence)
            {
                counter = applyWaitMethod(barrier, counter);
            }

            return availableSequence;
        }
    }

    final static class Yielding extends WaitStrategy {

        private static final int SPIN_TRIES = 100;

        private int applyWaitMethod(final Runnable barrier, int counter)
                throws Exceptions.AlertException
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

        @Override
        public long waitFor(final long sequence, LongSupplier cursor, final Runnable barrier)
                throws Exceptions.AlertException, InterruptedException
        {
            long availableSequence;
            int counter = SPIN_TRIES;

            while ((availableSequence = cursor.getAsLong()) < sequence)
            {
                counter = applyWaitMethod(barrier, counter);
            }

            return availableSequence;
        }
    }
}
