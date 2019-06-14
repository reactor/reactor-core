/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.util.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Strategy employed to wait for specific {@link LongSupplier} values with various spinning strategies.
 * @deprecated Has been moved to io.projectreactor.addons:reactor-extra:3.3.0+ and will be removed in 3.4.0
 */
@Deprecated
public abstract class WaitStrategy {

    /**
     * Blocking strategy that uses a lock and condition variable for consumer waiting on a barrier.
     *
     * This strategy can be used when throughput and low-latency are not as important as CPU resource.
     * @return the wait strategy
     */
    public static WaitStrategy blocking() {
        return new Blocking();
    }

    /**
     * Busy Spin strategy that uses a busy spin loop for consumers waiting on a barrier.
     *
     * This strategy will use CPU resource to avoid syscalls which can introduce latency jitter.  It is best
     * used when threads can be bound to specific CPU cores.
     * @return the wait strategy
     */
    public static WaitStrategy busySpin() {
        return BusySpin.INSTANCE;
    }

    /**
     * Test if exception is alert
     * @param t exception checked
     * @return true if this is an alert signal
     */
    public static boolean isAlert(Throwable t){
	    return t == AlertException.INSTANCE;
    }

    /**
     * Variation of the {@link #blocking()} that attempts to elide conditional wake-ups when the lock is uncontended.
     * Shows performance improvements on microbenchmarks.  However this wait strategy should be considered experimental
     * as I have not full proved the correctness of the lock elision code.
     * @return the wait strategy
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
     * @return the wait strategy
     */
    public static WaitStrategy parking() {
        return Parking.INSTANCE;
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
     * @return the wait strategy
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
     * @param spinTimeout the spin timeout
     * @param yieldTimeout the yield timeout
     * @param units the time unit
     * @param delegate the target wait strategy to fallback on
     * @return the wait strategy
     */
    public static WaitStrategy phasedOff(long spinTimeout, long yieldTimeout, TimeUnit units, WaitStrategy delegate) {
        return new PhasedOff(spinTimeout, yieldTimeout, units, delegate);
    }

    /**
     * Block with wait/notifyAll semantics
     * @param spinTimeout the spin timeout
     * @param yieldTimeout the yield timeout
     * @param units the time unit
     * @return the wait strategy
     */
    public static WaitStrategy phasedOffLiteLock(long spinTimeout, long yieldTimeout, TimeUnit units) {
        return phasedOff(spinTimeout, yieldTimeout, units, liteBlocking());
    }

    /**
     * Block with wait/notifyAll semantics
     * @param spinTimeout the spin timeout
     * @param yieldTimeout the yield timeout
     * @param units the time unit
     * @return the wait strategy
     */
    public static WaitStrategy phasedOffLock(long spinTimeout, long yieldTimeout, TimeUnit units) {
        return phasedOff(spinTimeout, yieldTimeout, units, blocking());
    }

    /**
     * Block by parking in a loop
     * @param spinTimeout the spin timeout
     * @param yieldTimeout the yield timeout
     * @param units the time unit
     * @return the wait strategy
     */
    public static WaitStrategy phasedOffSleep(long spinTimeout, long yieldTimeout, TimeUnit units) {
        return phasedOff(spinTimeout, yieldTimeout, units, parking(0));
    }

    /**
     * Yielding strategy that uses a Thread.sleep(1) for consumers waiting on a
     * barrier
     * after an initially spinning.
     *
     * This strategy will incur up a latency of 1ms and save a maximum CPU resources.
     * @return the wait strategy
     */
    public static WaitStrategy sleeping() {
        return Sleeping.INSTANCE;
    }

    /**
     * Yielding strategy that uses a Thread.yield() for consumers waiting on a
     * barrier
     * after an initially spinning.
     *
     * This strategy is a good compromise between performance and CPU resource without incurring significant latency spikes.
     * @return the wait strategy
     */
    public static WaitStrategy yielding() {
        return Yielding.INSTANCE;
    }

    /**
     * Implementations should signal the waiting consumers that the cursor has advanced.
     */
    public void signalAllWhenBlocking() {
    }

    /**
     * Wait for the given sequence to be available.  It is possible for this method to return a value
     * less than the sequence number supplied depending on the implementation of the WaitStrategy.  A common
     * use for this is to signal a timeout.  Any EventProcessor that is using a WaitStrategy to get notifications
     * about message becoming available should remember to handle this case.
     *
     * @param sequence to be waited on.
     * @param cursor the main sequence from ringbuffer. Wait/notify strategies will
     *    need this as is notified upon update.
     * @param spinObserver Spin observer
     * @return the sequence that is available which may be greater than the requested sequence.
     * @throws InterruptedException if the thread is interrupted.
     */
    public abstract long waitFor(long sequence, LongSupplier cursor, Runnable spinObserver)
            throws InterruptedException;

	/**
	 * Throw an Alert signal exception (singleton) that can be checked against {@link #isAlert(Throwable)}
	 */
    public static void alert(){
	    throw AlertException.INSTANCE;
    }

    /**
     * Used to alert consumers waiting with a {@link WaitStrategy} for status changes.
     * <p>
     * It does not fill in a stack trace for performance reasons.
     */
    @SuppressWarnings("serial")
    static final class AlertException extends RuntimeException {
	    /** Pre-allocated exception to avoid garbage generation */
	    public static final AlertException INSTANCE = new AlertException();

	    /**
	     * Private constructor so only a single instance any.
	     */
	    private AlertException() {
	    }

	    /**
	     * Overridden so the stack trace is not filled in for this exception for performance reasons.
	     *
	     * @return this instance.
	     */
	    @Override
	    public Throwable fillInStackTrace() {
		    return this;
	    }

    }

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

        @SuppressWarnings("UnusedAssignment") //for availableSequence
        @Override
        public long waitFor(long sequence, LongSupplier cursorSequence, Runnable barrier)
                throws InterruptedException
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

	    static final WaitStrategy.BusySpin
			    INSTANCE = new WaitStrategy.BusySpin();

        @Override
        public long waitFor(final long sequence, LongSupplier cursor, final Runnable barrier)
                throws InterruptedException
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

	    static final WaitStrategy.Sleeping
			    INSTANCE = new WaitStrategy.Sleeping();

        @Override
        public long waitFor(final long sequence,
                LongSupplier cursor,
                final Runnable barrier)
                throws InterruptedException {
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

        @SuppressWarnings("UnusedAssignment") //for availableSequence
        @Override
        public long waitFor(long sequence, LongSupplier cursorSequence, Runnable barrier)
                throws InterruptedException
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

        private final long                                 spinTimeoutNanos;
        private final long                                 yieldTimeoutNanos;
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
                throws InterruptedException
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
        private static final int SPIN_TRIES = 10000;
    }

    final static class Parking extends WaitStrategy {

	    static final WaitStrategy.Parking
			    INSTANCE = new WaitStrategy.Parking();

        private final int retries;

        Parking() {
            this(DEFAULT_RETRIES);
        }

        Parking(int retries) {
            this.retries = retries;
        }

        @Override
        public long waitFor(final long sequence, LongSupplier cursor, final Runnable barrier)
                throws InterruptedException
        {
            long availableSequence;
            int counter = retries;

            while ((availableSequence = cursor.getAsLong()) < sequence)
            {
                counter = applyWaitMethod(barrier, counter);
            }

            return availableSequence;
        }

        private int applyWaitMethod(final Runnable barrier, int counter)
                throws WaitStrategy.AlertException
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
        private static final int DEFAULT_RETRIES = 200;
    }

    final static class Yielding extends WaitStrategy {

	    static final WaitStrategy.Yielding
			    INSTANCE = new WaitStrategy.Yielding();

	    @Override
	    public long waitFor(final long sequence, LongSupplier cursor, final Runnable barrier)
			    throws InterruptedException {
		    long availableSequence;
		    int counter = SPIN_TRIES;

		    while ((availableSequence = cursor.getAsLong()) < sequence) {
			    counter = applyWaitMethod(barrier, counter);
		    }

		    return availableSequence;
	    }

	    private int applyWaitMethod(final Runnable barrier, int counter)
			    throws WaitStrategy.AlertException {
		    barrier.run();

		    if (0 == counter) {
			    Thread.yield();
		    }
		    else {
			    --counter;
		    }

		    return counter;
	    }

	    private static final int SPIN_TRIES = 100;
    }
}
