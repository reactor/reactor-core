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
package reactor.core.support.rb.disruptor;

import reactor.core.error.AlertException;
import reactor.core.support.WaitStrategy;
import reactor.fn.LongSupplier;

/**
 * Used for Gating ringbuffer consumers on a cursor sequence and optional dependent ringbuffer consumer(s),
 * using the given WaitStrategy.
 */
public final class SequenceBarrier implements Runnable, LongSupplier
{
    private final WaitStrategy waitStrategy;
    private volatile boolean alerted = false;
    private final Sequence  cursorSequence;
    private final Sequencer sequencer;

    public SequenceBarrier(final Sequencer sequencer,
                           final WaitStrategy waitStrategy,
                           final Sequence cursorSequence) {
        this.sequencer = sequencer;
        this.waitStrategy = waitStrategy;
        this.cursorSequence = cursorSequence;
    }

    /**
     * Wait for the given sequence to be available for consumption.
     *
     * @param sequence to wait for
     * @return the sequence up to which is available
     * @throws AlertException if a status change has occurred for the Disruptor
     * @throws InterruptedException if the thread needs awaking on a condition variable.
     */
    public long waitFor(final long sequence)
      throws AlertException, InterruptedException {
        checkAlert();

        long availableSequence = waitStrategy.waitFor(sequence, cursorSequence, this);

        if (availableSequence < sequence) {
            return availableSequence;
        }

        return sequencer.getHighestPublishedSequence(sequence, availableSequence);
    }

    /**
     * Wait for the given sequence to be available for consumption.
     *
     * @param consumer
     * @param sequence to wait for
     * @return the sequence up to which is available
     * @throws AlertException if a status change has occurred for the Disruptor
     * @throws InterruptedException if the thread needs awaking on a condition variable.
     */
    public long waitFor(final long sequence, Runnable consumer)
      throws AlertException, InterruptedException {
        checkAlert();

        long availableSequence = waitStrategy.waitFor(sequence, cursorSequence, consumer);

        if (availableSequence < sequence) {
            return availableSequence;
        }

        return sequencer.getHighestPublishedSequence(sequence, availableSequence);
    }

    /**
         * Get the current cursor value that can be read.
         *
         * @return value of the cursor for entries that have been published.
         */
    public long getCursor()
    {
        return cursorSequence.get();
    }

    /**
         * The current alert status for the barrier.
         *
         * @return true if in alert otherwise false.
         */
    public boolean isAlerted()
    {
        return alerted;
    }

    /**
         * Alert the ringbuffer consumers of a status change and stay in this status until cleared.
         */
    public void alert()
    {
        alerted = true;
        waitStrategy.signalAllWhenBlocking();
    }

    /**
         * Clear the current alert status.
         */
    public void clearAlert()
    {
        alerted = false;
    }

    /**
         * Check if an alert has been raised and throw an {@link AlertException} if it has.
         *
         * @throws AlertException if alert has been raised.
         */
    public void checkAlert() throws AlertException
    {
        if (alerted)
        {
            throw AlertException.INSTANCE;
        }
    }

    @Override
    public long get() {
        return cursorSequence.get();
    }

    @Override
    public void run() {
        checkAlert();
    }
}