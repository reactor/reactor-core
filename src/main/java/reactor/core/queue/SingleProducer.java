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
package reactor.core.queue;

import java.util.concurrent.locks.LockSupport;

import reactor.core.util.Exceptions;
import reactor.core.util.Sequence;
import reactor.core.util.WaitStrategy;

abstract class SingleProducerSequencerPad extends RingBufferProducer
{
    protected long p1, p2, p3, p4, p5, p6, p7;
    public SingleProducerSequencerPad(int bufferSize, WaitStrategy waitStrategy, Runnable spinObserver)
    {
        super(bufferSize, waitStrategy, spinObserver);
    }
}

abstract class SingleProducerSequencerFields extends SingleProducerSequencerPad
{
    public SingleProducerSequencerFields(int bufferSize, WaitStrategy waitStrategy, Runnable spinObserver)
    {
        super(bufferSize, waitStrategy, spinObserver);
    }

    /** Set to -1 as sequence starting point */
    protected long nextValue = Sequence.INITIAL_VALUE;
    protected long cachedValue = Sequence.INITIAL_VALUE;
}

/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Not safe for use from multiple threads as it does not implement any barriers.</p>
 *
 * <p>Note on {@code RingBufferProducer.getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@code RingBufferProducer.publish(long)} is made.
 */

final class SingleProducerSequencer extends SingleProducerSequencerFields {
    protected long p1, p2, p3, p4, p5, p6, p7;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     * @param spinObserver the runnable to call on a spin-wait
     */
    public SingleProducerSequencer(int bufferSize, final WaitStrategy waitStrategy, Runnable spinObserver) {
        super(bufferSize, waitStrategy, spinObserver);
    }

    /**
     * See {@code RingBufferProducer.hasAvailableCapacity(int)}.
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity) {
        long nextValue = this.nextValue;

        long wrapPoint = (nextValue + requiredCapacity) - bufferSize;
        long cachedGatingSequence = this.cachedValue;

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue) {
            long minSequence = RingBuffer.getMinimumSequence(gatingSequences, nextValue);
            this.cachedValue = minSequence;

            if (wrapPoint > minSequence)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * See {@code RingBufferProducer.next()}.
     */
    @Override
    public long next() {
        return next(1);
    }

    /**
     * See {@code RingBufferProducer.next(int)}.
     */
    @Override
    public long next(int n) {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        long nextValue = this.nextValue;

        long nextSequence = nextValue + n;
        long wrapPoint = nextSequence - bufferSize;
        long cachedGatingSequence = this.cachedValue;

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue)
        {
            long minSequence;
            while (wrapPoint > (minSequence = RingBuffer.getMinimumSequence(gatingSequences, nextValue)))
            {
                if(spinObserver != null) {
                    spinObserver.run();
                }
                LockSupport.parkNanos(1L); // TODO: Use waitStrategy to spin?
            }

            this.cachedValue = minSequence;
        }

        this.nextValue = nextSequence;

        return nextSequence;
    }

    /**
     * See {@code RingBufferProducer.tryNext()}.
     */
    @Override
    public long tryNext() throws Exceptions.InsufficientCapacityException {
        return tryNext(1);
    }

    /**
     * See {@code RingBufferProducer.tryNext(int)}.
     */
    @Override
    public long tryNext(int n) throws Exceptions.InsufficientCapacityException {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        if (!hasAvailableCapacity(n))
        {
            throw Exceptions.failWithOverflow();
        }

        long nextSequence = this.nextValue += n;

        return nextSequence;
    }

    /**
     * See {@code RingBufferProducer.remainingCapacity()}.
     */
    @Override
    public long remainingCapacity() {
        return getBufferSize() - getPending();
    }

    /**
     * See {@code RingBufferProducer.getPending()}.
     */
    @Override
    public long getPending() {
        long nextValue = this.nextValue;

        long consumed = RingBuffer.getMinimumSequence(gatingSequences, nextValue);
        long produced = nextValue;
        return produced - consumed;
    }

    @Override
    public long cachedRemainingCapacity() {
        long nextValue = this.nextValue;

        long consumed = cachedValue;
        long produced = nextValue;
        return getBufferSize() - (produced - consumed);
    }

    /**
     * See {@code RingBufferProducer.claim(long)}.
     */
    @Override
    public void claim(long sequence) {
        this.nextValue = sequence;
    }

    /**
     * See {@code RingBufferProducer.publish(long)}.
     */
    @Override
    public void publish(long sequence) {
        cursor.set(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * See {@code RingBufferProducer.publish(long, long)}.
     */
    @Override
    public void publish(long lo, long hi) {
        publish(hi);
    }

    /**
     * See {@code RingBufferProducer.isAvailable(long)}.
     */
    @Override
    public boolean isAvailable(long sequence) {
        return sequence <= cursor.getAsLong();
    }

    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence) {
        return availableSequence;
    }
}
