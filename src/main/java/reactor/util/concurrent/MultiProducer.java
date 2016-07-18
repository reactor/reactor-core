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
package reactor.util.concurrent;

import java.util.concurrent.locks.LockSupport;

import sun.misc.Unsafe;

/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Suitable for use for sequencing across multiple publisher threads.</p>
 *
 * <p> 
 * <p>Note on {@code RingBufferProducer.getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@code RingBufferProducer.next()}, to determine the highest available sequence that can be read, then
 * {@code RingBufferProducer.getHighestPublishedSequence(long, long)} should be used.
 */
final class MultiProducer extends RingBufferProducer
{
    private static final Unsafe UNSAFE = RingBuffer.getUnsafe();
    private static final long   BASE   = UNSAFE.arrayBaseOffset(int[].class);
    private static final long   SCALE  = UNSAFE.arrayIndexScale(int[].class);

    private final Sequence gatingSequenceCache = new UnsafeSequence(RingBuffer.INITIAL_CURSOR_VALUE);

    // availableBuffer tracks the state of each ringbuffer slot
    // see below for more details on the approach
    private final int[] availableBuffer;
    private final int   indexMask;
    private final int   indexShift;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    MultiProducer(int bufferSize, final WaitStrategy waitStrategy, Runnable spinObserver) {
        super(bufferSize, waitStrategy, spinObserver);

        if (!QueueSupplier.isPowerOfTwo(bufferSize)) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        availableBuffer = new int[bufferSize];
        indexMask = bufferSize - 1;
        indexShift = RingBuffer.log2(bufferSize);
        initialiseAvailableBuffer();
    }

    /**
     * See {@code RingBufferProducer.hasAvailableCapacity(int)}.
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity) {
        return hasAvailableCapacity(gatingSequences, requiredCapacity, cursor.getAsLong());
    }

    private boolean hasAvailableCapacity(Sequence[] gatingSequences, final int requiredCapacity, long cursorValue) {
        long wrapPoint = (cursorValue + requiredCapacity) - bufferSize;
        long cachedGatingSequence = gatingSequenceCache.getAsLong();

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > cursorValue) {
            long minSequence = RingBuffer.getMinimumSequence(gatingSequences, cursorValue);
            gatingSequenceCache.set(minSequence);

            if (wrapPoint > minSequence) {
                return false;
            }
        }

        return true;
    }

    /**
     * See {@code RingBufferProducer.claim(long)}.
     */
    @Override
    public void claim(long sequence)
    {
        cursor.set(sequence);
    }

    /**
     * See {@code RingBufferProducer.next()}.
     */
    @Override
    public long next()
    {
        return next(1);
    }

    /**
     * See {@code RingBufferProducer.next(int)}.
     */
    @Override
    public long next(int n)
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        long current;
        long next;

        do
        {
            current = cursor.getAsLong();
            next = current + n;

            long wrapPoint = next - bufferSize;
            long cachedGatingSequence = gatingSequenceCache.getAsLong();

            if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
            {
                long gatingSequence = RingBuffer.getMinimumSequence(gatingSequences, current);

                if (wrapPoint > gatingSequence)
                {
                    if(spinObserver != null) {
                        spinObserver.run();
                    }
                    LockSupport.parkNanos(1); // TODO, should we spin based on the wait strategy?
                    continue;
                }

                gatingSequenceCache.set(gatingSequence);
            }
            else if (cursor.compareAndSet(current, next))
            {
                break;
            }
        }
        while (true);

        return next;
    }

    /**
     * See {@code RingBufferProducer.tryNext()}.
     */
    @Override
    public long tryNext() throws RingBuffer.InsufficientCapacityException
    {
        return tryNext(1);
    }

    /**
     * See {@code RingBufferProducer.tryNext(int)}.
     */
    @Override
    public long tryNext(int n) throws RingBuffer.InsufficientCapacityException
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        long current;
        long next;

        do
        {
            current = cursor.getAsLong();
            next = current + n;

            if (!hasAvailableCapacity(gatingSequences, n, current))
            {
                throw RingBuffer.InsufficientCapacityException.INSTANCE;
            }
        }
        while (!cursor.compareAndSet(current, next));

        return next;
    }

    /**
     * See {@code RingBufferProducer.remainingCapacity()}.
     */
    @Override
    public long remainingCapacity()
    {
        return getBufferSize() - getPending();
    }
    /**
     * See {@code RingBufferProducer.getPending()}.
     */
    @Override
    public long getPending()
    {
        long consumed = RingBuffer.getMinimumSequence(gatingSequences, cursor.getAsLong());
        long produced = cursor.getAsLong();
        return produced - consumed;
    }


    @Override
    public long cachedRemainingCapacity() {
        long consumed = gatingSequenceCache.getAsLong();
        long produced = cursor.getAsLong();
        return getBufferSize() - (produced - consumed);
    }

    private void initialiseAvailableBuffer()
    {
        for (int i = availableBuffer.length - 1; i != 0; i--)
        {
            setAvailableBufferValue(i, -1);
        }

        setAvailableBufferValue(0, -1);
    }

    /**
     * See {@code RingBufferProducer.publish(long)}.
     */
    @Override
    public void publish(final long sequence)
    {
        setAvailable(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * See {@code RingBufferProducer.publish(long, long)}.
     */
    @Override
    public void publish(long lo, long hi)
    {
        for (long l = lo; l <= hi; l++)
        {
            setAvailable(l);
        }
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * The below methods work on the availableBuffer flag.
     *
     * The prime reason is to avoid a shared sequence object between publisher threads.
     * (Keeping single pointers tracking start and end would require coordination
     * between the threads).
     *
     * --  Firstly we have the constraint that the delta between the cursor and minimum
     * gating sequence will never be larger than the buffer size (the code in
     * next/tryNext in the Sequence takes care of that).
     * -- Given that; take the sequence value and mask off the lower portion of the
     * sequence as the index into the buffer (indexMask). (aka modulo operator)
     * -- The upper portion of the sequence becomes the value to check for availability.
     * ie: it tells us how many times around the ring buffer we've been (aka division)
     * -- Because we can't wrap without the gating sequences moving forward (i.e. the
     * minimum gating sequence is effectively our last available position in the
     * buffer), when we have new data and successfully claimed a slot we can simply
     * write over the top.
     */
    private void setAvailable(final long sequence)
    {
        setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
    }

    private void setAvailableBufferValue(int index, int flag)
    {
        long bufferAddress = (index * SCALE) + BASE;
        UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
    }

    /**
     * See {@code RingBufferProducer.isAvailable(long)}
     */
    @Override
    public boolean isAvailable(long sequence)
    {
        int index = calculateIndex(sequence);
        int flag = calculateAvailabilityFlag(sequence);
        long bufferAddress = (index * SCALE) + BASE;
        return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag;
    }

    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence)
    {
        for (long sequence = lowerBound; sequence <= availableSequence; sequence++)
        {
            if (!isAvailable(sequence))
            {
                return sequence - 1;
            }
        }

        return availableSequence;
    }

    private int calculateAvailabilityFlag(final long sequence)
    {
        return (int) (sequence >>> indexShift);
    }

    private int calculateIndex(final long sequence)
    {
        return ((int) sequence) & indexMask;
    }
}
