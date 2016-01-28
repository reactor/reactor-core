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

import reactor.core.util.Exceptions;
import reactor.core.util.PlatformDependent;
import reactor.core.util.Sequence;
import reactor.fn.Supplier;
import sun.misc.Unsafe;

abstract class RingBufferPad<E> extends RingBuffer<E>
{
    protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class RingBufferFields<E> extends RingBufferPad<E>
{
    private static final int  BUFFER_PAD;
    private static final long REF_ARRAY_BASE;
    private static final int  REF_ELEMENT_SHIFT;
    private static final Unsafe UNSAFE = PlatformDependent.getUnsafe();

    static {
        final int scale = UNSAFE.arrayIndexScale(Object[].class);
        if (4 == scale) {
            REF_ELEMENT_SHIFT = 2;
        } else if (8 == scale) {
            REF_ELEMENT_SHIFT = 3;
        } else {
            throw new IllegalStateException("Unknown pointer size");
        }
        BUFFER_PAD = 128 / scale;
        // Including the buffer pad in the array base offset
        REF_ARRAY_BASE = UNSAFE.arrayBaseOffset(Object[].class) + (BUFFER_PAD << REF_ELEMENT_SHIFT);
    }

    private final   long               indexMask;
    private final   Object[]           entries;
    protected final int                bufferSize;
    protected final RingBufferProducer sequenceProducer;

    RingBufferFields(Supplier<E> eventFactory,
                     RingBufferProducer sequenceProducer) {
        this.sequenceProducer = sequenceProducer;
        this.bufferSize = sequenceProducer.getBufferSize();

        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        if (!RingBuffer.isPowerOfTwo(bufferSize))
        {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        this.indexMask = bufferSize - 1;
        this.entries   = new Object[sequenceProducer.getBufferSize() + 2 * BUFFER_PAD];
        fill(eventFactory);
    }

    private void fill(Supplier<E> eventFactory)
    {
        for (int i = 0; i < bufferSize; i++)
        {
            entries[BUFFER_PAD + i] = eventFactory.get();
        }
    }

    @SuppressWarnings("unchecked")
    protected final E elementAt(long sequence)
    {
        return (E) UNSAFE.getObject(entries, REF_ARRAY_BASE + ((sequence & indexMask) << REF_ELEMENT_SHIFT));
    }
}

/**
 * Ring based store of reusable entries containing the data representing
 * an event being exchanged between event producer and ringbuffer consumers.
 *
 * @param <E> implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
final class UnsafeRingBuffer<E> extends RingBufferFields<E>
{
    protected long p1, p2, p3, p4, p5, p6, p7;

    /**
     * Construct a RingBuffer with the full option set.
     *
     * @param eventFactory to newInstance entries for filling the RingBuffer
     * @param sequenceProducer sequencer to handle the ordering of events moving through the RingBuffer.
     * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
     */
    UnsafeRingBuffer(Supplier<E> eventFactory,
                     RingBufferProducer sequenceProducer)
    {
        super(eventFactory, sequenceProducer);
    }

    @Override
    public E get(long sequence)
    {
        return elementAt(sequence);
    }

    @Override
    public long next()
    {
        return sequenceProducer.next();
    }

    @Override
    public long next(int n)
    {
        return sequenceProducer.next(n);
    }

    @Override
    public long tryNext() throws Exceptions.InsufficientCapacityException
    {
        return sequenceProducer.tryNext();
    }

    @Override
    public long tryNext(int n) throws Exceptions.InsufficientCapacityException
    {
        return sequenceProducer.tryNext(n);
    }

    @Override
    public void resetTo(long sequence)
    {
        sequenceProducer.claim(sequence);
        sequenceProducer.publish(sequence);
    }

    @Override
    public E claimAndGetPreallocated(long sequence)
    {
        sequenceProducer.claim(sequence);
        return get(sequence);
    }

    @Override
    public boolean isPublished(long sequence)
    {
        return sequenceProducer.isAvailable(sequence);
    }

    @Override
    public void addGatingSequences(Sequence... gatingSequences)
    {
        sequenceProducer.addGatingSequences(gatingSequences);
    }

    @Override
    public void addGatingSequence(Sequence gatingSequence)
    {
        sequenceProducer.addGatingSequence(gatingSequence);
    }

    @Override
    public long getMinimumGatingSequence()
    {
        return getMinimumGatingSequence(null);
    }

    @Override
    public long getMinimumGatingSequence(Sequence sequence)
    {
        return sequenceProducer.getMinimumSequence(sequence);
    }

    @Override
    public boolean removeGatingSequence(Sequence sequence)
    {
        return sequenceProducer.removeGatingSequence(sequence);
    }

    @Override
    public RingBufferReceiver newBarrier()
    {
        return sequenceProducer.newBarrier();
    }

    @Override
    public long getCursor()
    {
        return sequenceProducer.getCursor();
    }

    @Override
    public Sequence getSequence()
    {
        return sequenceProducer.getSequence();
    }

    @Override
    public long getCapacity()
    {
        return bufferSize;
    }

    @Override
    public boolean hasAvailableCapacity(int requiredCapacity)
    {
        return sequenceProducer.hasAvailableCapacity(requiredCapacity);
    }

    @Override
    public void publish(long sequence)
    {
        sequenceProducer.publish(sequence);
    }

    @Override
    public void publish(long lo, long hi)
    {
        sequenceProducer.publish(lo, hi);
    }

    @Override
    public long remainingCapacity()
    {
        return sequenceProducer.remainingCapacity();
    }

    @Override
    public long getPending()
    {
        return sequenceProducer.getPending();
    }

    @Override
    public RingBufferProducer getSequencer() {
        return sequenceProducer;
    }

    @Override
    public long cachedRemainingCapacity()
    {
        return sequenceProducer.cachedRemainingCapacity();
    }


}
