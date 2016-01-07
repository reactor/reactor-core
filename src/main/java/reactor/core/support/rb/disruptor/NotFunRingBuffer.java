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


import reactor.core.error.InsufficientCapacityException;
import reactor.fn.Supplier;

abstract class NotFunRingBufferFields<E> extends RingBuffer<E>
{
    private final long indexMask;
    private final Object[] entries;
    protected final int bufferSize;
    protected final Sequencer sequencer;

    NotFunRingBufferFields(Supplier<E> eventFactory,
                     Sequencer       sequencer)
    {
        this.sequencer  = sequencer;
        this.bufferSize = sequencer.getBufferSize();

        if (bufferSize < 1)
        {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }

        this.indexMask = bufferSize - 1;
        this.entries   = new Object[sequencer.getBufferSize()];
        fill(eventFactory);
    }

    private void fill(Supplier<E> eventFactory)
    {
        for (int i = 0; i < bufferSize; i++)
        {
            entries[i] = eventFactory.get();
        }
    }

    @SuppressWarnings("unchecked")
    protected final E elementAt(long sequence)
    {
        return (E) entries[(int) (sequence & indexMask)];
    }
}

/**
 * Ring based store of reusable entries containing the data representing
 * an event being exchanged between event producer and ringbuffer consumers.
 *
 * @param <E> implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
public final class NotFunRingBuffer<E> extends NotFunRingBufferFields<E>
{
    /**
     * Construct a RingBuffer with the full option set.
     *
     * @param eventFactory to newInstance entries for filling the RingBuffer
     * @param sequencer sequencer to handle the ordering of events moving through the RingBuffer.
     * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
     */
    NotFunRingBuffer(Supplier<E> eventFactory,
                     Sequencer sequencer)
    {
        super(eventFactory, sequencer);
    }

    @Override
    public E get(long sequence)
    {
        return elementAt(sequence);
    }

    @Override
    public long next()
    {
        return sequencer.next();
    }

    @Override
    public long next(int n)
    {
        return sequencer.next(n);
    }

    @Override
    public long tryNext() throws InsufficientCapacityException
    {
        return sequencer.tryNext();
    }

    @Override
    public long tryNext(int n) throws InsufficientCapacityException
    {
        return sequencer.tryNext(n);
    }

    @Override
    public void resetTo(long sequence)
    {
        sequencer.claim(sequence);
        sequencer.publish(sequence);
    }

    @Override
    public E claimAndGetPreallocated(long sequence)
    {
        sequencer.claim(sequence);
        return get(sequence);
    }

    @Override
    public boolean isPublished(long sequence)
    {
        return sequencer.isAvailable(sequence);
    }

    @Override
    public void addGatingSequences(Sequence... gatingSequences)
    {
        sequencer.addGatingSequences(gatingSequences);
    }

    @Override
    public void addGatingSequence(Sequence gatingSequence)
    {
        sequencer.addGatingSequence(gatingSequence);
    }

    @Override
    public long getMinimumGatingSequence()
    {
        return getMinimumGatingSequence(null);
    }

    @Override
    public long getMinimumGatingSequence(Sequence sequence)
    {
        return sequencer.getMinimumSequence(sequence);
    }

    @Override
    public boolean removeGatingSequence(Sequence sequence)
    {
        return sequencer.removeGatingSequence(sequence);
    }

    @Override
    public SequenceBarrier newBarrier()
    {
        return sequencer.newBarrier();
    }

    @Override
    public long getCursor()
    {
        return sequencer.getCursor();
    }

    @Override
    public Sequence getSequence()
    {
        return sequencer.getSequence();
    }

    @Override
    public int getBufferSize()
    {
        return bufferSize;
    }

    @Override
    public boolean hasAvailableCapacity(int requiredCapacity)
    {
        return sequencer.hasAvailableCapacity(requiredCapacity);
    }

    @Override
    public void publish(long sequence)
    {
        sequencer.publish(sequence);
    }

    @Override
    public void publish(long lo, long hi)
    {
        sequencer.publish(lo, hi);
    }

    @Override
    public long remainingCapacity()
    {
        return sequencer.remainingCapacity();
    }

    @Override
    public long pending()
    {
        return sequencer.pending();
    }

    @Override
    public Sequencer getSequencer() {
        return sequencer;
    }

    @Override
    public long cachedRemainingCapacity()
    {
        return sequencer.cachedRemainingCapacity();
    }


}
