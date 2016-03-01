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

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A bounded, array backed, single-producer single-consumer queue.
 * 
 * This implementation is implemented based on by JCTools' SPSC algorithms:
 * <a href='https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/SpscArrayQueue.java'>SpscArrayQueue</a>
 * and <a href='https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/atomic/SpscAtomicArrayQueue.java'>SpscAtomicArrayQueue</a>
 * of which the {@code SpscAtomicArrayQueue} was contributed by one of the authors of this library. The notable difference
 * is that this class is not padded, inlines the AtomicReferenceArray directly and there is no lookahead cache involved;
 * padding has a toll on short lived or bursty uses and lookahead doesn't really matter with small queues.
 * 
 * @param <T> the value type
 */
final class SpscArrayQueue<T> extends AtomicReferenceArray<T> implements Queue<T> {
    /** */
    private static final long serialVersionUID = 494623116936946976L;

    volatile long producerIndex;
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<SpscArrayQueue> PRODUCER_INDEX =
            AtomicLongFieldUpdater.newUpdater(SpscArrayQueue.class, "producerIndex");

    volatile long consumerIndex;
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<SpscArrayQueue> CONSUMER_INDEX =
            AtomicLongFieldUpdater.newUpdater(SpscArrayQueue.class, "consumerIndex");
    
    
    final int mask;
    
    public SpscArrayQueue(int capacity) {
        super(RingBuffer.ceilingNextPowerOfTwo(capacity));
        mask = length() - 1;
    }
    
    @Override
    public boolean offer(T e) {
        Objects.requireNonNull(e, "e");
        long pi = producerIndex;
        int offset = (int)pi & mask;
        if (get(offset) != null) {
            return false;
        }
        PRODUCER_INDEX.lazySet(this, pi + 1);
        lazySet(offset, e);
        return true;
    }
    
    @Override
    public T poll() {
        long ci = consumerIndex;
        int offset = (int)ci & mask;
        
        T v = get(offset);
        if (v != null) {
            CONSUMER_INDEX.lazySet(this, ci + 1);
            lazySet(offset, null);
        }
        return v;
    }
    
    @Override
    public T peek() {
        int offset = (int)consumerIndex & mask;
        return get(offset);
    }
    
    @Override
    public boolean isEmpty() {
        return producerIndex == consumerIndex;
    }
    
    @Override
    public void clear() {
        while (poll() != null && !isEmpty());
    }

    @Override
    public int size() {
        long ci = consumerIndex;
        for (;;) {
            long pi = producerIndex;
            long ci2 = consumerIndex;
            if (ci == ci2) {
                return (int)(pi - ci);
            }
            ci = ci2;
        }
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> R[] toArray(R[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(T e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public T element() {
        throw new UnsupportedOperationException();
    }
}
