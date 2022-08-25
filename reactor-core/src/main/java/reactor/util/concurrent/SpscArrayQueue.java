/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.concurrent;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;

import reactor.util.annotation.Nullable;


/**
 * A bounded, array backed, single-producer single-consumer queue.
 * 
 * This implementation is based on JCTools' SPSC algorithms:
 * <a href='https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/SpscArrayQueue.java'>SpscArrayQueue</a>
 * and <a href='https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/atomic/SpscAtomicArrayQueue.java'>SpscAtomicArrayQueue</a>
 * of which the {@code SpscAtomicArrayQueue} was contributed by one of the authors of this library. The notable difference
 * is that this class inlines the AtomicReferenceArray directly and there is no lookahead cache involved;
 * item padding has a toll on short lived or bursty uses and lookahead doesn't really matter with small queues.
 * 
 * @param <T> the value type
 */
final class SpscArrayQueue<T> extends SpscArrayQueueP3<T> implements Queue<T> {
	/** */
	private static final long serialVersionUID = 494623116936946976L;

	SpscArrayQueue(int capacity) {
		super(Queues.ceilingNextPowerOfTwo(capacity));
	}
	
	@Override
	public boolean offer(T e) {
		Objects.requireNonNull(e, "e");
		long pi = producerIndex;
		int offset = (int)pi & mask;
		if (get(offset) != null) {
			return false;
		}
		lazySet(offset, e);
		PRODUCER_INDEX.lazySet(this, pi + 1);
		return true;
	}
	
	@Override
	@Nullable
	public T poll() {
		long ci = consumerIndex;
		int offset = (int)ci & mask;
		
		T v = get(offset);
		if (v != null) {
			lazySet(offset, null);
			CONSUMER_INDEX.lazySet(this, ci + 1);
		}
		return v;
	}
	
	@Override
	@Nullable
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

class SpscArrayQueueCold<T> extends AtomicReferenceArray<T> {
	/** */
	private static final long serialVersionUID = 8491797459632447132L;

	final int mask;
	
	public SpscArrayQueueCold(int length) {
		super(length);
		mask = length - 1;
	}
}
class SpscArrayQueueP1<T> extends SpscArrayQueueCold<T> {
	/** */
	private static final long serialVersionUID = -4461305682174876914L;

	byte b000,b001,b002,b003,b004,b005,b006,b007;//  8b
	byte b010,b011,b012,b013,b014,b015,b016,b017;// 16b
	byte b020,b021,b022,b023,b024,b025,b026,b027;// 24b
	byte b030,b031,b032,b033,b034,b035,b036,b037;// 32b
	byte b040,b041,b042,b043,b044,b045,b046,b047;// 40b
	byte b050,b051,b052,b053,b054,b055,b056,b057;// 48b
	byte b060,b061,b062,b063,b064,b065,b066,b067;// 56b
	byte b070,b071,b072,b073,b074,b075,b076,b077;// 64b
	byte b100,b101,b102,b103,b104,b105,b106,b107;// 72b
	byte b110,b111,b112,b113,b114,b115,b116,b117;// 80b
	byte b120,b121,b122,b123,b124,b125,b126,b127;// 88b
	byte b130,b131,b132,b133,b134,b135,b136,b137;// 96b
	byte b140,b141,b142,b143,b144,b145,b146,b147;//104b
	byte b150,b151,b152,b153,b154,b155,b156,b157;//112b
	byte b160,b161,b162,b163,b164,b165,b166,b167;//120b
	byte b170,b171,b172,b173,b174,b175,b176,b177;//128b

	SpscArrayQueueP1(int length) {
		super(length);
	}
}

class SpscArrayQueueProducer<T> extends SpscArrayQueueP1<T> {

	/** */
	private static final long serialVersionUID = 1657408315616277653L;
	
	SpscArrayQueueProducer(int length) {
		super(length);
	}

	volatile long producerIndex;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<SpscArrayQueueProducer> PRODUCER_INDEX =
			AtomicLongFieldUpdater.newUpdater(SpscArrayQueueProducer.class, "producerIndex");

}

class SpscArrayQueueP2<T> extends SpscArrayQueueProducer<T> {
	/** */
	private static final long serialVersionUID = -5400235061461013116L;

	byte b000,b001,b002,b003,b004,b005,b006,b007;//  8b
	byte b010,b011,b012,b013,b014,b015,b016,b017;// 16b
	byte b020,b021,b022,b023,b024,b025,b026,b027;// 24b
	byte b030,b031,b032,b033,b034,b035,b036,b037;// 32b
	byte b040,b041,b042,b043,b044,b045,b046,b047;// 40b
	byte b050,b051,b052,b053,b054,b055,b056,b057;// 48b
	byte b060,b061,b062,b063,b064,b065,b066,b067;// 56b
	byte b070,b071,b072,b073,b074,b075,b076,b077;// 64b
	byte b100,b101,b102,b103,b104,b105,b106,b107;// 72b
	byte b110,b111,b112,b113,b114,b115,b116,b117;// 80b
	byte b120,b121,b122,b123,b124,b125,b126,b127;// 88b
	byte b130,b131,b132,b133,b134,b135,b136,b137;// 96b
	byte b140,b141,b142,b143,b144,b145,b146,b147;//104b
	byte b150,b151,b152,b153,b154,b155,b156,b157;//112b
	byte b160,b161,b162,b163,b164,b165,b166,b167;//120b
	byte b170,b171,b172,b173,b174,b175,b176,b177;//128b

	SpscArrayQueueP2(int length) {
		super(length);
	}
}

class SpscArrayQueueConsumer<T> extends SpscArrayQueueP2<T> {

	/** */
	private static final long serialVersionUID = 4075549732218321659L;
	
	SpscArrayQueueConsumer(int length) {
		super(length);
	}

	volatile long consumerIndex;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<SpscArrayQueueConsumer> CONSUMER_INDEX =
			AtomicLongFieldUpdater.newUpdater(SpscArrayQueueConsumer.class, "consumerIndex");

}

class SpscArrayQueueP3<T> extends SpscArrayQueueConsumer<T> {
	/** */
	private static final long serialVersionUID = -2684922090021364171L;

	byte b000,b001,b002,b003,b004,b005,b006,b007;//  8b
	byte b010,b011,b012,b013,b014,b015,b016,b017;// 16b
	byte b020,b021,b022,b023,b024,b025,b026,b027;// 24b
	byte b030,b031,b032,b033,b034,b035,b036,b037;// 32b
	byte b040,b041,b042,b043,b044,b045,b046,b047;// 40b
	byte b050,b051,b052,b053,b054,b055,b056,b057;// 48b
	byte b060,b061,b062,b063,b064,b065,b066,b067;// 56b
	byte b070,b071,b072,b073,b074,b075,b076,b077;// 64b
	byte b100,b101,b102,b103,b104,b105,b106,b107;// 72b
	byte b110,b111,b112,b113,b114,b115,b116,b117;// 80b
	byte b120,b121,b122,b123,b124,b125,b126,b127;// 88b
	byte b130,b131,b132,b133,b134,b135,b136,b137;// 96b
	byte b140,b141,b142,b143,b144,b145,b146,b147;//104b
	byte b150,b151,b152,b153,b154,b155,b156,b157;//112b
	byte b160,b161,b162,b163,b164,b165,b166,b167;//120b
	byte b170,b171,b172,b173,b174,b175,b176,b177;//128b

	SpscArrayQueueP3(int length) {
		super(length);
	}
}
