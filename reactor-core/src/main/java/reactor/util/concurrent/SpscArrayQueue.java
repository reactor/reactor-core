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
@SuppressWarnings("unused")
class SpscArrayQueueP1<T> extends SpscArrayQueueCold<T> {
	/** */
	private static final long serialVersionUID = -4461305682174876914L;

	byte pad000,pad001,pad002,pad003,pad004,pad005,pad006,pad007;//  8b
	byte pad010,pad011,pad012,pad013,pad014,pad015,pad016,pad017;// 16b
	byte pad020,pad021,pad022,pad023,pad024,pad025,pad026,pad027;// 24b
	byte pad030,pad031,pad032,pad033,pad034,pad035,pad036,pad037;// 32b
	byte pad040,pad041,pad042,pad043,pad044,pad045,pad046,pad047;// 40b
	byte pad050,pad051,pad052,pad053,pad054,pad055,pad056,pad057;// 48b
	byte pad060,pad061,pad062,pad063,pad064,pad065,pad066,pad067;// 56b
	byte pad070,pad071,pad072,pad073,pad074,pad075,pad076,pad077;// 64b
	byte pad100,pad101,pad102,pad103,pad104,pad105,pad106,pad107;// 72b
	byte pad110,pad111,pad112,pad113,pad114,pad115,pad116,pad117;// 80b
	byte pad120,pad121,pad122,pad123,pad124,pad125,pad126,pad127;// 88b
	byte pad130,pad131,pad132,pad133,pad134,pad135,pad136,pad137;// 96b
	byte pad140,pad141,pad142,pad143,pad144,pad145,pad146,pad147;//104b
	byte pad150,pad151,pad152,pad153,pad154,pad155,pad156,pad157;//112b
	byte pad160,pad161,pad162,pad163,pad164,pad165,pad166,pad167;//120b
	byte pad170,pad171,pad172,pad173,pad174,pad175,pad176,pad177;//128b
	byte pad200,pad201,pad202,pad203;                            //132b
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

@SuppressWarnings("unused")
class SpscArrayQueueP2<T> extends SpscArrayQueueProducer<T> {
	/** */
	private static final long serialVersionUID = -5400235061461013116L;

	byte pad000,pad001,pad002,pad003,pad004,pad005,pad006,pad007;//  8b
	byte pad010,pad011,pad012,pad013,pad014,pad015,pad016,pad017;// 16b
	byte pad020,pad021,pad022,pad023,pad024,pad025,pad026,pad027;// 24b
	byte pad030,pad031,pad032,pad033,pad034,pad035,pad036,pad037;// 32b
	byte pad040,pad041,pad042,pad043,pad044,pad045,pad046,pad047;// 40b
	byte pad050,pad051,pad052,pad053,pad054,pad055,pad056,pad057;// 48b
	byte pad060,pad061,pad062,pad063,pad064,pad065,pad066,pad067;// 56b
	byte pad070,pad071,pad072,pad073,pad074,pad075,pad076,pad077;// 64b
	byte pad100,pad101,pad102,pad103,pad104,pad105,pad106,pad107;// 72b
	byte pad110,pad111,pad112,pad113,pad114,pad115,pad116,pad117;// 80b
	byte pad120,pad121,pad122,pad123,pad124,pad125,pad126,pad127;// 88b
	byte pad130,pad131,pad132,pad133,pad134,pad135,pad136,pad137;// 96b
	byte pad140,pad141,pad142,pad143,pad144,pad145,pad146,pad147;//104b
	byte pad150,pad151,pad152,pad153,pad154,pad155,pad156,pad157;//112b
	byte pad160,pad161,pad162,pad163,pad164,pad165,pad166,pad167;//120b
	byte pad170,pad171,pad172,pad173,pad174,pad175,pad176,pad177;//128b

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

@SuppressWarnings("unused")
class SpscArrayQueueP3<T> extends SpscArrayQueueConsumer<T> {
	/** */
	private static final long serialVersionUID = -2684922090021364171L;

	byte pad000,pad001,pad002,pad003,pad004,pad005,pad006,pad007;//  8b
	byte pad010,pad011,pad012,pad013,pad014,pad015,pad016,pad017;// 16b
	byte pad020,pad021,pad022,pad023,pad024,pad025,pad026,pad027;// 24b
	byte pad030,pad031,pad032,pad033,pad034,pad035,pad036,pad037;// 32b
	byte pad040,pad041,pad042,pad043,pad044,pad045,pad046,pad047;// 40b
	byte pad050,pad051,pad052,pad053,pad054,pad055,pad056,pad057;// 48b
	byte pad060,pad061,pad062,pad063,pad064,pad065,pad066,pad067;// 56b
	byte pad070,pad071,pad072,pad073,pad074,pad075,pad076,pad077;// 64b
	byte pad100,pad101,pad102,pad103,pad104,pad105,pad106,pad107;// 72b
	byte pad110,pad111,pad112,pad113,pad114,pad115,pad116,pad117;// 80b
	byte pad120,pad121,pad122,pad123,pad124,pad125,pad126,pad127;// 88b
	byte pad130,pad131,pad132,pad133,pad134,pad135,pad136,pad137;// 96b
	byte pad140,pad141,pad142,pad143,pad144,pad145,pad146,pad147;//104b
	byte pad150,pad151,pad152,pad153,pad154,pad155,pad156,pad157;//112b
	byte pad160,pad161,pad162,pad163,pad164,pad165,pad166,pad167;//120b
	byte pad170,pad171,pad172,pad173,pad174,pad175,pad176,pad177;//128b

	SpscArrayQueueP3(int length) {
		super(length);
	}
}
