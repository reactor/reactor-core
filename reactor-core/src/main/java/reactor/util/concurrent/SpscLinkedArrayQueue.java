/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiPredicate;

import reactor.util.annotation.Nullable;


/**
 * An unbounded, array-backed single-producer, single-consumer queue with a fixed link
 * size.
 * <p>
 * This implementation is based on JCTools' SPSC algorithms: <a
 * href='https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/SpscUnboundedArrayQueue.java'>SpscUnboundedArrayQueue</a>
 * and <a href='https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/atomic/SpscUnboundedAtomicArrayQueue.java'>SpscUnboundedAtomicArrayQueue</a>
 * of which the {@code SpscUnboundedAtomicArrayQueue} was contributed by one of the
 * authors of this library. The notable difference is that this class is not padded and
 * there is no lookahead cache involved; padding has a toll on short lived or bursty uses
 * and lookahead doesn't really matter with small queues.
 *
 * @param <T> the value type
 */
final class SpscLinkedArrayQueue<T> extends AbstractQueue<T>
		implements BiPredicate<T, T> {

	final int mask;

	volatile long producerIndex;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<SpscLinkedArrayQueue> PRODUCER_INDEX =
			AtomicLongFieldUpdater.newUpdater(SpscLinkedArrayQueue.class,
					"producerIndex");
	AtomicReferenceArray<Object> producerArray;

	volatile long consumerIndex;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<SpscLinkedArrayQueue> CONSUMER_INDEX =
			AtomicLongFieldUpdater.newUpdater(SpscLinkedArrayQueue.class,
					"consumerIndex");
	AtomicReferenceArray<Object> consumerArray;

	static final Object NEXT = new Object();

	SpscLinkedArrayQueue(int linkSize) {
		int c = Queues.ceilingNextPowerOfTwo(Math.max(8, linkSize));
		this.producerArray = this.consumerArray = new AtomicReferenceArray<>(c + 1);
		this.mask = c - 1;
	}

	@Override
	public boolean offer(T e) {
		Objects.requireNonNull(e);

		long pi = producerIndex;
		AtomicReferenceArray<Object> a = producerArray;
		int m = mask;

		int offset = (int) (pi + 1) & m;

		if (a.get(offset) != null) {
			offset = (int) pi & m;

			AtomicReferenceArray<Object> b = new AtomicReferenceArray<>(m + 2);
			producerArray = b;
			b.lazySet(offset, e);
			a.lazySet(m + 1, b);
			a.lazySet(offset, NEXT);
			PRODUCER_INDEX.lazySet(this, pi + 1);
		}
		else {
			offset = (int) pi & m;
			a.lazySet(offset, e);
			PRODUCER_INDEX.lazySet(this, pi + 1);
		}

		return true;
	}

	/**
	 * Offer two elements at the same time.
	 * <p>Don't use the regular offer() with this at all!
	 *
	 * @param first the first value, not null
	 * @param second the second value, not null
	 *
	 * @return true if the queue accepted the two new values
	 */
	@Override
	public boolean test(T first, T second) {
		final AtomicReferenceArray<Object> buffer = producerArray;
		final long p = producerIndex;
		final int m = mask;

		int pi = (int) (p + 2) & m;

		if (null != buffer.get(pi)) {
			final AtomicReferenceArray<Object> newBuffer =
					new AtomicReferenceArray<>(m + 2);
			producerArray = newBuffer;

			pi = (int) p & m;
			newBuffer.lazySet(pi + 1, second);// StoreStore
			newBuffer.lazySet(pi, first);
			buffer.lazySet(buffer.length() - 1, newBuffer);

			buffer.lazySet(pi, NEXT); // new buffer is visible after element is

			PRODUCER_INDEX.lazySet(this, p + 2);// this ensures correctness on 32bit
			// platforms
		}
		else {
			pi = (int) p & m;
			buffer.lazySet(pi + 1, second);
			buffer.lazySet(pi, first);
			PRODUCER_INDEX.lazySet(this, p + 2);
		}

		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	@Nullable
	public T poll() {
		long ci = consumerIndex;
		AtomicReferenceArray<Object> a = consumerArray;
		int m = mask;

		int offset = (int) ci & m;

		Object o = a.get(offset);

		if (o == null) {
			return null;
		}
		if (o == NEXT) {
			AtomicReferenceArray<Object> b = (AtomicReferenceArray<Object>) a.get(m + 1);
			a.lazySet(m + 1, null);
			o = b.get(offset);
			a = b;
			consumerArray = b;
		}
		a.lazySet(offset, null);
		CONSUMER_INDEX.lazySet(this, ci + 1);

		return (T) o;
	}

	@SuppressWarnings("unchecked")
	@Override
	@Nullable
	public T peek() {
		long ci = consumerIndex;
		AtomicReferenceArray<Object> a = consumerArray;
		int m = mask;

		int offset = (int) ci & m;

		Object o = a.get(offset);

		if (o == null) {
			return null;
		}
		if (o == NEXT) {
			a = (AtomicReferenceArray<Object>) a.get(m + 1);
			o = a.get(offset);
		}

		return (T) o;
	}

	@Override
	public boolean isEmpty() {
		return producerIndex == consumerIndex;
	}

	@Override
	public int size() {
		long ci = consumerIndex;
		for (; ; ) {
			long pi = producerIndex;
			long ci2 = consumerIndex;
			if (ci == ci2) {
				return (int) (pi - ci);
			}
			ci = ci2;
		}
	}

	@Override
	public void clear() {
		while (poll() != null && !isEmpty()) {
		}
	}

	@Override
	public Iterator<T> iterator() {
		throw new UnsupportedOperationException();
	}
}
