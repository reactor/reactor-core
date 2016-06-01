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

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import reactor.core.util.PlatformDependent;

/**
 * Provide a queue adapted for a given capacity
 *
 * @param <T>
 */
public final class QueueSupplier<T> implements Supplier<Queue<T>> {

	/**
	 * Calculate the next power of 2, greater than or equal to x.<p> From Hacker's Delight, Chapter 3, Harry S. Warren
	 * Jr.
	 *
	 * @param x Value to round up
	 *
	 * @return The next power of 2 from x inclusive
	 */
	public static int ceilingNextPowerOfTwo(final int x) {
		return 1 << (32 - Integer.numberOfLeadingZeros(x - 1));
	}

	/**
	 *
	 * @param batchSize the bounded or unbounded (long.max) queue size
	 * @param <T> the reified {@link Queue} generic type
	 * @return an unbounded or bounded {@link Queue} {@link Supplier}
	 */
	public static <T> Supplier<Queue<T>> get(long batchSize) {
		return get(batchSize, false, false);
	}

	/**
	 * @param batchSize the bounded or unbounded (long.max) queue size
	 * @param waiting if true {@link Queue#offer(Object)} will be spinning if under capacity
	 * @param multiproducer if true {@link Queue#offer(Object)} will support concurrent calls
	 * @param <T> the reified {@link Queue} generic type
	 *
	 * @return an unbounded or bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> get(long batchSize, boolean waiting, boolean multiproducer) {
		if (batchSize > 10_000_000) {
			return (Supplier<Queue<T>>) CLQ_SUPPLIER;
		}
		if (batchSize == PlatformDependent.XS_BUFFER_SIZE) {
			if(waiting) {
				return (Supplier<Queue<T>>) WAITING_XSRB_SUPPLIER;
			}
			else{
				return (Supplier<Queue<T>>) XSRB_SUPPLIER;
			}
		}
		if (batchSize == PlatformDependent.SMALL_BUFFER_SIZE) {
			if(waiting) {
				return (Supplier<Queue<T>>) WAITING_SMALLRB_SUPPLIER;
			}
			else{
				return (Supplier<Queue<T>>) SMALLRB_SUPPLIER;
			}
		}
		if (batchSize == 1 && !waiting) {
			return (Supplier<Queue<T>>) ONE_SUPPLIER;
		}
		return new QueueSupplier<>(batchSize, waiting, multiproducer);
	}

	/**
	 * @param x the int to test
	 *
	 * @return true if x is a power of 2
	 */
	public static boolean isPowerOfTwo(final int x) {
		return Integer.bitCount(x) == 1;
	}

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> one() {
		return (Supplier<Queue<T>>) ONE_SUPPLIER;
	}

	/**
	 * @param <T> the reified {@link Queue} generic type
	 *
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> small() {
		return (Supplier<Queue<T>>)SMALLRB_SUPPLIER;
	}

	/**
	 * @param waiting if true {@link Queue#offer(Object)} will be spinning if under capacity
	 * @param <T> the reified {@link Queue} generic type
	 *
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> small(boolean waiting) {
		if (!waiting) {
			return (Supplier<Queue<T>>) SMALLRB_SUPPLIER;
		}
		else {
			return (Supplier<Queue<T>>) WAITING_SMALLRB_SUPPLIER;
		}
	}

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return an unbounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> unbounded() {
		return (Supplier<Queue<T>>) CLQ_SUPPLIER;
	}

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return an unbounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> unbounded(int linkSize) {
		return  () -> new SpscArrayQueue<>(linkSize);
	}

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> xs() {
		return (Supplier<Queue<T>>) XSRB_SUPPLIER;
	}

	/**
	 *
	 * @param waiting if true {@link Queue#offer(Object)} will be spinning if under capacity
	 * @param <T> the reified {@link Queue} generic type
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> xs(boolean waiting) {
		if (!waiting) {
			return (Supplier<Queue<T>>) XSRB_SUPPLIER;
		}
		else {
			return (Supplier<Queue<T>>) WAITING_XSRB_SUPPLIER;
		}
	}
	final long    batchSize;
	final boolean waiting;
	final boolean multiproducer;

	QueueSupplier(long batchSize, boolean waiting, boolean multiproducer) {
		this.batchSize = batchSize;
		this.waiting = waiting;
		this.multiproducer = multiproducer;
	}

	@Override
	public Queue<T> get() {

		if(batchSize > 10_000_000){
			return new SpscLinkedArrayQueue<>(PlatformDependent.SMALL_BUFFER_SIZE);
		}
		else if (batchSize == 1) {
			if(waiting){
				throw new IllegalArgumentException("Cannot create blocking queues of " +
						"size one");
			}
			return new OneQueue<>();
		}
		else if(waiting) {
			return RingBuffer.blockingBoundedQueue(
					multiproducer ? RingBuffer.createSingleProducer((int) batchSize) :
							RingBuffer .createMultiProducer((int) batchSize),
					-1L);
		}
		else{
			return new SpscArrayQueue<>((int)batchSize);
		}
	}

	static final class OneQueue<T> extends AtomicReference<T> implements Queue<T> {
        @Override
		public boolean add(T t) {

		    while (!offer(t));

		    return true;
		}

		@Override
		public boolean addAll(Collection<? extends T> c) {
			return false;
		}

		@Override
		public void clear() {
			set(null);
		}

		@Override
		public boolean contains(Object o) {
			return Objects.equals(get(), o);
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			return false;
		}

		@Override
		public T element() {
			return get();
		}

		@Override
		public boolean isEmpty() {
			return get() == null;
		}

		@Override
		public Iterator<T> iterator() {
			return new QueueIterator<>(this);
		}

		@Override
		public boolean offer(T t) {
			if (get() != null) {
			    return false;
			}
			lazySet(t);
			return true;
		}

		@Override
		public T peek() {
			return get();
		}

		@Override
		public T poll() {
			T v = get();
			if (v != null) {
			    lazySet(null);
			}
			return v;
		}

		@Override
		public T remove() {
			return getAndSet(null);
		}

		@Override
		public boolean remove(Object o) {
			return false;
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			return false;
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			return false;
		}

		@Override
		public int size() {
			return get() == null ? 0 : 1;
		}

		@Override
		public Object[] toArray() {
			T t = get();
			if (t == null) {
				return new Object[0];
			}
			return new Object[]{t};
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T1> T1[] toArray(T1[] a) {
			if (a.length > 0) {
				a[0] = (T1)get();
				if (a.length > 1) {
				    a[1] = null;
				}
				return a;
			}
			return (T1[])toArray();
		}
		/** */
        private static final long serialVersionUID = -6079491923525372331L;
	}

	static class QueueIterator<T> implements Iterator<T> {

		final Queue<T> queue;

		public QueueIterator(Queue<T> queue) {
			this.queue = queue;
		}

		@Override
		public boolean hasNext() {
			return !queue.isEmpty();
		}

		@Override
		public T next() {
			return queue.poll();
		}

		@Override
		public void remove() {
			queue.remove();
		}
	}
	static final Supplier CLQ_SUPPLIER          = new QueueSupplier<>(Long.MAX_VALUE, false, false);
	static final Supplier ONE_SUPPLIER          = new QueueSupplier<>(1, false, true);
	static final Supplier XSRB_SUPPLIER         = new QueueSupplier<>(PlatformDependent.XS_BUFFER_SIZE, false, false);
	static final Supplier SMALLRB_SUPPLIER      = new QueueSupplier<>(PlatformDependent.SMALL_BUFFER_SIZE, false, false);
	static final Supplier WAITING_XSRB_SUPPLIER = new QueueSupplier<>(PlatformDependent.XS_BUFFER_SIZE, true, false);
	static final Supplier WAITING_SMALLRB_SUPPLIER = new QueueSupplier<>(PlatformDependent.SMALL_BUFFER_SIZE, true, false);
}
