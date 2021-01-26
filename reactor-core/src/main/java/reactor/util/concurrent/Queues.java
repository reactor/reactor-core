/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.util.concurrent;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import reactor.core.publisher.Hooks;
import reactor.util.annotation.Nullable;


/**
 * Queue utilities and suppliers for 1-producer/1-consumer ready queues adapted for
 * various given capacities.
 */
public final class Queues {

	public static final int CAPACITY_UNSURE = Integer.MIN_VALUE;

	/**
	 * Return the capacity of a given {@link Queue} in a best effort fashion. Queues that
	 * are known to be unbounded will return {@code Integer.MAX_VALUE} and queues that
	 * have a known bounded capacity will return that capacity. For other {@link Queue}
	 * implementations not recognized by this method or not providing this kind of
	 * information, {@link #CAPACITY_UNSURE} ({@code Integer.MIN_VALUE}) is returned.
	 *
	 * @param q the {@link Queue} to try to get a capacity for
	 * @return the capacity of the queue, if discoverable with confidence, or {@link #CAPACITY_UNSURE} negative constant.
	 */
	public static final int capacity(Queue q) {
		if (q instanceof ZeroQueue) {
			return 0;
		}
		if (q instanceof OneQueue) {
			return 1;
		}
		if (q instanceof SpscLinkedArrayQueue) {
			return Integer.MAX_VALUE;
		}
		else if (q instanceof SpscArrayQueue) {
			return ((SpscArrayQueue) q).length();
		}
		else if(q instanceof MpscLinkedQueue) {
			return Integer.MAX_VALUE;
		}
		else if (q instanceof BlockingQueue) {
			return ((BlockingQueue) q).remainingCapacity();
		}
		else if (q instanceof ConcurrentLinkedQueue) {
			return Integer.MAX_VALUE;
		}
		else {
			return CAPACITY_UNSURE;
		}
	}

	/**
	 * An allocation friendly default of available slots in a given container, e.g. slow publishers and or fast/few
	 * subscribers
	 */
	public static final int XS_BUFFER_SIZE    = Math.max(8,
			Integer.parseInt(System.getProperty("reactor.bufferSize.x", "32")));
	/**
	 * A small default of available slots in a given container, compromise between intensive pipelines, small
	 * subscribers numbers and memory use.
	 */
	public static final int SMALL_BUFFER_SIZE = Math.max(16,
			Integer.parseInt(System.getProperty("reactor.bufferSize.small", "256")));

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
	 * @param batchSize the bounded or unbounded (int.max) queue size
	 * @param <T> the reified {@link Queue} generic type
	 * @return an unbounded or bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> get(int batchSize) {
		if (batchSize == Integer.MAX_VALUE) {
			return SMALL_UNBOUNDED;
		}
		if (batchSize == XS_BUFFER_SIZE) {
			return XS_SUPPLIER;
		}
		if (batchSize == SMALL_BUFFER_SIZE) {
			return SMALL_SUPPLIER;
		}
		if (batchSize == 1) {
			return ONE_SUPPLIER;
		}
		if (batchSize == 0) {
			return ZERO_SUPPLIER;
		}

		final int adjustedBatchSize = Math.max(8, batchSize);
		if (adjustedBatchSize > 10_000_000) {
			return SMALL_UNBOUNDED;
		}
		else{
			return () -> Hooks.wrapQueue(new SpscArrayQueue<>(adjustedBatchSize));
		}
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
	 * A {@link Supplier} for an empty immutable {@link Queue}, to be used as a placeholder
	 * in methods that require a Queue when one doesn't expect to store any data in said
	 * Queue.
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return an immutable empty {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> empty() {
		return ZERO_SUPPLIER;
	}

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> one() {
		return ONE_SUPPLIER;
	}

	/**
	 * @param <T> the reified {@link Queue} generic type
	 *
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> small() {
		return SMALL_SUPPLIER;
	}

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return an unbounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> unbounded() {
		return SMALL_UNBOUNDED;
	}

	/**
	 * Returns an unbounded, linked-array-based Queue. Integer.max sized link will
	 * return the default {@link #SMALL_BUFFER_SIZE} size.
	 * @param linkSize the link size
	 * @param <T> the reified {@link Queue} generic type
	 * @return an unbounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> unbounded(int linkSize) {
		if (linkSize == XS_BUFFER_SIZE) {
			return XS_UNBOUNDED;
		}
		else if (linkSize == Integer.MAX_VALUE || linkSize == SMALL_BUFFER_SIZE) {
			return unbounded();
		}
		return  () -> Hooks.wrapQueue(new SpscLinkedArrayQueue<>(linkSize));
	}

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> xs() {
		return XS_SUPPLIER;
	}

	/**
	 * Returns an unbounded queue suitable for multi-producer/single-consumer (MPSC)
	 * scenarios.
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return an unbounded MPSC {@link Queue} {@link Supplier}
	 */
	public static <T> Supplier<Queue<T>> unboundedMultiproducer() {
		return () -> Hooks.wrapQueue(new MpscLinkedQueue<T>());
	}

	private Queues() {
		//prevent construction
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
		@Nullable
		public T peek() {
			return get();
		}

		@Override
		@Nullable
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
			int size = size();
			if (a.length < size) {
				a = (T1[]) java.lang.reflect.Array.newInstance(
						a.getClass().getComponentType(), size);
			}
			if (size == 1) {
				a[0] = (T1) get();
			}
			if (a.length > size) {
				a[size] = null;
			}
			return a;
		}

		private static final long serialVersionUID = -6079491923525372331L;
	}

	static final class ZeroQueue<T> implements Queue<T>, Serializable {

		@Override
		public boolean add(T t) {
			return false;
		}

		@Override
		public boolean addAll(Collection<? extends T> c) {
			return false;
		}

		@Override
		public void clear() {
			//NO-OP
		}

		@Override
		public boolean contains(Object o) {
			return false;
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			return false;
		}

		@Override
		public T element() {
			throw new NoSuchElementException("immutable empty queue");
		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public Iterator<T> iterator() {
			return Collections.emptyIterator();
		}

		@Override
		public boolean offer(T t) {
			return false;
		}

		@Override
		@Nullable
		public T peek() {
			return null;
		}

		@Override
		@Nullable
		public T poll() {
			return null;
		}

		@Override
		public T remove() {
			throw new NoSuchElementException("immutable empty queue");
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
			return 0;
		}

		@Override
		public Object[] toArray() {
			return new Object[0];
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T1> T1[] toArray(T1[] a) {
			if (a.length > 0) {
				a[0] = null;
			}
			return a;
		}

		private static final long serialVersionUID = -8876883675795156827L;
	}

	static final class QueueIterator<T> implements Iterator<T> {

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

    @SuppressWarnings("rawtypes")
    static final Supplier ZERO_SUPPLIER  = () -> Hooks.wrapQueue(new ZeroQueue<>());
    @SuppressWarnings("rawtypes")
    static final Supplier ONE_SUPPLIER   = () -> Hooks.wrapQueue(new OneQueue<>());
	@SuppressWarnings("rawtypes")
    static final Supplier XS_SUPPLIER    = () -> Hooks.wrapQueue(new SpscArrayQueue<>(XS_BUFFER_SIZE));
	@SuppressWarnings("rawtypes")
    static final Supplier SMALL_SUPPLIER = () -> Hooks.wrapQueue(new SpscArrayQueue<>(SMALL_BUFFER_SIZE));
	@SuppressWarnings("rawtypes")
	static final Supplier SMALL_UNBOUNDED =
			() -> Hooks.wrapQueue(new SpscLinkedArrayQueue<>(SMALL_BUFFER_SIZE));
	@SuppressWarnings("rawtypes")
	static final Supplier XS_UNBOUNDED = () -> Hooks.wrapQueue(new SpscLinkedArrayQueue<>(XS_BUFFER_SIZE));
}
