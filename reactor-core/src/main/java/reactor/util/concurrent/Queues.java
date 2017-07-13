/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.concurrent;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

/**
 * Utility methods around {@link Queue Queues} and pre-defined {@link QueueSupplier suppliers}.
 */
public class Queues {

	public static final int CAPACITY_UNSURE = Integer.MIN_VALUE;

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
			return QueueSupplier.SMALL_UNBOUNDED;
		}
		if (batchSize == XS_BUFFER_SIZE) {
			return QueueSupplier.XS_SUPPLIER;
		}
		if (batchSize == SMALL_BUFFER_SIZE) {
			return QueueSupplier.SMALL_SUPPLIER;
		}
		if (batchSize == 1) {
			return QueueSupplier.ONE_SUPPLIER;
		}
		return new QueueSupplier<>(Math.max(8, batchSize));
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
		return QueueSupplier.ONE_SUPPLIER;
	}

	/**
	 * @param <T> the reified {@link Queue} generic type
	 *
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> small() {
		return QueueSupplier.SMALL_SUPPLIER;
	}

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return an unbounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> unbounded() {
		return QueueSupplier.SMALL_UNBOUNDED;
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
			return QueueSupplier.XS_UNBOUNDED;
		}
		else if (linkSize == Integer.MAX_VALUE || linkSize == SMALL_BUFFER_SIZE) {
			return unbounded();
		}
		return  () -> new SpscLinkedArrayQueue<>(linkSize);
	}

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> xs() {
		return QueueSupplier.XS_SUPPLIER;
	}


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
		if (q instanceof BlockingQueue) {
			return ((BlockingQueue) q).remainingCapacity();
		}
		else if (q instanceof SpscLinkedArrayQueue) {
			return Integer.MAX_VALUE;
		}
		else if (q instanceof SpscArrayQueue) {
			return ((SpscArrayQueue) q).length();
		}
		else {
			return CAPACITY_UNSURE;
		}
	}

}
