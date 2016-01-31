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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import reactor.core.util.PlatformDependent;
import reactor.fn.Supplier;

/**
 * Provide a queue adapted for a given capacity
 *
 * @param <T>
 */
public final class QueueSupplier<T> implements Supplier<Queue<T>> {

	static final Supplier CLQ_SUPPLIER             = new QueueSupplier<>(Long.MAX_VALUE, false);
	static final Supplier ABQ_SUPPLIER             = new QueueSupplier<>(1, false);
	static final Supplier XSRB_SUPPLIER            = new QueueSupplier<>(PlatformDependent.XS_BUFFER_SIZE, false);
	static final Supplier SMALLRB_SUPPLIER         = new QueueSupplier<>(PlatformDependent.SMALL_BUFFER_SIZE, false);
	static final Supplier WAITING_XSRB_SUPPLIER    = new QueueSupplier<>(PlatformDependent.XS_BUFFER_SIZE, true);
	static final Supplier WAITING_SMALLRB_SUPPLIER = new QueueSupplier<>(PlatformDependent.SMALL_BUFFER_SIZE, true);

	final long    batchSize;
	final boolean waiting;


	/**
	 *
	 * @param batchSize the bounded or unbounded (long.max) queue size
	 * @param <T> the reified {@link Queue} generic type
	 * @return an unbounded or bounded {@link Queue} {@link Supplier}
	 */
	public static <T> Supplier<Queue<T>> get(long batchSize) {
		return get(batchSize, false);
	}

	/**
	 * @param batchSize the bounded or unbounded (long.max) queue size
	 * @param waiting if true {@link Queue#offer(Object)} will be spinning if under capacity
	 * @param <T> the reified {@link Queue} generic type
	 *
	 * @return an unbounded or bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> get(long batchSize, boolean waiting) {
		if (batchSize > 10_000_000) {
			return (Supplier<Queue<T>>) CLQ_SUPPLIER;
		}
		if (batchSize == 1 && !waiting) {
			return (Supplier<Queue<T>>) ABQ_SUPPLIER;
		}
		return new QueueSupplier<>(batchSize, waiting);
	}
	
	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> one() {
		return (Supplier<Queue<T>>)ABQ_SUPPLIER;
	}

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
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

	QueueSupplier(long batchSize, boolean waiting) {
		this.batchSize = batchSize;
		this.waiting = waiting;
	}

	@Override
	public Queue<T> get() {

		if(batchSize > 10_000_000){
			return new ConcurrentLinkedQueue<>();
		}
		else if(batchSize == 1 && !waiting){
			return new ArrayBlockingQueue<>(1);
		}
		else if(waiting) {
			return RingBuffer.blockingBoundedQueue(RingBuffer.<T>createSingleProducer((int) batchSize), -1L);
		}
		else{
			return RingBuffer.nonBlockingBoundedQueue(RingBuffer.<T>createSingleProducer((int) batchSize), -1L);
		}
	}
}
