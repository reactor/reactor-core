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

	private static final Supplier CLQ_SUPPLIER     = new QueueSupplier<>(Long.MAX_VALUE);
	private static final Supplier ABQ_SUPPLIER     = new QueueSupplier<>(1);
	private static final Supplier XSRB_SUPPLIER    = new QueueSupplier<>(PlatformDependent.XS_BUFFER_SIZE);
	private static final Supplier SMALLRB_SUPPLIER = new QueueSupplier<>(PlatformDependent.SMALL_BUFFER_SIZE);

	private final long batchSize;

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> small(){
		return (Supplier<Queue<T>>)SMALLRB_SUPPLIER;
	}

	/**
	 *
	 * @param <T> the reified {@link Queue} generic type
	 * @return a bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> xs(){
		return (Supplier<Queue<T>>)XSRB_SUPPLIER;
	}

	/**
	 *
	 * @param batchSize
	 * @param <T> the reified {@link Queue} generic type
	 * @return an unbounded or bounded {@link Queue} {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<Queue<T>> get(long batchSize){
		if(batchSize > 10_000_000){
			return (Supplier<Queue<T>>)CLQ_SUPPLIER;
		}
		if(batchSize == 1){
			return (Supplier<Queue<T>>)ABQ_SUPPLIER;
		}
		return new QueueSupplier<>(batchSize);
	}

	QueueSupplier(long batchSize) {
		this.batchSize = batchSize;
	}

	@Override
	public Queue<T> get() {

		if(batchSize > 10_000_000){
			return new ConcurrentLinkedQueue<>();
		}
		else if(batchSize == 1){
			return new ArrayBlockingQueue<>(1);
		}
		return RingBuffer.createSequencedQueue(RingBuffer.<T>createSingleProducer((int) batchSize));
	}
}
