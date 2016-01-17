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
package reactor.core.support;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import reactor.core.support.rb.disruptor.RingBuffer;
import reactor.fn.Supplier;

/**
 * Provide a queue adapted for a given capacity
 *
 * @param <T>
 */
public final class QueueSupplier<T> implements Supplier<Queue<T>> {

	private static final Supplier CLQ_SUPPLIER = new QueueSupplier<>(Long.MAX_VALUE);
	private static final Supplier ABQ_SUPPLIER = new QueueSupplier<>(1);

	private final long batchSize;

	/**
	 *
	 * @param batchSize
	 * @param <T>
	 * @return
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
		return RingBuffer.newSequencedQueue(RingBuffer.<T>createSingleProducer((int) batchSize));
	}
}
