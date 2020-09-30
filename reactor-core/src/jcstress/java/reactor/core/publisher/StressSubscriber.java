/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscription;

public class StressSubscriber<T> extends BaseSubscriber<T> {

	enum Operation {
		ON_NEXT,
		ON_ERROR,
		ON_COMPLETE,
		ON_SUBSCRIBE
	}

	final long initRequest;

	public AtomicReference<Operation> guard = new AtomicReference<>(null);

	public AtomicBoolean concurrentOnNext = new AtomicBoolean(false);

	public AtomicBoolean concurrentOnError = new AtomicBoolean(false);

	public AtomicBoolean concurrentOnComplete = new AtomicBoolean(false);

	public AtomicBoolean concurrentOnSubscribe = new AtomicBoolean(false);

	public final AtomicInteger onNextCalls = new AtomicInteger();

	public final AtomicInteger onErrorCalls = new AtomicInteger();

	public final AtomicInteger onCompleteCalls = new AtomicInteger();

	public final AtomicInteger onSubscribeCalls = new AtomicInteger();

	/**
	 * Build a {@link StressSubscriber} that makes an unbounded request upon subscription.
	 */
	public StressSubscriber() {
		this(Long.MAX_VALUE);
	}

	/**
	 * Build a {@link StressSubscriber} that requests the provided amount in
	 * {@link #onSubscribe(Subscription)}. Use {@code 0} to avoid any initial request
	 * upon subscription.
	 *
	 * @param initRequest the requested amount upon subscription, or zero to disable initial request
	 */
	public StressSubscriber(long initRequest) {
		this.initRequest = initRequest;
	}

	@Override
	protected void hookOnSubscribe(Subscription subscription) {
		if (!guard.compareAndSet(null, Operation.ON_SUBSCRIBE)) {
			concurrentOnSubscribe.set(true);
		} else {
			if (initRequest > 0) {
				subscription.request(initRequest);
			}
			guard.compareAndSet(Operation.ON_SUBSCRIBE, null);
		}
		onSubscribeCalls.incrementAndGet();
	}

	@Override
	protected void hookOnNext(T value) {
		if (!guard.compareAndSet(null, Operation.ON_NEXT)) {
			concurrentOnNext.set(true);
		} else {
			guard.compareAndSet(Operation.ON_NEXT, null);
		}
		onNextCalls.incrementAndGet();
	}

	@Override
	protected void hookOnError(Throwable throwable) {
		if (!guard.compareAndSet(null, Operation.ON_ERROR)) {
			concurrentOnError.set(true);
		} else {
			guard.compareAndSet(Operation.ON_ERROR, null);
		}
		onErrorCalls.incrementAndGet();
	}

	@Override
	protected void hookOnComplete() {
		if (!guard.compareAndSet(null, Operation.ON_COMPLETE)) {
			concurrentOnComplete.set(true);
		} else {
			guard.compareAndSet(Operation.ON_COMPLETE, null);
		}
		onCompleteCalls.incrementAndGet();
	}
}
