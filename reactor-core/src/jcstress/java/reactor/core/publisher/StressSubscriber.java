/*
 * Copyright (c) 2020-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
package reactor.core.publisher;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

public class StressSubscriber<T> implements CoreSubscriber<T> {

	enum Operation {
		ON_NEXT,
		ON_ERROR,
		ON_COMPLETE,
		ON_SUBSCRIBE
	}

	final long initRequest;

	final Context context;

	volatile Subscription subscription;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<StressSubscriber, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(StressSubscriber.class, Subscription.class, "subscription");

	public Throwable error;

	public List<Throwable> droppedErrors = new CopyOnWriteArrayList<>();

	public AtomicReference<Operation> guard = new AtomicReference<>(null);

	public AtomicBoolean concurrentOnNext = new AtomicBoolean(false);

	public AtomicBoolean concurrentOnError = new AtomicBoolean(false);

	public AtomicBoolean concurrentOnComplete = new AtomicBoolean(false);

	public AtomicBoolean concurrentOnSubscribe = new AtomicBoolean(false);

	public final AtomicInteger onNextCalls = new AtomicInteger();

	public final AtomicInteger onNextDiscarded = new AtomicInteger();

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
		this.context = Operators.enableOnDiscard(Context.of(Hooks.KEY_ON_ERROR_DROPPED,
				(Consumer<Throwable>) throwable -> {
					droppedErrors.add(throwable);
				}),
				(__) -> onNextDiscarded.incrementAndGet());
	}

	@Override
	public Context currentContext() {
		return this.context;
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		if (!guard.compareAndSet(null, Operation.ON_SUBSCRIBE)) {
			concurrentOnSubscribe.set(true);
		} else {
			if (Operators.setOnce(S, this, subscription)) {
				if (initRequest > 0) {
					subscription.request(initRequest);
				}
			}
			guard.compareAndSet(Operation.ON_SUBSCRIBE, null);
		}
		onSubscribeCalls.incrementAndGet();
	}

	@Override
	public void onNext(T value) {
		if (!guard.compareAndSet(null, Operation.ON_NEXT)) {
			concurrentOnNext.set(true);
		} else {
			guard.compareAndSet(Operation.ON_NEXT, null);
		}
		onNextCalls.incrementAndGet();
	}

	@Override
	public void onError(Throwable throwable) {
		if (!guard.compareAndSet(null, Operation.ON_ERROR)) {
			concurrentOnError.set(true);
		} else {
			guard.compareAndSet(Operation.ON_ERROR, null);
		}
		error = throwable;
		onErrorCalls.incrementAndGet();
	}

	@Override
	public void onComplete() {
		if (!guard.compareAndSet(null, Operation.ON_COMPLETE)) {
			concurrentOnComplete.set(true);
		} else {
			guard.compareAndSet(Operation.ON_COMPLETE, null);
		}
		onCompleteCalls.incrementAndGet();
	}

	public void request(long n) {
		if (Operators.validate(n)) {
			Subscription s = this.subscription;
			if (s != null) {
				s.request(n);
			}
		}
	}

	public void cancel() {
		Operators.terminate(S, this);
	}
}
