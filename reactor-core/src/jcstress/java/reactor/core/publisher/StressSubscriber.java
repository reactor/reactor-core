/*
 * Copyright (c) 2021-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.ArrayList;
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

	public AtomicReference<Throwable> guard = new AtomicReference<>(null);

	public AtomicBoolean concurrentOnNext = new AtomicBoolean(false);
	public Throwable stacktraceOnNext;

	public AtomicBoolean concurrentOnError = new AtomicBoolean(false);
	public Throwable stacktraceOnError;

	public AtomicBoolean concurrentOnComplete = new AtomicBoolean(false);
	public Throwable stacktraceOnComplete;

	public AtomicBoolean concurrentOnSubscribe = new AtomicBoolean(false);
	public Throwable stacktraceOnSubscribe;

	public final AtomicInteger onNextCalls = new AtomicInteger();

	public final List<T> receivedValues = new ArrayList<>();

	public final AtomicInteger onNextDiscarded = new AtomicInteger();

	public final List<Object> discardedValues = new CopyOnWriteArrayList<>();

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
				(value) -> {
					onNextDiscarded.incrementAndGet();
					discardedValues.add(value);
				});
	}

	@Override
	public Context currentContext() {
		return this.context;
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		final RuntimeException exception = new RuntimeException("onSubscribe");
		final Throwable previousException = guard.getAndSet(exception);
		if (previousException != null) {
			exception.addSuppressed(previousException);
			stacktraceOnSubscribe = exception;
			concurrentOnSubscribe.set(true);
		} else {
			final boolean wasSet = Operators.setOnce(S, this, subscription);
			guard.compareAndSet(exception, null);

			if (wasSet && initRequest > 0) {
				subscription.request(initRequest);
			}
		}
		onSubscribeCalls.incrementAndGet();
	}

	@Override
	public void onNext(T value) {
		final RuntimeException exception = new RuntimeException("onNext" + value);
		final Throwable previousException = guard.getAndSet(exception);
		if (previousException != null) {
			exception.addSuppressed(previousException);
			stacktraceOnNext = exception;
			concurrentOnNext.set(true);
		} else {
			guard.compareAndSet(exception, null);
		}
		receivedValues.add(value);
		onNextCalls.incrementAndGet();
	}

	@Override
	public void onError(Throwable throwable) {
		final RuntimeException exception = new RuntimeException("onError");
		final Throwable previousException = guard.getAndSet(exception);
		if (previousException != null) {
			exception.addSuppressed(previousException);
			stacktraceOnError = exception;
			concurrentOnError.set(true);
		} else {
			guard.compareAndSet(exception, null);
		}
		error = throwable;
		onErrorCalls.incrementAndGet();
	}

	Throwable oce;
	@Override
	public void onComplete() {
		final RuntimeException exception = new RuntimeException("onComplete");
		final Throwable previousException = guard.getAndSet(exception);
		if (previousException != null) {
			exception.addSuppressed(previousException);
			stacktraceOnComplete = exception;
			concurrentOnComplete.set(true);
		} else {
			guard.compareAndSet(exception, null);
		}

		final int calls = onCompleteCalls.incrementAndGet();

		if (calls > 1) {
			throw new RuntimeException("boom", oce);
		}
		oce = exception;
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
