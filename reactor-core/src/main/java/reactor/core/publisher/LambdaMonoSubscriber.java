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

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static reactor.core.Scannable.Attr.RUN_STYLE;
import static reactor.core.Scannable.Attr.RunStyle.SYNC;

/**
 * An unbounded Java Lambda adapter to {@link Subscriber}, targeted at {@link Mono}.
 *
 * @param <T> the value type
 */
final class LambdaMonoSubscriber<T> implements InnerConsumer<T>, Disposable {

	final Consumer<? super T>            consumer;
	final Consumer<? super Throwable>    errorConsumer;
	final Runnable                       completeConsumer;
	final Consumer<? super Subscription> subscriptionConsumer;
	final Context                        initialContext;

	volatile Subscription subscription;
	static final AtomicReferenceFieldUpdater<LambdaMonoSubscriber, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(LambdaMonoSubscriber.class,
					Subscription.class,
					"subscription");

	/**
	 * Create a {@link Subscriber} reacting onNext, onError and onComplete. The subscriber
	 * will automatically request Long.MAX_VALUE onSubscribe.
	 * <p>
	 * The argument {@code subscriptionHandler} is executed once by new subscriber to
	 * generate a context shared by every request calls.
	 *
	 * @param consumer A {@link Consumer} with argument onNext data
	 * @param errorConsumer A {@link Consumer} called onError
	 * @param completeConsumer A {@link Runnable} called onComplete with the actual
	 * context if any
	 * @param subscriptionConsumer A {@link Consumer} called with the {@link Subscription}
	 * to perform initial request, or null to request max
	 * @param initialContext A {@link Context} for this subscriber, or null to use the default
	 * of an {@link Context#empty() empty Context}.
	 */
	LambdaMonoSubscriber(@Nullable Consumer<? super T> consumer,
			@Nullable Consumer<? super Throwable> errorConsumer,
			@Nullable Runnable completeConsumer,
			@Nullable Consumer<? super Subscription> subscriptionConsumer,
			@Nullable Context initialContext) {
		this.consumer = consumer;
		this.errorConsumer = errorConsumer;
		this.completeConsumer = completeConsumer;
		this.subscriptionConsumer = subscriptionConsumer;
		this.initialContext = initialContext == null ? Context.empty() : initialContext;
	}

	/**
	 * Create a {@link Subscriber} reacting onNext, onError and onComplete. The subscriber
	 * will automatically request Long.MAX_VALUE onSubscribe.
	 * <p>
	 * The argument {@code subscriptionHandler} is executed once by new subscriber to
	 * generate a context shared by every request calls.
	 *
	 * @param consumer A {@link Consumer} with argument onNext data
	 * @param errorConsumer A {@link Consumer} called onError
	 * @param completeConsumer A {@link Runnable} called onComplete with the actual
	 * context if any
	 * @param subscriptionConsumer A {@link Consumer} called with the {@link Subscription}
	 * to perform initial request, or null to request max
	 */ //left mainly for the benefit of tests
	LambdaMonoSubscriber(@Nullable Consumer<? super T> consumer,
			@Nullable Consumer<? super Throwable> errorConsumer,
			@Nullable Runnable completeConsumer,
			@Nullable Consumer<? super Subscription> subscriptionConsumer) {
		this(consumer, errorConsumer, completeConsumer, subscriptionConsumer, null);
	}

	@Override
	public Context currentContext() {
		return this.initialContext;
	}

	@Override
	public final void onSubscribe(Subscription s) {
		if (Operators.validate(subscription, s)) {
			this.subscription = s;

			if (subscriptionConsumer != null) {
				try {
					subscriptionConsumer.accept(s);
				}
				catch (Throwable t) {
					Exceptions.throwIfFatal(t);
					s.cancel();
					onError(t);
				}
			}
			else {
				s.request(Long.MAX_VALUE);
			}

		}
	}

	@Override
	public final void onComplete() {
		Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
		if (s == Operators.cancelledSubscription()) {
			return;
		}
		if (completeConsumer != null) {
			try {
				completeConsumer.run();
			}
			catch (Throwable t) {
				Operators.onErrorDropped(t, this.initialContext);
			}
		}
	}

	@Override
	public final void onError(Throwable t) {
		Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
		if (s == Operators.cancelledSubscription()) {
			Operators.onErrorDropped(t, this.initialContext);
			return;
		}
		doError(t);
	}

	void doError(Throwable t) {
		if (errorConsumer != null) {
			errorConsumer.accept(t);
		}
		else {
			Operators.onErrorDropped(Exceptions.errorCallbackNotImplemented(t), this.initialContext);
		}
	}

	@Override
	public final void onNext(T x) {
		Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
		if (s == Operators.cancelledSubscription()) {
			Operators.onNextDropped(x, this.initialContext);
			return;
		}
		if (consumer != null) {
			try {
				consumer.accept(x);
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				s.cancel();
				doError(t);
			}
		}
		if (completeConsumer != null) {
			try {
				completeConsumer.run();
			}
			catch (Throwable t) {
				Operators.onErrorDropped(t, this.initialContext);
			}
		}
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) {
			return subscription;
		}
		if (key == Attr.PREFETCH) {
			return Integer.MAX_VALUE;
		}
		if (key == Attr.TERMINATED || key == Attr.CANCELLED) {
			return isDisposed();
		}
		if (key == RUN_STYLE) {
		    return SYNC;
		}

		return null;
	}

	@Override
	public boolean isDisposed() {
		return subscription == Operators.cancelledSubscription();
	}

	@Override
	public void dispose() {
		Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
		if (s != null && s != Operators.cancelledSubscription()) {
			s.cancel();
		}
	}
}
