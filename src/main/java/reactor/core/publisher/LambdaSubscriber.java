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

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.Exceptions;
import reactor.core.Receiver;
import reactor.core.Trackable;

/**
 * An unbounded Java Lambda adapter to {@link Subscriber}
 *
 * @param <T> the value type
 */
final class LambdaSubscriber<T>
		implements Subscriber<T>, Receiver, Cancellation, Trackable {

	final Consumer<? super T>         consumer;
	final Consumer<? super Throwable> errorConsumer;
	final Runnable                    completeConsumer;

	volatile Subscription subscription;
	static final AtomicReferenceFieldUpdater<LambdaSubscriber, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(LambdaSubscriber.class,
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
	 */
	public LambdaSubscriber(Consumer<? super T> consumer,
			Consumer<? super Throwable> errorConsumer,
			Runnable completeConsumer) {
		this.consumer = consumer;
		this.errorConsumer = errorConsumer;
		this.completeConsumer = completeConsumer;
	}

	@Override
	public final void onSubscribe(Subscription s) {
		if (Operators.validate(subscription, s)) {
			this.subscription = s;
			try {
				s.request(Long.MAX_VALUE);
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				onError(t);
			}
		}
	}

	@Override
	public Object upstream() {
		return subscription;
	}

	@Override
	public final void onComplete() {
		Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
		if ( s == Operators.cancelledSubscription()) {
			return;
		}
		if (completeConsumer != null) {
			try {
				completeConsumer.run();
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				onError(t);
			}
		}
	}

	@Override
	public final void onError(Throwable t) {
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}
		Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
		if (s == Operators.cancelledSubscription()) {
			Operators.onErrorDropped(t);
			return;
		}
		if (errorConsumer != null) {
			errorConsumer.accept(t);
		}
		else {
			throw Exceptions.errorCallbackNotImplemented(t);
		}
	}

	@Override
	public final void onNext(T x) {
		if (x == null) {
			throw Exceptions.argumentIsNullException();
		}
		try {
			if (consumer != null) {
				consumer.accept(x);
			}
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			onError(t);
		}
	}

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}

	@Override
	public boolean isStarted() {
		return subscription != null;
	}

	@Override
	public boolean isTerminated() {
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
