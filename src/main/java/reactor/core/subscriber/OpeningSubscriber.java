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
package reactor.core.subscriber;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.reactivestreams.Subscription;
import reactor.core.flow.Receiver;
import reactor.core.util.Exceptions;

/**
 * A Subscriber with a safe Subscription callback
 * @author Stephane Maldini
 */
final class OpeningSubscriber<T> implements BaseSubscriber<T>,
                                            SubscriberState, Receiver {

	final Consumer<? super Subscription>              subscriptionHandler;
	final BiConsumer<? super T, ? super Subscription> dataConsumer;
	final Consumer<? super Throwable>                 errorConsumer;
	final Runnable                                    completeConsumer;

	Subscription subscription;

	OpeningSubscriber(BiConsumer<? super T, ? super Subscription> dataConsumer,
			Consumer<? super Subscription> subscriptionHandler,
			Consumer<? super Throwable> errorConsumer,
			Runnable completeConsumer) {

		this.subscriptionHandler = Objects.requireNonNull(subscriptionHandler, "A subscription handler must be provided");
		this.dataConsumer = dataConsumer;
		this.errorConsumer = errorConsumer;
		this.completeConsumer = completeConsumer;
	}

	@Override
	public Object upstream() {
		return subscription;
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (SubscriptionHelper.validate(subscription, s)) {
			try {
				this.subscription = s;
				final RefSubscription proxyRequest = new RefSubscription();
				subscriptionHandler.accept(proxyRequest);

				if (proxyRequest.compareAndSet(Long.MIN_VALUE, 0)) {
					subscription.cancel();
				}
				else if (proxyRequest.get() > 0) {
					subscription.request(proxyRequest.get());
				}
			}
			catch (Throwable throwable) {
				Exceptions.throwIfFatal(throwable);
				onError(throwable);
			}
		}

	}

	@Override
	public void onNext(T t) {
		BaseSubscriber.super.onNext(t);

		if (dataConsumer != null) {
			try {
				dataConsumer.accept(t, subscription);
			}
			catch (Exceptions.CancelException ce) {
				throw ce;
			}
			catch (Throwable error) {
				onError(error);
			}
		}
	}

	@Override
	public void onError(Throwable t) {
		BaseSubscriber.super.onError(t);

		if (errorConsumer != null) {
			errorConsumer.accept(t);
		}
		else {
			Exceptions.onErrorDropped(t);
		}
	}

	@Override
	public void onComplete() {
		if (completeConsumer != null) {
			try {
				completeConsumer.run();
			}
			catch (Throwable t) {
				onError(t);
			}
		}
	}

	@Override
	public long getPending() {
		return -1L;
	}

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}

	final class RefSubscription extends AtomicLong implements Subscription {

		/** */
        private static final long serialVersionUID = -5436105907521342076L;

        @Override
		public void request(long n) {
			if (subscription == null && get() != Long.MIN_VALUE) {
				SubscriptionHelper.addAndGet(this, n);
			}
			else if (subscription != null) {
				subscription.request(n);
			}
		}

		@Override
		public void cancel() {
			if (subscription == null) {
				set(Long.MIN_VALUE);
			}
			else {
				subscription.cancel();
			}
		}
	}
}
