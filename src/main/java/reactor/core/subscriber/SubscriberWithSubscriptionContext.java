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

import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.subscription.SubscriptionWithContext;
import reactor.core.support.Assert;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;

/**
 * @author Stephane Maldini
 */
public final class SubscriberWithSubscriptionContext<T, C> extends BaseSubscriber<T>
		implements ReactiveState.Bounded, ReactiveState.Upstream {

	protected final Function<Subscription, C>                 subscriptionHandler;
	protected final BiConsumer<T, SubscriptionWithContext<C>> dataConsumer;
	protected final BiConsumer<Throwable, C>                  errorConsumer;
	protected final Consumer<C>                               completeConsumer;

	private SubscriptionWithContext<C> subscriptionWithContext;

	/**
	 *
	 * @param dataConsumer
	 * @param subscriptionHandler
	 * @param errorConsumer
	 * @param completeConsumer
	 */
	public SubscriberWithSubscriptionContext(BiConsumer<T, SubscriptionWithContext<C>> dataConsumer,
			Function<Subscription, C> subscriptionHandler,
			BiConsumer<Throwable, C> errorConsumer,
			Consumer<C> completeConsumer) {
		Assert.notNull(subscriptionHandler, "A subscription handler must be provided");
		this.dataConsumer = dataConsumer;
		this.subscriptionHandler = subscriptionHandler;
		this.errorConsumer = errorConsumer;
		this.completeConsumer = completeConsumer;
	}

	@Override
	public Object upstream() {
		return subscriptionWithContext;
	}

	@Override
	public void onSubscribe(Subscription s) {
		try {
			if (BackpressureUtils.validate(subscriptionWithContext, s)) {
				final AtomicLong proxyRequest = new AtomicLong();
				final C context = subscriptionHandler.apply(new Subscription() {
					@Override
					public void request(long n) {
						if (subscriptionWithContext == null && proxyRequest.get() != Long.MIN_VALUE) {
							BackpressureUtils.addAndGet(proxyRequest, n);
						}
						else if (subscriptionWithContext != null) {
							subscriptionWithContext.request(n);
						}
					}

					@Override
					public void cancel() {
						if (subscriptionWithContext == null) {
							proxyRequest.set(Long.MIN_VALUE);
						}
						else {
							subscriptionWithContext.cancel();
						}
					}
				});

				this.subscriptionWithContext = SubscriptionWithContext.create(s, context);
				if (proxyRequest.compareAndSet(Long.MIN_VALUE, 0)) {
					subscriptionWithContext.cancel();
				}
				else if (proxyRequest.get() > 0) {
					subscriptionWithContext.request(proxyRequest.get());
				}
			}
		}
		catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			onError(throwable);
		}

	}

	@Override
	public void onNext(T t) {
		super.onNext(t);

		if (dataConsumer != null) {
			try {
				dataConsumer.accept(t, subscriptionWithContext);
			}
			catch (CancelException ce) {
				throw ce;
			}
			catch (Throwable error) {
				onError(Exceptions.addValueAsLastCause(error, t));
			}
		}
	}

	@Override
	public void onError(Throwable t) {
		super.onError(t);

		if (errorConsumer != null) {
			errorConsumer.accept(t, subscriptionWithContext != null ? subscriptionWithContext.context() : null);
		}
		else {
			throw ReactorFatalException.create(t);
		}
	}

	@Override
	public void onComplete() {
		if (completeConsumer != null) {
			try {
				completeConsumer.accept(subscriptionWithContext != null ? subscriptionWithContext.context() : null);
			}
			catch (Throwable t) {
				onError(t);
			}
		}
	}

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}
}
