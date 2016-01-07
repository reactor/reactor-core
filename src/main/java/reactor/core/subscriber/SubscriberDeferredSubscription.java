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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscription.DeferredSubscription;
import reactor.core.support.ReactiveState;

/**
 * Arbitrates the requests and cancellation for a Subscription that may be set onSubscribe once only.
 * <p>
 * Note that {@link #request(long)} doesn't validate the amount.
 *
 * @param <I> the input value type
 * @param <O> the output value type
 */
public class SubscriberDeferredSubscription<I, O> extends DeferredSubscription
		implements Subscription, Subscriber<I>, ReactiveState.Downstream {

	protected final Subscriber<? super O> subscriber;

	/**
	 * Constructs a SingleSubscriptionArbiter with zero initial request.
	 *
	 * @param subscriber the actual subscriber
	 */
	public SubscriberDeferredSubscription(Subscriber<? super O> subscriber) {
		this.subscriber = Objects.requireNonNull(subscriber, "subscriber");
	}

	/**
	 * Constructs a SingleSubscriptionArbiter with the specified initial request amount.
	 *
	 * @param subscriber the actual subscriber
	 * @param initialRequest
	 *
	 * @throws IllegalArgumentException if initialRequest is negative
	 */
	public SubscriberDeferredSubscription(Subscriber<? super O> subscriber, long initialRequest) {
		if (initialRequest < 0) {
			throw new IllegalArgumentException("initialRequest >= required but it was " + initialRequest);
		}
		this.subscriber = Objects.requireNonNull(subscriber, "subscriber");
		setInitialRequest(initialRequest);
	}

	@Override
	public final Subscriber<? super O> downstream() {
		return subscriber;
	}

	@Override
	public void onSubscribe(Subscription s) {
		set(s);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onNext(I t) {
		subscriber.onNext((O) t);
	}

	@Override
	public void onError(Throwable t) {
		subscriber.onError(t);
	}

	@Override
	public void onComplete() {
		subscriber.onComplete();
	}
}
