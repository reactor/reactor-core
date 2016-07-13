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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.flow.Receiver;

/**
 * Base class for Subscribers that will receive their Subscriptions at any time yet
 * they need to be cancelled or requested at any time.
 */
public class DeferredSubscription
		implements Subscription, Receiver, SubscriberState {

	volatile Subscription s;
	static final AtomicReferenceFieldUpdater<DeferredSubscription, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(DeferredSubscription.class, Subscription.class, "s");

	volatile long requested;
	static final AtomicLongFieldUpdater<DeferredSubscription> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(DeferredSubscription.class, "requested");

	protected final void setInitialRequest(long n) {
		REQUESTED.lazySet(this, n);
	}

	/**
	 * Atomically sets the single subscription and requests the missed amount from it.
	 *
	 * @param s
	 * @return false if this arbiter is cancelled or there was a subscription already set
	 */
	public final boolean set(Subscription s) {
		Objects.requireNonNull(s, "s");
		Subscription a = this.s;
		if (a == SubscriptionHelper.cancelled()) {
			s.cancel();
			return false;
		}
		if (a != null) {
			s.cancel();
			SubscriptionHelper.reportSubscriptionSet();
			return false;
		}

		if (S.compareAndSet(this, null, s)) {

			long r = REQUESTED.getAndSet(this, 0L);

			if (r != 0L) {
				s.request(r);
			}

			return true;
		}

		a = this.s;

		if (a != SubscriptionHelper.cancelled()) {
			s.cancel();
			return false;
		}

		SubscriptionHelper.reportSubscriptionSet();
		return false;
	}

	@Override
	public void request(long n) {
		Subscription a = s;
		if (a != null) {
			a.request(n);
		} else {
			SubscriptionHelper.addAndGet(REQUESTED, this, n);

			a = s;

			if (a != null) {
				long r = REQUESTED.getAndSet(this, 0L);

				if (r != 0L) {
					a.request(r);
				}
			}
		}
	}

	@Override
	public void cancel() {
		Subscription a = s;
		if (a != SubscriptionHelper.cancelled()) {
			a = S.getAndSet(this, SubscriptionHelper.cancelled());
			if (a != null && a != SubscriptionHelper.cancelled()) {
				a.cancel();
			}
		}
	}
	/**
	 * Sets the Subscription once but does not request anything.
	 * @param s the Subscription to set
	 * @return true if successful, false if the current subscription is not null
	 */
	protected final boolean setWithoutRequesting(Subscription s) {
		Objects.requireNonNull(s, "s");
		for (;;) {
			Subscription a = this.s;
			if (a == SubscriptionHelper.cancelled()) {
				s.cancel();
				return false;
			}
			if (a != null) {
				s.cancel();
				SubscriptionHelper.reportSubscriptionSet();
				return false;
			}

			if (S.compareAndSet(this, null, s)) {
				return true;
			}
		}
	}

	/**
	 * Requests the deferred amount if not zero.
	 */
	protected final void requestDeferred() {
		long r = REQUESTED.getAndSet(this, 0L);

		if (r != 0L) {
			s.request(r);
		}
	}

	/**
	 * Returns true if this arbiter has been cancelled.
	 *
	 * @return true if this arbiter has been cancelled
	 */
	@Override
	public final boolean isCancelled() {
		return s == SubscriptionHelper.cancelled();
	}

	@Override
	public final boolean isStarted() {
		return s != null;
	}

	@Override
	public final boolean isTerminated() {
		return isCancelled();
	}

	@Override
	public long requestedFromDownstream() {
		return requested;
	}

	@Override
	public Subscription upstream() {
		return s;
	}

}