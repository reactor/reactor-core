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
package reactor.core.subscription;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.support.ReactiveState;

/**
 * A {@link Subscription} with a typed stateful context.
 *
 * @author Stephane Maldini
 * @since 2.0.2
 */
public class SubscriptionWithContext<C> implements Subscription, ReactiveState.Upstream, ReactiveState.Trace {

	private volatile       int                                                terminated         = 0;
	protected static final AtomicIntegerFieldUpdater<SubscriptionWithContext> TERMINATED_UPDATER =
	  AtomicIntegerFieldUpdater
	  .newUpdater(SubscriptionWithContext.class, "terminated");


	protected final C            context;
	protected final Subscription subscription;

	/**
	 * Attach a given arbitrary context (stateful information) to a {@link Subscription}, all Subscription methods
	 * will delegate properly.
	 *
	 * @param subscription the delegate subscription to invoke on request/cancel
	 * @param context      the contextual state of any type to bind for later use
	 * @param <C>          Type of attached stateful context
	 * @return a new Subscription with context information
	 */
	public static <C> SubscriptionWithContext<C> create(Subscription subscription, C context) {
		return new SubscriptionWithContext<>(context, subscription);
	}

	protected SubscriptionWithContext(C context, Subscription subscription) {
		this.context = context;
		this.subscription = subscription;
	}


	@Override
	public Object upstream() {
		return subscription;
	}

	/**
	 * The stateful context C
	 *
	 * @return the bound context
	 */
	public C context() {
		return context;
	}

	/**
	 * Throw a CancelException
	 */
	public void abort(){
		throw CancelException.get();
	}

	@Override
	public void request(long n) {
		subscription.request(n);
	}

	@Override
	public void cancel() {
		if (TERMINATED_UPDATER.compareAndSet(this, 0, 1)) {
			subscription.cancel();
		}
	}

	public boolean isCancelled() {
		return terminated == 1;
	}

}
