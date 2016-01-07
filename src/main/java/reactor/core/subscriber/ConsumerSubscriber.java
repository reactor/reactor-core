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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public class ConsumerSubscriber<T> extends BaseSubscriber<T>
		implements ReactiveState.Upstream, ReactiveState.ActiveUpstream, ReactiveState.Bounded {

	private final Consumer<? super T>         consumer;
	private final Consumer<? super Throwable> errorConsumer;
	private final Runnable              completeConsumer;

	private Subscription subscription;

	/**
	 * Create a {@link Subscriber} to request  Long.MAX_VALUE onSubscribe.
	 */
	public ConsumerSubscriber(){
		this(null, null, null);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext, onError and onComplete. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 * <p>
	 * The argument {@code subscriptionHandler} is executed once by new subscriber to generate a context shared by
	 * every
	 * request calls.
	 *
	 * @param consumer     A {@link Consumer} with argument onNext data
	 * @param errorConsumer    A {@link Consumer} called onError
	 * @param completeConsumer A {@link Runnable} called onComplete with the actual context if any
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public ConsumerSubscriber(Consumer<? super T> consumer,
			Consumer<? super Throwable> errorConsumer,
			Runnable completeConsumer) {
		this.consumer = consumer;
		this.errorConsumer = errorConsumer;
		this.completeConsumer = completeConsumer;
	}

	/**
	 *
	 * @param s
	 */
	protected void doSubscribe(Subscription s) {
		s.request(Long.MAX_VALUE);
	}

	@Override
	public final void onSubscribe(Subscription s) {
		if (BackpressureUtils.validate(subscription, s)) {
			this.subscription = s;
			try {
				doSubscribe(s);
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
		Subscription s = subscription;
		if (s == null) {
			return;
		}
		subscription = null;
		try {
			doComplete();
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			doError(t);
		}
	}

	/**
	 *
	 */
	protected void doComplete() {
		if (completeConsumer != null) {
			completeConsumer.run();
		}
	}

	@Override
	public final void onError(Throwable t) {
		super.onError(t);
		doError(t);
		subscription = null;
	}

	/**
	 *
	 * @param t
	 */
	protected void doError(Throwable t) {
		if (errorConsumer != null) {
			errorConsumer.accept(t);
		}
		else {
			throw ReactorFatalException.create(t);
		}
	}

	@Override
	public final void onNext(T x) {
		super.onNext(x);

		try {
			doNext(x);
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			Subscription s = subscription;
			if (s != null) {
				subscription = null;
				s.cancel();
			}
			doError(Exceptions.addValueAsLastCause(t, x));
		}
	}

	/**
	 *
	 * @param x
	 */
	protected void doNext(T x) {
		if (consumer != null) {
			consumer.accept(x);
		}
	}

	/**
	 *
	 * @param n
	 */
	protected void requestMore(long n){
		Subscription s = subscription;
		if (s != null) {
			s.request(n);
		}
	}

	/**
	 *
	 */
	protected void cancel() {
		Subscription s = subscription;
		if (s != null) {
			subscription = null;
			s.cancel();
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
		return false;
	}
}
