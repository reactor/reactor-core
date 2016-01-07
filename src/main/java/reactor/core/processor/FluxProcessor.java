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

package reactor.core.processor;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.core.error.Exceptions;
import reactor.core.subscription.EmptySubscription;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;

/**
 * A base processor with an async boundary trait to manage active subscribers (Threads), upstream subscription and
 * shutdown options.
 * @author Stephane Maldini
 * @since 2.0.2, 2.5
 */
public abstract class FluxProcessor<IN, OUT> extends Flux<OUT>
		implements Processor<IN, OUT>, ReactiveState.Bounded, ReactiveState.Upstream {

	//protected static final int DEFAULT_BUFFER_SIZE = 1024;


	protected Subscription upstreamSubscription;

	protected FluxProcessor() {
	}

	/**
	 *
	 * @return
	 */
	public FluxProcessor<IN, OUT> start() {
		onSubscribe(EmptySubscription.INSTANCE);
		return this;
	}

	/**
	 *
	 * @return
	 */
	public ReactiveSession<IN> startSession() {
		return bindSession(true);
	}

	/**
	 *
	 * @return
	 */
	public ReactiveSession<IN> bindSession(boolean autostart) {
		return ReactiveSession.create(this, autostart);
	}


	@Override
	public void onSubscribe(final Subscription s) {
		if (BackpressureUtils.validate(upstreamSubscription, s)) {
			this.upstreamSubscription = s;
			try {
				doOnSubscribe(s);
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				s.cancel();
				onError(t);
			}
		}
	}

	@Override
	public void onNext(IN t) {
		if (t == null) {
			throw Exceptions.spec_2_13_exception();
		}

	}

	@Override
	public void onError(Throwable t) {
		if (t == null) {
			throw Exceptions.spec_2_13_exception();
		}
		Exceptions.throwIfFatal(t);
	}

	protected void doOnSubscribe(Subscription s) {
		//IGNORE
	}

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}

	@Override
	public void subscribe(Subscriber<? super OUT> s) {
		if (s == null) {
			throw Exceptions.spec_2_13_exception();
		}
	}

	protected void cancel(Subscription subscription) {
		if (subscription != EmptySubscription.INSTANCE) {
			subscription.cancel();
		}
	}

	@Override
	public Object upstream() {
		return upstreamSubscription;
	}

}
