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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;

/**
 * Arbitrates the subscriber that can be set once anytime.
 * <p>
 * Note: it is expected to be {@link #onSubscribe(Subscription)} before the subscriber, and none of the RS usual
 * verifications (request, null onNext are performed as it is simply passthrough.
 *
 * @param <I> the relayed input value type
 */
final class FluxPassthrough<I> extends Flux<I> implements Subscriber<I>, Producer, Receiver, Subscription {

	Subscriber<? super I> subscriber;
	Subscription          s;

	static final int READY            = 0;
	static final int HAS_SUBSCRIPTION = 1;
	static final int HAS_SUBSCRIBER   = 2;
	static final int SUBSCRIBED       = 3;

	volatile int state;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<FluxPassthrough> STATE =
			AtomicIntegerFieldUpdater.newUpdater(FluxPassthrough.class, "state");

	@Override
	public final Subscriber<? super I> downstream() {
		return subscriber;
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (BackpressureUtils.validate(this.s, s)) {
			this.s = s;
			int st;
			for (; ; ) {
				st = STATE.get(this);
				if (st == HAS_SUBSCRIBER) {
					Subscriber<? super I> sub = this.subscriber;
					if (sub != null && STATE.compareAndSet(this, st, SUBSCRIBED)) {
						sub.onSubscribe(s);
						break;
					}
				}
				else if(st == READY){
					if(STATE.compareAndSet(this, st, HAS_SUBSCRIPTION)) {
						break;
					}
				}
				else{
					s.cancel();
					break;
				}
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onNext(I t) {
		subscriber.onNext(t);
	}

	@Override
	public void onError(Throwable t) {
		subscriber.onError(t);
	}

	@Override
	public void onComplete() {
		subscriber.onComplete();
	}

	@Override
	public void subscribe(Subscriber<? super I> sub) {
		int st;
		for(;;){
			st = STATE.get(this);
			if(st == READY || st == HAS_SUBSCRIPTION){
				if(STATE.compareAndSet(this, st, st == READY ? HAS_SUBSCRIBER : SUBSCRIBED)) {
					this.subscriber = sub;
					if(st == HAS_SUBSCRIPTION){
						sub.onSubscribe(this);
					}
					break;
				}
			}
			else {
				EmptySubscription.error(sub,
						new IllegalStateException("DeferredSubscriber doesn't support multi/subscribe"));
				break;
			}
		}
	}

	@Override
	public Subscription upstream() {
		return s;
	}

	@Override
	public void request(long n) {
		this.s.request(n);
	}

	@Override
	public void cancel() {
		if(STATE.compareAndSet(this, SUBSCRIBED, HAS_SUBSCRIPTION) ||
				STATE.compareAndSet(this, HAS_SUBSCRIBER, READY)) {
			subscriber = null;
			this.s.cancel();
		}
	}
}
