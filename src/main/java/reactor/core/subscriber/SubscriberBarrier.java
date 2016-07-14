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
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.util.Exceptions;

/**
 * A {@link Subscriber} with an asymetric typed wrapped subscriber. Yet it represents a unique relationship between
 * a Publisher and a Subscriber, it doesn't implement
 * the {@link org.reactivestreams.Processor} interface allowing multiple subscribes.
 *
 * @author Stephane Maldini
 * @since 2.0.4
 * 
 * @param <I> the input value type
 * @param <O> the output value type
 */
public class SubscriberBarrier<I, O>
		implements BaseSubscriber<I>, Subscription, SubscriberState, Receiver, Producer {

	protected final Subscriber<? super O> subscriber;

	protected Subscription subscription;

	public SubscriberBarrier(Subscriber<? super O> subscriber) {
		this.subscriber = subscriber;
	}

	@Override
	public Subscription upstream() {
		return subscription;
	}

	@Override
	public boolean isStarted() {
		return subscription != null;
	}

	@Override
	public Subscriber<? super O> downstream() {
		return subscriber;
	}

	@Override
	public final void onSubscribe(Subscription s) {
		if (SubscriptionHelper.validate(subscription, s)) {
			try {
				subscription = s;
				doOnSubscribe(s);
			}
			catch (Throwable throwable) {
				s.cancel();
				Exceptions.throwIfFatal(throwable);
				doOnSubscriberError(Exceptions.unwrap(throwable));
			}
		}
	}

	protected void doOnSubscribe(Subscription subscription) {
		subscriber.onSubscribe(this);
	}

	@Override
	public final void onNext(I i) {
		BaseSubscriber.super.onNext(i);
		try {
			doNext(i);
		}
		catch (Exceptions.CancelException c) {
			throw c;
		}
		catch (Throwable throwable) {
			Subscription subscription = this.subscription;
			if(subscription != null){
				subscription.cancel();
			}
			Exceptions.throwIfFatal(throwable);
			doOnSubscriberError(Exceptions.unwrap(throwable));
		}
	}

	@SuppressWarnings("unchecked")
	protected void doNext(I i) {
		subscriber.onNext((O) i);
	}

	@Override
	public final void onError(Throwable t) {
		BaseSubscriber.super.onError(t);
		doError(t);
	}

	protected void doError(Throwable throwable) {
		subscriber.onError(throwable);
	}

	protected void doOnSubscriberError(Throwable throwable){
		subscriber.onError(throwable);
	}

	@Override
	public final void onComplete() {
		try {
			doComplete();
		} catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			doOnSubscriberError(Exceptions.unwrap(throwable));
		}
	}

	protected void doComplete() {
		subscriber.onComplete();
	}

	@Override
	public final void request(long n) {
		try {
			SubscriptionHelper.checkRequest(n);
			doRequest(n);
		} catch (Throwable throwable) {
			doCancel();
			Exceptions.throwIfFatal(throwable);
			doOnSubscriberError(Exceptions.unwrap(throwable));
		}
	}

	protected void doRequest(long n) {
		Subscription s = this.subscription;
		if (s != null) {
			s.request(n);
		}
	}

	@Override
	public final void cancel() {
		try {
			doCancel();
		} catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			doOnSubscriberError(Exceptions.unwrap(throwable));
		}
	}

	protected void doCancel() {
		Subscription s = this.subscription;
		if (s != null) {
			this.subscription = null;
			s.cancel();
		}
	}

	@Override
	public boolean isTerminated() {
		return null != subscription && subscription instanceof SubscriberState && (
				(SubscriberState) subscription).isTerminated();
	}

	@Override
	public long getCapacity() {
		return subscriber != null && SubscriberState.class.isAssignableFrom(subscriber
				.getClass()) ?
				((SubscriberState) subscriber).getCapacity() :
		  Long.MAX_VALUE;
	}

	@Override
	public long getPending() {
		return subscriber != null && SubscriberState.class.isAssignableFrom(subscriber
				.getClass()) ?
				((SubscriberState) subscriber).getPending() :
				-1L;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
