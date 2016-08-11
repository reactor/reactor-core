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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.Exceptions;

/**
 * A {@link Subscriber} with an asymetric typed wrapped subscriber. Yet it represents a unique relationship between
 * a Publisher and a Subscriber, it doesn't implement
 * the {@link org.reactivestreams.Processor} interface allowing multiple subscribes.
 *
 * @author Stephane Maldini
 *
 * @param <I> the input value type
 * @param <O> the output value type
 */
public class OperatorAdapter<I, O>
		implements Subscriber<I>, Subscription, Trackable, Receiver, Producer {

	protected final Subscriber<? super O> subscriber;

	protected Subscription subscription;

	public OperatorAdapter(Subscriber<? super O> subscriber) {
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
		if (Operators.validate(subscription, s)) {
			try {
				subscription = s;
				doOnSubscribe(s);
			}
			catch (Throwable throwable) {
				doOnSubscriberError(Operators.onOperatorError(s, throwable));
			}
		}
	}

	/**
	 *
	 * @param subscription
	 */
	protected void doOnSubscribe(Subscription subscription) {
		subscriber.onSubscribe(this);
	}

	@Override
	public final void onNext(I i) {
		if (i == null) {
			throw Exceptions.argumentIsNullException();
		}
		try {
			doNext(i);
		}
		catch (Throwable throwable) {
			doOnSubscriberError(Operators.onOperatorError(subscription, throwable, i));
		}
	}

	@SuppressWarnings("unchecked")
	protected void doNext(I i) {
		subscriber.onNext((O) i);
	}

	@Override
	public final void onError(Throwable t) {
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}
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
			doOnSubscriberError(Operators.onOperatorError(throwable));
		}
	}

	protected void doComplete() {
		subscriber.onComplete();
	}

	@Override
	public final void request(long n) {
		try {
			Operators.checkRequest(n);
			doRequest(n);
		} catch (Throwable throwable) {
			doCancel();
			doOnSubscriberError(Operators.onOperatorError(throwable));
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
			doOnSubscriberError(Operators.onOperatorError(subscription, throwable));
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
		return null != subscription && subscription instanceof Trackable && (
				(Trackable) subscription).isTerminated();
	}

	@Override
	public long getCapacity() {
		return subscriber != null && Trackable.class.isAssignableFrom(subscriber
				.getClass()) ?
				((Trackable) subscriber).getCapacity() :
		  Long.MAX_VALUE;
	}

	@Override
	public long getPending() {
		return subscriber != null && Trackable.class.isAssignableFrom(subscriber
				.getClass()) ?
				((Trackable) subscriber).getPending() :
				-1L;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
