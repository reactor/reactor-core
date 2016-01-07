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
import reactor.core.subscription.EmptySubscription;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.BackpressureUtils;

/**
 * Convenience subscriber base class that checks for input errors and provide a self-subscription operation.
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public class BaseSubscriber<T> implements Subscriber<T> {

	/**
	 * Trigger onSubscribe with a stateless subscription to signal this subscriber it can start receiving
	 * onNext, onComplete and onError calls.
	 * <p>
	 * Doing so MAY allow direct UNBOUNDED onXXX calls and MAY prevent {@link org.reactivestreams.Publisher} to subscribe this
	 * subscriber.
	 *
	 * Note that {@link org.reactivestreams.Processor} can extend this behavior to effectively start its subscribers.
	 */
	public BaseSubscriber<T> start() {
		onSubscribe(EmptySubscription.INSTANCE);
		return this;
	}

	/**
	 *
	 * @return
	 */
	public ReactiveSession<T> startSession() {
		return bindSession(true);
	}

	/**
	 *
	 * @return
	 */
	public ReactiveSession<T> bindSession(boolean autostart) {
		return ReactiveSession.create(this, autostart);
	}

	@Override
	public void onSubscribe(Subscription s) {
		BackpressureUtils.validate(null, s);
		//To validate with BackpressureUtils.validate(current, s)
	}

	@Override
	public void onNext(T t) {
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

	@Override
	public void onComplete() {

	}
}
