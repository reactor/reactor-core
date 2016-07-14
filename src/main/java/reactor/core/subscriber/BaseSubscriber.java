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
import reactor.util.Exceptions;

/**
 * Convenience subscriber default interface that checks for input errors and provide a self-subscription operation.
 *
 * @author Stephane Maldini
 * @since 2.5
 * 
 * @param <T> the value type observed
 */
public interface BaseSubscriber<T> extends Subscriber<T> {

	/**
	 * Trigger onSubscribe with a stateless subscription to signal this subscriber it can start receiving
	 * onNext, onComplete and onError calls.
	 * <p>
	 * Doing so MAY allow direct UNBOUNDED onXXX calls and MAY prevent {@link org.reactivestreams.Publisher} to subscribe this
	 * subscriber.
	 *
	 * Note that {@link org.reactivestreams.Processor} can extend this behavior to effectively start its subscribers.
	 * 
	 * @return this
	 */
	default BaseSubscriber<T> connect() {
		onSubscribe(SubscriptionHelper.empty());
		return this;
	}

	/**
	 * Create a {@link SubmissionEmitter} and attach it via {@link #onSubscribe(Subscription)}.
	 *
	 * @return a new subscribed {@link SubmissionEmitter}
	 */
	default SubmissionEmitter<T> connectEmitter() {
		return connectEmitter(true);
	}

	/**
	 * Prepare a {@link SubmissionEmitter} and pass it to {@link #onSubscribe(Subscription)} if the autostart flag is
	 * set to true.
	 *
	 * @param autostart automatically start?
	 * @return a new {@link SubmissionEmitter}
	 */
	default SubmissionEmitter<T> connectEmitter(boolean autostart) {
		return SubmissionEmitter.create(this, autostart);
	}

	@Override
	default void onSubscribe(Subscription s) {
		SubscriptionHelper.validate(null, s);
		//To validate with SubscriptionHelper.validate(current, s)
	}

	@Override
	default void onNext(T t) {
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}

	}

	@Override
	default void onError(Throwable t) {
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}
		Exceptions.throwIfFatal(t);
	}

	@Override
	default void onComplete() {

	}
}
