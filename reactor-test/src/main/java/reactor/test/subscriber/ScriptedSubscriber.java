/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test.subscriber;

import java.time.Duration;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Subscriber implementation that verifies pre-defined expectations as part of its
 * subscription. Typical usage consists of the following steps: <ul> <li>Create a {@code
 * ScriptedSubscriber} builder using {@link #create()} or {@link #create(long)}</li>
 * <li>Set individual up value expectations using {@link StepBuilder#expectNext(Object[])}
 * expectNext(Object)}, {@link StepBuilder#expectNext(Object[])
 * expectNext(Object[])}, {@link StepBuilder#expectNextWith(Predicate)
 * expectNextWith(Predicate)}.</li> and/or <li>Set up subscription actions using either
 * {@link StepBuilder#thenRequest(long) thenRequest(long)} or {@link
 * StepBuilder#thenCancel() thenCancel()}. </li> <li>Build the {@code
 * ScriptedSubscriber} using {@link LastStepBuilder#expectComplete() expectComplete()},
 * {@link LastStepBuilder#expectError() expectError()}, {@link
 * LastStepBuilder#expectError(Class) expectError(Class)}, {@link
 * LastStepBuilder#expectErrorWith(Predicate) expectErrorWith(Predicate)}, or {@link
 * LastStepBuilder#thenCancel() thenCancel()}. </li> <li>Subscribe the built {@code
 * ScriptedSubscriber} to a {@code Publisher}.</li> <li>Verify the expectations using
 * either {@link #verify()} or {@link #verify(Duration)}.</li> <li>If any expectations
 * failed, an {@code AssertionError} will be thrown indicating the failures.</li> </ul>
 * <p>
 * <p>For example:
 * <pre>
 * ScriptedSubscriber&lt;String&gt; subscriber = ScriptedSubscriber.&lt;String&gt;create()
 *   .expectNext("foo")
 *   .expectNext("bar")
 *   .expectComplete();
 *
 * Publisher&lt;String&gt; publisher = Flux.just("foo", "bar");
 * publisher.subscribe(subscriber);
 *
 * subscriber.verify();
 * </pre>
 *
 * @author Arjen Poutsma
 * @author Stephane Maldini
 */
public interface ScriptedSubscriber<T> extends ScriptedVerification, Subscriber<T> {

	/**
	 * Make the specified publisher subscribe to  a new instance of this {@link ScriptedVerification} and then verify the
	 * signals received by the new instance. This method will <strong>block</strong>
	 * indefinitely until the stream has been terminated (either through {@link
	 * #onComplete()}, {@link #onError(Throwable)} or {@link Subscription#cancel()}).
	 *
	 * @param publisher the publisher to subscribe to
	 *
	 * @return the {@link Duration} of the verification
	 *
	 * @throws AssertionError in case of expectation failures
	 */
	Duration verify(Publisher<? extends T> publisher) throws AssertionError;

	/**
	 * Make the specified publisher subscribe to a new instance of this {@link ScriptedVerification} and
	 * then verify the
	 * signals received by the new instance. This method will <strong>block</strong> for
	 * the given duration or until the stream has been terminated (either through {@link
	 * #onComplete()}, {@link #onError(Throwable)} or {@link Subscription#cancel()}).
	 *
	 * @param publisher the publisher to subscribe to
	 *
	 * @return the {@link Duration} of the verification
	 *
	 * @throws AssertionError in case of expectation failures, or when the verification
	 *                        times out
	 */
	Duration verify(Publisher<? extends T> publisher, Duration duration)
			throws AssertionError;

	/**
	 * Prepare a new {@code FirstStepBuilder} to build a {@code
	 * ScriptedSubscriber} that requests an unbounded amount of
	 * values.
	 *
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for setting up value expectations
	 */
	static <T> FirstStepBuilder<T, ScriptedSubscriber<T>> create() {
		return create(Long.MAX_VALUE);
	}

	/**
	 * Prepare a new {@code FirstStepBuilder} to build a {@code
	 * ScriptedSubscriber} that requests a
	 * specified
	 * amount of
	 * values.
	 *
	 * @param n the amount of items to request
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for setting up value expectations
	 */
	static <T> FirstStepBuilder<T, ScriptedSubscriber<T>> create(long n) {
		DefaultScriptedSubscriberBuilder.checkPositive(n);
		return new DefaultScriptedSubscriberBuilder<>(n, null, null);
	}

}
