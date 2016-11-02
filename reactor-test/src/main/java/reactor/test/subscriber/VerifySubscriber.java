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

import org.reactivestreams.Subscriber;

/**
 * Subscriber implementation that verifies pre-defined expectations as part of its
 * subscription. Typical usage consists of the following steps: <ul> <li>Create a {@code
 * VerifySubscriber} builder using {@link #create()} or {@link #create(long)}</li>
 * <li>Set individual up value expectations using {@link Step#expectNext}, {@link
 * Step#expectNextWith(Predicate)
 * expectNextWith(Predicate)}.</li> and/or <li>Set up subscription actions using either
 * {@link Step#thenRequest(long) thenRequest(long)} or {@link
 * Step#thenCancel() thenCancel()}. </li> <li>Build the {@code
 * VerifySubscriber} using {@link LastStep#expectComplete() expectComplete()},
 * {@link LastStep#expectError() expectError()}, {@link
 * LastStep#expectError(Class) expectError(Class)}, {@link
 * LastStep#expectErrorWith(Predicate) expectErrorWith(Predicate)}, or {@link
 * LastStep#thenCancel() thenCancel()}. </li> <li>Subscribe the built {@code
 * VerifySubscriber} to a {@code Publisher}.</li> <li>Verify the expectations using
 * either {@link #verify()} or {@link #verify(Duration)}.</li> <li>If any expectations
 * failed, an {@code AssertionError} will be thrown indicating the failures.</li> </ul>
 * <p>For example:
 * <pre>
 * VerifySubscriber&lt;String&gt; subscriber = VerifySubscriber.&lt;String&gt;create()
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
public interface VerifySubscriber<T> extends Verifier, Subscriber<T> {

	/**
	 * Prepare a new {@code FirstStep} to build a {@code
	 * VerifySubscriber} that requests an unbounded amount of
	 * values.
	 *
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for setting up value expectations
	 */
	static <T> FirstStep<T, VerifySubscriber<T>> create() {
		return create(Long.MAX_VALUE);
	}

	/**
	 * Prepare a new {@code FirstStep} to build a {@code
	 * VerifySubscriber} that requests a
	 * specified
	 * amount of
	 * values.
	 *
	 * @param n the amount of items to request
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for setting up value expectations
	 */
	static <T> FirstStep<T, VerifySubscriber<T>> create(long n) {
		DefaultVerifierStepBuilder.checkPositive(n);
		return new DefaultVerifierStepBuilder<>(n, null, null);
	}

}
