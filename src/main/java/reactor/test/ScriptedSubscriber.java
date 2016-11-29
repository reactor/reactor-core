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

package reactor.test;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Subscriber implementation that verifies pre-defined expectations as part of its subscription.
 * Typical usage consists of the following steps:
 * <ul>
 * <li>Create a {@code ScriptedSubscriber} builder using {@link #create()} or
 * {@link #create(long)},</li>
 * <li>Set individual up value expectations using
 * {@link ValueBuilder#expectValue(Object) expectValue(Object)},
 * {@link ValueBuilder#expectValues(Object[]) expectValues(Object[])},
 * {@link ValueBuilder#expectValueWith(Predicate) expectValueWith(Predicate)}.</li>
 * and/or
 * <li>Set up subscription actions using either
 * {@link ValueBuilder#doRequest(long) doRequest(long)} or
 * {@link ValueBuilder#doCancel() doCancel()}.
 * </li>
 * <li>Build the {@code ScriptedSubscriber} using
 * {@link TerminationBuilder#expectComplete() expectComplete()},
 * {@link TerminationBuilder#expectError() expectError()},
 * {@link TerminationBuilder#expectError(Class) expectError(Class)},
 * {@link TerminationBuilder#expectErrorWith(Predicate) expectErrorWith(Predicate)}, or
 * {@link TerminationBuilder#doCancel() doCancel()}.
 * </li>
 * <li>Subscribe the built {@code ScriptedSubscriber} to a {@code Publisher}.</li>
 * <li>Verify the expectations using either {@link #verify()} or {@link #verify(Duration)}.</li>
 * <li>If any expectations failed, an {@code AssertionError} will be thrown indicating the
 * failures.</li>
 * </ul>
 * As an alternative to setting up individual expectation as described above, you can expect a
 * certain value count using {@link #expectValueCount(long)}.
 *
 * <p>For example:
 * <pre>
 * ScriptedSubscriber&lt;String&gt; subscriber = ScriptedSubscriber.&lt;String&gt;create()
 *   .expectValue("foo")
 *   .expectValue("bar")
 *   .expectComplete();
 *
 * Publisher&lt;String&gt; publisher = Flux.just("foo", "bar");
 * publisher.subscribe(subscriber);
 *
 * subscriber.verify();
 * </pre>
 *
 * @author Arjen Poutsma
 * @since 1.0
 */
public interface ScriptedSubscriber<T> extends Subscriber<T> {

	/**
	 * Verify the signals received by this subscriber. This method will <strong>block</strong>
	 * indefinitely until the stream has been terminated (either through {@link #onComplete()},
	 * {@link #onError(Throwable)} or {@link Subscription#cancel()}).
	 * @throws AssertionError in case of expectation failures
	 * @throws InterruptedException if the current thread is interrupted while waiting
	 */
	void verify() throws AssertionError, InterruptedException;

	/**
	 * Verify the signals received by this subscriber. This method will <strong>block</strong>
	 * for the given duration or until the stream has been terminated (either through
	 * {@link #onComplete()}, {@link #onError(Throwable)} or {@link Subscription#cancel()}).
	 * @throws AssertionError in case of expectation failures
	 * @throws InterruptedException if the current thread is interrupted while waiting
	 * @throws TimeoutException if the verification times out
	 */
	void verify(Duration duration) throws AssertionError, InterruptedException, TimeoutException;


	/**
	 * Create a new {@code ScriptedSubscriber} that requests an unbounded amount of values.
	 * @param <T> the type of the subscriber
	 * @return a builder for setting up value expectations
	 */
	static <T> ValueBuilder<T> create() {
		return create(Long.MAX_VALUE);
	}

	/**
	 * Create a new {@code ScriptedSubscriber} that requests a specified amount of values.
	 * @param n the amount of items to request
	 * @param <T> the type of the subscriber
	 * @return a builder for setting up value expectations
	 */
	static <T> ValueBuilder<T> create(long n) {
		DefaultScriptedSubscriberBuilder.checkForNegative(n);
		return new DefaultScriptedSubscriberBuilder<T>(n);
	}

	/**
	 * Create a new {@code ScriptedSubscriber} that expects the given amount of elements to be
	 * received.
	 *  @param n the expected amount of values
	 * @return a builder for setting up termination expectations
	 * @see Subscriber#onNext(Object)
	 */
	static <T> TerminationBuilder<T> expectValueCount(long n) {
		DefaultScriptedSubscriberBuilder.checkForNegative(n);
		return new DefaultScriptedSubscriberBuilder<T>(Long.MAX_VALUE, n);
	}

	/**
	 * Define a builder for terminal states.
	 *
	 * @param <T> the type of values that the subscriber contains
	 */
	interface TerminationBuilder<T> {

		/**
		 * Expect an unspecified error.
		 * @return the built subscriber
		 * @see Subscriber#onError(Throwable)
		 */
		ScriptedSubscriber<T> expectError();

		/**
		 * Expect an error of the specified type.
		 * @param clazz the expected error type
		 * @return the built subscriber
		 * @see Subscriber#onError(Throwable)
		 */
		ScriptedSubscriber<T> expectError(Class<? extends Throwable> clazz);

		/**
		 * Expect an error and evaluate with the given predicate.
		 * @param predicate the predicate to test on the next received error
		 * @return the built subscriber
		 * @see Subscriber#onError(Throwable)
		 */
		ScriptedSubscriber<T> expectErrorWith(Predicate<Throwable> predicate);

		/**
		 * Expect an error and evaluate with the given predicate. The given
		 * {@code assertionMessage} function is used to create the {@code AssertionError} message,
		 * if the expectation failed.
		 * @param predicate the predicate to test on the next received error
		 * @param assertionMessage supplies the exception message
		 * @return the built subscriber
		 * @see Subscriber#onError(Throwable)
		 */
		ScriptedSubscriber<T> expectErrorWith(Predicate<Throwable> predicate,
				Function<Throwable, String> assertionMessage);

		/**
		 * Expect the completion signal.
		 * @return the built subscriber
		 * @see Subscriber#onComplete()
		 */
		ScriptedSubscriber<T> expectComplete();

		/**
		 * Cancel the underlying subscription.
		 * {@link ScriptedSubscriber#create(long)}.
		 * @return the built subscriber
		 * @see Subscription#cancel()
		 */
		ScriptedSubscriber<T> doCancel();
	}

	/**
	 * Define a builder for expecting individual values.
	 *
	 * @param <T> the type of values that the subscriber contains
	 */
	interface ValueBuilder<T> extends TerminationBuilder<T> {

		/**
		 * Request the given amount of elements from the upstream {@code Publisher}. This is in
		 * addition to the initial number of elements requested by
		 * {@link ScriptedSubscriber#create(long)}.
		 * @param n the number of elements to request
		 * @return this builder
		 * @see Subscription#request(long)
		 */
		ValueBuilder<T> doRequest(long n);

		/**
		 * Expect the next element received to be equal to the given value.
		 * @param t the value to expect
		 * @return this builder
		 * @see Subscriber#onNext(Object)
		 */
		ValueBuilder<T> expectValue(T t);

		/**
		 * Expect the next elements received to be equal to the given values.
		 * @param ts the values to expect
		 * @return this builder
		 * @see Subscriber#onNext(Object)
		 */
		ValueBuilder<T> expectValues(T... ts);

		/**
		 * Expect an element and evaluate with the given predicate.
		 * @param predicate the predicate to test on the next received value
		 * @return this builder
		 * @see Subscriber#onNext(Object)
		 */
		ValueBuilder<T> expectValueWith(Predicate<T> predicate);

		/**
		 * Expect an element and evaluate with the given predicate. The given
		 * {@code assertionMessage} function is used to create the {@code AssertionError} message,
		 * if the expectation failed.
		 * @param predicate the predicate to test on the next received value
		 * @param assertionMessage supplies the exception message
		 * @return this builder
		 * @see Subscriber#onNext(Object)
		 */
		ValueBuilder<T> expectValueWith(Predicate<T> predicate,
				Function<T, String> assertionMessage);
	}
}
