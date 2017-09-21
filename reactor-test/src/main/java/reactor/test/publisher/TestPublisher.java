/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
package reactor.test.publisher;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import javax.annotation.Nullable;

/**
 * A {@link Publisher} that you can directly manipulate, triggering
 * {@link #next(Object) onNext}, {@link #complete() onComplete} and
 * {@link #error(Throwable) onError} events, for testing purposes.
 * You can assert the state of the publisher using its {@code assertXXX} methods,
 * usually inside a {@link reactor.test.StepVerifier}'s
 * {@link reactor.test.StepVerifier.Step#then(Runnable) then} callback.
 * <p>
 * The TestPublisher can also be made more lenient towards the RS spec
 * and allow "spec violations", as enumerated in {@link Violation}. Use the
 * {@link #createNoncompliant(Violation, Violation...)} factory method to create such
 * a misbehaving publisher.
 *
 * @author Simon Basle
 */
public abstract class TestPublisher<T> implements Publisher<T>, PublisherProbe<T> {

	/**
	 * Create a standard {@link TestPublisher}.
	 *
	 * @param <T> the type of the publisher
	 * @return the new {@link TestPublisher}
	 */
	public static <T> TestPublisher<T> create() {
		return new DefaultTestPublisher<>();
	}

	/**
	 * Create a {@link Violation noncompliant} {@link TestPublisher}
	 * with a given push of reactive streams spec violations that will be overlooked.
	 *
	 * @param first the first allowed {@link Violation}
	 * @param rest additional optional violations
	 * @param <T> the type of the publisher
	 * @return the new noncompliant {@link TestPublisher}
	 */
	public static <T> TestPublisher<T> createNoncompliant(Violation first, Violation... rest) {
		return new DefaultTestPublisher<>(first, rest);
	}

	/**
	 * Convenience method to wrap this {@link TestPublisher} to a {@link Flux}.
	 */
	public abstract Flux<T> flux();

	/**
	 * Convenience method to wrap this {@link TestPublisher} to a {@link Mono}.
	 */
	public abstract Mono<T> mono();

	/**
	 * Assert that the current minimum request of all this publisher's subscribers
	 * is &gt;= {@code n}.
	 *
	 * @param n the expected minimum request
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertMinRequested(long n);

	/**
	 * Asserts that this publisher has subscribers.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertSubscribers();

	/**
	 * Asserts that this publisher has exactly n subscribers.
	 *
	 * @param n the expected number of subscribers
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertSubscribers(int n);

	/**
	 * Asserts that this publisher has no subscribers.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertNoSubscribers();

	/**
	 * Asserts that this publisher has had at least one subscriber that has been cancelled.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertCancelled();

	/**
	 * Asserts that this publisher has had at least n subscribers that have been cancelled.
	 *
	 * @param n the expected number of subscribers to have been cancelled.
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertCancelled(int n);

	/**
	 * Asserts that this publisher has had no cancelled subscribers.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertNotCancelled();

	/**
	 * Asserts that this publisher has had subscriber that saw request overflow,
	 * that is received an onNext event despite having a requested amount of 0 at
	 * the time.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertRequestOverflow();

	/**
	 * Asserts that this publisher has had no subscriber with request overflow.
	 * Request overflow is receiving an onNext event despite having a requested amount
	 * of 0 at that time.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertNoRequestOverflow();

	/**
	 * Send 1 {@link Subscriber#onNext(Object) onNext} signal to the subscribers.
	 *
	 * @param value the item to emit (can be null if the relevant {@link Violation} is push)
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> next(@Nullable T value);

	/**
	 * Triggers an {@link Subscriber#onError(Throwable) error} signal to the subscribers.
	 *
	 * @param t the {@link Throwable} to trigger
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> error(Throwable t);

	/**
	 * Triggers {@link Subscriber#onComplete() completion} of this publisher.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> complete();

	/**
	 * Send 1-n {@link Subscriber#onNext(Object) onNext} signals to the subscribers.
	 *
	 * @param first the first item to emit
	 * @param rest the optional remaining items to emit
	 * @return this {@link TestPublisher} for chaining.
	 * @see #next(Object) next
	 */
	@SafeVarargs
	public final TestPublisher<T> next(@Nullable T first, T... rest) {
		Objects.requireNonNull(rest, "rest array is null, please cast to T if null T required");
		next(first);
		for (T t : rest) {
			next(t);
		}
		return this;
	}

	/**
	 * Combine emitting items and completing this publisher.
	 *
	 * @param values the values to emit to subscribers
	 * @return this {@link TestPublisher} for chaining.
	 * @see #next(Object) next
	 * @see #complete() complete
	 */
	@SafeVarargs
	public final TestPublisher<T> emit(T... values) {
		Objects.requireNonNull(values, "values array is null, please cast to T if null T required");
		for (T t : values) {
			next(t);
		}
		return complete();
	}

	/**
	 * Possible misbehavior for a {@link TestPublisher}.
	 */
	public enum Violation {
		/**
		 * Allow {@link TestPublisher#next(Object, Object[]) next} calls to be made
		 * despite insufficient request, without triggering an {@link IllegalStateException}.
		 */
		REQUEST_OVERFLOW,
		/**
		 * Allow {@link TestPublisher#next(Object, Object[]) next}  calls to be made
		 * with a {@code null} value without triggering a {@link NullPointerException}
		 */
		ALLOW_NULL,
		/**
		 * Allow termination signals to be sent several times in a row. This includes
		 * {@link TestPublisher#complete()}, {@link TestPublisher#error(Throwable)} and
		 * {@link TestPublisher#emit(Object[])}.
		 */
		CLEANUP_ON_TERMINATE
	}
}
