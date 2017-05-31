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

package reactor.test;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Schedulers;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.function.Tuple2;

/**
 * A {@link StepVerifier} provides a declarative way of creating a verifiable script for
 * an async {@link Publisher} sequence, by expressing expectations about the events that
 * will happen upon subscription. The verification must be triggered after the terminal
 * expectations (completion, error, cancellation) have been declared, by calling one of
 * the {@link #verify()} methods.
 *
 *
 * <ul> <li>Create a {@code StepVerifier} around a {@link Publisher} using
 * {@link #create create(Publisher)} or
 * {@link #withVirtualTime withVirtualTime(Supplier&lt;Publisher&gt;)}
 * (in which case you should lazily create the publisher inside the provided
 * {@link Supplier lambda}).</li>
 * <li>Set up individual value expectations using
 * {@link Step#expectNext expectNext},
 * {@link Step#expectNextMatches(Predicate) expectNextMatches(Predicate)},
 * {@link Step#assertNext(Consumer) assertNext(Consumer)},
 * {@link Step#expectNextCount(long) expectNextCount(long)} or
 * {@link Step#expectNextSequence(Iterable) expectNextSequence(Iterable)}.</li>
 * <li>Trigger subscription actions during the verification using either
 * {@link Step#thenRequest(long) thenRequest(long)} or {@link Step#thenCancel() thenCancel()}. </li>
 * <li>Finalize the test scenario using a terminal expectation:
 * {@link LastStep#expectComplete expectComplete()},
 * {@link LastStep#expectError expectError()},
 * {@link LastStep#expectError(Class) expectError(Class)},
 * {@link LastStep#expectErrorMatches(Predicate) expectErrorMatches(Predicate)}, or
 * {@link LastStep#thenCancel thenCancel()}.</li>
 * <li>Trigger the verification of the resulting {@code StepVerifier} on its {@code Publisher}
 * using either {@link #verify()} or {@link #verify(Duration)}. (note some of the terminal
 * expectations above have a "verify" prefixed alternative that both declare the
 * expectation and trigger the verification).</li>
 * <li>If any expectations failed, an {@link AssertionError} will be thrown indicating the
 * failures.</li>
 * </ul>
 *
 * <p>For example:
 * <pre>
 * StepVerifier.create(Flux.just("foo", "bar"))
 *   .expectNext("foo")
 *   .expectNext("bar")
 *   .expectComplete()
 *   .verify();
 * </pre>
 *
 * @author Arjen Poutsma
 * @author Stephane Maldini
 * @author Simon Baslé
 */
public interface StepVerifier {

	/**
	 * Prepare a new {@code StepVerifier} in an uncontrolled environment:
	 * {@link Step#thenAwait} will block in real time.
	 * Each {@link #verify()} will fully (re)play the scenario.
	 *
	 * @param publisher the publisher to subscribe to and verify
	 *
	 * @return a builder for expectation declaration and ultimately verification.
	 */
	static <T> FirstStep<T> create(Publisher<? extends T> publisher) {
		return create(publisher, Long.MAX_VALUE);
	}

	/**
	 * Prepare a new {@code StepVerifier} in an uncontrolled environment:
	 * {@link Step#thenAwait} will block in real time.
	 * Each {@link #verify()} will fully (re)play the scenario.
	 * The verification will request a specified amount of values.
	 *
	 * @param publisher the publisher to subscribe to and verify
	 * @param n the amount of items to request
	 *
	 * @return a builder for expectation declaration and ultimately verification.
	 */
	static <T> FirstStep<T> create(Publisher<? extends T> publisher, long n) {
		return create(publisher, StepVerifierOptions.create().initialRequest(n));
	}

	/**
	 * Prepare a new {@code StepVerifier} in an uncontrolled environment:
	 * {@link Step#thenAwait} will block in real time.
	 * Each {@link #verify()} will fully (re)play the scenario.
	 * The verification will request a specified amount of values according to
	 * the {@link StepVerifierOptions options} passed.
	 *
	 * @param publisher the publisher to subscribe to
	 * @param options the options for the verification
	 *
	 * @return a builder for expectation declaration and ultimately verification.
	 */
	static <T> FirstStep<T> create(Publisher<? extends T> publisher, StepVerifierOptions options) {
		return DefaultStepVerifierBuilder.newVerifier(options, () -> publisher);
	}

	/**
	 * Prepare a new {@code StepVerifier} in a controlled environment using
	 * {@link VirtualTimeScheduler} to manipulate a virtual clock via
	 * {@link Step#thenAwait}. The scheduler is injected into all {@link Schedulers} factories,
	 * which means that any operator created within the lambda without a specific scheduler
	 * will use virtual time.
	 * Each {@link #verify()} will fully (re)play the scenario.
	 * The verification will request an unbounded amount of values.
	 *
	 * @param scenarioSupplier a mandatory supplier of the {@link Publisher} to subscribe
	 * to and verify. In order for operators to use virtual time, they must be invoked
	 * from within the lambda.
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for expectation declaration and ultimately verification.
	 */
	static <T> FirstStep<T> withVirtualTime(Supplier<? extends Publisher<? extends T>> scenarioSupplier) {
		return withVirtualTime(scenarioSupplier, Long.MAX_VALUE);
	}

	/**
	 * Prepare a new {@code StepVerifier} in a controlled environment using
	 * {@link VirtualTimeScheduler} to manipulate a virtual clock via
	 * {@link Step#thenAwait}. The scheduler is injected into all {@link Schedulers} factories,
	 * which means that any operator created within the lambda without a specific scheduler
	 * will use virtual time.
	 * Each {@link #verify()} will fully (re)play the scenario.
	 * The verification will request a specified amount of values.
	 *
	 * @param scenarioSupplier a mandatory supplier of the {@link Publisher} to subscribe
	 * to and verify. In order for operators to use virtual time, they must be invoked
	 * from within the lambda.
	 * @param n the amount of items to request (must be &gt;= 0)
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for expectation declaration and ultimately verification.
	 */
	static <T> FirstStep<T> withVirtualTime(Supplier<? extends Publisher<? extends T>> scenarioSupplier,
			long n) {
		return withVirtualTime(scenarioSupplier, () -> VirtualTimeScheduler.getOrSet(), n);
	}

	/**
	 * Prepare a new {@code StepVerifier} in a controlled environment using
	 * a user-provided {@link VirtualTimeScheduler} to manipulate a virtual clock via
	 * {@link Step#thenAwait}. The scheduler is injected into all {@link Schedulers} factories,
	 * which means that any operator created within the lambda without a specific scheduler
	 * will use virtual time.
	 * Each {@link #verify()} will fully (re)play the scenario.
	 * The verification will request a specified amount of values.
	 *
	 * @param scenarioSupplier a mandatory supplier of the {@link Publisher} to subscribe
	 * to and verify. In order for operators to use virtual time, they must be invoked
	 * from within the lambda.
	 * @param vtsLookup the supplier of the {@link VirtualTimeScheduler} to inject and
	 * manipulate during verification.
	 * @param n the amount of items to request (must be &gt;= 0)
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for expectation declaration and ultimately verification.
	 */
	static <T> FirstStep<T> withVirtualTime(
			Supplier<? extends Publisher<? extends T>> scenarioSupplier,
			Supplier<? extends VirtualTimeScheduler> vtsLookup,
			long n) {
		return withVirtualTime(scenarioSupplier, StepVerifierOptions.create()
				.initialRequest(n)
				.virtualTimeSchedulerSupplier(vtsLookup));
	}

	/**
	 * Prepare a new {@code StepVerifier} in a controlled environment using
	 * a user-provided {@link VirtualTimeScheduler} to manipulate a virtual clock via
	 * {@link Step#thenAwait}. The scheduler is injected into all {@link Schedulers} factories,
	 * which means that any operator created within the lambda without a specific scheduler
	 * will use virtual time.
	 * Each {@link #verify()} will fully (re)play the scenario.
	 * The verification will request a specified amount of values according to
	 * the provided {@link StepVerifierOptions options}.
	 *
	 * @param scenarioSupplier a mandatory supplier of the {@link Publisher} to subscribe
	 * to and verify. In order for operators to use virtual time, they must be invoked
	 * from within the lambda.
	 * @param options the verification options, including the supplier of the
	 * {@link VirtualTimeScheduler} to inject and manipulate during verification.
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for expectation declaration and ultimately verification.
	 */
	static <T> FirstStep<T> withVirtualTime(
			Supplier<? extends Publisher<? extends T>> scenarioSupplier,
			StepVerifierOptions options) {
		DefaultStepVerifierBuilder.checkPositive(options.getInitialRequest());
		Objects.requireNonNull(options.getVirtualTimeSchedulerSupplier(), "vtsLookup");
		Objects.requireNonNull(scenarioSupplier, "scenarioSupplier");

		return DefaultStepVerifierBuilder.newVerifier(options,
				scenarioSupplier);
	}

	/**
	 * Activate debug logging of a description of the test scenario, as well as
	 * some details about certain verification steps.
	 *
	 * @return the verifier for final {@link #verify()} call
	 */
	StepVerifier log();

	/**
	 * Verify the signals received by this subscriber. This method will
	 * <strong>block</strong> until the stream has been terminated (either
	 * through {@link Subscriber#onComplete()}, {@link Subscriber#onError(Throwable)} or
	 * {@link Subscription#cancel()}). Depending on the declared expectations and actions,
	 * notably in case of undersized manual requests, such a verification could also block
	 * indefinitely.
	 *
	 * @return the actual {@link Duration} the verification took.
	 * @throws AssertionError in case of expectation failures
	 */
	Duration verify() throws AssertionError;

	/**
	 * Verify the signals received by this subscriber. This method will
	 * <strong>block</strong> for up to the given duration or until the stream has been
	 * terminated (either through {@link Subscriber#onComplete()},
	 * {@link Subscriber#onError(Throwable)} or {@link Subscription#cancel()}).
	 *
	 * @return the actual {@link Duration} the verification took.
	 * @throws AssertionError in case of expectation failures, or when the verification
	 *                        times out
	 */
	Duration verify(Duration duration) throws AssertionError;

	/**
	 * {@link #verify() Verifies} the signals received by this subscriber, then exposes
	 * various {@link Assertions assertion methods} on the final state.
	 * <p>
	 * Note this method will <strong>block</strong> until the stream has been
	 * terminated (either through {@link Subscriber#onComplete()},
	 * {@link Subscriber#onError(Throwable)} or {@link Subscription#cancel()}).
	 * Depending on the declared expectations and actions, notably in case of undersized
	 * manual requests, such a verification could also block indefinitely.
	 *
	 * @return the actual {@link Duration} the verification took.
	 * @throws AssertionError in case of expectation failures
	 */
	Assertions verifyThenAssertThat();

	/**
	 * Define a builder for terminal states.
	 */
	interface LastStep {

		/**
		 * Expect an error and consume with the given consumer. Any
		 * {@code AssertionError}s thrown by the consumer will be rethrown
		 * during {@linkplain #verify() verification}.
		 *
		 * @param consumer the consumer for the exception
		 *
		 * @return the built verification
		 */
		StepVerifier consumeErrorWith(Consumer<Throwable> consumer);

		/**
		 * Expect an unspecified error.
		 *
		 * @return the built verification scenario, ready to be verified
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		StepVerifier expectError();

		/**
		 * Expect an error of the specified type.
		 *
		 * @param clazz the expected error type
		 *
		 * @return the built verification scenario, ready to be verified
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		StepVerifier expectError(Class<? extends Throwable> clazz);

		/**
		 * Expect an error with the specified message.
		 *
		 * @param errorMessage the expected error message
		 *
		 * @return the built verification scenario, ready to be verified
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		StepVerifier expectErrorMessage(String errorMessage);

		/**
		 * Expect an error and evaluate with the given predicate.
		 *
		 * @param predicate the predicate to test on the next received error
		 *
		 * @return the built verification scenario, ready to be verified
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		StepVerifier expectErrorMatches(Predicate<Throwable> predicate);

		/**
		 * Expect the completion signal.
		 *
		 * @return the built verification scenario, ready to be verified
		 *
		 * @see Subscriber#onComplete()
		 */
		StepVerifier expectComplete();

		/**
		 * Cancel the underlying subscription.
		 *
		 * @return the built verification scenario, ready to be verified
		 *
		 * @see Subscription#cancel()
		 */
		StepVerifier thenCancel();

		/**
		 * Trigger the {@link #verify() verification}, expecting an unspecified error
		 * as terminal event.
		 * <p>
		 * This is a convenience method that calls {@link #verify()} in addition to the
		 * expectation. Explicitly use the expect method and verification method
		 * separately if you need something more specific (like activating logging or
		 * putting a timeout).
		 *
		 * @return the actual {@link Duration} the verification took.
		 *
		 * @see #expectError()
		 * @see #verify()
		 * @see Subscriber#onError(Throwable)
		 */
		Duration verifyError();

		/**
		 * Trigger the {@link #verify() verification}, expecting an error of the specified
		 * type as terminal event.
		 * <p>
		 * This is a convenience method that calls {@link #verify()} in addition to the
		 * expectation. Explicitly use the expect method and verification method
		 * separately if you need something more specific (like activating logging or
		 * putting a timeout).
		 *
		 * @param clazz the expected error type
		 * @return the actual {@link Duration} the verification took.
		 *
		 * @see #expectError(Class)
		 * @see #verify()
		 * @see Subscriber#onError(Throwable)
		 */
		Duration verifyError(Class<? extends Throwable> clazz);

		/**
		 * Trigger the {@link #verify() verification}, expecting an error with the
		 * specified message as terminal event.
		 * <p>
		 * This is a convenience method that calls {@link #verify()} in addition to the
		 * expectation. Explicitly use the expect method and verification method
		 * separately if you need something more specific (like activating logging or
		 * putting a timeout).
		 *
		 * @param errorMessage the expected error message
		 * @return the actual {@link Duration} the verification took.
		 *
		 * @see #expectErrorMessage(String)
		 * @see #verify()
		 * @see Subscriber#onError(Throwable)
		 */
		Duration verifyErrorMessage(String errorMessage);

		/**
		 * Trigger the {@link #verify() verification}, expecting an error that matches
		 * the given predicate as terminal event.
		 * <p>
		 * This is a convenience method that calls {@link #verify()} in addition to the
		 * expectation. Explicitly use the expect method and verification method
		 * separately if you need something more specific (like activating logging or
		 * putting a timeout).
		 *
		 * @param predicate the predicate to test on the next received error
		 * @return the actual {@link Duration} the verification took.
		 *
		 * @see #expectErrorMatches(Predicate)
		 * @see #verify()
		 * @see Subscriber#onError(Throwable)
		 */
		Duration verifyErrorMatches(Predicate<Throwable> predicate);

		/**
		 * Trigger the {@link #verify() verification}, expecting a completion signal
		 * as terminal event.
		 * <p>
		 * This is a convenience method that calls {@link #verify()} in addition to the
		 * expectation. Explicitly use the expect method and verification method
		 * separately if you need something more specific (like activating logging or
		 * putting a timeout).
		 *
		 * @return the actual {@link Duration} the verification took.
		 *
		 * @see #expectComplete()
		 * @see #verify()
		 * @see Subscriber#onComplete()
		 *
		 */
		Duration verifyComplete();

	}

	/**
	 * Define a builder for expecting main sequence individual signals.
	 *
	 * @param <T> the type of values that the subscriber contains
	 */
	interface Step<T> extends LastStep {

		/**
		 * Set a description for the previous verification step. Choosing
		 * a unique and descriptive name can make assertion errors easier to
		 * resolve.
		 * <p>
		 * Note that calling this several times in a row will only take the
		 * first description into account.
		 *
		 * @param description the description for the previous verification step
		 * @return this builder
		 */
		Step<T> as(String description);

		/**
		 * Expect an element and consume with the given consumer.Any {@code
		 * AssertionError}s thrown by the consumer will be rethrown during {@linkplain
		 * #verify() verification}.
		 *
		 * @param consumer the consumer for the value
		 *
		 * @return this builder
		 */
		Step<T> consumeNextWith(Consumer<? super T> consumer);

		/**
		 * Expect an element and consume it with the given consumer, usually performing
		 * assertions on it (eg. using Hamcrest, AssertJ or JUnit assertion methods).
		 * Alias for {@link #consumeNextWith(Consumer)} for better discoverability of
		 * that use case.
		 * <p>
		 * Any {@code AssertionError}s thrown by the consumer will be rethrown during
		 * {@linkplain #verify() verification}.
		 *
		 * @param assertionConsumer the consumer for the value, performing assertions
		 * @return this builder
		 */
		default Step<T> assertNext(Consumer<? super T> assertionConsumer) {
			return consumeNextWith(assertionConsumer);
		}

		/**
		 * Expect a recording session started via {@link #recordWith}, end it and verify
		 * it by applying the given consumer.
		 * Any {@code AssertionError}s thrown by the consumer will be rethrown during
		 * {@linkplain #verify() verification}.
		 *
		 * @param consumer the consumer used to apply assertions on the recorded session
		 *
		 * @return this builder
		 */
		Step<T> consumeRecordedWith(Consumer<? super Collection<T>> consumer);

		/**
		 * Expect the next element received to be equal to the given value.
		 *
		 * @param t the value to expect
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		Step<T> expectNext(T t);

		/**
		 * Expect the next elements received to be equal to the given values.
		 *
		 * @param t1 the first value to expect
		 * @param t2 the second value to expect
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		Step<T> expectNext(T t1, T t2);

		/**
		 * Expect the next elements received to be equal to the given values.
		 *
		 * @param t1 the first value to expect
		 * @param t2 the second value to expect
		 * @param t3 the third value to expect
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		Step<T> expectNext(T t1, T t2, T t3);

		/**
		 * Expect the next elements received to be equal to the given values.
		 *
		 * @param t1 the first value to expect
		 * @param t2 the second value to expect
		 * @param t3 the third value to expect
		 * @param t4 the fourth value to expect
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		Step<T> expectNext(T t1, T t2, T t3, T t4);

		/**
		 * Expect the next elements received to be equal to the given values.
		 *
		 * @param t1 the first value to expect
		 * @param t2 the second value to expect
		 * @param t3 the third value to expect
		 * @param t4 the fourth value to expect
		 * @param t5 the fifth value to expect
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		Step<T> expectNext(T t1, T t2, T t3, T t4, T t5);

		/**
		 * Expect the next elements received to be equal to the given values.
		 *
		 * @param t1 the first value to expect
		 * @param t2 the second value to expect
		 * @param t3 the third value to expect
		 * @param t4 the fourth value to expect
		 * @param t5 the fifth value to expect
		 * @param t6 the sixth value to expect
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		Step<T> expectNext(T t1, T t2, T t3, T t4, T t5, T t6);

		/**
		 * Expect the next elements received to be equal to the given values.
		 *
		 * @param ts the values to expect
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		Step<T> expectNext(T... ts);

		/**
		 * Expect to received {@code count} elements, starting from the previous
		 * expectation or onSubscribe.
		 *
		 * @param count the number of emitted items to expect.
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		Step<T> expectNextCount(long count);

		/**
		 * Expect the next elements to match the given {@link Iterable} until its
		 * iterator depletes.
		 *
		 * @param iterable the iterable containing the next expected values
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		Step<T> expectNextSequence(Iterable<? extends T> iterable);

		/**
		 * Expect an element and evaluate with the given predicate.
		 *
		 * @param predicate the predicate to test on the next received value
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		Step<T> expectNextMatches(Predicate<? super T> predicate);

		/**
		 * Expect a {@link Subscription} and consume with the given consumer. Any {@code
		 * AssertionError}s thrown by the consumer will be rethrown during {@linkplain
		 * #verify() verification}.
		 *
		 * @param consumer the consumer for the {@link Subscription}
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onSubscribe(Subscription)
		 */
		Step<T> consumeSubscriptionWith(Consumer<? super Subscription> consumer);

		/**
		 * Expect that no event has been observed by the verifier for the length of
		 * the provided {@link Duration}. If virtual time is used, this duration is
		 * verified using the virtual clock.
		 *
		 * @param duration the duration for which to observe no event has been received
		 *
		 * @return this builder
		 *
		 * @see Subscriber
		 */
		Step<T> expectNoEvent(Duration duration);

		/**
		 * Expect a recording session started via {@link #recordWith}, end it and verify
		 * it by ensuring the provided predicate matches.
		 *
		 * @param predicate the predicate to test on the recorded session
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		Step<T> expectRecordedMatches(Predicate<? super Collection<T>> predicate);

		/**
		 * Start a recording session storing {@link Subscriber#onNext(Object)} values in
		 * the supplied {@link Collection}. Further steps
		 * {@link #expectRecordedMatches(Predicate)} and
		 * {@link #consumeRecordedWith(Consumer)} can consume and assert the session.
		 * <p>If an existing recording session hasn't not been declaratively consumed, this step
		 * will override the current session.
		 *
		 * @param supplier the supplier for the {@link Collection} to use for recording.
		 *
		 * @return this builder
		 */
		Step<T> recordWith(Supplier<? extends Collection<T>> supplier);

		/**
		 * Run an arbitrary task scheduled after previous expectations or tasks.
		 *
		 * @param task the task to run
		 *
		 * @return this builder
		 */
		Step<T> then(Runnable task);

		/**
		 * Mark a Pause in the expectation evaluation.
		 * If a {@link VirtualTimeScheduler} has been configured,
		 * {@link VirtualTimeScheduler#advanceTime()} will be used and the
		 * pause will not block testing or {@link Publisher} thread.
		 *
		 * @return this builder
		 */
		default Step<T> thenAwait() {
			return thenAwait(Duration.ZERO);
		}

		/**
		 * Pause the expectation evaluation for a given {@link Duration}.
		 * If a {@link VirtualTimeScheduler} has been configured,
		 * {@link VirtualTimeScheduler#advanceTimeBy(Duration)} will be used and the
		 * pause will not block testing or {@link Publisher} thread.
		 *
		 * @param timeshift a pause {@link Duration}
		 *
		 * @return this builder
		 */
		Step<T> thenAwait(Duration timeshift);

		/**
		 * Consume further onNext signals as long as they match a predicate.
		 *
		 * @param predicate the condition to continue consuming onNext
		 *
		 * @return this builder
		 */
		Step<T> thenConsumeWhile(Predicate<T> predicate);

		/**
		 * Consume further onNext signals using a provided {@link Consumer} as long as
		 * they match a {@link Predicate}.  You can use the consumer to apply assertions
		 * on each value.
		 *
		 * @param predicate the condition to continue consuming onNext
		 * @param consumer  the consumer to use to consume the data, when the predicate
		 * matches
		 *
		 * @return this builder
		 */
		Step<T> thenConsumeWhile(Predicate<T> predicate, Consumer<T> consumer);

		/**
		 * Request the given amount of elements from the upstream {@code Publisher}. This
		 * is in addition to the initial number of elements requested by an
		 * initial passed demand like with {@link StepVerifier#create(Publisher, long)}.
		 *
		 * @param n the number of elements to request
		 *
		 * @return this builder
		 *
		 * @see Subscription#request(long)
		 */
		Step<T> thenRequest(long n);
	}

	/**
	 * Define a builder for explicitly expecting an initializing {@link Subscription} as
	 * first signal.
	 * <p>
	 * If {@link FirstStep} expectations are not used, the produced
	 * {@link StepVerifier} keeps a first expectation that will be checking if
	 * the first signal is a
	 * {@link Subscription}.
	 *
	 * @param <T> the type of values that the subscriber contains
	 */
	interface FirstStep<T> extends Step<T> {

		/**
		 * Expect the source {@link Publisher} to run with Reactor Fusion flow
		 * optimization. It will be requesting {@link Fuseable#ANY} fusion mode.
		 *
		 * @return this builder
		 *
		 * @see Fuseable
		 */
		Step<T> expectFusion();

		/**
		 * Expect the source {@link Publisher} to run the requested Reactor Fusion mode
		 * from any of these modes :
		 * {@link Fuseable#NONE}, {@link Fuseable#SYNC}, {@link Fuseable#ASYNC},
		 * {@link Fuseable#ANY}, {@link Fuseable#THREAD_BARRIER}.
		 *
		 * @param requested the requested and expected fusion mode
		 *
		 * @return this builder
		 *
		 * @see Fuseable
		 */
		Step<T> expectFusion(int requested);

		/**
		 * Expect the source {@link Publisher} to run with Reactor Fusion flow
		 * optimization.
		 * Expect the source {@link Publisher} to run the requested Reactor Fusion mode
		 * from any of these modes :
		 * {@link Fuseable#NONE}, {@link Fuseable#SYNC}, {@link Fuseable#ASYNC},
		 * {@link Fuseable#ANY}, {@link Fuseable#THREAD_BARRIER}.
		 *
		 * @param requested the requested fusion mode
		 * @param expected the expected fusion mode
		 *
		 * @return this builder
		 *
		 * @see Fuseable
		 */
		Step<T> expectFusion(int requested, int expected);

		/**
		 * Expect the source {@link Publisher} to NOT run with Reactor Fusion flow
		 * optimization. It will check if publisher is {@link Fuseable} or
		 * subscription is a {@link Fuseable.QueueSubscription}.
		 *
		 * @return this builder
		 *
		 * @see Fuseable
		 */
		Step<T> expectNoFusionSupport();

		/**
		 * Expect no event and no Subscription has been observed by the verifier for the
		 * length of the provided {@link Duration}. If virtual time is used, this duration
		 * is verified using the virtual clock.
		 * <p>
		 * Note that you should only use this method as the first expectation if you
		 * actually don't expect a subscription to happen. Use
		 * {@link FirstStep#expectSubscription()} combined with {@link Step#expectNoEvent(Duration)}
		 * to work around that.
		 *
		 * @param duration the duration for which to observe no event has been received
		 *
		 * @return this builder
		 */
		@Override
		FirstStep<T> expectNoEvent(Duration duration);

		/**
		 * Expect a {@link Subscription}.
		 * Effectively behave as the default implicit {@link Subscription} expectation.
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onSubscribe(Subscription)
		 */
		Step<T> expectSubscription();

		/**
		 * Expect a {@link Subscription} and evaluate with the given predicate.
		 *
		 * @param predicate the predicate to test on the received {@link Subscription}
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onSubscribe(Subscription)
		 */
		Step<T> expectSubscriptionMatches(Predicate<? super Subscription> predicate);
	}

	/**
	 * Exposes post-verification state assertions.
	 */
	interface Assertions {

		/**
		 * Assert that the tested publisher has dropped at least one element to the
		 * {@link Hooks#onNextDropped(Consumer)} hook.
		 */
		Assertions hasDroppedElements();

		/**
		 * Assert that the tested publisher has dropped at least all of the provided
		 * elements to the {@link Hooks#onNextDropped(Consumer)} hook, in any order.
		 */
		Assertions hasDropped(Object... values);

		/**
		 * Assert that the tested publisher has dropped all of the provided elements to
		 * the {@link Hooks#onNextDropped(Consumer)} hook, in any order, and that no
		 * other elements were dropped.
		 */
		Assertions hasDroppedExactly(Object... values);

		/**
		 * Assert that the tested publisher has dropped at least one error to the
		 * {@link Hooks#onErrorDropped(Consumer)} hook.
		 */
		Assertions hasDroppedErrors();

		/**
		 * Assert that the tested publisher has dropped exactly n errors to the
		 * {@link Hooks#onErrorDropped(Consumer)} hook.
		 */
		Assertions hasDroppedErrors(int n);

		/**
		 * Assert that the tested publisher has dropped exactly one error of the given type
		 * to the {@link Hooks#onErrorDropped(Consumer)} hook.
		 */
		Assertions hasDroppedErrorOfType(Class<? extends Throwable> clazz);

		/**
		 * Assert that the tested publisher has dropped exactly one error matching the given
		 * predicate to the {@link Hooks#onErrorDropped(Consumer)} hook.
		 */
		Assertions hasDroppedErrorMatching(Predicate<Throwable> matcher);

		/**
		 * Assert that the tested publisher has dropped exactly one error with the exact provided
		 * message to the {@link Hooks#onErrorDropped(Consumer)} hook.
		 */
		Assertions hasDroppedErrorWithMessage(String message);

		/**
		 * Assert that the tested publisher has dropped exactly one error with a message containing
		 * the provided string to the {@link Hooks#onErrorDropped(Consumer)} hook.
		 */
		Assertions hasDroppedErrorWithMessageContaining(String messagePart);

		/**
		 * Assert that the tested publisher has dropped one or more errors to the
		 * {@link Hooks#onErrorDropped(Consumer)} hook, and assert them as a collection.
		 */
		Assertions hasDroppedErrorsSatisfying(Consumer<Collection<Throwable>> errorsConsumer);

		/**
		 * Assert that the tested publisher has dropped one or more errors to the
		 * {@link Hooks#onErrorDropped(Consumer)} hook, and check that the collection of
		 * errors matches a predicate.
		 */
		Assertions hasDroppedErrorsMatching(Predicate<Collection<Throwable>> errorsConsumer);

		/**
		 * Assert that the tested publisher has triggered the {@link Hooks#onOperatorError(BiFunction) onOperatorError} hook
		 * at least once.
		 */
		Assertions hasOperatorErrors();

		/**
		 * Assert that the tested publisher has triggered the {@link Hooks#onOperatorError(BiFunction) onOperatorError} hook
		 * exactly n times.
		 */
		Assertions hasOperatorErrors(int n);

		/**
		 * Assert that the tested publisher has triggered the {@link Hooks#onOperatorError(BiFunction) onOperatorError} hook
		 * exactly once and the error is of the given type.
		 */
		Assertions hasOperatorErrorOfType(Class<? extends Throwable> clazz);

		/**
		 * Assert that the tested publisher has triggered the {@link Hooks#onOperatorError(BiFunction) onOperatorError} hook
		 * exactly once and the error matches the given predicate.
		 */
		Assertions hasOperatorErrorMatching(Predicate<Throwable> matcher);

		/**
		 * Assert that the tested publisher has triggered the {@link Hooks#onOperatorError(BiFunction) onOperatorError} hook
		 * exactly once and the error has the exact provided message.
		 */
		Assertions hasOperatorErrorWithMessage(String message);

		/**
		 * Assert that the tested publisher has triggered the {@link Hooks#onOperatorError(BiFunction) onOperatorError} hook
		 * exactly once, with the error message containing the provided string.
		 */
		Assertions hasOperatorErrorWithMessageContaining(String messagePart);

		/**
		 * Assert that the tested publisher has triggered the {@link Hooks#onOperatorError(BiFunction) onOperatorError} hook
		 * once or more, and assert the errors and optionally associated data as a collection.
		 */
		Assertions hasOperatorErrorsSatisfying(Consumer<Collection<Tuple2<Throwable, ?>>> errorsConsumer);

		/**
		 * Assert that the tested publisher has triggered the {@link Hooks#onOperatorError(BiFunction) onOperatorError} hook
		 * once or more, and check that the collection of errors and their optionally
		 * associated data matches a predicate.
		 */
		Assertions hasOperatorErrorsMatching(Predicate<Collection<Tuple2<Throwable, ?>>> errorsConsumer);

		/**
		 * Assert that the whole verification took strictly less than the provided
		 * duration to execute.
		 * @param d the expected maximum duration of the verification
		 */
		Assertions tookLessThan(Duration d);

		/**
		 * Assert that the whole verification took strictly more than the provided
		 * duration to execute.
		 * @param d the expected minimum duration of the verification
		 */
		Assertions tookMoreThan(Duration d);
	}

}
