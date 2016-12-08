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

package reactor.test;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * A {@link StepVerifier} is a verifiable, blocking script usually produced by
 * terminal expectations of the said script.
 * <ul> <li>Create a {@code
 * StepVerifier} builder using {@link #create} or {@link #withVirtualTime}</li>
 * <li>Set individual up value expectations using
 * {@link Step#expectNext}, {@link Step#expectNextMatches(Predicate)},
 * {@link Step#expectNextCount(long)} or
 * {@link Step#expectNextSequence(Iterable)}
 * .</li>  <li>Set up
 * subscription actions using either
 * {@link Step#thenRequest(long) thenRequest(long)} or {@link
 * Step#thenCancel() thenCancel()}. </li> <li>Build the {@code
 * StepVerifier} using {@link LastStep#expectComplete},
 * {@link LastStep#expectError}, {@link
 * LastStep#expectError(Class) expectError(Class)}, {@link
 * LastStep#expectErrorMatches(Predicate) expectErrorMatches(Predicate)}, or {@link
 * LastStep#thenCancel}. </li> <li>Subscribe the built {@code
 * StepVerifier} to a {@code Publisher}.</li> <li>Verify the expectations using
 * either {@link #verify()} or {@link #verify(Duration)}.</li> <li>If any expectations
 * failed, an {@code AssertionError} will be thrown indicating the failures.</li> </ul>
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
 */
public interface StepVerifier {

	/**
	 * Prepare a new {@code StepVerifier} in an uncontrolled environment: Expect non-virtual
	 * blocking
	 * wait via
	 * {@link Step#thenAwait}. Each {@link #verify()} will fully (re)play the
	 * scenario.
	 *
	 * @param publisher the publisher to subscribe to
	 *
	 * @return the {@link Duration} of the verification
	 *
	 * @throws AssertionError in case of expectation failures
	 */
	static <T> FirstStep<T> create(Publisher<? extends T> publisher) {
		return create(publisher, Long.MAX_VALUE);
	}

	/**
	 * Prepare a new {@code StepVerifier} in an uncontrolled environment: Expect non-virtual
	 * blocking
	 * wait via
	 * {@link Step#thenAwait}. Each {@link #verify()} will fully (re)play the
	 * scenario. The verification will request a
	 * specified amount of
	 * values.
	 *
	 * @param publisher the publisher to subscribe to
	 * @param n the amount of items to request
	 *
	 * @return the {@link Duration} of the verification
	 *
	 * @throws AssertionError in case of expectation failures, or when the verification
	 *                        times out
	 */
	static <T> FirstStep<T> create(Publisher<? extends T> publisher,
			long n) {
		return DefaultStepVerifierBuilder.newVerifier(n, () -> publisher, null);
	}

	/**
	 * Prepare a new {@code StepVerifier} in a controlled environment using
	 * {@link VirtualTimeScheduler} to schedule and expect virtual wait via
	 * {@link Step#thenAwait}. Each {@link #verify()} will fully (re)play the
	 * scenario. The verification will request an unbounded amount of values.
	 *
	 * @param scenarioSupplier a mandatory supplier of the {@link Publisher} to test
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for setting up value expectations
	 */
	static <T> FirstStep<T> withVirtualTime(Supplier<? extends Publisher<? extends T>> scenarioSupplier) {
		return withVirtualTime(scenarioSupplier, Long.MAX_VALUE);
	}

	/**
	 * @deprecated to be removed in 3.1.0 for parameter ordering harmonization. Please
	 * use {@link #withVirtualTime(Supplier, long)} instead.
	 */
	@Deprecated
	static <T> FirstStep<T> withVirtualTime(long n,
			Supplier<? extends Publisher<? extends T>> scenarioSupplier) {
		return withVirtualTime(scenarioSupplier, () -> VirtualTimeScheduler.enable(false), n);
	}

	/**
	 * Prepare a new {@code StepVerifier} in a controlled environment using
	 * {@link VirtualTimeScheduler} to schedule and expect virtual wait via
	 * {@link Step#thenAwait}. Each {@link #verify()} will fully (re)play the
	 * scenario. The verification will request a specified amount of values.
	 *
	 * @param scenarioSupplier a mandatory supplier of the {@link Publisher} to test
	 * @param n the amount of items to request (must be &gt;= 0)
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for setting up value expectations
	 */
	static <T> FirstStep<T> withVirtualTime(Supplier<? extends Publisher<? extends T>> scenarioSupplier,
			long n) {
		return withVirtualTime(scenarioSupplier, () -> VirtualTimeScheduler.enable(false), n);
	}

	/**
	 * @deprecated to be removed in 3.1.0 for parameter ordering harmonization. Please
	 * use {@link #withVirtualTime(Supplier, Supplier, long)} instead.
	 */
	static <T> FirstStep<T> withVirtualTime(long n,
			Supplier<? extends Publisher<? extends T>> scenarioSupplier,
			Supplier<? extends VirtualTimeScheduler> vtsLookup) {
		DefaultStepVerifierBuilder.checkPositive(n);
		Objects.requireNonNull(scenarioSupplier, "scenarioSupplier");
		Objects.requireNonNull(vtsLookup, "vtsLookup");

		return DefaultStepVerifierBuilder.newVerifier(n,
				scenarioSupplier,
				vtsLookup);
	}

	/**
	 * Create a new {@code StepVerifier} in a parameterized environment using
	 * passed
	 * {@link VirtualTimeScheduler} to schedule and expect virtual wait via
	 * {@link Step#thenAwait}. Each {@link #verify()} will fully (re)play the
	 * scenario. The verification will request a specified amount of values.
	 *
	 * @param scenarioSupplier a mandatory supplier of the {@link Publisher} to test
	 * @param vtsLookup a mandatory {@link VirtualTimeScheduler} lookup to use in {@code
	 * thenAwait}
	 * @param n the amount of items to request (must be &gt;= 0)
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for setting up value expectations
	 */
	static <T> FirstStep<T> withVirtualTime(
			Supplier<? extends Publisher<? extends T>> scenarioSupplier,
			Supplier<? extends VirtualTimeScheduler> vtsLookup,
			long n) {
		DefaultStepVerifierBuilder.checkPositive(n);
		Objects.requireNonNull(scenarioSupplier, "scenarioSupplier");
		Objects.requireNonNull(vtsLookup, "vtsLookup");

		return DefaultStepVerifierBuilder.newVerifier(n,
				scenarioSupplier,
				vtsLookup);
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
	 * <strong>block</strong> indefinitely until the stream has been terminated (either
	 * through {@link Subscriber#onComplete()}, {@link Subscriber#onError(Throwable)} or
	 * {@link Subscription#cancel()}).
	 *
	 * @return the {@link Duration} of the verification
	 *
	 * @throws AssertionError in case of expectation failures
	 */
	Duration verify() throws AssertionError;

	/**
	 * Verify the signals received by this subscriber. This method will
	 * <strong>block</strong> for the given duration or until the stream has been
	 * terminated (either through {@link Subscriber#onComplete()},
	 * {@link Subscriber#onError(Throwable)} or
	 * {@link Subscription#cancel()}).
	 *
	 * @return the {@link Duration} of the verification
	 *
	 * @throws AssertionError in case of expectation failures, or when the verification
	 *                        times out
	 */
	Duration verify(Duration duration) throws AssertionError;

	/**
	 * Define a builder for terminal states.
	 */
	interface LastStep {

		/**
		 * Expect an error and consume with the given consumer. Any {@code
		 * AssertionError}s thrown by the consumer will be rethrown during {@linkplain
		 * #verify() verification}.
		 *
		 * @param consumer the consumer for the exception
		 *
		 * @return the built verification
		 */
		StepVerifier consumeErrorWith(Consumer<Throwable> consumer);

		/**
		 * Expect an unspecified error.
		 *
		 * @return the built verification
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		StepVerifier expectError();

		/**
		 * Expect an error of the specified type.
		 *
		 * @param clazz the expected error type
		 *
		 * @return the built verification
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		StepVerifier expectError(Class<? extends Throwable> clazz);

		/**
		 * Expect an error with the specified message.
		 *
		 * @param errorMessage the expected error message
		 *
		 * @return the built verification
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		StepVerifier expectErrorMessage(String errorMessage);

		/**
		 * Expect an error and evaluate with the given predicate.
		 *
		 * @param predicate the predicate to test on the next received error
		 *
		 * @return the built verification
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		StepVerifier expectErrorMatches(Predicate<Throwable> predicate);

		/**
		 * Expect the completion signal.
		 *
		 * @return the built verification
		 *
		 * @see Subscriber#onComplete()
		 */
		StepVerifier expectComplete();

		/**
		 * Cancel the underlying subscription.
		 *
		 * @return the built verification
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
		 * @return the {@link Duration} of the verification
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
		 * @return the {@link Duration} of the verification
		 *
		 * @see #expectError(Class)
		 * @see #verify()
		 * @see Subscriber#onError(Throwable)
		 */
		Duration verifyError(Class<? extends Throwable> clazz);

		/**
		 * Trigger the {@link #verify() verification}, expecting an error with the
		 * specified messagee as terminal event.
		 * <p>
		 * This is a convenience method that calls {@link #verify()} in addition to the
		 * expectation. Explicitly use the expect method and verification method
		 * separately if you need something more specific (like activating logging or
		 * putting a timeout).
		 *
		 * @param errorMessage the expected error message
		 * @return the {@link Duration} of the verification
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
		 * @return the {@link Duration} of the verification
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
		 * @return the {@link Duration} of the verification
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
		 * Expect an element and consume with the given consumer. Any {@code
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
		 * Expect a recording session started via {@link #recordWith} and
		 * consume with
		 * the
		 * given consumer. Any {@code
		 * AssertionError}s thrown by the consumer will be rethrown during {@linkplain
		 * #verify() verification}.
		 *
		 * @param consumer the consumer for the value
		 *
		 * @return this builder
		 */
		Step<T> consumeRecordedWith(Consumer<? super Collection<T>> consumer);

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
		 * Expect an element count starting from the last expectation or onSubscribe.
		 *
		 * @param count the predicate to test on the next received value
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
		 * @param iterable the predicate to test on the next received value
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
		 * Expect that no event has been observed by the verifier. A duration is
		 * necessary to limit in time that "nothing" has effectively happened.
		 *
		 * @param duration the period to observe no event has been received
		 *
		 * @return this builder
		 *
		 * @see Subscriber
		 */
		Step<T> expectNoEvent(Duration duration);

		/**
		 * Expect and end a recording session started via {@link #recordWith} and
		 * consume with
		 * the
		 * given consumer.
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
		 * the
		 * supplied {@link Collection}. Further steps
		 * {@link #expectRecordedMatches(Predicate)} and
		 * {@link #consumeRecordedWith(Consumer)} can consume the session.
		 * <p>If an
		 * existing recording session hasn't not been declaratively consumed, this step
		 * will override the current session.
		 *
		 * @param supplier the task to run
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
		 * they match a {@link Predicate}.
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
		 * Expect no Subscription or any other event for the given duration.
		 *
		 * @param duration the period to observe no event has been received
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

}
