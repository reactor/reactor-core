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

package reactor.test.subscriber;

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
 * A {@link ScriptedVerification} is a verifiable, blocking script usually produced by
 * terminal expectations of the said script.
 * <ul> <li>Create a {@code
 * ScriptedVerification} builder using {@link #with(Supplier)} or
 * {@link #with(Supplier, long)}</li>
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
 *
 * @author Arjen Poutsma
 * @author Stephane Maldini
 * @see ScriptedSubscriber
 */
public interface ScriptedVerification {

	/**
	 * Create a new {@code ScriptedVerification} in a controlled environment using
	 * {@link VirtualTimeScheduler} to schedule and expect virtual wait via
	 * {@link StepBuilder#thenAwait}. Each {@link #verify()} will fully (re)play the
	 * scenario. The
	 * verification will request an
	 * unbounded amount of
	 * values.
	 *
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for setting up value expectations
	 */
	static <T> FirstStepBuilder<T, ? extends ScriptedVerification> with(Supplier<? extends Publisher<? extends T>> scenarioSupplier) {
		return with(scenarioSupplier, Long.MAX_VALUE);
	}

	/**
	 * Create a new {@code ScriptedVerification} in a controlled environment using
	 * {@link VirtualTimeScheduler} to schedule and expect virtual wait via
	 * {@link StepBuilder#thenAwait}. Each {@link #verify()} will fully (re)play the
	 * scenario. The verification will request a
	 * specified amount of
	 * values.
	 *
	 * @param scenarioSupplier
	 * @param n the amount of items to request
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for setting up value expectations
	 */
	static <T> FirstStepBuilder<T, ? extends ScriptedVerification> with(Supplier<? extends Publisher<? extends T>> scenarioSupplier,
			long n) {
		DefaultScriptedSubscriberBuilder.checkPositive(n);
		Objects.requireNonNull(scenarioSupplier, "scenarioSupplier");

		return new DefaultScriptedSubscriberBuilder<>(n,
				scenarioSupplier,
				() -> VirtualTimeScheduler.enable(false));
	}

	/**
	 * Create a new {@code ScriptedVerification} in a parameterized environment using
	 * passed
	 * {@link VirtualTimeScheduler} to schedule and expect virtual wait via
	 * {@link StepBuilder#thenAwait}. Each {@link #verify()} will fully (re)play the
	 * scenario. The verification will request a
	 * specified amount of
	 * values.
	 * <p>Note: verification can fallback to non-virtual time by passing an undefined
	 * reference {@code null} of {@link VirtualTimeScheduler}.
	 *
	 * @param source
	 * @param n the amount of items to request
	 * @param virtualTimeScheduler a {@link VirtualTimeScheduler} instance to use in
	 * {@code thenAwait}
	 * @param <T> the type of the subscriber
	 *
	 * @return a builder for setting up value expectations
	 */
	static <T> FirstStepBuilder<T, ? extends ScriptedVerification> with(Publisher<? extends T> source,
			long n,
			VirtualTimeScheduler virtualTimeScheduler) {
		DefaultScriptedSubscriberBuilder.checkPositive(n);
		Objects.requireNonNull(source, "source");

		return new DefaultScriptedSubscriberBuilder<>(n,
				() -> source,
				() -> virtualTimeScheduler);
	}

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
	 *
	 * @param <TARGET> the target {@link ScriptedVerification} type
	 */
	interface LastStepBuilder<TARGET extends ScriptedVerification> {

		/**
		 * Expect an unspecified error.
		 *
		 * @return the built verification
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		TARGET expectError();

		/**
		 * Expect an error of the specified type.
		 *
		 * @param clazz the expected error type
		 *
		 * @return the built verification
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		TARGET expectError(Class<? extends Throwable> clazz);

		/**
		 * Expect an error with the specified message.
		 *
		 * @param errorMessage the expected error message
		 *
		 * @return the built verification
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		TARGET expectErrorMessage(String errorMessage);

		/**
		 * Expect an error and evaluate with the given predicate.
		 *
		 * @param predicate the predicate to test on the next received error
		 *
		 * @return the built verification
		 *
		 * @see Subscriber#onError(Throwable)
		 */
		TARGET expectErrorWith(Predicate<Throwable> predicate);

		/**
		 * Expect an error and consume with the given consumer. Any {@code
		 * AssertionError}s thrown by the consumer will be rethrown during {@linkplain
		 * #verify() verification}.
		 *
		 * @param consumer the consumer for the exception
		 *
		 * @return the built verification
		 */
		TARGET consumeErrorWith(Consumer<Throwable> consumer);

		/**
		 * Expect the completion signal.
		 *
		 * @return the built verification
		 *
		 * @see Subscriber#onComplete()
		 */
		TARGET expectComplete();

		/**
		 * Cancel the underlying subscription.
		 * {@link ScriptedSubscriber#create(long)}.
		 *
		 * @return the built verification
		 *
		 * @see Subscription#cancel()
		 */
		TARGET thenCancel();
	}

	/**
	 * Define a builder for expecting main sequence individual signals.
	 *
	 * @param <T> the type of values that the subscriber contains
	 * @param <TARGET> the target {@link ScriptedVerification} type
	 */
	interface StepBuilder<T, TARGET extends ScriptedVerification>
			extends LastStepBuilder<TARGET> {

		/**
		 * Expect an element and consume with the given consumer. Any {@code
		 * AssertionError}s thrown by the consumer will be rethrown during {@linkplain
		 * #verify() verification}.
		 *
		 * @param consumer the consumer for the value
		 *
		 * @return this builder
		 */
		StepBuilder<T, TARGET> consumeNextWith(Consumer<? super T> consumer);

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
		StepBuilder<T, TARGET> consumeRecordedWith(Consumer<? super Collection<T>> consumer);

		/**
		 * Expect the next elements received to be equal to the given values.
		 *
		 * @param ts the values to expect
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		StepBuilder<T, TARGET> expectNext(T... ts);

		/**
		 * Expect an element count starting from the last expectation or onSubscribe.
		 *
		 * @param count the predicate to test on the next received value
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		StepBuilder<T, TARGET> expectNextCount(long count);

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
		StepBuilder<T, TARGET> expectNextSequence(Iterable<? extends T> iterable);

		/**
		 * Expect an element and evaluate with the given predicate.
		 *
		 * @param predicate the predicate to test on the next received value
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onNext(Object)
		 */
		StepBuilder<T, TARGET> expectNextWith(Predicate<? super T> predicate);

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
		StepBuilder<T, TARGET> expectRecordedWith(Predicate<? super Collection<T>> predicate);

		/**
		 * Start a recording session storing {@link Subscriber#onNext(Object)} values in
		 * the
		 * supplied {@link Collection}. Further steps
		 * {@link #expectRecordedWith(Predicate)} and
		 * {@link #consumeRecordedWith(Consumer)} can consume the session.
		 * <p>If an
		 * existing recording session hasn't not been declaratively consumed, this step
		 * will override the current session.
		 *
		 * @param supplier the task to run
		 *
		 * @return this builder
		 */
		StepBuilder<T, TARGET> recordWith(Supplier<? extends Collection<T>> supplier);

		/**
		 * Run an arbitrary task scheduled after previous expectations or tasks.
		 *
		 * @param task the task to run
		 *
		 * @return this builder
		 */
		StepBuilder<T, TARGET> then(Runnable task);

		/**
		 * Mark a Pause in the expectation evaluation.
		 * If a {@link VirtualTimeScheduler} has been configured,
		 * {@link VirtualTimeScheduler#advanceTime()} will be used and the
		 * pause will not block testing or {@link Publisher} thread.
		 *
		 * @return this builder
		 */
		default StepBuilder<T, TARGET> thenAwait() {
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
		StepBuilder<T, TARGET> thenAwait(Duration timeshift);

		/**
		 * Request the given amount of elements from the upstream {@code Publisher}. This
		 * is in addition to the initial number of elements requested by {@link
		 * ScriptedSubscriber#create(long)}.
		 *
		 * @param n the number of elements to request
		 *
		 * @return this builder
		 *
		 * @see Subscription#request(long)
		 */
		StepBuilder<T, TARGET> thenRequest(long n);
	}

	/**
	 * Define a builder for explicitly expecting an initializing {@link Subscription} as
	 * first signal.
	 * <p>
	 * If {@link FirstStepBuilder} expectations are not used, the produced
	 * {@link ScriptedSubscriber} keeps a first expectation that will be checking if
	 * the first signal is a
	 * {@link Subscription}.
	 *
	 * @param <T> the type of values that the subscriber contains
	 */
	interface FirstStepBuilder<T, TARGET extends ScriptedVerification>
			extends StepBuilder<T, TARGET> {

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
		StepBuilder<T, TARGET> consumeSubscriptionWith(Consumer<? super Subscription> consumer);

		/**
		 * Expect a {@link Subscription}.
		 * Effectively behave as the default implicit {@link Subscription} expectation.
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onSubscribe(Subscription)
		 */
		StepBuilder<T, TARGET> expectSubscription();

		/**
		 * Expect a {@link Subscription} and evaluate with the given predicate.
		 *
		 * @param predicate the predicate to test on the received {@link Subscription}
		 *
		 * @return this builder
		 *
		 * @see Subscriber#onSubscribe(Subscription)
		 */
		StepBuilder<T, TARGET> expectSubscriptionWith(Predicate<? super Subscription> predicate);

		/**
		 * Expect the source {@link Publisher} to NOT run with Reactor Fusion flow
		 * optimization. It will check if publisher is {@link Fuseable} or
		 * subscription is a {@link Fuseable.QueueSubscription}.
		 *
		 * @return this builder
		 *
		 * @see Fuseable
		 */
		StepBuilder<T, TARGET> expectNoFusionSupport();

		/**
		 * Expect the source {@link Publisher} to run with Reactor Fusion flow
		 * optimization. It will be requesting {@link Fuseable#ANY} fusion mode.
		 *
		 * @return this builder
		 *
		 * @see Fuseable
		 */
		StepBuilder<T, TARGET> expectFusion();

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
		StepBuilder<T, TARGET> expectFusion(int requested);

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
		StepBuilder<T, TARGET> expectFusion(int requested, int expected);
	}

}
