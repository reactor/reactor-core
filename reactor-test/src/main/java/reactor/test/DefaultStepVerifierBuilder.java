/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Default implementation of {@link StepVerifier.Step} and
 * {@link StepVerifier.LastStep}.
 *
 * @author Arjen Poutsma
 * @since 1.0
 */
final class DefaultStepVerifierBuilder<T>
		implements StepVerifier.FirstStep<T> {

	/**
	 * the timeout used locally by {@link DefaultStepVerifier#verify()}, changed by
	 * {@link StepVerifier#setDefaultTimeout(Duration)}
	 */
	static Duration defaultVerifyTimeout = StepVerifier.DEFAULT_VERIFY_TIMEOUT;

	private static class HookRecorder {
		final Queue<Object>                                   droppedElements   = new ConcurrentLinkedQueue<>();
		final Queue<Object>                                   discardedElements = new ConcurrentLinkedQueue<>();
		final Queue<Throwable>                                droppedErrors     = new ConcurrentLinkedQueue<>();
		final Queue<Tuple2<Optional<Throwable>, Optional<?>>> operatorErrors    = new ConcurrentLinkedQueue<>();

		private void plugHooks() {
			Hooks.onErrorDropped(droppedErrors::offer);
			Hooks.onNextDropped(droppedElements::offer);
			Hooks.onOperatorError((t, d) -> {
				operatorErrors.offer(Tuples.of(Optional.ofNullable(t), Optional.ofNullable(d)));
				return t;
			});
		}

		public void plugHooks(StepVerifierOptions verifierOptions) {
			plugHooks();

			Context userContext = verifierOptions.getInitialContext();
			verifierOptions.withInitialContext(Operators.enableOnDiscard(userContext, discardedElements::offer));
		}

		public void plugHooksForSubscriber(DefaultVerifySubscriber<?> subscriber) {
			plugHooks();

			Context userContext = subscriber.initialContext;
			subscriber.initialContext = Operators.enableOnDiscard(userContext, discardedElements::offer);
		}

		public void unplugHooks() {
			Hooks.resetOnNextDropped();
			Hooks.resetOnErrorDropped();
			Hooks.resetOnOperatorError();
		}

		public boolean hasDroppedElements() {
			return !droppedElements.isEmpty();
		}

		public boolean noDroppedElements() {
			return !hasDroppedElements();
		}

		public boolean droppedAllOf(Collection<Object> elements) {
			return droppedElements.containsAll(elements);
		}

		public boolean hasDiscardedElements() {
			return !discardedElements.isEmpty();
		}

		public boolean noDiscardedElements() {
			return !hasDiscardedElements();
		}

		public boolean discardedAllOf(Collection<Object> elements) {
			return discardedElements.containsAll(elements);
		}

		public boolean hasDroppedErrors() {
			return !droppedErrors.isEmpty();
		}

		public boolean noDroppedErrors() {
			return !hasDroppedErrors();
		}

		public boolean hasOperatorErrors() {
			return !operatorErrors.isEmpty();
		}
	}

	/**
	 * The {@link MessageFormatter} used for cases where no scenario name has been provided
	 * through {@link StepVerifierOptions}.
	 */
	static final MessageFormatter NO_NAME_MESSAGE_FORMATTER = new MessageFormatter(null, null,
			Collections.emptyList());

	static void checkPositive(long n) {
		if (n < 0) {
			throw new IllegalArgumentException("'n' should be >= 0 but was " + n);
		}
	}

	static void checkStrictlyPositive(long n) {
		if (n <= 0) {
			throw new IllegalArgumentException("'n' should be > 0 but was " + n);
		}
	}

	static <T> StepVerifier.FirstStep<T> newVerifier(StepVerifierOptions options,
			Supplier<? extends Publisher<? extends T>> scenarioSupplier) {
		DefaultStepVerifierBuilder.checkPositive(options.getInitialRequest());
		Objects.requireNonNull(scenarioSupplier, "scenarioSupplier");

		return new DefaultStepVerifierBuilder<>(options, scenarioSupplier);
	}

	final SignalEvent<T>                           defaultFirstStep;
	final List<Event<T>>                           script;
	final MessageFormatter                         messageFormatter;
	final long                                     initialRequest;
	final Supplier<? extends VirtualTimeScheduler> vtsLookup;
	final StepVerifierOptions                      options;

	@Nullable
	Supplier<? extends Publisher<? extends T>> sourceSupplier;
	long hangCheckRequested;
	int  requestedFusionMode = -1;
	int  expectedFusionMode  = -1;

	DefaultStepVerifierBuilder(StepVerifierOptions options,
			@Nullable Supplier<? extends Publisher<? extends T>> sourceSupplier) {
		this.initialRequest = options.getInitialRequest();
		this.options = options;
		if (options.getScenarioName() == null && options.getValueFormatter() == null) {
			this.messageFormatter = NO_NAME_MESSAGE_FORMATTER;
		}
		else {
			this.messageFormatter = new MessageFormatter(options.getScenarioName(), options.getValueFormatter(), options.getExtractors());
		}
		this.vtsLookup = options.getVirtualTimeSchedulerSupplier();
		this.sourceSupplier = sourceSupplier;
		this.script = new ArrayList<>();
		this.defaultFirstStep = newOnSubscribeStep(messageFormatter, "defaultOnSubscribe");
		this.script.add(defaultFirstStep);

		this.hangCheckRequested = initialRequest;
	}

	@Override
	public DefaultStepVerifierBuilder<T> as(String description) {
		this.script.add(new DescriptionEvent<>(description));
		return this;
	}

	@Override
	public DefaultStepVerifier<T> consumeErrorWith(Consumer<Throwable> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		return consumeErrorWith(consumer, "consumeErrorWith", false);
	}

	private DefaultStepVerifier<T> consumeErrorWith(Consumer<Throwable> assertionConsumer, String description, boolean wrap) {
		Objects.requireNonNull(assertionConsumer, "assertionConsumer");
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnError()) {
				return messageFormatter.failOptional(se, "expected: onError(); actual: %s", signal);
			}
			else {
				try {
					assertionConsumer.accept(signal.getThrowable());
					return Optional.empty();
				}
				catch (AssertionError e) {
					if (wrap) return messageFormatter.failOptional(se, "assertion failed on exception <%s>: %s", signal.getThrowable(), e.getMessage());
					throw e;
				}
			}
		}, description);
		this.script.add(event);
		return build();
	}

	@Override
	public DefaultStepVerifierBuilder<T> assertNext(Consumer<? super T> consumer) {
		return consumeNextWith(consumer, "assertNext");
	}

	@Override
	public DefaultStepVerifierBuilder<T> consumeNextWith(
			Consumer<? super T> consumer) {
		return consumeNextWith(consumer, "consumeNextWith");
	}

	private DefaultStepVerifierBuilder<T> consumeNextWith(Consumer<? super T> consumer, String description) {
		Objects.requireNonNull(consumer, "consumer");
		checkPotentialHang(1, description);
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnNext()) {
				return messageFormatter.failOptional(se, "expected: onNext(); actual: %s", signal);
			}
			else {
				consumer.accept(signal.get());
				return Optional.empty();
			}
		}, description);
		this.script.add(event);
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> consumeRecordedWith(
			Consumer<? super Collection<T>> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		this.script.add(new CollectEvent<>(consumer,
				messageFormatter, "consumeRecordedWith"));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> consumeSubscriptionWith(
			Consumer<? super Subscription> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		if(script.isEmpty() || (script.size() == 1 && script.get(0) == defaultFirstStep)) {
			this.script.set(0, new SignalEvent<>((signal, se) -> {
				if (!signal.isOnSubscribe()) {
					return messageFormatter.failOptional(se, "expected: onSubscribe(); actual: %s", signal);
				}
				else {
					consumer.accept(signal.getSubscription());
					return Optional.empty();
				}
			}, "consumeSubscriptionWith"));
		}
		else {
			this.script.add(new SubscriptionConsumerEvent<>(consumer,
					"consumeSubscriptionWith"));
		}
		return this;
	}

	@Override
	public StepVerifier.ContextExpectations<T> expectAccessibleContext() {
		return new DefaultContextExpectations<>(this, messageFormatter);
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNoAccessibleContext() {
		return consumeSubscriptionWith(sub -> {
					Scannable lowest = Scannable.from(sub);
					Scannable verifierSubscriber = Scannable.from(lowest.scan(Scannable.Attr.ACTUAL));

					Context c = Flux.fromStream(verifierSubscriber.parents())
					                .ofType(CoreSubscriber.class)
					                .map(CoreSubscriber::currentContext)
					                .blockLast();

					if (c != null) {
						throw messageFormatter.assertionError("Expected no accessible Context, got " + c);
					}
				});
	}

	@Override
	public DefaultStepVerifier<T> expectComplete() {
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnComplete()) {
				return messageFormatter.failOptional(se, "expected: onComplete(); actual: %s", signal);
			}
			else {
				return Optional.empty();
			}
		}, "expectComplete");
		this.script.add(event);
		return build();
	}

	@Override
	public DefaultStepVerifier<T> expectError() {
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnError()) {
				return messageFormatter.failOptional(se, "expected: onError(); actual: %s", signal);
			}
			else {
				return Optional.empty();
			}
		}, "expectError()");
		this.script.add(event);
		return build();

	}

	@Override
	public DefaultStepVerifier<T> expectError(Class<? extends Throwable> clazz) {
		Objects.requireNonNull(clazz, "clazz");
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnError()) {
				return messageFormatter.failOptional(se, "expected: onError(%s); actual: %s",
						clazz.getSimpleName(), signal);
			}
			else if (!clazz.isInstance(signal.getThrowable())) {
				return messageFormatter.failOptional(se, "expected error of type: %s; actual type: %s",
						clazz.getSimpleName(), signal.getThrowable());
			}
			else {
				return Optional.empty();
			}
		}, "expectError(Class)");
		this.script.add(event);
		return build();
	}

	@Override
	public DefaultStepVerifier<T> expectErrorMessage(String errorMessage) {
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnError()) {
				return messageFormatter.failOptional(se, "expected: onError(\"%s\"); actual: %s",
						errorMessage, signal);
			}
			else if (!Objects.equals(errorMessage,
					signal.getThrowable()
					      .getMessage())) {
				return messageFormatter.failOptional(se, "expected error message: \"%s\"; " + "actual " + "message: %s",
						errorMessage,
						signal.getThrowable()
						      .getMessage());
			}
			else {
				return Optional.empty();
			}
		}, "expectErrorMessage");
		this.script.add(event);
		return build();
	}

	@Override
	public DefaultStepVerifier<T> expectErrorMatches(Predicate<Throwable> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnError()) {
				return messageFormatter.failOptional(se, "expected: onError(); actual: %s", signal);
			}
			else if (!predicate.test(signal.getThrowable())) {
				return messageFormatter.failOptional(se, "predicate failed on exception: %s", signal.getThrowable());
			}
			else {
				return Optional.empty();
			}
		}, "expectErrorMatches");
		this.script.add(event);
		return build();
	}

	@Override
	public DefaultStepVerifier<T> expectErrorSatisfies(Consumer<Throwable> assertionConsumer) {
		return consumeErrorWith(assertionConsumer, "expectErrorSatisfies", true);
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNoFusionSupport() {
		return expectFusion(Fuseable.ANY, Fuseable.NONE);
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectFusion() {
		return expectFusion(Fuseable.ANY, Fuseable.ANY);
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectFusion(int requested) {
		return expectFusion(requested, requested);
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectFusion(int requested,
			int expected) {
		checkPositive(requested);
		checkPositive(expected);
		requestedFusionMode = requested;
		expectedFusionMode = expected;
		return this;
	}

	@Override
	public final DefaultStepVerifierBuilder<T> expectNext(T t) {
		return addExpectedValues(new Object[] { t });
	}

	@Override
	public final DefaultStepVerifierBuilder<T> expectNext(T t1, T t2) {
		return addExpectedValues(new Object[] { t1, t2 });
	}

	@Override
	public final DefaultStepVerifierBuilder<T> expectNext(T t1, T t2, T t3) {
		return addExpectedValues(new Object[] { t1, t2, t3 });
	}

	@Override
	public final DefaultStepVerifierBuilder<T> expectNext(T t1, T t2, T t3, T t4) {
		return addExpectedValues(new Object[] { t1, t2, t3, t4 });
	}

	@Override
	public final DefaultStepVerifierBuilder<T> expectNext(T t1, T t2, T t3, T t4, T t5) {
		return addExpectedValues(new Object[] { t1, t2, t3, t4, t5 });
	}

	@Override
	public final DefaultStepVerifierBuilder<T> expectNext(T t1, T t2, T t3, T t4, T t5, T t6) {
		return addExpectedValues(new Object[] { t1, t2, t3, t4, t5, t6 });
	}

	@Override
	@SafeVarargs
	public final DefaultStepVerifierBuilder<T> expectNext(T... ts) {
		Objects.requireNonNull(ts, "ts");
		Arrays.stream(ts).forEach(this::addExpectedValue);
		return this;
	}

	@SuppressWarnings("unchecked") // cast to a known type
	private DefaultStepVerifierBuilder<T> addExpectedValues(Object[] values) {
		Arrays.stream(values).map(val -> (T) val).forEach(this::addExpectedValue);
		return this;
	}

	private void addExpectedValue(T value) {
		String desc = messageFormatter.format("expectNext(%s)", value);
		checkPotentialHang(1, desc);
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnNext()) {
				return messageFormatter.failOptional(se, "expected: onNext(%s); actual: %s", value, signal);
			}
			else if (!Objects.equals(value, signal.get())) {
				return messageFormatter.failOptional(se, "expected value: %s; actual value: %s", value, signal.get());
			}
			else {
				return Optional.empty();
			}
		}, desc);
		this.script.add(event);
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNextSequence(
			Iterable<? extends T> iterable) {
		Objects.requireNonNull(iterable, "iterable");
		if (iterable.iterator().hasNext()) {
			if (iterable instanceof Collection) {
				checkPotentialHang(((Collection) iterable).size(), "expectNextSequence");
			} else {
				//best effort
				checkPotentialHang(-1, "expectNextSequence");
			}
			this.script.add(new SignalSequenceEvent<>(iterable,
					messageFormatter, "expectNextSequence"));
		}
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNextCount(long count) {
		checkPositive(count);
		if (count != 0) {
			String desc = "expectNextCount(" + count + ")";
			checkPotentialHang(count, desc);
			this.script.add(new SignalCountEvent<>(count, desc));
		}
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNextMatches(
			Predicate<? super T> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		checkPotentialHang(1, "expectNextMatches");
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnNext()) {
				return messageFormatter.failOptional(se, "expected: onNext(); actual: %s", signal);
			}
			else if (!predicate.test(signal.get())) {
				return messageFormatter.failOptional(se, "predicate failed on value: %s", signal.get());
			}
			else {
				return Optional.empty();
			}
		}, "expectNextMatches");
		this.script.add(event);
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectRecordedMatches(
			Predicate<? super Collection<T>> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		this.script.add(new CollectEvent<>(predicate,
				messageFormatter, "expectRecordedMatches"));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectSubscription() {
		if(this.script.get(0) instanceof NoEvent) {
			this.script.add(defaultFirstStep);
		}
		else{
			this.script.set(0, newOnSubscribeStep(messageFormatter, "expectSubscription"));
		}
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectSubscriptionMatches(
			Predicate<? super Subscription> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		this.script.set(0, new SignalEvent<>((signal, se) -> {
			if (!signal.isOnSubscribe()) {
				return messageFormatter.failOptional(se, "expected: onSubscribe(); actual: %s", signal);
			}
			else if (!predicate.test(signal.getSubscription())) {
				return messageFormatter.failOptional(se, "predicate failed on subscription: %s",
						signal.getSubscription());
			}
			else {
				return Optional.empty();
			}
		}, "expectSubscriptionMatches"));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNoEvent(Duration duration) {
		Objects.requireNonNull(duration, "duration");
		if(this.script.size() == 1 && this.script.get(0) == defaultFirstStep){
			this.script.set(0, new NoEvent<>(duration, "expectNoEvent"));
		}
		else {
			this.script.add(new NoEvent<>(duration, "expectNoEvent"));
		}
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> recordWith(Supplier<? extends Collection<T>> supplier) {
		Objects.requireNonNull(supplier, "supplier");
		this.script.add(new CollectEvent<>(supplier, messageFormatter, "recordWith"));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> then(Runnable task) {
		Objects.requireNonNull(task, "task");
		this.script.add(new TaskEvent<>(task, "then"));
		return this;
	}

	@Override
	public DefaultStepVerifier<T> thenCancel() {
		this.script.add(new SubscriptionEvent<>("thenCancel"));
		return build();
	}

	@Override
	public DefaultStepVerifier<T> expectTimeout(Duration duration) {
		if (sourceSupplier == null) {
			throw new IllegalStateException("Attempting to call expectTimeout() without a Supplier<Publisher>");
		}
		Supplier<? extends Publisher<? extends T>> originalSupplier = sourceSupplier;
		this.sourceSupplier = () -> Flux.from(originalSupplier.get()).timeout(duration);

		WaitEvent<T> timeout = new WaitEvent<>(duration, "expectTimeout-wait");
		SignalEvent<T> timeoutVerification = new SignalEvent<>((signal, se) -> {
			if (signal.isOnError() && signal.getThrowable() instanceof TimeoutException) {
				return Optional.empty();
			}
			else {
				return messageFormatter.failOptional(se, "expected: timeout(%s); actual: %s",
						ValueFormatters.DURATION_CONVERTER.apply(duration), signal);
			}
		}, "expectTimeout");
		this.script.add(timeout);
		this.script.add(timeoutVerification);
		return build();
	}

	@Override
	public Duration verifyError() {
		return expectError().verify();
	}

	@Override
	public Duration verifyError(Class<? extends Throwable> clazz) {
		return expectError(clazz).verify();
	}

	@Override
	public Duration verifyErrorMessage(String errorMessage) {
		return expectErrorMessage(errorMessage).verify();
	}

	@Override
	public Duration verifyErrorMatches(Predicate<Throwable> predicate) {
		return expectErrorMatches(predicate).verify();
	}

	@Override
	public Duration verifyErrorSatisfies(Consumer<Throwable> assertionConsumer) {
		return consumeErrorWith(assertionConsumer, "verifyErrorSatisfies", true).verify();
	}

	@Override
	public Duration verifyComplete() {
		return expectComplete().verify();
	}

	@Override
	public DefaultStepVerifierBuilder<T> thenRequest(long n) {
		checkStrictlyPositive(n);
		this.script.add(new RequestEvent<>(n, "thenRequest"));
		this.hangCheckRequested = Operators.addCap(hangCheckRequested, n);
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> thenAwait() {
		return thenAwait(Duration.ZERO);
	}

	@Override
	public DefaultStepVerifierBuilder<T> thenAwait(Duration timeshift) {
		Objects.requireNonNull(timeshift, "timeshift");
		this.script.add(new WaitEvent<>(timeshift, "thenAwait"));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> thenConsumeWhile(Predicate<T> predicate) {
		return thenConsumeWhile(predicate, t -> {});
	}

	@Override
	public DefaultStepVerifierBuilder<T> thenConsumeWhile(Predicate<T> predicate,
				Consumer<T> consumer) {
		Objects.requireNonNull(predicate, "predicate");
		checkPotentialHang(-1, "thenConsumeWhile");
		this.script.add(new SignalConsumeWhileEvent<>(predicate, consumer, "thenConsumeWhile"));
		return this;
	}

	private void checkPotentialHang(long expectedAmount, String stepDescription) {
		if (!options.isCheckUnderRequesting()) {
			return;
		}

		boolean bestEffort = false;
		if (expectedAmount == -1) {
			bestEffort = true;
			expectedAmount = 1;
		}
		if (this.hangCheckRequested < expectedAmount) {
			StringBuilder message = new StringBuilder()
					.append("The scenario will hang at ")
					.append(stepDescription)
					.append(" due to too little request being performed for the expectations to finish; ")
					.append("request remaining since last step: ")
					.append(hangCheckRequested)
					.append(", expected: ");
			if (bestEffort) {
				message.append("at least ")
				       .append(expectedAmount)
				       .append(" (best effort estimation)");
			} else {
				message.append(expectedAmount);
			}
			throw messageFormatter.error(IllegalArgumentException::new, message.toString());
		}
		else {
			this.hangCheckRequested -= expectedAmount;
		}
	}

	final DefaultStepVerifier<T> build() {
		return new DefaultStepVerifier<>(this);
	}

	interface Event<T> {

		boolean setDescription(String description);

		String getDescription();

	}

	final static class DefaultStepVerifier<T> implements StepVerifier {

		/** A global lock that is used to make withVirtualTime calls mutually exclusive */
		private static final Lock vtsLock = new ReentrantLock(true);

		private final DefaultStepVerifierBuilder<T> parent;
		private final int requestedFusionMode;
		private final int expectedFusionMode;

		private boolean debugEnabled;

		DefaultStepVerifier(DefaultStepVerifierBuilder<T> parent) {
			this.parent = parent;
			this.requestedFusionMode = parent.requestedFusionMode;
			this.expectedFusionMode = parent.expectedFusionMode == -1 ? parent.requestedFusionMode : parent.expectedFusionMode;
		}

		@Override
		public DefaultStepVerifier<T> log() {
			this.debugEnabled = true;
			return this;
		}

		@Override
		public StepVerifier verifyLater() {
			return toVerifierAndSubscribe();
		}

		@Override
		public Assertions verifyThenAssertThat() {
			return verifyThenAssertThat(defaultVerifyTimeout);
		}

		@Override
		public Assertions verifyThenAssertThat(Duration duration) {
			HookRecorder stepRecorder = new HookRecorder();
			stepRecorder.plugHooks(parent.options);

			try {
				//trigger the verify
				Duration time = verify(duration);

				//return the assertion API
				return new DefaultStepVerifierAssertions(stepRecorder, time, parent.messageFormatter);
			}
			finally {
				stepRecorder.unplugHooks();
			}
		}

		@Override
		public Duration verify() {
			return verify(defaultVerifyTimeout);
		}

		@Override
		public Duration verify(Duration duration) {
			Objects.requireNonNull(duration, "duration");
			Instant now = Instant.now();

			DefaultVerifySubscriber<T> newVerifier = toVerifierAndSubscribe();
			newVerifier.verify(duration);

			return Duration.between(now, Instant.now());
		}

		DefaultVerifySubscriber<T> toVerifierAndSubscribe() {
			if (parent.sourceSupplier == null) {
				throw new IllegalArgumentException("no source to automatically subscribe to for verification");
			}
			final VirtualTimeScheduler vts;
			final Disposable vtsCleanup;
			if (parent.vtsLookup != null) {
				vtsLock.lock(); //wait for other virtualtime verifies to finish
				vts = parent.vtsLookup.get();
				//this works even for the default case where StepVerifier has created
				// a vts through enable(false), because the CURRENT will already be that vts
				VirtualTimeScheduler.set(vts);
				vtsCleanup = () -> {
						vts.dispose();
				//explicitly reset the factory, rather than rely on vts shutdown doing so
				// because it could have been eagerly shut down in a test.
				VirtualTimeScheduler.reset();
				vtsLock.unlock();
				};
			}
			else {
				vts = null;
				vtsCleanup = Disposables.disposed();
			}
			try {
				Publisher<? extends T> publisher = parent.sourceSupplier.get();

				DefaultVerifySubscriber<T> newVerifier = new DefaultVerifySubscriber<>(
						this.parent.script,
						this.parent.messageFormatter,
						this.parent.initialRequest,
						this.requestedFusionMode,
						this.expectedFusionMode,
						this.debugEnabled,
						this.parent.options.getInitialContext(),
						vts,
						vtsCleanup);

				publisher.subscribe(newVerifier);
				return newVerifier;
			}
			catch (Throwable error) {
				//in case the subscription fails, make sure to cleanup the VTS
				vtsCleanup.dispose();
				throw error;
			}
		}

		/**
		 * Converts the {@link StepVerifier} to a {@link Subscriber}, leaving all the
		 * lifecycle management to the user. Most notably:
		 * <ul>
		 *     <li>no subscription is performed
		 *     <li>no {@link VirtualTimeScheduler} is registered in the Schedulers factories
		 * </ul>
		 * <p>
		 * However if a {@link VirtualTimeScheduler} supplier was passed in originally
		 * it will be invoked and the resulting scheduler will be affected by time
		 * manipulation methods. That scheduler can be retrieved from the subscriber's
		 * {@link DefaultVerifySubscriber#virtualTimeScheduler() virtualTimeScheduler()}
		 * method.
		 */
		DefaultVerifySubscriber<T> toSubscriber() {
			VirtualTimeScheduler vts = null;
			if (parent.vtsLookup != null) {
				vts = parent.vtsLookup.get();
			}
			return new DefaultVerifySubscriber<>(
					this.parent.script,
					this.parent.messageFormatter,
					this.parent.initialRequest,
					this.requestedFusionMode,
					this.expectedFusionMode,
					this.debugEnabled,
					this.parent.options.getInitialContext(),
					vts,
					null);
		}

	}

	final static class DefaultVerifySubscriber<T>
			extends AtomicReference<Subscription>
			implements StepVerifier, CoreSubscriber<T>, Scannable {

		final CountDownLatch       completeLatch;
		final Queue<Event<T>>      script;
		final MessageFormatter     messageFormatter;
		final Queue<TaskEvent<T>>  taskEvents;
		final int                  requestedFusionMode;
		final int                  expectedFusionMode;
		final long                 initialRequest;
		final VirtualTimeScheduler virtualTimeScheduler;
		final Disposable           postVerifyCleanup;

		Context                       initialContext;
		@Nullable
		Logger                        logger;
		int                           establishedFusionMode;
		Fuseable.QueueSubscription<T> qs;
		long                          produced;   //used for request tracking
		long                          unasserted; //used for expectNextXXX tracking
		volatile long                 requested;
		volatile boolean done; // async fusion
		Iterator<? extends T>         currentNextAs;
		Collection<T>                 currentCollector;

		static final AtomicLongFieldUpdater<DefaultVerifySubscriber> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(DefaultVerifySubscriber.class, "requested");

		@SuppressWarnings("unused")
		volatile int wip;

		@SuppressWarnings("unused")
		volatile Throwable errors;

		volatile boolean monitorSignal;

		/**
		 * used to keep track of the terminal error, if any
		 */
		volatile Signal<T> terminalError;

		/** The constructor used for verification, where a VirtualTimeScheduler can be
		 * passed */
		@SuppressWarnings("unchecked")
		DefaultVerifySubscriber(List<Event<T>> script,
				MessageFormatter messageFormatter,
				long initialRequest,
				int requestedFusionMode,
				int expectedFusionMode,
				boolean debugEnabled,
				@Nullable Context initialContext,
				@Nullable VirtualTimeScheduler vts,
				@Nullable Disposable postVerifyCleanup) {
			this.virtualTimeScheduler = vts;
			this.requestedFusionMode = requestedFusionMode;
			this.expectedFusionMode = expectedFusionMode;
			this.initialRequest = initialRequest;
			this.logger = debugEnabled ? Loggers.getLogger(StepVerifier.class) : null;
			this.script = conflateScript(script, this.logger);
			this.taskEvents = new ConcurrentLinkedQueue<>();
			Event<T> event;
			for (; ; ) {
				event = this.script.peek();
				if (event instanceof TaskEvent) {
					taskEvents.add((TaskEvent<T>) this.script.poll());
				}
				else {
					break;
				}
			}
			this.monitorSignal = taskEvents.peek() instanceof NoEvent;
			this.produced = 0L;
			this.unasserted = 0L;
			this.completeLatch = new CountDownLatch(1);
			this.requested = initialRequest;
			this.initialContext = initialContext == null ? Context.empty() : initialContext;
			this.messageFormatter = messageFormatter;
			this.postVerifyCleanup = postVerifyCleanup;
		}

		@Override
		public String toString() {
			return "StepVerifier Subscriber";
		}

		static <R> Queue<Event<R>> conflateScript(List<Event<R>> script, @Nullable Logger logger) {
			ConcurrentLinkedQueue<Event<R>> queue = new ConcurrentLinkedQueue<>(script);
			ConcurrentLinkedQueue<Event<R>> conflated = new ConcurrentLinkedQueue<>();

			Event event;
			while ((event = queue.peek()) != null) {
				if (event instanceof TaskEvent) {
					conflated.add(queue.poll());
					while (queue.peek() instanceof SubscriptionEvent) {
						conflated.add(new SubscriptionTaskEvent<>((SubscriptionEvent<R>) queue.poll()));
					}
				} else {
					conflated.add(queue.poll());
				}
			}

			Iterator<Event<R>> iterator = conflated.iterator();
			Event<R> previous = null;
			while(iterator.hasNext()) {
				Event<R> current = iterator.next();
				if (previous != null && current instanceof DescriptionEvent) {
					String newDescription = current.getDescription();
					String oldDescription = previous.getDescription();
					boolean applied = previous.setDescription(newDescription);
					if (logger != null && applied) {
						logger.debug("expectation <{}> now described as <{}>",
								oldDescription, newDescription);
					}
				}
				previous = current;
			}

			queue.clear();
			queue.addAll(conflated.stream()
			                      .filter(ev -> !(ev instanceof DescriptionEvent))
			                      .collect(Collectors.toList()));
			conflated = queue;

			if (logger != null) {
				logger.debug("Scenario:");
				for (Event<R> current : conflated) {
					logger.debug("\t<{}>", current.getDescription());
				}
			}
			//TODO simplify whole algo, remove DescriptionTasks

			return conflated;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				Subscription s = this.get();
				if (s instanceof Scannable) return s;
			}

			return null;
		}

		@Override
		public Context currentContext() {
			return initialContext;
		}

		/**
		 * @return the {@link VirtualTimeScheduler} this verifier will manipulate when
		 * using {@link #thenAwait(Duration)} methods, or null if real time is used
		 */
		VirtualTimeScheduler virtualTimeScheduler() {
			return this.virtualTimeScheduler;
		}

		boolean isCancelled() {
			return get() == Operators.cancelledSubscription();
		}

		boolean isTerminated() {
			return completeLatch.getCount() == 0L;
		}

		@Override
		public void onComplete() {
			if (establishedFusionMode != Fuseable.ASYNC) {
				onExpectation(Signal.complete());
				this.completeLatch.countDown();
			}
			else {
				done = true;
				serializeDrainAndSubscriptionEvent();
			}
		}

		@Override
		public void onError(Throwable t) {
			this.terminalError = Signal.error(t);
			onExpectation(terminalError);
			this.completeLatch.countDown();
		}

		@Override
		public void onNext(T t) {
			if (establishedFusionMode == Fuseable.ASYNC) {
				serializeDrainAndSubscriptionEvent();
			}
			else {
				produced++;
				unasserted++;
				if (currentCollector != null) {
					currentCollector.add(t);
				}
				Signal<T> signal = Signal.next(t);
				if (!checkRequestOverflow(signal)) {
					onExpectation(signal);
				}
			}
		}

		@Override
		public void onSubscribe(Subscription subscription) {
			Objects.requireNonNull(subscription, "onSubscribe");

			if (this.compareAndSet(null, subscription)) {
				onExpectation(Signal.subscribe(subscription));
				if (requestedFusionMode >= Fuseable.NONE) {
					startFusion(subscription);
				}
				else if (initialRequest != 0L) {
					subscription.request(initialRequest);
				}
			}
			else {
				subscription.cancel();
				if (isCancelled()) {
					setFailure(null, "an unexpected Subscription has been received: %s; actual: cancelled",
							subscription);
				}
				// subscribeOrReturn may throw an exception after calling onSubscribe.
				else if (!Operators.canAppearAfterOnSubscribe(subscription)) {
					setFailure(null, "an unexpected Subscription has been received: %s; actual: %s",
							subscription,
							this.get());
				}
			}
		}

		void drainAsyncLoop(){
			T t;
			long r = requested;
			for( ; ;) {
				boolean d = done;
				if (d && qs.isEmpty()) {
					if(get() == Operators.cancelledSubscription()){
						return;
					}
					if(errors != null){
						onExpectation(Signal.complete());
					}
					this.completeLatch.countDown();
					return;
				}

				if (r == 0L) {
					return;
				}
				long p = 0L;
				while (p != r) {
					if(get() == Operators.cancelledSubscription()){
						return;
					}
					try {
						t = qs.poll();
						if (t == null) {
							break;
						}
						p++;
						produced++;
						unasserted++;
					}
					catch (Throwable e) {
						Exceptions.throwIfFatal(e);
						cancel();
						onError(Exceptions.unwrap(e));
						return;
					}
					if (currentCollector != null) {
						currentCollector.add(t);
					}
					Signal<T> signal = Signal.next(t);
					if (!checkRequestOverflow(signal)) {
						onExpectation(signal);
						if (d && qs.isEmpty()) {
							if(get() == Operators.cancelledSubscription()){
								return;
							}
							if(errors != null){
								onExpectation(Signal.complete());
							}
							this.completeLatch.countDown();
							return;
						}
					}
					else {
						return;
					}
				}

				if (p != 0) {
					r = REQUESTED.addAndGet(this, -p);
				}

				if(r == 0L || qs.isEmpty()){
					break;
				}
			}
		}

		@Override
		public DefaultVerifySubscriber<T> log() {
			if (this.logger == null) {
				this.logger = Loggers.getLogger(StepVerifier.class);
			}
			return this;
		}

		@Override
		public StepVerifier verifyLater() {
			//intentionally NO-OP
			return this;
		}

		@Override
		public Assertions verifyThenAssertThat() {
			return verifyThenAssertThat(defaultVerifyTimeout);
		}

		@Override
		public Assertions verifyThenAssertThat(Duration duration) {
			HookRecorder stepRecorder = new HookRecorder();
			stepRecorder.plugHooksForSubscriber(this);

			try {
				//trigger the verify
				Duration time = verify(duration);

				//return the assertion API
				return new DefaultStepVerifierAssertions(stepRecorder, time,
						messageFormatter);
			}
			finally {
				stepRecorder.unplugHooks();
			}
		}

		@Override
		public Duration verify() {
			return verify(defaultVerifyTimeout);
		}

		@Override
		public Duration verify(Duration duration) {
			try {
				Objects.requireNonNull(duration, "duration");
				Instant now = Instant.now();
				try {
					pollTaskEventOrComplete(duration);
				}
				catch (InterruptedException ex) {
					Thread.currentThread()
					      .interrupt();
				}
				validate();
				return Duration.between(now, Instant.now());
			}
			finally {
				if (postVerifyCleanup != null) {
					postVerifyCleanup.dispose();
				}
			}
		}

		/**
		 * Signal a failure, always cancelling this subscriber. If the actual signal
		 * causing the failure might be either onComplete or onError (for which cancel
		 * is prohibited), prefer using {@link #setFailure(Event, Signal, String, Object...)}.
		 *
		 * @param event the event that triggered the failure
		 * @param msg the message for the error
		 * @param arguments the optional formatter arguments to the message
		 */
		final void setFailure(@Nullable Event<T> event, String msg, Object... arguments) {
			setFailure(event, null, msg, arguments);
		}

		/**
		 * Signal a failure, potentially cancelling this subscriber if the actual signal
		 * is neither onComplete nor onError (for which cancel is prohibited).
		 *
		 * @param event the event that triggered the failure
		 * @param actualSignal the actual signal that triggered the failure (used to
		 * decide whether or not to cancel, passing null will cause cancellation)
		 * @param msg the message for the error
		 * @param arguments the optional formatter arguments to the message
		 */
		final void setFailure(@Nullable Event<T> event, @Nullable Signal<T> actualSignal, String msg, Object... arguments) {
			Exceptions.addThrowable(ERRORS, this, messageFormatter.fail(event, msg, arguments));
			maybeCancel(actualSignal);
			this.completeLatch.countDown();
		}

		final void setFailurePrefix(String prefix, Signal<T> actualSignal, String msg, Object... arguments) {
			Exceptions.addThrowable(ERRORS, this, messageFormatter.failPrefix(prefix, msg, arguments));
			maybeCancel(actualSignal);
			this.completeLatch.countDown();
		}

		@Nullable
		final Subscription cancel() {
			Subscription s =
					this.getAndSet(Operators.cancelledSubscription());
			if (s != null && s != Operators.cancelledSubscription()) {
				s.cancel();
				if(establishedFusionMode == Fuseable.ASYNC) {
					qs.clear();
				}
			}
			return s;
		}

		/** Cancels this subscriber if the actual signal is null or not a complete/error */
		final void maybeCancel(@Nullable Signal<T> actualSignal) {
			if (actualSignal == null || (!actualSignal.isOnComplete() && !actualSignal.isOnError())) {
				cancel();
			}
		}

		final Optional<AssertionError> checkCountMismatch(SignalCountEvent<T> event, Signal<T> s) {
			long expected = event.count;
			if (!s.isOnNext()) {
				return messageFormatter.failOptional(event, "expected: count = %s; actual: counted = %s; signal: %s",
						expected,
						unasserted, s);
			}
			else {
				return Optional.empty();
			}
		}

		/** Returns true if the requested amount was overflown by the given signal */
		final boolean checkRequestOverflow(Signal<T> s) {
			long r = requested;
			if (!s.isOnNext()
					|| r < 0 || r == Long.MAX_VALUE //was Long.MAX from beginning or switched to unbounded
					|| (establishedFusionMode == Fuseable.ASYNC && r != 0L)
					|| r >= produced) {
				return false;
			}
			else {
				//not really an expectation failure so customize the message
				setFailurePrefix("request overflow (", s,
						"expected production of at most %s; produced: %s; request overflown by signal: %s", r, produced, s);
				return true;
			}
		}

		boolean onCollect(Signal<T> actualSignal) {
			Collection<T> c;
			CollectEvent<T> collectEvent = (CollectEvent<T>) this.script.poll();
			if (collectEvent.supplier != null) {
				c = collectEvent.get();
				this.currentCollector = c;

				if (c == null) {
					setFailure(collectEvent, actualSignal, "expected collection; actual supplied is [null]");
				}
				return true;
			}
			c = this.currentCollector;

			if (c == null) {
				setFailure(collectEvent, actualSignal, "expected record collector; actual record is [null]");
				return true;
			}

			Optional<AssertionError> error = collectEvent.test(c);
			if (error.isPresent()) {
				Exceptions.addThrowable(ERRORS, this, error.get());
				maybeCancel(actualSignal);
				this.completeLatch.countDown();
				return true;
			}
			return false;
		}

		@SuppressWarnings("unchecked")
		final void onExpectation(Signal<T> actualSignal) {
			if (monitorSignal) {
				setFailure(null, actualSignal, "expected no event: %s", actualSignal);
				return;
			}
			try {
				Event<T> event = this.script.peek();
				if (event == null) {
					waitTaskEvent();
					if (isCancelled()) {
						return;
					}
					setFailure(null, actualSignal, "did not expect: %s", actualSignal);
					return;
				}
				if (onTaskEvent()) {
					event = this.script.peek();
				}

				if (event instanceof DefaultStepVerifierBuilder.SignalConsumeWhileEvent) {
					if (consumeWhile(actualSignal, (SignalConsumeWhileEvent<T>) event)) {
						return;
					}
					//possibly re-evaluate the current onNext
					event = this.script.peek();
				}
				if (event instanceof SignalCountEvent) {
					if (onSignalCount(actualSignal, (SignalCountEvent<T>) event)) {
						return;
					}
				}
				else if (event instanceof CollectEvent) {
					if (onCollect(actualSignal)) {
						return;
					}
				}
				else if (event instanceof SignalSequenceEvent) {
					if (onSignalSequence(actualSignal, (SignalSequenceEvent<T>) event)) {
						return;
					}
				}
				else if (event instanceof SignalEvent) {
					if (onSignal(actualSignal)) {
						return;
					}
				}

				event = this.script.peek();

				for (; ; ) {
					if (event == null || !(event instanceof EagerEvent)) {
						break;
					}
					if (event instanceof SubscriptionEvent) {
						if (serializeDrainAndSubscriptionEvent()) {
							return;
						}
					}
					else if (event instanceof CollectEvent) {
						if (onCollect(actualSignal)) {
							return;
						}
					}
					else {
						onTaskEvent();
					}
					event = this.script.peek();
				}
			}
			catch (Throwable e) {
				Exceptions.throwIfFatal(e);
				if(e instanceof AssertionError){
					Exceptions.addThrowable(ERRORS, this, e);
				}
				else {
					String msg = e.getMessage() != null ? e.getMessage() : "";
					AssertionError wrapFailure = messageFormatter.failOptional(null,
							"failed running expectation on signal [%s] " + "with " + "[%s]:\n%s",
							actualSignal,
							Exceptions.unwrap(e)
							          .getClass()
							          .getName(),
							msg).get();
					wrapFailure.addSuppressed(e);
					Exceptions.addThrowable(ERRORS,
							this, wrapFailure);
				}
				maybeCancel(actualSignal);
				completeLatch.countDown();
			}
		}

		boolean onSignal(Signal<T> actualSignal) {
			SignalEvent<T> signalEvent = (SignalEvent<T>) this.script.poll();
			Optional<AssertionError> error = signalEvent.test(actualSignal);
			if (error.isPresent()) {
				Exceptions.addThrowable(ERRORS, this, error.get());
				// #55 ensure the onError is added as a suppressed to the AssertionError
				if(actualSignal.isOnError()) {
					error.get().addSuppressed(actualSignal.getThrowable());
				}
				maybeCancel(actualSignal);
				this.completeLatch.countDown();
				return true;
			}
			if (actualSignal.isOnNext()) {
				unasserted--;
			}
			return false;
		}

		boolean onSignalSequence(Signal<T> actualSignal,
				SignalSequenceEvent<T> sequenceEvent) {
			Iterator<? extends T> currentNextAs = this.currentNextAs;
			if (currentNextAs == null) {
				currentNextAs = sequenceEvent.iterable.iterator();
				this.currentNextAs = currentNextAs;
			}

			Optional<AssertionError> error =
					sequenceEvent.test(actualSignal, currentNextAs);

			if (error == EXPECT_MORE) {
				if (actualSignal.isOnNext()) {
					unasserted--;
				}
				return false;
			}
			if (!error.isPresent()) {
				this.currentNextAs = null;
				this.script.poll();
				if (actualSignal.isOnNext()) {
					unasserted--;
				}
			}
			else {
				Exceptions.addThrowable(ERRORS, this, error.get());
				if(actualSignal.isOnError()) {
					// #55 ensure the onError is added as a suppressed to the AssertionError
					error.get().addSuppressed(actualSignal.getThrowable());
				}
				maybeCancel(actualSignal);
				this.completeLatch.countDown();
				return true;
			}
			return false;
		}

		boolean consumeWhile(Signal<T> actualSignal, SignalConsumeWhileEvent<T> whileEvent) {
			if (actualSignal.isOnNext()) {
				if (whileEvent.test(actualSignal.get())) {
					//the value matches, gobble it up
					unasserted--;
					if (this.logger != null) {
						logger.debug("{} consumed {}", whileEvent.getDescription(), actualSignal);
					}
					return true;
				}
			}
			if (this.logger != null) {
				logger.debug("{} stopped at {}", whileEvent.getDescription(), actualSignal);
			}
			//stop evaluating the predicate
			this.script.poll();
			return false;
		}

		final boolean onSignalCount(Signal<T> actualSignal, SignalCountEvent<T> event) {
			if (unasserted >= event.count) {
				this.script.poll();
				unasserted -= event.count;
			}
			else {
				if (event.count != 0) {
					Optional<AssertionError> error =
							this.checkCountMismatch(event, actualSignal);

					if (error.isPresent()) {
						Exceptions.addThrowable(ERRORS, this, error.get());
						if(actualSignal.isOnError()) {
							// #55 ensure the onError is added as a suppressed to the AssertionError
							error.get().addSuppressed(actualSignal.getThrowable());
						}
						maybeCancel(actualSignal);
						this.completeLatch.countDown();
					}
				}
				return true;
			}
			return false;
		}

		boolean onTaskEvent() {
			Event<T> event;
			boolean foundTaskEvents = false;
			for (; ; ) {
				if (isCancelled()) {
					return foundTaskEvents;
				}
				event = this.script.peek();
				if (!(event instanceof TaskEvent)) {
					return foundTaskEvents;
				}
				event = this.script.poll();
				if (!(event instanceof TaskEvent)) {
					return foundTaskEvents;
				}
				taskEvents.add((TaskEvent<T>) event);
				foundTaskEvents = true;
			}
		}

		boolean onSubscriptionLoop(){
			SubscriptionEvent<T> subscriptionEvent;
			if (this.script.peek() instanceof SubscriptionEvent) {
				subscriptionEvent = (SubscriptionEvent<T>) this.script.poll();
				if (subscriptionEvent instanceof RequestEvent) {
					updateRequested(subscriptionEvent);
				}
				if (subscriptionEvent.isTerminal()) {
					doCancel();
					return true;
				}
				subscriptionEvent.consume(get());
			}
			return false;
		}

		boolean serializeDrainAndSubscriptionEvent() {
			int missed = WIP.incrementAndGet(this);
			if (missed != 1) {
				return true;
			}
			for (; ; ) {
				if(onSubscriptionLoop()){
					return true;
				}
				if(establishedFusionMode == Fuseable.ASYNC) {
					drainAsyncLoop();
				}
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
			return false;
		}

		void doCancel() {
			cancel();
			this.completeLatch.countDown();
		}

		void waitTaskEvent() {
			Event<T> event;
			while ((event = taskEvents.poll()) != null) {
				try {
					if (event instanceof SubscriptionTaskEvent) {
						updateRequested(event);
						((TaskEvent<T>) event).run(this);
						serializeDrainAndSubscriptionEvent();
					}
					else {
						((TaskEvent<T>) event).run(this);
					}
				}
				catch (Throwable t) {
					Exceptions.throwIfFatal(t);
					cancel();
					if (t instanceof AssertionError) {
						throw (AssertionError) t;
					}
					throw Exceptions.propagate(t);
				}
			}
		}

		@SuppressWarnings("unchecked")
		final void pollTaskEventOrComplete(Duration timeout) throws InterruptedException {
			Objects.requireNonNull(timeout, "timeout");
			Instant stop = Instant.now()
			                      .plus(timeout);

			for (; ; ) {
				waitTaskEvent();
				if (this.completeLatch.await(10, TimeUnit.NANOSECONDS)) {
					break;
				}
				if (timeout != Duration.ZERO && stop.isBefore(Instant.now())) {
					if (get() == null) {
						throw messageFormatter.error(IllegalStateException::new, "VerifySubscriber has not been subscribed");
					}
					else {
						throw messageFormatter.assertionError("VerifySubscriber timed out on " + get());
					}
				}
			}
		}

		private void updateRequested(Event<?> event) {
			RequestEvent requestEvent = null;
			if (event instanceof RequestEvent) requestEvent = (RequestEvent) event;
			else if (event instanceof SubscriptionTaskEvent) {
				SubscriptionTaskEvent ste = (SubscriptionTaskEvent) event;
				if (ste.delegate instanceof RequestEvent) {
					requestEvent = (RequestEvent) ste.delegate;
				}
			}

			if (requestEvent == null) {
				return;
			}
			else if (requestEvent.isBounded()) {
				Operators.addCap(REQUESTED, this, requestEvent.getRequestAmount());

			}
			else {
				REQUESTED.set(this, Long.MAX_VALUE);
			}
		}

		final void startFusion(Subscription s) {
			if (s instanceof Fuseable.QueueSubscription) {
				@SuppressWarnings("unchecked") Fuseable.QueueSubscription<T> qs =
						(Fuseable.QueueSubscription<T>) s;

				this.qs = qs;

				int m = qs.requestFusion(requestedFusionMode);
				if (expectedFusionMode == Fuseable.NONE && m != Fuseable.NONE) {
					setFailure(null,
							"expected no fusion; actual: %s",
							formatFusionMode(m));
					return;
				}
				if (expectedFusionMode != Fuseable.NONE && m == Fuseable.NONE) {
					setFailure(null,
							"expected fusion: %s; actual does not support " + "fusion",
							formatFusionMode(expectedFusionMode));
					return;
				}
				if ((m & expectedFusionMode) != m) {
					setFailure(null, "expected fusion mode: %s; actual: %s",
							formatFusionMode(expectedFusionMode),
							formatFusionMode(m));
					return;
				}

				this.establishedFusionMode = m;

				if (m == Fuseable.SYNC) {
					T v;
					for (; ; ) {
						if(get() == Operators.cancelledSubscription()){
							return;
						}
						try {
							v = qs.poll();
						}
						catch (Throwable e) {
							Exceptions.throwIfFatal(e);
							cancel();
							onError(Exceptions.unwrap(e));
							return;
						}
						if (v == null) {
							onComplete();
							break;
						}

						onNext(v);
					}
				}
				else if (initialRequest != 0) {
					s.request(initialRequest);
				}
			}
			else if (expectedFusionMode != Fuseable.NONE) {
				setFailure(null,
						"expected fuseable source but actual Subscription " + "is " +
								"not: %s",
						expectedFusionMode,
						s);
			}
			else if (initialRequest != 0L) {
				s.request(initialRequest);
			}
		}

		@SuppressWarnings("unchecked")
		final void validate() {
			if (get() == null) {
				throw messageFormatter.error(IllegalStateException::new, "VerifySubscriber has not been subscribed");
			}
			Throwable errors = this.errors;

			if (errors == null) {
				return;
			}

			if(errors instanceof AssertionError){
				throw (AssertionError)errors;
			}

			List<Throwable> flat = new ArrayList<>();
			flat.add(errors);
			flat.addAll(Arrays.asList(errors.getSuppressed()));

			StringBuilder messageBuilder = new StringBuilder("Expectation failure(s):\n");
			flat.stream()
			             .flatMap(error -> Stream.of(" - ", error, "\n"))
			             .forEach(messageBuilder::append);

			messageBuilder.delete(messageBuilder.length() - 1, messageBuilder.length());
			throw messageFormatter.assertionError(messageBuilder.toString(), errors);
		}

	}

	static class DefaultStepVerifierAssertions implements StepVerifier.Assertions {

		private final Duration         duration;
		private final MessageFormatter messageFormatter;
		private final HookRecorder     hookRecorder;

		DefaultStepVerifierAssertions(HookRecorder hookRecorder,
				Duration duration,
				MessageFormatter messageFormatter) {
			this.hookRecorder = hookRecorder;
			this.duration = duration;
			this.messageFormatter = messageFormatter;
		}

		private StepVerifier.Assertions satisfies(BooleanSupplier check, Supplier<String> message) {
			if (!check.getAsBoolean()) {
				throw messageFormatter.assertionError(message.get());
			}
			return this;
		}

		@Override
		public StepVerifier.Assertions hasDroppedElements() {
			return satisfies(hookRecorder::hasDroppedElements,
					() -> "Expected dropped elements, none found.");
		}

		@Override
		public StepVerifier.Assertions hasNotDroppedElements() {
			return satisfies(hookRecorder::noDroppedElements,
					() -> messageFormatter.format("Expected no dropped elements, found <%s>.", hookRecorder.droppedElements));
		}

		@Override
		public StepVerifier.Assertions hasDropped(Object... values) {
			//noinspection ConstantConditions
			satisfies(() -> values != null && values.length > 0, () -> "Require non-empty values");
			List<Object> valuesList = Arrays.asList(values);
			return satisfies(() -> hookRecorder.droppedAllOf(valuesList),
					() -> messageFormatter.format(
							"Expected dropped elements to contain <%s>, was <%s>.",
							valuesList, hookRecorder.droppedElements));
		}

		@Override
		public StepVerifier.Assertions hasDroppedExactly(Object... values) {
			//noinspection ConstantConditions
			satisfies(() -> values != null && values.length > 0, () -> "Require non-empty values");
			List<Object> valuesList = Arrays.asList(values);
			return satisfies(
					() -> hookRecorder.droppedAllOf(valuesList)
							&& hookRecorder.droppedElements.size() == valuesList.size(),
					() -> messageFormatter.format(
							"Expected dropped elements to contain exactly <%s>, was <%s>.",
							valuesList, hookRecorder.droppedElements));
		}

		@Override
		public StepVerifier.Assertions hasDiscardedElements() {
			return satisfies(hookRecorder::hasDiscardedElements,
					() -> "Expected discarded elements, none found.");
		}

		@Override
		public StepVerifier.Assertions hasNotDiscardedElements() {
			return satisfies(hookRecorder::noDiscardedElements,
					() -> messageFormatter.format("Expected no discarded elements, found <%s>.", hookRecorder.discardedElements));
		}

		@Override
		public StepVerifier.Assertions hasDiscarded(Object... values) {
			//noinspection ConstantConditions
			satisfies(() -> values != null && values.length > 0, () -> "Require non-empty values");
			List<Object> valuesList = Arrays.asList(values);
			return satisfies(() -> hookRecorder.discardedAllOf(valuesList),
					() -> messageFormatter.format(
							"Expected discarded elements to contain <%s>, was <%s>.",
							valuesList, hookRecorder.discardedElements));
		}

		@Override
		public StepVerifier.Assertions hasDiscardedExactly(Object... values) {
			//noinspection ConstantConditions
			satisfies(() -> values != null && values.length > 0, () -> "Require non-empty values");
			List<Object> valuesList = Arrays.asList(values);
			return satisfies(
					() -> hookRecorder.discardedAllOf(valuesList)
							&& hookRecorder.discardedElements.size() == valuesList.size(),
					() -> messageFormatter.format(
							"Expected discarded elements to contain exactly <%s>, was <%s>.",
							valuesList, hookRecorder.discardedElements));
		}

		@Override
		public StepVerifier.Assertions hasDiscardedElementsMatching(Predicate<Collection<Object>> matcher) {
			//noinspection ConstantConditions
			satisfies(() -> matcher != null, () -> "Require non-null matcher");
			hasDiscardedElements();
			return satisfies(() -> matcher.test(hookRecorder.discardedElements),
					() -> String.format(
							"Expected collection of discarded elements matching the given predicate, did not match: <%s>.",
							hookRecorder.discardedElements));
		}

		@Override
		public StepVerifier.Assertions hasDiscardedElementsSatisfying(Consumer<Collection<Object>> asserter) {
			//noinspection ConstantConditions
			satisfies(() -> asserter != null, () -> "Require non-null asserter");
			hasDiscardedElements();
			asserter.accept(hookRecorder.discardedElements);
			return this;
		}

		@Override
		public StepVerifier.Assertions hasNotDroppedErrors() {
			return satisfies(hookRecorder::noDroppedErrors,
					() -> String.format("Expected no dropped errors, found <%s>.",
							hookRecorder.droppedErrors));
		}

		@Override
		public StepVerifier.Assertions hasDroppedErrors() {
			return satisfies(hookRecorder::hasDroppedErrors,
					() -> "Expected at least 1 dropped error, none found.");
		}

		@Override
		public StepVerifier.Assertions hasDroppedErrors(int size) {
			return satisfies(() -> hookRecorder.droppedErrors.size() == size,
					() -> String.format("Expected exactly %d dropped errors, %d found.",
							size, hookRecorder.droppedErrors.size()));
		}

		@Override
		public StepVerifier.Assertions hasDroppedErrorOfType(Class<? extends Throwable> clazz) {
			//noinspection ConstantConditions
			satisfies(() -> clazz != null, () -> "Require non-null clazz");
			hasDroppedErrors(1);
			return satisfies(
					() -> clazz.isInstance(hookRecorder.droppedErrors.peek()),
					() -> String.format("Expected dropped error to be of type %s, was %s.",
							clazz.getCanonicalName(),
							hookRecorder.droppedErrors.peek().getClass().getCanonicalName()));
		}

		@Override
		public StepVerifier.Assertions hasDroppedErrorMatching(Predicate<Throwable> matcher) {
			//noinspection ConstantConditions
			satisfies(() -> matcher != null, () -> "Require non-null matcher");
			hasDroppedErrors(1);
			return satisfies(() -> matcher.test(hookRecorder.droppedErrors.peek()),
					() -> String.format(
							"Expected dropped error matching the given predicate, did not match: <%s>.",
							hookRecorder.droppedErrors.peek()));
		}

		@Override
		public StepVerifier.Assertions hasDroppedErrorWithMessage(String message) {
			//noinspection ConstantConditions
			satisfies(() -> message != null, () -> "Require non-null message");
			hasDroppedErrors(1);
			String actual = hookRecorder.droppedErrors.peek().getMessage();
			return satisfies(() -> message.equals(actual),
					() -> String.format("Expected dropped error with message <\"%s\">, was <\"%s\">.", message, actual));
		}

		@Override
		public StepVerifier.Assertions hasDroppedErrorWithMessageContaining(
				String messagePart) {
			//noinspection ConstantConditions
			satisfies(() -> messagePart != null, () -> "Require non-null messagePart");
			hasDroppedErrors(1);
			String actual = hookRecorder.droppedErrors.peek().getMessage();
			return satisfies(() -> actual != null && actual.contains(messagePart),
					() -> String.format("Expected dropped error with message containing <\"%s\">, was <\"%s\">.", messagePart, actual));
		}

		@Override
		public StepVerifier.Assertions hasDroppedErrorsMatching(Predicate<Collection<Throwable>> matcher) {
			//noinspection ConstantConditions
			satisfies(() -> matcher != null, () -> "Require non-null matcher");
			hasDroppedErrors();
			return satisfies(() -> matcher.test(hookRecorder.droppedErrors),
					() -> String.format(
							"Expected collection of dropped errors matching the given predicate, did not match: <%s>.",
							hookRecorder.droppedErrors));
		}

		@Override
		public StepVerifier.Assertions hasDroppedErrorsSatisfying(Consumer<Collection<Throwable>> asserter) {
			//noinspection ConstantConditions
			satisfies(() -> asserter != null, () -> "Require non-null asserter");
			hasDroppedErrors();
			asserter.accept(hookRecorder.droppedErrors);
			return this;
		}

		@Override
		public StepVerifier.Assertions hasOperatorErrors() {
			return satisfies(hookRecorder::hasOperatorErrors,
					() -> "Expected at least 1 operator error, none found.");
		}
		@Override
		public StepVerifier.Assertions hasOperatorErrors(int size) {
			return satisfies(() -> hookRecorder.operatorErrors.size() == size,
					() -> String.format(
							"Expected exactly %d operator errors, %d found.",
							size, hookRecorder.operatorErrors.size()));
		}

		StepVerifier.Assertions hasOneOperatorErrorWithError() {
			satisfies(() -> hookRecorder.operatorErrors.size() == 1,
					() -> String.format("Expected exactly one operator error, %d found.", hookRecorder.operatorErrors.size()));
			satisfies(() -> hookRecorder.operatorErrors.peek().getT1().isPresent(),
					() -> "Expected exactly one operator error with an actual throwable content, no throwable found.");
			return this;
		}

		@Override
		public StepVerifier.Assertions hasOperatorErrorOfType(Class<? extends Throwable> clazz) {
			//noinspection ConstantConditions
			satisfies(() -> clazz != null, () -> "Require non-null clazz");
			hasOneOperatorErrorWithError();
			return satisfies(
					() -> clazz.isInstance(hookRecorder.operatorErrors.peek().getT1().get()),
					() -> String.format("Expected operator error to be of type %s, was %s.",
							clazz.getCanonicalName(),
							hookRecorder.operatorErrors.peek().getT1().get().getClass().getCanonicalName()));
		}

		@Override
		public StepVerifier.Assertions hasOperatorErrorMatching(Predicate<Throwable> matcher) {
			//noinspection ConstantConditions
			satisfies(() -> matcher != null, () -> "Require non-null matcher");
			hasOneOperatorErrorWithError();
			return satisfies(
					() -> matcher.test(hookRecorder.operatorErrors.peek().getT1().orElse(null)),
					() -> String.format(
							"Expected operator error matching the given predicate, did not match: <%s>.",
							hookRecorder.operatorErrors.peek()));
		}

		@Override
		public StepVerifier.Assertions hasOperatorErrorWithMessage(String message) {
			//noinspection ConstantConditions
			satisfies(() -> message != null, () -> "Require non-null message");
			hasOneOperatorErrorWithError();
			String actual = hookRecorder.operatorErrors.peek().getT1().get().getMessage();
			return satisfies(() -> message.equals(actual),
					() -> String.format("Expected operator error with message <\"%s\">, was <\"%s\">.", message, actual));
		}

		@Override
		public StepVerifier.Assertions hasOperatorErrorWithMessageContaining(
				String messagePart) {
			//noinspection ConstantConditions
			satisfies(() -> messagePart != null, () -> "Require non-null messagePart");
			hasOneOperatorErrorWithError();
			String actual = hookRecorder.operatorErrors.peek().getT1().get().getMessage();
			return satisfies(() -> actual != null && actual.contains(messagePart),
					() -> String.format("Expected operator error with message containing <\"%s\">, was <\"%s\">.", messagePart, actual));
		}

		@Override
		public StepVerifier.Assertions hasOperatorErrorsMatching(Predicate<Collection<Tuple2<Optional<Throwable>, Optional<?>>>> matcher) {
			//noinspection ConstantConditions
			satisfies(() -> matcher != null, () -> "Require non-null matcher");
			hasOperatorErrors();
			return satisfies(() -> matcher.test(hookRecorder.operatorErrors),
					() -> String.format(
							"Expected collection of operator errors matching the given predicate, did not match: <%s>.",
							hookRecorder.operatorErrors));
		}

		@Override
		public StepVerifier.Assertions hasOperatorErrorsSatisfying(Consumer<Collection<Tuple2<Optional<Throwable>, Optional<?>>>> asserter) {
			//noinspection ConstantConditions
			satisfies(() -> asserter != null, () -> "Require non-null asserter");
			hasOperatorErrors();
			asserter.accept(hookRecorder.operatorErrors);
			return this;
		}

		@Override
		public StepVerifier.Assertions tookLessThan(Duration d) {
			return satisfies(() -> duration.compareTo(d) <= 0,
					() -> String.format("Expected scenario to be verified in less than %sms, took %sms.",
							d.toMillis(), duration.toMillis()));
		}

		@Override
		public StepVerifier.Assertions tookMoreThan(Duration d) {
			return satisfies(() -> duration.compareTo(d) >= 0,
					() -> String.format("Expected scenario to be verified in more than %sms, took %sms.",
							d.toMillis(), duration.toMillis()));
		}
	}



	interface EagerEvent<T> extends Event<T> {

	}

	static abstract class AbstractEagerEvent<T> implements EagerEvent<T> {

		String description = "";

		public AbstractEagerEvent(String description) {
			this.description = description;
		}

		public boolean setDescription(String description) {
			this.description = description;
			return true;
		}

		public String getDescription() {
			return description;
		}

		@Override
		public String toString() {
			return description + "_" + getClass().getSimpleName();
		}
	}

	static class SubscriptionEvent<T> extends AbstractEagerEvent<T> {

		final Consumer<Subscription> consumer;

		SubscriptionEvent(String desc) {
			this(null, desc);
		}

		SubscriptionEvent(@Nullable Consumer<Subscription> consumer, String desc) {
			super(desc);
			this.consumer = consumer;
		}

		void consume(Subscription subscription) {
			if (consumer != null) {
				this.consumer.accept(subscription);
			}
		}

		boolean isTerminal() {
			return consumer == null;
		}
	}

	static final class RequestEvent<T> extends SubscriptionEvent<T> {

		final long requestAmount;

		RequestEvent(long n, String desc) {
			super(s -> s.request(n), desc);
			this.requestAmount = n;
		}

		public long getRequestAmount() {
			return requestAmount;
		}

		public boolean isBounded() {
			return requestAmount >= 0 && requestAmount < Long.MAX_VALUE;
		}
	}

	static abstract class AbstractSignalEvent<T> implements Event<T> {

		String description;

		public AbstractSignalEvent(String description) {
			this.description = description;
		}

		public boolean setDescription(String description) {
			this.description = description;
			return true;
		}

		public String getDescription() {
			return description;
		}

		@Override
		public String toString() {
			return description + "_" + getClass().getSimpleName();
		}
	}

	static final class SignalEvent<T> extends AbstractSignalEvent<T> {

		final BiFunction<Signal<T>, SignalEvent<T>, Optional<AssertionError>> function;

		SignalEvent(BiFunction<Signal<T>, SignalEvent<T>, Optional<AssertionError>> function,
				String desc) {
			super(desc);
			this.function = function;
		}

		Optional<AssertionError> test(Signal<T> signal) {
			return this.function.apply(signal, this);
		}

	}

	static final class SignalCountEvent<T> extends AbstractSignalEvent<T> {

		final long count;

		SignalCountEvent(long count, String desc) {
			super(desc);
			this.count = count;
		}

	}

	static final class CollectEvent<T> extends AbstractEagerEvent<T> {

		final MessageFormatter messageFormatter;

		final Supplier<? extends Collection<T>> supplier;

		final Predicate<? super Collection<T>>  predicate;

		final Consumer<? super Collection<T>>   consumer;

		CollectEvent(Supplier<? extends Collection<T>> supplier, MessageFormatter messageFormatter, String desc) {
			super(desc);
			this.messageFormatter = messageFormatter;
			this.supplier = supplier;
			this.predicate = null;
			this.consumer = null;
		}

		CollectEvent(Consumer<? super Collection<T>> consumer, MessageFormatter messageFormatter, String desc) {
			super(desc);
			this.messageFormatter = messageFormatter;
			this.supplier = null;
			this.predicate = null;
			this.consumer = consumer;
		}

		CollectEvent(Predicate<? super Collection<T>> predicate, MessageFormatter messageFormatter, String desc) {
			super(desc);
			this.messageFormatter = messageFormatter;
			this.supplier = null;
			this.predicate = predicate;
			this.consumer = null;
		}

		@Nullable
		Collection<T> get() {
			return supplier != null ? supplier.get() : null;
		}

		Optional<AssertionError> test(Collection<T> collection) {
			if (predicate != null) {
				if (!predicate.test(collection)) {
					return messageFormatter.failOptional(this, "expected collection predicate match; actual: %s",
							collection);
				}
				else {
					return Optional.empty();
				}
			}
			else if (consumer != null) {
				consumer.accept(collection);
			}
			return Optional.empty();
		}

	}

	static class TaskEvent<T> extends AbstractEagerEvent<T> {

		final Runnable task;

		TaskEvent(@Nullable Runnable task, String desc) {
			super(desc);
			this.task = task;
		}

		void run(DefaultVerifySubscriber<T> parent) throws Exception {
			if (task != null) {
				task.run();
			}
		}
	}

	static final class SubscriptionConsumerEvent<T> extends TaskEvent<T> {

		final Consumer<? super Subscription> task;

		SubscriptionConsumerEvent(Consumer<? super Subscription> task, String desc) {
			super(null, desc);
			this.task = task;
		}

		@Override
		void run(DefaultVerifySubscriber<T> parent) throws Exception {
			task.accept(parent.get());
		}
	}

	static void virtualOrRealWait(Duration duration, DefaultVerifySubscriber<?> s)
			throws Exception {
		if (s.virtualTimeScheduler == null) {
			s.completeLatch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
		}
		else {
			s.virtualTimeScheduler.advanceTimeBy(duration);
		}
	}

	static final class NoEvent<T> extends TaskEvent<T> {

		final Duration duration;

		NoEvent(Duration duration, String desc) {
			super(null, desc);
			this.duration = duration;
		}

		@Override
		void run(DefaultVerifySubscriber<T> parent) throws Exception {
			if(parent.virtualTimeScheduler != null) {
				parent.monitorSignal = true;
				virtualOrRealWait(duration.minus(Duration.ofNanos(1)), parent);
				parent.monitorSignal = false;
				if (parent.terminalError != null && !parent.isCancelled()) {
					Throwable terminalError = parent.terminalError.getThrowable();
					throw parent.messageFormatter.assertionError("Unexpected error during a no-event expectation: " + terminalError, terminalError);
				}
				else if (parent.isTerminated() && !parent.isCancelled()) {
					throw parent.messageFormatter.assertionError("Unexpected completion during a no-event expectation");
				}
				virtualOrRealWait(Duration.ofNanos(1), parent);
			}
			else{
				parent.monitorSignal = true;
				virtualOrRealWait(duration, parent);
				parent.monitorSignal = false;
				if (parent.terminalError != null && !parent.isCancelled()) {
					Throwable terminalError = parent.terminalError.getThrowable();
					throw parent.messageFormatter.assertionError("Unexpected error during a no-event expectation: " + terminalError, terminalError);
				}
				else if (parent.isTerminated() && !parent.isCancelled()) {
					throw parent.messageFormatter.assertionError("Unexpected completion during a no-event expectation");
				}
			}
		}
	}

	static final class WaitEvent<T> extends TaskEvent<T> {

		final Duration duration;

		WaitEvent(Duration duration, String desc) {
			super(null, desc);
			this.duration = duration;
		}

		@Override
		void run(DefaultVerifySubscriber<T> s) throws Exception {
			virtualOrRealWait(duration, s);
		}

	}

	/**
	 * A lazy cancellation task that will only trigger cancellation after all previous
	 * tasks have been processed (avoiding short-circuiting of time manipulating tasks).
	 */
	static final class SubscriptionTaskEvent<T> extends TaskEvent<T> {

		final SubscriptionEvent<T> delegate;

		SubscriptionTaskEvent(SubscriptionEvent<T> subscriptionEvent) {
			super(null, subscriptionEvent.getDescription());
			this.delegate = subscriptionEvent;
		}

		@Override
		void run(DefaultVerifySubscriber<T> parent) throws Exception {
			if (delegate.isTerminal()) {
				parent.doCancel();
			} else {
				delegate.consume(parent.get());
			}
		}
	}

	static final class SignalSequenceEvent<T> extends AbstractSignalEvent<T> {

		final Iterable<? extends T> iterable;
		final MessageFormatter      messageFormatter;

		SignalSequenceEvent(Iterable<? extends T> iterable, MessageFormatter messageFormatter, String desc) {
			super(desc);
			this.iterable = iterable;
			this.messageFormatter = messageFormatter;
		}

		Optional<AssertionError> test(Signal<T> signal, Iterator<? extends T> iterator) {
			if (signal.isOnNext()) {
				if (!iterator.hasNext()) {
					return Optional.empty();
				}
				T d2 = iterator.next();
				if (!Objects.equals(signal.get(), d2)) {
					return messageFormatter.failOptional(this, "expected : onNext(%s); actual: %s; iterable: %s",
							d2,
							signal.get(),
							iterable);
				}
				return iterator.hasNext() ? EXPECT_MORE : Optional.empty();

			}
			if (iterator.hasNext() || signal.isOnError()) {
				return messageFormatter.failOptional(this, "expected next value: %s; actual signal: %s; iterable: %s",
						iterator.hasNext() ? iterator.next() : "none",
						signal, iterable);
			}
			return Optional.empty();
		}
	}

	static final class SignalConsumeWhileEvent<T> extends AbstractSignalEvent<T> {

		private final Predicate<T> predicate;
		private final Consumer<T>  consumer;

		SignalConsumeWhileEvent(Predicate<T> predicate, Consumer<T> consumer, String desc) {
			super(desc);
			this.predicate = predicate;
			this.consumer = consumer;
		}

		boolean test(T actual) {
			if (predicate.test(actual)) {
				consumer.accept(actual);
				return true;
			}
			return false;
		}
	}

	static final class DescriptionEvent<T> implements Event<T> {

		final String description;

		public DescriptionEvent(String description) {
			this.description = description;
		}

		@Override
		public boolean setDescription(String description) {
			//NO OP
			return false;
		}

		@Override
		public String getDescription() {
			return description;
		}
	}

	static String formatFusionMode(int m) {
		switch (m) {
			case Fuseable.ANY:
				return "(any)";
			case Fuseable.SYNC:
				return "(sync)";
			case Fuseable.ASYNC:
				return "(async)";
			case Fuseable.NONE:
				return "none";
			case Fuseable.THREAD_BARRIER:
				return "(thread-barrier)";
		}
		return "" + m;
	}

	static <T> SignalEvent<T> newOnSubscribeStep(MessageFormatter messageFormatter, String desc){
		return new SignalEvent<>((signal, se) -> {
			if (!signal.isOnSubscribe()) {
				return messageFormatter.failOptional(se, "expected: onSubscribe(); actual: %s", signal);
			}
			else {
				return Optional.empty();
			}
		}, desc);
	}

	static final class DefaultContextExpectations<T>
			implements StepVerifier.ContextExpectations<T> {

		private final MessageFormatter             messageFormatter;
		private final StepVerifier.Step<T>         step;
		private       Consumer<CoreSubscriber<?>>  contextExpectations;

		DefaultContextExpectations(StepVerifier.Step<T> step, MessageFormatter messageFormatter) {
			this.messageFormatter = messageFormatter;
			this.step = step;
			this.contextExpectations = c -> {
				if (c == null) throw messageFormatter.assertionError("No propagated Context");
			};
		}

		@Override
		public StepVerifier.Step<T> then() {
			return step.consumeSubscriptionWith(s -> {
				//the current subscription
				Scannable lowest = Scannable.from(s);

				//attempt to go back to the leftmost parent to check the Context from its perspective
				CoreSubscriber<?> subscriber = Flux.<Scannable>
						fromStream(lowest.parents())
						.ofType(CoreSubscriber.class)
						.takeLast(1)
						.singleOrEmpty()
						//no parent? must be a ScalaSubscription or similar source
						.switchIfEmpty(
								Mono.just(lowest)
								    //unless it is directly a CoreSubscriber, let's try to scan the leftmost, see if it has an ACTUAL
								    .map(sc -> (sc instanceof CoreSubscriber) ?
										    sc :
										    sc.scanOrDefault(Scannable.Attr.ACTUAL, Scannable.from(null)))
								    //we are ultimately only interested in CoreSubscribers' Context
								    .ofType(CoreSubscriber.class)
						)
						//if it wasn't a CoreSubscriber (eg. custom or vanilla Subscriber) there won't be a Context
						.block();

				this.contextExpectations.accept(subscriber);
			});
		}

		@Override
		public StepVerifier.ContextExpectations<T> hasKey(Object key) {
			this.contextExpectations = this.contextExpectations.andThen(subscriber -> {
				Context c = subscriber.currentContext();
				if (!c.hasKey(key))
						throw messageFormatter.assertionError(formatErrorMessage(subscriber, "Key %s not found", key));
			});
			return this;
		}

		static String formatErrorMessage(CoreSubscriber<?> subscriber, String format, Object... args) {
			Scannable scannable = Scannable.from(subscriber);
			final String name;
			// OnAssembly covers both OnAssemblySubscriber and OnAssemblyConditionalSubscriber
			if (subscriber.getClass().getName().contains("OnAssembly")) {
				name = scannable.name();
			}
			else {
				name = scannable.scanOrDefault(Scannable.Attr.PARENT, scannable).name();
			}

			return String.format(
					"%s\nContext: %s\nCaptured at: %s",
					String.format(format, args),
					subscriber.currentContext(),
					name
			);
		}

		@Override
		public StepVerifier.ContextExpectations<T> hasSize(int size) {
			this.contextExpectations = this.contextExpectations.andThen(subscriber -> {
				Context c = subscriber.currentContext();
				long realSize = c.stream().count();
				if (realSize != size)
					throw messageFormatter.assertionError(
							formatErrorMessage(subscriber, "Expected Context of size %d, got %d", size, realSize));
			});
			return this;
		}

		@Override
		public StepVerifier.ContextExpectations<T> contains(Object key, Object value) {
			this.contextExpectations = this.contextExpectations.andThen(subscriber -> {
				Context c = subscriber.currentContext();
				Object realValue = c.getOrDefault(key, null);
				if (realValue == null)
					throw messageFormatter.assertionError(
							formatErrorMessage(subscriber, "Expected value %s for key %s, key not present", value, key));

				if (!value.equals(realValue))
					throw messageFormatter.assertionError(
							formatErrorMessage(subscriber, "Expected value %s for key %s, got %s", value, key, realValue));
			});
			return this;
		}

		@Override
		public StepVerifier.ContextExpectations<T> containsAllOf(Context other) {
			this.contextExpectations = this.contextExpectations.andThen(subscriber -> {
				Context c = subscriber.currentContext();
				boolean all = other.stream().allMatch(e -> e.getValue().equals(c.getOrDefault(e.getKey(), null)));
				if (!all) {
					throw messageFormatter.assertionError(formatErrorMessage(subscriber, "Expected Context to contain all of %s", other));
				}
			});
			return this;
		}

		@Override
		public StepVerifier.ContextExpectations<T> containsAllOf(Map<?, ?> other) {
			this.contextExpectations = this.contextExpectations.andThen(subscriber -> {
				Context c = subscriber.currentContext();
				boolean all = other.entrySet()
				                   .stream()
				                   .allMatch(e -> e.getValue().equals(c.getOrDefault(e.getKey(), null)));
				if (!all) {
					throw messageFormatter.assertionError(formatErrorMessage(subscriber, "Expected Context to contain all of %s", other));
				}
			});
			return this;
		}

		@Override
		public StepVerifier.ContextExpectations<T> containsOnly(Context other) {
			this.contextExpectations = this.contextExpectations.andThen(subscriber -> {
				Context c = subscriber.currentContext();
				if (c.stream().count() != other.stream().count()) {
					throw messageFormatter.assertionError(
							formatErrorMessage(subscriber, "Expected Context to contain same values as %s, but they differ in size", other));
				}
				boolean all = other.stream()
				                   .allMatch(e -> e.getValue().equals(c.getOrDefault(e.getKey(), null)));
				if (!all) {
					throw messageFormatter.assertionError(
							formatErrorMessage(subscriber, "Expected Context to contain same values as %s, but they differ in content", other));
				}
			});
			return this;
		}

		@Override
		public StepVerifier.ContextExpectations<T> containsOnly(Map<?, ?> other) {
			this.contextExpectations = this.contextExpectations.andThen(subscriber -> {
				Context c = subscriber.currentContext();
				if (c.stream().count() != other.size()) {
					throw messageFormatter.assertionError(
							formatErrorMessage(subscriber, "Expected Context to contain same values as %s, but they differ in size", other));
				}
				boolean all = other.entrySet()
				                   .stream()
				                   .allMatch(e -> e.getValue().equals(c.getOrDefault(e.getKey(), null)));
				if (!all) {
					throw messageFormatter.assertionError(
							formatErrorMessage(subscriber, "Expected Context to contain same values as %s, but they differ in content", other));
				}
			});
			return this;
		}

		@Override
		public StepVerifier.ContextExpectations<T> assertThat(Consumer<Context> assertingConsumer) {
			this.contextExpectations = this.contextExpectations.andThen(subscriber -> {
				assertingConsumer.accept(subscriber != null ? subscriber.currentContext() : null);
			});
			return this;
		}

		@Override
		public StepVerifier.ContextExpectations<T> matches(Predicate<Context> predicate) {
			this.contextExpectations = this.contextExpectations.andThen(subscriber -> {
				Context c = subscriber.currentContext();
				if (!predicate.test(c)) {
					throw messageFormatter.assertionError(
							formatErrorMessage(subscriber, "Context doesn't match predicate"));
				}
			});
			return this;
		}

		@Override
		public StepVerifier.ContextExpectations<T> matches(Predicate<Context> predicate, String description) {
			this.contextExpectations = this.contextExpectations.andThen(subscriber -> {
				Context c = subscriber.currentContext();
				if (!predicate.test(c)) {
					throw messageFormatter.assertionError(
							formatErrorMessage(subscriber, "Context doesn't match predicate %s", description));
				}
			});
			return this;
		}
	}

	static final AtomicReferenceFieldUpdater<DefaultVerifySubscriber, Throwable>
			ERRORS =
			AtomicReferenceFieldUpdater.newUpdater(DefaultVerifySubscriber.class,
					Throwable.class,
					"errors");

	static final AtomicIntegerFieldUpdater<DefaultVerifySubscriber> WIP =
			AtomicIntegerFieldUpdater.newUpdater(DefaultVerifySubscriber.class, "wip");

	static final Optional<AssertionError> EXPECT_MORE = Optional.of(new AssertionError("EXPECT MORE"));

}
