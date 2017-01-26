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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.Logger;
import reactor.util.Loggers;
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

	@SuppressWarnings("unchecked")
	static <T> SignalEvent<T> defaultFirstStep() {
		return (SignalEvent<T>) DEFAULT_ONSUBSCRIBE_STEP;
	}

	final List<Event<T>>                             script;
	final long                                       initialRequest;
	final Supplier<? extends VirtualTimeScheduler>   vtsLookup;
	final Supplier<? extends Publisher<? extends T>> sourceSupplier;
	private final StepVerifierOptions options;

	long hangCheckRequested;
	int  requestedFusionMode = -1;
	int  expectedFusionMode  = -1;

	DefaultStepVerifierBuilder(StepVerifierOptions options,
			Supplier<? extends Publisher<? extends T>> sourceSupplier) {
		this.initialRequest = options.getInitialRequest();
		this.options = options;
		this.vtsLookup = options.getVirtualTimeSchedulerSupplier();
		this.sourceSupplier = sourceSupplier;
		this.script = new ArrayList<>();
		this.script.add(defaultFirstStep());

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
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnError()) {
				return fail(se, "expected: onError(); actual: %s", signal);
			}
			else {
				consumer.accept(signal.getThrowable());
				return Optional.empty();
			}
		}, "consumeErrorWith");
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
				return fail(se, "expected: onNext(); actual: %s", signal);
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
		this.script.add(new CollectEvent<>(consumer, "consumeRecordedWith"));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> consumeSubscriptionWith(
			Consumer<? super Subscription> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		if(script.isEmpty() || (script.size() == 1 && script.get(0) == DEFAULT_ONSUBSCRIBE_STEP)) {
			this.script.set(0, new SignalEvent<>((signal, se) -> {
				if (!signal.isOnSubscribe()) {
					return fail(se, "expected: onSubscribe(); actual: %s", signal);
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
	public DefaultStepVerifier<T> expectComplete() {
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnComplete()) {
				return fail(se, "expected: onComplete(); actual: %s", signal);
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
				return fail(se, "expected: onError(); actual: %s", signal);
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
				return fail(se, "expected: onError(%s); actual: %s",
						clazz.getSimpleName(), signal);
			}
			else if (!clazz.isInstance(signal.getThrowable())) {
				return fail(se, "expected error of type: %s; actual type: %s",
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
				return fail(se, "expected: onError(\"%s\"); actual: %s",
						errorMessage, signal);
			}
			else if (!Objects.equals(errorMessage,
					signal.getThrowable()
					      .getMessage())) {
				return fail(se, "expected error message: \"%s\"; " + "actual " + "message: %s",
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
				return fail(se, "expected: onError(); actual: %s", signal);
			}
			else if (!predicate.test(signal.getThrowable())) {
				return fail(se, "predicate failed on exception: %s", signal.getThrowable());
			}
			else {
				return Optional.empty();
			}
		}, "expectErrorMatches");
		this.script.add(event);
		return build();
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
	@SafeVarargs
	public final DefaultStepVerifierBuilder<T> expectNext(T... ts) {
		Objects.requireNonNull(ts, "ts");
		SignalEvent<T> event;
		for (T t : ts) {
			String desc = String.format("expectNext(%s)", t);
			checkPotentialHang(1, desc);
			event = new SignalEvent<>((signal, se) -> {
				if (!signal.isOnNext()) {
					return fail(se, "expected: onNext(%s); actual: %s", t, signal);
				}
				else if (!Objects.equals(t, signal.get())) {
					return fail(se, "expected value: %s; actual value: %s", t, signal.get());

				}
				else {
					return Optional.empty();
				}
			}, desc);
			this.script.add(event);
		}
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNextSequence(
			Iterable<? extends T> iterable) {
		Objects.requireNonNull(iterable, "iterable");
		if (iterable instanceof Collection) {
			checkPotentialHang(((Collection) iterable).size(), "expectNextSequence");
		}
		else {
			//best effort
			checkPotentialHang(-1, "expectNextSequence");
		}
		this.script.add(new SignalSequenceEvent<>(iterable, "expectNextSequence"));

		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNextCount(long count) {
		checkPositive(count);
		String desc = "expectNextCount(" + count + ")";
		checkPotentialHang(count, desc);
		this.script.add(new SignalCountEvent<>(count, desc));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNextMatches(
			Predicate<? super T> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		checkPotentialHang(1, "expectNextMatches");
		SignalEvent<T> event = new SignalEvent<>((signal, se) -> {
			if (!signal.isOnNext()) {
				return fail(se, "expected: onNext(); actual: %s", signal);
			}
			else if (!predicate.test(signal.get())) {
				return fail(se, "predicate failed on value: %s", signal.get());
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
		this.script.add(new CollectEvent<>(predicate, "expectRecordedMatches"));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectSubscription() {
		if(this.script.get(0) instanceof NoEvent) {
			this.script.add(defaultFirstStep());
		}
		else{
			this.script.set(0, newOnSubscribeStep("expectSubscription"));
		}
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectSubscriptionMatches(
			Predicate<? super Subscription> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		this.script.set(0, new SignalEvent<>((signal, se) -> {
			if (!signal.isOnSubscribe()) {
				return fail(se, "expected: onSubscribe(); actual: %s", signal);
			}
			else if (!predicate.test(signal.getSubscription())) {
				return fail(se, "predicate failed on subscription: %s",
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
		if(this.script.size() == 1 && this.script.get(0) == defaultFirstStep()){
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
		this.script.add(new CollectEvent<>(supplier, "recordWith"));
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
	public Duration verifyComplete() {
		return expectComplete().verify();
	}

	@Override
	public DefaultStepVerifierBuilder<T> thenRequest(long n) {
		checkStrictlyPositive(n);
		this.script.add(new RequestEvent<T>(n, "thenRequest"));
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
			throw new IllegalArgumentException(message.toString());
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
		public StepVerifierAssertions verifyThenAssertThat() {
			//plug in the correct hooks
			Queue<Object> droppedElements = new ConcurrentLinkedQueue<>();
			Queue<Throwable> droppedErrors = new ConcurrentLinkedQueue<>();
			Queue<Tuple2<Throwable, ?>> operatorErrors = new ConcurrentLinkedQueue<>();
			Hooks.onErrorDropped(droppedErrors::offer);
			Hooks.onNextDropped(droppedElements::offer);
			Hooks.onOperatorError((t, d) -> {
				operatorErrors.offer(Tuples.of(t, d));
				return t;
			});

			try {
				//trigger the verify
				Duration time = verify();

				//return the assertion API
				return new DefaultStepVerifierAssertions(droppedElements, droppedErrors, operatorErrors, time);
			}
			finally {
				//unplug the hooks
				Hooks.resetOnNextDropped();
				Hooks.resetOnErrorDropped();
				Hooks.resetOnOperatorError();
			}
		}

		@Override
		public Duration verify() {
			return verify(Duration.ZERO);
		}

		@Override
		public Duration verify(Duration duration) {
			Objects.requireNonNull(duration, "duration");
			if (parent.sourceSupplier != null) {
				VirtualTimeScheduler vts = null;
				if (parent.vtsLookup != null) {
					vts = parent.vtsLookup.get();
					//this works even for the default case where StepVerifier has created
					// a vts through enable(false), because the CURRENT will already be that vts
					VirtualTimeScheduler.set(vts);
				}
				try {
					Publisher<? extends T> publisher = parent.sourceSupplier.get();
					Instant now = Instant.now();

					DefaultVerifySubscriber<T> newVerifier = new DefaultVerifySubscriber<>(
							this.parent.script,
							this.parent.initialRequest,
							this.requestedFusionMode,
							this.expectedFusionMode,
							this.debugEnabled,
							vts);

					publisher.subscribe(newVerifier);
					newVerifier.verify(duration);

					return Duration.between(now, Instant.now());
				}
				finally {
					if (vts != null) {
						vts.shutdown();
						//explicitly reset the factory, rather than rely on vts shutdown doing so
						// because it could have been eagerly shut down in a test.
						VirtualTimeScheduler.reset();
					}
				}
			} else {
				return toSubscriber().verify(duration);
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
					this.parent.initialRequest,
					this.requestedFusionMode,
					this.expectedFusionMode,
					this.debugEnabled,
					vts);
		}

	}

	final static class DefaultVerifySubscriber<T>
			implements StepVerifier, Subscriber<T>, Trackable, Receiver {

		final AtomicReference<Subscription> subscription;
		final CountDownLatch                completeLatch;
		final Queue<Event<T>>               script;
		final Queue<TaskEvent<T>>           taskEvents;
		final int                           requestedFusionMode;
		final int                           expectedFusionMode;
		final long                          initialRequest;
		final VirtualTimeScheduler          virtualTimeScheduler;

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

		/** The constructor used for verification, where a VirtualTimeScheduler can be
		 * passed */
		@SuppressWarnings("unchecked")
		DefaultVerifySubscriber(List<Event<T>> script,
				long initialRequest,
				int requestedFusionMode,
				int expectedFusionMode,
				boolean debugEnabled,
				VirtualTimeScheduler vts) {
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
			this.subscription = new AtomicReference<>();
			this.requested = initialRequest;
		}

		static <R> Queue<Event<R>> conflateScript(List<Event<R>> script, Logger logger) {
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
			//TODO simplified whole algo, remove DescriptionTasks

			return conflated;
		}

		/**
		 * @return the {@link VirtualTimeScheduler} this verifier will manipulate when
		 * using {@link #thenAwait(Duration)} methods, or null if real time is used
		 */
		public VirtualTimeScheduler virtualTimeScheduler() {
			return this.virtualTimeScheduler;
		}

		@Override
		public Throwable getError() {
			return errors;
		}

		@Override
		public boolean isCancelled() {
			return upstream() == Operators.cancelledSubscription();
		}

		@Override
		public boolean isStarted() {
			return upstream() != null;
		}

		@Override
		public boolean isTerminated() {
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
			onExpectation(Signal.error(t));
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
			if (subscription == null) {
				throw Exceptions.argumentIsNullException();
			}

			if (this.subscription.compareAndSet(null, subscription)) {
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
				else {
					setFailure(null, "an unexpected Subscription has been received: %s; actual: ",
							subscription,
							this.subscription);
				}
			}
		}

		void drainAsyncLoop(){
			T t;
			long r = requested;
			for( ; ;) {
				boolean d = done;
				if (d && qs.isEmpty()) {
					if(subscription.get() == Operators.cancelledSubscription()){
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
					if(subscription.get() == Operators.cancelledSubscription()){
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
							if(subscription.get() == Operators.cancelledSubscription()){
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
					r = Operators.addAndGet(REQUESTED, this, -p);
				}

				if(r == 0L || qs.isEmpty()){
					break;
				}
			}
		}

		@Override
		public Subscription upstream() {
			return this.subscription.get();
		}

		@Override
		public DefaultVerifySubscriber<T> log() {
			if (this.logger == null) {
				this.logger = Loggers.getLogger(StepVerifier.class);
			}
			return this;
		}

		@Override
		public StepVerifierAssertions verifyThenAssertThat() {
			//plug in the correct hooks
			Queue<Object> droppedElements = new ConcurrentLinkedQueue<>();
			Queue<Throwable> droppedErrors = new ConcurrentLinkedQueue<>();
			Queue<Tuple2<Throwable, ?>> operatorErrors = new ConcurrentLinkedQueue<>();
			Hooks.onErrorDropped(droppedErrors::offer);
			Hooks.onNextDropped(droppedElements::offer);
			Hooks.onOperatorError((t, d) -> {
				operatorErrors.offer(Tuples.of(t, d));
				return t;
			});

			try {
				//trigger the verify
				Duration time = verify();

				//return the assertion API
				return new DefaultStepVerifierAssertions(droppedElements, droppedErrors, operatorErrors, time);
			}
			finally {
				//unplug the hooks
				Hooks.resetOnNextDropped();
				Hooks.resetOnErrorDropped();
				Hooks.resetOnOperatorError();
			}
		}

		@Override
		public Duration verify() {
			return verify(Duration.ZERO);
		}

		@Override
		public Duration verify(Duration duration) {
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

		/**
		 * Signal a failure, always cancelling this subscriber. If the actual signal
		 * causing the failure might be either onComplete or onError (for which cancel
		 * is prohibited), prefer using {@link #setFailure(Event, Signal, String, Object...)}.
		 *
		 * @param event the event that triggered the failure
		 * @param msg the message for the error
		 * @param arguments the optional formatter arguments to the message
		 */
		final void setFailure(Event<T> event, String msg, Object... arguments) {
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
		final void setFailure(Event<T> event, Signal<T> actualSignal, String msg, Object... arguments) {
			Exceptions.addThrowable(ERRORS, this, fail(event, msg, arguments).get());
			maybeCancel(actualSignal);
			this.completeLatch.countDown();
		}

		final void setFailurePrefix(String prefix, Signal<T> actualSignal, String msg, Object... arguments) {
			Exceptions.addThrowable(ERRORS, this, failPrefix(prefix, msg, arguments).get());
			maybeCancel(actualSignal);
			this.completeLatch.countDown();
		}

		final Subscription cancel() {
			Subscription s =
					this.subscription.getAndSet(Operators.cancelledSubscription());
			if (s != null && s != Operators.cancelledSubscription()) {
				s.cancel();
				if(establishedFusionMode == Fuseable.ASYNC) {
					qs.clear();
				}
			}
			return s;
		}

		/** Cancels this subscriber if the actual signal is null or not a complete/error */
		final void maybeCancel(Signal<T> actualSignal) {
			if (actualSignal == null || (!actualSignal.isOnComplete() && !actualSignal.isOnError())) {
				cancel();
			}
		}

		final Optional<AssertionError> checkCountMismatch(SignalCountEvent<T> event, Signal<T> s) {
			long expected = event.count;
			if (!s.isOnNext()) {
				return fail(event, "expected: count = %s; actual: " + "counted = %s; " +
								"signal: %s",
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
			return true;
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
					setFailure(null, actualSignal, "did not expect: %s", actualSignal);
					return;
				}

				onTaskEvent();
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
				if (event == null || !(event instanceof EagerEvent)) {
					return;
				}

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
					AssertionError wrapFailure = fail(null,
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
			if (actualSignal.isOnNext() && currentNextAs == null) {
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

		void onTaskEvent() {
			Event<T> event;
			for (; ; ) {
				if (isCancelled()) {
					return;
				}
				event = this.script.peek();
				if (!(event instanceof TaskEvent)) {
					break;
				}
				event = this.script.poll();
				if (!(event instanceof TaskEvent)) {
					return;
				}
				taskEvents.add((TaskEvent<T>) event);
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
				subscriptionEvent.consume(upstream());
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

		@SuppressWarnings("unchecked")
		final void pollTaskEventOrComplete(Duration timeout) throws InterruptedException {
			Objects.requireNonNull(timeout, "timeout");
			Event<T> event;
			Instant stop = Instant.now()
			                      .plus(timeout);

			boolean skip = true;
			for (; ; ) {
				while ((event = taskEvents.poll()) != null) {
					try {
						skip = false;
						if (event instanceof SubscriptionTaskEvent) {
							updateRequested(event);
						}
						((TaskEvent<T>) event).run(this);
					}
					catch (Throwable t) {
						Exceptions.throwIfFatal(t);
						cancel();
						if(t instanceof AssertionError){
							throw (AssertionError)t;
						}
						throw Exceptions.propagate(t);
					}
				}
				if (!skip) {
					event = script.peek();
					if (event instanceof SubscriptionEvent) {
						serializeDrainAndSubscriptionEvent();
					}
				}
				if (this.completeLatch.await(10, TimeUnit.NANOSECONDS)) {
					break;
				}
				if (timeout != Duration.ZERO && stop.isBefore(Instant.now())) {
					if (!isStarted()) {
						throw new IllegalStateException(
								"VerifySubscriber has not been subscribed");
					}
					else {
						throw new AssertionError("VerifySubscriber timed out on " + upstream());
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
				Operators.addAndGet(REQUESTED, this, requestEvent.getRequestAmount());

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
						if(subscription.get() == Operators.cancelledSubscription()){
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
			if (!isStarted()) {
				throw new IllegalStateException(
						"VerifySubscriber has not been subscribed");
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
			throw new AssertionError(messageBuilder.toString(), errors);
		}

	}

	static class DefaultStepVerifierAssertions implements
	                                            StepVerifier.StepVerifierAssertions {

		private final Queue<Object> droppedElements;
		private final Queue<Throwable> droppedErrors;
		private final Queue<Tuple2<Throwable, ?>> operatorErrors;
		private final Duration duration;

		DefaultStepVerifierAssertions(Queue<Object> droppedElements,
				Queue<Throwable> droppedErrors,
				Queue<Tuple2<Throwable, ?>> operatorErrors,
				Duration duration) {
			this.droppedElements = droppedElements;
			this.droppedErrors = droppedErrors;
			this.operatorErrors = operatorErrors;
			this.duration = duration;
		}

		private StepVerifier.StepVerifierAssertions satisfies(BooleanSupplier check, Supplier<String> message) {
			if (!check.getAsBoolean()) {
				throw new AssertionError(message.get());
			}
			return this;
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasDroppedElements() {
			return satisfies(() -> !droppedElements.isEmpty(), () -> "Expected dropped elements, none found.");
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasDropped(Object... values) {
			satisfies(() -> values != null && values.length > 0, () -> "Require non-empty values");
			List<Object> valuesList = Arrays.asList(values);
			return satisfies(() -> droppedElements.containsAll(valuesList),
					() -> String.format("Expected dropped elements to contain <%s>, was <%s>.", valuesList, droppedElements));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasDroppedExactly(Object... values) {
			satisfies(() -> values != null && values.length > 0, () -> "Require non-empty values");
			List<Object> valuesList = Arrays.asList(values);
			return satisfies(() -> droppedElements.containsAll(valuesList)
							&& droppedElements.size() == valuesList.size(),
					() -> String.format("Expected dropped elements to contain exactly <%s>, was <%s>.", valuesList, droppedElements));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasDroppedErrors() {
			return satisfies(() -> !droppedErrors.isEmpty(),
					() -> "Expected at least 1 dropped error, none found.");
		}
		@Override
		public StepVerifier.StepVerifierAssertions hasDroppedErrors(int size) {
			return satisfies(() -> droppedErrors.size() == size,
					() -> String.format("Expected exactly %d dropped errors, %d found.", size, droppedErrors.size()));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasDroppedErrorOfType(Class<? extends Throwable> clazz) {
			satisfies(() -> clazz != null, () -> "Require non-null clazz");
			hasDroppedErrors(1);
			return satisfies(() -> clazz.isInstance(droppedErrors.peek()),
					() -> String.format("Expected dropped error to be of type %s, was %s.", clazz.getCanonicalName(), droppedErrors.peek().getClass().getCanonicalName()));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasDroppedErrorMatching(Predicate<Throwable> matcher) {
			satisfies(() -> matcher != null, () -> "Require non-null matcher");
			hasDroppedErrors(1);
			return satisfies(() -> matcher.test(droppedErrors.peek()),
					() -> String.format("Expected dropped error matching the given predicate, did not match: <%s>.", droppedErrors.peek()));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasDroppedErrorWithMessage(String message) {
			satisfies(() -> message != null, () -> "Require non-null message");
			hasDroppedErrors(1);
			String actual = droppedErrors.peek().getMessage();
			return satisfies(() -> message.equals(actual),
					() -> String.format("Expected dropped error with message <\"%s\">, was <\"%s\">.", message, actual));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasDroppedErrorWithMessageContaining(
				String messagePart) {
			satisfies(() -> messagePart != null, () -> "Require non-null messagePart");
			hasDroppedErrors(1);
			String actual = droppedErrors.peek().getMessage();
			return satisfies(() -> actual != null && actual.contains(messagePart),
					() -> String.format("Expected dropped error with message containing <\"%s\">, was <\"%s\">.", messagePart, actual));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasDroppedErrorsMatching(Predicate<Collection<Throwable>> matcher) {
			satisfies(() -> matcher != null, () -> "Require non-null matcher");
			hasDroppedErrors();
			return satisfies(() -> matcher.test(droppedErrors),
					() -> String.format("Expected collection of dropped errors matching the given predicate, did not match: <%s>.", droppedErrors));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasDroppedErrorsSatisfying(Consumer<Collection<Throwable>> asserter) {
			satisfies(() -> asserter != null, () -> "Require non-null asserter");
			hasDroppedErrors();
			asserter.accept(droppedErrors);
			return this;
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasOperatorErrors() {
			return satisfies(() -> !operatorErrors.isEmpty(),
					() -> "Expected at least 1 operator error, none found.");
		}
		@Override
		public StepVerifier.StepVerifierAssertions hasOperatorErrors(int size) {
			return satisfies(() -> operatorErrors.size() == size,
					() -> String.format("Expected exactly %d operator errors, %d found.", size, operatorErrors.size()));
		}

		StepVerifier.StepVerifierAssertions hasOneOperatorErrorWithError() {
			satisfies(() -> operatorErrors.size() == 1,
					() -> String.format("Expected exactly one operator error, %d found.", operatorErrors.size()));
			satisfies(() -> operatorErrors.peek().getT1() != null,
					() -> "Expected exactly one operator error with an actual throwable content, no throwable found.");
			return this;
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasOperatorErrorOfType(Class<? extends Throwable> clazz) {
			satisfies(() -> clazz != null, () -> "Require non-null clazz");
			hasOneOperatorErrorWithError();
			return satisfies(() -> clazz.isInstance(operatorErrors.peek().getT1()),
					() -> String.format("Expected operator error to be of type %s, was %s.",
							clazz.getCanonicalName(), operatorErrors.peek().getT1().getClass().getCanonicalName()));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasOperatorErrorMatching(Predicate<Throwable> matcher) {
			satisfies(() -> matcher != null, () -> "Require non-null matcher");
			hasOneOperatorErrorWithError();
			return satisfies(() -> matcher.test(operatorErrors.peek().getT1()),
					() -> String.format("Expected operator error matching the given predicate, did not match: <%s>.", operatorErrors.peek()));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasOperatorErrorWithMessage(String message) {
			satisfies(() -> message != null, () -> "Require non-null message");
			hasOneOperatorErrorWithError();
			String actual = operatorErrors.peek().getT1().getMessage();
			return satisfies(() -> message.equals(actual),
					() -> String.format("Expected operator error with message <\"%s\">, was <\"%s\">.", message, actual));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasOperatorErrorWithMessageContaining(
				String messagePart) {
			satisfies(() -> messagePart != null, () -> "Require non-null messagePart");
			hasOneOperatorErrorWithError();
			String actual = operatorErrors.peek().getT1().getMessage();
			return satisfies(() -> actual != null && actual.contains(messagePart),
					() -> String.format("Expected operator error with message containing <\"%s\">, was <\"%s\">.", messagePart, actual));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasOperatorErrorsMatching(Predicate<Collection<Tuple2<Throwable, ?>>> matcher) {
			satisfies(() -> matcher != null, () -> "Require non-null matcher");
			hasOperatorErrors();
			return satisfies(() -> matcher.test(operatorErrors),
					() -> String.format("Expected collection of operator errors matching the given predicate, did not match: <%s>.", operatorErrors));
		}

		@Override
		public StepVerifier.StepVerifierAssertions hasOperatorErrorsSatisfying(Consumer<Collection<Tuple2<Throwable, ?>>> asserter) {
			satisfies(() -> asserter != null, () -> "Require non-null asserter");
			hasOperatorErrors();
			asserter.accept(operatorErrors);
			return this;
		}

		@Override
		public StepVerifier.StepVerifierAssertions tookLessThan(Duration d) {
			return satisfies(() -> duration.compareTo(d) <= 0,
					() -> String.format("Expected scenario to be verified in less than %sms, took %sms.",
							d.toMillis(), duration.toMillis()));
		}

		@Override
		public StepVerifier.StepVerifierAssertions tookMoreThan(Duration d) {
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
	}

	static class SubscriptionEvent<T> extends AbstractEagerEvent<T> {

		final Consumer<Subscription> consumer;
		
		SubscriptionEvent(String desc) {
			this(null, desc);
		}

		SubscriptionEvent(Consumer<Subscription> consumer, String desc) {
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

		final Supplier<? extends Collection<T>> supplier;

		final Predicate<? super Collection<T>>  predicate;

		final Consumer<? super Collection<T>>   consumer;

		CollectEvent(Supplier<? extends Collection<T>> supplier, String desc) {
			super(desc);
			this.supplier = supplier;
			this.predicate = null;
			this.consumer = null;
		}

		CollectEvent(Consumer<? super Collection<T>> consumer, String desc) {
			super(desc);
			this.supplier = null;
			this.predicate = null;
			this.consumer = consumer;
		}

		CollectEvent(Predicate<? super Collection<T>> predicate, String desc) {
			super(desc);
			this.supplier = null;
			this.predicate = predicate;
			this.consumer = null;
		}

		Collection<T> get() {
			return supplier != null ? supplier.get() : null;
		}

		Optional<AssertionError> test(Collection<T> collection) {
			if (predicate != null) {
				if (!predicate.test(collection)) {
					return fail(this, "expected collection predicate match; actual: %s",
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
		
		TaskEvent(Runnable task, String desc) {
			super(desc);
			this.task = task;
		}

		void run(DefaultVerifySubscriber<T> parent) throws Exception {
			task.run();
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
			task.accept(parent.subscription.get());
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
				if(parent.isTerminated() && !parent.isCancelled()){
					throw new AssertionError("unexpected end during a no-event expectation");
				}
				virtualOrRealWait(Duration.ofNanos(1), parent);
			}
			else{
				parent.monitorSignal = true;
				virtualOrRealWait(duration, parent);
				parent.monitorSignal = false;
				if(parent.isTerminated() && !parent.isCancelled()){
					throw new AssertionError("unexpected end during a no-event expectation");
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
				delegate.consume(parent.upstream());
			}
		}
	}

	static final class SignalSequenceEvent<T> extends AbstractSignalEvent<T> {

		final Iterable<? extends T> iterable;

		SignalSequenceEvent(Iterable<? extends T> iterable, String desc) {
			super(desc);
			this.iterable = iterable;
		}

		Optional<AssertionError> test(Signal<T> signal, Iterator<? extends T> iterator) {
			if (signal.isOnNext()) {
				if (!iterator.hasNext()) {
					return Optional.empty();
				}
				T d2 = iterator.next();
				if (!Objects.equals(signal.get(), d2)) {
					return fail(this, "expected : onNext(%s); actual: %s; iterable: %s",
							d2,
							signal.get(),
							iterable);
				}
				return iterator.hasNext() ? EXPECT_MORE : Optional.empty();

			}
			if (iterator != null && iterator.hasNext() || signal.isOnError()) {
				return fail(this, "expected next value: %s; actual signal: %s; iterable: %s",
						iterator != null && iterator.hasNext() ? iterator.next() : "none",
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

	static Optional<AssertionError> fail(Event<?> event, String msg, Object... args) {
		String prefix = "expectation failed (";
		if (event != null && event.getDescription() != null) {
			prefix = String.format("expectation \"%s\" failed (", event.getDescription());
		}

		return failPrefix(prefix, msg, args);
	}

	static Optional<AssertionError> failPrefix(String prefix, String msg, Object... args) {
		return Optional.of(new AssertionError(prefix + String.format(msg, args) + ")"));
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

	static <T> SignalEvent<T> newOnSubscribeStep(String desc){
		return new SignalEvent<>((signal, se) -> {
			if (!signal.isOnSubscribe()) {
				return fail(se, "expected: onSubscribe(); actual: %s", signal);
			}
			else {
				return Optional.empty();
			}
		}, desc);
	}

	static final SignalEvent DEFAULT_ONSUBSCRIBE_STEP = newOnSubscribeStep("defaultOnSubscribe");

	static final AtomicReferenceFieldUpdater<DefaultVerifySubscriber, Throwable>
			ERRORS =
			AtomicReferenceFieldUpdater.newUpdater(DefaultVerifySubscriber.class,
					Throwable.class,
					"errors");

	static final AtomicIntegerFieldUpdater<DefaultVerifySubscriber> WIP =
			AtomicIntegerFieldUpdater.newUpdater(DefaultVerifySubscriber.class, "wip");

	static final Optional<AssertionError> EXPECT_MORE = Optional.of(new AssertionError("EXPECT MORE"));

}
