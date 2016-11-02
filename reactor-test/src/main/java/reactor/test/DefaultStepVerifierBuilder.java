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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * Default implementation of {@link StepVerifier.Step} and
 * {@link StepVerifier.LastStep}.
 *
 * @author Arjen Poutsma
 * @since 1.0
 */
final class DefaultStepVerifierBuilder<T>
		implements StepVerifier.FirstStep<T, StepVerifier> {

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

	static <T> StepVerifier.FirstStep<T, StepVerifier> newVerifier(long n,
			Supplier<? extends Publisher<? extends T>> scenarioSupplier,
			Supplier<? extends VirtualTimeScheduler> vtsLookup){
		DefaultStepVerifierBuilder.checkPositive(n);
		Objects.requireNonNull(scenarioSupplier, "scenarioSupplier");

		return new DefaultStepVerifierBuilder<>
				(n, scenarioSupplier, vtsLookup);
	}

	@SuppressWarnings("unchecked")
	static <T> SignalEvent<T> defaultFirstStep() {
		return (SignalEvent<T>) DEFAULT_ONSUBSCRIBE_STEP;
	}

	final List<Event<T>>                             script;
	final long                                       initialRequest;
	final Supplier<? extends VirtualTimeScheduler>   vtsLookup;
	final Supplier<? extends Publisher<? extends T>> sourceSupplier;

	int requestedFusionMode = -1;
	int expectedFusionMode  = -1;

	DefaultStepVerifierBuilder(long initialRequest,
			Supplier<? extends Publisher<? extends T>> sourceSupplier,
			Supplier<? extends VirtualTimeScheduler> vtsLookup) {
		this.initialRequest = initialRequest;
		this.vtsLookup = vtsLookup;
		this.sourceSupplier = sourceSupplier;
		this.script = new ArrayList<>();
		this.script.add(defaultFirstStep());
	}

	@Override
	public DefaultVerifySubscriber<T> consumeErrorWith(Consumer<Throwable> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnError()) {
				return fail("expected: onError(); actual: %s", signal);
			}
			else {
				consumer.accept(signal.getThrowable());
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();
	}

	@Override
	public DefaultStepVerifierBuilder<T> consumeNextWith(
			Consumer<? super T> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnNext()) {
				return fail("expected: onNext(); actual: %s", signal);
			}
			else {
				consumer.accept(signal.get());
				return Optional.empty();
			}
		});
		this.script.add(event);
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> consumeRecordedWith(
			Consumer<? super Collection<T>> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		this.script.add(new CollectEvent<>(consumer));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> consumeSubscriptionWith(
			Consumer<? super Subscription> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		this.script.set(0, new SignalEvent<>(signal -> {
			if (!signal.isOnSubscribe()) {
				return fail("expected: onSubscribe(); actual: %s", signal);
			}
			else {
				consumer.accept(signal.getSubscription());
				return Optional.empty();
			}
		}));
		return this;
	}

	@Override
	public DefaultVerifySubscriber<T> expectComplete() {
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnComplete()) {
				return fail("expected: onComplete(); actual: %s", signal);
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();
	}

	@Override
	public DefaultVerifySubscriber<T> expectError() {
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnError()) {
				return fail("expected: onError(); actual: %s", signal);
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();

	}

	@Override
	public DefaultVerifySubscriber<T> expectError(Class<? extends Throwable> clazz) {
		Objects.requireNonNull(clazz, "clazz");
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnError()) {
				return fail("expected: onError(%s); actual: %s",
						clazz.getSimpleName(), signal);
			}
			else if (!clazz.isInstance(signal.getThrowable())) {
				return fail("expected error of type: %s; actual type: %s",
						clazz.getSimpleName(), signal.getThrowable());
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();
	}

	@Override
	public DefaultVerifySubscriber<T> expectErrorMessage(String errorMessage) {
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnError()) {
				return fail("expected: onError(\"%s\"); actual: %s",
						errorMessage, signal);
			}
			else if (!Objects.equals(errorMessage,
					signal.getThrowable()
					      .getMessage())) {
				return fail("expected error message: \"%s\"; " + "actual " + "message: %s",
						errorMessage,
						signal.getThrowable()
						      .getMessage());
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();
	}

	@Override
	public DefaultVerifySubscriber<T> expectErrorWith(Predicate<Throwable> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnError()) {
				return fail("expected: onError(); actual: %s", signal);
			}
			else if (!predicate.test(signal.getThrowable())) {
				return fail("predicate failed on exception: %s", signal.getThrowable());
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNoFusionSupport() {
		requestedFusionMode = NO_FUSION_SUPPORT;
		return this;
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
	public DefaultStepVerifierBuilder<T> expectNext(T... ts) {
		Objects.requireNonNull(ts, "ts");
		SignalEvent<T> event;
		for (T t : ts) {
			event = new SignalEvent<>(signal -> {
				if (!signal.isOnNext()) {
					return fail("expected: onNext(%s); actual: %s", t, signal);
				}
				else if (!Objects.equals(t, signal.get())) {
					return fail("expected value: %s; actual value: %s", t, signal.get());

				}
				else {
					return Optional.empty();
				}
			});
			this.script.add(event);
		}
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNextSequence(
			Iterable<? extends T> iterable) {
		Objects.requireNonNull(iterable, "iterable");
		this.script.add(new SignalSequenceEvent<>(iterable));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNextCount(long count) {
		checkPositive(count);
		this.script.add(new SignalCountEvent<>(count));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectNextWith(
			Predicate<? super T> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnNext()) {
				return fail("expected: onNext(); actual: %s", signal);
			}
			else if (!predicate.test(signal.get())) {
				return fail("predicate failed on value: %s", signal.get());
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return this;

	}

	@Override
	public DefaultStepVerifierBuilder<T> expectRecordedWith(
			Predicate<? super Collection<T>> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		this.script.add(new CollectEvent<>(predicate));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectSubscription() {
		if(this.script.get(0) instanceof NoEvent) {
			this.script.add(defaultFirstStep());
		}
		else{
			this.script.set(0, newOnSubscribeStep());
		}
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> expectSubscriptionWith(
			Predicate<? super Subscription> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		this.script.set(0, new SignalEvent<>(signal -> {
			if (!signal.isOnSubscribe()) {
				return fail("expected: onSubscribe(); actual: %s", signal);
			}
			else if (!predicate.test(signal.getSubscription())) {
				return fail("predicate failed on subscription: %s",
						signal.getSubscription());
			}
			else {
				return Optional.empty();
			}
		}));
		return this;
	}

	@Override
	public StepVerifier.FirstStep<T, StepVerifier> expectNoEvent(Duration duration) {
		Objects.requireNonNull(duration, "duration");
		if(this.script.size() == 1 && this.script.get(0) == defaultFirstStep()){
			this.script.set(0, new NoEvent<>(duration));
		}
		else {
			this.script.add(new NoEvent<>(duration));
		}
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> recordWith(Supplier<? extends Collection<T>> supplier) {
		Objects.requireNonNull(supplier, "supplier");
		this.script.add(new CollectEvent<>(supplier));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> then(Runnable task) {
		Objects.requireNonNull(task, "task");
		this.script.add(new TaskEvent<>(task));
		return this;
	}

	@Override
	public DefaultVerifySubscriber<T> thenCancel() {
		this.script.add(new SubscriptionEvent<>());
		return build();
	}

	@Override
	public DefaultStepVerifierBuilder<T> thenRequest(long n) {
		checkStrictlyPositive(n);
		this.script.add(new SubscriptionEvent<>(subscription -> subscription.request(n)));
		return this;
	}

	@Override
	public DefaultStepVerifierBuilder<T> thenAwait() {
		return thenAwait(Duration.ZERO);
	}

	@Override
	public DefaultStepVerifierBuilder<T> thenAwait(Duration timeshift) {
		Objects.requireNonNull(timeshift, "timeshift");
		this.script.add(new WaitEvent<>(timeshift));
		return this;
	}

	final DefaultVerifySubscriber<T> build() {
		return new DefaultVerifySubscriber<>(this);
	}

	@SuppressWarnings("unused")
	interface Event<T> {

	}

	final static class DefaultVerifySubscriber<T>
			implements StepVerifier, Subscriber<T>, Trackable, Receiver {

		final AtomicReference<Subscription> subscription;
		final CountDownLatch                completeLatch;
		final Queue<Event<T>>               script;
		final Queue<TaskEvent<T>>           taskEvents;
		final int                           requestedFusionMode;
		final int                           expectedFusionMode;
		final DefaultStepVerifierBuilder<T> parent;
		final boolean                       supplyOnVerify;

		int                           establishedFusionMode;
		Fuseable.QueueSubscription<T> qs;
		long                          produced;
		Iterator<? extends T>         currentNextAs;
		Collection<T>                 currentCollector;

		@SuppressWarnings("unused")
		volatile int wip;

		@SuppressWarnings("unused")
		volatile Throwable errors;

		volatile boolean monitorSignal = false;

		DefaultVerifySubscriber(DefaultStepVerifierBuilder<T> parent) {
			this(parent, parent.sourceSupplier != null);
		}

		@SuppressWarnings("unchecked")
		DefaultVerifySubscriber(DefaultStepVerifierBuilder<T> parent,
				boolean supplyOnVerify) {
			this.parent = parent;
			this.script = new ConcurrentLinkedQueue<>(parent.script);
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
			this.supplyOnVerify = supplyOnVerify;
			this.requestedFusionMode = parent.requestedFusionMode;
			this.expectedFusionMode =
					parent.expectedFusionMode == -1 ? parent.requestedFusionMode :
							parent.expectedFusionMode;
			this.produced = 0L;
			this.completeLatch = new CountDownLatch(1);
			this.subscription = new AtomicReference<>();
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
			onExpectation(Signal.complete());
			this.completeLatch.countDown();
		}

		@Override
		public void onError(Throwable t) {
			onExpectation(Signal.error(t));
			this.completeLatch.countDown();
		}

		@Override
		public void onNext(T t) {
			if (establishedFusionMode == Fuseable.ASYNC) {
				for (; ; ) {
					try {
						t = qs.poll();
						if (t == null) {
							break;
						}
					}
					catch (Throwable e) {
						Exceptions.throwIfFatal(e);
						onExpectation(Signal.error(e));
						cancel();
						completeLatch.countDown();
						return;
					}
					produced++;
					if (currentCollector != null) {
						currentCollector.add(t);
					}
					onExpectation(Signal.next(t));
				}
			}
			else {
				produced++;
				if (currentCollector != null) {
					currentCollector.add(t);
				}
				onExpectation(Signal.next(t));
			}
		}

		@Override
		public void onSubscribe(Subscription subscription) {
			if (subscription == null) {
				throw Exceptions.argumentIsNullException();
			}

			if (this.subscription.compareAndSet(null, subscription)) {
				onExpectation(Signal.subscribe(subscription));
				if (requestedFusionMode == NO_FUSION_SUPPORT &&
						subscription instanceof Fuseable.QueueSubscription){
					setFailure("unexpected fusion support: %s", subscription);
				}
				else if (requestedFusionMode >= Fuseable.NONE) {
					startFusion(subscription);
				}
				else if (parent.initialRequest != 0L) {
					subscription.request(parent.initialRequest);
				}
			}
			else {
				subscription.cancel();
				if (isCancelled()) {
					setFailure("an unexpected Subscription has been received: %s; " + "actual: cancelled",
							subscription);
				}
				else {
					setFailure("an unexpected Subscription has been received: %s; " + "actual: ",
							subscription,
							this.subscription);
				}
			}
		}

		@Override
		public Subscription upstream() {
			return this.subscription.get();
		}

		@Override
		public Duration verify() {
			return verify(Duration.ZERO);
		}

		@Override
		public Duration verify(Duration duration) {
			Objects.requireNonNull(duration, "duration");
			if (supplyOnVerify) {
				VirtualTimeScheduler vts = null;
				if (parent.vtsLookup != null) {
					vts = parent.vtsLookup.get();
				}
				try {
					Publisher<? extends T> publisher = parent.sourceSupplier.get();
					precheckVerify(publisher);
					Instant now = Instant.now();

					DefaultVerifySubscriber<T> newVerifier =
							new DefaultVerifySubscriber<>(parent, false);

					publisher.subscribe(newVerifier);
					newVerifier.verify(duration);

					return Duration.between(now, Instant.now());
				}
				finally {
					if (vts != null) {
						vts.shutdown();
					}
				}
			}

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

		final void setFailure(String msg, Object... arguments) {
			Exceptions.addThrowable(ERRORS, this, fail(msg, arguments).get());
			cancel();
			this.completeLatch.countDown();
		}

		final Subscription cancel() {
			Subscription s =
					this.subscription.getAndSet(Operators.cancelledSubscription());
			if (s != null && s != Operators.cancelledSubscription()) {
				s.cancel();
			}
			return s;
		}

		final Optional<AssertionError> checkCountMismatch(long expected, Signal<T> s) {
			if (!s.isOnNext()) {
				return fail("expected: count = %s; actual: " + "produced = %s; " + "signal: %s",
						expected,
						produced, s);
			}
			else {
				return Optional.empty();
			}
		}

		boolean onCollect() {
			Collection<T> c;
			CollectEvent<T> collectEvent = (CollectEvent<T>) this.script.poll();
			if (collectEvent.supplier != null) {
				c = collectEvent.get();
				this.currentCollector = c;

				if (c == null) {
					setFailure("expected collection; actual supplied is [null]");
				}
				return true;
			}
			c = this.currentCollector;

			if (c == null) {
				setFailure("expected record collector; actual record is [null]");
				return true;
			}

			Optional<AssertionError> error = collectEvent.test(c);
			if (error.isPresent()) {
				Exceptions.addThrowable(ERRORS, this, error.get());
				cancel();
				this.completeLatch.countDown();
				return true;
			}
			return true;
		}

		@SuppressWarnings("unchecked")
		final void onExpectation(Signal<T> actualSignal) {
			if (monitorSignal) {
				setFailure("did not expect: %s", actualSignal);
				return;
			}
			try {
				Event<T> event = this.script.peek();
				if (event == null) {
					setFailure("did not expect: %s", actualSignal);
					return;
				}

				onTaskEvent();
				if (event instanceof SignalCountEvent) {
					if (onSignalCount(actualSignal, (SignalCountEvent<T>) event)) {
						return;
					}
				}
				else if (event instanceof CollectEvent) {
					if (onCollect()) {
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
						if (onSubscription()) {
							return;
						}
					}
					else if (event instanceof CollectEvent) {
						if (onCollect()) {
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
					Exceptions.addThrowable(ERRORS,
							this,
							fail("failed running expectation on signal [%s] with " + "[%s]:\n%s",
									Exceptions.unwrap(e)
									          .getClass()
									          .getName(),
									msg).get());
				}
				cancel();
				completeLatch.countDown();
			}
		}

		boolean onSignal(Signal<T> actualSignal) {
			SignalEvent<T> signalEvent = (SignalEvent<T>) this.script.poll();
			Optional<AssertionError> error = signalEvent.test(actualSignal);
			if (error.isPresent()) {
				Exceptions.addThrowable(ERRORS, this, error.get());
				cancel();
				this.completeLatch.countDown();
				return true;
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
				return false;
			}
			if (!error.isPresent()) {
				this.currentNextAs = null;
				this.script.poll();
			}
			else {
				Exceptions.addThrowable(ERRORS, this, error.get());
				cancel();
				this.completeLatch.countDown();
				return true;
			}
			return false;
		}

		final boolean onSignalCount(Signal<T> actualSignal, SignalCountEvent<T> event) {
			if (produced >= event.count) {
				this.script.poll();
				produced = 0L;
			}
			else {
				if (event.count != 0) {
					Optional<AssertionError> error =
							this.checkCountMismatch(event.count, actualSignal);

					if (error.isPresent()) {
						Exceptions.addThrowable(ERRORS, this, error.get());
						cancel();
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

		boolean onSubscription() {
			int missed = WIP.incrementAndGet(this);
			if (missed != 1) {
				return true;
			}
			SubscriptionEvent<T> subscriptionEvent;
			for (; ; ) {
				if (this.script.peek() instanceof SubscriptionEvent) {
					subscriptionEvent = (SubscriptionEvent<T>) this.script.poll();
					if (subscriptionEvent.isTerminal()) {
						cancel();
						this.completeLatch.countDown();
						return true;
					}
					subscriptionEvent.consume(upstream());
				}
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
			return false;
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
						onSubscription();
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

		void precheckVerify(Publisher<? extends T> publisher) {
			Objects.requireNonNull(publisher, "publisher");
			if (requestedFusionMode == NO_FUSION_SUPPORT && publisher instanceof Fuseable){
				throw new AssertionError("The source publisher supports fusion");
			}
			else if (requestedFusionMode >= Fuseable.NONE && !(publisher instanceof
					Fuseable)) {
				throw new AssertionError("The source publisher does not support fusion");
			}
		}

		final void startFusion(Subscription s) {
			if (s instanceof Fuseable.QueueSubscription) {
				@SuppressWarnings("unchecked") Fuseable.QueueSubscription<T> qs =
						(Fuseable.QueueSubscription<T>) s;

				this.qs = qs;

				int m = qs.requestFusion(requestedFusionMode);
				if ((m & expectedFusionMode) != m) {
					setFailure("expected fusion mode: %s; actual: %s",
							formatFusionMode(expectedFusionMode),
							formatFusionMode(m));
					return;
				}

				this.establishedFusionMode = m;

				if (m == Fuseable.SYNC) {
					T v;
					for (; ; ) {
						try {
							v = qs.poll();
						}
						catch (Throwable e) {
							Exceptions.throwIfFatal(e);
							onExpectation(Signal.error(e));
							return;
						}
						if (v == null) {
							onComplete();
							break;
						}

						onNext(v);
					}
				}
				else if (parent.initialRequest != 0) {
					s.request(parent.initialRequest);
				}
			}
			else {
				setFailure("expected fusion-ready source but actual Subscription is " + "not: %s",
						expectedFusionMode,
						s);
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

			if (errors.getSuppressed().length == 0){
				if(errors instanceof AssertionError){
					throw (AssertionError)errors;
				}
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

	interface EagerEvent<T> extends Event<T> {

	}

	static final class SubscriptionEvent<T> implements EagerEvent<T> {

		final Consumer<Subscription> consumer;

		SubscriptionEvent() {
			this(null);
		}

		SubscriptionEvent(Consumer<Subscription> consumer) {
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
	static final class SignalEvent<T> implements Event<T> {

		final Function<Signal<T>, Optional<AssertionError>> function;

		SignalEvent(Function<Signal<T>, Optional<AssertionError>> function) {
			this.function = function;
		}

		Optional<AssertionError> test(Signal<T> signal) {
			return this.function.apply(signal);
		}

	}

	static final class SignalCountEvent<T> implements Event<T> {

		final long count;

		SignalCountEvent(long count) {
			this.count = count;
		}

	}

	static final class CollectEvent<T> implements EagerEvent<T> {

		final Supplier<? extends Collection<T>> supplier;

		final Predicate<? super Collection<T>>  predicate;

		final Consumer<? super Collection<T>>   consumer;
		CollectEvent(Supplier<? extends Collection<T>> supplier) {
			this.supplier = supplier;
			this.predicate = null;
			this.consumer = null;
		}
		CollectEvent(Consumer<? super Collection<T>> consumer) {
			this.supplier = null;
			this.predicate = null;
			this.consumer = consumer;
		}

		CollectEvent(Predicate<? super Collection<T>> predicate) {
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
					return fail("expected collection predicate" + " match;" + " actual: %s",
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

	static class TaskEvent<T> implements EagerEvent<T> {

		final Runnable task;

		TaskEvent(Runnable task) {
			this.task = task;
		}

		void run(DefaultVerifySubscriber<T> parent) throws Exception {
			task.run();
		}

	}

	static void virtualOrRealWait(Duration duration, DefaultVerifySubscriber<?> s)
			throws Exception {
		if (s.parent.vtsLookup == null) {
			s.completeLatch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
		}
		else {
			s.parent.vtsLookup.get()
			                  .advanceTimeBy(duration);
		}
	}

	static final class NoEvent<T> extends TaskEvent<T> {

		final Duration duration;

		NoEvent(Duration duration) {
			super(null);
			this.duration = duration;
		}

		@Override
		void run(DefaultVerifySubscriber<T> parent) throws Exception {
			if(parent.parent.vtsLookup != null) {
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

		WaitEvent(Duration duration) {
			super(null);
			this.duration = duration;
		}

		@Override
		void run(DefaultVerifySubscriber<T> s) throws Exception {
			virtualOrRealWait(duration, s);
		}

	}

	static final class SignalSequenceEvent<T> implements Event<T> {

		final Iterable<? extends T> iterable;

		SignalSequenceEvent(Iterable<? extends T> iterable) {
			this.iterable = iterable;
		}

		Optional<AssertionError> test(Signal<T> signal, Iterator<? extends T> iterator) {
			if (signal.isOnNext()) {
				if (!iterator.hasNext()) {
					return fail("unexpected iterator request; " + "onNext(%s); iterable: %s",
							signal.get(), iterable);
				}
				T d2 = iterator.next();
				if (!Objects.equals(signal.get(), d2)) {
					return fail("expected : onNext(%s); actual: " + "%s; iterable: %s",
							d2,
							signal.get(),
							iterable);
				}
				return iterator.hasNext() ? EXPECT_MORE : Optional.empty();

			}
			if (iterator != null && iterator.hasNext() || signal.isOnError()) {
				return fail("expected next " + "value: %s; actual " + "actual signal: " + "%s; iterable: %s",
						iterator != null && iterator.hasNext() ? iterator.next() : "none",
						signal, iterable);
			}
			return Optional.empty();
		}
	}

	static Optional<AssertionError> fail(String msg, Object... args) {
		return Optional.of(new AssertionError(String.format(msg, args)));
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

	static <T> SignalEvent<T> newOnSubscribeStep(){
		return new SignalEvent<>(signal -> {
			if (!signal.isOnSubscribe()) {
				return fail("expected: onSubscribe(); actual: %s", signal);
			}
			else {
				return Optional.empty();
			}
		});
	}

	static final SignalEvent DEFAULT_ONSUBSCRIBE_STEP = newOnSubscribeStep();

	static final int NO_FUSION_SUPPORT = Integer.MIN_VALUE;

	static final AtomicReferenceFieldUpdater<DefaultVerifySubscriber, Throwable>
			ERRORS =
			AtomicReferenceFieldUpdater.newUpdater(DefaultVerifySubscriber.class,
					Throwable.class,
					"errors");

	static final AtomicIntegerFieldUpdater<DefaultVerifySubscriber> WIP =
			AtomicIntegerFieldUpdater.newUpdater(DefaultVerifySubscriber.class, "wip");

	static final Optional<AssertionError> EXPECT_MORE = Optional.ofNullable(null);

}
