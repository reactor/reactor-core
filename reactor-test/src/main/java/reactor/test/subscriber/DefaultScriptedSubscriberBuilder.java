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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * Default implementation of {@link ScriptedSubscriber.StepBuilder} and
 * {@link ScriptedSubscriber.LastStepBuilder}.
 *
 * @author Arjen Poutsma
 * @since 1.0
 */
final class DefaultScriptedSubscriberBuilder<T>
		implements ScriptedSubscriber.FirstStepBuilder<T> {

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

	@SuppressWarnings("unchecked")
	static <T> SignalEvent<T> defaultFirstStep() {
		return (SignalEvent<T>) DEFAULT_ONSUBSCRIBE_STEP;
	}

	final List<Event<T>> script = new ArrayList<>();
	final long initialRequest;
	int requestedFusionMode = -1;
	int expectedFusionMode  = -1;

	DefaultScriptedSubscriberBuilder(long initialRequest) {
		this.initialRequest = initialRequest;
		this.script.add(defaultFirstStep());
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> advanceTime() {
		this.script.add(new TaskEvent<>(() -> VirtualTimeScheduler.get()
		                                                          .advanceTime()));
		return this;
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> advanceTimeBy(Duration timeshift) {
		this.script.add(new TaskEvent<>(() -> VirtualTimeScheduler.get()
		                                                          .advanceTimeBy(timeshift.toNanos(),
				                                                          TimeUnit.NANOSECONDS)));
		return this;
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> advanceTimeTo(Instant instant) {

		this.script.add(new TaskEvent<>(() -> VirtualTimeScheduler.get()
		                                                          .advanceTimeTo(instant.toEpochMilli(),
				                                                          TimeUnit.MILLISECONDS)));
		return this;
	}

	@Override
	public ScriptedSubscriber<T> consumeErrorWith(Consumer<Throwable> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnError()) {
				return Optional.of(String.format("expected: onError(); actual: %s",
						signal));
			}
			else {
				try {
					consumer.accept(signal.getThrowable());
					return Optional.empty();
				}
				catch (AssertionError assertion) {
					String msg =
							assertion.getMessage() == null ? "" : assertion.getMessage();
					return Optional.of(msg);
				}
			}
		});
		this.script.add(event);
		return build();
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> consumeNextWith(Consumer<? super T> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnNext()) {
				return Optional.of(String.format("expected: onNext(); actual: %s",
						signal));
			}
			else {
				try {
					consumer.accept(signal.get());
					return Optional.empty();
				}
				catch (AssertionError assertion) {
					String msg =
							assertion.getMessage() == null ? "" : assertion.getMessage();
					return Optional.of(msg);
				}
			}
		});
		this.script.add(event);
		return this;
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> consumeRecordedWith(Consumer<? super Collection<T>> consumer) {
		this.script.add(new CollectEvent<>(consumer));
		return this;
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> consumeSubscriptionWith(Consumer<? super Subscription> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		this.script.set(0, new SignalEvent<>(signal -> {
			if (!signal.isOnSubscribe()) {
				return Optional.of(String.format("expected: onSubscribe(); actual: %s",
						signal));
			}
			else {
				try {
					consumer.accept(signal.getSubscription());
					return Optional.empty();
				}
				catch (AssertionError assertion) {
					String msg =
							assertion.getMessage() == null ? "" : assertion.getMessage();
					return Optional.of(msg);
				}
			}
		}));
		return this;
	}

	@Override
	public ScriptedSubscriber<T> expectComplete() {
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnComplete()) {
				return Optional.of(String.format("expected: onComplete(); actual: %s",
						signal));
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();
	}

	@Override
	public ScriptedSubscriber<T> expectError() {
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnError()) {
				return Optional.of(String.format("expected: onError(); actual: %s",
						signal));
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();

	}

	@Override
	public ScriptedSubscriber<T> expectError(Class<? extends Throwable> clazz) {
		Objects.requireNonNull(clazz, "clazz");
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnError()) {
				return Optional.of(String.format("expected: onError(%s); actual: %s",
						clazz.getSimpleName(),
						signal));
			}
			else if (!clazz.isInstance(signal.getThrowable())) {
				return Optional.of(String.format(
						"expected error of type: %s; actual type: %s",
						clazz.getSimpleName(),
						signal.getThrowable()));
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();
	}

	@Override
	public ScriptedSubscriber<T> expectErrorMessage(String errorMessage) {
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnError()) {
				return Optional.of(String.format("expected: onError(\"%s\"); actual: %s",
						errorMessage,
						signal));
			}
			else if (!Objects.equals(errorMessage,
					signal.getThrowable()
					      .getMessage())) {
				return Optional.of(String.format("expected error message: \"%s\"; " + "actual " + "message: %s",
						errorMessage,
						signal.getThrowable()
						      .getMessage()));
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();
	}

	@Override
	public ScriptedSubscriber<T> expectErrorWith(Predicate<Throwable> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnError()) {
				return Optional.of(String.format("expected: onError(); actual: %s",
						signal));
			}
			else if (!predicate.test(signal.getThrowable())) {
				return Optional.of(String.format("predicate failed on exception: %s",
						signal.getThrowable()));
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> expectFusion() {
		return expectFusion(Fuseable.ANY, Fuseable.ANY);
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> expectFusion(int requested) {
		return expectFusion(requested, requested);
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> expectFusion(int requested, int expected) {
		checkPositive(requested);
		checkPositive(expected);
		requestedFusionMode = requested;
		expectedFusionMode = expected;
		return this;
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> expectNext(T... ts) {
		Objects.requireNonNull(ts, "ts");
		SignalEvent<T> event;
		for (T t : ts) {
			event = new SignalEvent<>(signal -> {
				if (!signal.isOnNext()) {
					return Optional.of(String.format("expected: onNext(%s); actual: %s",
							t,
							signal));
				}
				else if (!Objects.equals(t, signal.get())) {
					return Optional.of(String.format(
							"expected value: %s; actual value: %s",
							t,
							signal.get()));

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
	public ScriptedSubscriber.StepBuilder<T> expectNextAs(Iterable<? extends T> iterable) {
		Objects.requireNonNull(iterable, "iterable");
		this.script.add(new SignalSequenceEvent<>(iterable));
		return this;
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> expectNextCount(long count) {
		checkPositive(count);
		this.script.add(new SignalCountEvent<>(count));
		return this;
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> expectNextWith(Predicate<? super T> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnNext()) {
				return Optional.of(String.format("expected: onNext(); actual: %s",
						signal));
			}
			else if (!predicate.test(signal.get())) {
				return Optional.of(String.format("predicate failed on value: %s",
						signal.get()));
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return this;

	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> expectRecordedWith(Predicate<? super Collection<T>> predicate) {
		this.script.add(new CollectEvent<>(predicate));
		return this;
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> expectSubscription() {
		this.script.set(0, defaultFirstStep());
		return this;
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> expectSubscriptionWith(Predicate<? super Subscription> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		this.script.set(0, new SignalEvent<>(signal -> {
			if (!signal.isOnSubscribe()) {
				return Optional.of(String.format("expected: onSubscribe(); actual: %s",
						signal));
			}
			else if (!predicate.test(signal.getSubscription())) {
				return Optional.of(String.format("predicate failed on subscription: %s",
						signal.getSubscription()));
			}
			else {
				return Optional.empty();
			}
		}));
		return this;
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> recordWith(Supplier<? extends Collection<T>> supplier) {
		this.script.add(new CollectEvent<>(supplier));
		return this;
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> then(Runnable task) {
		Objects.requireNonNull(task, "task");
		this.script.add(new TaskEvent<>(task));
		return this;
	}

	@Override
	public ScriptedSubscriber<T> thenCancel() {
		this.script.add(new SubscriptionEvent<>());
		return build();
	}

	@Override
	public ScriptedSubscriber.StepBuilder<T> thenRequest(long n) {
		checkStrictlyPositive(n);
		this.script.add(new SubscriptionEvent<>(subscription -> subscription.request(n)));
		return this;
	}

	final ScriptedSubscriber<T> build() {
		Queue<Event<T>> copy = new ConcurrentLinkedQueue<>(this.script);
		return new DefaultScriptedSubscriber<>(copy,
				this.initialRequest,
				requestedFusionMode,
				expectedFusionMode);
	}

	@SuppressWarnings("unused")
	interface Event<T> {

	}

	final static class DefaultScriptedSubscriber<T> extends AtomicBoolean
			implements ScriptedSubscriber<T>, Trackable, Receiver {

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

		final AtomicReference<Subscription> subscription;
		final CountDownLatch                completeLatch;
		final Queue<Event<T>>               script;
		final long                          initialRequest;
		final int                           requestedFusionMode;
		final int                           expectedFusionMode;
		final List<String>                  failures;

		int                           establishedFusionMode;
		Fuseable.QueueSubscription<T> qs;
		long                          produced;
		Iterator<? extends T>         currentNextAs;
		Collection<T>                 currentCollector;

		@SuppressWarnings("unused")
		volatile int wip;

		DefaultScriptedSubscriber(Queue<Event<T>> script,
				long initialRequest,
				int requestedFusionMode,
				int expectedFusionMode) {
			this.script = script;
			this.requestedFusionMode = requestedFusionMode;
			this.expectedFusionMode =
					expectedFusionMode == -1 ? requestedFusionMode : expectedFusionMode;
			this.produced = 0L;
			this.initialRequest = initialRequest;
			this.failures = new LinkedList<>();
			this.completeLatch = new CountDownLatch(1);
			this.subscription = new AtomicReference<>();
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
					if(currentCollector != null){
						currentCollector.add(t);
					}
					onExpectation(Signal.next(t));
				}
			}
			else {
				produced++;
				if(currentCollector != null){
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
				if (requestedFusionMode >= Fuseable.NONE) {
					if (!startFusion(subscription)) {
						cancel();
						this.completeLatch.countDown();
					}
				}
				else if (this.initialRequest != 0L) {
					subscription.request(this.initialRequest);
				}
			}
			else {
				subscription.cancel();
				if (isCancelled()) {
					addFailure("an unexpected Subscription has been received: %s; " + "actual: cancelled",
							subscription);
				}
				else {
					addFailure("an unexpected Subscription has been received: %s; " + "actual: ",
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
			Instant now = Instant.now();
			try {
				pollTaskEventOrComplete(Duration.ZERO);
			}
			catch (InterruptedException ex) {
				Thread.currentThread()
				      .interrupt();
			}
			validate();
			return Duration.between(now, Instant.now());
		}

		@Override
		public Duration verify(Duration duration) {
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

		@Override
		public Duration verify(Publisher<? extends T> publisher) {
			precheckVerify(publisher);
			Instant now = Instant.now();
			publisher.subscribe(this);
			verify();
			return Duration.between(now, Instant.now());
		}

		@Override
		public Duration verify(Publisher<? extends T> publisher, Duration duration) {
			precheckVerify(publisher);
			Instant now = Instant.now();
			publisher.subscribe(this);
			verify(duration);
			return Duration.between(now, Instant.now());
		}

		final void addFailure(String msg, Object... arguments) {
			this.failures.add(String.format(msg, arguments));
		}

		final Subscription cancel() {
			Subscription s =
					this.subscription.getAndSet(Operators.cancelledSubscription());
			if (s != null && s != Operators.cancelledSubscription()) {
				s.cancel();
			}
			return s;
		}

		final Optional<String> checkCountMismatch(long expected, Signal<T> s) {
			if (!s.isOnNext()) {
				return Optional.of(String.format("expected: count = %s; actual: " + "produced = %s; " + "signal: %s",
						expected,
						produced,
						s));
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
					addFailure("expected collection; actual supplied is [null]");
					cancel();
					this.completeLatch.countDown();
				}
				return true;
			}
			c = this.currentCollector;

			if (c == null) {
				addFailure("expected record collector; actual record is [null]");
				cancel();
				this.completeLatch.countDown();
				return true;
			}

			Optional<String> error = collectEvent.test(c);
			if (error.isPresent()) {
				addFailure(error.get());
				cancel();
				this.completeLatch.countDown();
				return true;
			}
			return true;
		}

		@SuppressWarnings("unchecked")
		final void onExpectation(Signal<T> actualSignal) {
			try {
				Event<T> event = this.script.peek();
				if (event == null) {
					addFailure("did not expect: %s", actualSignal);
				}
				else if (event instanceof TaskEvent) {
					if (onTaskEvent()) {
						return;
					}
				}
				else if (event instanceof SignalCountEvent) {
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
					event = this.script.peek();
				}
			}
			catch (Throwable e) {
				Exceptions.throwIfFatal(e);
				String msg = e.getMessage() != null ? e.getMessage() : "";
				addFailure("failed running expectation with [%s]:\n%s",
						Exceptions.unwrap(e)
						          .getClass()
						          .getName(),
						msg);
				cancel();
				completeLatch.countDown();
				return;
			}
		}

		boolean onSignal(Signal<T> actualSignal) {
			SignalEvent<T> signalEvent = (SignalEvent<T>) this.script.poll();
			Optional<String> error = signalEvent.test(actualSignal);
			if (error.isPresent()) {
				this.failures.add(error.get());
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

			Optional<String> error = sequenceEvent.test(actualSignal, currentNextAs);

			if (error == EXPECT_MORE) {
				return false;
			}
			if (!error.isPresent()) {
				this.currentNextAs = null;
				this.script.poll();
			}
			else {
				this.failures.add(error.get());
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
					Optional<String> error =
							this.checkCountMismatch(event.count, actualSignal);

					if (error.isPresent()) {
						this.failures.add(error.get());
						cancel();
						this.completeLatch.countDown();
					}
				}
				return true;
			}
			return false;
		}

		boolean onTaskEvent() {
			Event<T> event;
			for (; ; ) {
				if (isCancelled()) {
					return true;
				}
				event = this.script.peek();
				if (!(event instanceof TaskEvent)) {
					break;
				}
				LockSupport.parkNanos(1_000);
			}
			return false;
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
				event = script.peek();
				if (event != null && event instanceof TaskEvent) {
					event = script.poll();
					skip = false;
					try {
						((TaskEvent<T>) event).run();
					}
					catch (Throwable t) {
						Exceptions.throwIfFatal(t);
						cancel();
						throw Exceptions.propagate(t);
					}
				}
				else if (!skip) {
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
								"ScriptedSubscriber has not been subscribed");
					}
					else {
						throw new AssertionError("ScriptedSubscriber timed out on " + upstream());
					}
				}
			}
		}

		void precheckVerify(Publisher<? extends T> publisher) {
			Objects.requireNonNull(publisher, "publisher");

			if (!compareAndSet(false, true)) {
				throw new IllegalStateException("The ScriptedSubscriber has already " + "been started");
			}
			if (requestedFusionMode >= Fuseable.NONE && !(publisher instanceof Fuseable)) {
				throw new AssertionError("The source publisher does not support fusion");
			}
		}

		final boolean startFusion(Subscription s) {
			if (s instanceof Fuseable.QueueSubscription) {
				@SuppressWarnings("unchecked") Fuseable.QueueSubscription<T> qs =
						(Fuseable.QueueSubscription<T>) s;

				this.qs = qs;

				int m = qs.requestFusion(requestedFusionMode);
				if ((m & expectedFusionMode) != m) {
					addFailure("expected fusion mode: %s; actual: %s",
							formatFusionMode(expectedFusionMode),
							formatFusionMode(m));
					return false;
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
							return false;
						}
						if (v == null) {
							onComplete();
							break;
						}

						onNext(v);
					}
				}
				else if (this.initialRequest != 0) {
					s.request(this.initialRequest);
				}
				return true;
			}
			else {
				addFailure("expected fusion-ready source but actual Subscription is " + "not: %s",
						expectedFusionMode,
						s);
				return false;
			}
		}

		final void validate() {
			if (!isStarted()) {
				throw new IllegalStateException(
						"ScriptedSubscriber has not been subscribed");
			}
			if (this.failures.isEmpty()) {
				return;
			}
			StringBuilder messageBuilder = new StringBuilder("Expectation failure(s):\n");
			this.failures.stream()
			             .flatMap(error -> Stream.of(" - ", error, "\n"))
			             .forEach(messageBuilder::append);

			messageBuilder.delete(messageBuilder.length() - 1, messageBuilder.length());
			throw new AssertionError(messageBuilder.toString());
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

		final Function<Signal<T>, Optional<String>> function;

		SignalEvent(Function<Signal<T>, Optional<String>> function) {
			this.function = function;
		}

		Optional<String> test(Signal<T> signal) {
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

		Optional<String> test(Collection<T> collection) {
			if (predicate != null) {
				if (!predicate.test(collection)) {
					return Optional.of(String.format("expected collection predicate" + " match;" + " actual: %s",
							collection));
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

static final class TaskEvent<T> implements Event<T> {

	final Runnable task;

	TaskEvent(Runnable task) {
		this.task = task;
	}

	void run() {
		task.run();
	}

}

static final class SignalSequenceEvent<T> implements Event<T> {

		final Iterable<? extends T> iterable;

		SignalSequenceEvent(Iterable<? extends T> iterable) {
			this.iterable = iterable;
		}

		Optional<String> test(Signal<T> signal, Iterator<? extends T> iterator) {
			if (signal.isOnNext()) {
				if (!iterator.hasNext()) {
					return Optional.of(String.format("unexpected iterator request; " + "onNext(%s); iterable: %s",
							signal.get(),
							iterable));
				}
				T d2 = iterator.next();
				if (!Objects.equals(signal.get(), d2)) {
					return Optional.of(String.format("expected : onNext(%s); actual: " + "%s; iterable: %s",
							d2, signal.get(), iterable));
				}
				return iterator.hasNext() ? EXPECT_MORE : Optional.empty();

			}
			if (iterator != null && iterator.hasNext() || signal.isOnError()) {
				return Optional.of(String.format("expected next value: %s; actual " + "actual signal: " + "%s; iterable: %s",
						iterator != null && iterator.hasNext() ? iterator.next() : "none",
						signal,
						iterable));
			}
			return Optional.empty();
		}
	}

	static final AtomicIntegerFieldUpdater<DefaultScriptedSubscriber> WIP                      =
			AtomicIntegerFieldUpdater.newUpdater(DefaultScriptedSubscriber.class, "wip");
	static final Optional<String>                                     EXPECT_MORE              =
			Optional.empty();
	static final SignalEvent
	                                                                  DEFAULT_ONSUBSCRIBE_STEP =
			new SignalEvent<>(signal -> {
				if (!signal.isOnSubscribe()) {
					return Optional.of(String.format("expected: onSubscribe(); actual: %s",
							signal));
				}
				else {
					return Optional.empty();
				}
			});
}
