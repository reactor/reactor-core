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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.test.scheduler.TestScheduler;

/**
 * Default implementation of {@link ScriptedSubscriber.ValueBuilder} and
 * {@link ScriptedSubscriber.TerminationBuilder}.
 *
 * @author Arjen Poutsma
 * @since 1.0
 */
final class DefaultScriptedSubscriberBuilder<T>
		implements ScriptedSubscriber.ValueBuilder<T> {

	final List<Event<T>> script = new ArrayList<>();

	final long initialRequest;

	DefaultScriptedSubscriberBuilder(long initialRequest) {
		this.initialRequest = initialRequest;

		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnSubscribe()) {
				return Optional.of(String.format("expected: onSubscribe(); actual: %s",
						signal));
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
	}

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

	@Override
	public ScriptedSubscriber.ValueBuilder<T> advanceTime() {
		this.script.add(new TaskEvent<>(() -> TestScheduler.get()
		                                                   .advanceTime()));
		return this;
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> advanceTimeBy(Duration timeshift) {
		this.script.add(new TaskEvent<>(() -> TestScheduler.get()
		                                                   .advanceTimeBy(timeshift.toNanos(),
				                                                   TimeUnit.NANOSECONDS)));
		return this;
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> advanceTimeTo(Instant instant) {

		this.script.add(new TaskEvent<>(() -> TestScheduler.get()
		                                                   .advanceTimeTo(instant.toEpochMilli(),
				                                                   TimeUnit.MILLISECONDS)));
		return this;
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> expectValue(T t) {
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnNext()) {
				return Optional.of(String.format("expected: onNext(%s); actual: %s",
						t,
						signal));
			}
			else if (!Objects.equals(t, signal.get())) {
				return Optional.of(String.format("expected value: %s; actual value: %s",
						t,
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
	public ScriptedSubscriber.ValueBuilder<T> expectValues(T... ts) {
		Arrays.stream(ts)
		      .forEach(this::expectValue);
		return this;
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> expectValueWith(Predicate<T> predicate) {

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
	public ScriptedSubscriber.ValueBuilder<T> consumeValueWith(Consumer<T> consumer) {
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
					return Optional.of(assertion.getMessage());
				}
			}
		});
		this.script.add(event);
		return this;
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
	public ScriptedSubscriber<T> consumeErrorWith(Consumer<Throwable> consumer) {
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
					return Optional.of(assertion.getMessage());
				}
			}
		});
		this.script.add(event);
		return build();
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
	public ScriptedSubscriber.ValueBuilder<T> expectValueCount(long count) {
		checkPositive(count);
		this.script.add(new SignalCountEvent<>(count));
		return this;
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> doRequest(long n) {
		checkStrictlyPositive(n);
		this.script.add(new SubscriptionEvent<>(subscription -> subscription.request(n)));
		return this;
	}

	@Override
	public ScriptedSubscriber<T> doCancel() {
		this.script.add(new SubscriptionEvent<>());
		return build();
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> then(Runnable task) {
		Objects.requireNonNull(task, "task");
		this.script.add(new TaskEvent<>(task));
		return this;
	}

	final ScriptedSubscriber<T> build() {
		Queue<Event<T>> copy = new ConcurrentLinkedQueue<>(this.script);
		return new DefaultScriptedSubscriber<>(copy, this.initialRequest);
	}

	final static class DefaultScriptedSubscriber<T>
			implements ScriptedSubscriber<T>, Trackable, Receiver {

		final AtomicReference<Subscription> subscription = new AtomicReference<>();

		final CountDownLatch completeLatch = new CountDownLatch(1);

		final Queue<Event<T>> script;

		final long initialRequest;

		final List<String> failures = new LinkedList<>();

		long produced;

		volatile int wip;

		public DefaultScriptedSubscriber(Queue<Event<T>> script, long initialRequest) {
			this.script = script;
			this.produced = 0L;
			this.initialRequest = initialRequest;
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
		public void onSubscribe(Subscription subscription) {
			Objects.requireNonNull(subscription, "Subscription cannot be null");

			if (this.subscription.compareAndSet(null, subscription)) {
				onExpectation(Signal.subscribe(subscription));
				if (this.initialRequest != 0L) {
					subscription.request(this.initialRequest);
				}
			}
			else {
				subscription.cancel();
			}
		}

		@Override
		public void onNext(T t) {
			produced++;
			onExpectation(Signal.next(t));
		}

		@Override
		public void onError(Throwable t) {
			onExpectation(Signal.error(t));
			this.completeLatch.countDown();
		}

		@Override
		public void onComplete() {
			onExpectation(Signal.complete());
			this.completeLatch.countDown();
		}

		final void addFailure(String msg, Object... arguments) {
			this.failures.add(String.format(msg, arguments));
		}

		@Override
		public Subscription upstream() {
			return this.subscription.get();
		}

		@SuppressWarnings("unchecked")
		final void onExpectation(Signal<T> actualSignal) {
			Event<T> event = this.script.peek();
			if (event == null) {
				addFailure("did not expect: %s", actualSignal);
			}
			else if (event instanceof SignalCountEvent) {
				SignalCountEvent<T> countEvent = (SignalCountEvent) event;

				if (countEvent.test(produced)) {
					this.script.poll();
					produced = 0L;
				}
				else {
					if (countEvent.count != 0) {
						Optional<String> error = this.checkCountMismatch(countEvent
								.count, actualSignal);

						if(error.isPresent()){
							this.failures.add(error.get());
							cancel();
							this.completeLatch.countDown();
						}
					}
					return;
				}

			}
			else if (event instanceof SignalEvent) {

				SignalEvent<T> signalEvent = (SignalEvent<T>) this.script.poll();
				Optional<String> error = signalEvent.test(actualSignal);
				if (error.isPresent()) {
					this.failures.add(error.get());
					cancel();
					this.completeLatch.countDown();
					return;
				}
			}

			event = this.script.peek();
			if (event == null || !(event instanceof SubscriptionEvent)) {
				return;
			}

			drainSubscriptionOperations(event);
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

		final void drainSubscriptionOperations(Event<T> event) {
			int missed = WIP.incrementAndGet(this);
			if (missed == 1) {
				for (; ; ) {
					for (; ; ) {
						if (event == null || !(event instanceof SubscriptionEvent)) {
							break;
						}
						SubscriptionEvent<T> subscriptionEvent =
								(SubscriptionEvent<T>) this.script.poll();
						if (subscriptionEvent.isTerminal()) {
							cancel();
							this.completeLatch.countDown();
							return;
						}
						subscriptionEvent.consume(upstream());
						event = this.script.peek();
					}
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}

			}
		}

		final Subscription cancel() {
			Subscription s =
					this.subscription.getAndSet(Operators.cancelledSubscription());
			if (s != null && s != Operators.cancelledSubscription()) {
				s.cancel();
			}
			return s;
		}

		@SuppressWarnings("unchecked")
		final void pollTaskEventOrComplete(Duration timeout) throws InterruptedException {
			Objects.requireNonNull(timeout, "timeout");
			Event<T> event;
			Instant stop = Instant.now()
			                      .plus(timeout);
			boolean skipSubscriptionOp = true;
			for (; ; ) {
				event = script.peek();
				if (event != null) {
					if (event instanceof TaskEvent) {
						skipSubscriptionOp = false;
						event = script.poll();
						try {
							((TaskEvent<T>) event).run();
						}
						catch (Throwable t) {
							Exceptions.throwIfFatal(t);
							cancel();
						}
					}
					else if (!skipSubscriptionOp) {
						drainSubscriptionOperations(event);
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

		@Override
		public void verify() {
			try {
				pollTaskEventOrComplete(Duration.ZERO);
			}
			catch (InterruptedException ex) {
				Thread.currentThread()
				      .interrupt();
			}
			validate();
		}

		@Override
		public void verify(Publisher<? extends T> publisher) {
			publisher.subscribe(this);
			verify();
		}

		@Override
		public void verify(Duration duration) {
			try {
				pollTaskEventOrComplete(duration);
			}
			catch (InterruptedException ex) {
				Thread.currentThread()
				      .interrupt();
			}
			validate();
		}

		@Override
		public void verify(Publisher<? extends T> publisher, Duration duration) {
			publisher.subscribe(this);
			verify(duration);
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

	static final AtomicIntegerFieldUpdater<DefaultScriptedSubscriber> WIP =
			AtomicIntegerFieldUpdater.newUpdater(DefaultScriptedSubscriber.class, "wip");

	@SuppressWarnings("unused")
	abstract static class Event<T> {

	}

	static final class SubscriptionEvent<T> extends Event<T> {

		final Consumer<Subscription> consumer;

		SubscriptionEvent() {
			this(null);
		}

		SubscriptionEvent(Consumer<Subscription> consumer) {
			this.consumer = consumer;
		}

		void consume(Subscription subscription) {
			if(consumer != null) {
				this.consumer.accept(subscription);
			}
		}

		boolean isTerminal() {
			return consumer == null;
		}
	}

	static final class SignalEvent<T> extends Event<T> {

		final Function<Signal<T>, Optional<String>> function;

		SignalEvent(Function<Signal<T>, Optional<String>> function) {
			this.function = function;
		}

		Optional<String> test(Signal<T> signal) {
			return this.function.apply(signal);
		}

	}

	static final class SignalCountEvent<T> extends Event<T> {

		final long count;

		SignalCountEvent(long count) {
			this.count = count;
		}

		boolean test(long current) {
			return current >= count;
		}

	}

	static final class TaskEvent<T> extends Event<T> {

		final Runnable task;

		TaskEvent(Runnable task) {
			this.task = task;
		}

		void run() {
			task.run();
		}

	}

}
