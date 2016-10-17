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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
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

	final long expectedValueCount;


	DefaultScriptedSubscriberBuilder(long initialRequest) {
		this(initialRequest, -1);
	}

	DefaultScriptedSubscriberBuilder(long initialRequest, long expectedValueCount) {
		this.initialRequest = initialRequest;
		this.expectedValueCount = expectedValueCount;

		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnSubscribe()) {
				return Optional.of(String.format("expected: onSubscribe(); actual: %s", signal));
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
	}

	static void checkForNegative(long n) {
		if (n < 0) {
			throw new IllegalArgumentException("'n' should be >= 0 but was " + n);
		}
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> advanceTime() {
		this.script.add(new VirtualTimeEvent<>(() -> TestScheduler.get()
		                                                          .advanceTime()));
		return this;
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> advanceTimeBy(Duration timeshift) {
		this.script.add(new VirtualTimeEvent<>(() -> TestScheduler.get()
		                                                          .advanceTimeBy(timeshift.toNanos(),
				                                                          TimeUnit.NANOSECONDS)));
		return this;
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> advanceTimeTo(Instant instant) {

		this.script.add(new VirtualTimeEvent<>(() -> TestScheduler.get()
		                                                          .advanceTimeTo(instant.toEpochMilli(),
				                                                          TimeUnit.MILLISECONDS)));
		return this;
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> expectValue(T t) {
		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnNext()) {
				return Optional.of(String.format("expected: onNext(%s); actual: %s", t, signal));
			}
			else if (!Objects.equals(t, signal.get())) {
				return Optional.of(String.format("expected value: %s; actual value: %s", t,
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
		Arrays.stream(ts).forEach(this::expectValue);
		return this;
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> expectValueWith(Predicate<T> predicate) {

		SignalEvent<T> event = new SignalEvent<>(signal -> {
			if (!signal.isOnNext()) {
				return Optional.of(String.format("expected: onNext(); actual: %s", signal));
			}
			else if (!predicate.test(signal.get())) {
				return Optional.of(String.format("predicate failed on value: %s", signal.get()));
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
				return Optional.of(String.format("expected: onNext(); actual: %s", signal));
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
				return Optional.of(String.format("expected: onError(); actual: %s", signal));
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
						clazz.getSimpleName(), signal));
			}
			else if (!clazz.isInstance(signal.getThrowable())) {
				return Optional.of(String.format("expected error of type: %s; actual type: %s",
						clazz.getSimpleName(), signal.getThrowable()));
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
				return Optional.of(String.format("expected: onError(); actual: %s", signal));
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
				return Optional.of(String.format("expected: onError(); actual: %s", signal));
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
				return Optional.of(String.format("expected: onComplete(); actual: %s", signal));
			}
			else {
				return Optional.empty();
			}
		});
		this.script.add(event);
		return build();
	}

	@Override
	public ScriptedSubscriber.ValueBuilder<T> doRequest(long n) {
		checkForNegative(n);
		this.script.add(new SubscriptionEvent<T>(subscription -> subscription.request(n), false));
		return this;
	}

	@Override
	public ScriptedSubscriber<T> doCancel() {
		this.script.add(new SubscriptionEvent<T>(Subscription::cancel, true));
		return build();
	}

	final ScriptedSubscriber<T> build() {
		Queue<Event<T>> copy = new LinkedList<>(this.script);
		return new DefaultScriptedSubscriber<T>(copy, this.initialRequest, this.expectedValueCount);
	}

	final static class DefaultScriptedSubscriber<T> implements ScriptedSubscriber<T> {

		final AtomicReference<Subscription> subscription = new AtomicReference<>();

		final CountDownLatch completeLatch = new CountDownLatch(1);

		final Queue<Event<T>> script;

		final long initialRequest;

		final List<String> failures = new LinkedList<>();

		final AtomicLong valueCount = new AtomicLong();

		final long expectedValueCount;


		public DefaultScriptedSubscriber(Queue<Event<T>> script, long initialRequest, long expectedValueCount) {
			this.script = script;
			this.initialRequest = initialRequest;
			this.expectedValueCount = expectedValueCount;
		}

		@Override
		public void onSubscribe(Subscription subscription) {
			Objects.requireNonNull(subscription, "Subscription cannot be null");

			if (this.subscription.compareAndSet(null, subscription)) {
				checkExpectation(Signal.subscribe(subscription));
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
			if (this.expectedValueCount < 0) {
				checkExpectation(Signal.next(t));
			} else {
				this.valueCount.incrementAndGet();
			}
		}

		@Override
		public void onError(Throwable t) {
			checkExpectation(Signal.error(t));
			this.completeLatch.countDown();
		}

		@Override
		public void onComplete() {
			checkExpectation(Signal.complete());
			this.completeLatch.countDown();
		}

		final void addFailure(String msg, Object... arguments) {
			this.failures.add(String.format(msg, arguments));
		}

		@SuppressWarnings("unchecked")
		final void checkExpectation(Signal<T> actualSignal) {
			Event<T> event = this.script.poll();
			if (event == null) {
				addFailure("did not expect: %s", actualSignal);
			}
			else {
				SignalEvent<T> signalEvent = (SignalEvent<T>) event;
				Optional<String> error = signalEvent.test(actualSignal);
				error.ifPresent(this.failures::add);
			}

			for(;;) {
				event = this.script.peek();
				if (event == null || event instanceof SignalEvent) {
					break;
				}
				else if (event instanceof DefaultScriptedSubscriberBuilder.VirtualTimeEvent) {
					if (!actualSignal.isOnSubscribe()) {
						VirtualTimeEvent<T> virtualTimeEvent =
								(VirtualTimeEvent<T>) this.script.poll();
						virtualTimeEvent.run();
					}
					else{
						break;
					}
				}
				else {
					SubscriptionEvent<T> subscriptionEvent = (SubscriptionEvent<T>) this.script.poll();
					Subscription s = this.subscription.get();
					subscriptionEvent.consume(s);
					if (subscriptionEvent.isTerminal()) {
						this.completeLatch.countDown();
						break;
					}
				}
			}

		}

		final void checkVirtualTimeEvent(){
			Event<T> event;
			for(;;){
				event = this.script.peek();
				if (event == null || !(event instanceof VirtualTimeEvent)) {
					break;
				}
				else {
						VirtualTimeEvent<T> virtualTimeEvent =
								(VirtualTimeEvent<T>) this.script.poll();
						virtualTimeEvent.run();
					}
			}
		}

		@Override
		public void verify() {
			checkVirtualTimeEvent();
			try {
				this.completeLatch.await();
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			if (this.subscription.get() == null) {
				throw new IllegalStateException("ScriptedSubscriber has not been subscribed");
			}
			verifyInternal();
		}

		@Override
		public void verify(Publisher<? extends T> publisher) {
			publisher.subscribe(this);
			verify();
		}

		@Override
		public void verify(Duration duration) {
			try {
				if (!this.completeLatch.await(duration.toMillis(), TimeUnit.MILLISECONDS)) {
					if (this.subscription.get() == null) {
						throw new IllegalStateException("ScriptedSubscriber has not been subscribed");
					}
					else {
						throw new AssertionError("ScriptedSubscriber timed out on " +
								this.subscription.get());
					}
				}
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			if (this.subscription.get() == null) {
				throw new IllegalStateException("ScriptedSubscriber has not been subscribed");
			}
			verifyInternal();
		}

		@Override
		public void verify(Publisher<? extends T> publisher, Duration duration) {
			publisher.subscribe(this);
			checkTasks();
			verify(duration);
		}

		final void checkTasks() {

		}

		final void verifyInternal() {
			boolean validValueCount = hasValidValueCount();
			if (this.failures.isEmpty() && validValueCount) {
				return;
			}
			StringBuilder messageBuilder = new StringBuilder("Expectation failure(s):\n");
			this.failures.stream()
					.flatMap(error -> Stream.of(" - ", error, "\n"))
					.forEach(messageBuilder::append);
			if (!validValueCount) {
				messageBuilder.append(String.format(" - expected %d values; got %d values",
						this.expectedValueCount, this.valueCount.get()));
			}
			messageBuilder.delete(messageBuilder.length() - 1, messageBuilder.length());
			throw new AssertionError(messageBuilder.toString());
		}

		final boolean hasValidValueCount() {
			if (this.expectedValueCount < 0) {
				return true;
			}
			else {
				return this.expectedValueCount == this.valueCount.get();
			}
		}

	}

	@SuppressWarnings("unused")
	abstract static class Event<T> {
	}

	static final class SubscriptionEvent<T> extends Event<T> {

		final Consumer<Subscription> consumer;

		final boolean terminal;

		public SubscriptionEvent(Consumer<Subscription> consumer, boolean terminal) {
			this.consumer = consumer;
			this.terminal = terminal;
		}

		public void consume(Subscription subscription) {
			this.consumer.accept(subscription);
		}

		public boolean isTerminal() {
			return this.terminal;
		}
	}

	static final class SignalEvent<T> extends Event<T> {

		final Function<Signal<T>, Optional<String>> function;

		public SignalEvent(Function<Signal<T>, Optional<String>> function) {
			this.function = function;
		}

		public Optional<String> test(Signal<T> signal) {
			return this.function.apply(signal);
		}

	}

	static final class VirtualTimeEvent<T> extends Event<T> {

		final Runnable task;

		public VirtualTimeEvent(Runnable task) {
			this.task = task;
		}

		public void run() {
			task.run();
		}

	}

}
