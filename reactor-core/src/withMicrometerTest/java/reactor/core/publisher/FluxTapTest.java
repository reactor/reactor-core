/*
 * Copyright (c) 2022-2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.Scannable;
import reactor.core.Scannable.Attr;
import reactor.core.Scannable.Attr.RunStyle;
import reactor.test.ParameterizedTestWithName;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.TestSubscriber;
import reactor.util.context.Context;
import reactor.util.context.ContextView;
import reactor.core.observability.SignalListener;
import reactor.core.observability.SignalListenerFactory;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Simon Baslé
 */
class FluxTapTest {

	private static class TestSignalListener<T> implements SignalListener<T> {

		/**
		 * The String representation of the events, or doOnXxx methods.
		 */
		public final Deque<String> events = new ConcurrentLinkedDeque<>();

		/**
		 * The errors passed to the {@link #handleListenerError(Throwable)} hook. Unused by default.
		 */
		public final Deque<Throwable> listenerErrors = new ConcurrentLinkedDeque<>();

		@Override
		public void doFirst() throws Throwable {
			events.offer("doFirst");
		}

		@Override
		public void doFinally(SignalType terminationType) throws Throwable {
			events.offer("doFinally:" + terminationType.name());
		}

		@Override
		public void doOnSubscription() throws Throwable {
			events.offer("doOnSubscription");
		}

		@Override
		public void doOnFusion(int negotiatedFusion) throws Throwable {
			events.offer("doOnFusion:" + Fuseable.fusionModeName(negotiatedFusion));
		}

		@Override
		public void doOnRequest(long requested) throws Throwable {
			events.offer("doOnRequest:" + (requested == Long.MAX_VALUE ? "unbounded" : requested));
		}

		@Override
		public void doOnCancel() throws Throwable {
			events.offer("doOnCancel");
		}

		@Override
		public void doOnNext(T value) throws Throwable {
			events.offer("doOnNext:" + value);
		}

		@Override
		public void doOnComplete() throws Throwable {
			events.offer("doOnComplete");
		}

		@Override
		public void doOnError(Throwable error) throws Throwable {
			events.offer("doOnError:" + error);
		}

		@Override
		public void doAfterComplete() throws Throwable {
			events.offer("doAfterComplete");
		}

		@Override
		public void doAfterError(Throwable error) throws Throwable {
			events.offer("doAfterError:" + error);
		}

		@Override
		public void doOnMalformedOnNext(T value) throws Throwable {
			events.offer("doOnMalformedOnNext:" + value);
		}

		@Override
		public void doOnMalformedOnError(Throwable error) throws Throwable {
			events.offer("doOnMalformedOnError:" + error);
		}

		@Override
		public void doOnMalformedOnComplete() throws Throwable {
			events.offer("doOnMalformedOnComplete");
		}

		@Override
		public void handleListenerError(Throwable listenerError) {
			listenerErrors.offer(listenerError);
		}
	}

	private static final <T> TestSingletonFactory<T> factoryOf(TestSignalListener<T> listener) {
		return new TestSingletonFactory<>(listener);
	}

	private static <T> TestSingletonFactory<T> ignoredFactory() {
		return new TestSingletonFactory<>(new TestSignalListener<T>());
	}

	private static final class TestSingletonFactory<T> implements SignalListenerFactory<T, Void> {

		final TestSignalListener<T> singleton;

		private TestSingletonFactory(TestSignalListener<T> singleton) {
			this.singleton = singleton;
		}

		@Override
		public Void initializePublisherState(Publisher<? extends T> source) {
			return null;
		}

		@Override
		public SignalListener<T> createListener(Publisher<? extends T> source, ContextView listenerContext,
													  Void publisherContext) {
			return singleton;
		}
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = {true, false})
	void scenarioTerminatingOnComplete(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		TestSignalListener<Integer> testSignalListener = new TestSignalListener<>();

		Flux<Integer> fullFlux = Flux.just(1, 2, 3).hide();

		fullFlux.tap(() -> testSignalListener)
			.subscribeWith(TestSubscriber.create());

		assertThat(testSignalListener.listenerErrors).as("listener errors").isEmpty();

		assertThat(testSignalListener.events)
			.containsExactly(
				"doFirst",
				"doOnSubscription",
				"doOnRequest:unbounded",
				"doOnNext:1",
				"doOnNext:2",
				"doOnNext:3",
				"doOnComplete",
				"doAfterComplete",
				"doFinally:ON_COMPLETE"
			);
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = {true, false})
	void scenarioTerminatingOnError(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		TestSignalListener<Integer> testSignalListener = new TestSignalListener<>();
		RuntimeException expectedError = new RuntimeException("expected");

		Flux<Integer> fullFlux = Flux.just(1, 2, 3).concatWith(Mono.error(expectedError)).hide();

		fullFlux.tap(() -> testSignalListener)
			.subscribeWith(TestSubscriber.create());

		assertThat(testSignalListener.listenerErrors).as("listener errors").isEmpty();

		assertThat(testSignalListener.events)
			.containsExactly(
				"doFirst",
				"doOnSubscription",
				"doOnRequest:unbounded",
				"doOnNext:1",
				"doOnNext:2",
				"doOnNext:3",
				"doOnError:" + expectedError,
				"doAfterError:" + expectedError,
				"doFinally:ON_ERROR"
			);
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = {true, false})
	void multipleRequests(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		TestSignalListener<Integer> testSignalListener = new TestSignalListener<>();
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder().initialRequest(0L).build();

		Flux<Integer> fullFlux = Flux.just(1, 2, 3).hide();

		fullFlux.tap(() -> testSignalListener)
			.subscribeWith(testSubscriber);

		assertThat(testSignalListener.listenerErrors).as("listener errors").isEmpty();

		assertThat(testSignalListener.events)
			.as("before first request")
			.containsExactly(
				"doFirst",
				"doOnSubscription"
			);

		testSubscriber.request(1);
		assertThat(testSignalListener.events)
			.as("first request")
			.hasSize(4)
			.endsWith(
				"doOnRequest:1",
				"doOnNext:1"
			);

		testSubscriber.request(1);
		assertThat(testSignalListener.events)
			.as("second request")
			.hasSize(6)
			.endsWith(
				"doOnRequest:1",
				"doOnNext:2"
			);

		testSubscriber.request(10);
		assertThat(testSignalListener.events)
			.as("final request")
			.containsExactly(
				"doFirst",
				"doOnSubscription",
				"doOnRequest:1",
				"doOnNext:1",
				"doOnRequest:1",
				"doOnNext:2",
				"doOnRequest:10",
				"doOnNext:3",
				"doOnComplete",
				"doAfterComplete",
				"doFinally:ON_COMPLETE"
			);
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = {true, false})
	void withCancellation(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		TestSignalListener<Integer> testSignalListener = new TestSignalListener<>();
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder().initialRequest(0L).build();

		Flux<Integer> fullFlux = Flux.just(1, 2, 3).hide();

		fullFlux.tap(() -> testSignalListener)
			.subscribeWith(testSubscriber);

		assertThat(testSignalListener.listenerErrors).as("listener errors").isEmpty();

		testSubscriber.request(2);

		assertThat(testSignalListener.events)
			.as("partial request")
			.containsExactly(
				"doFirst",
				"doOnSubscription",
				"doOnRequest:2",
				"doOnNext:1",
				"doOnNext:2"
			);

		testSubscriber.cancel();

		assertThat(testSignalListener.events)
			.as("cancelled")
			.containsExactly(
				"doFirst",
				"doOnSubscription",
				"doOnRequest:2",
				"doOnNext:1",
				"doOnNext:2",
				"doOnCancel",
				"doFinally:CANCEL"
			);
	}

	@ParameterizedTestWithName
	@CsvSource({
			"ON_COMPLETE, true",
			"ON_COMPLETE, false",
			"ON_ERROR, true",
			"ON_ERROR, false",
	})
	void malformedOnNext(SignalType termination, boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		AtomicReference<Object> dropped = new AtomicReference<>();
		Hooks.onNextDropped(dropped::set);

		TestSignalListener<Integer> testSignalListener = new TestSignalListener<>();
		TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		TestSubscriber<Integer> ignored = TestSubscriber.create();

		testPublisher.flux().hide()
			.tap(() -> testSignalListener)
			.subscribeWith(ignored);

		testPublisher.next(1, 2, 3);

		if (termination == SignalType.ON_COMPLETE) {
			testPublisher.complete();
			testPublisher.next(-1);

			assertThat(testSignalListener.events)
				.containsExactly(
					"doFirst",
					"doOnSubscription",
					"doOnRequest:unbounded",
					"doOnNext:1",
					"doOnNext:2",
					"doOnNext:3",
					"doOnComplete",
					"doAfterComplete",
					"doFinally:ON_COMPLETE",
					"doOnMalformedOnNext:-1"
				);
		}
		else {
			Throwable errorTermination = new RuntimeException("onError termination");
			testPublisher.error(errorTermination);
			testPublisher.next(-1);


			assertThat(testSignalListener.events)
				.containsExactly(
					"doFirst",
					"doOnSubscription",
					"doOnRequest:unbounded",
					"doOnNext:1",
					"doOnNext:2",
					"doOnNext:3",
					"doOnError:" + errorTermination,
					"doAfterError:" + errorTermination,
					"doFinally:ON_ERROR",
					"doOnMalformedOnNext:-1"
				);
		}

		assertThat(testSignalListener.listenerErrors).as("listener errors").isEmpty();
		assertThat(dropped).as("dropped onNext").hasValue(-1);
	}

	@ParameterizedTestWithName
	@CsvSource({
			"ON_COMPLETE, true",
			"ON_COMPLETE, false",
			"ON_ERROR, true",
			"ON_ERROR, false",
	})
	void malformedOnComplete(SignalType termination, boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		TestSignalListener<Integer> testSignalListener = new TestSignalListener<>();
		TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		TestSubscriber<Integer> ignored = TestSubscriber.create();

		testPublisher.flux().hide()
			.tap(() -> testSignalListener)
			.subscribeWith(ignored);

		testPublisher.next(1, 2, 3);

		if (termination == SignalType.ON_COMPLETE) {
			testPublisher.complete();
			testPublisher.complete();

			assertThat(testSignalListener.events)
				.containsExactly(
					"doFirst",
					"doOnSubscription",
					"doOnRequest:unbounded",
					"doOnNext:1",
					"doOnNext:2",
					"doOnNext:3",
					"doOnComplete",
					"doAfterComplete",
					"doFinally:ON_COMPLETE",
					"doOnMalformedOnComplete"
				);
		}
		else {
			Exception errorTermination = new RuntimeException("onError termination");
			testPublisher.error(errorTermination);
			testPublisher.complete();

			assertThat(testSignalListener.events)
				.containsExactly(
					"doFirst",
					"doOnSubscription",
					"doOnRequest:unbounded",
					"doOnNext:1",
					"doOnNext:2",
					"doOnNext:3",
					"doOnError:" + errorTermination,
					"doAfterError:" + errorTermination,
					"doFinally:ON_ERROR",
					"doOnMalformedOnComplete"
				);
		}

		assertThat(testSignalListener.listenerErrors).as("listener errors").isEmpty();
	}

	@ParameterizedTestWithName
	@CsvSource({
			"ON_COMPLETE, true",
			"ON_COMPLETE, false",
			"ON_ERROR, true",
			"ON_ERROR, false",
	})
	void malformedOnError(SignalType termination, boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		AtomicReference<Throwable> dropped = new AtomicReference<>();
		Hooks.onErrorDropped(dropped::set);

		TestSignalListener<Integer> testSignalListener = new TestSignalListener<>();
		TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		TestSubscriber<Integer> ignored = TestSubscriber.create();
		Throwable malformedError = new RuntimeException("expected malformed onError");

		testPublisher.flux().hide()
			.tap(() -> testSignalListener)
			.subscribeWith(ignored);

		testPublisher.next(1, 2, 3);

		if (termination == SignalType.ON_COMPLETE) {
			testPublisher.complete();
			testPublisher.error(malformedError);

			assertThat(testSignalListener.events)
				.containsExactly(
					"doFirst",
					"doOnSubscription",
					"doOnRequest:unbounded",
					"doOnNext:1",
					"doOnNext:2",
					"doOnNext:3",
					"doOnComplete",
					"doAfterComplete",
					"doFinally:ON_COMPLETE",
					"doOnMalformedOnError:" + malformedError
				);
		}
		else {
			Exception errorTermination = new RuntimeException("onError termination");
			testPublisher.error(errorTermination);
			testPublisher.error(malformedError);

			assertThat(testSignalListener.events)
				.containsExactly(
					"doFirst",
					"doOnSubscription",
					"doOnRequest:unbounded",
					"doOnNext:1",
					"doOnNext:2",
					"doOnNext:3",
					"doOnError:" + errorTermination,
					"doAfterError:" + errorTermination,
					"doFinally:ON_ERROR",
					"doOnMalformedOnError:" + malformedError
				);
		}

		assertThat(testSignalListener.listenerErrors).as("listener errors").isEmpty();
		assertThat(dropped).as("malformed error was dropped").hasValue(malformedError);
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = {true, false})
	void throwingCreateListener(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		SignalListenerFactory<Integer, Void> listenerFactory = new SignalListenerFactory<Integer, Void>() {
			@Override
			public Void initializePublisherState(Publisher<? extends Integer> source) {
				return null;
			}

			@Override
			public SignalListener<Integer> createListener(Publisher<? extends Integer> source,
														  ContextView listenerContext, Void publisherContext) {
				throw new IllegalStateException("expected");
			}
		};

		if (automatic) {
			FluxTapRestoringThreadLocals<Integer, Void> test =
					new FluxTapRestoringThreadLocals<>(Flux.just(1), listenerFactory);
			assertThatCode(() -> test.subscribe(testSubscriber))
					.doesNotThrowAnyException();
		} else {
			FluxTap<Integer, Void> test = new FluxTap<>(Flux.just(1), listenerFactory);
			assertThatCode(() -> test.subscribeOrReturn(testSubscriber))
					.doesNotThrowAnyException();
		}

		assertThat(testSubscriber.expectTerminalError())
			.as("downstream error")
			.isInstanceOf(IllegalStateException.class)
			.hasMessage("expected");
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = {true, false})
	void doFirstListenerError(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		Throwable listenerError = new IllegalStateException("expected from doFirst");

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		TestSignalListener<Integer> listener = new TestSignalListener<Integer>() {
			@Override
			public void doFirst() throws Throwable {
				throw listenerError;
			}
		};

		if (automatic) {
			FluxTapRestoringThreadLocals<Integer, Void> test =
					new FluxTapRestoringThreadLocals<>(Flux.just(1), factoryOf(listener));

			assertThatCode(() -> test.subscribe(testSubscriber))
					.doesNotThrowAnyException();
		} else {
			FluxTap<Integer, Void> test = new FluxTap<>(Flux.just(1), factoryOf(listener));

			assertThatCode(() -> test.subscribeOrReturn(testSubscriber))
					.doesNotThrowAnyException();
		}

		assertThat(listener.listenerErrors)
			.as("listenerErrors")
			.containsExactly(listenerError);

		assertThat(listener.events)
			.as("events")
			.isEmpty();
	}

	@Nested
	class FluxTapFuseableTest {

		@Test
		void implementationSmokeTest() {
			Flux<Integer> fuseableSource = Flux.just(1);
			//the TestSubscriber "requireFusion" configuration below is intentionally inverted
			//so that an exception describing the actual Subscription is thrown when calling block()
			TestSubscriber<Integer> testSubscriberForFuseable = TestSubscriber.builder().requireNotFuseable().build();
			Flux<Integer> fuseable = fuseableSource.tap(TestSignalListener::new);

			assertThat(fuseableSource).as("smoke test fuseableSource").isInstanceOf(Fuseable.class);
			assertThat(fuseable).as("fuseable").isInstanceOf(FluxTapFuseable.class);

			assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> fuseable.subscribeWith(testSubscriberForFuseable).block())
				.as("TapFuseableSubscriber")
				.withMessageContaining("got reactor.core.publisher.FluxTapFuseable$TapFuseableSubscriber");
		}

		@Test
		void throwingCreateListener() {
			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
			FluxTapFuseable<Integer, Void> test = new FluxTapFuseable<>(Flux.just(1),
				new SignalListenerFactory<Integer, Void>() {
					@Override
					public Void initializePublisherState(Publisher<? extends Integer> source) {
						return null;
					}

					@Override
					public SignalListener<Integer> createListener(Publisher<? extends Integer> source,
																  ContextView listenerContext, Void publisherContext) {
						throw new IllegalStateException("expected");
					}
				});

			assertThatCode(() -> test.subscribeOrReturn(testSubscriber))
				.doesNotThrowAnyException();

			assertThat(testSubscriber.expectTerminalError())
				.as("downstream error")
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("expected");
		}

		//doFirst is invoked from each publisher's subscribeOrReturn
		@Test
		void doFirst() {
			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
			TestSignalListener<Integer> listener = new TestSignalListener<>();

			FluxTapFuseable<Integer, Void> test = new FluxTapFuseable<>(Flux.just(1), factoryOf(listener));

			assertThatCode(() -> test.subscribeOrReturn(testSubscriber))
				.doesNotThrowAnyException();

			assertThat(listener.listenerErrors).as("listenerErrors").isEmpty();
			assertThat(listener.events)
				.as("events")
				.containsExactly(
					"doFirst"
				);
		}

		@Test
		void doFirstListenerError() {
			Throwable listenerError = new IllegalStateException("expected from doFirst");

			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
			TestSignalListener<Integer> listener = new TestSignalListener<Integer>() {
				@Override
				public void doFirst() throws Throwable {
					throw listenerError;
				}
			};

			FluxTapFuseable<Integer, Void> test = new FluxTapFuseable<>(Flux.just(1), factoryOf(listener));

			assertThatCode(() -> test.subscribeOrReturn(testSubscriber))
				.doesNotThrowAnyException();

			assertThat(listener.listenerErrors)
				.as("listenerErrors")
				.containsExactly(listenerError);

			assertThat(listener.events)
				.as("events")
				.isEmpty();
		}

		//TODO test clear/size/isEmpty

		//TODO test poll

		//TODO test ASYNC fusion onNext

		//TODO test SYNC fusion onNext
	}

	@Nested
	class FluxTapConditionalTest {

		@Test
		void implementationSmokeTest() {
			Flux<Integer> normalSource = Flux.just(1).hide();
			//the TestSubscriber "requireFusion" configuration below is intentionally inverted
			//so that an exception describing the actual Subscription is thrown when calling block()
			TestSubscriber<Integer> conditionalTestSubscriber = TestSubscriber.builder().requireFusion(2).buildConditional(i -> true);
			Flux<Integer> normal = normalSource.tap(TestSignalListener::new);

			assertThat(normalSource).as("smoke test normal source").isNotInstanceOf(Fuseable.class);
			assertThat(normal).as("normal").isInstanceOf(FluxTap.class);

			assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> normal.subscribeWith(conditionalTestSubscriber).block())
				.as("TapConditionalSubscriber")
				.withMessageContaining("got reactor.core.publisher.FluxTap$TapConditionalSubscriber");
		}

		//TODO test tryOnNext
	}

	@Nested
	class FluxTapConditionalFuseableTest {

		//TODO test tryOnNext

		@Test
		void implementationSmokeTest() {
			Flux<Integer> fuseableSource = Flux.just(1);
			//the TestSubscriber "requireFusion" configuration below is intentionally inverted
			//so that an exception describing the actual Subscription is thrown when calling block()
			TestSubscriber<Integer> conditionalTestSubscriberForFuseable = TestSubscriber.builder().requireNotFuseable().buildConditional(i -> true);
			Flux<Integer> fuseable = fuseableSource.tap(TestSignalListener::new);

			assertThat(fuseableSource).as("smoke test fuseableSource").isInstanceOf(Fuseable.class);
			assertThat(fuseable).as("fuseable").isInstanceOf(FluxTapFuseable.class);

			assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> fuseable.subscribeWith(conditionalTestSubscriberForFuseable).block())
				.as("TapConditionalFuseableSubscriber")
				.withMessageContaining("got reactor.core.publisher.FluxTapFuseable$TapConditionalFuseableSubscriber");
		}
	}

	@Nested
	class MonoTapTest {

		@Test
		void subscriberImplementationsFromFluxTap() {
			Mono<Integer> normalSource = Mono.just(1).hide();

			assertThat(normalSource).as("smoke test normalSource").isNotInstanceOf(Fuseable.class);

			Mono<Integer> normal = normalSource.tap(TestSignalListener::new);

			assertThat(normal).as("normal").isInstanceOf(MonoTap.class);

			//the TestSubscriber "requireFusion" configuration below are intentionally inverted
			//so that an exception describing the actual Subscription is thrown when calling block()
			TestSubscriber<Integer> testSubscriberForNormal = TestSubscriber.builder().requireFusion(2).build();
			TestSubscriber<Integer> testSubscriberForNormalConditional = TestSubscriber.builder().requireFusion(2).buildConditional(i -> true);

			assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> normal.subscribeWith(testSubscriberForNormal).block())
				.as("TapSubscriber")
				.withMessageContaining("got reactor.core.publisher.FluxTap$TapSubscriber");

			assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> normal.subscribeWith(testSubscriberForNormalConditional).block())
				.as("TapConditionalSubscriber")
				.withMessageContaining("got reactor.core.publisher.FluxTap$TapConditionalSubscriber");
		}

		@Test
		void throwingCreateListener() {
			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
			MonoTap<Integer, Void> test = new MonoTap<>(Mono.just(1),
				new SignalListenerFactory<Integer, Void>() {
					@Override
					public Void initializePublisherState(Publisher<? extends Integer> source) {
						return null;
					}

					@Override
					public SignalListener<Integer> createListener(Publisher<? extends Integer> source,
																  ContextView listenerContext, Void publisherContext) {
						throw new IllegalStateException("expected");
					}
				});

			assertThatCode(() -> test.subscribeOrReturn(testSubscriber))
				.doesNotThrowAnyException();

			assertThat(testSubscriber.expectTerminalError())
				.as("downstream error")
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("expected");
		}

		//doFirst is invoked from each publisher's subscribeOrReturn
		@Test
		void doFirst() {
			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
			TestSignalListener<Integer> listener = new TestSignalListener<>();

			MonoTap<Integer, Void> test = new MonoTap<>(Mono.just(1), factoryOf(listener));

			assertThatCode(() -> test.subscribeOrReturn(testSubscriber))
				.doesNotThrowAnyException();

			assertThat(listener.listenerErrors).as("listenerErrors").isEmpty();
			assertThat(listener.events)
				.as("events")
				.containsExactly(
					"doFirst"
				);
		}

		@Test
		void doFirstListenerError() {
			Throwable listenerError = new IllegalStateException("expected from doFirst");

			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
			TestSignalListener<Integer> listener = new TestSignalListener<Integer>() {
				@Override
				public void doFirst() throws Throwable {
					throw listenerError;
				}
			};

			MonoTap<Integer, Void> test = new MonoTap<>(Mono.just(1), factoryOf(listener));

			assertThatCode(() -> test.subscribeOrReturn(testSubscriber))
				.doesNotThrowAnyException();

			assertThat(listener.listenerErrors)
				.as("listenerErrors")
				.containsExactly(listenerError);

			assertThat(listener.events)
				.as("events")
				.isEmpty();
		}
	}
	
	@Nested
	class MonoTapFuseableTest {

		@Test
		void subscriberImplementationsFromFluxTapFuseable() {
			Mono<Integer> fuseableSource = Mono.just(1);

			assertThat(fuseableSource).as("smoke test fuseableSource").isInstanceOf(Fuseable.class);

			Mono<Integer> fuseable = fuseableSource.tap(TestSignalListener::new);

			assertThat(fuseable).as("fuseable").isInstanceOf(MonoTapFuseable.class);

			//the TestSubscriber "requireFusion" configuration below are intentionally inverted
			//so that an exception describing the actual Subscription is thrown when calling block()
			TestSubscriber<Integer> testSubscriberForFuseable = TestSubscriber.builder().requireNotFuseable().build();
			TestSubscriber<Integer> testSubscriberForFuseableConditional = TestSubscriber.builder().requireNotFuseable().buildConditional(i -> true);

			assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> fuseable.subscribeWith(testSubscriberForFuseable).block())
				.as("TapFuseableSubscriber")
				.withMessageContaining("got reactor.core.publisher.FluxTapFuseable$TapFuseableSubscriber");

			assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> fuseable.subscribeWith(testSubscriberForFuseableConditional).block())
				.as("TapFuseableConditionalSubscriber")
				.withMessageContaining("got reactor.core.publisher.FluxTapFuseable$TapConditionalFuseableSubscriber");
		}

		@Test
		void throwingCreateListener() {
			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
			MonoTapFuseable<Integer, Void> test = new MonoTapFuseable<>(Mono.just(1),
				new SignalListenerFactory<Integer, Void>() {
					@Override
					public Void initializePublisherState(Publisher<? extends Integer> source) {
						return null;
					}

					@Override
					public SignalListener<Integer> createListener(Publisher<? extends Integer> source,
																  ContextView listenerContext, Void publisherContext) {
						throw new IllegalStateException("expected");
					}
				});

			assertThatCode(() -> test.subscribeOrReturn(testSubscriber))
				.doesNotThrowAnyException();

			assertThat(testSubscriber.expectTerminalError())
				.as("downstream error")
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("expected");
		}

		//doFirst is invoked from each publisher's subscribeOrReturn
		@Test
		void doFirst() {
			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
			TestSignalListener<Integer> listener = new TestSignalListener<>();

			MonoTapFuseable<Integer, Void> test = new MonoTapFuseable<>(Mono.just(1), factoryOf(listener));

			assertThatCode(() -> test.subscribeOrReturn(testSubscriber))
				.doesNotThrowAnyException();

			assertThat(listener.listenerErrors).as("listenerErrors").isEmpty();
			assertThat(listener.events)
				.as("events")
				.containsExactly(
					"doFirst"
				);
		}

		@Test
		void doFirstListenerError() {
			Throwable listenerError = new IllegalStateException("expected from doFirst");

			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
			TestSignalListener<Integer> listener = new TestSignalListener<Integer>() {
				@Override
				public void doFirst() throws Throwable {
					throw listenerError;
				}
			};

			MonoTapFuseable<Integer, Void> test = new MonoTapFuseable<>(Mono.just(1), factoryOf(listener));

			assertThatCode(() -> test.subscribeOrReturn(testSubscriber))
				.doesNotThrowAnyException();

			assertThat(listener.listenerErrors)
				.as("listenerErrors")
				.containsExactly(listenerError);

			assertThat(listener.events)
				.as("events")
				.isEmpty();
		}
	}

	@Nested
	class TapScannableTest {

		@Test
		void scanFluxTap() {
			Flux<Integer> source = Flux.just(1);
			FluxTap<Integer, Void> testPublisher = new FluxTap<>(source, ignoredFactory());

			Scannable test = Scannable.from(testPublisher);
			assertThat(test).isSameAs(testPublisher)
				.matches(Scannable::isScanAvailable, "isScanAvailable");

			assertThat(test.scan(Attr.PREFETCH)).as("PREFETCH").isEqualTo(-1);
			assertThat(test.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isEqualTo(RunStyle.SYNC);
			assertThat(test.scanUnsafe(Attr.PARENT)).as("PARENT").isSameAs(source);
		}

		@Test
		void scanFluxTapRestoringThreadLocals() {
			Flux<Integer> source = Flux.just(1);
			FluxTapRestoringThreadLocals<Integer, Void> testPublisher =
					new FluxTapRestoringThreadLocals<>(source, ignoredFactory());

			Scannable test = Scannable.from(testPublisher);
			assertThat(test).isSameAs(testPublisher)
			                .matches(Scannable::isScanAvailable, "isScanAvailable");

			assertThat(test.scan(Attr.PREFETCH)).as("PREFETCH").isEqualTo(-1);
			assertThat(test.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isEqualTo(RunStyle.SYNC);
			assertThat(test.scanUnsafe(Attr.PARENT)).as("PARENT").isSameAs(source);
		}

		@Test
		void scanFluxTapFuseable() {
			Flux<Integer> source = Flux.just(1);
			FluxTapFuseable<Integer, Void> testPublisher = new FluxTapFuseable<>(source, ignoredFactory());

			Scannable test = Scannable.from(testPublisher);
			assertThat(test).isSameAs(testPublisher)
				.matches(Scannable::isScanAvailable, "isScanAvailable");

			assertThat(test.scan(Attr.PREFETCH)).as("PREFETCH").isEqualTo(-1);
			assertThat(test.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isEqualTo(RunStyle.SYNC);
			assertThat(test.scanUnsafe(Attr.PARENT)).as("PARENT").isSameAs(source);
		}

		@Test
		void scanMonoListen() {
			Mono<Integer> source = Mono.just(1);
			MonoTap<Integer, Void> testPublisher = new MonoTap<>(source, ignoredFactory());

			Scannable test = Scannable.from(testPublisher);
			assertThat(test).isSameAs(testPublisher)
				.matches(Scannable::isScanAvailable, "isScanAvailable");

			assertThat(test.scan(Attr.PREFETCH)).as("PREFETCH").isEqualTo(-1);
			assertThat(test.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isEqualTo(RunStyle.SYNC);
			assertThat(test.scanUnsafe(Attr.PARENT)).as("PARENT").isSameAs(source);
		}

		@Test
		void scanMonoListenRestoringThreadLocals() {
			Mono<Integer> source = Mono.just(1);
			MonoTapRestoringThreadLocals<Integer, Void> testPublisher =
					new MonoTapRestoringThreadLocals<>(source, ignoredFactory());

			Scannable test = Scannable.from(testPublisher);
			assertThat(test).isSameAs(testPublisher)
			                .matches(Scannable::isScanAvailable, "isScanAvailable");

			assertThat(test.scan(Attr.PREFETCH)).as("PREFETCH").isEqualTo(-1);
			assertThat(test.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isEqualTo(RunStyle.SYNC);
			assertThat(test.scanUnsafe(Attr.PARENT)).as("PARENT").isSameAs(source);
		}

		@Test
		void scanMonoListenFuseable() {
			Mono<Integer> source = Mono.just(1);
			MonoTapFuseable<Integer, Void> testPublisher = new MonoTapFuseable<>(source, ignoredFactory());

			Scannable test = Scannable.from(testPublisher);
			assertThat(test).isSameAs(testPublisher)
				.matches(Scannable::isScanAvailable, "isScanAvailable");

			assertThat(test.scan(Attr.PREFETCH)).as("PREFETCH").isEqualTo(-1);
			assertThat(test.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isEqualTo(RunStyle.SYNC);
			assertThat(test.scanUnsafe(Attr.PARENT)).as("PARENT").isSameAs(source);
		}

		@Test
		void scanListenSubscriber() {
			CoreSubscriber<Integer> actual = Operators.drainSubscriber();
			Subscription subscription = Operators.emptySubscription();

			FluxTap.TapSubscriber<?> subscriber = new FluxTap.TapSubscriber<>(
				actual, new TestSignalListener<>());

			subscriber.onSubscribe(subscription);

			Scannable test = Scannable.from(subscriber);
			assertThat(test.isScanAvailable()).as("isScanAvailable").isTrue();
			assertThat(test).isSameAs(subscriber);

			assertThat(test.scanUnsafe(Attr.ACTUAL)).as("ACTUAL").isSameAs(actual);
			assertThat(test.scanUnsafe(Attr.PARENT)).as("PARENT").isSameAs(subscription);
			assertThat(test.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isSameAs(RunStyle.SYNC);

			subscriber.onComplete();
			assertThat(test.scan(Attr.TERMINATED)).as("TERMINATED").isTrue();
		}

		@Test
		void scanListenSubscriberRestoringThreadLocals() {
			CoreSubscriber<Integer> actual = Operators.drainSubscriber();
			Subscription subscription = Operators.emptySubscription();

			FluxTapRestoringThreadLocals.TapSubscriber<?> subscriber =
					new FluxTapRestoringThreadLocals.TapSubscriber<>(actual,
							new TestSignalListener<>(), Context.empty());

			subscriber.onSubscribe(subscription);

			Scannable test = Scannable.from(subscriber);
			assertThat(test.isScanAvailable()).as("isScanAvailable").isTrue();
			assertThat(test).isSameAs(subscriber);

			assertThat(test.scanUnsafe(Attr.ACTUAL)).as("ACTUAL").isSameAs(actual);
			assertThat(test.scanUnsafe(Attr.PARENT)).as("PARENT").isSameAs(subscription);
			assertThat(test.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isSameAs(RunStyle.SYNC);

			subscriber.onComplete();
			assertThat(test.scan(Attr.TERMINATED)).as("TERMINATED").isTrue();
		}

		@Test
		void scanListenConditionalSubscriber() {
			ConditionalSubscriber<? super Integer> actual = Operators.toConditionalSubscriber(Operators.drainSubscriber());
			Subscription subscription = Operators.emptySubscription();

			FluxTap.TapConditionalSubscriber<?> subscriber = new FluxTap.TapConditionalSubscriber<>(
				actual, new TestSignalListener<>());

			subscriber.onSubscribe(subscription);

			Scannable test = Scannable.from(subscriber);
			assertThat(test.isScanAvailable()).as("isScanAvailable").isTrue();
			assertThat(test).isSameAs(subscriber);

			assertThat(test.scanUnsafe(Attr.ACTUAL)).as("ACTUAL").isSameAs(actual);
			assertThat(test.scanUnsafe(Attr.PARENT)).as("PARENT").isSameAs(subscription);
			assertThat(test.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isSameAs(RunStyle.SYNC);

			subscriber.onComplete();
			assertThat(test.scan(Attr.TERMINATED)).as("TERMINATED").isTrue();
		}

		@Test
		void scanListenFuseableSubscriber() {
			CoreSubscriber<Integer> actual = Operators.drainSubscriber();
			Subscription subscription = Operators.emptySubscription();

			FluxTapFuseable.TapFuseableSubscriber<?> subscriber = new FluxTapFuseable.TapFuseableSubscriber<>(
				actual, new TestSignalListener<>());

			subscriber.onSubscribe(subscription);

			Scannable test = Scannable.from(subscriber);
			assertThat(test.isScanAvailable()).as("isScanAvailable").isTrue();
			assertThat(test).isSameAs(subscriber);

			assertThat(test.scanUnsafe(Attr.ACTUAL)).as("ACTUAL").isSameAs(actual);
			assertThat(test.scanUnsafe(Attr.PARENT)).as("PARENT").isSameAs(subscription);
			assertThat(test.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isSameAs(RunStyle.SYNC);

			subscriber.onComplete();
			assertThat(test.scan(Attr.TERMINATED)).as("TERMINATED").isTrue();
		}

		@Test
		void scanListenConditionalFuseableSubscriber() {
			ConditionalSubscriber<? super Integer> actual = Operators.toConditionalSubscriber(Operators.drainSubscriber());
			Subscription subscription = Operators.emptySubscription();

			FluxTapFuseable.TapConditionalFuseableSubscriber<?>
				subscriber = new FluxTapFuseable.TapConditionalFuseableSubscriber<>(
				actual, new TestSignalListener<>());

			subscriber.onSubscribe(subscription);

			Scannable test = Scannable.from(subscriber);
			assertThat(test.isScanAvailable()).as("isScanAvailable").isTrue();
			assertThat(test).isSameAs(subscriber);

			assertThat(test.scanUnsafe(Attr.ACTUAL)).as("ACTUAL").isSameAs(actual);
			assertThat(test.scanUnsafe(Attr.PARENT)).as("PARENT").isSameAs(subscription);
			assertThat(test.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isSameAs(RunStyle.SYNC);

			subscriber.onComplete();
			assertThat(test.scan(Attr.TERMINATED)).as("TERMINATED").isTrue();
		}
	}
}