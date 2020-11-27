/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

/**
 * @author Simon Baslé
 */
class SinksTest {

	@Test
	void oneIsSerialized() {
		assertThat(Sinks.one())
				.isInstanceOf(SinksSpecs.AbstractSerializedSink.class)
				.isExactlyInstanceOf(SinkOneSerialized.class);
	}

	@Test
	void emptyIsSerialized() {
		assertThat(Sinks.empty())
				.isInstanceOf(SinksSpecs.AbstractSerializedSink.class)
				.isExactlyInstanceOf(SinkEmptySerialized.class);
	}

	@Test
	void unsafeOneIsNotSerialized() {
		assertThat(Sinks.unsafe().one()).isNotInstanceOf(SinksSpecs.AbstractSerializedSink.class);
	}

	@Test
	void unsafeEmptyIsNotSerialized() {
		assertThat(Sinks.unsafe().empty()).isNotInstanceOf(SinksSpecs.AbstractSerializedSink.class);
	}


	@Nested
	class Multicast {

		//TODO Multicast has slightly different behavior with early onNext + onError : doesn't buffer elements for benefit of 1st subscriber
		//(this is a behavioral difference in EmitterProcessor)

		final Supplier<Sinks.Many<Integer>> supplier = () -> Sinks.many().multicast().onBackpressureBuffer(10);

		@TestFactory
		Stream<DynamicContainer> checkSemantics() {
			return Stream.of(
					expectMulticast(supplier, 10),
					expectReplay(supplier, NONE),
					dynamicContainer("buffers all before 1st subscriber, except for errors",
									 expectBufferingBeforeFirstSubscriber(supplier, ALL).getChildren().filter(dn -> !dn.getDisplayName().equals("replayAndErrorFirstSubscriber")))
			);
		}

		@Test
		void noReplayBeforeFirstSubscriberIfEarlyError() {
			Sinks.Many<Integer> sink = supplier.get();
			Flux<Integer> flux = sink.asFlux();
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.emitNext(1, FAIL_FAST);
			sink.emitNext(2, FAIL_FAST);
			sink.emitNext(3, FAIL_FAST);
			sink.emitError(new IllegalStateException("boom"), FAIL_FAST);
			flux.subscribe(first);

			first.assertNoValues()
			     .assertErrorMessage("boom");
		}
	}

	@Nested
	class MulticastReplayAll {

		final Supplier<Sinks.Many<Integer>> supplier = () -> Sinks.many().replay().all();

		@TestFactory
		Stream<DynamicContainer> checkSemantics() {
			return Stream.of(
					expectMulticast(supplier, Integer.MAX_VALUE),
					expectReplay(supplier, ALL),
					expectBufferingBeforeFirstSubscriber(supplier, ALL)
			);
		}
	}

	@Nested
	class MulticastReplayN {

		@TestFactory
		Stream<DynamicContainer> checkSemanticsSize5() {
			final int historySize = 5;
			final Supplier<Sinks.Many<Integer>> supplier = () -> Sinks.many().replay().limit(historySize);

			return Stream.of(
					expectMulticast(supplier, Integer.MAX_VALUE),
					expectReplay(supplier, historySize),
					expectBufferingBeforeFirstSubscriber(supplier, historySize)
			);
		}

		@TestFactory
		Stream<DynamicContainer> checkSemanticsSize0() {
			final int historySize = 0;
			final Supplier<Sinks.Many<Integer>> supplier = () -> Sinks.many().replay().limit(historySize);

			return Stream.of(
					expectMulticast(supplier, Integer.MAX_VALUE),
					expectReplay(supplier, historySize),
					expectBufferingBeforeFirstSubscriber(supplier, historySize)
			);
		}

		@TestFactory
		Stream<DynamicContainer> checkSemanticsSize100() {
			final int historySize = 100;
			final Supplier<Sinks.Many<Integer>> supplier = () -> Sinks.many().replay().limit(historySize);

			return Stream.of(
					expectMulticast(supplier, Integer.MAX_VALUE),
					expectReplay(supplier, historySize),
					expectBufferingBeforeFirstSubscriber(supplier, historySize)
			);
		}
	}

	@Nested
	class MulticastReplayDuration {

		final Duration            duration = Duration.ofMillis(1000);
		final int 				  event = 12;
		Sinks.Many<Integer>       replaySink;
		Flux<Integer>             flux;

		@BeforeEach
		void setup() {
			replaySink = Sinks.many().replay().limit(duration);
			flux = replaySink.asFlux();
		}

		// fixes: https://github.com/reactor/reactor-core/issues/2513
		@Test
		void lateSubscriberReceivesEventInRetentionTime() {
			replaySink.emitNext(event, FAIL_FAST);

			StepVerifier.withVirtualTime(() -> flux, 0L)
					.thenAwait(duration.minusMillis(1))
					.thenRequest(1)
					.expectNext(event)
					.thenCancel()
					.verify(Duration.ofMillis(20));
		}

		// fixes: https://github.com/reactor/reactor-core/issues/2513
		@Test
		void lateSubscriberDoesntReceiveEventOutsideRetentionTime() {
			replaySink.emitNext(event, FAIL_FAST);

			StepVerifier.withVirtualTime(() -> flux, 0L)
					.thenAwait(duration.plusMillis(1))
					.thenRequest(1)
					.expectNoEvent(Duration.ofMillis(20))
					.thenCancel()
					.verify(Duration.ofMillis(20));
		}
	}

	@Nested
	class MulticastDirectBestEffort {

		final Supplier<Sinks.Many<Integer>> supplier = () -> Sinks.many().multicast().directBestEffort();

		@TestFactory
		Stream<DynamicContainer> checkSemantics() {
			return Stream.of(
					expectMulticast(supplier, 0, true),
					expectReplay(supplier, NONE)
			);
		}
	}

	@Nested
	class MulticastDirectAllOrNothing {

		final Supplier<Sinks.Many<Integer>> supplier = () -> Sinks.many().multicast().directAllOrNothing();

		@TestFactory
		Stream<DynamicContainer> checkSemantics() {
			return Stream.of(
					expectMulticast(supplier, 0),
					expectReplay(supplier, NONE)
			);
		}
	}

	@Nested
	class Unicast {

		final Supplier<Sinks.Many<Integer>> supplier = () -> Sinks.many().unicast().onBackpressureBuffer();

		@TestFactory
		Stream<DynamicContainer> checkSemantics() {
			return Stream.of(
					expectUnicast(supplier),
					expectBufferingBeforeFirstSubscriber(supplier, ALL)
			);
		}
	}

	@Nested
	class SingleOrEmpty {

		Sinks.One<Integer> singleOrEmpty;
		Mono<Integer>      mono;

		@BeforeEach
		void createSingleOrEmpty() {
			singleOrEmpty = Sinks.one();
			mono = singleOrEmpty.asMono();
		}

		//TODO racing dual completions ?

		@Test
		void singleOrEmptyIsCompletableOnlyOnce_emptyVsValued() {
			StepVerifier.create(mono)
						.then(() -> {
							singleOrEmpty.emitValue(null, FAIL_FAST);
							singleOrEmpty.emitValue(-1, FAIL_FAST);
						})
						.expectComplete()
						.verifyThenAssertThat()
						.hasDropped(-1);
		}

		@Test
		void singleOrEmptyIsCompletableOnlyOnce_emptyVsError() {
			StepVerifier.create(mono)
						.then(() -> {
							singleOrEmpty.emitEmpty(FAIL_FAST);
							singleOrEmpty.emitError(new IllegalStateException("boom"), FAIL_FAST);
						})
						.expectComplete()
						.verifyThenAssertThat()
						.hasDroppedErrorWithMessage("boom");
		}

		@Test
		void singleOrEmptyIsCompletableOnlyOnce_valuedVsEmpty() {
			StepVerifier.create(mono)
						.then(() -> {
							singleOrEmpty.emitValue(1, FAIL_FAST);
							singleOrEmpty.emitValue(null, FAIL_FAST);
						})
						.expectNext(1)
						.expectComplete()
						.verifyThenAssertThat()
						.hasNotDroppedElements();
		}

		@Test
		void singleOrEmptyIsCompletableOnlyOnce_valuedVsError() {
			StepVerifier.create(mono)
						.then(() -> {
							singleOrEmpty.emitValue(1, FAIL_FAST);
							singleOrEmpty.emitError(new IllegalStateException("boom"), FAIL_FAST);
						})
						.expectNext(1)
						.expectComplete()
						.verifyThenAssertThat()
						.hasDroppedErrorWithMessage("boom");
		}

		@Test
		void singleOrEmptyIsCompletableOnlyOnce_errorVsValued() {
			StepVerifier.create(mono)
						.then(() -> {
							singleOrEmpty.emitError(new IllegalStateException("boom"), FAIL_FAST);
							singleOrEmpty.emitValue(-1, FAIL_FAST);
						})
						.expectErrorMessage("boom")
						.verifyThenAssertThat()
						.hasDropped(-1);
		}

		@Test
		void singleOrEmptyIsCompletableOnlyOnce_errorVsEmpty() {
			StepVerifier.create(mono)
						.then(() -> {
							singleOrEmpty.emitError(new IllegalStateException("boom"), FAIL_FAST);
							singleOrEmpty.emitEmpty(FAIL_FAST);
						})
						.expectErrorMessage("boom")
						.verifyThenAssertThat()
						.hasNotDroppedElements()
						.hasNotDroppedErrors();
		}

		@Test
		void canBeValuedEarly() {
			singleOrEmpty.emitValue(1, FAIL_FAST);

			StepVerifier.create(mono)
						.expectNext(1)
						.verifyComplete();
		}

		@Test
		void canBeCompletedEarly() {
			singleOrEmpty.emitEmpty(FAIL_FAST);

			StepVerifier.create(mono)
						.verifyComplete();
		}

		@Test
		void canBeErroredEarly() {
			singleOrEmpty.emitError(new IllegalStateException("boom"), FAIL_FAST);

			StepVerifier.create(mono)
						.verifyErrorMessage("boom");
		}

		@Test
		void canBeValuedLate() {
			StepVerifier.create(mono)
						.expectSubscription()
						.expectNoEvent(Duration.ofMillis(100))
						.then(() -> singleOrEmpty.emitValue(1, FAIL_FAST))
						.expectNext(1)
						.verifyComplete();
		}

		@Test
		void canBeCompletedLate() {
			StepVerifier.create(mono)
						.expectSubscription()
						.expectNoEvent(Duration.ofMillis(100))
						.then(() -> singleOrEmpty.emitEmpty(FAIL_FAST))
						.verifyComplete();
		}

		@Test
		void canBeErroredLate() {
			StepVerifier.create(mono)
						.expectSubscription()
						.expectNoEvent(Duration.ofMillis(100))
						.then(() -> singleOrEmpty.emitError(new IllegalStateException("boom"), FAIL_FAST))
						.verifyErrorMessage("boom");
		}

		@Test
		void replaysValuedCompletionToLateSubscribersWithBackpressure() {
			singleOrEmpty.emitValue(1, FAIL_FAST);
			mono.subscribe(); //first subscriber

			StepVerifier.create(mono, StepVerifierOptions.create()
														 .scenarioName("second subscriber, no backpressure"))
						.expectNext(1)
						.verifyComplete();

			StepVerifier.create(mono, StepVerifierOptions.create()
														 .scenarioName("third subscriber, backpressure")
														 .initialRequest(0))
						.expectSubscription()
						.expectNoEvent(Duration.ofMillis(100))
						.thenRequest(1)
						.expectNext(1)
						.verifyComplete();
		}

		@Test
		void replaysEmptyCompletionToLateSubscribersEvenWithoutRequest() {
			singleOrEmpty.emitEmpty(FAIL_FAST);
			mono.subscribe(); //first subscriber

			StepVerifier.create(mono, StepVerifierOptions.create()
														 .scenarioName("second subscriber, no backpressure"))
						.verifyComplete();

			StepVerifier.create(mono, StepVerifierOptions.create()
														 .scenarioName("third subscriber, 0 request")
														 .initialRequest(0))
						.expectSubscription()
						//notice no expectNoEvent / request here
						.verifyComplete();
		}

		@Test
		void replaysErrorCompletionToLateSubscribers() {
			singleOrEmpty.emitError(new IllegalStateException("boom"), FAIL_FAST);
			mono.subscribe(); //first subscriber

			StepVerifier.create(mono, StepVerifierOptions.create()
														 .scenarioName("second subscriber, no backpressure"))
						.verifyErrorMessage("boom");

			StepVerifier.create(mono, StepVerifierOptions.create()
														 .scenarioName("third subscriber, 0 request")
														 .initialRequest(0))
						.expectSubscription()
						//notice no expectNoEvent / request here
						.verifyErrorMessage("boom");
		}
	}

	private static final int NONE = 0;
	private static final int ALL = Integer.MAX_VALUE;

	DynamicContainer expectMulticast(Supplier<Sinks.Many<Integer>> sinkSupplier, int acceptableDifference) {
		return expectMulticast(sinkSupplier, acceptableDifference, false);
	}

	DynamicContainer expectMulticast(Supplier<Sinks.Many<Integer>> sinkSupplier, int acceptableDifference, boolean dropForSlow) {
		return dynamicContainer("multicast", Stream.of(

				dynamicTest("fluxViewReturnsSameInstance", () -> {
					Sinks.Many<Integer> sink = sinkSupplier.get();
					Flux<Integer> flux = sink.asFlux();

					assertThat(flux).isSameAs(sink.asFlux());
				}),

				dynamicTest("acceptsMoreThanOneSubscriber", () -> {
					Flux<Integer> flux = sinkSupplier.get()
													 .asFlux();
					assertThatCode(() -> {
						flux.subscribe();
						flux.subscribe();
					}).doesNotThrowAnyException();
				}),

				dynamicTest("honorsMultipleSubscribersBackpressure", () -> {
					Sinks.Many<Integer> sink = sinkSupplier.get();
					Flux<Integer> flux = sink.asFlux();
					ExecutorService es = Executors.newFixedThreadPool(2);

					try {
						CountDownLatch requestLatch = new CountDownLatch(2);
						final Future<?> f1 = es.submit(() -> {
							AssertSubscriber<Integer> test1 = AssertSubscriber.create(2);
							flux.subscribe(test1);
							test1.assertNoValues();
							requestLatch.countDown();

							int expectedCount = (dropForSlow || acceptableDifference > 0) ? 2 : 1;

							if (expectedCount == 2) {
								test1.awaitAndAssertNextValues(1, 2);
							}
							else {
								test1.awaitAndAssertNextValues(1);
							}
							try {
								Awaitility.await()
										  .atMost(2, TimeUnit.SECONDS)
										  .with()
										  .pollDelay(1, TimeUnit.SECONDS)
										  .untilAsserted(() -> test1.assertValueCount(expectedCount));
							}
							finally {
								test1.cancel();
							}
						});
						final Future<?> f2 = es.submit(() -> {
							AssertSubscriber<Integer> test2 = AssertSubscriber.create(1);
							flux.subscribe(test2);
							requestLatch.countDown();

							test2.awaitAndAssertNextValues(1);
							try {
								Awaitility.await()
										  .atMost(2, TimeUnit.SECONDS)
										  .with()
										  .pollDelay(1, TimeUnit.SECONDS)
										  .untilAsserted(() -> test2.assertValueCount(1));
							}
							finally {
								test2.cancel();
							}
						});

						requestLatch.await(1, TimeUnit.SECONDS);
						Sinks.EmitResult expectedFor2 = (!dropForSlow && acceptableDifference < 1) ?
								Sinks.EmitResult.FAIL_OVERFLOW  :
								Sinks.EmitResult.OK;
						Sinks.EmitResult expectedFor3 = acceptableDifference < 2 ?
								Sinks.EmitResult.FAIL_OVERFLOW  :
								Sinks.EmitResult.OK;
						Sinks.EmitResult expectedFor4 = acceptableDifference < 3 ?
								Sinks.EmitResult.FAIL_OVERFLOW  :
								Sinks.EmitResult.OK;

						assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(Sinks.EmitResult.OK);
						assertThat(sink.tryEmitNext(2)).as("tryEmitNext(2)").isEqualTo(expectedFor2);
						assertThat(sink.tryEmitNext(3)).as("tryEmitNext(3)").isEqualTo(expectedFor3);
						assertThat(sink.tryEmitNext(4)).as("tryEmitNext(4)").isEqualTo(expectedFor4);
						assertThat(sink.tryEmitComplete()).as("tryEmitComplete()").isEqualTo(Sinks.EmitResult.OK);

						f1.get();
						f2.get();
					}
					finally {
						es.shutdownNow();
					}
				})
		));
	}

	DynamicContainer expectUnicast(Supplier<Sinks.Many<Integer>> sinkSupplier) {
		return dynamicContainer("unicast", Stream.of(

				dynamicTest("fluxViewReturnsSameInstance", () -> {
					Sinks.Many<Integer> sink = sinkSupplier.get();
					Flux<Integer> flux = sink.asFlux();

					assertThat(flux).isSameAs(sink.asFlux());
				}),

				dynamicTest("acceptsOnlyOneSubscriber", () -> {
					Sinks.Many<Integer> sink = sinkSupplier.get();
					Flux<Integer> flux = sink.asFlux();
					sink.emitComplete(FAIL_FAST);

					assertThatCode(flux::subscribe).doesNotThrowAnyException();
					StepVerifier.create(flux)
								.verifyErrorSatisfies(e -> assertThat(e).hasMessageEndingWith("allows only a single Subscriber"));
				}),

				dynamicTest("honorsSubscriberBackpressure", () -> {
					Sinks.Many<Integer> sink = sinkSupplier.get();
					Flux<Integer> flux = sink.asFlux();
					ExecutorService es = Executors.newFixedThreadPool(2);

					try {
						CountDownLatch requestLatch = new CountDownLatch(1);
						final Future<?> future = es.submit(() -> {
							AssertSubscriber<Integer> test = AssertSubscriber.create(2);
							flux.subscribe(test);
							test.assertNoValues();
							requestLatch.countDown();

							test.awaitAndAssertNextValues(1, 2);
							try {
								Awaitility.await()
										  .atMost(2, TimeUnit.SECONDS)
										  .with()
										  .pollDelay(1, TimeUnit.SECONDS)
										  .untilAsserted(() -> test.assertValueCount(2));
							}
							finally {
								test.cancel();
							}
						});

						requestLatch.await(1, TimeUnit.SECONDS);
						sink.emitNext(1, FAIL_FAST);
						sink.emitNext(2, FAIL_FAST);
						sink.emitNext(3, FAIL_FAST);
						sink.emitNext(4, FAIL_FAST);
						sink.emitComplete(FAIL_FAST);

						future.get();
					}
					finally {
						es.shutdownNow();
					}
				})));
	}

	DynamicContainer expectReplay(Supplier<Sinks.Many<Integer>> sinkSupplier, int expectedReplay) {
		if (expectedReplay == NONE) {
			DynamicTest doesNotReplayToLateSubscribers = dynamicTest("doesNotReplayToLateSubscribers", () -> {
				Sinks.Many<Integer> sink = sinkSupplier.get();
				Flux<Integer> flux = sink.asFlux();
				AssertSubscriber<Integer> s1 = AssertSubscriber.create();
				AssertSubscriber<Integer> s2 = AssertSubscriber.create();

				flux.subscribe(s1);
				sink.emitNext(1, FAIL_FAST);
				sink.emitNext(2, FAIL_FAST);
				sink.emitNext(3, FAIL_FAST);
				s1.assertValues(1, 2, 3);

				flux.subscribe(s2);
				s2.assertNoValues()
				  .assertNotComplete();

				sink.emitComplete(FAIL_FAST);
				s1.assertValueCount(3)
				  .assertComplete();
				s2.assertNoValues()
				  .assertComplete();
			});

			DynamicTest immediatelyCompleteLateSubscriber = dynamicTest("immediatelyCompleteLateSubscriber", () -> {
				Sinks.Many<Integer> sink = sinkSupplier.get();
				Flux<Integer> flux = sink.asFlux();

				flux.subscribe(); //first subscriber
				AssertSubscriber<Integer> late = AssertSubscriber.create();

				sink.emitNext(1, FAIL_FAST);
				sink.emitComplete(FAIL_FAST);
				flux.subscribe(late);

				late.assertNoValues()
					.assertComplete();
			});

			DynamicTest immediatelyErrorLateSubscriber = dynamicTest("immediatelyErrorLateSubscriber", () -> {
				Sinks.Many<Integer> sink = sinkSupplier.get();
				Flux<Integer> flux = sink.asFlux();

				flux.onErrorReturn(-1)
					.subscribe(); //first subscriber, ignore errors
				AssertSubscriber<Integer> late = AssertSubscriber.create();

				sink.emitNext(1, FAIL_FAST);
				sink.emitError(new IllegalStateException("boom"), FAIL_FAST);
				flux.subscribe(late);

				late.assertNoValues()
					.assertErrorMessage("boom");
			});

			return dynamicContainer("no replay", Stream.of(
					doesNotReplayToLateSubscribers,
					immediatelyCompleteLateSubscriber,
					immediatelyErrorLateSubscriber
			));
		}
		else if (expectedReplay == ALL) {
			return dynamicContainer("replays all", Stream.of(dynamicTest("doesReplayAllToLateSubscribers", () -> {
																 Sinks.Many<Integer>
																		 sink = sinkSupplier.get();
																 Flux<Integer> flux = sink.asFlux();
																 AssertSubscriber<Integer> s1 = AssertSubscriber.create();
																 AssertSubscriber<Integer> s2 = AssertSubscriber.create();

																 flux.subscribe(s1);
																 sink.emitNext(1, FAIL_FAST);
																 sink.emitNext(2, FAIL_FAST);
																 sink.emitNext(3, FAIL_FAST);
																 s1.assertValues(1, 2, 3);

																 flux.subscribe(s2);
																 s2.assertValues(1, 2, 3)
																   .assertNotComplete();

																 sink.emitComplete(FAIL_FAST);
																 s1.assertValueCount(3)
																   .assertComplete();
																 s2.assertValues(1, 2, 3)
																   .assertComplete();
															 }),

															 dynamicTest("immediatelyCompleteLateSubscriber", () -> {
																 Sinks.Many<Integer>
																		 sink = sinkSupplier.get();
																 Flux<Integer> flux = sink.asFlux();
															 }),

															 dynamicTest("immediatelyErrorLateSubscriber", () -> {
																 Sinks.Many<Integer>
																		 sink = sinkSupplier.get();
																 Flux<Integer> flux = sink.asFlux();
															 })));
		}
		else {
			return dynamicContainer("replays " + expectedReplay, Stream.of(dynamicTest("doesReplay" + expectedReplay + "ToLateSubscribers", () -> {
																			   Sinks.Many<Integer>
																					   sink = sinkSupplier.get();
																			   Flux<Integer> flux = sink.asFlux();
																			   AssertSubscriber<Integer> s1 = AssertSubscriber.create();
																			   AssertSubscriber<Integer> s2 = AssertSubscriber.create();

																			   flux.subscribe(s1);
																			   List<Integer> expected = new ArrayList<>();
																			   for (int i = 0; i < expectedReplay + 10; i++) {
																				   sink.emitNext(i, FAIL_FAST);
																				   if (i >= 10)
																					   expected.add(i);
																			   }
																			   s1.assertValueCount(expectedReplay + 10);

																			   flux.subscribe(s2);
																			   s2.assertValueSequence(expected)
																				 .assertNotComplete();

																			   sink.emitComplete(FAIL_FAST);
																			   s1.assertValueCount(expectedReplay + 10)
																				 .assertComplete();
																			   s2.assertValueSequence(expected)
																				 .assertComplete();
																		   }),

																		   dynamicTest("replay" + expectedReplay + "AndCompleteLateSubscriber", () -> {
																			   Sinks.Many<Integer>
																					   sink = sinkSupplier.get();
																			   Flux<Integer> flux = sink.asFlux();
																			   flux.subscribe(); //first
																			   AssertSubscriber<Integer> late = AssertSubscriber.create();

																			   List<Integer> expected = new ArrayList<>();
																			   for (int i = 0; i < expectedReplay + 10; i++) {
																				   sink.emitNext(i, FAIL_FAST);
																				   if (i >= 10)
																					   expected.add(i);
																			   }
																			   sink.emitComplete(FAIL_FAST);
																			   flux.subscribe(late);

																			   late.assertValueSequence(expected)
																				   .assertComplete();
																		   }),

																		   dynamicTest("replay" + expectedReplay + "AndErrorLateSubscriber", () -> {
																			   Sinks.Many<Integer>
																					   sink = sinkSupplier.get();
																			   Flux<Integer> flux = sink.asFlux();
																			   flux.onErrorReturn(-1)
																				   .subscribe(); //first subscriber, ignore errors
																			   AssertSubscriber<Integer> late = AssertSubscriber.create();

																			   List<Integer> expected = new ArrayList<>();
																			   for (int i = 0; i < expectedReplay + 10; i++) {
																				   sink.emitNext(i, FAIL_FAST);
																				   if (i >= 10)
																					   expected.add(i);
																			   }
																			   sink.emitError(new IllegalStateException("boom"), FAIL_FAST);
																			   flux.subscribe(late);

																			   late.assertValueSequence(expected)
																				   .assertErrorMessage("boom");
																		   })));
		}
	}

	DynamicContainer expectBufferingBeforeFirstSubscriber(Supplier<Sinks.Many<Integer>> sinkSupplier, int expectedBuffering) {
		if (expectedBuffering == NONE) {
			return dynamicContainer("no buffering before 1st subscriber", Stream.of(dynamicTest("doesNotBufferBeforeFirstSubscriber", () -> {
				Sinks.Many<Integer> sink = sinkSupplier.get();
				Flux<Integer> flux = sink.asFlux();
				AssertSubscriber<Integer> first = AssertSubscriber.create();

				sink.emitNext(1, FAIL_FAST);
				sink.emitNext(2, FAIL_FAST);
				sink.emitNext(3, FAIL_FAST);
				flux.subscribe(first);

				first.assertNoValues()
					 .assertNotComplete();

				sink.emitComplete(FAIL_FAST);
				first.assertNoValues()
					 .assertComplete();
			}), dynamicTest("immediatelyCompleteFirstSubscriber", () -> {
				Sinks.Many<Integer> sink = sinkSupplier.get();
				Flux<Integer> flux = sink.asFlux();
				AssertSubscriber<Integer> first = AssertSubscriber.create();

				sink.emitNext(1, FAIL_FAST);
				sink.emitComplete(FAIL_FAST);
				flux.subscribe(first);

				first.assertNoValues()
					 .assertComplete();
			}), dynamicTest("immediatelyErrorFirstSubscriber", () -> {
				Sinks.Many<Integer> sink = sinkSupplier.get();
				Flux<Integer> flux = sink.asFlux();
				AssertSubscriber<Integer> first = AssertSubscriber.create();

				sink.emitNext(1, FAIL_FAST);
				sink.emitError(new IllegalStateException("boom"), FAIL_FAST);
				flux.subscribe(first);

				first.assertNoValues()
					 .assertErrorMessage("boom");
			})));
		}
		else if (expectedBuffering == ALL) {
			return dynamicContainer("buffers all before 1st subscriber", Stream.of(dynamicTest("doesBufferBeforeFirstSubscriber", () -> {
																					   Sinks.Many<Integer>
																							   sink = sinkSupplier.get();
																					   Flux<Integer> flux = sink.asFlux();
																					   AssertSubscriber<Integer> first = AssertSubscriber.create();

																					   sink.emitNext(1, FAIL_FAST);
																					   sink.emitNext(2, FAIL_FAST);
																					   sink.emitNext(3, FAIL_FAST);
																					   flux.subscribe(first);

																					   first.assertValues(1, 2, 3)
																							.assertNotComplete();

																					   sink.emitComplete(FAIL_FAST);
																					   first.assertComplete();
																				   }),

																				   dynamicTest("replayAndCompleteFirstSubscriber", () -> {
																					   Sinks.Many<Integer>
																							   sink = sinkSupplier.get();
																					   Flux<Integer> flux = sink.asFlux();
																					   AssertSubscriber<Integer> first = AssertSubscriber.create();

																					   sink.emitNext(1, FAIL_FAST);
																					   sink.emitComplete(FAIL_FAST);
																					   flux.subscribe(first);

																					   first.assertValues(1)
																							.assertComplete();
																				   }),

																				   dynamicTest("replayAndErrorFirstSubscriber", () -> {
																					   Sinks.Many<Integer>
																							   sink = sinkSupplier.get();
																					   Flux<Integer> flux = sink.asFlux();
																					   AssertSubscriber<Integer> first = AssertSubscriber.create();

																					   sink.emitNext(1, FAIL_FAST);
																					   sink.emitNext(2, FAIL_FAST);
																					   sink.emitNext(3, FAIL_FAST);
																					   sink.emitError(new IllegalStateException("boom"), FAIL_FAST);
																					   flux.subscribe(first);

																					   first.assertValues(1, 2, 3)
																							.assertErrorMessage("boom");
																				   })));
		}
		else {
			return dynamicContainer("buffers " + expectedBuffering + " before 1st subscriber", Stream.of(dynamicTest("doesBuffer" + expectedBuffering + "BeforeFirstSubscriber", () -> {
				Sinks.Many<Integer> sink = sinkSupplier.get();
				Flux<Integer> flux = sink.asFlux();
				AssertSubscriber<Integer> first = AssertSubscriber.create();

				List<Integer> expected = new ArrayList<>();
				for (int i = 0; i < 10 + expectedBuffering; i++) {
					sink.emitNext(i, FAIL_FAST);
					if (i >= 10) {
						expected.add(i);
					}
				}
				flux.subscribe(first);

				first.assertValueSequence(expected)
					 .assertNotComplete();

				sink.emitComplete(FAIL_FAST);
				first.assertValueSequence(expected)
					 .assertComplete();
			}), dynamicTest("replayLimitedHistoryAndCompleteFirstSubscriber", () -> {
				Sinks.Many<Integer> sink = sinkSupplier.get();
				Flux<Integer> flux = sink.asFlux();
				AssertSubscriber<Integer> first = AssertSubscriber.create();

				List<Integer> expected = new ArrayList<>();
				for (int i = 0; i < 10 + expectedBuffering; i++) {
					sink.emitNext(i, FAIL_FAST);
					if (i >= 10) {
						expected.add(i);
					}
				}
				sink.emitComplete(FAIL_FAST);
				flux.subscribe(first);

				first.assertValueSequence(expected)
					 .assertComplete();
			}), dynamicTest("replayLimitedHistoryAndErrorFirstSubscriber", () -> {
				Sinks.Many<Integer> sink = sinkSupplier.get();
				Flux<Integer> flux = sink.asFlux();
				AssertSubscriber<Integer> first = AssertSubscriber.create();

				List<Integer> expected = new ArrayList<>();
				for (int i = 0; i < 10 + expectedBuffering; i++) {
					sink.emitNext(i, FAIL_FAST);
					if (i >= 10) {
						expected.add(i);
					}
				}
				sink.emitError(new IllegalStateException("boom"), FAIL_FAST);
				flux.subscribe(first);

				first.assertValueSequence(expected)
					 .assertErrorMessage("boom");
			})));
		}
	}

}