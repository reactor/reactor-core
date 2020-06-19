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
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * @author Simon Baslé
 */
class SinksTest {

	@Nested
	class MulticastNoWarmup {

		final Supplier<Sinks.StandaloneFluxSink<Integer>> supplier = Sinks::multicastNoWarmup;

		@TestFactory
		Stream<DynamicContainer> checkSemantics() {
			return Stream.of(
					expectMulticast(supplier),
					expectReplay(supplier, NONE),
					expectBufferingBeforeFirstSubscriber(supplier, NONE)
			);
		}
	}

	@Nested
	class Multicast {

		//TODO Multicast has slightly different behavior with early onNext + onError : doesn't buffer elements for benefit of 1st subscriber
		//(this is a behavioral difference in EmitterProcessor)

		final Supplier<Sinks.StandaloneFluxSink<Integer>> supplier = Sinks::multicast;

		@TestFactory
		Stream<DynamicContainer> checkSemantics() {
			return Stream.of(
					expectMulticast(supplier),
					expectReplay(supplier, NONE),
					dynamicContainer("buffers all before 1st subscriber, except for errors",
							expectBufferingBeforeFirstSubscriber(supplier, ALL)
									.getChildren().filter(dn -> !dn.getDisplayName().equals("replayAndErrorFirstSubscriber")))
			);
		}

		@Test
		void noReplayBeforeFirstSubscriberIfEarlyError() {
			Sinks.StandaloneFluxSink<Integer> sink = supplier.get();
			Flux<Integer> flux = sink.asFlux();
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3).error(new IllegalStateException("boom"));
			flux.subscribe(first);

			first.assertNoValues().assertErrorMessage("boom");
		}
	}

	@Nested
	class MulticastReplayAll {

		final Supplier<Sinks.StandaloneFluxSink<Integer>> supplier = Sinks::replayAll;

		@TestFactory
		Stream<DynamicContainer> checkSemantics() {
			return Stream.of(
					expectMulticast(supplier),
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
			final Supplier<Sinks.StandaloneFluxSink<Integer>> supplier = () -> Sinks.replay(historySize);

			return Stream.of(
					expectMulticast(supplier),
					expectReplay(supplier, historySize),
					expectBufferingBeforeFirstSubscriber(supplier, historySize)
			);
		}

		@TestFactory
		Stream<DynamicContainer> checkSemanticsSize0() {
			final int historySize = 0;
			final Supplier<Sinks.StandaloneFluxSink<Integer>> supplier = () -> Sinks.replay(historySize);

			return Stream.of(
					expectMulticast(supplier),
					expectReplay(supplier, historySize),
					expectBufferingBeforeFirstSubscriber(supplier, historySize)
			);
		}

		@TestFactory
		Stream<DynamicContainer> checkSemanticsSize100() {
			final int historySize = 100;
			final Supplier<Sinks.StandaloneFluxSink<Integer>> supplier = () -> Sinks.replay(historySize);

			return Stream.of(
					expectMulticast(supplier),
					expectReplay(supplier, historySize),
					expectBufferingBeforeFirstSubscriber(supplier, historySize)
			);
		}
	}

	@Nested
	class Unicast {

		final Supplier<Sinks.StandaloneFluxSink<Integer>> supplier = Sinks::unicast;

		@TestFactory
		Stream<DynamicContainer> checkSemantics() {
			return Stream.of(
					expectUnicast(supplier),
					expectBufferingBeforeFirstSubscriber(supplier, ALL)
			);
		}
	}

	//FIXME trigger mono sink tests

	private static final int NONE = 0;
	private static final int ALL = Integer.MAX_VALUE;

	DynamicContainer expectMulticast(Supplier<Sinks.StandaloneFluxSink<Integer>> sinkSupplier) {
		return dynamicContainer("multicast", Stream.of(

				dynamicTest("fluxViewReturnsSameInstance", () -> {
					Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
					Flux<Integer> flux = sink.asFlux();

					assertThat(flux).isSameAs(sink.asFlux());
				}),

				dynamicTest("acceptsMoreThanOneSubscriber", () -> {
					Flux<Integer> flux = sinkSupplier.get().asFlux();
					assertThatCode(() -> {
						flux.subscribe();
						flux.subscribe();
					}).doesNotThrowAnyException();
				}),

				dynamicTest("honorsMultipleSubscribersBackpressure", () -> {
					Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
					Flux<Integer> flux = sink.asFlux();
					ExecutorService es = Executors.newFixedThreadPool(2);

					try {
						CountDownLatch requestLatch = new CountDownLatch(2);
						final Future<?> f1 = es.submit(() -> {
							AssertSubscriber<Integer> test1 = AssertSubscriber.create(2);
							flux.subscribe(test1);
							test1.assertNoValues();
							requestLatch.countDown();

							test1.awaitAndAssertNextValues(1, 2);
							try {
								Awaitility.await().atMost(2, TimeUnit.SECONDS)
								          .with().pollDelay(1, TimeUnit.SECONDS)
								          .untilAsserted(() -> test1.assertValueCount(2));
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
								Awaitility.await().atMost(2, TimeUnit.SECONDS)
								          .with().pollDelay(1, TimeUnit.SECONDS)
								          .untilAsserted(() -> test2.assertValueCount(1));
							}
							finally {
								test2.cancel();
							}
						});

						requestLatch.await(1, TimeUnit.SECONDS);
						sink.next(1)
						    .next(2)
						    .next(3)
						    .next(4)
						    .complete();

						f1.get();
						f2.get();
					}
					finally {
						es.shutdownNow();
					}
				})
		));
	}

	DynamicContainer expectUnicast(Supplier<Sinks.StandaloneFluxSink<Integer>> sinkSupplier) {
		return dynamicContainer("unicast", Stream.of(

				dynamicTest("fluxViewReturnsSameInstance", () -> {
					Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
					Flux<Integer> flux = sink.asFlux();

					assertThat(flux).isSameAs(sink.asFlux());
				}),

				dynamicTest("acceptsOnlyOneSubscriber", () -> {
					Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
					Flux<Integer> flux = sink.asFlux();
					sink.complete();

					assertThatCode(flux::subscribe).doesNotThrowAnyException();
					StepVerifier.create(flux)
					            .verifyErrorSatisfies(e -> assertThat(e).hasMessageEndingWith("allows only a single Subscriber"));
				}),

				dynamicTest("honorsSubscriberBackpressure", () -> {
					Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
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
								Awaitility.await().atMost(2, TimeUnit.SECONDS)
								          .with().pollDelay(1, TimeUnit.SECONDS)
								          .untilAsserted(() -> test.assertValueCount(2));
							}
							finally {
								test.cancel();
							}
						});

						requestLatch.await(1, TimeUnit.SECONDS);
						sink.next(1)
						    .next(2)
						    .next(3)
						    .next(4)
						    .complete();

						future.get();
					}
					finally {
						es.shutdownNow();
					}
				})
		));
	}

	DynamicContainer expectReplay(Supplier<Sinks.StandaloneFluxSink<Integer>> sinkSupplier, int expectedReplay) {
		if (expectedReplay == NONE) {
			return dynamicContainer("no replay", Stream.of(
					dynamicTest("doesNotReplayToLateSubscribers", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> s1 = AssertSubscriber.create();
						AssertSubscriber<Integer> s2 = AssertSubscriber.create();

						flux.subscribe(s1);
						sink.next(1).next(2).next(3);
						s1.assertValues(1, 2, 3);

						flux.subscribe(s2);
						s2.assertNoValues().assertNotComplete();

						sink.complete();
						s1.assertValueCount(3).assertComplete();
						s2.assertNoValues().assertComplete();
					}),

					dynamicTest("immediatelyCompleteLateSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();

						flux.subscribe(); //first subscriber
						AssertSubscriber<Integer> late = AssertSubscriber.create();

						sink.next(1).complete();
						flux.subscribe(late);

						late.assertNoValues().assertComplete();
					}),

					dynamicTest("immediatelyErrorLateSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();

						flux.onErrorReturn(-1).subscribe(); //first subscriber, ignore errors
						AssertSubscriber<Integer> late = AssertSubscriber.create();

						sink.next(1).error(new IllegalStateException("boom"));
						flux.subscribe(late);

						late.assertNoValues().assertErrorMessage("boom");
					})
			));
		}
		else if (expectedReplay == ALL) {
			return dynamicContainer("replays all", Stream.of(
					dynamicTest("doesReplayAllToLateSubscribers", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> s1 = AssertSubscriber.create();
						AssertSubscriber<Integer> s2 = AssertSubscriber.create();

						flux.subscribe(s1);
						sink.next(1).next(2).next(3);
						s1.assertValues(1, 2, 3);

						flux.subscribe(s2);
						s2.assertValues(1, 2, 3).assertNotComplete();

						sink.complete();
						s1.assertValueCount(3).assertComplete();
						s2.assertValues(1, 2, 3).assertComplete();
					}),

					dynamicTest("immediatelyCompleteLateSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
					}),

					dynamicTest("immediatelyErrorLateSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
					})
			));
		}
		else {
			return dynamicContainer("replays " + expectedReplay, Stream.of(
					dynamicTest("doesReplay" + expectedReplay + "ToLateSubscribers", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> s1 = AssertSubscriber.create();
						AssertSubscriber<Integer> s2 = AssertSubscriber.create();

						flux.subscribe(s1);
						List<Integer> expected = new ArrayList<>();
						for (int i = 0; i < expectedReplay + 10; i++) {
							sink.next(i);
							if (i >= 10) expected.add(i);
						}
						s1.assertValueCount(expectedReplay + 10);

						flux.subscribe(s2);
						s2.assertValueSequence(expected).assertNotComplete();

						sink.complete();
						s1.assertValueCount(expectedReplay + 10).assertComplete();
						s2.assertValueSequence(expected).assertComplete();
					}),

					dynamicTest("replay" + expectedReplay + "AndCompleteLateSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						flux.subscribe(); //first
						AssertSubscriber<Integer> late = AssertSubscriber.create();

						List<Integer> expected = new ArrayList<>();
						for (int i = 0; i < expectedReplay + 10; i++) {
							sink.next(i);
							if (i >= 10) expected.add(i);
						}
						sink.complete();
						flux.subscribe(late);

						late.assertValueSequence(expected).assertComplete();
					}),

					dynamicTest("replay" + expectedReplay + "AndErrorLateSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						flux.onErrorReturn(-1).subscribe(); //first subscriber, ignore errors
						AssertSubscriber<Integer> late = AssertSubscriber.create();

						List<Integer> expected = new ArrayList<>();
						for (int i = 0; i < expectedReplay + 10; i++) {
							sink.next(i);
							if (i >= 10) expected.add(i);
						}
						sink.error(new IllegalStateException("boom"));
						flux.subscribe(late);

						late.assertValueSequence(expected).assertErrorMessage("boom");
					})
			));
		}
	}

	DynamicContainer expectBufferingBeforeFirstSubscriber(Supplier<Sinks.StandaloneFluxSink<Integer>> sinkSupplier, int expectedBuffering) {
		if (expectedBuffering == NONE) {
			return dynamicContainer("no buffering before 1st subscriber", Stream.of(
					dynamicTest("doesNotBufferBeforeFirstSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> first = AssertSubscriber.create();

						sink.next(1).next(2).next(3);
						flux.subscribe(first);

						first.assertNoValues().assertNotComplete();

						sink.complete();
						first.assertNoValues().assertComplete();
					}),
					dynamicTest("immediatelyCompleteFirstSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> first = AssertSubscriber.create();

						sink.next(1).complete();
						flux.subscribe(first);

						first.assertNoValues().assertComplete();
					}),
					dynamicTest("immediatelyErrorFirstSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> first = AssertSubscriber.create();

						sink.next(1).error(new IllegalStateException("boom"));
						flux.subscribe(first);

						first.assertNoValues().assertErrorMessage("boom");
					})
			));
		}
		else if (expectedBuffering == ALL) {
			return dynamicContainer("buffers all before 1st subscriber", Stream.of(
					dynamicTest("doesBufferBeforeFirstSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> first = AssertSubscriber.create();

						sink.next(1).next(2).next(3);
						flux.subscribe(first);

						first.assertValues(1, 2, 3).assertNotComplete();

						sink.complete();
						first.assertComplete();
					}),

					dynamicTest("replayAndCompleteFirstSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> first = AssertSubscriber.create();

						sink.next(1).complete();
						flux.subscribe(first);

						first.assertValues(1).assertComplete();
					}),

					dynamicTest("replayAndErrorFirstSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> first = AssertSubscriber.create();

						sink.next(1).next(2).next(3).error(new IllegalStateException("boom"));
						flux.subscribe(first);

						first.assertValues(1, 2, 3).assertErrorMessage("boom");
					})
			));
		}
		else {
			return dynamicContainer("buffers " + expectedBuffering + " before 1st subscriber", Stream.of(
					dynamicTest("doesBuffer" + expectedBuffering + "BeforeFirstSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> first = AssertSubscriber.create();

						List<Integer> expected = new ArrayList<>();
						for (int i = 0; i < 10 + expectedBuffering; i++) {
							sink.next(i);
							if (i >= 10) {
								expected.add(i);
							}
						}
						flux.subscribe(first);

						first.assertValueSequence(expected).assertNotComplete();

						sink.complete();
						first.assertValueSequence(expected).assertComplete();
					}),
					dynamicTest("replayLimitedHistoryAndCompleteFirstSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> first = AssertSubscriber.create();

						List<Integer> expected = new ArrayList<>();
						for (int i = 0; i < 10 + expectedBuffering; i++) {
							sink.next(i);
							if (i >= 10) {
								expected.add(i);
							}
						}
						sink.complete();
						flux.subscribe(first);

						first.assertValueSequence(expected).assertComplete();
					}),
					dynamicTest("replayLimitedHistoryAndErrorFirstSubscriber", () -> {
						Sinks.StandaloneFluxSink<Integer> sink = sinkSupplier.get();
						Flux<Integer> flux = sink.asFlux();
						AssertSubscriber<Integer> first = AssertSubscriber.create();

						List<Integer> expected = new ArrayList<>();
						for (int i = 0; i < 10 + expectedBuffering; i++) {
							sink.next(i);
							if (i >= 10) {
								expected.add(i);
							}
						}
						sink.error(new IllegalStateException("boom"));
						flux.subscribe(first);

						first.assertValueSequence(expected).assertErrorMessage("boom");
					})
			));
		}
	}

}