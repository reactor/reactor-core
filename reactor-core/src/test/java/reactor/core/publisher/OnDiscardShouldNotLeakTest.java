/*
 * Copyright (c) 2020-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.MemoryUtils;
import reactor.test.MemoryUtils.Tracked;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuples;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

// TODO Junit 5: would maybe be better handled as a dynamic test, but  was migrated kind of "as is" to make sure
// test count did not regress.
@Tag("slow")
public class OnDiscardShouldNotLeakTest {

	private static final int NB_ITERATIONS = 100;
	// add DiscardScenarios here to test more operators
	private static final DiscardScenario[] SCENARIOS = new DiscardScenario[] {
			DiscardScenario.allFluxSourceArray("merge", 4, Flux::merge),
			DiscardScenario.allFluxSourceArray("when", 4,
					sources -> Mono.when(sources.toArray(new Publisher[0])).thenReturn(Tracked.RELEASED)),
			DiscardScenario.allFluxSourceArray("monoZip", 4,
					sources -> Mono.zip(
										sources.stream()
					                           .map(Flux::last)
					                           .collect(Collectors.toList()),
						                values -> Arrays.stream(values)
						                                .map(v -> (Tracked) v)
						                                .collect(Collectors.toList())
					               )
					               .doOnSuccess(l -> l.forEach(Tracked::safeRelease))
					               .thenReturn(Tracked.RELEASED)),
			DiscardScenario.allFluxSourceArray("zip", 4,
					sources -> {
						Publisher<Tracked>[] sources1 = sources.toArray(new Publisher[0]);
						return Flux.zip(Tuples::fromArray, sources1)
						           .doOnNext(l -> {
							           for (Object o : (Iterable<Object>) l) {
								           ((Tracked) o).release();
							           }
						           })
						           .then(Mono.just(Tracked.RELEASED));
					}),
			DiscardScenario.allFluxSourceArray("zipFusedAll", 4,
					sources -> {
						Publisher<Tracked>[] sources1 = sources.stream().map(s -> s.publishOn(Schedulers.immediate())).toArray(Publisher[]::new);
						return Flux.zip(Tuples::fromArray, sources1)
						           .doOnNext(l -> {
							           for (Object o : (Iterable<Object>) l) {
								           ((Tracked) o).release();
							           }
						           })
						           .then(Mono.just(Tracked.RELEASED));
					}),
			DiscardScenario.allFluxSourceArray("zipFusedRandom", 4,
					sources -> {
						Publisher<Tracked>[] sources1 = sources.stream()
						                                       .map(s ->
								                                       ThreadLocalRandom.current()
								                                                        .nextBoolean() ?
										                                       s.publishOn(
												                                       Schedulers.immediate()) :
										                                       s)
						                                       .toArray(Publisher[]::new);
						return Flux.zip(Tuples::fromArray, sources1)
						           .doOnNext(l -> {
							           for (Object o : (Iterable<Object>) l) {
								           ((Tracked) o).release();
							           }
						           })
						           .then(Mono.just(Tracked.RELEASED));
					}),
			DiscardScenario.allFluxSourceArray("zipScalar", 4,
					sources -> {
						Publisher<Tracked>[] sources1 =
								Stream.concat(sources.stream(), Stream.of(Mono.just(Tracked.RELEASED))).toArray(Publisher[]::new);
						return Flux.zip(Tuples::fromArray, sources1)
						           .doOnNext(l -> {
							           for (Object o : (Iterable<Object>) l) {
								           ((Tracked) o).release();
							           }
						           })
						           .then(Mono.just(Tracked.RELEASED));
					}),
			DiscardScenario.allFluxSourceArray("zipMono", 4,
					sources -> {
						Mono<Tracked>[] sources1 =
								Stream.concat(sources.stream().map(f -> f.next()), Stream.of(Mono.just(Tracked.RELEASED))).toArray(Mono[]::new);
						return Mono.zip(Tuples::fromArray, sources1)
						           .doOnNext(l -> {
							           for (Object o : (Iterable<Object>) l) {
								           ((Tracked) o).release();
							           }
						           })
						           .then(Mono.just(Tracked.RELEASED));
					}),
			DiscardScenario.fluxSource("onBackpressureBuffer", Flux::onBackpressureBuffer),
			DiscardScenario.fluxSource("onBackpressureBufferAndPublishOn", f -> f
					.onBackpressureBuffer()
					.publishOn(Schedulers.immediate())),
			DiscardScenario.fluxSource("onBackpressureBufferAndPublishOnWithMaps", f -> f
					.onBackpressureBuffer()
					.map(Function.identity())
					.map(Function.identity())
					.map(Function.identity())
					.publishOn(Schedulers.immediate())),
			DiscardScenario.rawSource("flatMapInner", raw -> Flux.just(1).flatMap(f -> raw)),
			DiscardScenario.fluxSource("flatMap", main -> main.flatMap(f -> Mono.just(f).hide().flux())),
			DiscardScenario.fluxSource("monoFlatMap", main -> main.last().flatMap(f -> Mono.just(f).hide())),
			DiscardScenario.fluxSource("monoFilterWhen", main -> main.last().filterWhen(__ -> Mono.just(true).hide())),
			DiscardScenario.fluxSource("monoFilterWhenFalse", main -> main.last().filterWhen(__ -> Mono.just(false).hide())),
			DiscardScenario.fluxSource("last", main -> main.last(new Tracked("default")).flatMap(f -> Mono.just(f).hide())),
			DiscardScenario.fluxSource("flatMapIterable", f -> f.flatMapIterable(Arrays::asList)),
			// no asynchronicity is supported in these tests so long timeout is used to be
			// effectively disabled in bufferTimeout test:
			DiscardScenario.fluxSource("bufferTimeout", f -> f.bufferTimeout(2, Duration.ofDays(1), true).flatMapIterable(Function.identity())),
			DiscardScenario.fluxSource("publishOnDelayErrors", f -> f.publishOn(Schedulers.immediate())),
			DiscardScenario.fluxSource("publishOnImmediateErrors", f -> f.publishOn(Schedulers.immediate(), false, Queues.SMALL_BUFFER_SIZE)),
			DiscardScenario.fluxSource("publishOnAndPublishOn", main -> main
					.publishOn(Schedulers.immediate())
					.publishOn(Schedulers.immediate())),
			DiscardScenario.fluxSource("publishOnAndPublishOnWithMaps", main -> main
					.publishOn(Schedulers.immediate())
					.map(Function.identity())
					.map(Function.identity())
					.map(Function.identity())
					.publishOn(Schedulers.immediate())),
			// TODO: uncomment me. Proper discard is not supported yet since we dont have stable
			//  downstream context available all the time. This should be uncommented once we have
			//  an explicitly passed onDiscard handler
			/*DiscardScenario.fluxSource("publishOnAndPublish", main -> main
					.publishOn(Schedulers.immediate())
					.publish()
					.refCount()),*/
			DiscardScenario.sinkSource("unicastSink",  Sinks.unsafe().many().unicast()::onBackpressureBuffer, null),
			DiscardScenario.sinkSource("unicastSinkAndPublishOn",  Sinks.unsafe().many().unicast()::onBackpressureBuffer,
					f -> f.publishOn(Schedulers.immediate())),
			DiscardScenario.fluxSource("singleOrEmpty", f -> f.singleOrEmpty().onErrorReturn(Tracked.RELEASED)),
			DiscardScenario.fluxSource("collect", f -> f.collect(ArrayList::new, ArrayList::add)
			                                               .doOnSuccess(l -> l.forEach(Tracked::safeRelease))
			                                               .thenReturn(Tracked.RELEASED)),
			DiscardScenario.fluxSource("collectList", f -> f.collectList()
			                                                   .thenReturn(Tracked.RELEASED)),
			DiscardScenario.fluxSource("streamCollector", f -> f.collect(Collectors.toList())
			                                               .doOnSuccess(l -> l.forEach(Tracked::safeRelease))
			                                               .thenReturn(Tracked.RELEASED)),
			DiscardScenario.fluxSource("reduce", f -> f.reduce((t1, t2) -> {
				                                           t1.release();
				                                           return t2;
			                                           })
			                                           .thenReturn(Tracked.RELEASED)),
			DiscardScenario.fluxSource("reduceSeed", f -> f.reduce(new Tracked("seed"), (t1, t2) -> {
				                                           t1.release();
				                                           return t2;
			                                           })
			                                           .thenReturn(Tracked.RELEASED)),
			DiscardScenario.fluxSource("reduceWith", f -> f.reduceWith(() -> new Tracked("seed"), (t1, t2) -> {
				                                               t1.release();
				                                               return t2;
			                                               })
			                                               .thenReturn(Tracked.RELEASED)),
			DiscardScenario.fluxSource("reduceWith", f -> f.defaultIfEmpty(new Tracked("default")))
	};

	private static final boolean[][] CONDITIONAL_AND_FUSED = new boolean[][] {
			{ false, false },
			{ true, false },
			{ false, true },
			{ true, true }
	};

	public static List<Arguments> data() {
		List<Arguments> parameters = new ArrayList<>(CONDITIONAL_AND_FUSED.length * SCENARIOS.length);
		for (DiscardScenario scenario : SCENARIOS) {
			for (boolean[] booleans : CONDITIONAL_AND_FUSED) {
				StringBuilder desc = new StringBuilder("for ").append(scenario.description);
				if (booleans[0]) {
					desc.append(" (conditional");
					if (booleans[1]) desc.append(" and fused");
					desc.append(')');
				}
				else if (booleans[1]) desc.append(" (fused)");

				parameters.add(arguments(booleans[0], booleans[1], Named.named(desc.toString(), scenario)));
			}
		}
		return parameters;
	}

	private Scheduler scheduler;
	private MemoryUtils.OffHeapDetector tracker;

	private void installScheduler(String description, int size) {
		scheduler = Schedulers.newParallel(description + "DiscardScheduler", size);
	}

	@BeforeEach
	void setUp() {
		tracker = new MemoryUtils.OffHeapDetector();
		Hooks.onNextDropped(Tracked::safeRelease);
		Hooks.onErrorDropped(e -> {});
		Hooks.onOperatorError((e, v) -> null);
	}

	@AfterEach
	void tearDown() {
		Hooks.resetOnNextDropped();
		Hooks.resetOnErrorDropped();
		Hooks.resetOnNextError();
		Hooks.resetOnOperatorError();
		if (scheduler != null) {
			scheduler.dispose();
		}
	}

	@DisplayName("Multiple Subscribers racing Cancel/OnNext/Request")
	@ParameterizedTest(name="{displayName} [{index}] {2}")
	@MethodSource("data")
	public void ensureMultipleSubscribersSupportWithNoLeaksWhenRacingCancelAndOnNextAndRequest(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		installScheduler(discardScenario.description, discardScenario.numberOfSubscriptions + 2 /* Cancel, request*/);

		int numberOfSubscriptions = discardScenario.numberOfSubscriptions;
		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			List<TestPublisher<Tracked>> testPublishers = new ArrayList<>(numberOfSubscriptions);

			for (int i1 = 0; i1 < numberOfSubscriptions; i1++) {
				testPublishers.add(TestPublisher.createNoncompliant(
						TestPublisher.Violation.DEFER_CANCELLATION,
						TestPublisher.Violation.REQUEST_OVERFLOW)
				);
			}

			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublishers.get(0), testPublishers.subList(1, testPublishers.size()));

			if (conditional) {
				if (source instanceof Flux) {
					source = ((Flux<Tracked>) source).filter(t -> true);
				}
				else {
					source = ((Mono<Tracked>) source).filter(t -> true);
				}
			}

			AssertSubscriber<Tracked> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
			if (fused) {
				assertSubscriber.requestedFusionMode(Fuseable.ANY);
			}
			source.subscribe(assertSubscriber);

			Tracked[] tvalues = new Tracked[numberOfSubscriptions];
			Runnable[] runnables = new Runnable[numberOfSubscriptions + 2];
			runnables[0] = assertSubscriber::cancel;
			runnables[1] = () -> assertSubscriber.request(Long.MAX_VALUE);
			for (int j = 0 ; j < numberOfSubscriptions ; j++) {
				int jj = j;
				tvalues[j] = tracker.track(j);
				runnables[j + 2] = () -> testPublishers.get(jj).next(tvalues[jj]);
			}
			RaceTestUtils.race(scheduler, runnables);

			List<Tracked> values = assertSubscriber.values();
			values.forEach(Tracked::release);

			tracker.assertNoLeaks();
		}
	}

	@DisplayName("Multiple Subscribers with populated queue racing Cancel/OnNext/Request")
	@ParameterizedTest(name="{displayName} [{index}] {2}")
	@MethodSource("data")
	public void ensureMultipleSubscribersSupportWithNoLeaksWhenPopulatedQueueRacingCancelAndOnNextAndRequest(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isGreaterThan(1);

		installScheduler(discardScenario.description, discardScenario.numberOfSubscriptions + 2 /* Cancel, Request */);

		int numberOfSubscriptions = discardScenario.numberOfSubscriptions;

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			List<TestPublisher<Tracked>> testPublishers = new ArrayList<>(numberOfSubscriptions);

			for (int i1 = 0; i1 < numberOfSubscriptions; i1++) {
				testPublishers.add(TestPublisher.createNoncompliant(
						TestPublisher.Violation.DEFER_CANCELLATION,
						TestPublisher.Violation.REQUEST_OVERFLOW)
				);
			}

			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublishers.get(0), testPublishers.subList(1, testPublishers.size()));

			if (conditional) {
				if (source instanceof Flux) {
					source = ((Flux<Tracked>) source).filter(t -> true);
				}
				else {
					source = ((Mono<Tracked>) source).filter(t -> true);
				}
			}

			AssertSubscriber<Tracked> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
			if (fused) {
				assertSubscriber.requestedFusionMode(Fuseable.ANY);
			}
			source.subscribe(assertSubscriber);

			Tracked[][] tvalues = new Tracked[numberOfSubscriptions][4];
			Runnable[] runnables = new Runnable[numberOfSubscriptions + 2];
			runnables[0] = assertSubscriber::cancel;
			runnables[1] = () -> assertSubscriber.request(Long.MAX_VALUE);
			for (int j = 0; j < numberOfSubscriptions; j++) {
				for (int jj = 0; jj < 4; jj++) {
					tvalues[j][jj] = tracker.track(String.format("%d-%d", j, jj));
				}
				int fj = j;
				runnables[j + 2] = () -> testPublishers.get(fj).next(tvalues[fj][0], tvalues[fj][1], tvalues[fj][2], tvalues[fj][3]);
			}

			RaceTestUtils.race(scheduler, runnables);
			List<Tracked> values = assertSubscriber.values();
			values.forEach(Tracked::release);

			tracker.assertNoLeaks();
		}
	}

	@DisplayName("Populated queue racing Cancel/OnNext")
	@ParameterizedTest(name="{displayName} [{index}] {2}")
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnNext(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isOne();

		installScheduler(discardScenario.description, 2 /* Cancel, OnNext*3 */);

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			TestPublisher<Tracked> testPublisher = TestPublisher.createNoncompliant(
					TestPublisher.Violation.DEFER_CANCELLATION,
					TestPublisher.Violation.REQUEST_OVERFLOW);
			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublisher, null);

			if (conditional) {
				if (source instanceof Flux) {
					source = ((Flux<Tracked>) source).filter(t -> true);
				}
				else {
					source = ((Mono<Tracked>) source).filter(t -> true);
				}
			}

			Scannable scannable = Scannable.from(source);
			Integer prefetch = scannable.scan(Scannable.Attr.PREFETCH);

			Assumptions.assumeThat(prefetch).isNotZero();

			AssertSubscriber<Tracked> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
			if (fused) {
				assertSubscriber.requestedFusionMode(Fuseable.ANY);
			}
			source.subscribe(assertSubscriber);

			testPublisher.next(tracker.track(1));
			testPublisher.next(tracker.track(2));

			Tracked value3 = tracker.track(3);
			Tracked value4 = tracker.track(4);
			Tracked value5 = tracker.track(5);

			RaceTestUtils.race(scheduler, assertSubscriber::cancel, () -> {
				testPublisher.next(value3);
				testPublisher.next(value4);
				testPublisher.next(value5);
			});

			List<Tracked> values = assertSubscriber.values();
			values.forEach(Tracked::release);

			tracker.assertNoLeaks();
		}
	}

	@DisplayName("Populated queue racing Cancel/OnComplete")
	@ParameterizedTest(name="{displayName} [{index}] {2}")
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnComplete(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isOne();

		installScheduler(discardScenario.description, 2 /* Cancel, Complete*/);

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			TestPublisher<Tracked> testPublisher = TestPublisher.createNoncompliant(
					TestPublisher.Violation.DEFER_CANCELLATION,
					TestPublisher.Violation.REQUEST_OVERFLOW);
			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublisher, null);

			if (conditional) {
				if (source instanceof Flux) {
					source = ((Flux<Tracked>) source).filter(t -> true);
				}
				else {
					source = ((Mono<Tracked>) source).filter(t -> true);
				}
			}

			Scannable scannable = Scannable.from(source);
			Integer prefetch = scannable.scan(Scannable.Attr.PREFETCH);

			Assumptions.assumeThat(prefetch).isNotZero();

			AssertSubscriber<Tracked> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
			if (fused) {
				assertSubscriber.requestedFusionMode(Fuseable.ANY);
			}
			source.subscribe(assertSubscriber);

			testPublisher.next(tracker.track(1));
			testPublisher.next(tracker.track(2));
			testPublisher.next(tracker.track(3));
			testPublisher.next(tracker.track(4));

			RaceTestUtils.race(
					scheduler, assertSubscriber::cancel,
					() -> testPublisher.complete()
			);

			List<Tracked> values = assertSubscriber.values();
			values.forEach(Tracked::release);

			tracker.assertNoLeaks();
		}
	}

	@DisplayName("Populated queue racing Cancel/OnError")
	@ParameterizedTest(name="{displayName} [{index}] {2}")
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnError(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isOne();

		installScheduler(discardScenario.description, 2 /* Cancel, Error */);

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			TestPublisher<Tracked> testPublisher = TestPublisher.createNoncompliant(
					TestPublisher.Violation.DEFER_CANCELLATION,
					TestPublisher.Violation.REQUEST_OVERFLOW);
			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublisher, null);

			if (conditional) {
				if (source instanceof Flux) {
					source = ((Flux<Tracked>) source).filter(t -> true);
				}
				else {
					source = ((Mono<Tracked>) source).filter(t -> true);
				}
			}

			Scannable scannable = Scannable.from(source);
			Integer prefetch = scannable.scan(Scannable.Attr.PREFETCH);

			Assumptions.assumeThat(prefetch).isNotZero();

			AssertSubscriber<Tracked> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
			if (fused) {
				assertSubscriber.requestedFusionMode(Fuseable.ANY);
			}
			source.subscribe(assertSubscriber);

			testPublisher.next(tracker.track(1));
			testPublisher.next(tracker.track(2));
			testPublisher.next(tracker.track(3));
			testPublisher.next(tracker.track(4));

			RaceTestUtils.race(
					scheduler, assertSubscriber::cancel,
					() -> testPublisher.error(new RuntimeException("test"))
			);

			List<Tracked> values = assertSubscriber.values();
			values.forEach(Tracked::release);
			if (assertSubscriber.isTerminated()) {
				// has a chance to error with rejected exception
				assertSubscriber.assertError();
			}

			tracker.assertNoLeaks();
		}
	}

	@DisplayName("Populated queue racing Cancel/overflow Error")
	@ParameterizedTest(name="{displayName} [{index}] {2}")
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOverflowError(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isOne();

		installScheduler(discardScenario.description, 2 /* Cancel, overflow error */);

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			TestPublisher<Tracked> testPublisher = TestPublisher.createNoncompliant(
					TestPublisher.Violation.DEFER_CANCELLATION,
					TestPublisher.Violation.REQUEST_OVERFLOW);
			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublisher, null);

			if (conditional) {
				if (source instanceof Flux) {
					source = ((Flux<Tracked>) source).filter(t -> true);
				}
				else {
					source = ((Mono<Tracked>) source).filter(t -> true);
				}
			}

			Scannable scannable = Scannable.from(source);
			Integer capacity = scannable.scan(Scannable.Attr.CAPACITY);
			Integer prefetch = Math.min(scannable.scan(Scannable.Attr.PREFETCH),
					capacity == 0 ? Integer.MAX_VALUE : capacity);

			Assumptions.assumeThat(prefetch).isNotZero();
			Assumptions.assumeThat(prefetch).isNotEqualTo(Integer.MAX_VALUE);
			AssertSubscriber<Tracked> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
			if (fused) {
				assertSubscriber.requestedFusionMode(Fuseable.ANY);
			}
			source.subscribe(assertSubscriber);

			for (int j = 0; j < prefetch - 1; j++) {
				testPublisher.next(tracker.track(j));
			}

			Tracked lastValue = tracker.track(prefetch - 1);
			Tracked overflowValue1 = tracker.track(prefetch);
			Tracked overflowValue2 = tracker.track(prefetch + 1);

			RaceTestUtils.race(assertSubscriber::cancel, () -> {
				testPublisher.next(lastValue);
				testPublisher.next(overflowValue1);
				testPublisher.next(overflowValue2);
			});

			List<Tracked> values = assertSubscriber.values();
			values.forEach(Tracked::release);
			if (assertSubscriber.isTerminated()) {
				// has a chance to error with rejected exception
				assertSubscriber.assertError();
			}

			tracker.assertNoLeaks();
		}
	}

	@DisplayName("Populated queue racing Cancel/Request")
	@ParameterizedTest(name="{displayName} [{index}] {2}")
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndRequest(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isOne();

		installScheduler(discardScenario.description, 2 /* Cancel, request */);

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			TestPublisher<Tracked> testPublisher = TestPublisher.createNoncompliant(
					TestPublisher.Violation.DEFER_CANCELLATION,
					TestPublisher.Violation.REQUEST_OVERFLOW);
			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublisher, null);

			if (conditional) {
				if (source instanceof Flux) {
					source = ((Flux<Tracked>) source).filter(t -> true);
				}
				else {
					source = ((Mono<Tracked>) source).filter(t -> true);
				}
			}

			Scannable scannable = Scannable.from(source);
			Integer prefetch = scannable.scan(Scannable.Attr.PREFETCH);

			Assumptions.assumeThat(prefetch).isNotZero();

			AssertSubscriber<Tracked> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
			if (fused) {
				assertSubscriber.requestedFusionMode(Fuseable.ANY);
			}
			source.subscribe(assertSubscriber);

			testPublisher.next(tracker.track(1));
			testPublisher.next(tracker.track(2));
			testPublisher.next(tracker.track(3));
			testPublisher.next(tracker.track(4));

			RaceTestUtils.race(
					scheduler, assertSubscriber::cancel,
					() -> assertSubscriber.request(Long.MAX_VALUE)
			);

			List<Tracked> values = assertSubscriber.values();
			values.forEach(Tracked::release);

			tracker.assertNoLeaks();
		}
	}

	static class DiscardScenario {

		static DiscardScenario rawSource(String desc, Function<TestPublisher<Tracked>, Publisher<Tracked>> rawToPublisherProducer) {
			return new DiscardScenario(desc, 1, (main, others) -> rawToPublisherProducer.apply(main));
		}

		static DiscardScenario fluxSource(String desc, Function<Flux<Tracked>, Publisher<Tracked>> fluxToPublisherProducer) {
			return new DiscardScenario(desc, 1, (main, others) -> fluxToPublisherProducer.apply(main.flux()));
		}

		static DiscardScenario sinkSource(String desc,
				Supplier<Sinks.Many<Tracked>> sinksManySupplier,
				@Nullable Function<Flux<Tracked>, Flux<Tracked>> furtherTransformation) {
			return new DiscardScenario(desc, 1, (main, others) -> {
				Sinks.Many<Tracked> sink = sinksManySupplier.get();
				//noinspection CallingSubscribeInNonBlockingScope
				main.flux().subscribe(
						v -> sink.emitNext(v, FAIL_FAST),
						e -> sink.emitError(e, FAIL_FAST),
						() -> sink.emitComplete(FAIL_FAST));
				Flux<Tracked> finalFlux = sink.asFlux();
				if (furtherTransformation != null) {
					finalFlux = furtherTransformation.apply(finalFlux);
				}
				return finalFlux;
			});
		}

		static DiscardScenario allFluxSourceArray(String desc, int subs,
				Function<List<Flux<Tracked>>, Publisher<Tracked>> producer) {
			return new DiscardScenario(desc, subs, (main, others) -> {
				List<Flux<Tracked>> inners = new ArrayList<>(subs);
				inners.add(main.flux());
				for (int i = 1; i < subs; i++) {
					inners.add( others.get(i - 1).flux());
				}
				return producer.apply(inners);
			});
		}

		final String description;
		final int numberOfSubscriptions;

		private final BiFunction<TestPublisher<Tracked>, List<TestPublisher<Tracked>>, Publisher<Tracked>>
				publisherProducer;

		DiscardScenario(String description, int numberOfSubscriptions, BiFunction<TestPublisher<Tracked>, List<TestPublisher<Tracked>>, Publisher<Tracked>> publisherProducer) {
			this.description = description;
			this.numberOfSubscriptions = numberOfSubscriptions;
			this.publisherProducer = publisherProducer;
		}

		Publisher<Tracked> producePublisherFromSources(TestPublisher<Tracked> mainSource, List<TestPublisher<Tracked>> otherSources) {
			return publisherProducer.apply(mainSource, otherSources);
		}

		@Override
		public String toString() {
			return description;
		}
	}
}
