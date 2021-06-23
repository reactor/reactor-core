/*
 * Copyright (c) 2011-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
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
import reactor.util.concurrent.Queues;

// TODO Junit 5: would maybe be better handled as a dynamic test, but  was migrated kind of "as is" to make sure
// test count did not regress.
public class OnDiscardShouldNotLeakTest {

	private static final int NB_ITERATIONS = 100;
	// add DiscardScenarios here to test more operators
	private static final DiscardScenario[] SCENARIOS = new DiscardScenario[] {
			DiscardScenario.allFluxSourceArray("merge", 4, Flux::merge),
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
			DiscardScenario.fluxSource("flatMapIterable", f -> f.flatMapIterable(Arrays::asList)),
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
			DiscardScenario.fluxSource("unicastProcessor", f -> f.subscribeWith(UnicastProcessor.create())),
			DiscardScenario.fluxSource("unicastProcessorAndPublishOn", f -> f
					.subscribeWith(UnicastProcessor.create())
					.publishOn(Schedulers.immediate())),
			DiscardScenario.fluxSource("singleOrEmpty", f -> f.singleOrEmpty().onErrorReturn(Tracked.RELEASED)),
			DiscardScenario.fluxSource("collect", f -> f.collect(ArrayList::new, ArrayList::add)
			                                               .doOnSuccess(l -> l.forEach(Tracked::safeRelease))
			                                               .thenReturn(Tracked.RELEASED)),
			DiscardScenario.fluxSource("collectList", f -> f.collectList()
			                                                   .doOnSuccess(l -> l.forEach(Tracked::safeRelease))
			                                                   .thenReturn(Tracked.RELEASED))
	};

	private static final boolean[][] CONDITIONAL_AND_FUSED = new boolean[][] {
			{ false, false },
			{ true, false },
			{ false, true },
			{ true, true }
	};

	public static Collection<Object[]> data() {
		List<Object[]> parameters = new ArrayList<>(CONDITIONAL_AND_FUSED.length * SCENARIOS.length);
		for (DiscardScenario scenario : SCENARIOS) {
			for (boolean[] booleans : CONDITIONAL_AND_FUSED) {
				parameters.add(new Object[] { booleans[0], booleans[1], scenario });
			}
		}
		return parameters;
	}

	private Scheduler scheduler;
	private MemoryUtils.OffHeapDetector tracker;

	private void installScheduler(String description, int size) {
		scheduler = Schedulers.newParallel(description + "DiscardScheduler", size);
		scheduler.start();
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
	@ParameterizedTest(name="{index} {displayName} [{argumentsWithNames}]")
	@MethodSource("data")
	public void ensureMultipleSubscribersSupportWithNoLeaksWhenRacingCancelAndOnNextAndRequest(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		installScheduler(discardScenario.description, discardScenario.numberOfSubscriptions + 2 /* Cancel, request*/);

		int numberOfSubscriptions = discardScenario.numberOfSubscriptions;
		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			@SuppressWarnings("unchecked")
			TestPublisher<Tracked>[] testPublishers = new TestPublisher[numberOfSubscriptions];

			for (int i1 = 0; i1 < numberOfSubscriptions; i1++) {
				testPublishers[i1] = TestPublisher.createNoncompliant(
						TestPublisher.Violation.DEFER_CANCELLATION,
						TestPublisher.Violation.REQUEST_OVERFLOW);
			}

			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublishers[0], Arrays.copyOfRange(testPublishers, 1, testPublishers.length));

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
				runnables[j + 2] = () -> testPublishers[jj].next(tvalues[jj]);
			}
			RaceTestUtils.race(scheduler, runnables);

			List<Tracked> values = assertSubscriber.values();
			values.forEach(Tracked::release);

			tracker.assertNoLeaks();
		}
	}

	@DisplayName("Multiple Subscribers with populated queue racing Cancel/OnNext/Request")
	@ParameterizedTest(name="{index} {displayName} [{argumentsWithNames}]")
	@MethodSource("data")
	public void ensureMultipleSubscribersSupportWithNoLeaksWhenPopulatedQueueRacingCancelAndOnNextAndRequest(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isGreaterThan(1);

		installScheduler(discardScenario.description, discardScenario.numberOfSubscriptions + 2 /* Cancel, Request */);

		int numberOfSubscriptions = discardScenario.numberOfSubscriptions;

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			@SuppressWarnings("unchecked")
			TestPublisher<Tracked>[] testPublishers = new TestPublisher[numberOfSubscriptions];

			for (int i1 = 0; i1 < numberOfSubscriptions; i1++) {
				testPublishers[i1] = TestPublisher.createNoncompliant(
						TestPublisher.Violation.DEFER_CANCELLATION,
						TestPublisher.Violation.REQUEST_OVERFLOW);
			}

			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublishers[0], Arrays.copyOfRange(testPublishers, 1, testPublishers.length));

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
				runnables[j + 2] = () -> testPublishers[fj].next(tvalues[fj][0], tvalues[fj][1], tvalues[fj][2], tvalues[fj][3]);
			}

			RaceTestUtils.race(scheduler, runnables);
			List<Tracked> values = assertSubscriber.values();
			values.forEach(Tracked::release);

			tracker.assertNoLeaks();
		}
	}

	@DisplayName("Populated queue racing Cancel/OnNext")
	@ParameterizedTest(name="{index} {displayName} [{argumentsWithNames}]")
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnNext(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isOne();

		installScheduler(discardScenario.description, 2 /* Cancel, OnNext*3 */);

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			TestPublisher<Tracked> testPublisher = TestPublisher.createNoncompliant(
					TestPublisher.Violation.DEFER_CANCELLATION,
					TestPublisher.Violation.REQUEST_OVERFLOW);
			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublisher);

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
	@ParameterizedTest(name="{index} {displayName} [{argumentsWithNames}]")
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnComplete(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isOne();

		installScheduler(discardScenario.description, 2 /* Cancel, Complete*/);

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			TestPublisher<Tracked> testPublisher = TestPublisher.createNoncompliant(
					TestPublisher.Violation.DEFER_CANCELLATION,
					TestPublisher.Violation.REQUEST_OVERFLOW);
			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublisher);

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
	@ParameterizedTest(name="{index} {displayName} [{argumentsWithNames}]")
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnError(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isOne();

		installScheduler(discardScenario.description, 2 /* Cancel, Error */);

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			TestPublisher<Tracked> testPublisher = TestPublisher.createNoncompliant(
					TestPublisher.Violation.DEFER_CANCELLATION,
					TestPublisher.Violation.REQUEST_OVERFLOW);
			@SuppressWarnings("unchecked")
			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublisher);

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
	@ParameterizedTest(name="{index} {displayName} [{argumentsWithNames}]")
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOverflowError(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isOne();

		installScheduler(discardScenario.description, 2 /* Cancel, overflow error */);

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			TestPublisher<Tracked> testPublisher = TestPublisher.createNoncompliant(
					TestPublisher.Violation.DEFER_CANCELLATION,
					TestPublisher.Violation.REQUEST_OVERFLOW);
			@SuppressWarnings("unchecked")
			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublisher);

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
	@ParameterizedTest(name="{index} {displayName} [{argumentsWithNames}]")
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndRequest(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.numberOfSubscriptions).isOne();

		installScheduler(discardScenario.description, 2 /* Cancel, request */);

		for (int i = 0; i < NB_ITERATIONS; i++) {
			tracker.reset();
			TestPublisher<Tracked> testPublisher = TestPublisher.createNoncompliant(
					TestPublisher.Violation.DEFER_CANCELLATION,
					TestPublisher.Violation.REQUEST_OVERFLOW);
			@SuppressWarnings("unchecked")
			Publisher<Tracked> source = discardScenario.producePublisherFromSources(testPublisher);

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

		static DiscardScenario allFluxSourceArray(String desc, int subs,
				Function<Flux<Tracked>[], Publisher<Tracked>> producer) {
			return new DiscardScenario(desc, subs, (main, others) -> {
				@SuppressWarnings("unchecked")
				Flux<Tracked>[] inners = new Flux[subs];
				inners[0] = main.flux();
				for (int i = 1; i < subs; i++) {
					inners[i] = others[i - 1].flux();
				}
				return producer.apply(inners);
			});
		}

		final String description;
		final int numberOfSubscriptions;

		private final BiFunction<TestPublisher<Tracked>, TestPublisher<Tracked>[], Publisher<Tracked>>
				publisherProducer;

		DiscardScenario(String description, int numberOfSubscriptions, BiFunction<TestPublisher<Tracked>, TestPublisher<Tracked>[], Publisher<Tracked>> publisherProducer) {
			this.description = description;
			this.numberOfSubscriptions = numberOfSubscriptions;
			this.publisherProducer = publisherProducer;
		}

		@SafeVarargs
		final Publisher<Tracked> producePublisherFromSources(TestPublisher<Tracked> mainSource,
		                                                     TestPublisher<Tracked>... otherSources) {
			return publisherProducer.apply(mainSource, otherSources);
		}

		@Override
		public String toString() {
			return description;
		}
	}
}
