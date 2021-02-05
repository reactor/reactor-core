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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
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
@Tag("slow")
public class OnDiscardShouldNotLeakTest {

	// add DiscardScenarios here to test more operators
	private static DiscardScenario[] SCENARIOS = new DiscardScenario[] {
			DiscardScenario.allFluxSourceArray("merge", 4, Flux::merge),
			DiscardScenario.fluxSource("onBackpressureBuffer", 1, Flux::onBackpressureBuffer),
			DiscardScenario.fluxSource("onBackpressureBufferAndPublishOn", 1, f -> f
					.onBackpressureBuffer()
					.publishOn(Schedulers.immediate())),
			DiscardScenario.fluxSource("onBackpressureBufferAndPublishOnWithMaps", 1, f -> f
					.onBackpressureBuffer()
					.map(Function.identity())
					.map(Function.identity())
					.map(Function.identity())
					.publishOn(Schedulers.immediate())),
			DiscardScenario.rawSource("flatMapInner", 1, raw -> Flux.just(1).flatMap(f -> raw)),
			DiscardScenario.fluxSource("flatMap", 1, main -> main.flatMap(f -> Mono.just(f).hide().flux())),
			DiscardScenario.fluxSource("flatMapIterable", 1, f -> f.flatMapIterable(Arrays::asList)),
			DiscardScenario.fluxSource("publishOnDelayErrors", 1, f -> f.publishOn(Schedulers.immediate())),
			DiscardScenario.fluxSource("publishOnImmediateErrors", 1, f -> f.publishOn(Schedulers.immediate(), false, Queues.SMALL_BUFFER_SIZE)),
			DiscardScenario.fluxSource("publishOnAndPublishOn", 1, main -> main
					.publishOn(Schedulers.immediate())
					.publishOn(Schedulers.immediate())),
			DiscardScenario.fluxSource("publishOnAndPublishOnWithMaps", 1, main -> main
					.publishOn(Schedulers.immediate())
					.map(Function.identity())
					.map(Function.identity())
					.map(Function.identity())
					.publishOn(Schedulers.immediate())),
			DiscardScenario.fluxSource("unicastProcessor", 1, f -> f.subscribeWith(UnicastProcessor.create())),
			DiscardScenario.fluxSource("unicastProcessorAndPublishOn", 1, f -> f
					.subscribeWith(UnicastProcessor.create())
					.publishOn(Schedulers.immediate())),
			//FIXME known issue in 3.3.14.RELEASE and 3.4.3
//			DiscardScenario.fluxSource("singleOrEmpty", 1, f -> f.singleOrEmpty().onErrorReturn(Tracked.RELEASED)),
			DiscardScenario.fluxSource("collect", 1, f -> f.collect(ArrayList::new, ArrayList::add)
			                                               .doOnSuccess(l -> l.forEach(Tracked::safeRelease))
			                                               .thenReturn(Tracked.RELEASED)),
			DiscardScenario.fluxSource("collectList", 1, f -> f.collectList()
			                                                   .doOnSuccess(l -> l.forEach(Tracked::safeRelease))
			                                                   .thenReturn(Tracked.RELEASED))
	};

	private static boolean[][] CONDITIONAL_AND_FUSED = new boolean[][] {
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

	private boolean conditional;
	private boolean fused;
	private DiscardScenario discardScenario;

	private Scheduler scheduler;
	private MemoryUtils.OffHeapDetector tracker;

	private void installScheduler(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		this.conditional = conditional;
		this.fused = fused;
		this.discardScenario = discardScenario;

		scheduler = Schedulers.newParallel(discardScenario.scenarioDescription + "DiscardScheduler", discardScenario.subscriptionsNumber + 1);
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

	@ParameterizedTest
	@MethodSource("data")
	public void ensureMultipleSubscribersSupportWithNoLeaksWhenRacingCancelAndOnNextAndRequest(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		installScheduler(conditional, fused, discardScenario);

		int subscriptionsNumber = discardScenario.subscriptionsNumber;
		for (int i = 0; i < 10000; i++) {
			tracker.reset();
			int[] index = new int[]{subscriptionsNumber};
			TestPublisher<Tracked>[] testPublishers = new TestPublisher[subscriptionsNumber];

			for (int i1 = 0; i1 < subscriptionsNumber; i1++) {
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

			if (subscriptionsNumber == 1) {
				Tracked value = tracker.track(1);
				RaceTestUtils.race(
						scheduler, () -> RaceTestUtils.race(
								scheduler, assertSubscriber::cancel,
								() -> assertSubscriber.request(Long.MAX_VALUE)
						),
						() -> testPublishers[0].next(value)
				);
			}
			else {
				int startIndex = --index[0];
				Tracked value1 = tracker.track(startIndex);
				int secondIndex = --index[0];
				Tracked value2 = tracker.track(secondIndex);
				Runnable action = () -> RaceTestUtils.race(
						scheduler, () -> testPublishers[startIndex].next(value1),
						() -> testPublishers[secondIndex].next(value2)
				);

				while (index[0] > 0) {
					int nextIndex = --index[0];
					Tracked nextValue = tracker.track(nextIndex);
					Runnable nextAction = action;
					action = () -> RaceTestUtils.race(
							scheduler, nextAction,
							() -> testPublishers[nextIndex].next(nextValue)
					);
				}
				RaceTestUtils.race(scheduler, () ->
						RaceTestUtils.race(
								scheduler, assertSubscriber::cancel,
								() -> assertSubscriber.request(Long.MAX_VALUE)
						),
						action
				);
			}

			List<Tracked> values = assertSubscriber.values();
			values.forEach(Tracked::release);

			tracker.assertNoLeaks();
		}
	}

	@ParameterizedTest
	@MethodSource("data")
	public void ensureMultipleSubscribersSupportWithNoLeaksWhenPopulatedQueueRacingCancelAndOnNextAndRequest(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.subscriptionsNumber).isGreaterThan(1);

		installScheduler(conditional, fused, discardScenario);

		int subscriptionsNumber = discardScenario.subscriptionsNumber;

		for (int i = 0; i < 10000; i++) {
			tracker.reset();
			int[] index = new int[]{subscriptionsNumber};
			TestPublisher<Tracked>[] testPublishers = new TestPublisher[subscriptionsNumber];

			for (int i1 = 0; i1 < subscriptionsNumber; i1++) {
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
			int startIndex = --index[0];
			Tracked value11 = tracker.track(startIndex+"1");
			Tracked value12 = tracker.track(startIndex+"2");
			Tracked value13 = tracker.track(startIndex+"3");
			Tracked value14 = tracker.track(startIndex+"4");
			int secondIndex = --index[0];
			Tracked value21 = tracker.track(secondIndex+"1");
			Tracked value22 = tracker.track(secondIndex+"2");
			Tracked value23 = tracker.track(secondIndex+"3");
			Tracked value24 = tracker.track(secondIndex+"4");
			Runnable action = () -> RaceTestUtils.race(
					scheduler, () -> testPublishers[startIndex].next(value11, value12, value13, value14),
					() -> testPublishers[secondIndex].next(value21, value22, value23, value24)
			);

			while (index[0] > 0) {
				int nextIndex = --index[0];
				Tracked nextValue1 = tracker.track(nextIndex+"1");
				Tracked nextValue2 = tracker.track(nextIndex+"2");
				Tracked nextValue3 = tracker.track(nextIndex+"3");
				Tracked nextValue4 = tracker.track(nextIndex+"4");
				Runnable nextAction = action;
				action = () -> RaceTestUtils.race(
						scheduler, nextAction,
						() -> testPublishers[nextIndex].next(nextValue1, nextValue2, nextValue3, nextValue4)
				);
			}
			RaceTestUtils.race(scheduler, () ->
					RaceTestUtils.race(
							scheduler, assertSubscriber::cancel,
							() -> assertSubscriber.request(Long.MAX_VALUE)
					),
					action
			);
			List<Tracked> values = assertSubscriber.values();
			values.forEach(Tracked::release);

			tracker.assertNoLeaks();
		}
	}

	@ParameterizedTest
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnNext(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.subscriptionsNumber).isOne();

		installScheduler(conditional, fused, discardScenario);

		for (int i = 0; i < 10000; i++) {
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

	@ParameterizedTest
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnComplete(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.subscriptionsNumber).isOne();

		installScheduler(conditional, fused, discardScenario);

		for (int i = 0; i < 10000; i++) {
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

	@ParameterizedTest
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnError(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.subscriptionsNumber).isOne();

		installScheduler(conditional, fused, discardScenario);

		for (int i = 0; i < 10000; i++) {
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

	@ParameterizedTest
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOverflowError(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.subscriptionsNumber).isOne();

		installScheduler(conditional, fused, discardScenario);

		for (int i = 0; i < 10000; i++) {
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

	@ParameterizedTest
	@MethodSource("data")
	public void ensureNoLeaksPopulatedQueueAndRacingCancelAndRequest(boolean conditional, boolean fused, DiscardScenario discardScenario) {
		Assumptions.assumeThat(discardScenario.subscriptionsNumber).isOne();

		installScheduler(conditional, fused, discardScenario);

		for (int i = 0; i < 10000; i++) {
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

		static DiscardScenario rawSource(String desc, int subs, Function<TestPublisher<Tracked>, Publisher<Tracked>> rawToPublisherProducer) {
			return new DiscardScenario(desc, subs, (main, others) -> rawToPublisherProducer.apply(main));
		}

		static DiscardScenario fluxSource(String desc, int subs, Function<Flux<Tracked>, Publisher<Tracked>> fluxToPublisherProducer) {
			return new DiscardScenario(desc, subs, (main, others) -> fluxToPublisherProducer.apply(main.flux()));
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

		final String scenarioDescription;
		final int    subscriptionsNumber;

		private final BiFunction<TestPublisher<Tracked>, TestPublisher<Tracked>[], Publisher<Tracked>>
				publisherProducer;

		DiscardScenario(String description, int subscriptionsNumber, BiFunction<TestPublisher<Tracked>, TestPublisher<Tracked>[], Publisher<Tracked>> publisherProducer) {
			this.scenarioDescription = description;
			this.subscriptionsNumber = subscriptionsNumber;
			this.publisherProducer = publisherProducer;
		}

		Publisher<Tracked> producePublisherFromSources(TestPublisher<Tracked> mainSource, TestPublisher<Tracked>... otherSources) {
			return publisherProducer.apply(mainSource, otherSources);
		}

		@Override
		public String toString() {
			return scenarioDescription;
		}
	}
}
