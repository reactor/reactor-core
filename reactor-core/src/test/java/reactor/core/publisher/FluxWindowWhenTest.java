/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.MemoryUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxWindowWhenTest {

	private static final Logger LOGGER = Loggers.getLogger(FluxWindowWhenTest.class);

	static <T> AssertSubscriber<T> toList(Publisher<T> windows) {
		AssertSubscriber<T> ts = AssertSubscriber.create();
		windows.subscribe(ts);
		return ts;
	}

	@SafeVarargs
	static <T> void expect(AssertSubscriber<Flux<T>> ts, int index, T... values) {
		toList(ts.values()
		         .get(index)).assertValues(values)
		                     .assertComplete()
		                     .assertNoError();
	}

	//see https://github.com/reactor/reactor-core/issues/975
	@Test
	public void noWindowRetained_gh975() throws InterruptedException {
		LongAdder created = new LongAdder();
		class Wrapper {

			final int i;

			Wrapper(int i) {
				created.increment();
				this.i = i;
			}

			@Override
			public String toString() {
				return "{i=" + i + '}';
			}
		}
		MemoryUtils.RetainedDetector finalizedTracker = new MemoryUtils.RetainedDetector();

		final CountDownLatch latch = new CountDownLatch(1);

		Flux<Wrapper> emitter = Flux.range(1, 400)
		                            .delayElements(Duration.ofMillis(10))
		                            .map(i -> finalizedTracker.tracked(new Wrapper(i)));

		AtomicReference<FluxWindowWhen.WindowWhenMainSubscriber> startEndMain = new AtomicReference<>();
		AtomicReference<List> windows = new AtomicReference<>();

		LOGGER.info("stat[index, windows in flight, finalized] windowStart windowEnd:");
		Mono<List<Tuple3<Long, Integer, Long>>> buffers =
				emitter.window(Duration.ofMillis(1000), Duration.ofMillis(500))
				         .doOnSubscribe(s -> {
					         FluxWindowWhen.WindowWhenMainSubscriber sem =
							         (FluxWindowWhen.WindowWhenMainSubscriber) s;
					         startEndMain.set(sem);
					         windows.set(sem.windows);
				         })
				         .flatMap(Flux::collectList)
				         .index().elapsed()
				         .doOnNext(it -> System.gc())
				         //index, number of windows "in flight", finalized
				       .map(elapsed -> {
					       long millis = elapsed.getT1();
					       Tuple2<Long, List<Wrapper>> t2 = elapsed.getT2();
					       Tuple3<Long, Integer, Long> stat = Tuples.of(t2.getT1(), windows.get().size(), finalizedTracker.finalizedCount());
					       //log the window boundaries without retaining them in the tuple
					       if (t2.getT2().isEmpty()) {
						       LOGGER.info("{}ms : {} empty window", millis, stat);
					       }
					       else {
						       LOGGER.info("{}ms : {}\t{}\t{}", millis, stat, t2.getT2().get(0),
								       t2.getT2().get(t2.getT2().size() - 1));
					       }
					       return stat;
				         })
				         .doOnComplete(latch::countDown)
				         .collectList();

		List<Tuple3<Long, Integer, Long>> finalizeStats = buffers.block();
		LOGGER.info("Tracked {} elements, collected {} windows", finalizedTracker.trackedTotal(), finalizeStats.size());

		//at least 5 intermediate finalize
		Condition<? super Tuple3<Long, Integer, Long>> hasFinalized = new Condition<>(
				t3 -> t3.getT3() > 0, "has finalized");
		assertThat(finalizeStats).areAtLeast(5, hasFinalized);

		assertThat(finalizeStats).allMatch(t3 -> t3.getT2() <= 5, "limited number of windows in flight");

		latch.await(10, TimeUnit.SECONDS);
		System.gc();
		Thread.sleep(500);

		assertThat(windows.get().size())
				.as("no window retained")
				.isZero();

		assertThat(finalizedTracker.finalizedCount())
				.as("final GC collects all")
				.isEqualTo(created.longValue());
	}

	@Test
	public void normal() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp3 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp4 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux().windowWhen(sp2.asFlux(), v -> v == 1 ? sp3.asFlux() : sp4.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);

		sp2.emitNext(1, FAIL_FAST);

		sp1.emitNext(2, FAIL_FAST);

		sp2.emitNext(2, FAIL_FAST);

		sp1.emitNext(3, FAIL_FAST);

		sp3.emitNext(1, FAIL_FAST);

		sp1.emitNext(4, FAIL_FAST);

		sp4.emitNext(1, FAIL_FAST);

		sp1.emitComplete(FAIL_FAST);

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 2, 3);
		expect(ts, 1, 3, 4);

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
		assertThat(sp3.currentSubscriberCount()).as("sp3 has subscriber").isZero();
		assertThat(sp4.currentSubscriberCount()).as("sp4 has subscriber").isZero();
	}

	@Test
	public void normalStarterEnds() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> openSelector = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> closeSelectorFor1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> closeSelectorForOthers = Sinks.unsafe().many().multicast().directBestEffort();

		source.asFlux().windowWhen(openSelector.asFlux(), v -> v == 1 ? closeSelectorFor1.asFlux() : closeSelectorForOthers.asFlux())
		      .subscribe(ts);

		source.emitNext(1, FAIL_FAST);

		openSelector.emitNext(1, FAIL_FAST);

		source.emitNext(2, FAIL_FAST);

		openSelector.emitNext(2, FAIL_FAST);

		source.emitNext(3, FAIL_FAST);

		closeSelectorFor1.emitNext(1, FAIL_FAST);

		source.emitNext(4, FAIL_FAST);

		closeSelectorForOthers.emitNext(1, FAIL_FAST);

		openSelector.emitComplete(FAIL_FAST);
		source.emitComplete(FAIL_FAST); //TODO evaluate, should the open completing cause the source to lose subscriber?

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 2, 3);
		expect(ts, 1, 3, 4);

		assertThat(openSelector.currentSubscriberCount()).as("openSelector has subscriber").isZero();
		assertThat(closeSelectorFor1.currentSubscriberCount()).as("closeSelectorFor1 has subscriber").isZero();
		assertThat(closeSelectorForOthers.currentSubscriberCount()).as("closeSelectorForOthers has subscriber").isZero();
		assertThat(source.currentSubscriberCount()).as("source has subscriber").isZero();
	}

	@Test
	public void oneWindowOnly() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> openSelector = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> closeSelectorFor1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> closeSelectorOthers = Sinks.unsafe().many().multicast().directBestEffort();

		source.asFlux().windowWhen(openSelector.asFlux(), v -> v == 1 ? closeSelectorFor1.asFlux() : closeSelectorOthers.asFlux())
		      .subscribe(ts);

		openSelector.emitNext(1, FAIL_FAST);

		source.emitNext(1, FAIL_FAST);
		source.emitNext(2, FAIL_FAST);
		source.emitNext(3, FAIL_FAST);

		closeSelectorFor1.emitComplete(FAIL_FAST);

		source.emitNext(4, FAIL_FAST);
		source.emitComplete(FAIL_FAST);

		ts.assertValueCount(1)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 1, 2, 3);

		assertThat(openSelector.currentSubscriberCount()).as("openSelector has subscriber").isZero();
		assertThat(closeSelectorFor1.currentSubscriberCount()).as("closeSelectorFor1 has subscriber").isZero();
		assertThat(closeSelectorOthers.currentSubscriberCount()).as("closeSelectorOthers has subscriber").isZero();
		assertThat(source.currentSubscriberCount()).as("source has subscriber").isZero();
	}


	@Test
	public void windowWillAccumulateMultipleListsOfValuesOverlap() {
		//given: "a source and a collected flux"
		Sinks.Many<Integer> numbers = Sinks.many().multicast().onBackpressureBuffer();
		Sinks.Many<Integer> bucketOpening = Sinks.many().multicast().onBackpressureBuffer();

		//"overlapping buffers"
		Sinks.Many<Integer> boundaryFlux = Sinks.many().multicast().onBackpressureBuffer();

		StepVerifier.create(numbers.asFlux()
								   .windowWhen(bucketOpening.asFlux(), u -> boundaryFlux.asFlux())
								   .flatMap(Flux::buffer)
								   .collectList())
					.then(() -> {
						numbers.emitNext(1, FAIL_FAST);
						numbers.emitNext(2, FAIL_FAST);
						bucketOpening.emitNext(1, FAIL_FAST);
						numbers.emitNext(3, FAIL_FAST);
						bucketOpening.emitNext(1, FAIL_FAST);
						numbers.emitNext(5, FAIL_FAST);
						boundaryFlux.emitNext(1, FAIL_FAST);
						bucketOpening.emitNext(1, FAIL_FAST);
						boundaryFlux.emitComplete(FAIL_FAST);
						numbers.emitComplete(FAIL_FAST);
						//"the collected overlapping lists are available"
					})
					.assertNext(res -> assertThat(res).containsExactly(Arrays.asList(3, 5), Arrays.asList(5)))
					.verifyComplete();
	}



	Flux<List<Integer>> scenario_windowWillSubdivideAnInputFluxOverlapTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .window(Duration.ofMillis(300), Duration.ofMillis(200))
		           .concatMap(Flux::buffer);
	}

	@Test
	public void windowWillSubdivideAnInputFluxOverlapTime() {
		StepVerifier.withVirtualTime(this::scenario_windowWillSubdivideAnInputFluxOverlapTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2, 3))
		            .assertNext(t -> assertThat(t).containsExactly(3, 4, 5))
		            .assertNext(t -> assertThat(t).containsExactly(5, 6, 7))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}


	Flux<List<Integer>> scenario_windowWillSubdivideAnInputFluxSameTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .window(Duration.ofMillis(300), Duration.ofMillis(300))
		           .concatMap(Flux::buffer);
	}

	@Test
	public void windowWillSubdivideAnInputFluxSameTime() {
		StepVerifier.withVirtualTime(this::scenario_windowWillSubdivideAnInputFluxSameTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2, 3))
		            .assertNext(t -> assertThat(t).containsExactly(4, 5, 6))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}

	Flux<List<Integer>> scenario_windowWillSubdivideAnInputFluxGapTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .window(Duration.ofMillis(200), Duration.ofMillis(300))
		           .concatMap(Flux::buffer);
	}

	@Test
	public void windowWillSubdivideAnInputFluxGapTime() {
		StepVerifier.withVirtualTime(this::scenario_windowWillSubdivideAnInputFluxGapTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2))
		            .assertNext(t -> assertThat(t).containsExactly(4, 5))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}

	@Test
	public void startError() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> start = TestPublisher.create();
		final TestPublisher<Integer> end = TestPublisher.create();

		StepVerifier.create(source.flux()
		                          .windowWhen(start, v -> end)
		                          .flatMap(Flux.identityFunction())
		)
		            .then(() -> start.error(new IllegalStateException("boom")))
		            .expectErrorMessage("boom")
		            .verify();

		source.assertNoSubscribers();
		start.assertNoSubscribers();
		end.assertNoSubscribers();
	}

	@Test
	public void startDoneThenError() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> start = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		final TestPublisher<Integer> end = TestPublisher.create();

		StepVerifier.create(source.flux()
		                          .windowWhen(start, v -> end)
		                          .flatMap(Flux.identityFunction())
		)
		            .then(() -> start.error(new IllegalStateException("boom"))
		                             .error(new IllegalStateException("boom2")))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDroppedErrorWithMessage("boom2");

		source.assertNoSubscribers();
		//start doesn't cleanup and as such still has a subscriber
		end.assertNoSubscribers();
	}

	@Test
	public void startDoneThenComplete() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> start = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		final TestPublisher<Integer> end = TestPublisher.create();

		StepVerifier.create(source.flux()
		                          .windowWhen(start, v -> end)
		                          .flatMap(Flux.identityFunction())
		)
		            .then(() -> start.error(new IllegalStateException("boom"))
		                             .complete())
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasNotDroppedErrors();

		source.assertNoSubscribers();
		//start doesn't cleanup and as such still has a subscriber
		end.assertNoSubscribers();
	}

	@Test
	public void startDoneThenNext() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> start = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		final TestPublisher<Integer> end = TestPublisher.create();

		StepVerifier.create(source.flux()
		                          .windowWhen(start, v -> end)
		                          .flatMap(Flux.identityFunction())
		)
		            .then(() -> start.error(new IllegalStateException("boom"))
		                             .next(1))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasNotDroppedErrors()
		            .hasNotDroppedElements();

		source.assertNoSubscribers();
		//start doesn't cleanup and as such still has a subscriber
		end.assertNoSubscribers();
	}

	@Test
	public void endError() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> start = TestPublisher.create();
		TestPublisher<Integer> end = TestPublisher.create();

		StepVerifier.create(source.flux()
		                          .windowWhen(start, v -> end)
		                          .flatMap(Flux.identityFunction())
		)
		            .then(() -> start.next(1))
		            .then(() -> end.error(new IllegalStateException("boom")))
		            .expectErrorMessage("boom")
		            .verify();

		source.assertNoSubscribers();
		start.assertNoSubscribers();
		end.assertNoSubscribers();
	}

	@Test
	public void endDoneThenError() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> start = TestPublisher.create();
		TestPublisher<Integer> end = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(source.flux()
		                          .windowWhen(start, v -> end)
		                          .flatMap(Flux.identityFunction())
		)
		            .then(() -> start.next(1))
		            .then(() -> end.error(new IllegalStateException("boom"))
		                           .error(new IllegalStateException("boom2")))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDroppedErrorWithMessage("boom2");

		source.assertNoSubscribers();
		start.assertNoSubscribers();
		//end doesn't cleanup and as such still has a subscriber
	}

	@Test
	public void endDoneThenComplete() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> start = TestPublisher.create();
		TestPublisher<Integer> end = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(source.flux()
		                          .windowWhen(start, v -> end)
		                          .flatMap(Flux.identityFunction())
		)
		            .then(() -> start.next(1))
		            .then(() -> end.error(new IllegalStateException("boom"))
		                           .complete())
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasNotDroppedErrors();

		source.assertNoSubscribers();
		start.assertNoSubscribers();
		//end doesn't cleanup and as such still has a subscriber
	}

	@Test
	public void endDoneThenNext() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> start = TestPublisher.create();
		TestPublisher<Integer> end = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(source.flux()
		                          .windowWhen(start, v -> end)
		                          .flatMap(Flux.identityFunction())
		)
		            .then(() -> start.next(1))
		            .then(() -> end.error(new IllegalStateException("boom"))
		                           .next(1))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasNotDroppedErrors()
		            .hasNotDroppedElements();

		source.assertNoSubscribers();
		start.assertNoSubscribers();
		//end doesn't cleanup and as such still has a subscriber
	}

	@Test
	public void mainError() {
		StepVerifier.create(Flux.error(new IllegalStateException("boom"))
		                        .windowWhen(Flux.never(), v -> Mono.just(1))
		                        .flatMap(Flux::count))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void mainDoneThenNext() {
		TestPublisher<Integer> source = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(source.flux()
		                        .windowWhen(Flux.never(), v -> Mono.just(1))
		                        .flatMap(Flux.identityFunction()))
		            .then(() -> source.complete().next(1))
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped(1);
	}

	@Test
	public void mainDoneThenError() {
		TestPublisher<Integer> source = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(source.flux()
		                        .windowWhen(Flux.never(), v -> Mono.just(1))
		                        .flatMap(Flux.identityFunction()))
		            .then(() -> source.complete().error(new IllegalStateException("boom")))
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedErrorWithMessage("boom");
	}

	@Test
	public void mainDoneThenComplete() {
		TestPublisher<Integer> source = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(source.flux()
		                        .windowWhen(Flux.never(), v -> Mono.just(1))
		                        .flatMap(Flux.identityFunction()))
		            .then(() -> source.complete().complete())
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedErrors()
		            .hasNotDroppedElements();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxWindowWhen<Integer, Integer, Object> test = new FluxWindowWhen<>(parent, Flux.just(2), v -> Flux.empty(), Queues.empty());

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanMainSubscriber() {
        CoreSubscriber<Flux<Integer>> actual = new LambdaSubscriber<>(null, e -> {}, null,
		        sub -> sub.request(1));
        FluxWindowWhen.WindowWhenMainSubscriber<Integer, Integer, Integer> test =
        		new FluxWindowWhen.WindowWhenMainSubscriber<>(actual,
				        Flux.never(), Flux::just,
				        Queues.unbounded());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onComplete();
		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();

		Assertions.assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM))
		          .isEqualTo(0);
		test.request(123L);
		Assertions.assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM))
		          .isEqualTo(123L);
    }

	@Test
    public void scanMainSubscriberError() {
        CoreSubscriber<Flux<Integer>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxWindowWhen.WindowWhenMainSubscriber<Integer, Integer, Integer> test =
        		new FluxWindowWhen.WindowWhenMainSubscriber<>(actual,
				        Flux.never(), Flux::just,
				        Queues.unbounded());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		Assertions.assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

		test.onError(new IllegalStateException("boom"));
		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		Assertions.assertThat(test.scan(Scannable.Attr.ERROR))
		          .isNotNull()
		          .hasMessage("boom");
    }
}
