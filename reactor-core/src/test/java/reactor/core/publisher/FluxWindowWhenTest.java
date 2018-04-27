/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;

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

	@Test
	public void noWindowRetained_gh975() throws InterruptedException {
		LongAdder created = new LongAdder();
		LongAdder finalized = new LongAdder();
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

			@Override
			protected void finalize() {
				finalized.increment();
			}
		}

		final CountDownLatch latch = new CountDownLatch(1);
		final FluxProcessorSink<Wrapper> processor = Processors.unicast();

		Flux<Integer> emitter = Flux.range(1, 400)
		                            .delayElements(Duration.ofMillis(10))
		                            .doOnNext(i -> processor.next(new Wrapper(i)))
		                            .doOnComplete(processor::complete);

		AtomicReference<FluxWindowWhen.WindowWhenMainSubscriber> startEndMain = new AtomicReference<>();
		AtomicReference<List> windows = new AtomicReference<>();

		Mono<List<Tuple3<Long, Integer, Long>>> buffers =
				processor.asFlux()
				         .window(Duration.ofMillis(1000), Duration.ofMillis(500))
				         .doOnSubscribe(s -> {
					         FluxWindowWhen.WindowWhenMainSubscriber sem =
							         (FluxWindowWhen.WindowWhenMainSubscriber) s;
					         startEndMain.set(sem);
					         windows.set(sem.windows);
				         })
				         .flatMap(f -> f.take(2))
				         .index()
				         .doOnNext(it -> System.gc())
				         //index, number of windows "in flight", finalized
				         .map(t2 -> Tuples.of(t2.getT1(), windows.get().size(), finalized.longValue()))
				         .doOnNext(v -> LOGGER.info(v.toString()))
				         .doOnComplete(latch::countDown)
				         .collectList();

		emitter.subscribe();

		List<Tuple3<Long, Integer, Long>> finalizeStats = buffers.block();

		//at least 5 intermediate finalize
		Condition<? super Tuple3<Long, Integer, Long>> hasFinalized = new Condition<>(
				t3 -> t3.getT3() > 0, "has finalized");
		assertThat(finalizeStats).areAtLeast(5, hasFinalized);

		assertThat(finalizeStats).allMatch(t3 -> t3.getT2() <= 3, "max 3 windows in flight");

		latch.await(10, TimeUnit.SECONDS);
		System.gc();
		Thread.sleep(500);

		assertThat(windows.get().size())
				.as("no window retained")
				.isZero();
		assertThat(finalized.longValue())
				.as("final GC collects all")
				.isEqualTo(created.longValue());
	}

	@Test
	public void normal() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		FluxProcessorSink<Integer> sp1 = Processors.direct();
		FluxProcessorSink<Integer> sp2 = Processors.direct();
		FluxProcessorSink<Integer> sp3 = Processors.direct();
		FluxProcessorSink<Integer> sp4 = Processors.direct();

		sp1.asFlux()
		   .windowWhen(sp2.asFlux(), v -> v == 1 ? sp3.asFlux() : sp4.asFlux())
		   .subscribe(ts);

		sp1.next(1);

		sp2.next(1);

		sp1.next(2);

		sp2.next(2);

		sp1.next(3);

		sp3.next(1);

		sp1.next(4);

		sp4.next(1);

		sp1.complete();

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 2, 3);
		expect(ts, 1, 3, 4);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp3 has subscribers?", sp3.hasDownstreams());
		Assert.assertFalse("sp4 has subscribers?", sp4.hasDownstreams());
	}

	@Test
	public void normalStarterEnds() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		FluxProcessorSink<Integer> source = Processors.direct();
		FluxProcessorSink<Integer> openSelector = Processors.direct();
		FluxProcessorSink<Integer> closeSelectorFor1 = Processors.direct();
		FluxProcessorSink<Integer> closeSelectorForOthers = Processors.direct();

		source.asFlux()
		      .windowWhen(openSelector.asFlux(), v -> v == 1 ? closeSelectorFor1.asFlux() : closeSelectorForOthers.asFlux())
		      .subscribe(ts);

		source.next(1);

		openSelector.next(1);

		source.next(2);

		openSelector.next(2);

		source.next(3);

		closeSelectorFor1.next(1);

		source.next(4);

		closeSelectorForOthers.next(1);

		openSelector.complete();
		source.complete(); //TODO evaluate, should the open completing cause the source to lose subscriber?

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 2, 3);
		expect(ts, 1, 3, 4);

		Assert.assertFalse("openSelector has subscribers?", openSelector.hasDownstreams());
		Assert.assertFalse("closeSelectorFor1 has subscribers?", closeSelectorFor1.hasDownstreams());
		Assert.assertFalse("closeSelectorForOthers has subscribers?", closeSelectorForOthers.hasDownstreams());
		Assert.assertFalse("source has subscribers?", source.hasDownstreams());
	}

	@Test
	public void oneWindowOnly() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		FluxProcessorSink<Integer> source = Processors.direct();
		FluxProcessorSink<Integer> openSelector = Processors.direct();
		FluxProcessorSink<Integer> closeSelectorFor1 = Processors.direct();
		FluxProcessorSink<Integer> closeSelectorOthers = Processors.direct();

		source.asFlux()
		      .windowWhen(openSelector.asFlux(), v -> v == 1 ? closeSelectorFor1.asFlux() : closeSelectorOthers.asFlux())
		      .subscribe(ts);

		openSelector.next(1);

		source.next(1);
		source.next(2);
		source.next(3);

		closeSelectorFor1.complete();

		source.next(4);
		source.complete();

		ts.assertValueCount(1)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 1, 2, 3);

		Assert.assertFalse("source has subscribers?", source.hasDownstreams());
		Assert.assertFalse("openSelector has subscribers?", openSelector.hasDownstreams());
		Assert.assertFalse("closeSelectorFor1 has subscribers?", closeSelectorFor1.hasDownstreams());
		Assert.assertFalse("closeSelectorOthers has subscribers?", closeSelectorOthers.hasDownstreams());
	}


	@Test
	public void windowWillAcumulateMultipleListsOfValuesOverlap() {
		//given: "a source and a collected flux"
		FluxProcessorSink<Integer> numbers = Processors.emitter();
		FluxProcessorSink<Integer> bucketOpening = Processors.emitter();

		//"overlapping buffers"
		FluxProcessorSink<Integer> boundaryFlux = Processors.emitter();

		MonoProcessor<List<List<Integer>>> res = numbers.asFlux()
		                                                .windowWhen(bucketOpening.asFlux(), u -> boundaryFlux.asFlux())
		                                                .flatMap(Flux::buffer)
		                                                .buffer()
		                                                .publishNext()
		                                                .toProcessor();
		res.subscribe();

		numbers.next(1);
		numbers.next(2);
		bucketOpening.next(1);
		numbers.next(3);
		bucketOpening.next(1);
		numbers.next(5);
		boundaryFlux.next(1);
		bucketOpening.next(1);
		boundaryFlux.complete();
		numbers.complete();

		//"the collected overlapping lists are available"
		assertThat(res.block()).containsExactly(
				Arrays.asList(3, 5),
				Arrays.asList(5));
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
