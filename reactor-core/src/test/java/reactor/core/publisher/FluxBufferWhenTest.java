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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.FluxBufferWhen.BufferWhenMainSubscriber;
import reactor.test.MemoryUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxBufferWhenTest {

	private static final Logger LOGGER = Loggers.getLogger(FluxBufferWhenTest.class);

	//see https://github.com/reactor/reactor-core/issues/969
	@Test
	public void bufferedCanCompleteIfOpenNeverCompletesDropping() {
		//this test ensures that dropping buffers will complete if the source is exhausted before the open publisher finishes
		Mono<Integer> buffered = Flux.range(1, 200)
									 .delayElements(Duration.ofMillis(25))
									 .bufferWhen(Flux.interval(Duration.ZERO, Duration.ofMillis(200)), open -> Mono.delay(Duration.ofMillis(100)))
									 .log(LOGGER, Level.FINE, false)
									 .reduce(new HashSet<Integer>(), (set, buffer) -> {
										 set.addAll(buffer);
										 return set;
									 })
									 .map(HashSet::size);

		StepVerifier.create(buffered)
					.assertNext(size -> assertThat(size).as("approximate size with drops")
														.isBetween(80, 110))
					.expectComplete()
					.verify(Duration.ofSeconds(10));
	}

	//see https://github.com/reactor/reactor-core/issues/969
	@Test
	public void bufferedCanCompleteIfOpenNeverCompletesOverlapping() {
		//this test ensures that overlapping buffers will complete if the source is exhausted before the open publisher finishes
		Mono<Integer> buffered = Flux.range(1, 200)
									 .delayElements(Duration.ofMillis(25))
									 .bufferWhen(Flux.interval(Duration.ZERO, Duration.ofMillis(100)), open -> Mono.delay(Duration.ofMillis(200)))
									 .log(LOGGER, Level.FINE, false)
									 .reduce(new HashSet<Integer>(), (set, buffer) -> {
										 set.addAll(buffer);
										 return set;
									 })
									 .map(HashSet::size);

		StepVerifier.create(buffered)
					.expectNext(200)
					.expectComplete()
					.verify(Duration.ofSeconds(10));
	}

	//see https://github.com/reactor/reactor-core/issues/969
	@Test
	public void timedOutBuffersDontLeak() throws InterruptedException {
		LongAdder created = new LongAdder();
		MemoryUtils.RetainedDetector retainedDetector = new MemoryUtils.RetainedDetector();
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

		final CountDownLatch latch = new CountDownLatch(1);

		Flux<Wrapper> emitter = Flux.range(1, 400)
									.delayElements(Duration.ofMillis(10))
									.map(i -> retainedDetector.tracked(new Wrapper(i)));

		Mono<List<Tuple3<Long, String, Long>>> buffers = emitter.buffer(Duration.ofMillis(1000), Duration.ofMillis(500))
																.filter(b -> b.size() > 0)
																.index()
																.elapsed()
																.doOnNext(it -> System.gc())
																//index, bounds of buffer, finalized
																.map(elapsed -> {
																	long millis = elapsed.getT1();
																	Tuple2<Long, List<Wrapper>> t2 = elapsed.getT2();
																	Tuple3<Long, String, Long> stat = Tuples.of(t2.getT1(), String.format("from %s to %s", t2.getT2()
																																							 .get(0), t2.getT2()
																																										.get(t2.getT2()
																																											   .size() - 1)), retainedDetector.finalizedCount());

																	LOGGER.info("{}ms : {}", millis, stat);
																	return stat;
																})
																.doOnComplete(latch::countDown)
																.collectList();

		List<Tuple3<Long, String, Long>> finalizeStats = buffers.block(Duration.ofSeconds(50));

		Condition<? super Tuple3<Long, String, Long>> hasFinalized = new Condition<>(t3 -> t3.getT3() > 0, "has finalized");

		//at least 5 intermediate finalize
		assertThat(finalizeStats).areAtLeast(5, hasFinalized);

		assertThat(latch.await(1, TimeUnit.SECONDS)).as("buffers already blocked")
													.isTrue();
		LOGGER.debug("final GC");
		System.gc();
		Thread.sleep(500);

		assertThat(retainedDetector.finalizedCount()).as("final GC collects all")
													 .isEqualTo(created.longValue());
	}

	@Test
	public void normal() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp3 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp4 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .bufferWhen(sp2.asFlux(), v -> v == 1 ? sp3.asFlux() : sp4.asFlux())
		   .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitNext(1, FAIL_FAST);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.emitNext(1, FAIL_FAST);

		assertThat(sp3.currentSubscriberCount()).as("sp3 has no subscribers?").isPositive();

		sp1.emitNext(2, FAIL_FAST);
		sp1.emitNext(3, FAIL_FAST);
		sp1.emitNext(4, FAIL_FAST);

		sp3.emitComplete(FAIL_FAST);

		ts.assertValues(Arrays.asList(2, 3, 4))
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitNext(5, FAIL_FAST);

		sp2.emitNext(2, FAIL_FAST);

		assertThat(sp4.currentSubscriberCount()).as("sp4 has subscriber").isPositive();

		sp1.emitNext(6, FAIL_FAST);

		sp4.emitComplete(FAIL_FAST);

		ts.assertValues(Arrays.asList(2, 3, 4), Collections.singletonList(6))
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitComplete(FAIL_FAST);

		ts.assertValues(Arrays.asList(2, 3, 4), Collections.singletonList(6))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void startCompletes() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> open = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> close = Sinks.unsafe().many().multicast().directBestEffort();

		source.asFlux()
			  .bufferWhen(open.asFlux(), v -> close.asFlux())
			  .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.emitNext(1, FAIL_FAST);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		open.emitNext(1, FAIL_FAST);
		open.emitComplete(FAIL_FAST);

		assertThat(close.currentSubscriberCount()).as("close has subscriber").isPositive();

		source.emitNext(2, FAIL_FAST);
		source.emitNext(3, FAIL_FAST);
		source.emitNext(4, FAIL_FAST);

		close.emitComplete(FAIL_FAST);

		ts.assertValues(Arrays.asList(2, 3, 4))
		  .assertNoError()
		  .assertComplete();

		assertThat(source.currentSubscriberCount()).as("source has subscriber").isZero();
		assertThat(open.currentSubscriberCount()).as("open has subscriber").isZero();
		assertThat(close.currentSubscriberCount()).as("close has subscriber").isZero();
	}


	@Test
	public void bufferWillAcumulateMultipleListsOfValuesOverlap() {
		//given: "a source and a collected flux"
		Sinks.Many<Integer> numbers = Sinks.many().multicast().onBackpressureBuffer();
		Sinks.Many<Integer> bucketOpening = Sinks.many().multicast().onBackpressureBuffer();

		//"overlapping buffers"
		Sinks.Many<Integer> boundaryFlux = Sinks.many().multicast().onBackpressureBuffer();

		StepVerifier.create(numbers.asFlux()
								   .bufferWhen(bucketOpening.asFlux(), u -> boundaryFlux.asFlux())
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
					.assertNext(res -> assertThat(res).containsExactly(Arrays.asList(3, 5), Collections.singletonList(5), Collections.emptyList()))
					.verifyComplete();
	}

	Flux<List<Integer>> scenario_bufferWillSubdivideAnInputFluxOverlapTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
				   .delayElements(Duration.ofMillis(99))
				   .buffer(Duration.ofMillis(300), Duration.ofMillis(200));
	}

	@Test
	public void bufferWillSubdivideAnInputFluxOverlapTime() {
		StepVerifier.withVirtualTime(this::scenario_bufferWillSubdivideAnInputFluxOverlapTime)
					.thenAwait(Duration.ofSeconds(10))
					.assertNext(t -> assertThat(t).containsExactly(1, 2, 3))
					.assertNext(t -> assertThat(t).containsExactly(3, 4, 5))
					.assertNext(t -> assertThat(t).containsExactly(5, 6, 7))
					.assertNext(t -> assertThat(t).containsExactly(7, 8))
					.verifyComplete();
	}

	Flux<List<Integer>> scenario_bufferWillSubdivideAnInputFluxOverlapTime2() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
				   .delayElements(Duration.ofMillis(99))
				   .buffer(Duration.ofMillis(300L), Duration.ofMillis(200L));
	}

	@Test
	public void bufferWillSubdivideAnInputFluxOverlapTime2() {
		StepVerifier.withVirtualTime(this::scenario_bufferWillSubdivideAnInputFluxOverlapTime2)
					.thenAwait(Duration.ofSeconds(10))
					.assertNext(t -> assertThat(t).containsExactly(1, 2, 3))
					.assertNext(t -> assertThat(t).containsExactly(3, 4, 5))
					.assertNext(t -> assertThat(t).containsExactly(5, 6, 7))
					.assertNext(t -> assertThat(t).containsExactly(7, 8))
					.verifyComplete();
	}

	Flux<List<Integer>> scenario_bufferWillSubdivideAnInputFluxSameTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
				   .delayElements(Duration.ofMillis(99))
				   .buffer(Duration.ofMillis(300L), Duration.ofMillis(300L));
	}

	@Test
	public void bufferWillSubdivideAnInputFluxSameTime() {
		StepVerifier.withVirtualTime(this::scenario_bufferWillSubdivideAnInputFluxSameTime)
					.thenAwait(Duration.ofSeconds(10))
					.assertNext(t -> assertThat(t).containsExactly(1, 2, 3))
					.assertNext(t -> assertThat(t).containsExactly(4, 5, 6))
					.assertNext(t -> assertThat(t).containsExactly(7, 8))
					.verifyComplete();
	}

	Flux<List<Integer>> scenario_bufferWillSubdivideAnInputFluxGapTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
				   .delayElements(Duration.ofMillis(99))
				   .buffer(Duration.ofMillis(200), Duration.ofMillis(300));
	}

	@Test
	public void bufferWillSubdivideAnInputFluxGapTime() {
		StepVerifier.withVirtualTime(this::scenario_bufferWillSubdivideAnInputFluxGapTime)
					.thenAwait(Duration.ofSeconds(10))
					.assertNext(t -> assertThat(t).containsExactly(1, 2))
					.assertNext(t -> assertThat(t).containsExactly(4, 5))
					.assertNext(t -> assertThat(t).containsExactly(7, 8))
					.verifyComplete();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> source = Flux.just(1);
		FluxBufferWhen<Integer, ?, ?, List<Integer>> test = new FluxBufferWhen<>(source,
				Flux.interval(Duration.ZERO, Duration.ofMillis(200)),
				open -> Mono.delay(Duration.ofMillis(100)),
				ArrayList::new,
				Queues.unbounded(Queues.XS_BUFFER_SIZE));

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanStartEndMain() {
		CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {
		}, null, null);

		BufferWhenMainSubscriber<String, Integer, Long, List<String>> test = new BufferWhenMainSubscriber<String, Integer, Long, List<String>>(actual, ArrayList::new, Queues.small(), Mono.just(1), u -> Mono.just(1L));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);
		test.request(100L);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0); //TODO
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(100L);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanStartEndMainCancelled() {
		CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {
		}, null, null);

		BufferWhenMainSubscriber<String, Integer, Long, List<String>> test = new BufferWhenMainSubscriber<String, Integer, Long, List<String>>(actual, ArrayList::new, Queues.small(), Mono.just(1), u -> Mono.just(1L));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanStartEndMainCompleted() {
		CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {
		}, null, null);

		BufferWhenMainSubscriber<String, Integer, Long, List<String>> test = new BufferWhenMainSubscriber<String, Integer, Long, List<String>>(actual, ArrayList::new, Queues.small(), Mono.just(1), u -> Mono.just(1L));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();

		test.onComplete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}


	@Test
	public void scanWhenCloseSubscriber() {
		CoreSubscriber<Object> actual = new LambdaSubscriber<>(null, null, null, null);

		BufferWhenMainSubscriber<String, Integer, Long, List<String>> main = new BufferWhenMainSubscriber<>(actual, ArrayList::new, Queues.small(), Mono.just(1), u -> Mono.just(1L));

		FluxBufferWhen.BufferWhenCloseSubscriber test = new FluxBufferWhen.BufferWhenCloseSubscriber<>(main, 5);

		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(Long.MAX_VALUE);

		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.dispose();
		assertThat(test.isDisposed()).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanWhenOpenSubscriber() {
		CoreSubscriber<Object> actual = new LambdaSubscriber<>(null, null, null, null);

		BufferWhenMainSubscriber<String, Integer, Long, List<String>> main = new BufferWhenMainSubscriber<>(actual, ArrayList::new, Queues.small(), Mono.just(1), u -> Mono.just(1L));

		FluxBufferWhen.BufferWhenOpenSubscriber test = new FluxBufferWhen.BufferWhenOpenSubscriber<>(main);


		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(Long.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.dispose();
		assertThat(test.isDisposed()).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void openCloseDisposedOnComplete() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> open = TestPublisher.create();
		TestPublisher<Integer> close = TestPublisher.create();

		StepVerifier.create(source.flux()
								  .bufferWhen(open, o -> close))
					.then(() -> {
						source.assertSubscribers();
						open.assertSubscribers();
						close.assertNoSubscribers();
						open.next(1);
					})
					.then(() -> {
						open.assertSubscribers();
						close.assertSubscribers();
						source.complete();
					})
					.expectNextMatches(List::isEmpty)
					.verifyComplete();

		open.assertNoSubscribers();
		close.assertNoSubscribers();
	}

	@Test
	public void openCloseMainError() {
		StepVerifier.create(Flux.error(new IllegalStateException("boom"))
								.bufferWhen(Flux.never(), a -> Flux.never()))
					.verifyErrorMessage("boom");
	}

	@Test
	public void openCloseBadSource() {
		TestPublisher<Object> badSource = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		StepVerifier.create(badSource.flux()
									 .bufferWhen(Flux.never(), a -> Flux.never()))
					.then(() -> {
						badSource.error(new IOException("ioboom"));
						badSource.complete();
						badSource.next(1);
						badSource.error(new IllegalStateException("boom"));
					})
					.expectErrorMessage("ioboom")
					.verifyThenAssertThat()
					.hasNotDroppedElements()
					.hasDroppedErrorWithMessage("boom");
	}

	@Test
	public void openCloseOpenCompletes() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> open = TestPublisher.create();
		TestPublisher<Integer> close = TestPublisher.create();

		StepVerifier.create(source.flux()
								  .bufferWhen(open, o -> close))
					.then(() -> {
						open.next(1);
						close.assertSubscribers();
					})
					.then(() -> {
						open.complete();
						source.assertSubscribers();
						close.assertSubscribers();
					})
					.then(() -> {
						close.complete();
						source.assertNoSubscribers();
					})
					.expectNextMatches(List::isEmpty)
					.verifyComplete();
	}

	@Test
	public void openCloseOpenCompletesNoBuffers() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> open = TestPublisher.create();
		TestPublisher<Integer> close = TestPublisher.create();

		StepVerifier.create(source.flux()
								  .bufferWhen(open, o -> close))
					.then(() -> {
						open.next(1);
						close.assertSubscribers();
					})
					.then(() -> {
						close.complete();
						source.assertSubscribers();
						open.assertSubscribers();
					})
					.then(() -> {
						open.complete();
						source.assertNoSubscribers();
					})
					.expectNextMatches(List::isEmpty)
					.verifyComplete();
	}

	@Test
	public void openCloseTake() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> open = TestPublisher.create();
		TestPublisher<Integer> close = TestPublisher.create();

		StepVerifier.create(source.flux()
								  .bufferWhen(open, o -> close)
								  .take(1), 2)
					.then(() -> {
						open.next(1);
						close.complete();
					})
					.then(() -> {
						source.assertNoSubscribers();
						open.assertNoSubscribers();
						close.assertNoSubscribers();
					})
					.expectNextMatches(List::isEmpty)
					.verifyComplete();
	}

	@Test
	public void openCloseLimit() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> open = TestPublisher.create();
		TestPublisher<Integer> close = TestPublisher.create();

		StepVerifier.create(source.flux()
								  .bufferWhen(open, o -> close)
								  .limitRequest(1))
					.then(() -> {
						open.next(1);
						close.complete();
					})
					.then(() -> {
						source.assertNoSubscribers();
						open.assertNoSubscribers();
						close.assertNoSubscribers();
					})
					.expectNextMatches(List::isEmpty)
					.verifyComplete();
	}

	@Test
	public void openCloseEmptyBackpressure() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> open = TestPublisher.create();
		TestPublisher<Integer> close = TestPublisher.create();

		StepVerifier.create(source.flux()
								  .bufferWhen(open, o -> close), 0)
					.then(() -> {
						source.complete();
						open.assertNoSubscribers();
						close.assertNoSubscribers();
					})
					.verifyComplete();
		//		ts.assertResult();
	}

	@Test
	public void openCloseErrorBackpressure() {
		TestPublisher<Integer> source = TestPublisher.create();
		TestPublisher<Integer> open = TestPublisher.create();
		TestPublisher<Integer> close = TestPublisher.create();

		StepVerifier.create(source.flux()
								  .bufferWhen(open, o -> close), 0)
					.then(() -> {
						source.error(new IllegalStateException("boom"));

						open.assertNoSubscribers();
						close.assertNoSubscribers();
					})
					.verifyErrorMessage("boom");
	}

	@Test
	public void openCloseBadOpen() {
		TestPublisher<Object> badOpen = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(Flux.never()
								.bufferWhen(badOpen, o -> Flux.never()))
					.then(() -> {
						badOpen.error(new IOException("ioboom"));
						badOpen.complete();
						badOpen.next(1);
						badOpen.error(new IllegalStateException("boom"));
					})

					.expectErrorMessage("ioboom")
					.verifyThenAssertThat()
					.hasNotDroppedElements()
					.hasDroppedErrorWithMessage("boom");
	}

	@Test
	public void openCloseBadClose() {
		TestPublisher<Object> badClose = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(Flux.never()
								.bufferWhen(Flux.just(1)
												.concatWith(Flux.never()), o -> badClose))
					.then(() -> {
						badClose.error(new IOException("ioboom"));
						badClose.complete();
						badClose.next(1);
						badClose.error(new IllegalStateException("boom"));
					})
					.expectErrorMessage("ioboom")
					.verifyThenAssertThat()
					.hasNotDroppedElements()
					.hasDroppedErrorWithMessage("boom");
	}

	@Test
	public void immediateOpen() {
		StepVerifier.create(Flux.just(1, 2, 3)
								.bufferWhen(Mono.just("OPEN"), u -> Mono.delay(Duration.ofMillis(100)))
								.flatMapIterable(Function.identity()))
					.expectNext(1, 2, 3)
					.verifyComplete();
	}

	@Test
	@Timeout(5)
	public void cancelWinsOverDrain() {
		Queue<List<Integer>> queue = Queues.<List<Integer>>small().get();
		queue.offer(Arrays.asList(1, 2, 3));

		AssertSubscriber<List<Integer>> actual = AssertSubscriber.create();

		BufferWhenMainSubscriber<Integer, String, Void, List<Integer>> main = new BufferWhenMainSubscriber<>(actual, ArrayList::new, () -> queue, Mono.just("open"), i -> Mono.never());
		main.onSubscribe(Operators.emptySubscription());

		RaceTestUtils.race(main, m -> {
			m.cancel();
			m.drain();
			return m;
		}, m -> m.cancelled, (m1, m2) -> /* ignored */ true);

		assertThat(main.cancelled).as("cancelled")
								  .isTrue();
		//TODO windows went as far up as 3, verify if that is indeed concurrent cancels
		assertThat(main.windows).as("windows")
								.isLessThan(4);
		assertThat(queue).as("queue was cleared")
						 .isEmpty();

		//we also check no values were drained to the actual
		assertThat(actual.values()).as("no buffer should be drained")
								   .isEmpty();
	}

	@Test
	public void discardOnCancel() {
		StepVerifier.create(Flux.just(1, 2, 3)
								.concatWith(Mono.never())
								.bufferWhen(Flux.just(1), u -> Mono.never()))
					.thenAwait(Duration.ofMillis(100))
					.thenCancel()
					.verifyThenAssertThat()
					.hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardOnCancelPostQueueing() {
		List<Object> discarded = new ArrayList<>();

		CoreSubscriber<List<Integer>> actual = new AssertSubscriber<>(Context.of(Hooks.KEY_ON_DISCARD, (Consumer<?>) discarded::add));
		FluxBufferWhen.BufferWhenMainSubscriber<Integer, Integer, Integer, List<Integer>> operator = new FluxBufferWhen.BufferWhenMainSubscriber<Integer, Integer, Integer, List<Integer>>(actual, ArrayList::new, Queues.small(), Flux.just(1), i -> Flux.never());
		operator.onSubscribe(new Operators.EmptySubscription());

		operator.buffers.put(0L, Arrays.asList(4, 5));
		operator.queue.offer(Arrays.asList(1, 2, 3));
		operator.cancel();

		assertThat(discarded).containsExactly(1, 2, 3, 4, 5);
	}

	@Test
	public void discardOnNextWhenNoBuffers() {
		StepVerifier.create(Flux.just(1, 2, 3)
								//buffer don't open in time
								.bufferWhen(Mono.delay(Duration.ofSeconds(2)), u -> Mono.never()))
					.expectComplete()
					.verifyThenAssertThat()
					.hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardOnError() {
		StepVerifier.create(Flux.just(1, 2, 3)
								.concatWith(Mono.error(new IllegalStateException("boom")))
								.bufferWhen(Mono.delay(Duration.ofSeconds(2)), u -> Mono.never()))
					.expectErrorMessage("boom")
					.verifyThenAssertThat()
					.hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardOnDrainCancelled() {
		List<Object> discarded = new ArrayList<>();

		CoreSubscriber<List<Integer>> actual = new AssertSubscriber<>(Context.of(Hooks.KEY_ON_DISCARD, (Consumer<?>) discarded::add));
		FluxBufferWhen.BufferWhenMainSubscriber<Integer, Integer, Integer, List<Integer>> operator = new FluxBufferWhen.BufferWhenMainSubscriber<Integer, Integer, Integer, List<Integer>>(actual, ArrayList::new, Queues.small(), Flux.just(1), i -> Flux.never());
		operator.onSubscribe(new Operators.EmptySubscription());
		operator.request(1);

		operator.buffers.put(0L, Arrays.asList(4, 5));
		operator.queue.offer(Arrays.asList(1, 2, 3));
		operator.cancelled = true;
		operator.drain();

		//drain only deals with queue, other method calling drain should deal with the open buffers (notably cancel)
		assertThat(discarded).containsExactly(1, 2, 3);
	}

	@Test
	public void discardOnDrainDoneWithErrors() {
		List<Object> discarded = new ArrayList<>();

		CoreSubscriber<List<Integer>> actual = new AssertSubscriber<>(Context.of(Hooks.KEY_ON_DISCARD, (Consumer<?>) discarded::add));
		FluxBufferWhen.BufferWhenMainSubscriber<Integer, Integer, Integer, List<Integer>> operator = new FluxBufferWhen.BufferWhenMainSubscriber<Integer, Integer, Integer, List<Integer>>(actual, ArrayList::new, Queues.small(), Flux.just(1), i -> Flux.never());
		operator.onSubscribe(new Operators.EmptySubscription());
		operator.request(1);

		operator.buffers.put(0L, Arrays.asList(4, 5));
		operator.queue.offer(Arrays.asList(1, 2, 3));
		operator.onError(new IllegalStateException("boom")); //triggers the drain

		assertThat(discarded).containsExactly(1, 2, 3, 4, 5);
	}

	@Test
	public void discardOnDrainEmittedAllCancelled() {
		List<Object> discarded = new ArrayList<>();

		CoreSubscriber<List<Integer>> actual = new AssertSubscriber<>(Context.of(Hooks.KEY_ON_DISCARD, (Consumer<?>) discarded::add));
		FluxBufferWhen.BufferWhenMainSubscriber<Integer, Integer, Integer, List<Integer>> operator = new FluxBufferWhen.BufferWhenMainSubscriber<Integer, Integer, Integer, List<Integer>>(actual, ArrayList::new, Queues.small(), Flux.just(1), i -> Flux.never());
		operator.onSubscribe(new Operators.EmptySubscription());

		operator.buffers.put(0L, Arrays.asList(4, 5));
		operator.queue.offer(Arrays.asList(1, 2, 3));
		operator.cancelled = true;
		operator.drain();

		//drain only deals with queue, other method calling drain should deal with the open buffers (notably cancel)
		assertThat(discarded).containsExactly(1, 2, 3);
	}

	@Test
	public void discardOnDrainEmittedAllWithErrors() {
		List<Object> discarded = new ArrayList<>();

		CoreSubscriber<List<Integer>> actual = new AssertSubscriber<>(Context.of(Hooks.KEY_ON_DISCARD, (Consumer<?>) discarded::add));
		FluxBufferWhen.BufferWhenMainSubscriber<Integer, Integer, Integer, List<Integer>> operator = new FluxBufferWhen.BufferWhenMainSubscriber<Integer, Integer, Integer, List<Integer>>(actual, ArrayList::new, Queues.small(), Flux.just(1), i -> Flux.never());
		operator.onSubscribe(new Operators.EmptySubscription());

		operator.buffers.put(0L, Arrays.asList(4, 5));
		operator.queue.offer(Arrays.asList(1, 2, 3));
		operator.onError(new IllegalStateException("boom"));

		assertThat(discarded).containsExactly(1, 2, 3, 4, 5);
	}

	@Test
	public void discardOnOpenError() {
		StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ZERO, Duration.ofMillis(100)) // 0, 1, 2
											   .map(Long::intValue)
											   .take(3)
											   .bufferWhen(Flux.interval(Duration.ZERO, Duration.ofMillis(100)), u -> (u == 2) ? null : Mono.never()))
					.thenAwait(Duration.ofSeconds(2))
					.expectErrorMessage("The bufferClose returned a null Publisher")
					.verifyThenAssertThat()
					.hasDiscardedExactly(0, 1, 1);
	}

	@Test
	public void discardOnBoundaryError() {
		StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ZERO, Duration.ofMillis(100)) // 0, 1, 2
											   .map(Long::intValue)
											   .take(3)
											   .bufferWhen(Flux.interval(Duration.ZERO, Duration.ofMillis(100)), u -> (u == 2) ? Mono.error(new IllegalStateException("boom")) : Mono.never()))
					.thenAwait(Duration.ofSeconds(2))
					.expectErrorMessage("boom")
					.verifyThenAssertThat()
					.hasDiscardedExactly(0, 1, 1);

	}
}
