/*
 * Copyright (c) 2016-2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Scannable.from;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxBufferTimeoutFairBackpressureTest {

	@AfterEach
	public void tearDown() {
		VirtualTimeScheduler.reset();
	}

	@Test
	@Tag("slow")
	void backpressureSupported() throws InterruptedException {
		final int eventProducerDelayMillis = 200;
		// Event Producer emits requested items to downstream with a 200ms delay
		Flux<String> eventProducer = Flux.<String>create(sink -> {
			sink.onRequest(request -> {
				if (request == Long.MAX_VALUE) {
					sink.error(new RuntimeException("No_Backpressure unsupported"));
				} else {
					System.out.println("Backpressure Request(" + request + ")");
					LongStream.range(0, request)
							.mapToObj(String::valueOf)
							.forEach(message -> {
								try {
									TimeUnit.MILLISECONDS.sleep(eventProducerDelayMillis);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								System.out.println("Producing: " + message);
								sink.next(message);
							});
				}
			});
		}).subscribeOn(Schedulers.boundedElastic());

		// Event Consumer buffers 5 items or up to 150ms from first item.
		// Because the producer emits every 200ms, we expect only 1 item per each buffer.
		// Every buffer is delayed for 500ms before delivering to the subscriber.
		// We expect all items to be delivered with proper backpressure which involves
		// time passing.

		Scheduler scheduler = Schedulers.newBoundedElastic(10, 10_000, "queued-tasks");
		Semaphore completed = new Semaphore(1);
		AtomicBoolean hasError = new AtomicBoolean(false);
		completed.acquire();

		final int bufferTimeoutMillis = 150;
		final int consumerDelayMillis = 500;

		Disposable subscription = eventProducer
				.bufferTimeout(5, Duration.ofMillis(bufferTimeoutMillis), true)
				.doOnRequest(r -> System.out.println("BUFFER requested " + r))
				.concatMap(b -> {
					System.out.println("CONCATMAP delivered " + b);
					return Flux.just(b)
					           .delayElements(Duration.ofMillis(consumerDelayMillis), scheduler);
					}, 1)
				.subscribe(
					buffer -> {
						System.out.println("Received buffer of size " + buffer.size());
						for (String message : buffer) {
							System.out.println("Consuming: " + message);
						}},
					error -> {
						System.err.println("Error: " + error);
						hasError.set(true);
						completed.release();
						},
					() -> {
						System.out.println("Completed.");
						completed.release();
					}
				);

		try {
			Assertions.assertFalse(completed.tryAcquire(1, TimeUnit.MINUTES), "Should " +
					"not finish before subscription cancelled.");
			Assertions.assertFalse(hasError.get(), "Should not have received an error.");
		} finally {
			subscription.dispose();
		}

		System.out.println("Completed test.");
	}

	@Test
	void downstreamNoReplenishButTimeout() {
		Sinks.Many<Integer> sink = Sinks.many()
		                               .unicast()
		                               .onBackpressureBuffer();
		StepVerifier.withVirtualTime(
				() -> sink.asFlux()
				          .bufferTimeout(10, Duration.ofMillis(100), true),
				1
		)
		            .expectSubscription()
		            .then(() -> sink.tryEmitNext(0))
		            .thenAwait(Duration.ofMillis(100))
		            .assertNext(l -> assertThat(l).hasSize(1))
		            .then(() -> sink.tryEmitNext(1))
		            .thenAwait(Duration.ofMillis(100))
					.expectNoEvent(Duration.ofMillis(300))
		            .thenCancel()
		            .verify();
	}

	Flux<List<Integer>> scenario_bufferWithTimeoutAccumulateOnSize() {
		return Flux.range(1, 6)
		           .delayElements(Duration.ofMillis(300))
		           .bufferTimeout(5, Duration.ofMillis(2000), true);
	}

	@Test
	public void bufferWithTimeoutAccumulateOnSize() {
		StepVerifier.withVirtualTime(this::scenario_bufferWithTimeoutAccumulateOnSize)
		            .thenAwait(Duration.ofMillis(1500))
		            .assertNext(s -> assertThat(s).containsExactly(1, 2, 3, 4, 5))
		            .thenAwait(Duration.ofMillis(2000))
		            .assertNext(s -> assertThat(s).containsExactly(6))
		            .verifyComplete();
	}

	Flux<List<Integer>> scenario_bufferWithTimeoutAccumulateOnTime() {
		return Flux.range(1, 6)
		           .delayElements(Duration.ofNanos(300)).log("delayed")
		           .bufferTimeout(15, Duration.ofNanos(1500), true).log("buffered");
	}

	@Test
	public void bufferWithTimeoutAccumulateOnTime() {
		StepVerifier.withVirtualTime(this::scenario_bufferWithTimeoutAccumulateOnTime)
		            .thenAwait(Duration.ofNanos(1800))
		            .assertNext(s -> assertThat(s).containsExactly(1, 2, 3, 4, 5))
		            .thenAwait(Duration.ofNanos(2000))
		            .assertNext(s -> assertThat(s).containsExactly(6))
		            .verifyComplete();
	}

	@Test
	void bufferWithTimeoutAvoidingNegativeRequests() {
		final List<Long> requestPattern = new CopyOnWriteArrayList<>();

		StepVerifier.withVirtualTime(() ->
						            Flux.range(1, 3)
						                .delayElements(Duration.ofMillis(100))
						                .doOnRequest(requestPattern::add)
						                .bufferTimeout(5, Duration.ofMillis(100), true),
				            0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenRequest(2)
		            .expectNoEvent(Duration.ofMillis(100))
		            .assertNext(s -> assertThat(s).containsExactly(1))
		            .expectNoEvent(Duration.ofMillis(100))
		            .assertNext(s -> assertThat(s).containsExactly(2))
		            .thenRequest(1) // This should not cause a negative upstream request
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenCancel()
		            .verify();

		assertThat(requestPattern).allSatisfy(r -> assertThat(r).isPositive());
	}

	@Test
	void processesLargeDataset() {
		TreeSet<Integer> set = new TreeSet<>();
		Flux.fromStream(IntStream.range(0, 1_000_000).boxed())
		    .hide()
		    .bufferTimeout(1000, Duration.ofSeconds(1), true)
		    .doOnNext(set::addAll)
		    .blockLast();

		assertThat(set.size()).isEqualTo(1_000_000);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);

		final Scheduler.Worker worker = Schedulers.boundedElastic()
		                                          .createWorker();
		FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<String, List<String>> test =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<String, List<String>>(
				actual, 123, 1000, TimeUnit.HOURS,
				worker, ArrayList::new, null);

		try {
			Subscription subscription = Operators.emptySubscription();
			test.onSubscribe(subscription);

			test.requested = 3L;
			for (int i = 0; i < 100; i++) {
				test.onNext(Integer.toString(i));
			}

			assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(worker);
			assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(subscription);
			assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

			assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(3L);
			assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(123 << 2);
			assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(100);
			assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);

			assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
			assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();

			test.onError(new IllegalStateException("boom"));
			assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
			assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		}
		finally {
			worker.dispose();
		}
	}
	@Test
	public void shouldShowActualSubscriberDemand() {
		Subscription[] subscriptionsHolder = new Subscription[1];
		CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(
				null, e -> {}, null, s -> subscriptionsHolder[0] = s
		);

		FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<String, List<String>> test =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<String, List<String>>(
						actual, 123, 1000, TimeUnit.MILLISECONDS,
						Schedulers.boundedElastic().createWorker(), ArrayList::new, null
				);

		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);
		subscriptionsHolder[0].request(10);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(10L);
		subscriptionsHolder[0].request(5);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(15L);
	}

	@Test
	public void downstreamDemandShouldBeAbleToDecreaseOnFullBuffer() {
		Subscription[] subscriptionsHolder = new Subscription[1];
		CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(
				null, e -> {}, null, s -> subscriptionsHolder[0] = s
		);

		FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<String, List<String>> test =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<String, List<String>>(
						actual, 5, 1000, TimeUnit.MILLISECONDS,
						Schedulers.boundedElastic().createWorker(), ArrayList::new, null
				);

		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);
		subscriptionsHolder[0].request(1);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(1L);

		for (int i = 0; i < 5; i++) {
			test.onNext(String.valueOf(i));
		}

		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);
	}

	@Test
	public void downstreamDemandShouldBeAbleToDecreaseOnTimeSpan() {
		Subscription[] subscriptionsHolder = new Subscription[1];
		CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(
				null, e -> {}, null, s -> subscriptionsHolder[0] = s
		);

		VirtualTimeScheduler timeScheduler = VirtualTimeScheduler.getOrSet();
		FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<String, List<String>> test =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<String, List<String>>(
						actual, 5, 100, TimeUnit.MILLISECONDS,
						timeScheduler.createWorker(), ArrayList::new, null
				);

		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);
		subscriptionsHolder[0].request(1);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(1L);
		timeScheduler.advanceTimeBy(Duration.ofMillis(100));
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(1L);
		test.onNext("0");
		timeScheduler.advanceTimeBy(Duration.ofMillis(100));
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);
	}

	@Test
	public void requestedFromUpstreamShouldNotExceedDownstreamDemand() {
		Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
		Flux<String> emitter = sink.asFlux();

		AtomicLong requestedOutstanding = new AtomicLong(0);

		VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

		Flux<List<String>> flux = emitter.doOnRequest(requestedOutstanding::addAndGet)
		                                 .bufferTimeout(
												 5, Duration.ofMillis(100), scheduler, true
		                                 )
		                                 .doOnNext(list ->
				                                 requestedOutstanding.addAndGet(-list.size())
		                                 );

		StepVerifier.withVirtualTime(() -> flux, () -> scheduler, 0)
		            .expectSubscription()
		            .then(() -> assertThat(requestedOutstanding).hasValue(0))
		            .thenRequest(2)
		            .then(() -> assertThat(requestedOutstanding.get()).isEqualTo(20))
		            // prefetch size is 5 << 2, so 5 * 4 = 20
		            .then(() -> sink.emitNext("a", FAIL_FAST))
		            .thenAwait(Duration.ofMillis(100))
		            .assertNext(s -> assertThat(s).containsExactly("a"))
		            .then(() -> assertThat(requestedOutstanding).hasValue(19))
		            .thenRequest(1)
		            .then(() -> assertThat(requestedOutstanding).hasValue(19))
		            .thenCancel()
		            .verify();
	}

	@Test
	public void scanSubscriberCancelled() {
		CoreSubscriber<List<String>>
				actual = new LambdaSubscriber<>(null, e -> {}, null, null);

		FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<String, List<String>> test =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<String, List<String>>(
						actual, 123, 1000, TimeUnit.MILLISECONDS,
						Schedulers.boundedElastic().createWorker(), ArrayList::new, null
				);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
	}

	@Test
	public void bufferTimeoutShouldNotRaceWithNext() {
		Set<Integer> seen = new HashSet<>();
		AtomicBoolean complete = new AtomicBoolean();
		Consumer<List<Integer>> consumer = integers -> {
			for (Integer i : integers) {
				if (!seen.add(i)) {
					throw new IllegalStateException("Duplicate! " + i);
				}
			}
		};
		CoreSubscriber<List<Integer>> actual = new LambdaSubscriber<>(
				consumer, null, () -> complete.set(true), null);

		FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<Integer, List<Integer>> test =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<Integer, List<Integer>>(
						actual, 3, 1000, TimeUnit.MILLISECONDS,
						Schedulers.boundedElastic().createWorker(), ArrayList::new, null
				);
		test.onSubscribe(Operators.emptySubscription());

		AtomicInteger counter = new AtomicInteger();
		for (int i = 0; i < 500; i++) {
			RaceTestUtils.race(
					() -> test.onNext(counter.getAndIncrement()),
					test::bufferTimedOut
			);
		}

		test.onComplete();

		assertThat(seen.size()).isEqualTo(500);
		assertThat(complete.get()).isTrue();
	}

	//see https://github.com/reactor/reactor-core/issues/1247
	@Test
	public void rejectedOnNextLeadsToOnError() {
		Scheduler scheduler = Schedulers.newSingle("rejectedOnNextLeadsToOnError");
		scheduler.dispose();

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .bufferTimeout(4, Duration.ofMillis(500), scheduler, true))
		            .assertNext(l -> assertThat(l).containsExactly(1))
		            .expectError(RejectedExecutionException.class)
		            .verify(Duration.ofSeconds(1));
	}

	@Test
	public void discardOnCancel() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatWith(Mono.never())
		                        .bufferTimeout(10, Duration.ofMillis(100), true))
		            .thenAwait(Duration.ofMillis(10))
		            .thenCancel()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardOnTimerRejected() {
		Scheduler scheduler = Schedulers.newSingle("discardOnTimerRejected");

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .doOnNext(n -> scheduler.dispose())
		                        .bufferTimeout(10, Duration.ofMillis(100), scheduler, true))
		            .assertNext(l -> assertThat(l).containsExactly(1))
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(RejectedExecutionException.class))
		            .verify();
	}

	@Test
	public void discardOnError() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatWith(Mono.error(new IllegalStateException("boom")))
		                        .bufferTimeout(10, Duration.ofMillis(100), true))
		            .assertNext(l -> assertThat(l).containsExactly(1, 2, 3))
		            .expectErrorMessage("boom")
		            .verify();
	}
}
