/*
 * Copyright (c) 2015-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxReplayTest extends FluxOperatorTest<String, String> {

	VirtualTimeScheduler vts;

	@BeforeEach
	public void vtsStart() {
		//delayElements (notably) now uses parallel() so VTS must be enabled everywhere
		vts = VirtualTimeScheduler.getOrSet();
	}

	@AfterEach
	public void vtsStop() {
		VirtualTimeScheduler.reset();
	}

	static final Logger log = Loggers.getLogger(FluxReplayTest.class);

	@Test
	void checkTimeCacheResubscribesAndCompletesAfterRepetitions() {
		VirtualTimeScheduler.reset();
		Flux<Integer> flow = getSource2()
				.doOnSubscribe(__ -> log.info("Loading source..."))
				.cache(Duration.ofMillis(200))
				.doOnSubscribe(__ -> log.info("Pooling cycle starting..."))
				.flatMap(this::process, 2)
				.repeat(1000);

		StepVerifier.create(flow)
		            .expectNextCount(1001 * 5)
		            .verifyComplete();
	}

	private Flux<Integer> getSource2() {
		return Flux.just(1, 2, 3, 4, 5);
	}

	private Mono<Integer> process(int channel) {
		return Mono.just(channel)
		           .doOnNext(rec -> log.info("Processing: {}", rec))
		           .delayElement(Duration.ofMillis(5));
	}

	@Test
	void checkTimeCacheResubscribesAndCompletesAfterRepetitions2() {
		//vtsStop(); //only important because I added the test in FluxReplayTest.java
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		AtomicInteger sourceLoad = new AtomicInteger();
		AtomicInteger pollStart = new AtomicInteger();
		final AtomicInteger pollEnd = new AtomicInteger();

		Flux<String> flow = getSource()
				.doOnSubscribe(__ -> log.info("Loading source #" + sourceLoad.incrementAndGet()))
				.cache(Duration.ofSeconds(1), vts)
				.flatMap(v -> {
					if (v == 1) {
						pollStart.incrementAndGet();
					}
					else if (v == 5) { //this assume a 5 element source
						pollEnd.incrementAndGet();
					}
					return process(pollStart.get() + "_" + v, vts);
				}, 2)
				.repeat(3);

		StepVerifier.create(flow)
		            .expectNextCount(4 * 5)
		            .expectComplete()
		            .verify(Duration.ofSeconds(300));
	}

	private Flux<Integer> getSource() {
		return Flux.just(1, 2, 3, 4, 5)
		           .doOnRequest(r -> log.info("source.request({})", r == Long.MAX_VALUE ? "unbounded" : r))
		           .hide();
	}

	private Mono<String> process(String channel, VirtualTimeScheduler timeScheduler) {
		if (channel.equals("2_4")) {
			timeScheduler.advanceTimeBy(Duration.ofMillis(1001));
		}
		return Mono.fromCallable(() -> {
			log.info("Processing: {}", channel);
			return channel;
		});
	}

	// === overrides to configure the abstract test ===

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.prefetch(Integer.MAX_VALUE)
				.shouldAssertPostTerminateState(false);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.replay().autoConnect()),

				scenario(f -> f.replay().refCount())
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_touchAndAssertState() {
		return Arrays.asList(
				scenario(f -> f.replay().autoConnect())
		);
	}

	// === start of tests ===

	@Test
	public void failPrefetch() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Flux.never()
					.replay(-1);
		});
	}

	@Test
	public void failTime() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Flux.never()
					.replay(Duration.ofDays(-1));
		});
	}

	@Test
	public void cacheFlux() {

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay()
		                                         .hide()
		                                         .autoConnect()
		                                         .hide()
		                                         .elapsed();

		StepVerifier.create(source)
		            .expectNoFusionSupport()
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxFused() {

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay()
		                                         .autoConnect()
		                                         .elapsed();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxTTL() {
		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay(Duration.ofMillis(2000))
		                                         .hide()
		                                         .autoConnect()
		                                         .hide()
		                                         .elapsed();

		StepVerifier.create(source)
		            .expectNoFusionSupport()
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxTTLFused() {

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay(Duration.ofMillis(2000))
		                                         .autoConnect()
		                                         .elapsed();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxTTLMillis() {
		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay(Duration.ofMillis(2000), vts)
		                                         .hide()
		                                         .autoConnect()
		                                         .hide()
		                                         .elapsed();

		StepVerifier.create(source)
		            .expectNoFusionSupport()
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxTTLNanos() {
		Flux<Integer> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofNanos(1000), vts)
		                                         .replay(Duration.ofNanos(2000), vts)
		                                         .hide()
		                                         .autoConnect()
												 .hide();

		StepVerifier.create(source)
		            .expectNoFusionSupport()
		            .then(() -> vts.advanceTimeBy(Duration.ofNanos(3000)))
		            .expectNext(1)
		            .expectNext(2)
		            .expectNext(3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofNanos(3000)))
		            .expectNext(2)
		            .expectNext(3)
		            .verifyComplete();
	}

	@Test
	public void cacheFluxHistoryTTL() {

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay(2, Duration.ofMillis(2000))
		                                         .hide()
		                                         .autoConnect()
		                                         .hide()
		                                         .elapsed();

		StepVerifier.create(source)
		            .expectNoFusionSupport()
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxHistoryTTLFused() {
		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay(2, Duration.ofMillis(2000))
		                                         .autoConnect()
		                                         .elapsed();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();
	}

	@Test
	public void minimalInitialRequestIsHistory() {
		List<Long> requests = new ArrayList<>();
		TwoRequestsSubscriber threeThenEightSubscriber = new TwoRequestsSubscriber(3, 8);

		ConnectableFlux<Integer> replay =
				Flux.range(1, 5)
				    .doOnRequest(requests::add)
				    .replay(5);

		assertThat(requests).isEmpty();

		replay.subscribe(threeThenEightSubscriber);
		replay.connect();

		assertThat(requests).startsWith(5L);
	}

	@Test
	public void minimalInitialRequestIsMaxOfSubscribersInitialRequests() {
		List<Long> requests = new ArrayList<>();
		TwoRequestsSubscriber fiveThenEightSubscriber = new TwoRequestsSubscriber(5, 8);
		TwoRequestsSubscriber sevenThenEightSubscriber = new TwoRequestsSubscriber(7, 8);

		ConnectableFlux<Integer> replay =
				Flux.range(1, 5)
				    .doOnRequest(requests::add)
				    .replay(7);

		assertThat(requests).isEmpty();

		replay.subscribe(fiveThenEightSubscriber);
		replay.subscribe(sevenThenEightSubscriber);
		replay.connect();

		assertThat(requests).startsWith(7L).doesNotContain(5L);
	}

	@Test
	public void multipleEarlySubscribersPropagateTheirLateRequests() {
		List<Long> requests = new ArrayList<>();
		TwoRequestsSubscriber fiveThenEightSubscriber = new TwoRequestsSubscriber(5, 8);
		TwoRequestsSubscriber sevenThenEightSubscriber = new TwoRequestsSubscriber(7, 8);

		ConnectableFlux<Integer> replay =
				Flux.range(1, 100)
				    .doOnRequest(requests::add)
				    .replay(7);

		replay.take(13, false).subscribe(fiveThenEightSubscriber);
		replay.subscribe(sevenThenEightSubscriber);
		replay.connect();



		assertThat(requests).startsWith(7L).doesNotContain(5L);
	}

	@Test
	public void minimalInitialRequestWithUnboundedSubscriber() {
		List<Long> requests = new ArrayList<>();
		TwoRequestsSubscriber fiveThenEightSubscriber = new TwoRequestsSubscriber(5, 8);

		ConnectableFlux<Integer> replay =
				Flux.range(1, 11)
				    .doOnRequest(requests::add)
				    .replay(8);

		assertThat(requests).isEmpty();

		replay.subscribe(fiveThenEightSubscriber);
		replay.subscribe(); //unbounded
		replay.connect();

		assertThat(requests).containsExactly(8L, 6L);
	}

	@Test
	public void minimalInitialRequestUnboundedWithFused() {
		List<Long> requests = new ArrayList<>();
		TwoRequestsSubscriber fiveThenEightSubscriber = new TwoRequestsSubscriber(5, 8);

		ConnectableFlux<Integer> replay =
				Flux.range(1, 11)
				    .doOnRequest(requests::add)
				    .replay(8);

		assertThat(requests).isEmpty();

		replay.subscribe(fiveThenEightSubscriber);
		replay.subscribe(); //unbounded
		replay.connect();

		assertThat(requests).containsExactly(8L, 6L);
	}

	@Test
	public void onlyInitialRequestWithLateUnboundedSubscriber() {
		List<Long> requests = new ArrayList<>();
		TwoRequestsSubscriber fiveThenEightSubscriber = new TwoRequestsSubscriber(5, 8);

		ConnectableFlux<Integer> replay =
				Flux.range(1, 11)
				    .doOnRequest(requests::add)
				    .replay(8);

		assertThat(requests).isEmpty();

		replay.subscribe(fiveThenEightSubscriber);
		replay.connect();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		replay.subscribe(ts); //unbounded

		assertThat(requests).containsExactly(8L, 6L);
		ts.assertValueCount(8); //despite unbounded, as it was late it only sees the replay capacity
	}

	@Test
	public void cancel() {
		ConnectableFlux<Integer> replay = Sinks.many().unicast().<Integer>onBackpressureBuffer().asFlux().replay(2);

		replay.subscribe(v -> {}, e -> { throw Exceptions.propagate(e); });

		Disposable connected = replay.connect();

		//the lambda subscriber itself is cancelled so it will bubble the exception
		//propagated by connect().dispose()
		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(connected::dispose)
	            .withMessage("Disconnected");

		boolean cancelled = (((FluxReplay.ReplaySubscriber) connected).state & FluxReplay.ReplaySubscriber.DISPOSED_FLAG) == FluxReplay.ReplaySubscriber.DISPOSED_FLAG;
		assertThat(cancelled).isTrue();
	}

	@Test
    public void scanMain() {
        Flux<Integer> parent = Flux.just(1).map(i -> i);
        FluxReplay<Integer> test = new FluxReplay<>(parent, 25, 1000, Schedulers.single());

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(25);
        assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.single());
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

	@Test
    public void scanInner() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxReplay<Integer> main = new FluxReplay<>(Flux.just(1), 2, 1000, Schedulers.single());
        FluxReplay.ReplaySubscriber<Integer> parent =
		        new FluxReplay.ReplaySubscriber<>(new FluxReplay.UnboundedReplayBuffer<>(10), main, 10);
        FluxReplay.ReplayInner<Integer> test = new FluxReplay.ReplayInner<>(actual, parent);
        parent.add(test);
        parent.buffer.replay(test);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0); // RS: TODO non-zero size
		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.single());

		test.request(35);
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.parent.terminate();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanSubscriber() {
		FluxReplay<Integer> parent = new FluxReplay<>(Flux.just(1), 2, 1000, Schedulers.single());
        FluxReplay.ReplaySubscriber<Integer> test =
		        new FluxReplay.ReplaySubscriber<>(new FluxReplay.UnboundedReplayBuffer<>(10), parent, 10);
        Subscription sub = Operators.emptySubscription();
        test.onSubscribe(sub);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
        assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(Integer.MAX_VALUE);
        test.buffer.add(1);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
        test.terminate();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.state = test.state | FluxReplay.ReplaySubscriber.DISPOSED_FLAG;
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
	@Timeout(5)
	public void cacheSingleSubscriberWithMultipleRequestsDoesntHang() {
		List<Integer> listFromStream = Flux
				.range(0, 1000)
				.cache(5)
				.toStream(10)
				.collect(Collectors.toList());

		assertThat(listFromStream).hasSize(1000);
	}

	@Test
	public void cacheNotOverrunByMaxPrefetch() {
		Flux<Integer> s = Flux.range(1, 30)
		                      .cache(5);

		StepVerifier.create(s, 10)
		    .expectNextCount(10)
		    .thenRequest(20)
		    .expectNextCount(20)
		    .verifyComplete();

		StepVerifier.create(s)
		            .expectNextCount(5)
		            .verifyComplete();
	}

	@Test
	public void ifSubscribeBeforeConnectThenTrackFurtherRequests() {
		ConnectableFlux<Long> connectableFlux = Flux.just(1L, 2L, 3L, 4L)
		                                            .replay(2);

		StepVerifier.create(connectableFlux, 1)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .then(connectableFlux::connect)
		            .expectNext(1L)
		            .thenRequest(10)
		            .expectNext(2L, 3L, 4L)
		            .expectComplete()
		            .verify(Duration.ofSeconds(10));

		StepVerifier.create(connectableFlux, 1)
		            .expectNext(3L)
		            .thenRequest(10)
		            .expectNext(4L)
		            .expectComplete()
		            .verify(Duration.ofSeconds(10));
	}

	@Test
	public void ifNoSubscriptionBeforeConnectThenPrefetches() {
		Queue<Long> totalRequested = new ArrayBlockingQueue<>(10);
		ConnectableFlux<Integer> connectable = Flux.range(1, 24)
		                                           .doOnRequest(totalRequested::offer)
		                                           .replay(8);

		connectable.connect();

		StepVerifier.create(connectable)
		            .expectNext(17, 18, 19, 20, 21, 22, 23, 24)
		            .expectComplete()
		            .verify(Duration.ofSeconds(10));

		assertThat(totalRequested).containsExactly(8L, 6L, 6L, 6L, 6L);
	}

	private static final class TwoRequestsSubscriber extends BaseSubscriber<Integer> {

		final long firstRequest;
		final long secondRequest;

		private TwoRequestsSubscriber(long firstRequest, long secondRequest) {
			this.firstRequest = firstRequest;
			this.secondRequest = secondRequest;
		}

		@Override
		protected void hookOnSubscribe(Subscription subscription) {
			request(firstRequest);
		}

		@Override
		protected void hookOnNext(Integer value) {
			if (value.longValue() == firstRequest) {
				request(secondRequest);
			}
		}
	}

}
