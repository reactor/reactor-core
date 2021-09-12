/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxPrefetchTest {

	private static final int sourceSize = 1000;

	private static final Object[] nonFuseableSource   =
			new Object[]{Flux.range(1, sourceSize).hide(), Fuseable.NONE};
	private static final Object[] syncFuseableSource  =
			new Object[]{Flux.range(1, sourceSize), Fuseable.SYNC};
	private static final Object[] asyncFuseableSource =
			new Object[]{Flux.range(1, sourceSize).onBackpressureBuffer(),
					Fuseable.ASYNC};

	private static Stream<Integer> requestedModes() {
		return Stream.of(Fuseable.NONE, Fuseable.ASYNC, Fuseable.ANY);
	}

	private static Stream<Object[]> requestedModesWithPrefetchModes() {
		return requestedModes().flatMap(requestMode -> Stream.of(true, false)
		                                                     .map(prefetchMode -> new Object[]{
				                                                     requestMode,
				                                                     prefetchMode}));
	}

	private static Stream<Object[]> sources() {
		return Stream.of(nonFuseableSource, syncFuseableSource, asyncFuseableSource);
	}

	@Test
	public void failPrefetch() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Flux.range(1, 10)
			    .prefetch(-1);
		});
	}

	// Fusions Tests
	@DisplayName("Prefetch value from Non-Fused upstream")
	@ParameterizedTest(name = "Prefetch value from Non-Fused upstream with downstream with fusion={0} and Prefetch Mode={1}")
	@MethodSource("requestedModesWithPrefetchModes")
	public void prefetchValuesFromNonFuseableUpstream(int requestedMode,
			boolean prefetchMode) {
		Flux<Integer> nonFuseableSource = Flux.range(1, sourceSize)
		                                      .hide();

		StepVerifier.FirstStep<Integer> firstStep =
				StepVerifier.create(nonFuseableSource.prefetch(prefetchMode));

		StepVerifier.Step<Integer> step;
		if (requestedMode != Fuseable.NONE) {
			step = firstStep.expectFusion(requestedMode, Fuseable.ASYNC);
		}
		else {
			step = firstStep.expectSubscription();
		}

		step.thenAwait()
		    .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(
				    Fuseable.NONE))

		    .expectNextCount(sourceSize)
		    .verifyComplete();
	}

	@DisplayName("Prefetch Value from Async-Fused upstream")
	@ParameterizedTest(name = "Prefetch value from Async-Fused upstream with downstream with fusion={0} and Prefetch Mode={1}")
	@MethodSource("requestedModesWithPrefetchModes")
	public void prefetchValuesFromAsyncFuseableUpstream(int requestedMode,
			boolean prefetchMode) {
		Flux<Integer> asyncFuseableSource = Flux.range(1, sourceSize)
		                                        .onBackpressureBuffer();

		StepVerifier.FirstStep<Integer> firstStep =
				StepVerifier.create(asyncFuseableSource.prefetch(prefetchMode));

		StepVerifier.Step<Integer> step;
		if (requestedMode != Fuseable.NONE) {
			step = firstStep.expectFusion(requestedMode, Fuseable.ASYNC);
		}
		else {
			step = firstStep.expectSubscription();
		}

		step.thenAwait()
		    .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(
				    Fuseable.ASYNC))

		    .expectNextCount(sourceSize)
		    .verifyComplete();
	}

	@DisplayName("Prefetch Value from Sync-Fused upstream")
	@ParameterizedTest(name = "Prefetch Value from Sync-Fused upstream with downstream with fusion={0} and Prefetch Mode={1}")
	@MethodSource("requestedModesWithPrefetchModes")
	public void prefetchValuesFromSyncFuseableUpstream(int requestedMode,
			boolean prefetchMode) {
		Flux<Integer> syncFuseableSource = Flux.range(1, sourceSize);

		StepVerifier.FirstStep<Integer> firstStep =
				StepVerifier.create(syncFuseableSource.prefetch(prefetchMode));

		StepVerifier.Step<Integer> step;
		if (requestedMode != Fuseable.NONE) {
			step = firstStep.expectFusion(requestedMode,
					                (requestedMode & Fuseable.SYNC) != 0 ? Fuseable.SYNC : Fuseable.ASYNC)

			                .thenAwait()
			                .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(
					                (requestedMode & Fuseable.SYNC) != 0 ? Fuseable.SYNC :
							                Fuseable.NONE));
		}
		else {
			step = firstStep.expectSubscription()
			                .thenAwait()
			                .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(
					                Fuseable.SYNC));
		}

		step.expectNextCount(sourceSize)
		    .verifyComplete();
	}

	// Backpressure Tests
	@DisplayName("Check backpressure from different sources")
	@ParameterizedTest(name = "Prefetch value from {0}")
	@MethodSource("sources")
	public void backpressureFromDifferentSourcesTypes(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		source.prefetch()
		      .subscribe(ts);

		ts.assertNoEvents();

		int step = 250;
		int count = 0;
		while (count < sourceSize) {
			ts.request(step);
			count += step;
			ts.assertValueCount(count);
		}

		ts.assertComplete();
	}

	// Prefetch mode Tests
	@ParameterizedTest
	@MethodSource("requestedModes")
	public void immediatePrefetchInEagerPrefetchMode(int requestedMode) {
		int prefetch = 256;
		ArrayList<Long> requests = new ArrayList<>();

		Flux<Integer> nonFuseableSource = Flux.range(1, sourceSize)
		                                      .hide()
		                                      .doOnRequest(requests::add);

		StepVerifier.FirstStep<Integer> firstStep =
				StepVerifier.create(nonFuseableSource.prefetch(prefetch, false), 0);

		StepVerifier.Step<Integer> step;
		if (requestedMode != Fuseable.NONE) {
			step = firstStep.expectFusion(requestedMode, requestedMode & Fuseable.ASYNC);
		}
		else {
			step = firstStep.expectSubscription();
		}

		step.then(() -> assertThat(requests).hasSize(1)
		                                    .containsExactly((long) prefetch))
		    .thenRequest(Long.MAX_VALUE)
		    .expectNextCount(sourceSize)
		    .verifyComplete();
	}

	@ParameterizedTest
	@MethodSource("requestedModes")
	public void delayedPrefetchInLazyPrefetchMode(int requestedMode) {
		LongAdder requestCount = new LongAdder();

		Flux<Integer> nonFuseableSource = Flux.range(1, sourceSize)
		                                      .hide()
		                                      .doOnRequest((r) -> requestCount.increment());

		StepVerifier.FirstStep<Integer> firstStep =
				StepVerifier.create(nonFuseableSource.prefetch(true), 0);

		StepVerifier.Step<Integer> step;
		if (requestedMode != Fuseable.NONE) {
			step = firstStep.expectFusion(requestedMode, requestedMode & Fuseable.ASYNC);
		}
		else {
			step = firstStep.expectSubscription();
		}

		step.then(() -> assertThat(requestCount.longValue()).isEqualTo(0))
		    .thenRequest(Long.MAX_VALUE)
		    .expectNextCount(sourceSize)
		    .verifyComplete();
	}

	// LimitRate Tests
	@Test
	public void limitRate() {
		List<Long> upstreamRequests = new LinkedList<>();
		List<Long> downstreamRequests = new LinkedList<>();
		Flux<Integer> source = Flux.range(1, 400)
		                           .doOnRequest(upstreamRequests::add)
		                           .doOnRequest(r -> System.out.println(
				                           "upstream request of " + r))
		                           .hide()
		                           .limitRate(40)
		                           .doOnRequest(downstreamRequests::add)
		                           .doOnRequest(r -> System.out.println(
				                           "downstream request of " + r));

		AssertSubscriber<Integer> ts = AssertSubscriber.create(400);
		source.subscribe(ts);
		ts.await(Duration.ofMillis(100))
		  .assertComplete();

		assertThat(downstreamRequests.size()).as("downstream number of requests").isOne();
		assertThat(downstreamRequests.get(0)).as("unique request").isEqualTo(400L);
		long total = 0L;
		for (Long requested : upstreamRequests) {
			total += requested;
			//30 is the optimization that eagerly prefetches when 3/4 of the request has been served
			assertThat(requested).as("rate limit check").isIn(40L, 30L);
		}
		assertThat(total).as("upstream total request")
		                 .isGreaterThanOrEqualTo(400L)
		                 .isLessThan(440L);
	}

	@Test
	public void limitRateWithCloseLowTide() {
		List<Long> rebatchedRequest = Collections.synchronizedList(new ArrayList<>());

		final Flux<Integer> test = Flux
				.range(1, 14)
				.hide()
				.doOnRequest(rebatchedRequest::add)
				.limitRate(10,8);

		StepVerifier.create(test, 14)
		            .expectNextCount(14)
		            .verifyComplete();

		assertThat(rebatchedRequest)
				.containsExactly(10L, 8L);
	}

	@Test
	public void limitRateWithVeryLowTide() {
		List<Long> rebatchedRequest = Collections.synchronizedList(new ArrayList<>());

		final Flux<Integer> test = Flux
				.range(1, 14)
				.hide()
				.doOnRequest(rebatchedRequest::add)
				.limitRate(10,2);

		StepVerifier.create(test, 14)
		            .expectNextCount(14)
		            .verifyComplete();

		assertThat(rebatchedRequest)
				.containsExactly(10L, 2L, 2L, 2L, 2L, 2L, 2L, 2L);
	}

	@Test
	public void limitRateDisabledLowTide() throws InterruptedException {
		LongAdder request = new LongAdder();
		CountDownLatch latch = new CountDownLatch(1);

		Flux.range(1, 99).hide()
		    .doOnRequest(request::add)
		    .limitRate(10, 0)
		    .subscribe(new BaseSubscriber<Integer>() {
			    @Override
			    protected void hookOnSubscribe(Subscription subscription) {
				    request(100);
			    }

			    @Override
			    protected void hookFinally(SignalType type) {
				    latch.countDown();
			    }
		    });

		assertThat(latch.await(1, TimeUnit.SECONDS)).as("took less than 1s").isTrue();
		assertThat(request.sum()).as("request not compounded").isEqualTo(100L);
	}
}
