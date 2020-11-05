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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static reactor.core.Scannable.Attr.*;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;
import static reactor.core.scheduler.Schedulers.fromExecutor;
import static reactor.core.scheduler.Schedulers.fromExecutorService;

public class FluxPublishOnTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.prefetch(Queues.SMALL_BUFFER_SIZE)
		                     .fusionModeThreadBarrier(Fuseable.ASYNC)
		                     .fusionMode(Fuseable.ASYNC);
	}

	void assertRejected(StepVerifier.Step<String> step) {
		try {
			step.verifyErrorSatisfies(e -> assertThat(Exceptions.unwrap(e)).isInstanceOf(RejectedExecutionException.class));
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).isInstanceOf(RejectedExecutionException.class);
		}
	}

	void assertNoRejected(StepVerifier.Step<String> step) {
		try {
			step
					.thenAwait()
					.consumeErrorWith(e -> assertThat(Exceptions.unwrap(e)).isInstanceOf(RejectedExecutionException.class))
					.verify(Duration.ofMillis(1));
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).isInstanceOf(RejectedExecutionException.class);
		}
		catch (AssertionError e){
			if(!e.getMessage().contains("timed out")) {
				throw e;
			}
		}
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(

				scenario(f -> f.publishOn(Schedulers.fromExecutor(d -> {
					throw exception();
				})))
						.producerError(new RejectedExecutionException("Scheduler unavailable"))
						.receiverDemand(0),

				scenario(f -> f.publishOn(new FailWorkerScheduler()))
						.producerEmpty(),

				scenario(f -> f.publishOn(new FailNullWorkerScheduler()))
						.producerEmpty(),

				scenario(f -> f.publishOn(new RejectingWorkerScheduler(false, false)))
						.verifier(this::assertNoRejected)
						.receiverDemand(0),

				scenario(f -> f.publishOn(new RejectingWorkerScheduler(true, false)))
						.verifier(this::assertRejected)
						.receiverDemand(0),

				scenario(f -> f.publishOn(new RejectingWorkerScheduler(true, true)))
						.verifier(this::assertNoRejected)
						.receiverDemand(0),

				scenario(f -> f.publishOn(new RejectingWorkerScheduler(false, true)))
						.verifier(this::assertNoRejected),

				scenario(f -> f.publishOn(new RejectingWorkerScheduler(false, false)))
						.verifier(this::assertNoRejected)
						.receiverDemand(0),

				scenario(f -> f.publishOn(new RejectingWorkerScheduler(false, true)))
						.verifier(this::assertNoRejected)
						.producerEmpty(),

				scenario(f -> f.publishOn(new RejectingWorkerScheduler(true, false)))
						.verifier(this::assertRejected)
						.receiverDemand(0),

				scenario(f -> f.publishOn(new RejectingWorkerScheduler(true, true)))
						.verifier(this::assertNoRejected)
						.receiverDemand(0)
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
 				scenario(f -> f.publishOn(Schedulers.immediate())),

				scenario(f -> f.publishOn(Schedulers.immediate(), false, 4))
						.prefetch(4),

				scenario(f -> f.publishOn(Schedulers.immediate(), 1))
						.prefetch(1),

				scenario(f -> f.publishOn(Schedulers.immediate(), Integer.MAX_VALUE))
						.prefetch(Integer.MAX_VALUE)

		);
	}

	public static ExecutorService exec;

	@BeforeAll
	public static void before() {
		exec = Executors.newSingleThreadExecutor();
	}

	@AfterAll
	public static void after() {
		exec.shutdownNow();
	}

	@Test
	public void failPrefetch() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Flux.range(1, 10)
					.publishOn(Schedulers.immediate(), -1);
		});
	}

	@Test
	public void normal() {
		StepVerifier.create(Flux.range(1, 1_000_000)
		                        .hide()
		                        .publishOn(Schedulers.fromExecutorService(exec)))
		            .expectNextCount(1_000_000)
		            .verifyComplete();
	}

	@Test
	public void normalBackpressured1() throws Exception {
		StepVerifier.create(Flux.range(1, 1_000)
		                        .hide()
		                        .publishOn(Schedulers.fromExecutorService(exec)), 0)
		            .thenRequest(500)
		            .expectNextCount(500)
		            .thenRequest(500)
		            .expectNextCount(500)
		            .verifyComplete();
	}

	@Test
	public void normalBackpressured() throws Exception {
		StepVerifier.create(Flux.range(1, 1_000_000)
		                        .hide()
		                        .publishOn(Schedulers.fromExecutorService(exec)), 0)
		            .thenRequest(500_000)
		            .expectNextCount(500_000)
		            .thenRequest(500_000)
		            .expectNextCount(500_000)
		            .verifyComplete();
	}

	@Test
	public void normalSyncFused() {
		StepVerifier.create(Flux.range(1, 1_000_000)
		                        .publishOn(Schedulers.fromExecutorService(exec)))
		            .expectNextCount(1_000_000)
		            .verifyComplete();
	}

	@Test
	public void normalSyncFusedBackpressured() throws Exception {
		StepVerifier.create(Flux.range(1, 1_000_000)
		                        .publishOn(Schedulers.fromExecutorService(exec)), 0)
		            .thenRequest(500_000)
		            .expectNextCount(500_000)
		            .thenRequest(500_000)
		            .expectNextCount(500_000)
		            .verifyComplete();
	}

	@Test
	public void normalAsyncFused() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> up =
				Sinks.unsafe().many().unicast().onBackpressureBuffer(new ConcurrentLinkedQueue<>());

		for (int i = 0; i < 1_000_000; i++) {
			up.emitNext(i, FAIL_FAST);
		}
		up.emitComplete(FAIL_FAST);

		up.asFlux()
		  .publishOn(Schedulers.fromExecutorService(exec))
		  .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalAsyncFusedBackpressured() throws Exception {
		Sinks.Many<Integer> up =
				Sinks.unsafe().many().unicast().onBackpressureBuffer(Queues.<Integer>unbounded(1024).get());

		for (int i = 0; i < 1_000_000; i++) {
			up.emitNext(0, FAIL_FAST);
		}
		up.emitComplete(FAIL_FAST);

		StepVerifier.create(up.asFlux().publishOn(Schedulers.fromExecutorService(exec)), 0)
		            .expectSubscription()
		            .thenRequest(500_000)
		            .expectNextCount(500_000)
		            .thenRequest(500_000)
		            .expectNextCount(500_000)
		            .verifyComplete();
	}

	@Test
	public void error() {
		StepVerifier.create(Flux.error(new RuntimeException("forced failure"))
		                        .publishOn(Schedulers.fromExecutorService(exec)))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void errorHide() {
		StepVerifier.create(Flux.error(new RuntimeException("forced failure"))
		                        .hide()
		                        .publishOn(Schedulers.fromExecutorService(exec)))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>empty().publishOn(Schedulers.fromExecutorService(exec))
		                     .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorDelayed() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> err = Flux.error(new RuntimeException("forced " + "failure"));

		Flux.range(1, 1000)
		    .concatWith(err)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1000)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void classicJust() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void classicJustBackpressured() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		Thread.sleep(100);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void filtered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2_000_000)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void filtered1() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2_000)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalFilteredBackpressured() throws Exception {
		StepVerifier.create(Flux.range(1, 2_000_000)
		                        .hide()
		                        .publishOn(Schedulers.fromExecutorService(exec))
		                        .filter(v -> (v & 1) == 0), 0)
		            .thenRequest(500_000)
		            .expectNextCount(250_000)
		            .thenRequest(500_000)
		            .expectNextCount(750_000)
		            .verifyComplete();
	}

	@Test
	public void normalFilteredBackpressured1() throws Exception {
		StepVerifier.create(Flux.range(1, 2_000)
		                        .hide()
		                        .publishOn(Schedulers.fromExecutorService(exec))
		                        .filter(v -> (v & 1) == 0), 0)
		            .thenRequest(500)
		            .expectNextCount(250)
		            .thenRequest(500)
		            .expectNextCount(750)
		            .verifyComplete();
	}

	@Test
	public void callableEvaluatedTheRightTime() {

		AtomicInteger count = new AtomicInteger();

		Mono<Integer> p = Mono.fromCallable(count::incrementAndGet)
		                      .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()));

		assertThat(count).hasValue(0);

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		p.subscribe(ts);

		if (!ts.await(Duration.ofSeconds(5))
		       .isTerminated()) {
			ts.cancel();
			fail("AssertSubscriber timed out");
		}

		assertThat(count).hasValue(1);
	}

	@Test
	public void diamond() {
		Sinks.Many<Integer> sink = Sinks.many().multicast().<Integer>onBackpressureBuffer();
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> fork1 = sink.asFlux().map(d -> d)
		                        .publishOn(Schedulers.fromExecutorService(exec));
		Flux<Integer> fork2 = sink.asFlux().map(d -> d)
		                        .publishOn(Schedulers.fromExecutorService(exec));

		ts.request(256);
		Flux.merge(fork1, fork2)
		    .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		    .subscribe(ts);

		Flux.range(0, 128)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		    .subscribe(v -> sink.emitNext(v, FAIL_FAST), e -> sink.emitError(e, FAIL_FAST), () -> sink.emitComplete(FAIL_FAST));

		ts.await(Duration.ofSeconds(5))
		  .assertTerminated()
		  .assertValueCount(256)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void prefetchAmountOnly() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		ConcurrentLinkedQueue<Long> clq = new ConcurrentLinkedQueue<>();

		Flux.range(1, 2)
		    .hide()
		    .doOnRequest(v -> {
			    clq.offer(v);
		    })
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(100));

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();

		int s = clq.size();
		assertThat(s == 1 || s == 2 || s == 3).as("More requests?").isTrue();
		assertThat(clq.poll()).isEqualTo((Long) (long) Queues.SMALL_BUFFER_SIZE);
	}

	@Test
	public void boundedQueue() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 100_000)
		    .hide()
		    .publishOn(Schedulers.fromExecutor(exec), 128)
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(1));

		ts.assertValueCount(100_000)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void boundedQueueFilter() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 100_000)
		    .hide()
		    .publishOn(Schedulers.fromExecutor(exec), 128)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(1));

		ts.assertValueCount(50_000)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void withFlatMap() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 100_000)
		    .flatMap(Flux::just)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(100_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void syncSourceWithNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux.just(1, null, 1)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void syncSourceWithNull2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux.fromIterable(Arrays.asList(1, null, 1))
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void mappedsyncSourceWithNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux.just(1, 2)
		    .map(v -> v == 2 ? null : v)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void mappedsyncSourceWithNullHidden() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux.just(1, 2)
		    .hide()
		    .map(v -> v == 2 ? null : v)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void mappedsyncSourceWithNullPostFilterHidden() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux.just(1, 2)
		    .hide()
		    .map(v -> v == 2 ? null : v)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .filter(v -> true)
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void mappedsyncSourceWithNull2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux.fromIterable(Arrays.asList(1, 2))
		    .map(v -> v == 2 ? null : v)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void mappedsyncSourceWithNull2Hidden() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux.fromIterable(Arrays.asList(1, 2))
		    .hide()
		    .map(v -> v == 2 ? null : v)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void mappedFilteredSyncSourceWithNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux.just(1, 2)
		    .map(v -> v == 2 ? null : v)
		    .filter(v -> true)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void mappedFilteredSyncSourceWithNull2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux.fromIterable(Arrays.asList(1, 2))
		    .map(v -> v == 2 ? null : v)
		    .filter(v -> true)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void mappedAsyncSourceWithNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Sinks.Many<Integer> up =
				Sinks.unsafe().many().unicast().onBackpressureBuffer(Queues.<Integer>get(2).get());
		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		up.asFlux().map(v -> v == 2 ? null : v)
		  .publishOn(Schedulers.fromExecutorService(exec))
		  .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void mappedAsyncSourceWithNullPostFilter() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Sinks.Many<Integer> up =
				Sinks.unsafe().many().unicast().onBackpressureBuffer(Queues.<Integer>get(2).get());
		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		up.asFlux().map(v -> v == 2 ? null : v)
		  .publishOn(Schedulers.fromExecutorService(exec))
		  .filter(v -> true)
		  .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void crossRangeHidden() {
		int count = 1000000;

		StepVerifier.create(Flux.range(1, count)
		                        .hide()
		                        .flatMap(v -> Flux.range(v, 2)
		                                          .hide(), false, 128, 1)
		                        .hide()
		                        .publishOn(Schedulers.fromExecutorService(exec)))
		            .expectNextCount(2 * count)
		            .verifyComplete();
	}

	@Test
	public void crossRange() {
		int count = 1000000;

		StepVerifier.create(Flux.range(1, count)
		                        .flatMap(v -> Flux.range(v, 2), false, 128, 1)
		                        .publishOn(Schedulers.fromExecutorService(exec)))
		            .expectNextCount(2 * count)
		            .verifyComplete();
	}

	@Test
	public void crossRangeMaxHidden() throws Exception {
		int count = 1000000;

		StepVerifier.create(Flux.range(1, count)
		                        .hide()
		                        .flatMap(v -> Flux.range(v, 2)
		                                          .hide(), false, 4, 32)
		                        .hide()
		                        .publishOn(Schedulers.fromExecutorService(exec)))
		            .expectNextCount(2 * count)
		            .verifyComplete();

	}

	@Test
	public void crossRangeMax() {
		int count = 1000000;

		StepVerifier.create(Flux.range(1, count)
		                        .flatMap(v -> Flux.range(v, 2), false, 128, 32)
		                        .publishOn(Schedulers.fromExecutorService(exec)))
		            .expectNextCount(2 * count)
		            .verifyComplete();
	}

	@Test
	public void crossRangeMaxUnbounded() {
		int count = 1000000;

		StepVerifier.create(Flux.range(1, count)
		                        .flatMap(v -> Flux.range(v, 2))
		                        .publishOn(Schedulers.fromExecutorService(exec)))
		            .expectNextCount(2 * count)
		            .verifyComplete();
	}

	@Test
	public void threadBoundaryPreventsInvalidFusionMap() {
		Sinks.Many<Integer> up =
				Sinks.unsafe().many().unicast().onBackpressureBuffer(Queues.<Integer>get(2).get());

		AssertSubscriber<String> ts = AssertSubscriber.create();

		up.asFlux().map(v -> Thread.currentThread()
		                           .getName())
		  .publishOn(Schedulers.fromExecutorService(exec))
		  .subscribe(ts);

		up.emitNext(1, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(Thread.currentThread()
		                      .getName())
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void threadBoundaryPreventsInvalidFusionFilter() {
		Sinks.Many<Integer> up =
				Sinks.unsafe().many().unicast().onBackpressureBuffer(Queues.<Integer>get(2).get());

		String s = Thread.currentThread()
		                 .getName();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		up.asFlux().filter(v -> s.equals(Thread.currentThread()
		                                       .getName()))
		  .publishOn(Schedulers.fromExecutorService(exec))
		  .subscribe(ts);

		up.emitNext(1, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void crossRangePerfDefault() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Scheduler scheduler = Schedulers.fromExecutorService(exec);

		int count = 1000;

		Flux<Integer> source = Flux.range(1, count)
		                           .flatMap(v -> Flux.range(v, 2), false, 128, 32);

		source.publishOn(scheduler)
		      .subscribe(ts);

		if (!ts.await(Duration.ofSeconds(10))
		       .isTerminated()) {
			ts.cancel();
		}

		ts.assertValueCount(count * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void limitRate() {
		List<Long> upstreamRequests = new LinkedList<>();
		List<Long> downstreamRequests = new LinkedList<>();
		Flux<Integer> source = Flux.range(1, 400)
		                           .doOnRequest(upstreamRequests::add)
		                           .doOnRequest(r -> System.out.println(
				                           "upstream request of " + r))
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
	@Timeout(5)
	public void rejectedExecutionExceptionOnDataSignalExecutor()
			throws InterruptedException {

		final AtomicReference<Throwable> throwableInOnOperatorError =
				new AtomicReference<>();
		final AtomicReference<Object> dataInOnOperatorError = new AtomicReference<>();

		try {

			CountDownLatch hookLatch = new CountDownLatch(1);

			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				hookLatch.countDown();
				return t;
			});

			ExecutorService executor = newCachedThreadPool();
			CountDownLatch latch = new CountDownLatch(1);

			AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
			Flux.range(0, 5)
			    .publishOn(fromExecutorService(executor))
			    .doOnNext(s -> {
				    try {
					    latch.await();
				    }
				    catch (InterruptedException e) {
				    }
			    })
			    .publishOn(fromExecutor(executor))
			    .subscribe(assertSubscriber);

			executor.shutdownNow();

			assertSubscriber.assertNoValues()
			                .assertNoError()
			                .assertNotComplete();

			hookLatch.await();

			assertThat(throwableInOnOperatorError.get()).isInstanceOf(RejectedExecutionException.class);
			assertThat(dataInOnOperatorError).hasValue(0);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test
	@Disabled //Fix or deprecate fromExecutor, this test might randomly hang on CI
	public void rejectedExecutionExceptionOnErrorSignalExecutor()
			throws InterruptedException {

		Exception exception = new IllegalStateException();

		final AtomicReference<Throwable> throwableInOnOperatorError =
				new AtomicReference<>();
		final AtomicReference<Object> dataInOnOperatorError = new AtomicReference<>();

		try {

			CountDownLatch hookLatch = new CountDownLatch(2);

			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				hookLatch.countDown();
				return t;
			});

			ExecutorService executor = newCachedThreadPool();
			CountDownLatch latch = new CountDownLatch(1);

			AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
			Flux.range(0, 5)
			    .publishOn(fromExecutorService(executor))
			    .doOnNext(s -> {
				    try {
					    latch.await();
				    }
				    catch (InterruptedException e) {
					    throw Exceptions.propagate(exception);
				    }
			    })
			    .publishOn(fromExecutor(executor))
			    .subscribe(assertSubscriber);

			executor.shutdownNow();

			assertSubscriber.assertNoValues()
			                .assertNoError()
			                .assertNotComplete();

			hookLatch.await();

			assertThat(throwableInOnOperatorError.get()).isInstanceOf(RejectedExecutionException.class);
			assertThat(throwableInOnOperatorError.get().getSuppressed()[0]).isSameAs(exception);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test
	@Timeout(5)
	@Disabled
	public void rejectedExecutionExceptionOnDataSignalExecutorService()
			throws InterruptedException {

		CountDownLatch hookLatch = new CountDownLatch(1);

		Hooks.onOperatorError((t, d) -> {
			assertThat(t).isInstanceOf(RejectedExecutionException.class);
			assertThat(d).isNotNull();
			hookLatch.countDown();
			return t;
		});

		try {
			ExecutorService executor = newCachedThreadPool();
			StepVerifier.create(Flux.range(0, 5)
			                        .log()
			                        .publishOn(Schedulers.boundedElastic())
			                        .doOnRequest(n -> executor.shutdownNow())
			                        .publishOn(fromExecutorService(executor))
			                        .doOnNext(this::infiniteBlock))
			            .then(() -> {
				            try {
					            hookLatch.await();
				            }
				            catch (InterruptedException e) {
				            }
			            })
			            .thenCancel()
			            .verify();
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	void infiniteBlock(Integer t) {
		try {
			new CountDownLatch(1).await();
		}
		catch (InterruptedException e) {
		}
	}

	@Test
	public void rejectedExecutionExceptionOnErrorSignalExecutorService() {
		Exception exception = new IllegalStateException();

		ExecutorService executor = newCachedThreadPool();
		CountDownLatch latch = new CountDownLatch(1);

		final Flux<Integer> flux = Flux.range(0, 5)
		                               .publishOn(fromExecutorService(executor))
		                               .doOnNext(s -> {
			                               try {
				                               latch.await();
			                               }
			                               catch (InterruptedException e) {
				                               throw Exceptions.propagate(exception);
			                               }
		                               })
		                               .publishOn(fromExecutorService(executor));

		StepVerifier.create(flux, 0)
		            .expectSubscription()
		            .then(executor::shutdownNow)
		            .expectNextCount(0)
		            .expectErrorMatches(throwable ->
			                throwable instanceof RejectedExecutionException &&
				            throwable.getSuppressed()[0] == exception)
		            .verify(Duration.ofSeconds(5));
	}

	/**
	 * See #294 the consumer received more or less calls than expected Better reproducible
	 * with big thread pools, e.g. 128 threads
	 *
	 * @throws InterruptedException on interrupt
	 */
	@Test
	public void mapNotifiesOnce() throws InterruptedException {

		final int COUNT = 10000;
		final Object internalLock = new Object();
		final Object consumerLock = new Object();

		final CountDownLatch internalLatch = new CountDownLatch(COUNT);
		final CountDownLatch consumerLatch = new CountDownLatch(COUNT);

		final AtomicInteger internalCounter = new AtomicInteger(0);
		final AtomicInteger consumerCounter = new AtomicInteger(0);

		final ConcurrentHashMap<Object, Long> seenInternal = new ConcurrentHashMap<>();
		final ConcurrentHashMap<Object, Long> seenConsumer = new ConcurrentHashMap<>();

		Sinks.Many<Integer> s = Sinks.many().multicast().onBackpressureBuffer();
		Flux<Integer> d = s.asFlux();

		/*Disposable c = */
		d.publishOn(Schedulers.parallel())
		 .parallel(8)
		 .groups()
		 .subscribe(stream -> stream.publishOn(Schedulers.parallel())
		                            .map(o -> {
			                            synchronized (internalLock) {

				                            internalCounter.incrementAndGet();

				                            long curThreadId = Thread.currentThread()
				                                                     .getId();
				                            Long prevThreadId =
						                            seenInternal.put(o, curThreadId);
				                            if (prevThreadId != null) {
					                            fail(String.format(
							                            "The object %d has already been seen internally on the thread %d, current thread %d",
							                            o,
							                            prevThreadId,
							                            curThreadId));
				                            }

				                            internalLatch.countDown();
			                            }
			                            return -o;
		                            })
		                            .subscribe(o -> {
			                            synchronized (consumerLock) {
				                            consumerCounter.incrementAndGet();

				                            long curThreadId = Thread.currentThread()
				                                                     .getId();
				                            Long prevThreadId =
						                            seenConsumer.put(o, curThreadId);
				                            if (prevThreadId != null) {
					                            fail(String.format(
							                            "The object %d has already been seen by the consumer on the thread %d, current thread %d",
							                            o,
							                            prevThreadId,
							                            curThreadId));
				                            }

				                            consumerLatch.countDown();
			                            }
		                            }));

		final long start = System.currentTimeMillis();
		for (int i = 0; i < COUNT; i++) {
			long busyLoops = 0;
			while (s.tryEmitNext(i).isFailure()) {
				busyLoops++;
				if (busyLoops % 5000 == 0 && System.currentTimeMillis() - start >= 10_0000) {
					throw new RuntimeException("Busy loop timed out");
				}
			}
		}

		internalLatch.await(5, TimeUnit.SECONDS);
		assertThat(internalCounter).as("after 5s").hasValue(COUNT);
		consumerLatch.await(5, TimeUnit.SECONDS);
		assertThat(internalCounter).as("after 10s").hasValue(COUNT);
	}

	@Test
	public void mapManyFlushesAllValuesThoroughly() throws InterruptedException {
		int items = 1000;
		CountDownLatch latch = new CountDownLatch(items);
		Random random = ThreadLocalRandom.current();

		Sinks.Many<String> s = Sinks.many().multicast().onBackpressureBuffer();
		Flux<String> d = s.asFlux();

		Flux<Integer> tasks = d.publishOn(Schedulers.parallel())
		                       .parallel(8)
		                       .groups()
		                       .flatMap(stream -> stream.publishOn(Schedulers.parallel())
		                                                .map((String str) -> {
			                                                try {
				                                                Thread.sleep(random.nextInt(
						                                                10));
			                                                }
			                                                catch (InterruptedException e) {
				                                                Thread.currentThread()
				                                                      .interrupt();
			                                                }
			                                                return Integer.parseInt(str);
		                                                }));

		/* Disposable tail =*/
		tasks.subscribe(i -> {
			latch.countDown();
		});

		final long start = System.currentTimeMillis();
		for (int i = 1; i <= items; i++) {
			long busyLoops = 0;
			while (s.tryEmitNext(String.valueOf(i)).isFailure()) {
				busyLoops++;
				if (busyLoops % 5000 == 0 && System.currentTimeMillis() - start >= 10_0000) {
					throw new RuntimeException("Busy loop timed out");
				}
			}
		}
		latch.await(15, TimeUnit.SECONDS);
		assertThat(latch.getCount())
				.withFailMessage("%d of %d items were not counted down", latch.getCount(), items)
				.isZero();
	}

	@Test
	public void callablePath() {
		StepVerifier.create(Mono.fromCallable(() -> "test")
		                        .flux()
		                        .publishOn(Schedulers.immediate()))
		            .expectNext("test")
		            .verifyComplete();

		StepVerifier.create(Mono.fromCallable(() -> {
			throw new Exception("test");
		})
		                        .flux()
		                        .publishOn(Schedulers.immediate()))
		            .verifyErrorMessage("test");

		StepVerifier.create(Mono.fromCallable(() -> null)
		                        .flux()
		                        .publishOn(Schedulers.immediate()))
		            .verifyComplete();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> source = Flux.just(1);
		Scheduler scheduler = Schedulers.immediate();
		FluxPublishOn<Integer> test = new FluxPublishOn<>(source, scheduler, false, 3, 4, Queues.one());

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(PREFETCH)).isEqualTo(3);
		assertThat(test.scan(RUN_ON)).isSameAs(scheduler);
	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(RunStyle.ASYNC);
	}

	@Test
    public void scanSubscriber() throws InterruptedException {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxPublishOn.PublishOnSubscriber<Integer> test = new FluxPublishOn.PublishOnSubscriber<>(actual,
        		Schedulers.single(), Schedulers.single().createWorker(), true, 123, 123, Queues.unbounded());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(RunStyle.ASYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();

        //once cancelled, there shouldn't be any draining left
        // => better place to test that BUFFERED reflects the size of the queue
		Thread.sleep(50); //"hiccup" to ensure cancellation / draining is done
        test.queue.add(1);
        test.queue.add(1);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(2);
    }

	@Test
    public void scanConditionalSubscriber() {
		@SuppressWarnings("unchecked")
		Fuseable.ConditionalSubscriber<Integer> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
        FluxPublishOn.PublishOnConditionalSubscriber<Integer> test =
        		new FluxPublishOn.PublishOnConditionalSubscriber<>(actual, Schedulers.single(),
        				Schedulers.single().createWorker(), true, 123, 123, Queues.unbounded());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);
        test.queue.add(1);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(RunStyle.ASYNC);

        assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    //see https://github.com/reactor/reactor-core/issues/767
    @Test
    public void publishOnAsyncDetection() {
	    Publisher<String> a = Flux.just("a");
	    Publisher<String> b = Mono.just("b");

	    Flux<Tuple2<String, String>> flux =
			    Flux.from(a)
			        .flatMap(value -> Mono.just(value)
			                              .zipWith(Mono.from(b)))
			        .publishOn(Schedulers.single());

	    StepVerifier.create(flux)
	                .expectFusion(Fuseable.ASYNC)
	                .assertNext(tuple -> {
	                	assertThat(tuple.getT1()).isEqualTo("a");
		                assertThat(tuple.getT2()).isEqualTo("b");
	                })
	                .verifyComplete();
    }

    //see https://github.com/reactor/reactor-core/issues/767
    @Test
    public void publishOnAsyncDetectionConditional() {
	    Publisher<String> a = Flux.just("a");
	    Publisher<String> b = Mono.just("b");

	    Flux<Tuple2<String, String>> flux =
			    Flux.from(a)
			        .flatMap(value -> Mono.just(value)
			                              .zipWith(Mono.from(b)))
			        .publishOn(Schedulers.single())
			        .filter(t -> true);

	    StepVerifier.create(flux)
	                .expectFusion(Fuseable.ASYNC)
	                .assertNext(tuple -> {
	                	assertThat(tuple.getT1()).isEqualTo("a");
		                assertThat(tuple.getT2()).isEqualTo("b");
	                })
	                .verifyComplete();
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

	private static class FailNullWorkerScheduler implements Scheduler {

		@Override
		public Disposable schedule(Runnable task) {
			throw Exceptions.failWithRejected();
		}

		@Override
		@Nullable
		public Worker createWorker() {
			return null;
		}
	}

	private static class RejectingWorkerScheduler implements Scheduler {

		final boolean isSchedulerTerminated;
		final boolean isWorkerTerminated;

		RejectingWorkerScheduler(boolean isSchedulerTerminated,
				boolean isWorkerTerminated) {
			this.isSchedulerTerminated = isSchedulerTerminated;
			this.isWorkerTerminated = isWorkerTerminated;
		}

		@Override
		public Disposable schedule(Runnable task) {
			throw Exceptions.failWithRejected();
		}

		@Override
		public boolean isDisposed() {
			return isSchedulerTerminated;
		}

		@Override
		public Worker createWorker() {
			return new Worker() {

				@Override
				public Disposable schedule(Runnable task) {
					throw Exceptions.failWithRejected();
				}

				@Override
				public void dispose() {

				}

				@Override
				public boolean isDisposed() {
					return isWorkerTerminated;
				}
			};
		}
	}

	private class FailWorkerScheduler implements Scheduler {

		@Override
		public Disposable schedule(Runnable task) {
			throw Exceptions.failWithRejected();
		}

		@Override
		public Worker createWorker() {
			throw exception();
		}
	}

	@Test
	public void scanRunOn() {
		Scannable publishOnScannable = Scannable.from(
				Flux.just(1).hide()
				    .publishOn(Schedulers.boundedElastic())
		);
		Scannable runOnScannable = publishOnScannable.scan(Scannable.Attr.RUN_ON);

		assertThat(runOnScannable).isNotNull()
		                          .matches(Scannable::isScanAvailable, "isScanAvailable");

		System.out.println(runOnScannable + " isScannable " + runOnScannable.isScanAvailable());
		System.out.println(runOnScannable.scan(Scannable.Attr.NAME));
		runOnScannable.parents().forEach(System.out::println);
		System.out.println(runOnScannable.scan(Scannable.Attr.BUFFERED));
	}
}
