/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.*;
import static reactor.core.scheduler.Schedulers.fromExecutor;
import static reactor.core.scheduler.Schedulers.fromExecutorService;

public class FluxPublishOnTest {

	public static ExecutorService exec;

	@BeforeClass
	public static void before() {
		exec = Executors.newSingleThreadExecutor();
	}

	@AfterClass
	public static void after() {
		exec.shutdownNow();
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

		UnicastProcessor<Integer> up =
				UnicastProcessor.create(new ConcurrentLinkedQueue<>());

		for (int i = 0; i < 1_000_000; i++) {
			up.onNext(i);
		}
		up.onComplete();

		up.publishOn(Schedulers.fromExecutorService(exec))
		  .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalAsyncFusedBackpressured() throws Exception {
		UnicastProcessor<Integer> up =
				UnicastProcessor.create(QueueSupplier.<Integer>unbounded(1024).get());

		for (int i = 0; i < 1_000_000; i++) {
			up.onNext(0);
		}
		up.onComplete();

		StepVerifier.create(up.publishOn(Schedulers.fromExecutorService(exec)), 0)
		            .expectSubscription()
		            .thenRequest(500_000)
		            .expectNextCount(500_000)
		            .thenRequest(500_000)
		            .expectNextCount(500_000)
		            .verifyComplete();
	}

	@Test
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>error(new RuntimeException("forced failure")).publishOn(Schedulers.fromExecutorService(
				exec))
		                                                           .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
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

		Assert.assertEquals(0, count.get());

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		p.subscribe(ts);

		if (!ts.await(Duration.ofSeconds(5))
		       .isTerminated()) {
			ts.cancel();
			Assert.fail("AssertSubscriber timed out");
		}

		Assert.assertEquals(1, count.get());
	}

	public void diamond() {

		DirectProcessor<Integer> sp = DirectProcessor.create();
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> fork1 = sp.map(d -> d)
		                        .publishOn(Schedulers.fromExecutorService(exec));
		Flux<Integer> fork2 = sp.map(d -> d)
		                        .publishOn(Schedulers.fromExecutorService(exec));

		ts.request(256);
		Flux.merge(fork1, fork2)
		    .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		    .subscribe(ts);

		Flux.range(0, 128)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(ForkJoinPool.commonPool()))
		    .subscribe(sp);

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
		Assert.assertTrue("More requests?" + clq, s == 1 || s == 2 || s == 3);
		Assert.assertEquals((Long) (long) QueueSupplier.SMALL_BUFFER_SIZE, clq.poll());
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
		UnicastProcessor<Integer> up =
				UnicastProcessor.create(QueueSupplier.<Integer>get(2).get());
		up.onNext(1);
		up.onNext(2);
		up.onComplete();

		up.map(v -> v == 2 ? null : v)
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
		UnicastProcessor<Integer> up =
				UnicastProcessor.create(QueueSupplier.<Integer>get(2).get());
		up.onNext(1);
		up.onNext(2);
		up.onComplete();

		up.map(v -> v == 2 ? null : v)
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
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		int count = 1000000;

		Flux.range(1, count)
		    .hide()
		    .flatMap(v -> Flux.range(v, 2)
		                      .hide(), false, 128, 1)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		if (!ts.await(Duration.ofSeconds(5))
		       .isTerminated()) {
			ts.cancel();
		}

		ts.assertValueCount(count * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void crossRange() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		int count = 1000000;

		Flux.range(1, count)
		    .flatMap(v -> Flux.range(v, 2), false, 128, 1)
		    .publishOn(Schedulers.fromExecutorService(exec))
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
	public void crossRangeMaxHidden() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		int count = 1000000;

		Flux.range(1, count)
		    .hide()
		    .flatMap(v -> Flux.range(v, 2)
		                      .hide(), false, 4, 32)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(10))
		  .assertTerminated()
		  .assertValueCount(count * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void crossRangeMax() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		int count = 1000000;

		Flux.range(1, count)
		    .flatMap(v -> Flux.range(v, 2), false, 128, 32)
		    .publishOn(Schedulers.fromExecutorService(exec))
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
	public void crossRangeMaxUnbounded() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		int count = 1000000;

		Flux.range(1, count)
		    .flatMap(v -> Flux.range(v, 2))
		    .publishOn(Schedulers.fromExecutorService(exec))
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
	public void threadBoundaryPreventsInvalidFusionMap() {
		UnicastProcessor<Integer> up =
				UnicastProcessor.create(QueueSupplier.<Integer>get(2).get());

		AssertSubscriber<String> ts = AssertSubscriber.create();

		up.map(v -> Thread.currentThread()
		                  .getName())
		  .publishOn(Schedulers.fromExecutorService(exec))
		  .subscribe(ts);

		up.onNext(1);
		up.onComplete();

		ts.await(Duration.ofSeconds(5));

		ts.assertValues(Thread.currentThread()
		                      .getName())
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void threadBoundaryPreventsInvalidFusionFilter() {
		UnicastProcessor<Integer> up =
				UnicastProcessor.create(QueueSupplier.<Integer>get(2).get());

		String s = Thread.currentThread()
		                 .getName();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		up.filter(v -> s.equals(Thread.currentThread()
		                              .getName()))
		  .publishOn(Schedulers.fromExecutorService(exec))
		  .subscribe(ts);

		up.onNext(1);
		up.onComplete();

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

		Assert.assertThat("downstream didn't single request",
				downstreamRequests.size(),
				is(1));
		Assert.assertThat("downstream didn't request 400",
				downstreamRequests.get(0),
				is(400L));
		long total = 0L;
		for (Long requested : upstreamRequests) {
			total += requested;
			Assert.assertThat("rate limit not applied to request: " + requested,
					//30 is the optimization that eagerly prefetches when 3/4 of the request has been served
					requested,
					anyOf(is(40L), is(30L)));
		}
		Assert.assertThat("bad upstream total request",
				total,
				allOf(is(greaterThanOrEqualTo(400L)), is(lessThan(440L))));
	}

	@Test(timeout = 5000)
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

			Assert.assertThat(throwableInOnOperatorError.get(),
					CoreMatchers.instanceOf(RejectedExecutionException.class));
			Assert.assertSame(dataInOnOperatorError.get(), 0);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test
	@Ignore //Fix or deprecate fromExecutor, this test might randomly hang on CI
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

			Assert.assertThat(throwableInOnOperatorError.get(),
					CoreMatchers.instanceOf(RejectedExecutionException.class));
			Assert.assertSame(throwableInOnOperatorError.get()
			                                            .getSuppressed()[0], exception);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test(timeout = 5000)
	public void rejectedExecutionExceptionOnDataSignalExecutorService()
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
			    .publishOn(fromExecutorService(executor))
			    .subscribe(assertSubscriber);

			executor.shutdownNow();

			assertSubscriber.assertNoValues()
			                .assertNoError()
			                .assertNotComplete();

			hookLatch.await();

			Assert.assertThat(throwableInOnOperatorError.get(),
					CoreMatchers.instanceOf(RejectedExecutionException.class));
			Assert.assertSame(dataInOnOperatorError.get(), 0);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test(timeout = 5000)
	public void rejectedExecutionExceptionOnErrorSignalExecutorService()
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
			    .publishOn(fromExecutorService(executor))
			    .subscribe(assertSubscriber);

			executor.shutdownNow();

			assertSubscriber.assertNoValues()
			                .assertNoError()
			                .assertNotComplete();

			hookLatch.await();

			Assert.assertThat(throwableInOnOperatorError.get(),
					CoreMatchers.instanceOf(RejectedExecutionException.class));
			Assert.assertSame(throwableInOnOperatorError.get()
			                                            .getSuppressed()[0], exception);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
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
		final CountDownLatch counsumerLatch = new CountDownLatch(COUNT);

		final AtomicInteger internalCounter = new AtomicInteger(0);
		final AtomicInteger consumerCounter = new AtomicInteger(0);

		final ConcurrentHashMap<Object, Long> seenInternal = new ConcurrentHashMap<>();
		final ConcurrentHashMap<Object, Long> seenConsumer = new ConcurrentHashMap<>();

		EmitterProcessor<Integer> d = EmitterProcessor.create();
		BlockingSink<Integer> s = BlockingSink.create(d);

		/*Cancellation c = */
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
					                            System.out.println(String.format(
							                            "The object %d has already been seen by the consumer on the thread %d, current thread %d",
							                            o,
							                            prevThreadId,
							                            curThreadId));
					                            fail();
				                            }

				                            counsumerLatch.countDown();
			                            }
		                            }));

		for (int i = 0; i < COUNT; i++) {
			s.submit(i);
		}

		internalLatch.await(5, TimeUnit.SECONDS);
		assertEquals(COUNT, internalCounter.get());
		counsumerLatch.await(5, TimeUnit.SECONDS);
		assertEquals(COUNT, consumerCounter.get());
	}

	@Test
	public void mapManyFlushesAllValuesThoroughly() throws InterruptedException {
		int items = 1000;
		CountDownLatch latch = new CountDownLatch(items);
		Random random = ThreadLocalRandom.current();

		EmitterProcessor<String> d = EmitterProcessor.create();
		BlockingSink<String> s = d.connectSink();

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

		/* Cancellation tail =*/
		tasks.subscribe(i -> {
			latch.countDown();
		});

		for (int i = 1; i <= items; i++) {
			s.submit(String.valueOf(i));
		}
		latch.await(15, TimeUnit.SECONDS);
		assertTrue(latch.getCount() + " of " + items + " items were not counted down",
				latch.getCount() == 0);
	}
}
