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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static reactor.core.scheduler.Schedulers.fromExecutor;
import static reactor.core.scheduler.Schedulers.fromExecutorService;

public class FluxPublishOnTest {

	static ExecutorService exec;

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
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1_000_000)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured1() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 1_000)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500);

		Thread.sleep(250);

		ts.assertValueCount(500)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 1_000_000)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500_000);

		Thread.sleep(250);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500_000);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalSyncFused() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1_000_000)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalSyncFusedBackpressured() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 1_000_000)
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500_000);

		Thread.sleep(500);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500_000);

		ts.await(Duration.ofSeconds(10));
		ts.assertTerminated();

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
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
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		UnicastProcessor<Integer> up =
				UnicastProcessor.create(QueueSupplier.<Integer>unbounded(1024).get());

		for (int i = 0; i < 1_000_000; i++) {
			up.onNext(0);
		}
		up.onComplete();

		up.publishOn(Schedulers.fromExecutorService(exec))
		  .subscribe(ts);

		try {
			ts.assertNoValues()
			  .assertNoError()
			  .assertNotComplete();

			ts.request(500_000);

			Thread.sleep(250);

			ts.assertValueCount(500_000)
			  .assertNoError()
			  .assertNotComplete();

			ts.request(500_000);

			if (!ts.await(Duration.ofSeconds(5))
			       .isTerminated()) {
				ts.cancel();
				Assert.fail("AssertSubscriber timed out: " + ts.values()
				                                             .size());
			}

			ts.assertValueCount(1_000_000)
			  .assertNoError()
			  .assertComplete();
		}
		finally {
			ts.cancel();
		}
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
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2_000_000)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500_000);

		Thread.sleep(500);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500_000);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalFilteredBackpressured1() throws Exception {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2_000)
		    .hide()
		    .publishOn(Schedulers.fromExecutorService(exec))
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500);

		Thread.sleep(500);

		ts.assertValueCount(500)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(500);

		ts.await(Duration.ofSeconds(5));

		ts.assertValueCount(1_000)
		  .assertNoError()
		  .assertComplete();
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
	public void crossRangePerfDefaultLoop() {
		for (int i = 0; i < 100000; i++) {
			if (i % 2000 == 0) {
				crossRangePerfDefault();
			}
		}
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
	public void crossRangePerfDefaultLoop2() {
		Scheduler scheduler = Schedulers.fromExecutorService(exec);

		int count = 1000;

		for (int j = 1; j < 256; j *= 2) {

			Flux<Integer> source = Flux.range(1, count)
			                           .flatMap(v -> Flux.range(v, 2), false, 128, j)
			                           .publishOn(scheduler);

			for (int i = 0; i < 10000; i++) {
				AssertSubscriber<Integer> ts = AssertSubscriber.create();

				source.subscribe(ts);

				if (!ts.await(Duration.ofSeconds(15))
				       .isTerminated()) {
					ts.cancel();
					Assert.fail("Timed out @ maxConcurrency = " + j);
				}

				ts.assertValueCount(count * 2)
				  .assertNoError()
				  .assertComplete();
			}
		}
	}

	@Test
	public void limitRate() {
		List<Long> upstreamRequests = new LinkedList<>();
		List<Long> downstreamRequests = new LinkedList<>();
		Flux<Integer> source = Flux
				.range(1, 400)
				.doOnRequest(upstreamRequests::add)
				.doOnRequest(r -> System.out.println("upstream request of " + r))
				.limitRate(40)
				.doOnRequest(downstreamRequests::add)
				.doOnRequest(r -> System.out.println("downstream request of " + r));

		AssertSubscriber<Integer> ts = AssertSubscriber.create(400);
		source.subscribe(ts);
		ts.await(Duration.ofMillis(100))
		  .assertComplete();

		Assert.assertThat("downstream didn't single request", downstreamRequests.size(), is(1));
		Assert.assertThat("downstream didn't request 400", downstreamRequests.get(0), is(400L));
		long total = 0L;
		for (Long requested : upstreamRequests) {
			total += requested;
			Assert.assertThat("rate limit not applied to request: " + requested,
			//30 is the optimization that eagerly prefetches when 3/4 of the request has been served
					requested, anyOf(is(40L), is(30L)));
		}
		Assert.assertThat("bad upstream total request", total, allOf(
				is(greaterThanOrEqualTo(400L)),
				is(lessThan(440L)
		)));
	}

	@Test
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
			Flux.range(0,5)
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
			Flux.range(0,5)
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

	@Test
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
			Flux.range(0,5)
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

	@Test
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
			Flux.range(0,5)
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

}
