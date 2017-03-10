/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.concurrent.WaitStrategy;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static reactor.util.concurrent.WaitStrategy.liteBlocking;

/**
 */
public class WorkQueueProcessorTest {

	static final Logger logger = Loggers.getLogger(WorkQueueProcessorTest.class);

	static final String e = "Element";

	static final String s = "Synchronizer";

	public static void submitInCurrentThread(BlockingSink<String> emitter) {
		Random rand = new Random();
		for (int i = 0; i < 1000; i++) {
			long re = emitter.submit(e);
			logger.debug("Submit element result " + re);
			LockSupport.parkNanos(2_000_000 + rand.nextInt(200_000) - 100_000);
			synchronized (s) {
				long rd = emitter.submit(s);
				logger.debug("Submit drain result " + rd);
				timeoutWait(s);
			}
		}
	}

	private static void timeoutWait(Object o) {
		long t0 = System.currentTimeMillis();
		try {
			o.wait(5_000);
		}
		catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		if (System.currentTimeMillis() - t0 > 5_000) {
			throw new RuntimeException("Timeout!");
		}
	}

	/* see https://github.com/reactor/reactor-core/issues/199 */
	@Test
	public void fixedThreadPoolWorkQueueRejectsSubscribers() {
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		WorkQueueProcessor<String> bc = WorkQueueProcessor.create(executorService, 16);
		CountDownLatch latch = new CountDownLatch(3);
		TestWorkQueueSubscriber spec1 = new TestWorkQueueSubscriber(latch, "spec1");
		TestWorkQueueSubscriber spec2 = new TestWorkQueueSubscriber(latch, "spec2");
		TestWorkQueueSubscriber spec3 = new TestWorkQueueSubscriber(latch, "spec3");

		bc.subscribe(spec1);
		bc.subscribe(spec2);
		bc.subscribe(spec3);

		bc.onNext("foo");
		bc.onComplete();

		assertThat(spec1.error, is(nullValue()));
		assertThat(spec2.error, is(nullValue()));
		assertThat(spec3.error, is(notNullValue()));
		assertThat(spec3.error.getMessage(),
				startsWith(
						"The executor service could not accommodate another subscriber, detected limit 2"));

		try {
			latch.await(1, TimeUnit.SECONDS);
		}
		catch (InterruptedException e1) {
			fail(e1.toString());
		}
	}

	/* see https://github.com/reactor/reactor-core/issues/199 */
	@Test
	public void forkJoinPoolWorkQueueRejectsSubscribers() {
		ExecutorService executorService = Executors.newWorkStealingPool(2);
		WorkQueueProcessor<String> bc = WorkQueueProcessor.create(executorService, 16);
		CountDownLatch latch = new CountDownLatch(2);
		TestWorkQueueSubscriber spec1 = new TestWorkQueueSubscriber(latch, "spec1");
		TestWorkQueueSubscriber spec2 = new TestWorkQueueSubscriber(latch, "spec2");
		TestWorkQueueSubscriber spec3 = new TestWorkQueueSubscriber(latch, "spec3");

		bc.subscribe(spec1);
		bc.subscribe(spec2);
		bc.subscribe(spec3);

		bc.onNext("foo");
		bc.onComplete();

		assertThat(spec1.error, is(nullValue()));
		assertThat(spec2.error, is(nullValue()));
		assertThat(spec3.error, is(notNullValue()));
		assertThat(spec3.error.getMessage(),
				is("The executor service could not accommodate another subscriber, detected limit 2"));

		try {
			latch.await(1, TimeUnit.SECONDS);
		}
		catch (InterruptedException e1) {
			fail(e1.toString());
		}
	}

	@Test
	public void highRate() throws Exception {
		WorkQueueProcessor<String> queueProcessor =
				WorkQueueProcessor.share("Processor", 256, liteBlocking());
		Scheduler timer = Schedulers.newTimer("Timer");
		queueProcessor.bufferTimeout(32, Duration.ofMillis(2), timer)
		              .subscribe(new Subscriber<List<String>>() {
			              int counter;

			              @Override
			              public void onComplete() {
				              System.out.println("Consumed in total: " + counter);
			              }

			              @Override
			              public void onError(Throwable t) {
				              t.printStackTrace();
			              }

			              @Override
			              public void onNext(List<String> strings) {
				              int size = strings.size();
				              counter += size;
				              if (strings.contains(s)) {
					              synchronized (s) {
						              //logger.debug("Synchronizer!");
						              s.notifyAll();
					              }
				              }
			              }

			              @Override
			              public void onSubscribe(Subscription s) {
				              s.request(Long.MAX_VALUE);
			              }
		              });
		BlockingSink<String> emitter = queueProcessor.connectSink();

		try {
			submitInCurrentThread(emitter);
		}
		finally {
			logger.debug("Finishing");
			emitter.finish();
			timer.dispose();
		}
		TimeUnit.SECONDS.sleep(1);
	}

	@Test(timeout = 15000L)
	public void cancelDoesNotHang() throws Exception {
		WorkQueueProcessor<String> wq = WorkQueueProcessor.create();

		Disposable d = wq.subscribe();

		Assert.assertTrue(wq.downstreamCount() == 1);

		d.dispose();

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 2) {
		}
	}

	@Test(timeout = 15000L)
	public void completeDoesNotHang() throws Exception {
		WorkQueueProcessor<String> wq = WorkQueueProcessor.create();

		wq.subscribe();

		Assert.assertTrue(wq.downstreamCount() == 1);

		wq.onComplete();

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 2) {
		}
	}

	@Test(timeout = 15000L)
	public void disposeSubscribeNoThreadLeak() throws Exception {
		WorkQueueProcessor<String> wq = WorkQueueProcessor.create(false);

		Disposable d = wq.subscribe();
		d.dispose();
		d = wq.subscribe();
		d.dispose();
		d = wq.subscribe();
		d.dispose();

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 2) {
		}
	}

	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberCold() throws Exception {
		AtomicInteger errors = new AtomicInteger(3);
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);
		AtomicInteger onNextSignals = new AtomicInteger();
		wq.onNext(1);
		wq.onNext(2);
		wq.onNext(3);
		wq.onComplete();
		StepVerifier.create(wq.log()
		                      .doOnNext(e -> onNextSignals.incrementAndGet()).<Integer>handle(
						(s1, sink) -> {
							if (errors.decrementAndGet() > 0) {
								sink.error(new RuntimeException());
							}
							else {
								sink.next(s1);
							}
						}).retry())
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		assertThat(onNextSignals.get(), equalTo(5));

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}

	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHot() throws Exception {
		AtomicInteger errors = new AtomicInteger(3);
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);
		AtomicInteger onNextSignals = new AtomicInteger();

		StepVerifier.create(wq.doOnNext(e -> onNextSignals.incrementAndGet()).<Integer>handle(
				(s1, sink) -> {
					if (errors.decrementAndGet() > 0) {
						sink.error(new RuntimeException());
					}
					else {
						sink.next(s1);
					}
				}).retry())
		            .then(() -> {
			            wq.onNext(1);
			            wq.onNext(2);
			            wq.onNext(3);
		            })
		            .expectNext(3)
		            .thenCancel()
		            .verify();

		assertThat(onNextSignals.get(), equalTo(3));

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}

	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHotPoisonSignal()
			throws Exception {
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);
		AtomicInteger onNextSignals = new AtomicInteger();

		StepVerifier.create(wq.doOnNext(e -> onNextSignals.incrementAndGet()).<Integer>handle(
				(s1, sink) -> {
					if (s1 == 1) {
						sink.error(new RuntimeException());
					}
					else {
						sink.next(s1);
					}
				}).retry())
		            .then(() -> {
			            wq.onNext(1);
			            wq.onNext(2);
			            wq.onNext(3);
		            })
		            .expectNext(2, 3)
		            .thenCancel()
		            .verify();

		assertThat(onNextSignals.get(), equalTo(3));

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}

	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHotPoisonSignal2()
			throws Exception {
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);
		AtomicInteger onNextSignals = new AtomicInteger();

		StepVerifier.create(wq.log()
		                      .doOnNext(e -> onNextSignals.incrementAndGet()).<Integer>handle(
						(s1, sink) -> {
							if (s1 == 2) {
								sink.error(new RuntimeException());
							}
							else {
								sink.next(s1);
							}
						}).retry())
		            .then(() -> {
			            wq.onNext(1);
			            wq.onNext(2);
			            wq.onNext(3);
		            })
		            .expectNext(1, 3)
		            .thenCancel()
		            .verify();

		assertThat(onNextSignals.get(), equalTo(3));

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}

	@Test()
	public void retryNoThreadLeak() throws Exception {
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);

		wq.handle((integer, sink) -> sink.error(new RuntimeException()))
		  .retry(10)
		  .subscribe();
		wq.onNext(1);
		wq.onNext(2);
		wq.onNext(3);
		wq.onComplete();

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}

	@Test
	public void simpleTest() throws Exception {
		final TopicProcessor<Integer> sink = TopicProcessor.create("topic");
		final WorkQueueProcessor<Integer> processor = WorkQueueProcessor.create("queue");

		int elems = 10000;
		CountDownLatch latch = new CountDownLatch(elems);

		//List<Integer> list = new CopyOnWriteArrayList<>();
		AtomicLong count = new AtomicLong();
		AtomicLong errorCount = new AtomicLong();

		processor.log("wqp.fail1").subscribe(d -> {
			         errorCount.incrementAndGet();
			         throw Exceptions.failWithCancel();
		         });

		processor.log("wqp.works").doOnNext(d -> count.incrementAndGet())
		         .subscribe(d -> {
			         latch.countDown();
			         //list.add(d);
		         });

		sink.subscribe(processor);
		sink.connect();
		for (int i = 0; i < elems; i++) {

			sink.onNext(i);
			if (i % 1000 == 0) {
				processor.log("wqp.fail2").subscribe(d -> {
					errorCount.incrementAndGet();
					throw Exceptions.failWithCancel();
				});
			}
		}

		latch.await(5, TimeUnit.SECONDS);
		System.out.println("count " + count + " errors: " + errorCount);
		sink.onComplete();
		Assert.assertTrue("Latch is " + latch.getCount(), latch.getCount() <= 1);
	}

	/* see https://github.com/reactor/reactor-core/issues/199 */
	@Test
	public void singleThreadWorkQueueDoesntRejectsSubscribers() {
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		WorkQueueProcessor<String> bc = WorkQueueProcessor.create(executorService, 2);
		CountDownLatch latch = new CountDownLatch(1);
		TestWorkQueueSubscriber spec1 = new TestWorkQueueSubscriber(latch, "spec1");
		TestWorkQueueSubscriber spec2 = new TestWorkQueueSubscriber(latch, "spec2");

		bc.subscribe(spec1);
		bc.subscribe(spec2);

		bc.onNext("foo");
		bc.onNext("bar");

		Executors.newSingleThreadScheduledExecutor()
		         .schedule(bc::onComplete, 200, TimeUnit.MILLISECONDS);
		try {
			bc.onNext("baz");
			fail("expected 3rd next to time out as newSingleThreadExecutor cannot be introspected");
		}
		catch (Throwable e) {
			assertTrue("expected AlertException, got " + e, WaitStrategy.isAlert(e));
		}
	}

	/* see https://github.com/reactor/reactor-core/issues/199 */
	@Test(timeout = 400)
	public void singleThreadWorkQueueSucceedsWithOneSubscriber() {
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		WorkQueueProcessor<String> bc = WorkQueueProcessor.create(executorService, 2);
		CountDownLatch latch = new CountDownLatch(1);
		TestWorkQueueSubscriber spec1 = new TestWorkQueueSubscriber(latch, "spec1");

		bc.subscribe(spec1);

		bc.onNext("foo");
		bc.onNext("bar");

		Executors.newSingleThreadScheduledExecutor()
		         .schedule(bc::onComplete, 200, TimeUnit.MILLISECONDS);
		bc.onNext("baz");

		try {
			latch.await(800, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e1) {
			fail(e1.toString());
		}

		assertNull(spec1.error);
	}

	@Test
	public void testBestEffortMaxSubscribers() {
		int expectedUnbounded = Integer.MAX_VALUE;
		int expectedUnknown = Integer.MIN_VALUE;

		ExecutorService executorService1 = Executors.newSingleThreadExecutor();
		ScheduledExecutorService executorService2 =
				Executors.newSingleThreadScheduledExecutor();
		ExecutorService executorService3 = Executors.newCachedThreadPool();
		ExecutorService executorService4 = Executors.newFixedThreadPool(2);
		ScheduledExecutorService executorService5 = Executors.newScheduledThreadPool(3);
		ExecutorService executorService6 = Executors.newWorkStealingPool(4);
		ExecutorService executorService7 =
				Executors.unconfigurableExecutorService(executorService4);
		ExecutorService executorService8 =
				Executors.unconfigurableScheduledExecutorService(executorService5);

		int maxSub1 = WorkQueueProcessor.bestEffortMaxSubscribers(executorService1);
		int maxSub2 = WorkQueueProcessor.bestEffortMaxSubscribers(executorService2);
		int maxSub3 = WorkQueueProcessor.bestEffortMaxSubscribers(executorService3);
		int maxSub4 = WorkQueueProcessor.bestEffortMaxSubscribers(executorService4);
		int maxSub5 = WorkQueueProcessor.bestEffortMaxSubscribers(executorService5);
		int maxSub6 = WorkQueueProcessor.bestEffortMaxSubscribers(executorService6);
		int maxSub7 = WorkQueueProcessor.bestEffortMaxSubscribers(executorService7);
		int maxSub8 = WorkQueueProcessor.bestEffortMaxSubscribers(executorService8);

		executorService1.shutdown();
		executorService2.shutdown();
		executorService3.shutdown();
		executorService4.shutdown();
		executorService5.shutdown();
		executorService6.shutdown();
		executorService7.shutdown();
		executorService8.shutdown();

		assertEquals("newSingleThreadExecutor", expectedUnknown, maxSub1);
		assertEquals("newSingleThreadScheduledExecutor", expectedUnknown, maxSub2);
		assertEquals("newCachedThreadPool", expectedUnbounded, maxSub3);
		assertEquals("newFixedThreadPool(2)", 2, maxSub4);
		assertEquals("newScheduledThreadPool(3)", expectedUnbounded, maxSub5);
		assertEquals("newWorkStealingPool(4)", 4, maxSub6);
		assertEquals("unconfigurableExecutorService", expectedUnknown, maxSub7);
		assertEquals("unconfigurableScheduledExecutorService", expectedUnknown, maxSub8);
	}

	private static class TestWorkQueueSubscriber extends BaseSubscriber<String> {

		private final CountDownLatch latch;
		private final String         id;

		Throwable error;

		private TestWorkQueueSubscriber(CountDownLatch latch, String id) {
			this.latch = latch;
			this.id = id;
		}

		@Override
		protected void hookOnSubscribe(Subscription subscription) {
			requestUnbounded();
		}

		@Override
		protected void hookOnNext(String value) {
			System.out.println(id + " received: " + value);
		}

		@Override
		protected void hookOnError(Throwable throwable) {
			error = throwable;
		}

		@Override
		protected void hookFinally(SignalType type) {
			System.out.println(id + " finished with: " + type);
			latch.countDown();
		}
	}

	@Test
	public void chainedWorkQueueProcessor() throws Exception{
		ExecutorService es = Executors.newFixedThreadPool(2);
		try {
			WorkQueueProcessor<String> bc = WorkQueueProcessor.create(es, 16);

			int elems = 18;
			CountDownLatch latch = new CountDownLatch(elems);

			bc.subscribe(TopicProcessorTest.sub("spec1", latch));
			Flux.range(0, elems)
			    .map(s -> "hello " + s)
			    .subscribe(bc);

			assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
		}
		finally {
			es.shutdown();
		}
	}

	@Test
	public void testWorkQueueProcessorGetters() {

		final int TEST_BUFFER_SIZE = 16;
		WorkQueueProcessor<Object> processor = WorkQueueProcessor.create("testProcessor", TEST_BUFFER_SIZE);

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.onNext(new Object());

		assertEquals(TEST_BUFFER_SIZE - 1, processor.getAvailableCapacity());
		processor.awaitAndShutdown();

	}

	@Test(expected = IllegalArgumentException.class)
	public void failNonPowerOfTwo() {
		TopicProcessor.create("test", 3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNullBufferSize() {
		WorkQueueProcessor.create("test", 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNegativeBufferSize() {
		WorkQueueProcessor.create("test", -1);
	}


	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHotPublishOn()
			throws Exception {
		AtomicInteger errors = new AtomicInteger(3);
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);
		AtomicInteger onNextSignals = new AtomicInteger();

		StepVerifier.create(wq.log()
		                      .publishOn(Schedulers.parallel())
		                      .publish()
		                      .autoConnect()
		                      .doOnNext(e -> onNextSignals.incrementAndGet())
		                      .map(s1 -> {
			                      if (errors.decrementAndGet() > 0) {
				                      throw new RuntimeException();
			                      }
			                      else {
				                      return s1;
			                      }
		                      })
		                      .log()
		                      .retry())
		            .then(() -> {
			            wq.onNext(1);
			            wq.onNext(2);
			            wq.onNext(3);
		            })
		            .expectNext(3)
		            .thenCancel()
		            .verify();

		// Need to explicitly complete processor due to use of publish()
		wq.onComplete();

		assertThat(onNextSignals.get(), equalTo(3));

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}

	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHotPublishOnPrefetch1()
			throws Exception {
		AtomicInteger errors = new AtomicInteger(3);
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);
		AtomicInteger onNextSignals = new AtomicInteger();

		StepVerifier.create(wq.publishOn(Schedulers.parallel(), 1)
		                      .publish()
		                      .autoConnect()
		                      .log()
		                      .doOnNext(e -> onNextSignals.incrementAndGet())
		                      .map(s1 -> {
			                      if (errors.decrementAndGet() > 0) {
				                      throw new RuntimeException();
			                      }
			                      else {
				                      return s1;
			                      }
		                      })
		                      .log()
		                      .retry())
		            .then(() -> {
			            wq.onNext(1);
			            wq.onNext(2);
			            wq.onNext(3);
		            })
		            .expectNext(3)
		            .thenCancel()
		            .verify();

		assertThat(onNextSignals.get(), equalTo(3));

		// Need to explicitly complete processor due to use of publish()
		wq.onComplete();

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}

	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHotPoisonSignalPublishOn()
			throws Exception {
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);
		AtomicInteger onNextSignals = new AtomicInteger();

		StepVerifier.create(wq.publishOn(Schedulers.parallel())
		                      .publish()
		                      .autoConnect()
		                      .doOnNext(e -> onNextSignals.incrementAndGet()).<Integer>handle(
						(s1, sink) -> {
							if (s1 == 1) {
								sink.error(new RuntimeException());
							}
							else {
								sink.next(s1);
							}
						}).retry())
		            .then(() -> {
			            wq.onNext(1);
			            wq.onNext(2);
			            wq.onNext(3);
		            })
		            .expectNext(2, 3)
		            .thenCancel()
		            .verify();

		assertThat(onNextSignals.get(), equalTo(3));

		// Need to explicitly complete processor due to use of publish()
		wq.onComplete();

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}

	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHotPoisonSignalParallel()
			throws Exception {
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);
		AtomicInteger onNextSignals = new AtomicInteger();

		Function<Flux<Integer>, Flux<Integer>> function =
				flux -> flux.doOnNext(e -> onNextSignals.incrementAndGet())
				            .handle((s1, sink) -> {
					            if (s1 == 1) {
						            sink.error(new RuntimeException());
					            }
					            else {
						            sink.next(s1);
					            }
				            });

		StepVerifier.create(wq.parallel(4)
		                      .runOn(Schedulers.newParallel("par", 4))
		                      .transform(flux -> ParallelFlux.from(flux.groups()
		                                                               .flatMap(s -> s.publish()
		                                                                              .autoConnect()
		                                                                              .transform(
				                                                                              function),
				                                                               true,
				                                                               QueueSupplier.SMALL_BUFFER_SIZE,
				                                                               QueueSupplier.XS_BUFFER_SIZE)))
		                      .sequential()
		                      .retry())
		            .then(() -> {
			            wq.onNext(1);
			            wq.onNext(2);
			            wq.onNext(3);
		            })
		            .expectNextMatches(d -> d == 2 || d == 3)
		            .expectNextMatches(d -> d == 2 || d == 3)
		            .thenCancel()
		            .verify();

		assertThat(onNextSignals.get(), CoreMatchers.either(equalTo(2)).or(equalTo(3)));

		// Need to explicitly complete processor due to use of publish()
		wq.onComplete();

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}

	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHotPoisonSignalPublishOnPrefetch1()
			throws Exception {
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);
		AtomicInteger onNextSignals = new AtomicInteger();

		StepVerifier.create(wq.publishOn(Schedulers.parallel(), 1)
		                      .publish()
		                      .autoConnect()
		                      .doOnNext(e -> onNextSignals.incrementAndGet()).<Integer>handle(
						(s1, sink) -> {
							if (s1 == 1) {
								sink.error(new RuntimeException());
							}
							else {
								sink.next(s1);
							}
						}).retry())
		            .then(() -> {
			            wq.onNext(1);
			            wq.onNext(2);
			            wq.onNext(3);
		            })
		            .expectNext(2, 3)
		            .thenCancel()
		            .verify();

		// Need to explicitly complete processor due to use of publish()
		wq.onComplete();

		assertThat(onNextSignals.get(), equalTo(3));

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}

	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHotPoisonSignalFlatMap()
			throws Exception {
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);
		AtomicInteger onNextSignals = new AtomicInteger();

		StepVerifier.create(wq.publish()
		                      .autoConnect()
		                      .flatMap(i -> Mono.just(i)
		                                        .doOnNext(e -> onNextSignals.incrementAndGet()).<Integer>handle(
						                      (s1, sink) -> {
							                      if (s1 == 1) {
								                      sink.error(new RuntimeException());
							                      }
							                      else {
								                      sink.next(s1);
							                      }
						                      }).subscribeOn(Schedulers.parallel()),
				                      true,
				                      QueueSupplier.XS_BUFFER_SIZE,
				                      QueueSupplier.SMALL_BUFFER_SIZE)
		                      .retry())
		            .then(() -> {
			            wq.onNext(1);
			            wq.onNext(2);
			            wq.onNext(3);
		            })
		            .expectNextMatches(d -> d == 2 || d == 3)
		            .expectNextMatches(d -> d == 2 || d == 3)
		            .thenCancel()
		            .verify();

		wq.onComplete();

		assertThat(onNextSignals.get(), equalTo(3));

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}

	// This test runs ok on it's own but hangs in when running whole class!
	@Ignore
	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHotPoisonSignalFlatMapPrefetch1()
			throws Exception {
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);
		AtomicInteger onNextSignals = new AtomicInteger();

		StepVerifier.create(wq.flatMap(i -> Mono.just(i)
		                                        .doOnNext(e -> onNextSignals.incrementAndGet()).<Integer>handle(
						(s1, sink) -> {
							if (s1 == 1) {
								sink.error(new RuntimeException());
							}
							else {
								sink.next(s1);
							}
						}).subscribeOn(Schedulers.parallel()),
				QueueSupplier.XS_BUFFER_SIZE,
				1)
		                      .retry())
		            .then(() -> {
			            wq.onNext(1);
			            wq.onNext(2);
			            wq.onNext(3);
		            })
		            .expectNextMatches(d -> d == 2 || d == 3)
		            .expectNextMatches(d -> d == 2 || d == 3)
		            .thenCancel()
		            .verify();

		wq.onComplete();

		assertThat(onNextSignals.get(), equalTo(3));

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}


	//see https://github.com/reactor/reactor-core/issues/445
	@Test(timeout = 5_000)
	public void testBufferSize1Shared() throws Exception {
		WorkQueueProcessor<String> broadcast = WorkQueueProcessor.share("share-name", 1, true);

		int simultaneousSubscribers = 3000;
		CountDownLatch latch = new CountDownLatch(simultaneousSubscribers);
		Scheduler scheduler = Schedulers.single();

		BlockingSink<String> sink = broadcast.connectSink();
		Flux<String> flux = broadcast.filter(Objects::nonNull)
		                             .publishOn(scheduler)
		                             .cache(1);

		for (int i = 0; i < simultaneousSubscribers; i++) {
			flux.subscribe(s -> latch.countDown());
		}
		sink.submit("data", 1, TimeUnit.SECONDS);

		Assertions.assertThat(latch.await(4, TimeUnit.SECONDS))
		          .overridingErrorMessage("Data not received")
		          .isTrue();
	}

	//see https://github.com/reactor/reactor-core/issues/445
	@Test(timeout = 5_000)
	public void testBufferSize1Created() throws Exception {
		WorkQueueProcessor<String> broadcast = WorkQueueProcessor.create("share-name", 1, true);

		int simultaneousSubscribers = 3000;
		CountDownLatch latch = new CountDownLatch(simultaneousSubscribers);
		Scheduler scheduler = Schedulers.single();

		BlockingSink<String> sink = broadcast.connectSink();
		Flux<String> flux = broadcast.filter(Objects::nonNull)
		                             .publishOn(scheduler)
		                             .cache(1);

		for (int i = 0; i < simultaneousSubscribers; i++) {
			flux.subscribe(s -> latch.countDown());
		}

		sink.submit("data", 1, TimeUnit.SECONDS);

		Assertions.assertThat(latch.await(4, TimeUnit.SECONDS))
		          .overridingErrorMessage("Data not received")
		          .isTrue();
	}

	@Test
	public void testDefaultRequestTaskThreadName() {
		String mainName = "workQueueProcessorRequestTask";
		String expectedName = mainName + "[request-task]";

		WorkQueueProcessor<Object> processor = WorkQueueProcessor.create(mainName, 8);

		processor.requestTask(Operators.cancelledSubscription());

		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		Condition<Thread> defaultRequestTaskThread = new Condition<>(
				thread -> expectedName.equals(thread.getName()),
				"a thread named \"%s\"", expectedName);

		Assertions.assertThat(threads)
		          .haveExactly(1, defaultRequestTaskThread);
	}

	@Test
	public void testCustomRequestTaskThreadName() {
		String expectedName = "workQueueProcessorRequestTask";
		WorkQueueProcessor<Object> processor = WorkQueueProcessor.create("testProcessor", 8);
		processor.setRequestTaskThreadFactory(r -> new Thread(r, expectedName));

		processor.requestTask(Operators.cancelledSubscription());

		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		Condition<Thread> customRequestTaskThread = new Condition<>(
				thread -> expectedName.equals(thread.getName()),
				"a thread named \"%s\"", expectedName);

		Assertions.assertThat(threads)
		          .haveExactly(1, customRequestTaskThread);
	}
}
