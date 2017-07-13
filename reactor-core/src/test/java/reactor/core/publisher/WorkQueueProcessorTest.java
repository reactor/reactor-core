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
import javax.annotation.Nullable;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.FluxCreate.SerializedSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;
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

	public static void submitInCurrentThread(FluxSink<String> emitter) {
		Random rand = new Random();
		for (int i = 0; i < 1000; i++) {
			emitter.next(e);
			LockSupport.parkNanos(2_000_000 + rand.nextInt(200_000) - 100_000);
			synchronized (s) {
				emitter.next(s);
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
		WorkQueueProcessor<String> bc = WorkQueueProcessor.<String>builder().executor(executorService).bufferSize(16).build();
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
		WorkQueueProcessor<String> bc = WorkQueueProcessor.<String>builder().executor(executorService).bufferSize(16).build();
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
		WorkQueueProcessor<String> queueProcessor = WorkQueueProcessor.<String>builder()
				.share(true)
				.name("Processor")
				.bufferSize(256)
				.waitStrategy(liteBlocking())
				.build();
		Scheduler timer = Schedulers.newSingle("Timer");
		queueProcessor.bufferTimeout(32, Duration.ofMillis(2), timer)
		              .subscribe(new CoreSubscriber<List<String>>() {
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
		FluxSink<String> emitter = queueProcessor.sink();

		try {
			submitInCurrentThread(emitter);
		}
		finally {
			logger.debug("Finishing");
			emitter.complete();
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
		WorkQueueProcessor<String> wq = WorkQueueProcessor.<String>builder().autoCancel(false).build();

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
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();
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
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();
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
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();
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
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();
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
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();

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
		final TopicProcessor<Integer> sink = TopicProcessor.<Integer>builder().name("topic").build();
		final WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder().name("queue").build();

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
		WorkQueueProcessor<String> bc = WorkQueueProcessor.<String>builder().executor(executorService).bufferSize(2).build();
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
		WorkQueueProcessor<String> bc = WorkQueueProcessor.<String>builder().executor(executorService).bufferSize(2).build();
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
			WorkQueueProcessor<String> bc = WorkQueueProcessor.<String>builder().executor(es).bufferSize(16).build();

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
		WorkQueueProcessor<Object> processor = WorkQueueProcessor.builder().name("testProcessor").bufferSize(TEST_BUFFER_SIZE).build();

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.onNext(new Object());

		assertEquals(TEST_BUFFER_SIZE - 1, processor.getAvailableCapacity());
		processor.awaitAndShutdown();

	}

	@Test(expected = IllegalArgumentException.class)
	public void failNonPowerOfTwo() {
		WorkQueueProcessor.builder().name("test").bufferSize(3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNullBufferSize() {
		WorkQueueProcessor.builder().name("test").bufferSize(0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNegativeBufferSize() {
		WorkQueueProcessor.builder().name("test").bufferSize(-1);
	}


	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHotPublishOn()
			throws Exception {
		AtomicInteger errors = new AtomicInteger(3);
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();
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
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();
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
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();
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
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();
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
				                                                               Queues.SMALL_BUFFER_SIZE,
				                                                               Queues.XS_BUFFER_SIZE)))
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
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();
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
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();
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
				                      Queues.XS_BUFFER_SIZE,
				                      Queues.SMALL_BUFFER_SIZE)
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
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.<Integer>builder().autoCancel(false).build();
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
				Queues.XS_BUFFER_SIZE,
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
		WorkQueueProcessor<String> broadcast = WorkQueueProcessor.<String>builder()
				.share(true)
				.name("share-name")
				.bufferSize(1)
				.autoCancel(true)
				.build();

		int simultaneousSubscribers = 3000;
		CountDownLatch latch = new CountDownLatch(simultaneousSubscribers);
		Scheduler scheduler = Schedulers.single();

		FluxSink<String> sink = broadcast.sink();
		Flux<String> flux = broadcast.filter(Objects::nonNull)
		                             .publishOn(scheduler)
		                             .cache(1);

		for (int i = 0; i < simultaneousSubscribers; i++) {
			flux.subscribe(s -> latch.countDown());
		}
		sink.next("data");

		Assertions.assertThat(latch.await(4, TimeUnit.SECONDS))
		          .overridingErrorMessage("Data not received")
		          .isTrue();
	}

	//see https://github.com/reactor/reactor-core/issues/445
	@Test(timeout = 5_000)
	public void testBufferSize1Created() throws Exception {
		WorkQueueProcessor<String> broadcast = WorkQueueProcessor.<String>builder()
				.share(true).name("share-name")
				.bufferSize(1)
				.autoCancel(true)
				.build();

		int simultaneousSubscribers = 3000;
		CountDownLatch latch = new CountDownLatch(simultaneousSubscribers);
		Scheduler scheduler = Schedulers.single();

		FluxSink<String> sink = broadcast.sink();
		Flux<String> flux = broadcast.filter(Objects::nonNull)
		                             .publishOn(scheduler)
		                             .cache(1);

		for (int i = 0; i < simultaneousSubscribers; i++) {
			flux.subscribe(s -> latch.countDown());
		}

		sink.next("data");

		Assertions.assertThat(latch.await(4, TimeUnit.SECONDS))
		          .overridingErrorMessage("Data not received")
		          .isTrue();
	}

	@Test
	public void testDefaultRequestTaskThreadName() {
		String mainName = "workQueueProcessorRequestTask";
		String expectedName = mainName + "[request-task]";

		WorkQueueProcessor<Object> processor = WorkQueueProcessor.builder().name(mainName).bufferSize(8).build();

		processor.requestTask(Operators.cancelledSubscription());

		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		//cleanup to avoid visibility in other tests
		processor.forceShutdown();

		Condition<Thread> defaultRequestTaskThread = new Condition<>(
				thread -> expectedName.equals(thread.getName()),
				"a thread named \"%s\"", expectedName);

		Assertions.assertThat(threads)
		          .haveExactly(1, defaultRequestTaskThread);
	}

	@Test
	public void testCustomRequestTaskThreadNameCreate() {
		String expectedName = "workQueueProcessorRequestTaskCreate";
		//NOTE: the below single executor should not be used usually as requestTask assumes it immediately gets executed
		ExecutorService customTaskExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, expectedName));
		WorkQueueProcessor<Object> processor = WorkQueueProcessor.builder()
				.executor(Executors.newCachedThreadPool())
				.requestTaskExecutor(customTaskExecutor)
				.bufferSize(8)
				.waitStrategy(WaitStrategy.liteBlocking())
				.autoCancel(true)
				.build();

		processor.requestTask(Operators.cancelledSubscription());
		processor.subscribe();

		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		//cleanup to avoid visibility in other tests
		customTaskExecutor.shutdownNow();
		processor.forceShutdown();

		for (Thread thread : threads) {
			System.out.println(thread.getName());
		}

		Condition<Thread> customRequestTaskThread = new Condition<>(
				thread -> expectedName.equals(thread.getName()),
				"a thread named \"%s\"", expectedName);

		Assertions.assertThat(threads)
		          .haveExactly(1, customRequestTaskThread);
	}

	@Test
	public void testCustomRequestTaskThreadNameShare() {
		String expectedName = "workQueueProcessorRequestTaskShare";
		//NOTE: the below single executor should not be used usually as requestTask assumes it immediately gets executed
		ExecutorService customTaskExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, expectedName));
		WorkQueueProcessor<Object> processor = WorkQueueProcessor.builder()
				.executor(Executors.newCachedThreadPool())
				.requestTaskExecutor(customTaskExecutor)
				.bufferSize(8)
				.waitStrategy(WaitStrategy.liteBlocking())
				.autoCancel(true)
				.build();

		processor.requestTask(Operators.cancelledSubscription());
		processor.subscribe();

		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		//cleanup to avoid visibility in other tests
		customTaskExecutor.shutdownNow();
		processor.forceShutdown();

		for (Thread thread : threads) {
			System.out.println(thread.getName());
		}

		Condition<Thread> customRequestTaskThread = new Condition<>(
				thread -> expectedName.equals(thread.getName()),
				"a thread named \"%s\"", expectedName);

		Assertions.assertThat(threads)
		          .haveExactly(1, customRequestTaskThread);
	}

	@Test
	public void customRequestTaskThreadRejectsNull() {
		ExecutorService customTaskExecutor = null;

		Assertions.assertThatExceptionOfType(NullPointerException.class)
		          .isThrownBy(() -> new WorkQueueProcessor<>(
				          Thread::new,
				          Executors.newCachedThreadPool(),
				          customTaskExecutor,
				          8, WaitStrategy.liteBlocking(), true, true)
		          );
	}

	@Test
	public void createDefault() {
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.create();
		assertProcessor(processor, false, null, null, null, null, null, null);
	}

	@Test
	public void createOverrideAutoCancel() {
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, false, null, null, null, autoCancel, null, null);
	}

	@Test
	public void createOverrideName() {
		String name = "nameOverride";
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.name(name)
				.build();
		assertProcessor(processor, false, name, null, null, null, null, null);
	}

	@Test
	public void createOverrideNameBufferSize() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.create(name, bufferSize);
		assertProcessor(processor, false, name, bufferSize, null, null, null, null);
	}

	@Test
	public void createOverrideNameBufferSizeAutoCancel() {
		String name = "nameOverride";
		int bufferSize = 1024;
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.name(name)
				.bufferSize(bufferSize)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, false, name, bufferSize, null, autoCancel, null, null);
	}

	@Test
	public void createOverrideNameBufferSizeWaitStrategy() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.name(name)
				.bufferSize(bufferSize)
				.waitStrategy(waitStrategy)
				.build();
		assertProcessor(processor, false, name, bufferSize, waitStrategy, null, null, null);
	}

	@Test
	public void createDefaultExecutorOverrideAll() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.name(name)
				.bufferSize(bufferSize)
				.waitStrategy(waitStrategy)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, false, name, bufferSize, waitStrategy, autoCancel, null, null);
	}

	@Test
	public void createOverrideExecutor() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.executor(executor)
				.build();
		assertProcessor(processor, false, null, null, null, null, executor, null);
	}

	@Test
	public void createOverrideExecutorAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.executor(executor)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, false, null, null, null, autoCancel, executor, null);
	}

	@Test
	public void createOverrideExecutorBufferSize() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.executor(executor)
				.bufferSize(bufferSize)
				.build();
		assertProcessor(processor, false, null, bufferSize, null, null, executor, null);
	}

	@Test
	public void createOverrideExecutorBufferSizeAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.executor(executor)
				.bufferSize(bufferSize)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, false, null, bufferSize, null, autoCancel, executor, null);
	}

	@Test
	public void createOverrideExecutorBufferSizeWaitStrategy() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.executor(executor)
				.bufferSize(bufferSize)
				.waitStrategy(waitStrategy)
				.build();
		assertProcessor(processor, false, null, bufferSize, waitStrategy, null, executor, null);
	}

	@Test
	public void createOverrideExecutorBufferSizeWaitStrategyAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.executor(executor)
				.bufferSize(bufferSize)
				.waitStrategy(waitStrategy)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, false, null, bufferSize, waitStrategy, autoCancel, executor, null);
	}

	@Test
	public void createOverrideAll() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		ExecutorService requestTaskExecutor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.executor(executor)
				.requestTaskExecutor(requestTaskExecutor)
				.bufferSize(bufferSize)
				.waitStrategy(waitStrategy)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, false, null, bufferSize, waitStrategy, autoCancel, executor, requestTaskExecutor);
	}

	@Test
	public void shareOverrideAutoCancel() {
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, true, null, null, null, autoCancel, null, null);
	}

	@Test
	public void shareOverrideNameBufferSize() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.share(name, bufferSize);
		assertProcessor(processor, true, name, bufferSize, null, null, null, null);
	}

	@Test
	public void shareOverrideNameBufferSizeWaitStrategy() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.name(name)
				.bufferSize(bufferSize)
				.waitStrategy(waitStrategy)
				.build();
		assertProcessor(processor, true, name, bufferSize, waitStrategy, null, null, null);
	}

	@Test
	public void shareDefaultExecutorOverrideAll() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.name(name)
				.bufferSize(bufferSize)
				.waitStrategy(waitStrategy)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, true, name, bufferSize, waitStrategy, autoCancel, null, null);
	}

	@Test
	public void shareOverrideExecutor() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.executor(executor)
				.build();
		assertProcessor(processor, true, null, null, null, null, executor, null);
	}

	@Test
	public void shareOverrideExecutorAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.executor(executor)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, true, null, null, null, autoCancel, executor, null);
	}

	@Test
	public void shareOverrideExecutorBufferSize() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.executor(executor)
				.bufferSize(bufferSize)
				.build();
		assertProcessor(processor, true, null, bufferSize, null, null, executor, null);
	}

	@Test
	public void shareOverrideExecutorBufferSizeAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.executor(executor)
				.bufferSize(bufferSize)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, true, null, bufferSize, null, autoCancel, executor, null);
	}

	@Test
	public void shareOverrideExecutorBufferSizeWaitStrategy() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.executor(executor)
				.bufferSize(bufferSize)
				.waitStrategy(waitStrategy)
				.build();
		assertProcessor(processor, true, null, bufferSize, waitStrategy, null, executor, null);
	}

	@Test
	public void shareOverrideExecutorBufferSizeWaitStrategyAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.executor(executor)
				.bufferSize(bufferSize)
				.waitStrategy(waitStrategy)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, true, null, bufferSize, waitStrategy, autoCancel, executor, null);
	}

	@Test
	public void shareOverrideAll() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		ExecutorService requestTaskExecutor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		WorkQueueProcessor<Integer> processor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.executor(executor)
				.requestTaskExecutor(requestTaskExecutor)
				.bufferSize(bufferSize)
				.waitStrategy(waitStrategy)
				.autoCancel(autoCancel)
				.build();
		assertProcessor(processor, true, null, bufferSize, waitStrategy, autoCancel, executor, requestTaskExecutor);
	}

	@Test
	public void scanProcessor() {
		WorkQueueProcessor<String> test = WorkQueueProcessor.create("name", 16);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isEqualTo(subscription);

		Assertions.assertThat(test.scan(Scannable.IntAttr.CAPACITY)).isEqualTo(16);
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		Assertions.assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();

		test.onError(new IllegalStateException("boom"));
		Assertions.assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanInner() {
		WorkQueueProcessor<String> main = WorkQueueProcessor.create("name", 16);
		CoreSubscriber<String> subscriber = new LambdaSubscriber<>(null, e -> {}, null, null);

		WorkQueueProcessor.WorkQueueInner<String> test = new WorkQueueProcessor.WorkQueueInner<>(
				subscriber, main);

		Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(main);
		Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(subscriber);
		Assertions.assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		Assertions.assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);

		test.pendingRequest.set(123);
		Assertions.assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(123L);

		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

		main.terminated = 1;
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

		test.cancel();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

	private void assertProcessor(WorkQueueProcessor<Integer> processor,
			boolean shared,
			@Nullable String name,
			@Nullable Integer bufferSize,
			@Nullable WaitStrategy waitStrategy,
			@Nullable Boolean autoCancel,
			@Nullable ExecutorService executor,
			@Nullable ExecutorService requestTaskExecutor) {

		String expectedName = name != null ? name : WorkQueueProcessor.class.getSimpleName();
		int expectedBufferSize = bufferSize != null ? bufferSize : Queues.SMALL_BUFFER_SIZE;
		boolean expectedAutoCancel = autoCancel != null ? autoCancel : true;
		WaitStrategy expectedWaitStrategy = waitStrategy != null ? waitStrategy : WaitStrategy.liteBlocking();
		Class<?> sequencerClass = shared ? MultiProducerRingBuffer.class : SingleProducerSequencer.class;

		assertEquals(expectedName, processor.name);
		assertEquals(expectedBufferSize, processor.getBufferSize());
		assertEquals(expectedAutoCancel, processor.autoCancel);
		assertEquals(expectedWaitStrategy.getClass(), processor.ringBuffer.getSequencer().waitStrategy.getClass());
		assertEquals(sequencerClass, processor.ringBuffer.getSequencer().getClass());
		if (executor != null)
			assertEquals(executor, processor.executor);
		if (requestTaskExecutor != null)
			assertEquals(requestTaskExecutor, processor.requestTaskExecutor);
	}

	@Test
	public void serializedSinkSingleProducer() throws Exception {
		WorkQueueProcessor<Integer> queueProcessor = WorkQueueProcessor.<Integer>builder()
				.share(false).build();

		FluxSink<Integer> sink = queueProcessor.sink();
		Assertions.assertThat(sink).isInstanceOf(SerializedSink.class);
		sink = sink.next(1);
		Assertions.assertThat(sink).isInstanceOf(SerializedSink.class);
		sink = sink.onRequest(n -> {});
		Assertions.assertThat(sink).isInstanceOf(SerializedSink.class);
	}

	@Test
	public void nonSerializedSinkMultiProducer() throws Exception {
		int count = 1000;
		WorkQueueProcessor<Integer> queueProcessor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.build();
		TestSubscriber subscriber = new TestSubscriber(count);
		queueProcessor.subscribe(subscriber);
		FluxSink<Integer> sink = queueProcessor.sink();
		Assertions.assertThat(sink).isNotInstanceOf(SerializedSink.class);

		for (int i = 0; i < count; i++) {
			sink = sink.next(i);
			Assertions.assertThat(sink).isNotInstanceOf(SerializedSink.class);
		}
		subscriber.await(Duration.ofSeconds(5));
		assertNull("Unexpected exception in subscriber", subscriber.failure);
	}

	@Test
	public void serializedSinkMultiProducerWithOnRequest() throws Exception {
		int count = 1000;
		WorkQueueProcessor<Integer> queueProcessor = WorkQueueProcessor.<Integer>builder()
				.share(true)
				.build();
		TestSubscriber subscriber = new TestSubscriber(count);
		queueProcessor.subscribe(subscriber);
		FluxSink<Integer> sink = queueProcessor.sink();
		AtomicInteger next = new AtomicInteger();
		FluxSink<Integer> serializedSink = sink.onRequest(n -> {
			for (int i = 0; i < n; i++) {
				synchronized (s) { // to ensure that elements are in order for testing
					FluxSink<Integer> retSink = sink.next(next.getAndIncrement());
					Assertions.assertThat(retSink).isInstanceOf(SerializedSink.class);
				}
			}
		});
		Assertions.assertThat(serializedSink).isInstanceOf(SerializedSink.class);

		subscriber.await(Duration.ofSeconds(5));
		sink.complete();
		assertNull("Unexpected exception in subscriber", subscriber.failure);
	}

	static class TestSubscriber implements CoreSubscriber<Integer> {

		final CountDownLatch latch;

		final AtomicInteger next;

		Throwable failure;

		TestSubscriber(int count) {
			latch = new CountDownLatch(count);
			next = new AtomicInteger();
		}

		public void await(Duration duration) throws InterruptedException {
			assertTrue("Did not receive all, remaining=" + latch.getCount(), latch.await(duration.toMillis(), TimeUnit.MILLISECONDS));
		}

		@Override
		public void onComplete() {
			try {
				assertEquals(0, latch.getCount());
			}
			catch (Throwable t) {
				failure = t;
			}
		}

		@Override
		public void onError(Throwable t) {
			failure = t;
		}

		@Override
		public void onNext(Integer n) {
			latch.countDown();
			try {
				assertEquals(next.getAndIncrement(), n.intValue());
			}
			catch (Throwable t) {
				failure = t;
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Long.MAX_VALUE);
		}
	}
}
