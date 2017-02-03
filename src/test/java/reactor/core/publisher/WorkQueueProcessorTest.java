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

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.test.StepVerifier;
import reactor.util.Logger;
import reactor.util.Loggers;
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
		TimedScheduler timer = Schedulers.newTimer("Timer");
		queueProcessor.bufferMillis(32, 2, timer)
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

		StepVerifier.create(wq.log().<Integer>handle((s1, sink) -> {
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
			            wq.onComplete();
		            })
		            .expectNext(2, 3)
		            .verifyComplete();

		while (wq.downstreamCount() != 0 && Thread.activeCount() > 1) {
		}
	}



	@Test
	public void retryErrorPropagatedFromWorkQueueSubscriberHot() throws Exception {
		AtomicInteger errors = new AtomicInteger(3);
		WorkQueueProcessor<Integer> wq = WorkQueueProcessor.create(false);

		StepVerifier.create(wq.log().<Integer>handle((s1, sink) -> {
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
		            .expectNextMatches(d -> d == 1 || d == 2)
		            .expectNextMatches(d -> d == 2 || d == 3)
		            .thenCancel()
		            .verify();

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
			if (i % 100 == 0) {
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
			request(Long.MAX_VALUE);
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
}