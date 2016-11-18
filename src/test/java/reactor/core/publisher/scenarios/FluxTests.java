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

package reactor.core.publisher.scenarios;

import java.awt.event.KeyEvent;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.Exceptions;
import reactor.core.publisher.AbstractReactorTest;
import reactor.core.publisher.BlockingSink;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.TopicProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuples;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.*;

public class FluxTests extends AbstractReactorTest {

	static final String2Integer STRING_2_INTEGER = new String2Integer();

	@Test
	public void testThenPublisherVoid() throws InterruptedException {
		Mono<Void> testVoidPublisher = Flux
				.just("A", "B")
				.thenEmpty(Mono.fromRunnable(() -> { }));

		AssertSubscriber<Void> ts = AssertSubscriber.create();
		testVoidPublisher.subscribe(ts);

		ts.assertValueCount(0);
		ts.assertComplete();
	}

	@Test
	public void testComposeFromSingleValue() throws InterruptedException {
		Flux<String> stream = Flux.just("Hello World!");
		Flux<String> s = stream.map(s1 -> "Goodbye then!");

		await(s, is("Goodbye then!"));
	}

	@Test
	public void testComposeFromMultipleValues() throws InterruptedException {
		Flux<String> stream = Flux.just("1", "2", "3", "4", "5");
		Flux<Integer> s = stream.map(STRING_2_INTEGER)
		                           .map(new Function<Integer, Integer>() {
			                          int sum = 0;

			                          @Override
			                          public Integer apply(Integer i) {
				                          sum += i;
				                          return sum;
			                          }
		                          });
		await(5, s, is(15));
	}

	@Test
	public void simpleReactiveSubscriber() throws InterruptedException {
		EmitterProcessor<String> str = EmitterProcessor.create();

		str.publishOn(asyncGroup)
		   .subscribe(new FooSubscriber());

		str.onNext("Goodbye World!");
		str.onNext("Goodbye World!");
		str.onComplete();

		Thread.sleep(500);
	}

	@Test
	public void testComposeFromMultipleFilteredValues() throws InterruptedException {
		Flux<String> stream = Flux.just("1", "2", "3", "4", "5");
		Flux<Integer> s = stream.map(STRING_2_INTEGER)
		                           .filter(i -> i % 2 == 0);

		await(2, s, is(4));
	}

	@Test
	public void testComposedErrorHandlingWithMultipleValues() throws InterruptedException {
		Flux<String> stream = Flux.just("1", "2", "3", "4", "5");

		final AtomicBoolean exception = new AtomicBoolean(false);
		Flux<Integer> s = stream.map(STRING_2_INTEGER)
		                           .map(new Function<Integer, Integer>() {
			                          int sum = 0;

			                          @Override
			                          public Integer apply(Integer i) {
				                          if (i >= 5) {
					                          throw new IllegalArgumentException();
				                          }
				                          sum += i;
				                          return sum;
			                          }
		                          })
		                           .doOnError(IllegalArgumentException.class, e -> exception.set(true));

		await(5, s, is(10));
		assertThat("error triggered", exception.get(), is(true));
	}

	@Test
	public void testReduce() throws InterruptedException {
		Flux<String> stream = Flux.just("1", "2", "3", "4", "5");
		Mono<Integer> s = stream.map(STRING_2_INTEGER)
		                          .reduce(1, (acc, next) -> acc * next);
		await(1, s, is(120));
	}

	@Test
	public void testMerge() throws InterruptedException {
		Flux<String> stream1 = Flux.just("1", "2");
		Flux<String> stream2 = Flux.just("3", "4", "5");
		Mono<Integer> s = Flux.merge(stream1, stream2)
		                         //.publishOn(env)
		                         .log("merge")
		                         .map(STRING_2_INTEGER)
		                         .reduce(1, (acc, next) -> acc * next);
		await(1, s, is(120));
	}

	@Test
	public void testStreamBatchesResults() {
		Flux<String> stream = Flux.just("1", "2", "3", "4", "5");
		Mono<List<Integer>> s = stream.map(STRING_2_INTEGER)
		                                .collectList();

		final AtomicInteger batchCount = new AtomicInteger();
		final AtomicInteger count = new AtomicInteger();
		s.subscribe(is -> {
			batchCount.incrementAndGet();
			for (int i : is) {
				count.addAndGet(i);
			}
		});

		assertThat("batchCount is 3", batchCount.get(), is(1));
		assertThat("count is 15", count.get(), is(15));
	}

	@Test
	public void testHandlersErrorsDownstream() throws InterruptedException {
		Flux<String> stream = Flux.just("1", "2", "a", "4", "5");
		final CountDownLatch latch = new CountDownLatch(1);
		Flux<Integer> s = stream.map(STRING_2_INTEGER)
		                           .map(new Function<Integer, Integer>() {
			                          int sum = 0;

			                          @Override
			                          public Integer apply(Integer i) {
				                          if (i >= 5) {
					                          throw new IllegalArgumentException();
				                          }
				                          sum += i;
				                          return sum;
			                          }
		                          })
		                           .doOnError(NumberFormatException.class, new Consumer<NumberFormatException>() {
			                          @Override
			                          public void accept(NumberFormatException e) {
				                          latch.countDown();
			                          }
		                          });

		await(2, s, is(3));
		assertThat("error handler was invoked", latch.getCount(), is(0L));
	}

	@Test
	public void promiseAcceptCountCannotExceedOne() {
		MonoProcessor<Object> deferred = MonoProcessor.create();
		deferred.onNext("alpha");
		try {
			deferred.onNext("bravo");
		}
		catch (Exception e) {
			if(!Exceptions.isCancel(e)) {
				throw e;
			}
		}
		assertEquals(deferred.block(), "alpha");
	}

	@Test
	public void promiseErrorCountCannotExceedOne() {
		MonoProcessor<Object> deferred = MonoProcessor.create();
		Throwable error = new Exception();
		try {
			deferred.onError(error);
			deferred.onNext(error);
			fail();
		}
		catch (Exception ise) {
			if(!Exceptions.isBubbling(ise)){
				throw ise;
			}
		}
		assertTrue(deferred.getError() instanceof Exception);
	}

	@Test
	public void promiseAcceptCountAndErrorCountCannotExceedOneInTotal() {
		MonoProcessor<Object> deferred = MonoProcessor.create();
		Throwable error = new Exception();
		try {
			deferred.onError(error);
			deferred.onNext("alpha");
			fail();
		}
		catch (RuntimeException ise) {
			if(!Exceptions.isBubbling(ise)){
				throw ise;
			}
		}
		assertTrue(deferred.getError() instanceof Exception);
	}

	@Test
	public void mapManyFlushesAllValuesThoroughly() throws InterruptedException {
		int items = 1000;
		CountDownLatch latch = new CountDownLatch(items);
		Random random = ThreadLocalRandom.current();

		EmitterProcessor<String> d = EmitterProcessor.create();
		BlockingSink<String> s = d.connectSink();

		Flux<Integer> tasks = d.publishOn(asyncGroup)
		                       .parallel(8)
		                       .groups()
		                       .flatMap(stream -> stream.publishOn(asyncGroup)
		                                                  .map((String str) -> {
			                                                  try {
				                                                  Thread.sleep(random.nextInt(10));
			                                                  }
			                                                  catch (InterruptedException e) {
				                                                  Thread.currentThread()
				                                                        .interrupt();
			                                                  }
			                                                  return Integer.parseInt(str);
		                                                  }));

		/* Cancellation tail =*/ tasks.subscribe(i -> {
			latch.countDown();
		});

		for (int i = 1; i <= items; i++) {
			s.submit(String.valueOf(i));
		}
		latch.await(15, TimeUnit.SECONDS);
		assertTrue(latch.getCount() + " of " + items + " items were not counted down", latch.getCount() == 0);
	}

	@Test
	public void mapManyFlushesAllValuesConsistently() throws InterruptedException {
		int iterations = 5;
		for (int i = 0; i < iterations; i++) {
			mapManyFlushesAllValuesThoroughly();
		}
	}

	<T> void await(Flux<T> s, Matcher<T> expected) throws InterruptedException {
		await(1, s, expected);
	}

	<T> void await(int count, final Publisher<T> s, Matcher<T> expected) throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(count);
		final AtomicReference<T> ref = new AtomicReference<>();
		Flux.from(s).subscribe(t -> {
			ref.set(t);
			latch.countDown();
		}, t -> {
			t.printStackTrace();
			latch.countDown();
		});

		long startTime = System.currentTimeMillis();
		T result = null;
		try {
			latch.await(10, TimeUnit.SECONDS);

			result = ref.get();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		long duration = System.currentTimeMillis() - startTime;

		assertThat(result, expected);
		assertThat(duration, is(lessThan(2000L)));
	}

	static class String2Integer implements Function<String, Integer> {

		@Override
		public Integer apply(String s) {
			return Integer.parseInt(s);
		}
	}

	@Test
	public void mapNotifiesOnceConsistent() throws InterruptedException {
		for (int i = 0; i < 100; i++) {
			mapNotifiesOnce();
		}
	}

	/**
	 * See #294 the consumer received more or less calls than expected Better reproducible with big thread pools, e.g.
	 * 128 threads
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

		/*Cancellation c = */d.publishOn(asyncGroup)
		                  .parallel(8)
		                  .groups()
		                  .subscribe(stream -> stream.publishOn(asyncGroup)
		                                      .map(o -> {
			                                      synchronized (internalLock) {

				                                      internalCounter.incrementAndGet();

				                                      long curThreadId = Thread.currentThread()
				                                                               .getId();
				                                      Long prevThreadId = seenInternal.put(o, curThreadId);
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
				                                      Long prevThreadId = seenConsumer.put(o, curThreadId);
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
	public void analyticsTest() throws Exception {
		ReplayProcessor<Integer> source = ReplayProcessor.<Integer>create().connect();

		long avgTime = 50l;

		Mono<Long> result = source
				.log("delay")
				.publishOn(asyncGroup)
		                          .delay(Duration.ofMillis(avgTime))

		                          .elapsed()
		                          .skip(1)
		                          .groupBy(w -> w.getT1())
								  .flatMap(w -> w.count().map(c -> Tuples.of(w.key(), c)))
		                          .log("elapsed")
		                          .collectSortedList((a, b) -> a.getT1().compareTo(b.getT1()))
		                          .flatMap(Flux::fromIterable)
		                          .reduce(-1L, (acc, next) -> acc > 0l ? ((next.getT1() + acc) / 2) : next.getT1())
		                          .log("reduced-elapsed")
		                          .cache();

		source.subscribe();

		for (int j = 0; j < 10; j++) {
			source.onNext(1);
		}
		source.onComplete();

		Assert.assertTrue(result.block(Duration.ofSeconds(5)) >= avgTime * 0.6);
	}

	@Test
	public void konamiCode() throws InterruptedException {
		final TopicProcessor<Integer> keyboardStream = TopicProcessor.create();

		Mono<List<Boolean>> konamis = keyboardStream
		                                     .skipWhile(key -> KeyEvent.VK_UP != key)
		                                     .buffer(10, 1)
		                                     .map(keys -> keys.size() == 10 &&
				                                     keys.get(0) == KeyEvent.VK_UP &&
				                                     keys.get(1) == KeyEvent.VK_UP &&
				                                     keys.get(2) == KeyEvent.VK_DOWN &&
				                                     keys.get(3) == KeyEvent.VK_DOWN &&
				                                     keys.get(4) == KeyEvent.VK_LEFT &&
				                                     keys.get(5) == KeyEvent.VK_RIGHT &&
				                                     keys.get(6) == KeyEvent.VK_LEFT &&
				                                     keys.get(7) == KeyEvent.VK_RIGHT &&
				                                     keys.get(8) == KeyEvent.VK_B &&
				                                     keys.get(9) == KeyEvent.VK_A)
		                                     .collectList();

		keyboardStream.onNext(KeyEvent.VK_UP);
		keyboardStream.onNext(KeyEvent.VK_UP);
		keyboardStream.onNext(KeyEvent.VK_UP);
		keyboardStream.onNext(KeyEvent.VK_DOWN);
		keyboardStream.onNext(KeyEvent.VK_DOWN);
		keyboardStream.onNext(KeyEvent.VK_LEFT);
		keyboardStream.onNext(KeyEvent.VK_RIGHT);
		keyboardStream.onNext(KeyEvent.VK_LEFT);
		keyboardStream.onNext(KeyEvent.VK_RIGHT);
		keyboardStream.onNext(KeyEvent.VK_B);
		keyboardStream.onNext(KeyEvent.VK_A);
		keyboardStream.onNext(KeyEvent.VK_C);
		keyboardStream.onComplete();

		List<Boolean> res = konamis.block();

		Assert.assertTrue(res.size() == 12);
		Assert.assertFalse(res.get(0));
		Assert.assertTrue(res.get(1));
		Assert.assertFalse(res.get(2));
		Assert.assertFalse(res.get(3));
		Assert.assertFalse(res.get(4));
		Assert.assertFalse(res.get(5));
		Assert.assertFalse(res.get(6));
		Assert.assertFalse(res.get(7));
		Assert.assertFalse(res.get(8));
		Assert.assertFalse(res.get(9));
		Assert.assertFalse(res.get(10));
		Assert.assertFalse(res.get(11));
	}

	@Test
	public void parallelTests() throws InterruptedException {
		parallelMapManyTest("sync", 1_000_000);
		parallelMapManyTest("shared", 1_000_000);
		parallelTest("sync", 1_000_000);
		parallelTest("shared", 1_000_000);
		parallelTest("partitioned", 1_000_000);
		parallelMapManyTest("partitioned", 1_000_000);
		parallelBufferedTimeoutTest(1_000_000);
	}

	private void parallelBufferedTimeoutTest(int iterations) throws InterruptedException {

		System.out.println("Buffered Stream: " + iterations);

		final CountDownLatch latch = new CountDownLatch(iterations);

		EmitterProcessor<String> deferred = EmitterProcessor.create();
		deferred.connect();
		deferred.publishOn(asyncGroup)
		        .parallel(8)
		        .groups()
		        .subscribe(stream -> stream.publishOn(asyncGroup)
		                                 .buffer(1000 / 8, Duration.ofSeconds(1))
		                                 .subscribe(batch -> {
			                                 for (int j = 0; j < batch.size(); j++) {
                                                latch.countDown();
                                            }
		                                 }));

		String[] data = new String[iterations];
		for (int i = 0; i < iterations; i++) {
			data[i] = Integer.toString(i);
		}

		long start = System.currentTimeMillis();

		for (String i : data) {
			deferred.onNext(i);
		}
		if (!latch.await(10, TimeUnit.SECONDS)) {
			throw new RuntimeException(latch.getCount()+ " ");
		}

		long stop = System.currentTimeMillis() - start;
		stop = stop > 0 ? stop : 1;

		System.out.println("Time spent: " + stop + "ms");
		System.out.println("ev/ms: " + iterations / stop);
		System.out.println("ev/s: " + iterations / stop * 1000);
		System.out.println("");
		assertEquals(0, latch.getCount());
	}

	private void parallelTest(String dispatcher, int iterations) throws InterruptedException {

		System.out.println("Dispatcher: " + dispatcher);
		System.out.println("..........:  " + iterations);

		int[] data;
		CountDownLatch latch = new CountDownLatch(iterations);
		EmitterProcessor<Integer> deferred;
		switch (dispatcher) {
			case "partitioned":
				deferred = EmitterProcessor.create();
				deferred.connect();
				deferred.publishOn(asyncGroup)
				        .parallel(2)
				        .groups()
				        .subscribe(stream -> stream.publishOn(asyncGroup)
				                                                    .map(i -> i)
				                                                    .scan(1, (acc, next) -> acc + next)
				                                                    .subscribe(i -> latch.countDown()));

				break;

			default:
				deferred = EmitterProcessor.create();
				deferred.connect();
				deferred.publishOn(asyncGroup)
				        .map(i -> i)
				        .scan(1, (acc, next) -> acc + next)
				        .subscribe(i -> latch.countDown());
		}

		data = new int[iterations];
		for (int i = 0; i < iterations; i++) {
			data[i] = i;
		}

		long start = System.currentTimeMillis();
		for (int i : data) {
			deferred.onNext(i);
		}

		if (!latch.await(15, TimeUnit.SECONDS)) {
			throw new RuntimeException("Count:" + (iterations - latch.getCount()) + " ");
		}

		long stop = System.currentTimeMillis() - start;
		stop = stop > 0 ? stop : 1;

		System.out.println("Time spent: " + stop + "ms");
		System.out.println("ev/ms: " + iterations / stop);
		System.out.println("ev/s: " + iterations / stop * 1000);
		System.out.println("");
		assertEquals(0, latch.getCount());

	}

	private void parallelMapManyTest(String dispatcher, int iterations) throws InterruptedException {

		System.out.println("MM Dispatcher: " + dispatcher);
		System.out.println("..........:  " + iterations);

		int[] data;
		CountDownLatch latch = new CountDownLatch(iterations);
		EmitterProcessor<Integer> mapManydeferred;
		switch (dispatcher) {
			case "partitioned":
				mapManydeferred = EmitterProcessor.create();
				mapManydeferred.parallel(4)
				               .groups()
				               .subscribe(substream -> substream.publishOn(asyncGroup)
				                                              .subscribe(i -> latch.countDown()));
				break;
			default:
				mapManydeferred = EmitterProcessor.create();
				("sync".equals(dispatcher) ? mapManydeferred : mapManydeferred.publishOn(asyncGroup))
				               .flatMap(Flux::just)
				               .subscribe(i -> latch.countDown());
		}
		mapManydeferred.connect();
		data = new int[iterations];
		for (int i = 0; i < iterations; i++) {
			data[i] = i;
		}

		long start = System.currentTimeMillis();

		for (int i : data) {
			mapManydeferred.onNext(i);
		}

		if (!latch.await(20, TimeUnit.SECONDS)) {
			throw new RuntimeException(latch.getCount()+"");
		}
		else {
			System.out.println(latch.getCount());
		}
		assertEquals(0, latch.getCount());

		long stop = System.currentTimeMillis() - start;
		stop = stop > 0 ? stop : 1;

		System.out.println("MM Dispatcher: " + dispatcher);
		System.out.println("Time spent: " + stop + "ms");
		System.out.println("ev/ms: " + iterations / stop);
		System.out.println("ev/s: " + iterations / stop * 1000);
		System.out.println("");
	}

	/**
	 * See https://github.com/reactor/reactor/issues/451
     * @throws Exception for convenience
	 */
	@Test
	public void partitionByHashCodeShouldNeverCreateMoreStreamsThanSpecified() throws Exception {
		Flux<Integer> stream = Flux.range(-10, 20)
		                                 .map(Integer::intValue);

		assertThat(stream.parallel(2)
		                 .groups()
		                 .count()
		                 .block(), is(equalTo(2L)));
	}

	/**
	 * original from @oiavorskyl https://github.com/eventBus/eventBus/issues/358
	 * @throws Exception for convenience
	 */
	//@Test
	public void shouldNotFlushStreamOnTimeoutPrematurelyAndShouldDoItConsistently() throws Exception {
		for (int i = 0; i < 100; i++) {
			shouldNotFlushStreamOnTimeoutPrematurely();
		}
	}

	/**
	 * original from @oiavorskyl https://github.com/eventBus/eventBus/issues/358
     * @throws Exception for convenience
	 */
	@Test
	public void shouldNotFlushStreamOnTimeoutPrematurely() throws Exception {
		final int NUM_MESSAGES = 100000;
		final int BATCH_SIZE = 1000;
		final int TIMEOUT = 100;
		final int PARALLEL_STREAMS = 2;

		/**
		 * Relative tolerance, default to 90% of the batches, in an operative environment, random factors can impact
		 * the fluxion latency, e.g. GC pause if system is under pressure.
		 */
		final double TOLERANCE = 0.9;

		FluxProcessor<Integer, Integer> batchingStreamDef = EmitterProcessor.create();
		batchingStreamDef.connect();

		List<Integer> testDataset = createTestDataset(NUM_MESSAGES);

		final CountDownLatch latch = new CountDownLatch(NUM_MESSAGES);
		Map<Integer, Integer> batchesDistribution = new ConcurrentHashMap<>();
		batchingStreamDef.publishOn(asyncGroup)
		                 .parallel(PARALLEL_STREAMS)
		                 .groups()
		                 .subscribe(substream -> substream.hide().publishOn(asyncGroup)
		                                                .buffer(BATCH_SIZE, Duration.ofMillis(TIMEOUT))
		                                                .subscribe(items -> {
			                                                batchesDistribution.compute(items.size(),
					                                                (key, value) -> value == null ? 1 : value + 1);
			                                                items.forEach(item -> latch.countDown());
		                                                }));

		testDataset.forEach(d -> {
			batchingStreamDef.onNext(d);
		});

		System.out.println(batchesDistribution);

		if (!latch.await(10, TimeUnit.SECONDS)) {
			throw new RuntimeException(latch.getCount() + " ");

		}

		int messagesProcessed = batchesDistribution.entrySet()
		                                           .stream()
		                                           .mapToInt(entry -> entry.getKey() * entry.getValue())
		                                           .reduce(Integer::sum)
		                                           .getAsInt();

		assertEquals(NUM_MESSAGES, messagesProcessed);
		assertTrue("Less than 90% (" + NUM_MESSAGES / BATCH_SIZE * TOLERANCE +
						") of the batches are matching the buffer size: " + batchesDistribution.get(BATCH_SIZE),
				NUM_MESSAGES / BATCH_SIZE * TOLERANCE >= batchesDistribution.get(BATCH_SIZE) * TOLERANCE);
	}

	@Test
	public void prematureFlatMapCompletion() throws Exception {

		long res = Flux.range(0, 1_000_000)
		                  .flatMap(v -> Flux.range(v, 2))
		                  .count()
		                  .block(Duration.ofSeconds(5));

		assertTrue("Latch is " + res, res == 2_000_000);
	}
	@Test
	public void cancelOn() throws Exception {
		CountDownLatch countDownLatch = new CountDownLatch(1);
		AtomicReference<Thread> thread = new AtomicReference<>();
		Cancellation res = Flux.never()
		                  .doOnCancel(() -> {
		                  	thread.set(Thread.currentThread());
			                  countDownLatch.countDown();
		                  })
		                  .cancelOn(asyncGroup)
				.subscribe();
		res.dispose();
		assertTrue(countDownLatch.await(3, TimeUnit.SECONDS));
		assertTrue(thread.get() != Thread.currentThread());
	}

	@Test
	public void sequenceEqual() throws Exception {
		boolean res = Mono.sequenceEqual(Flux.just(1, 2, 3), Flux.just(1, 2, 3))
		                  .block();
		assertTrue(res);

		res = Mono.sequenceEqual(Flux.just(1, 3), Flux.just(1, 2, 3))
		                  .block();
		assertFalse(res);
	}

	@Test
	public void zipOfNull() {
		try {
			Flux<String> as = Flux.just("x");
			Flux<String> bs = Flux.just((String)null);

			assertNull(Flux.zip(as, bs).next().block());
		}
		catch (NullPointerException npe) {
			return;
		}
		assertFalse("Should have failed", true);

	}

	@Test
	public void shouldCorrectlyDispatchComplexFlow() throws InterruptedException {
		EmitterProcessor<Integer> globalFeed = EmitterProcessor.create();
		globalFeed.connect();

		CountDownLatch afterSubscribe = new CountDownLatch(1);
		CountDownLatch latch = new CountDownLatch(4);

		Flux<Integer> s = Flux.just("2222")
		                            .map(Integer::parseInt)
		                            .flatMap(l -> Flux.merge(globalFeed.publishOn(asyncGroup),
				                           Flux.just(1111, l, 3333, 4444, 5555, 6666)).log("merged")
		                                                                                 .publishOn(asyncGroup)
		                                                                                 .log("dispatched")
		                                                                                 .doOnSubscribe(x -> afterSubscribe.countDown())
		                                                                                 .filter(nearbyLoc -> 3333 >= nearbyLoc)
		                                                                                 .filter(nearbyLoc -> 2222 <= nearbyLoc)

		                           );

		/*Cancellation action = */s
		                  .subscribe(integer -> {
			                  latch.countDown();
			                  System.out.println(integer);
		                  }, 1);


		afterSubscribe.await(5, TimeUnit.SECONDS);

		globalFeed.onNext(2223);
		globalFeed.onNext(2224);

		latch.await(5, TimeUnit.SECONDS);
		assertEquals("Must have counted 4 elements", 0, latch.getCount());

	}

	@Test
	public void testParallelAsyncStream2() throws InterruptedException {

		final int numOps = 25;

		CountDownLatch latch = new CountDownLatch(numOps);

		for (int i = 0; i < numOps; i++) {
			final String source = "ASYNC_TEST " + i;

			Flux.just(source)
			    .transform(operationStream -> operationStream.publishOn(asyncGroup)
			                                          .delay(Duration.ofMillis(100))
			                                          .map(s -> s + " MODIFIED")
			                                          .map(s -> {
						                                         latch.countDown();
						                                         return s;
			                                          }))
			    .take(Duration.ofSeconds(2))
			    .log("parallelStream")
			    .subscribe(System.out::println);
		}

		latch.await(15, TimeUnit.SECONDS);
		assertEquals(0, latch.getCount());
	}

	/**
	 * https://gist.github.com/nithril/444d8373ce67f0a8b853 Contribution by Nicolas Labrot
	 * @throws InterruptedException on interrupt
	 */
	@Test
	public void testParallelWithJava8StreamsInput() throws InterruptedException {
		Scheduler supplier = Schedulers.newParallel("test-p", 2);

		int max = ThreadLocalRandom.current()
		                           .nextInt(100, 300);
		CountDownLatch countDownLatch = new CountDownLatch(max);

		Flux<Integer> worker = Flux.range(0, max)
		                                 .publishOn(asyncGroup);
		worker.parallel(2)
		      .runOn(supplier)
		      .map(v -> v)
		      .subscribe(v -> countDownLatch.countDown());

		countDownLatch.await(10, TimeUnit.SECONDS);
		Assert.assertEquals(0, countDownLatch.getCount());
	}

	@Test
	public void testBeyondLongMaxMicroBatching() throws InterruptedException {
		List<Integer> tasks = IntStream.range(0, 1500)
		                               .boxed()
		                               .collect(Collectors.toList());

		CountDownLatch countDownLatch = new CountDownLatch(tasks.size());
		Flux<Integer> worker = Flux.fromIterable(tasks)
		                                 .log("before")
		                                 .publishOn(asyncGroup);

		/*Cancellation tail = */worker.log("after")
		                          .parallel(2)
		                          .groups()
		                          .subscribe(s -> s.log("w"+s.key())
		                                    .publishOn(asyncGroup)
		                                    .map(v -> v)
		                                    .subscribe(v -> countDownLatch.countDown(), Throwable::printStackTrace));

		countDownLatch.await(5, TimeUnit.SECONDS);
		Assert.assertEquals("Count max: "+ tasks.size(), 0, countDownLatch.getCount());
	}

	@Test
	@Ignore
	public void testDiamond() throws InterruptedException, IOException {
		Flux<Point> points = Flux.<Double, Random>generate(Random::new, (r, sub) -> {
			sub.next(r.nextDouble());
			return r;
		}).log("points")
		  .buffer(2)
		  .map(pairs -> new Point(pairs.get(0), pairs.get(1)))
		  .subscribeWith(TopicProcessor.create("tee", 32));

		Flux<InnerSample> innerSamples = points.log("inner-1")
		                                          .filter(Point::isInner)
		                                          .map(InnerSample::new)
		                                          .log("inner-2");

		Flux<OuterSample> outerSamples = points.log("outer-1")
		                                          .filter(p -> !p.isInner())
		                                          .map(OuterSample::new)
		                                          .log("outer-2");

		Flux.merge(innerSamples, outerSamples)
		       .publishOn(asyncGroup)
		       .scan(new SimulationState(0l, 0l), SimulationState::withNextSample)
		       .log("result")
		       .map(s -> System.out.printf("After %8d samples π is approximated as %.5f", s.totalSamples, s.pi()))
		       .take(10000)
		       .subscribe();

		System.in.read();
	}

	private static final class Point {

		final Double x, y;

		public Point(Double x, Double y) {
			this.x = x;
			this.y = y;
		}

		boolean isInner() {
			return x * x + y * y < 1.0;
		}

		@Override
		public String toString() {
			return "Point{" +
					"x=" + x +
					", y=" + y +
					'}';
		}
	}

	private static class Sample {

		final Point point;

		public Sample(Point point) {
			this.point = point;
		}

		@Override
		public String toString() {
			return "Sample{" +
					"point=" + point +
					'}';
		}
	}

	private static final class InnerSample extends Sample {

		public InnerSample(Point point) {
			super(point);
		}
	}

	private static final class OuterSample extends Sample {

		public OuterSample(Point point) {
			super(point);
		}
	}

	private static final class SimulationState {

		final Long totalSamples;
		final Long inCircle;

		public SimulationState(Long totalSamples, Long inCircle) {
			this.totalSamples = totalSamples;
			this.inCircle = inCircle;
		}

		Double pi() {
			return (inCircle.doubleValue() / totalSamples) * 4.0;
		}

		SimulationState withNextSample(Sample sample) {
			return new SimulationState(totalSamples + 1, sample instanceof InnerSample ? inCircle + 1 : inCircle);
		}

		@Override
		public String toString() {
			return "SimulationState{" +
					"totalSamples=" + totalSamples +
					", inCircle=" + inCircle +
					'}';
		}
	}

	@Test
	public void shouldWindowCorrectly() throws InterruptedException {
		Flux<Integer> sensorDataStream = Flux.fromIterable(createTestDataset(1000));

		CountDownLatch endLatch = new CountDownLatch(1000 / 100);

		/*Cancellation controls = */sensorDataStream
				/*     step 2  */.window(100)
				///*     step 3  */.timeout(1000)
				/*     step 4  */
				.subscribe(batchedStream -> {
					System.out.println("New window starting");
					batchedStream
						/*   step 4.1  */.reduce(Integer.MAX_VALUE, Math::min)
						/* ad-hoc step */
						.doOnSuccess(v -> endLatch.countDown())
						/* final step  */
						.subscribe(i -> System.out.println("Minimum " + i));
				});

		endLatch.await(10, TimeUnit.SECONDS);

		Assert.assertEquals(0, endLatch.getCount());
	}

	@Test
	public void shouldCorrectlyDispatchBatchedTimeout() throws InterruptedException {

		long timeout = 100;
		final int batchsize = 4;
		int parallelStreams = 16;
		CountDownLatch latch = new CountDownLatch(1);

		final EmitterProcessor<Integer> streamBatcher = EmitterProcessor.create();
		streamBatcher.connect();
		streamBatcher.publishOn(asyncGroup)
		             .buffer(batchsize, Duration.ofSeconds(timeout))
		             .log("batched")
		             .parallel(parallelStreams)
		             .groups()
		             .log("batched-inner")
		             .subscribe(innerStream -> innerStream.publishOn(asyncGroup)
		                                                .doOnError(Throwable::printStackTrace)
		                                                .subscribe(i -> latch.countDown()));

		streamBatcher.onNext(12);
		streamBatcher.onNext(123);
		streamBatcher.onNext(42);
		streamBatcher.onNext(666);

		boolean finished = latch.await(2, TimeUnit.SECONDS);
		if (!finished) {
			throw new RuntimeException(latch.getCount()+"");
		}
		else {
			assertEquals("Must have correct latch number : " + latch.getCount(), latch.getCount(), 0);
		}
	}

	@Test
	public void mapLotsOfSubAndCancel() throws InterruptedException {
		for (long i = 0; i < 199; i++) {
			mapPassThru();
		}
	}

	public void mapPassThru() throws InterruptedException {
		Flux.just(1)
		       .map(Function.identity());
	}

	@Test
	public void consistentMultithreadingWithPartition() throws InterruptedException {
		Scheduler supplier1 = Schedulers.newParallel("groupByPool", 2);
		Scheduler supplier2 = Schedulers.newParallel("partitionPool", 5);

		CountDownLatch latch = new CountDownLatch(10);

		/*Cancellation c = */Flux.range(1, 10)
		                     .groupBy(n -> n % 2 == 0)
		                     .flatMap(stream -> stream.publishOn(supplier1)
		                                            .log("groupBy-" + stream.key()))
		                     .parallel(5)
		                     .runOn(supplier2)
		                     .sequential()
		                     .publishOn(asyncGroup)
		                     .log("join")
		                     .subscribe(t -> {
			                   latch.countDown();
		                   });


		latch.await(30, TimeUnit.SECONDS);
		assertThat("Not totally dispatched: " + latch.getCount(), latch.getCount() == 0);
		supplier1.shutdown();
		supplier2.shutdown();
	}

	@Test
	public void fluxCreateDemoElasticScheduler() throws Exception {
		final int inputCount = 1000;
		final CountDownLatch latch = new CountDownLatch(inputCount);
		Flux.create(
				sink -> {
					for (int i = 0; i < inputCount; i++) {
						sink.next(i);
					}
					sink.complete();
				}).
				    subscribeOn(Schedulers.newSingle("production")).
				    publishOn(Schedulers.elastic()).
				    subscribe(i -> {
					    LockSupport.parkNanos(100L);
					    latch.countDown();
				    });
		latch.await();
	}

	@Test
	public void subscribeOnDispatchOn() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(100);

		Flux.range(1, 100)
		       .log("testOn")
		       .subscribeOn(ioGroup)
		       .publishOn(asyncGroup)
		       .subscribe(t -> latch.countDown(), 1);

		assertThat("Not totally dispatched", latch.await(30, TimeUnit.SECONDS));
	}
	@Test
	public void unimplementedErrorCallback() throws InterruptedException {

		Flux.error(new Exception("forced"))
		       .log("error")
		       .subscribe();

		try{
			Flux.error(new Exception("forced"))
			    .subscribe();
		}
		catch(Exception e){
			return;
		}
		fail();
	}

	@Test
	public void delayEach() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(3);

		Flux.range(1, 3)
		       .delayMillis(1000)
		       .log("delay")
		       .subscribe(t -> latch.countDown());

		assertThat("Not totally dispatched", latch.await(30, TimeUnit.SECONDS));
	}

	// Test issue https://github.com/reactor/reactor/issues/474
// code by @masterav10
	@Test
	public void combineWithOneElement() throws InterruptedException, TimeoutException {
		AtomicReference<Object> ref = new AtomicReference<>(null);

		Phaser phaser = new Phaser(2);

		Flux<Object> s1 = ReplayProcessor.cacheLastOrDefault(new Object())
		                                 .connect()
		                                 .publishOn(asyncGroup);
		Flux<Object> s2 = ReplayProcessor.cacheLastOrDefault(new Object())
		                                  .connect()
		                                .publishOn(asyncGroup);

		// The following works:
		//List<Flux<Object>> list = Arrays.collectList(s1);
		// The following fails:
		List<Flux<Object>> list = Arrays.asList(s1, s2);

		Flux.combineLatest(list, t -> t)
		       .log()
		       .doOnNext(obj -> {
			       ref.set(obj);
			       phaser.arrive();
		       })
		       .subscribe();

		phaser.awaitAdvanceInterruptibly(phaser.arrive(), 1, TimeUnit.SECONDS);
		Assert.assertNotNull(ref.get());
	}

	/**
	 * This test case demonstrates a silent failure of {@link Flux#intervalMillis(long)}
	 * when a resolution is specified that
	 * is less than the backing {@link Timer} class.
	 *
	 * @throws InterruptedException - on failure.
	 * @throws TimeoutException     - on failure. <p> by @masterav10 : https://github.com/reactor/reactor/issues/469
	 */
	@Test
	@Ignore
	public void endLessTimer() throws InterruptedException, TimeoutException {
		int tasks = 50;
		long delayMS = 50; // XXX: Fails when less than 100
		Phaser barrier = new Phaser(tasks + 1);

		List<Long> times = new ArrayList<>();

		// long localTime = System.currentTimeMillis(); for java 7
		long localTime = Instant.now()
		                        .toEpochMilli();
		long elapsed = System.nanoTime();

		Cancellation ctrl = Flux.interval(Duration.ofMillis(delayMS))
		                        .log("test")
		                                            .map((signal) -> {
			                      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - elapsed);
		                      })
		                                            .doOnNext((elapsedMillis) -> {
			                      times.add(localTime + elapsedMillis);
			                      barrier.arrive();
		                      })
		                                            .subscribe();

		barrier.awaitAdvanceInterruptibly(barrier.arrive(), tasks * delayMS + 1000, TimeUnit.MILLISECONDS);
		ctrl.dispose();

		Assert.assertEquals(tasks, times.size());

		for (int i = 1; i < times.size(); i++) {
			Long prev = times.get(i - 1);
			Long time = times.get(i);

			Assert.assertTrue(prev > 0);
			Assert.assertTrue(time > 0);
			Assert.assertTrue("was " + (time - prev), time - prev <= delayMS * 1.2);
		}
	}

	private List<Integer> createTestDataset(int i) {
		List<Integer> list = new ArrayList<>(i);
		for (int k = 0; k < i; k++) {
			list.add(k);
		}
		return list;
	}

	class FooSubscriber implements Subscriber<String> {

		private final Logger log = Loggers.getLogger(getClass());

		private Subscription subscription;

		@Override
		public void onSubscribe(Subscription subscription) {
			if (null != this.subscription) {
				subscription.cancel();
				return;
			}
			this.subscription = subscription;
			this.subscription.request(1);
		}

		@Override
		public void onNext(String s) {
			if (s.startsWith("GOODBYE")) {
				log.info("This is the end");
			}
			subscription.request(1);
		}

		@Override
		public void onError(Throwable throwable) {
			log.error(throwable.getMessage(), throwable);
		}

		@Override
		public void onComplete() {
			log.info("fluxion complete");
		}

	}

	/**
	 * Should work with {@link Processor} but it doesn't.
     * @throws Exception for convenience
	 */
	//@Test
	public void multiplexUsingProcessors1000() throws Exception {
		for (int i = 0; i < 1000; i++) {
			System.out.println("new test " + i);
			multiplexUsingProcessors();
			System.out.println();
		}
	}

	@Test(timeout = TIMEOUT)
	public void multiplexUsingProcessors() throws Exception {

		final Flux<Integer> forkStream = Flux.just(1, 2, 3)
		                                           .log("begin-computation");
		final Flux<Integer> forkStream2 = Flux.just(1, 2, 3)
		                                            .log("begin-persistence");

		final TopicProcessor<Integer> computationEmitterProcessor = TopicProcessor.create("computation", BACKLOG);
		final Flux<String> computationStream = computationEmitterProcessor
		                                                 .map(i -> Integer.toString(i));

		final TopicProcessor<Integer> persistenceEmitterProcessor = TopicProcessor.create("persistence", BACKLOG);
		final Flux<String> persistenceStream = persistenceEmitterProcessor
		                                                 .map(i -> "done " + i);

		forkStream.subscribe(computationEmitterProcessor);
		forkStream2.subscribe(persistenceEmitterProcessor);

		final Semaphore doneSemaphore = new Semaphore(0);

		final Flux<List<String>> joinStream =
				Flux.zip(computationStream.log("log1"), persistenceStream.log("log2"), (a, b) -> Arrays.asList(a,b));

		// Method chaining doesn't compile.
		joinStream.log("log-final")
		          .subscribe(list -> println("Joined: ", list), t -> println("Join failed: ", t.getMessage()), () -> {
			          println("Join complete.");
			          doneSemaphore.release();
		          });

		doneSemaphore.acquire();

	}

	/**
	 * <pre>
	 *                 forkStream
	 *                 /        \      < - - - int
	 *                v          v
	 * persistenceStream        computationStream
	 *                 \        /      < - - - List< String >
	 *                  v      v
	 *                 joinStream      < - - - String
	 *                 splitStream
	 *             observedSplitStream
	 * </pre>
     * @throws Exception for convenience
	 */
	@Test(timeout = TIMEOUT)
	public void multiplexUsingDispatchersAndSplit() throws Exception {

		final EmitterProcessor<Integer> forkEmitterProcessor = EmitterProcessor.create();
		forkEmitterProcessor.connect();

		final EmitterProcessor<Integer> computationEmitterProcessor = EmitterProcessor.create(false);

		Scheduler computation = Schedulers.newSingle("computation");
		Scheduler persistence = Schedulers.newSingle("persistence");
		Scheduler forkJoin = Schedulers.newParallel("forkJoin", 2);

		final Flux<List<String>> computationStream =
				computationEmitterProcessor.publishOn(computation)
				                      .map(i -> {
					                      final List<String> list = new ArrayList<>(i);
					                      for (int j = 0; j < i; j++) {
						                      list.add("i" + j);
					                      }
					                      return list;
				                      })
				                      .doOnNext(ls -> println("Computed: ", ls))
				                      .log("computation");

		final EmitterProcessor<Integer> persistenceEmitterProcessor = EmitterProcessor.create(false);

		final Flux<List<String>> persistenceStream =
				persistenceEmitterProcessor.publishOn(persistence)
				                      .doOnNext(i -> println("Persisted: ", i))
				                      .map(i -> Collections.singletonList("done" + i))
				                      .log("persistence");

		Flux<Integer> forkStream = forkEmitterProcessor.publishOn(forkJoin)
		                                             .log("fork");

		forkStream.subscribe(computationEmitterProcessor);
		forkStream.subscribe(persistenceEmitterProcessor);

		final Flux<List<String>> joinStream = Flux.zip(computationStream, persistenceStream, (a, b) -> Arrays.asList(a, b))
		                                                .publishOn(forkJoin)
		                                                .map(listOfLists -> {
			                                               listOfLists.get(0)
			                                                          .addAll(listOfLists.get(1));
			                                               return listOfLists.get(0);
		                                               })
		                                                .log("join");

		final Semaphore doneSemaphore = new Semaphore(0);

		final Mono<List<String>> listPromise = joinStream.flatMap(Flux::fromIterable)
		                                                 .log("resultStream")
		                                                 .collectList()
		                                                 .doOnTerminate((v, e) -> doneSemaphore.release())
		                                                 .subscribe();

		forkEmitterProcessor.onNext(1);
		forkEmitterProcessor.onNext(2);
		forkEmitterProcessor.onNext(3);
		forkEmitterProcessor.onComplete();

		List<String> res = listPromise.block(Duration.ofSeconds(5));
		assertEquals(Arrays.asList("i0", "done1", "i0", "i1", "done2", "i0", "i1", "i2", "done3"), res);

		forkJoin.shutdown();
		persistence.shutdown();
		computation.shutdown();
	}

	@Test
	@Ignore
	public void splitBugEventuallyHappens() throws Exception {
		int successCount = 0;
		try {
			for (; ; ) {
				multiplexUsingDispatchersAndSplit();
				println("**** Success! ****");
				successCount++;
			}
		}
		finally {
			println("Succeeded " + successCount + " time" + (successCount <= 1 ? "." : "s."));
		}

	}

	@Test
	public void testBufferPredicateUntilIncludesBoundaryLast() {
		String[] colorSeparated = new String[]{"red", "green", "blue", "#", "green", "green", "#", "blue", "cyan"};

		Flux<List<String>> colors = Flux
				.fromArray(colorSeparated)
				.bufferUntil(val -> val.equals("#"))
				.log();

		StepVerifier.create(colors)
		            .consumeNextWith(l1 -> Assert.assertThat(l1, contains("red", "green", "blue", "#")))
		            .consumeNextWith(l2 -> Assert.assertThat(l2, contains("green", "green", "#")))
		            .consumeNextWith(l3 -> Assert.assertThat(l3, contains("blue", "cyan")))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void testBufferPredicateUntilCutBeforeIncludesBoundaryFirst() {
		String[] colorSeparated = new String[]{"red", "green", "blue", "#", "green", "green", "#", "blue", "cyan"};

		Flux<List<String>> colors = Flux
				.fromArray(colorSeparated)
				.bufferUntil(val -> val.equals("#"), true)
				.log();

		StepVerifier.create(colors)
		            .thenRequest(1)
		            .consumeNextWith(l1 -> Assert.assertThat(l1, contains("red", "green", "blue")))
		            .consumeNextWith(l2 -> Assert.assertThat(l2, contains("#", "green", "green")))
		            .consumeNextWith(l3 -> Assert.assertThat(l3, contains("#", "blue", "cyan")))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void testBufferPredicateWhileDoesntIncludeBoundary() {
		String[] colorSeparated = new String[]{"red", "green", "blue", "#", "green", "green", "#", "blue", "cyan"};

		Flux<List<String>> colors = Flux
				.fromArray(colorSeparated)
				.bufferWhile(val -> !val.equals("#"))
				.log();

		StepVerifier.create(colors)
		            .consumeNextWith(l1 -> Assert.assertThat(l1, contains("red", "green", "blue")))
		            .consumeNextWith(l2 -> Assert.assertThat(l2, contains("green", "green")))
		            .consumeNextWith(l3 -> Assert.assertThat(l3, contains("blue", "cyan")))
		            .expectComplete()
		            .verify();
	}

	private static final long TIMEOUT = 10_000;

	// Setting it to 1 doesn't help.
	private static final int BACKLOG = 1024;

	private static void println(final Object... fragments) {
		final Thread currentThread = Thread.currentThread();
		synchronized (System.out) {
			System.out.print(String.format("[%s] ", currentThread.getName()));
			for (final Object fragment : fragments) {
				System.out.print(fragment);
			}
			System.out.println();
		}
	}
}
