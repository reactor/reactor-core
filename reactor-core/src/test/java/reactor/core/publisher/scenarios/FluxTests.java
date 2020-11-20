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

package reactor.core.publisher.scenarios;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.Callable;
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
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.AutoDisposingExtension;
import reactor.test.util.LoggerUtils;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.TestLogger;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxTests extends AbstractReactorTest {

	static final Logger LOG = Loggers.getLogger(FluxTests.class);

	static final String2Integer STRING_2_INTEGER = new String2Integer();

	@RegisterExtension
	AutoDisposingExtension afterTest = new AutoDisposingExtension();

	@Test
	public void discardLocalMultipleFilters() {
		AtomicInteger discardNumberCount = new AtomicInteger();
		AtomicInteger discardStringCount = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 12)
		                        .hide() //hide both avoid the fuseable AND tryOnNext usage
		                        .filter(i -> i % 2 == 0)
		                        .map(String::valueOf)
		                        .filter(s -> s.length() < 2)
		                        .doOnDiscard(Number.class, i -> discardNumberCount.incrementAndGet())
		                        .doOnDiscard(String.class, i -> discardStringCount.incrementAndGet())
		)
		            .expectNext("2", "4", "6", "8")
		            .expectComplete()
		            .verify();

		assertThat(discardNumberCount).hasValue(6); //1 3 5 7 9 11
		assertThat(discardStringCount).hasValue(2); //10 12
	}

	@Test
	public void discardLocalOrder() {
		List<String> discardOrder = Collections.synchronizedList(new ArrayList<>(2));

		StepVerifier.create(Flux.range(1, 2)
		                        .hide() //hide both avoid the fuseable AND tryOnNext usage
		                        .filter(i -> i % 2 == 0)
		                        .doOnDiscard(Number.class, i -> discardOrder.add("FIRST"))
		                        .doOnDiscard(Integer.class, i -> discardOrder.add("SECOND"))
		)
		            .expectNext(2)
		            .expectComplete()
		            .verify();

		assertThat(discardOrder).containsExactly("FIRST", "SECOND");
	}

	@Test
	public void delayErrorConcatMapVsFlatMap() {
		Function<Integer, Flux<String>> mapFunction = i -> {
			char c = (char) ('A' + i);
			return Flux.range(1, i + 1)
			           .doOnNext(v -> {
			           	if (i == 3 && v == 3) {
			           		throw new IllegalStateException("boom " + c + v);
			            }
			           })
			    .map(v -> "" + c + "" + v);
		};

		Flux<Integer> source = Flux.range(0, 5);

		Flux<String> concatMap = source.concatMapDelayError(mapFunction)
		                               .materialize()
				                       .map(Object::toString);
		Flux<String> flatMap = source.flatMapDelayError(mapFunction, 2, 32)
		                               .materialize()
				                       .map(Object::toString);

		List<String> signalsConcat = concatMap.collectList().block();
		List<String> signalsFlat = flatMap.collectList().block();

		assertThat(signalsConcat)
		          .containsExactlyElementsOf(signalsFlat);
	}

	@Test
	public void delayErrorConcatMapVsFlatMapTwoErrors() {
		Function<Integer, Flux<String>> mapFunction = i -> {
			char c = (char) ('A' + i);
			return Flux.range(1, i + 1)
			           .doOnNext(v -> {
			           	if ((i == 3 || i == 2) && v == 2) {
			           		throw new IllegalStateException("boom " + c + v);
			            }
			           })
			    .map(v -> "" + c + "" + v);
		};

		List<Throwable> concatSuppressed = new ArrayList<>();
		List<Throwable> flatSuppressed = new ArrayList<>();

		Flux<Integer> source = Flux.range(0, 5);

		Flux<String> concatMap = source.concatMapDelayError(mapFunction)
		                               .doOnError(t -> concatSuppressed.addAll(
		                               		Arrays.asList(t.getSuppressed())))
		                               .materialize()
		                               .map(Object::toString);
		Flux<String> flatMap = source.flatMapDelayError(mapFunction, 2, 32)
		                             .doOnError(t -> flatSuppressed.addAll(
		                             		Arrays.asList(t.getSuppressed())))
		                             .materialize()
		                             .map(Object::toString);

		List<String> signalsConcat = concatMap.collectList().block();
		List<String> signalsFlat = flatMap.collectList().block();

		assertThat(signalsConcat)
		          .containsExactlyElementsOf(signalsFlat);

		List<String> flatSuppressedMessages = flatSuppressed
				.stream()
				.map(Throwable::getMessage)
				.collect(Collectors.toList());
		assertThat(concatSuppressed)
		          .extracting(Throwable::getMessage)
		          .containsExactlyElementsOf(flatSuppressedMessages);
	}

	@Test
	public void testDoOnEachSignal() {
		List<Signal<Integer>> signals = new ArrayList<>(4);
		List<Integer> values = new ArrayList<>(2);
		Flux<Integer> flux = Flux.just(1, 2)
		                         .doOnEach(signals::add)
		                         .doOnEach(s -> {
		                         	if (s.isOnNext())
		                         		values.add(s.get());
		                         });
		StepVerifier.create(flux)
		            .expectSubscription()
		            .expectNext(1, 2)
		            .expectComplete()
		            .verify();

		assertThat(signals).hasSize(3);
		assertThat(signals.get(0).get()).withFailMessage("onNext signal are not reused"). isEqualTo(2);
		assertThat(signals.get(1).get()).withFailMessage("onNext signal isn't last value").isEqualTo(2);
		assertThat(signals.get(2).isOnComplete()).as("last signal isOnComplete()?").isTrue();
		assertThat(values.get(0)).as("1st onNext value").isEqualTo(1);
		assertThat(values.get(1)).as("2nd onNext value").isEqualTo(2);
	}

	@Test
	public void testDoOnEachSignalSingleNextInstance() {
		Set<Signal<Integer>> signals = Collections.newSetFromMap(
				new IdentityHashMap<>(2));
		Flux<Integer> flux = Flux.range(1, 1_000)
		                         .doOnEach(signals::add);
		StepVerifier.create(flux)
		            .expectSubscription()
		            .expectNextCount(997)
		            .expectNext(998, 999, 1_000)
		            .expectComplete()
		            .verify();

		assertThat(signals).hasSize(2);

		int nextValue = 0;
		boolean foundComplete = false;
		boolean foundOther = false;
		for (Signal<Integer> signal : signals) {
			if (signal.isOnComplete()) foundComplete = true;
			else if (signal.isOnNext()) nextValue = signal.get();
			else foundOther = true;
		}

		assertThat(nextValue).isEqualTo(1000);
		assertThat(foundComplete).as("foundComplete").isTrue();
		assertThat(foundOther).as("foundOther").isFalse();
	}

	@Test
	public void testDoOnEachSignalWithError() {
		List<Signal<Integer>> signals = new ArrayList<>(4);
		Flux<Integer> flux = Flux.<Integer>error(new IllegalArgumentException("foo"))
		                         .doOnEach(signals::add);
		StepVerifier.create(flux)
		            .expectSubscription()
		            .expectErrorMessage("foo")
		            .verify();

		assertThat(signals).hasSize(1);
		assertThat(signals.get(0).isOnError()).as("first signal isOnError?").isTrue();
		assertThat(signals.get(0).getThrowable()).as("first signal throwable").hasMessage("foo");
	}

	@Test
	public void testDoOnEachSignalNullConsumer() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.just(1).doOnEach(null);
		});
	}

	@Test
	public void testDoOnEachSignalToSubscriber() {
		AssertSubscriber<Integer> peekSubscriber = AssertSubscriber.create();
		Flux<Integer> flux = Flux.just(1, 2)
		                         .doOnEach(s -> s.accept(peekSubscriber));
		StepVerifier.create(flux)
		            .expectSubscription()
		            .expectNext(1, 2)
		            .expectComplete()
		            .verify();

		peekSubscriber.assertNotSubscribed();
		peekSubscriber.assertValues(1, 2);
		peekSubscriber.assertComplete();
	}

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

		assertThat(await(s)).isEqualTo("Goodbye then!");
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
		assertThat(await(5, s)).isEqualTo(15);
	}

	@Test
	public void testComposeFromMultipleFilteredValues() throws InterruptedException {
		Flux<String> stream = Flux.just("1", "2", "3", "4", "5");
		Flux<Integer> s = stream.map(STRING_2_INTEGER)
		                           .filter(i -> i % 2 == 0);

		assertThat(await(2, s)).isEqualTo(4);
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
					                          throw new IllegalArgumentException("expected");
				                          }
				                          sum += i;
				                          return sum;
			                          }
		                          })
		                           .doOnError(IllegalArgumentException.class, e -> exception.set(true));

		assertThat(await(5, s)).isEqualTo(10);
		assertThat(exception).as("error triggered").isTrue();
	}

	@Test
	public void testReduce() throws InterruptedException {
		Flux<String> stream = Flux.just("1", "2", "3", "4", "5");
		Mono<Integer> s = stream.map(STRING_2_INTEGER)
		                          .reduce(1, (acc, next) -> acc * next);
		assertThat(await(1, s)).isEqualTo(120);
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
		assertThat(await(1, s)).isEqualTo(120);
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

		assertThat(batchCount).hasValue(1);
		assertThat(count).hasValue(15);
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

		assertThat(await(2, s)).isEqualTo(3);
		assertThat(latch.getCount()).as("error handler latch count").isEqualTo(0L);
	}

	@Test
	public void promiseAcceptCountCannotExceedOne() {
		Sinks.One<Object> deferred = Sinks.one();
		deferred.emitValue("alpha", FAIL_FAST);
		try {
			deferred.emitValue("bravo", FAIL_FAST);
		}
		catch (Exception e) {
			if(!Exceptions.isCancel(e)) {
				throw e;
			}
		}
		assertThat(deferred.asMono().block()).isEqualTo("alpha");
	}

	@Test
	public void promiseErrorCountCannotExceedOne() {
		Sinks.One<Object> deferred = Sinks.one();
		Throwable error = new IOException("foo");

		StepVerifier.create(deferred.asMono())
		            .then(() -> {
			            deferred.emitError(error, FAIL_FAST);
			            deferred.emitValue(error, FAIL_FAST);
		            })
		            .expectErrorMessage("foo")
		            .verifyThenAssertThat()
		            .hasDroppedExactly(error);

		assertThat(Scannable.from(deferred).scan(Scannable.Attr.ERROR)).isSameAs(error);
	}

	@Test
	public void promiseAcceptCountAndErrorCountCannotExceedOneInTotal() {
		Sinks.One<Object> deferred = Sinks.one();
		Throwable error = new IOException("foo");

		StepVerifier.create(deferred.asMono())
		            .then(() -> {
			            deferred.emitError(error, FAIL_FAST);
			            deferred.emitValue("alpha", FAIL_FAST);
		            })
		            .expectErrorMessage("foo")
		            .verifyThenAssertThat()
		            .hasDroppedExactly("alpha");

		assertThat(Scannable.from(deferred).scan(Scannable.Attr.ERROR)).isSameAs(error);
	}

	private <T> T await(Flux<T> s) throws InterruptedException {
		return await(1, s);
	}

	private <T> T await(int count, final Publisher<T> s) {
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

		assertThat(duration).isLessThan(2000L);
		return result;
	}

	static class String2Integer implements Function<String, Integer> {

		@Override
		public Integer apply(String s) {
			return Integer.parseInt(s);
		}
	}

	@Test
	public void analyticsTest() throws Exception {
		Sinks.Many<Integer> source = Sinks.many().replay().all();

		long avgTime = 50l;

		Mono<Long> result = source
				.asFlux()
				.log("delay")
				.publishOn(asyncGroup)
		                          .delayElements(Duration.ofMillis(avgTime))

		                          .elapsed()
		                          .skip(1)
		                          .groupBy(w -> w.getT1())
								  .flatMap(w -> w.count().map(c -> Tuples.of(w.key(), c)))
		                          .log("elapsed")
		                          .collectSortedList(Comparator.comparing(Tuple2::getT1))
		                          .flatMapMany(Flux::fromIterable)
		                          .reduce(-1L, (acc, next) -> acc > 0l ? ((next.getT1() + acc) / 2) : next.getT1())
		                          .log("reduced-elapsed")
		                          .cache();

		source.asFlux()
			  .subscribe();

		for (int j = 0; j < 10; j++) {
			source.emitNext(1, FAIL_FAST);
		}
		source.emitComplete(FAIL_FAST);

		assertThat(result.block(Duration.ofSeconds(5))).isGreaterThanOrEqualTo((long)(avgTime * 0.6));
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

		Sinks.Many<String> deferred = Sinks.many().multicast().onBackpressureBuffer();
		deferred.asFlux()
				.publishOn(asyncGroup)
		        .parallel(8)
		        .groups()
		        .subscribe(stream -> stream.publishOn(asyncGroup)
		                                 .bufferTimeout(1000 / 8, Duration.ofSeconds(1))
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
			long busyLoops = 0;
			while (deferred.tryEmitNext(i).isFailure()) {
				busyLoops++;
				if (busyLoops % 5000 == 0 && System.currentTimeMillis() - start >= 10_0000) {
					throw new RuntimeException("Busy loop timed out");
				}
			}
		}
		if (!latch.await(10, TimeUnit.SECONDS)) {
			throw new RuntimeException(latch.getCount()+ " ");
		}

		long stop = System.currentTimeMillis() - start;
		stop = stop > 0 ? stop : 1;

		System.out.println("Time spent: " + stop + "ms");
		System.out.println("ev/ms: " + iterations / stop);
		System.out.println("ev/s: " + iterations / stop * 1000);
		System.out.println();
		assertThat(latch.getCount()).isEqualTo(0);
	}

	private void parallelTest(String dispatcher, int iterations) throws InterruptedException {

		System.out.println("Dispatcher: " + dispatcher);
		System.out.println("..........:  " + iterations);

		int[] data;
		CountDownLatch latch = new CountDownLatch(iterations);
		Sinks.Many<Integer> deferred;
		switch (dispatcher) {
			case "partitioned":
				deferred = Sinks.many().multicast().onBackpressureBuffer();
				deferred.asFlux()
						.publishOn(asyncGroup)
				        .parallel(2)
				        .groups()
				        .subscribe(stream -> stream.publishOn(asyncGroup)
				                                                    .map(i -> i)
				                                                    .scan(1, (acc, next) -> acc + next)
				                                                    .subscribe(i -> latch.countDown()));

				break;

			default:
				deferred = Sinks.many().multicast().onBackpressureBuffer();
				deferred.asFlux()
						.publishOn(asyncGroup)
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
			long busyLoops = 0;
			while (deferred.tryEmitNext(i).isFailure()) {
				busyLoops++;
				if (busyLoops % 5000 == 0 && System.currentTimeMillis() - start >= 10_0000) {
					throw new RuntimeException("Busy loop timed out");
				}
			}
		}

		if (!latch.await(15, TimeUnit.SECONDS)) {
			throw new RuntimeException("Count:" + (iterations - latch.getCount()) + " ");
		}

		long stop = System.currentTimeMillis() - start;
		stop = stop > 0 ? stop : 1;

		System.out.println("Time spent: " + stop + "ms");
		System.out.println("ev/ms: " + iterations / stop);
		System.out.println("ev/s: " + iterations / stop * 1000);
		System.out.println();
		assertThat(latch.getCount()).isEqualTo(0);

	}

	private void parallelMapManyTest(String dispatcher, int iterations) throws InterruptedException {

		System.out.println("MM Dispatcher: " + dispatcher);
		System.out.println("..........:  " + iterations);

		int[] data;
		CountDownLatch latch = new CountDownLatch(iterations);
		Sinks.Many<Integer> mapManydeferred;
		switch (dispatcher) {
			case "partitioned":
				mapManydeferred = Sinks.many().multicast().onBackpressureBuffer();
				mapManydeferred.asFlux()
							   .parallel(4)
				               .groups()
				               .subscribe(substream -> substream.publishOn(asyncGroup)
				                                              .subscribe(i -> latch.countDown()));
				break;
			default:
				mapManydeferred = Sinks.many().multicast().onBackpressureBuffer();
				("sync".equals(dispatcher) ? mapManydeferred.asFlux() : mapManydeferred.asFlux().publishOn(asyncGroup))
				               .flatMap(Flux::just)
				               .subscribe(i -> latch.countDown());
		}
		data = new int[iterations];
		for (int i = 0; i < iterations; i++) {
			data[i] = i;
		}

		long start = System.currentTimeMillis();

		for (int i : data) {
			long busyLoops = 0;
			while (mapManydeferred.tryEmitNext(i).isFailure()) {
				busyLoops++;
				if (busyLoops % 5000 == 0 && System.currentTimeMillis() - start >= 10_0000) {
					throw new RuntimeException("Busy loop timed out");
				}
			}
		}

		if (!latch.await(20, TimeUnit.SECONDS)) {
			throw new RuntimeException(latch.getCount()+"");
		}
		else {
			System.out.println(latch.getCount());
		}
		assertThat(latch.getCount()).isEqualTo(0);

		long stop = System.currentTimeMillis() - start;
		stop = stop > 0 ? stop : 1;

		System.out.println("MM Dispatcher: " + dispatcher);
		System.out.println("Time spent: " + stop + "ms");
		System.out.println("ev/ms: " + iterations / stop);
		System.out.println("ev/s: " + iterations / stop * 1000);
		System.out.println();
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
		                 .block()).isEqualTo(2);
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

		Sinks.Many<Integer> batchingStreamDef = Sinks.many().multicast().onBackpressureBuffer();

		List<Integer> testDataset = createTestDataset(NUM_MESSAGES);

		final CountDownLatch latch = new CountDownLatch(NUM_MESSAGES);
		Map<Integer, Integer> batchesDistribution = new ConcurrentHashMap<>();
		batchingStreamDef.asFlux()
						 .publishOn(asyncGroup)
		                 .parallel(PARALLEL_STREAMS)
		                 .groups()
		                 .subscribe(substream -> substream.hide().publishOn(asyncGroup)
		                                                .bufferTimeout(BATCH_SIZE, Duration.ofMillis(TIMEOUT))
		                                                .subscribe(items -> {
			                                                batchesDistribution.compute(items.size(),
					                                                (key, value) -> value == null ? 1 : value + 1);
			                                                items.forEach(item -> latch.countDown());
		                                                }));

		final long start = System.currentTimeMillis();
		testDataset.forEach(data -> {
			long busyLoops = 0;
			while (batchingStreamDef.tryEmitNext(data).isFailure()) {
				busyLoops++;
				if (busyLoops % 5000 == 0 && System.currentTimeMillis() - start >= 10_0000) {
					throw new RuntimeException("Busy loop timed out");
				}
			}
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

		assertThat(messagesProcessed).isEqualTo(NUM_MESSAGES);
		assertThat(NUM_MESSAGES / BATCH_SIZE * TOLERANCE)
				.as("Less than 90% (%d) of the batches are matching the buffer size: %d", NUM_MESSAGES / BATCH_SIZE * TOLERANCE, batchesDistribution.get(BATCH_SIZE))
				.isGreaterThanOrEqualTo(batchesDistribution.get(BATCH_SIZE) * TOLERANCE);
	}

	@Test
	public void prematureFlatMapCompletion() throws Exception {

		long res = Flux.range(0, 1_000_000)
		                  .flatMap(v -> Flux.range(v, 2))
		                  .count()
		                  .block(Duration.ofSeconds(5));

		assertThat(res).as("latch value").isEqualTo(2_000_000);
	}
	@Test
	public void cancelOn() throws Exception {
		CountDownLatch countDownLatch = new CountDownLatch(1);
		AtomicReference<Thread> thread = new AtomicReference<>();
		Disposable res = Flux.never()
		                     .doOnCancel(() -> {
		                  	thread.set(Thread.currentThread());
			                  countDownLatch.countDown();
		                  })
		                     .cancelOn(asyncGroup)
		                     .subscribe();
		res.dispose();
		assertThat(countDownLatch.await(3, TimeUnit.SECONDS)).isTrue();
		assertThat(thread.get()).isNotSameAs(Thread.currentThread());
	}

	@Test
	public void sequenceEqual() throws Exception {
		boolean res = Mono.sequenceEqual(Flux.just(1, 2, 3), Flux.just(1, 2, 3))
		                  .block();
		assertThat(res).isTrue();

		res = Mono.sequenceEqual(Flux.just(1, 3), Flux.just(1, 2, 3))
		                  .block();
		assertThat(res).isFalse();
	}

	@Test
	public void zipOfNull() {
		try {
			Flux<String> as = Flux.just("x");
			Flux<String> bs = Flux.just((String)null);

			assertThat(Flux.zip(as, bs).next().block()).isNull();
			fail("Exception expected");
		}
		catch (NullPointerException npe) {
			return;
		}

	}

	@Test
	public void shouldCorrectlyDispatchComplexFlow() throws InterruptedException {
		Sinks.Many<Integer> globalFeed = Sinks.many().multicast().onBackpressureBuffer();

		CountDownLatch afterSubscribe = new CountDownLatch(1);
		CountDownLatch latch = new CountDownLatch(4);

		Flux<Integer> s = Flux.just("2222")
		                            .map(Integer::parseInt)
		                            .flatMap(l -> Flux.merge(globalFeed.asFlux().publishOn(asyncGroup),
				                           Flux.just(1111, l, 3333, 4444, 5555, 6666)).log("merged")
		                                                                                 .publishOn(asyncGroup)
		                                                                                 .log("dispatched")
		                                                                                 .doOnSubscribe(x -> afterSubscribe.countDown())
		                                                                                 .filter(nearbyLoc -> 3333 >= nearbyLoc)
		                                                                                 .filter(nearbyLoc -> 2222 <= nearbyLoc)

		                           );

		/*Disposable action = */s
							.limitRate(1)
		                  .subscribe(integer -> {
			                  latch.countDown();
			                  System.out.println(integer);
		                  });


		afterSubscribe.await(5, TimeUnit.SECONDS);

		globalFeed.emitNext(2223, FAIL_FAST);
		globalFeed.emitNext(2224, FAIL_FAST);

		latch.await(5, TimeUnit.SECONDS);
		assertThat(latch.getCount()).as("latch count").isEqualTo(0);

	}

	@Test
	public void testParallelAsyncStream2() throws InterruptedException {

		final int numOps = 25;

		CountDownLatch latch = new CountDownLatch(numOps);

		for (int i = 0; i < numOps; i++) {
			final String source = "ASYNC_TEST " + i;

			Flux.just(source)
			    .transform(operationStream -> operationStream.publishOn(asyncGroup)
			                                          .delayElements(Duration.ofMillis(100))
			                                          .map(s -> s + " MODIFIED")
			                                          .map(s -> {
						                                         latch.countDown();
						                                         return s;
			                                          }))
			    .take(Duration.ofSeconds(2))
			    .log("parallelStream", Level.FINE)
			    .subscribe(LOG::debug);
		}

		latch.await(15, TimeUnit.SECONDS);
		assertThat(latch.getCount()).isEqualTo(0);
	}

	/**
	 * https://gist.github.com/nithril/444d8373ce67f0a8b853 Contribution by Nicolas Labrot
	 * @throws InterruptedException on interrupt
	 */
	@Test
	public void testParallelWithJava8StreamsInput() throws InterruptedException {
		Scheduler supplier = afterTest.autoDispose(Schedulers.newParallel("test-p", 2));

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
		assertThat(countDownLatch.getCount()).isEqualTo(0);
	}

	@Test
	public void testBeyondLongMaxMicroBatching() throws InterruptedException {
		List<Integer> tasks = IntStream.range(0, 1500)
		                               .boxed()
		                               .collect(Collectors.toList());

		CountDownLatch countDownLatch = new CountDownLatch(tasks.size());
		Flux<Integer> worker = Flux.fromIterable(tasks)
		                                 .log("before", Level.FINE)
		                                 .publishOn(asyncGroup);

		/*Disposable tail = */worker.log("after", Level.FINE)
		                          .parallel(2)
		                          .groups()
		                          .subscribe(s -> s.log("w"+s.key(), Level.FINE)
		                                    .publishOn(asyncGroup)
		                                    .map(v -> v)
		                                    .subscribe(v -> countDownLatch.countDown(), Throwable::printStackTrace));

		countDownLatch.await(5, TimeUnit.SECONDS);
		assertThat(countDownLatch.getCount()).as("Count max: %d", tasks.size()).isEqualTo(0);
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

		/*Disposable controls = */sensorDataStream
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

		assertThat(endLatch.getCount()).isEqualTo(0);
	}

	@Test
	public void shouldCorrectlyDispatchBatchedTimeout() throws InterruptedException {

		long timeout = 100;
		final int batchsize = 4;
		int parallelStreams = 16;
		CountDownLatch latch = new CountDownLatch(1);

		final Sinks.Many<Integer> streamBatcher = Sinks.many().multicast().onBackpressureBuffer();
		streamBatcher.asFlux()
					 .publishOn(asyncGroup)
		             .bufferTimeout(batchsize, Duration.ofSeconds(timeout))
		             .log("batched")
		             .parallel(parallelStreams)
		             .groups()
		             .log("batched-inner")
		             .subscribe(innerStream -> innerStream.publishOn(asyncGroup)
		                                                .doOnError(Throwable::printStackTrace)
		                                                .subscribe(i -> latch.countDown()));

		streamBatcher.emitNext(12, FAIL_FAST);
		streamBatcher.emitNext(123, FAIL_FAST);
		streamBatcher.emitNext(42, FAIL_FAST);
		streamBatcher.emitNext(666, FAIL_FAST);

		boolean finished = latch.await(2, TimeUnit.SECONDS);
		if (!finished) {
			throw new RuntimeException(latch.getCount()+"");
		}
		else {
			assertThat(latch.getCount()).as("latch count").isEqualTo(0);
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
		Scheduler supplier1 = afterTest.autoDispose(Schedulers.newParallel("groupByPool", 2));
		Scheduler supplier2 = afterTest.autoDispose(Schedulers.newParallel("partitionPool", 5));

		CountDownLatch latch = new CountDownLatch(10);

		/*Disposable c = */Flux.range(1, 10)
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
		assertThat(latch.getCount()).as("dispatch count").isEqualTo(0L);
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
				    subscribeOn(afterTest.autoDispose(Schedulers.newSingle("production"))).
				    publishOn(Schedulers.boundedElastic()).
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
		       .log("testOn", Level.FINE)
		       .subscribeOn(ioGroup)
		       .publishOn(asyncGroup)
		        .limitRate(1)
		       .subscribe(t -> latch.countDown());

		latch.await(30, TimeUnit.SECONDS);
		assertThat(latch.getCount()).as("dispatch count").isEqualTo(0L);
	}
	@Test
	public void unimplementedErrorCallback() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			Flux.error(new Exception("forced1"))
			    .log("error")
			    .subscribe();

			Flux.error(new Exception("forced2"))
			    .subscribe();

			assertThat(testLogger.getErrContent())
			          .contains("Operator called default onErrorDropped")
			          .contains("reactor.core.Exceptions$ErrorCallbackNotImplemented: java.lang.Exception: forced2");
		}
		finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void delayEach() throws InterruptedException {
		StepVerifier.withVirtualTime(() -> Flux.range(1, 3).delayElements(Duration.ofMillis(1000)))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .expectNext(1)
		            .expectNoEvent(Duration.ofSeconds(1))
		            .expectNext(2)
		            .expectNoEvent(Duration.ofSeconds(1))
		            .expectNext(3)
		            .verifyComplete();
	}

	// Test issue https://github.com/reactor/reactor/issues/474
// code by @masterav10
	@Test
	public void combineWithOneElement() throws InterruptedException, TimeoutException {
		AtomicReference<Object> ref = new AtomicReference<>(null);

		Phaser phaser = new Phaser(2);

		Flux<Object> s1 = Sinks.unsafe().many()
							   .replay()
							   .latestOrDefault(new Object())
							   .asFlux()
							   .publishOn(asyncGroup);
		Flux<Object> s2 = Sinks.unsafe().many()
							   .replay()
							   .latestOrDefault(new Object())
							   .asFlux()
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
		assertThat(ref).doesNotHaveValue(null);
	}

	/**
	 * This test case demonstrates a silent failure of {@link Flux#interval(Duration)}
	 * when a resolution is specified that
	 * is less than the backing {@link Timer} class.
	 *
	 * @throws InterruptedException - on failure.
	 * @throws TimeoutException     - on failure. <p> by @masterav10 : https://github.com/reactor/reactor/issues/469
	 */
	@Test
	@Disabled
	public void endLessTimer() throws InterruptedException, TimeoutException {
		int tasks = 50;
		long delayMS = 50; // XXX: Fails when less than 100
		Phaser barrier = new Phaser(tasks + 1);

		List<Long> times = new ArrayList<>();

		// long localTime = System.currentTimeMillis(); for java 7
		long localTime = Instant.now()
		                        .toEpochMilli();
		long elapsed = System.nanoTime();

		Disposable ctrl = Flux.interval(Duration.ofMillis(delayMS))
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

		assertThat(times.size()).isEqualTo(tasks);

		for (int i = 1; i < times.size(); i++) {
			Long prev = times.get(i - 1);
			Long time = times.get(i);

			assertThat(prev).isGreaterThan(0L);
			assertThat(time).isGreaterThan(0L);
			assertThat(time - prev).isLessThanOrEqualTo((long) (delayMS * 1.2));
		}
	}

	private List<Integer> createTestDataset(int i) {
		List<Integer> list = new ArrayList<>(i);
		for (int k = 0; k < i; k++) {
			list.add(k);
		}
		return list;
	}

	class FooSubscriber implements CoreSubscriber<String> {

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
	 */
	@Test
	@Timeout(10)
	public void multiplexUsingDispatchersAndSplit() {
		final Sinks.Many<Integer> forkEmitterProcessor = Sinks.many().multicast().onBackpressureBuffer();
		final Sinks.Many<Integer> computationEmitterProcessor = Sinks.unsafe()
		                                                             .many()
																	 .multicast()
																	 .onBackpressureBuffer(256, false);

		Scheduler computation = afterTest.autoDispose(Schedulers.newSingle("computation"));
		Scheduler persistence = afterTest.autoDispose(Schedulers.newSingle("persistence"));
		Scheduler forkJoin = afterTest.autoDispose(Schedulers.newParallel("forkJoin", 2));

		final Flux<List<String>> computationStream =
				computationEmitterProcessor.asFlux()
										   .publishOn(computation)
				                      		.map(i -> {
												  final List<String> list = new ArrayList<>(i);
												  for (int j = 0; j < i; j++) {
													  list.add("i" + j);
												  }
												  return list;
											})
											.doOnNext(ls -> println("Computed: ", ls))
											.log("computation");

		final Sinks.Many<Integer> persistenceEmitterProcessor = Sinks.unsafe()
																	 .many()
																	 .multicast()
																	 .onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);

		final Flux<List<String>> persistenceStream =
				persistenceEmitterProcessor.asFlux()
										   .publishOn(persistence)
										   .doOnNext(i -> println("Persisted: ", i))
										   .map(i -> Collections.singletonList("done" + i))
										   .log("persistence");

		Flux<Integer> forkStream = forkEmitterProcessor.asFlux()
													   .publishOn(forkJoin)
													   .log("fork");

		//from(sink) calls below should return same instance since both processor and standalone sink
		forkStream.subscribe(v -> computationEmitterProcessor.emitNext(v, FAIL_FAST),
				e -> computationEmitterProcessor.emitError(e, FAIL_FAST),
				() -> computationEmitterProcessor.emitComplete(FAIL_FAST));
		forkStream.subscribe(v -> persistenceEmitterProcessor.emitNext(v, FAIL_FAST),
				e -> persistenceEmitterProcessor.emitError(e, FAIL_FAST),
				() -> persistenceEmitterProcessor.emitComplete(FAIL_FAST));

		final Flux<List<String>> joinStream = Flux.zip(computationStream, persistenceStream, (a, b) -> Arrays.asList(a, b))
		                                                .publishOn(forkJoin)
		                                                .map(listOfLists -> {
			                                               listOfLists.get(0)
			                                                          .addAll(listOfLists.get(1));
			                                               return listOfLists.get(0);
		                                               })
		                                                .log("join");

		final Semaphore doneSemaphore = new Semaphore(0);

		StepVerifier.create(joinStream.flatMap(Flux::fromIterable)
		                                                 .log("resultStream")
		                                                 .collectList()
		                                                 .doOnTerminate(doneSemaphore::release))
					.then(() -> {
						forkEmitterProcessor.emitNext(1, FAIL_FAST);
						forkEmitterProcessor.emitNext(2, FAIL_FAST);
						forkEmitterProcessor.emitNext(3, FAIL_FAST);
						forkEmitterProcessor.emitComplete(FAIL_FAST);
					})
					.assertNext(res -> assertThat(res).containsExactly("i0", "done1", "i0", "i1", "done2", "i0", "i1", "i2", "done3"))
					.verifyComplete();
	}

	@Test
	public void testThrowWithoutOnErrorShowsUpInSchedulerHandler() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		AtomicReference<String> failure = new AtomicReference<>(null);
		AtomicBoolean handled = new AtomicBoolean(false);

		Thread.setDefaultUncaughtExceptionHandler((t, e) -> failure.set(
				"unexpected call to default" + " UncaughtExceptionHandler with " + e));
		CountDownLatch latch = new CountDownLatch(1);
		AtomicInteger uncompletedWork = new AtomicInteger();
		Schedulers.onHandleError((t, e) -> handled.set(true));
		Schedulers.onScheduleHook("test", r -> {
			uncompletedWork.incrementAndGet();
			return () -> {
				try {
					r.run();
				} finally {
					uncompletedWork.decrementAndGet();
				}
			};
		});

		try {
			Flux.interval(Duration.ofMillis(100))
			    .take(1)
			    .publishOn(Schedulers.parallel())
			    .doOnCancel(latch::countDown)
			    .subscribe(i -> {
				    System.out.println("About to throw...");
				    throw new IllegalArgumentException();
			    });

			assertThat(latch.await(5, TimeUnit.SECONDS)).as("Expected latch to count down before timeout", latch.await(5, TimeUnit.SECONDS)).isTrue();
			// awaiting to all threads done
			while (uncompletedWork.get() != 0) {
				Thread.sleep(100);
			}
		}
		catch (Throwable e) {
			fail(e.toString());
		}
		finally {
			LoggerUtils.disableCapture();
			Thread.setDefaultUncaughtExceptionHandler(null);
			Schedulers.resetOnHandleError();
			Schedulers.resetOnScheduleHook("test");
		}
		assertThat(handled).as("Uncaught error handler")
		                   .isFalse();
		assertThat(failure).as("Uncaught error handler")
		                   .hasValue(null);
		assertThat(testLogger.getErrContent())
		          .contains("Operator called default onErrorDropped")
		          .contains(
				          "reactor.core.Exceptions$ErrorCallbackNotImplemented: java.lang.IllegalArgumentException");
	}

	@Test
	@Disabled
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
	public void fluxFromFluxSourceDoesntCallAssemblyHook() {
		final Flux<Integer> source = Flux.range(1, 10);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Flux.from(source);
		assertThat(wrappedCount).hasValue(0);
	}

	@Test
	public void fluxFromScalarJustCallsAssemblyHook() {
		final Mono<String> source = Mono.just("scalarJust");

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Flux.from(source);
		assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void fluxFromScalarErrorCallsAssemblyHook() {
		final Mono<Object> source = Mono.error(new IllegalStateException("scalarError"));

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Flux.from(source);
		assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void fluxFromScalarEmptyCallsAssemblyHook() {
		final Mono<Object> source = Mono.empty();

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Flux.from(source);
		assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void fluxFromMonoFuseableCallsAssemblyHook() {
		Mono<String> source = Mono.just("monoFuseable").map(i -> i);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Flux.from(source);
		assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void fluxFromMonoNormalCallsAssemblyHook() {
		final Mono<String> source = Mono.just("monoNormal")
		                                .hide()
		                                .map(i -> i);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Flux.from(source);
		assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void fluxFromPublisherCallsAssemblyHook() {
		Publisher<String> publisher = sub -> sub.onSubscribe(Operators.emptySubscription());

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		Flux.from(publisher);
		assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void fluxNextScalarEmptyCallsAssemblyHook() {
		Flux<Integer> source = Flux.empty();

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		source.next();
		assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void fluxNextScalarValuedCallsAssemblyHook() {
		Flux<Integer> source = Flux.just(1);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		source.next();
		assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void fluxNextScalarErrorCallsAssemblyHook() {
		Flux<Integer> source = Flux.error(new IllegalStateException("boom"));

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		source.next();
		assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void fluxNextCallableCallsAssemblyHook() {
		Flux<Integer> source = Mono.fromCallable(() -> 1).flux();
		assertThat(source) //smoke test that we go into the right case
		          .isInstanceOf(Callable.class)
		          .isNotInstanceOf(Mono.class)
		          .isNotInstanceOf(Fuseable.ScalarCallable.class);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		source.next();
		assertThat(wrappedCount).hasValue(1);
	}

	@Test
	public void fluxNextNormalCallsAssemblyHook() {
		Flux<Integer> source = Flux.range(1, 10);

		//set the hook AFTER the original operators have been invoked (since they trigger assembly themselves)
		AtomicInteger wrappedCount = new AtomicInteger();
		Hooks.onEachOperator(p -> {
			wrappedCount.incrementAndGet();
			return p;
		});

		source.next();
		assertThat(wrappedCount).hasValue(1);
	}

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
