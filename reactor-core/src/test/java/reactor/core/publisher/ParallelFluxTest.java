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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.AutoDisposingExtension;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ParallelFluxTest {

	@RegisterExtension
	public AutoDisposingExtension afterTest = new AutoDisposingExtension();

	@Test
	public void sequentialMode() {
		Flux<Integer> source = Flux.range(1, 1_000_000)
		                           .hide();
		for (int i = 1; i < 33; i++) {
			Flux<Integer> result = ParallelFlux.from(source, i)
			                                   .map(v -> v + 1)
			                                   .sequential();

			AssertSubscriber<Integer> ts = AssertSubscriber.create();

			result.subscribe(ts);

			ts.assertSubscribed()
			  .assertValueCount(1_000_000)
			  .assertComplete()
			  .assertNoError();
		}

	}

	@Test
	public void sequentialModeFused() {
		Flux<Integer> source = Flux.range(1, 1_000_000);
		for (int i = 1; i < 33; i++) {
			Flux<Integer> result = ParallelFlux.from(source, i)
			                                   .map(v -> v + 1)
			                                   .sequential();

			AssertSubscriber<Integer> ts = AssertSubscriber.create();

			result.subscribe(ts);

			ts.assertSubscribed()
			  .assertValueCount(1_000_000)
			  .assertComplete()
			  .assertNoError();
		}

	}

	@Test
	public void parallelMode() {
		Flux<Integer> source = Flux.range(1, 1_000_000)
		                           .hide();
		int ncpu = Math.max(8,
				Runtime.getRuntime()
				       .availableProcessors());
		for (int i = 1; i < ncpu + 1; i++) {

			Scheduler scheduler = Schedulers.newParallel("test", i);

			try {
				Flux<Integer> result = ParallelFlux.from(source, i)
				                                   .runOn(scheduler)
				                                   .map(v -> v + 1)
				                                   .sequential();

				AssertSubscriber<Integer> ts = AssertSubscriber.create();

				result.subscribe(ts);

				ts.await(Duration.ofSeconds(10));

				ts.assertSubscribed()
				  .assertValueCount(1_000_000)
				  .assertComplete()
				  .assertNoError();
			}
			finally {
				scheduler.dispose();
			}
		}

	}

	@Test
	public void parallelModeFused() {
		Flux<Integer> source = Flux.range(1, 1_000_000);
		int ncpu = Math.max(8,
				Runtime.getRuntime()
				       .availableProcessors());
		for (int i = 1; i < ncpu + 1; i++) {

			Scheduler scheduler = Schedulers.newParallel("test", i);

			try {
				Flux<Integer> result = ParallelFlux.from(source, i)
				                                   .runOn(scheduler)
				                                   .map(v -> v + 1)
				                                   .sequential();

				AssertSubscriber<Integer> ts = AssertSubscriber.create();

				result.subscribe(ts);

				ts.await(Duration.ofSeconds(10));

				ts.assertSubscribed()
				  .assertValueCount(1_000_000)
				  .assertComplete()
				  .assertNoError();
			}
			finally {
				scheduler.dispose();
			}
		}

	}

	@Test
	public void collectSortedList() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		Flux.just(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
		    .parallel()
		    .collectSortedList(Comparator.naturalOrder())
		    .subscribe(ts);

		ts.assertValues(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
	}

	@Test
	public void sorted() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
		    .parallel()
		    .sorted(Comparator.naturalOrder())
		    .subscribe(ts);

		ts.assertNoValues();

		ts.request(2);

		ts.assertValues(1, 2);

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7);

		ts.request(3);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}

	@Test
	public void groupMerge() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .parallel()
		    .groups()
		    .flatMap(v -> v)
		    .subscribe(ts);

		ts.assertContainValues(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void from() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		ParallelFlux.from(Flux.range(1, 5), Flux.range(6, 5))
		            .sequential()
		            .subscribe(ts);

		ts.assertContainValues(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void concatMapUnordered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .parallel()
		    .concatMap(v -> Flux.range(v * 10 + 1, 3))
		    .sequential()
		    .subscribe(ts);

		ts.assertValues(11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void flatMapUnordered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .parallel()
		    .flatMap(v -> Flux.range(v * 10 + 1, 3))
		    .sequential()
		    .subscribe(ts);

		ts.assertValues(11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void testDoOnEachSignal() throws InterruptedException {
		List<Signal<Integer>> signals = Collections.synchronizedList(new ArrayList<>(4));
		List<Integer> values = Collections.synchronizedList(new ArrayList<>(2));
		ParallelFlux<Integer> flux = Flux.just(1, 2)
		                                 .parallel(3)
		                                 .doOnEach(signals::add)
		                                 .doOnEach(s -> {
			                                 if (s.isOnNext()) values.add(s.get());
		                                 });

		//we use a lambda subscriber and latch to avoid using `sequential`
		CountDownLatch latch = new CountDownLatch(2);
		flux.subscribe(v -> {
		}, e -> latch.countDown(), latch::countDown);

		assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();

		assertThat(signals.size()).isEqualTo(5);
		assertThat(signals.get(0).get())
				.as("first onNext signal isn't first value")
				.isEqualTo(1);
		assertThat(signals.get(1).get())
				.as("second onNext signal isn't last value")
				.isEqualTo(2);
		assertThat(signals.get(2)
				       .isOnComplete()).as("onComplete for rail 1 expected").isTrue();
		assertThat(signals.get(3)
				       .isOnComplete()).as("onComplete for rail 2 expected").isTrue();
		assertThat(signals.get(4)
				       .isOnComplete()).as("onComplete for rail 3 expected").isTrue();
		assertThat(values.get(0)).as("1st onNext value unexpected").isEqualTo(1);
		assertThat(values.get(1)).as("2nd onNext value unexpected").isEqualTo(2);
	}

	@Test
	public void testDoOnEachSignalWithError() throws InterruptedException {
		List<Signal<Integer>> signals = Collections.synchronizedList(new ArrayList<>(4));
		ParallelFlux<Integer> flux = Flux.<Integer>error(new IllegalArgumentException("boom")).parallel(2)
		                                                                                      .runOn(Schedulers.parallel())
		                                                                                      .doOnEach(signals::add);

		//we use a lambda subscriber and latch to avoid using `sequential`
		CountDownLatch latch = new CountDownLatch(2);
		flux.subscribe(v -> {
		}, e -> latch.countDown(), latch::countDown);

		assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();

		assertThat(signals).hasSize(2);
		assertThat(signals.get(0)
				       .isOnError()).as("rail 1 onError expected").isTrue();
		assertThat(signals.get(1)
				       .isOnError()).as("rail 2 onError expected").isTrue();
		assertThat(signals.get(0).getThrowable()).as("plain exception rail 1 expected")
				.hasMessage("boom");
		assertThat(signals.get(1).getThrowable()).as("plain exception rail 2 expected")
				.hasMessage("boom");
	}

	@Test
	public void testDoOnEachSignalNullConsumer() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.just(1)
					.parallel()
					.doOnEach(null);
		});
	}

	@Test
	public void testDoOnEachSignalToSubscriber() {
		AssertSubscriber<Integer> peekSubscriber = AssertSubscriber.create();
		ParallelFlux<Integer> flux = Flux.just(1, 2)
		                                 .parallel(3)
		                                 .doOnEach(s -> s.accept(peekSubscriber));

		flux.subscribe();

		peekSubscriber.assertNotSubscribed();
		peekSubscriber.assertValues(1, 2);

		Assertions.assertThatExceptionOfType(AssertionError.class)
		          .isThrownBy(peekSubscriber::assertComplete)
		          .withMessage("Multiple completions: 3");
	}

	@Test
	public void transformGroups() {
		Set<Integer> values = new ConcurrentSkipListSet<>();

		Flux<Integer> flux = Flux.range(1, 10)
		                         .parallel(3)
		                         .runOn(Schedulers.parallel())
		                         .doOnNext(values::add)
		                         .transformGroups(p -> p.log("rail" + p.key())
		                                             .map(i -> (p.key() + 1) * 100 + i))
		                         .sequential();

		StepVerifier.create(flux.sort())
		            .assertNext(i -> assertThat(i - 100)
		                                       .isBetween(1, 10))
		            .thenConsumeWhile(i -> i / 100 == 1)
		            .assertNext(i -> assertThat(i - 200)
		                                       .isBetween(1, 10))
		            .thenConsumeWhile(i -> i / 100 == 2)
		            .assertNext(i -> assertThat(i - 300)
		                                       .isBetween(1, 10))
		            .thenConsumeWhile(i -> i / 100 == 3)
		            .verifyComplete();

		assertThat(values)
		          .hasSize(10)
		          .contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}

	@Test
	public void transformGroupsMaintainsParallelismAndPrefetch() {
		ParallelFlux<Integer> parallelFlux = Flux.range(1, 10)
		                                         .parallel(3)
		                                         .runOn(Schedulers.parallel(), 123);

		ParallelFlux<Integer> composed = parallelFlux.transformGroups(rail -> rail.map(i -> i + 2));

		assertThat(composed.parallelism())
				.as("maintains parallelism")
				.isEqualTo(parallelFlux.parallelism())
				.isEqualTo(3);

		assertThat(composed.getPrefetch())
				.as("maintains prefetch")
				.isEqualTo(parallelFlux.getPrefetch())
				.isEqualTo(123);
	}

	@Test
	public void transformGroupsMaintainsParallelism() {
		ParallelFlux<Integer> parallelFlux = Flux.range(1, 10)
		                                         .parallel(3)
		                                         .map(i -> i + 2);

		ParallelFlux<Integer> composed = parallelFlux.transformGroups(rail -> rail.map(i -> i + 2));

		assertThat(composed.parallelism())
				.as("maintains parallelism")
				.isEqualTo(parallelFlux.parallelism())
				.isEqualTo(3);

		assertThat(parallelFlux.getPrefetch())
				.as("parallel source no prefetch")
				.isEqualTo(-1);

		assertThat(composed.getPrefetch())
				.as("reset prefetch to default")
				.isNotEqualTo(parallelFlux.getPrefetch())
				.isEqualTo(Queues.SMALL_BUFFER_SIZE);
	}

	@Test
	public void fromSourceHasCpuParallelism() {
		int cpus = Runtime.getRuntime()
		                  .availableProcessors();
		ParallelFlux<Integer> parallelFlux = ParallelFlux.from(Flux.range(1, 10));

		assertThat(parallelFlux.parallelism())
		          .isEqualTo(cpus);
	}

	@Test
	public void fromZeroParallelismRejected() {
		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> ParallelFlux.from(Mono.just(1), 0))
		          .withMessage("parallelism > 0 required but it was 0");
	}

	@Test
	public void fromNegativeParallelismRejected() {
		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> ParallelFlux.from(Mono.just(1), -1))
		          .withMessage("parallelism > 0 required but it was -1");
	}

	@Test
	public void fromZeroPrefetchRejected() {
		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> ParallelFlux.from(Mono.just(1), 1, 0, Queues.small()))
		          .withMessage("prefetch > 0 required but it was 0");
	}

	@Test
	public void fromNegativePrefetchRejected() {
		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> ParallelFlux.from(Mono.just(1), 1, -1, Queues.small()))
		          .withMessage("prefetch > 0 required but it was -1");
	}

	@Test
	public void fromZeroPublishersRejected() {
		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> ParallelFlux.<Integer>from())
		          .withMessage("Zero publishers not supported");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void fromZeroLengthArrayPublishersRejected() {
		Publisher<Integer>[] array = new Publisher[0];
		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> ParallelFlux.from(array))
		          .withMessage("Zero publishers not supported");
	}

	@Test
	public void fromNullPublisherRejected() {
		Assertions.assertThatExceptionOfType(NullPointerException.class)
		          .isThrownBy(() -> ParallelFlux.from((Publisher<?>) null))
		          .withMessage("source");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void fromNullPublisherArrayRejected() {
		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> ParallelFlux.from((Publisher[]) null))
		          .withMessage("Zero publishers not supported");
	}

	@Test
	public void fromFuseableUsesThreadBarrier() {
		final Set<String> between = new HashSet<>();
		final ConcurrentHashMap<String, String> processing = new ConcurrentHashMap<>();

		Flux<Integer> test = Flux.range(1, 10)
		                         .publishOn(Schedulers.single(), false, 1)
		                         .doOnNext(v -> between.add(Thread.currentThread()
		                                                          .getName()))
		                         .parallel(2, 1)
		                         .runOn(Schedulers.boundedElastic(), 1)
		                         .map(v -> {
			                         processing.putIfAbsent(Thread.currentThread()
			                                                      .getName(), "");
			                         return v;
		                         })
		                         .sequential();

		StepVerifier.create(test)
		            .expectSubscription()
		            .recordWith(() -> Collections.synchronizedList(new ArrayList<>(10)))
		            .expectNextCount(10)
		            .consumeRecordedWith(r -> assertThat(r).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
		            .expectComplete()
		            .verify(Duration.ofSeconds(5));

		assertThat(between).hasSize(1);
		assertThat(between).first()
		                   .asString()
		                   .startsWith("single-");

		assertThat(processing.keySet())
				.allSatisfy(k -> assertThat(k).startsWith("boundedElastic-"));
	}

	@Test
	public void runOnZeroPrefetchRejected() {
		ParallelFlux<Integer> validSoFar = ParallelFlux.from(Mono.just(1));

		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> validSoFar.runOn(Schedulers.parallel(), 0))
		          .withMessage("prefetch > 0 required but it was 0");
	}

	@Test
	public void runOnNegativePrefetchRejected() {
		ParallelFlux<Integer> validSoFar = ParallelFlux.from(Mono.just(1));

		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> validSoFar.runOn(Schedulers.parallel(), -1))
		          .withMessage("prefetch > 0 required but it was -1");
	}

	@Test
	public void sequentialZeroPrefetchRejected() {
		ParallelFlux<Integer> validSoFar = ParallelFlux.from(Mono.just(1));

		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> validSoFar.sequential(0))
		          .withMessage("prefetch > 0 required but it was 0");
	}

	@Test
	public void sequentialNegativePrefetchRejected() {
		ParallelFlux<Integer> validSoFar = ParallelFlux.from(Mono.just(1));

		Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
		          .isThrownBy(() -> validSoFar.sequential(-1))
		          .withMessage("prefetch > 0 required but it was -1");
	}

	@Test
	public void subscribeOnNextOnErrorErrorsOnAllRails() {
		LongAdder valueAdder = new LongAdder();
		LongAdder errorAdder = new LongAdder();
		Flux.range(1, 3)
		    .concatWith(Mono.error(new IllegalStateException("boom")))
		    .parallel(2)
	        .subscribe(v -> valueAdder.increment(), e -> errorAdder.increment());

		assertThat(valueAdder.intValue()).isEqualTo(3);
		assertThat(errorAdder.intValue()).isEqualTo(2);
	}

	@Test
	public void validateTooFewSubscribers() {
		validateSubscribers(2);
	}
	@Test
	public void validateTooManySubscribers() {
		validateSubscribers(4);
	}

	@SuppressWarnings("unchecked")
	private void validateSubscribers(int size) {
		List<Throwable> errors = Collections.synchronizedList(new ArrayList<>(size));
		Subscriber<Integer>[] subs = new Subscriber[size];
		for (int i = 0; i < subs.length; i++) {
			subs[i] = new BaseSubscriber<Integer>() {
				@Override
				protected void hookOnSubscribe(Subscription subscription) { requestUnbounded(); }

				@Override
				protected void hookOnNext(Integer value) { }

				@Override
				protected void hookOnError(Throwable throwable) {
					errors.add(throwable);
				}
			};
		}

		Flux.range(1, 3)
		    .parallel(3)
		    .validate(subs);

		assertThat(errors)
		          .hasSize(size)
		          .allSatisfy(e -> assertThat(e).hasMessage("parallelism = 3, subscribers = " + size));
	}

	@Test
	public void fromPublishersDefaultPrefetchIsMinusOne() {
		assertThat(ParallelFlux.from(Flux.range(1, 5), Flux.range(5, 5))
				.getPrefetch()).isEqualTo(-1);
	}

	@Test
	public void fromPublisherDefaultPrefetchIsSmallBufferSize() {
		assertThat(ParallelFlux.from(Flux.range(1, 5))
				.getPrefetch()).isEqualTo(Queues.SMALL_BUFFER_SIZE);
	}

	@Test
	public void fromPublishersSequentialSubscribe() {
		List<Integer> values = Collections.synchronizedList(new ArrayList<>(10));
		ParallelFlux.from(Flux.range(1, 3), Flux.range(4, 3))
		            .runOn(Schedulers.parallel())
		            .doOnNext(values::add)
		            .sequential()
		            .blockLast();

		assertThat(values)
	              .hasSize(6)
	              .containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
	}

	@Test
	public void asChangesParallelism() {
		assertThat(ParallelFlux.from(Flux.range(1, 10), 3)
		                       .as(pf -> ParallelFlux.from(pf.sequential(), 5)
		                                             .log("secondParallel"))
		                       .parallelism())
				.isEqualTo(5);
	}

	@Test
	public void transformChangesPrefetch() {
		assertThat(ParallelFlux.from(Flux.range(1, 10), 3, 12, Queues.small())
		                       .transform(pf -> pf.runOn(Schedulers.parallel(), 3)
		                                          .log()
		                                          .hide())
		                       .getPrefetch())
				.isEqualTo(3);
	}

	@Test
	public void testPeekComplete() {
		List<Signal> signals = Collections.synchronizedList(new ArrayList<>());
		LongAdder subscribeCount = new LongAdder();
		LongAdder valueCount = new LongAdder();
		LongAdder requestCount = new LongAdder();
		LongAdder completeCount = new LongAdder();
		LongAdder cancelCount = new LongAdder();
		LongAdder errorCount = new LongAdder();
		LongAdder terminateCount = new LongAdder();
		LongAdder afterTerminateCount = new LongAdder();

		ParallelFlux.from(Flux.range(1, 10), 2)
	                .doOnEach(signals::add)
	                .doOnSubscribe(s -> subscribeCount.increment())
	                .doOnNext(v -> valueCount.increment())
	                .doOnRequest(r -> requestCount.increment())
	                .doOnComplete(completeCount::increment)
	                .doOnCancel(cancelCount::increment)
	                .doOnError(e -> errorCount.increment())
	                .doOnTerminate(terminateCount::increment)
	                .doAfterTerminate(afterTerminateCount::increment)
	                .subscribe(v -> {});

		assertThat(signals).as("signals").hasSize(10 + 2); //2x5 onNext, 2x1 onComplete
		assertThat(subscribeCount.longValue()).as("subscribe").isEqualTo(2); //1 per rail
		assertThat(valueCount.longValue()).as("values").isEqualTo(10);
		assertThat(requestCount.longValue()).as("request").isEqualTo(2); //1 per rail
		assertThat(completeCount.longValue()).as("complete").isEqualTo(2); //1 per rail
		assertThat(cancelCount.longValue()).as("cancel").isEqualTo(0);
		assertThat(errorCount.longValue()).as("errors").isEqualTo(0);
		assertThat(terminateCount.longValue()).as("terminate").isEqualTo(2); //1 per rail
		assertThat(afterTerminateCount.longValue()).as("afterTerminate").isEqualTo(2); //1 per rail
	}
	@Test
	public void testPeekError() {
		List<Signal> signals = Collections.synchronizedList(new ArrayList<>());
		LongAdder subscribeCount = new LongAdder();
		LongAdder valueCount = new LongAdder();
		LongAdder requestCount = new LongAdder();
		LongAdder completeCount = new LongAdder();
		LongAdder cancelCount = new LongAdder();
		LongAdder errorCount = new LongAdder();
		LongAdder terminateCount = new LongAdder();
		LongAdder afterTerminateCount = new LongAdder();

		ParallelFlux.from(Flux.range(1, 4).concatWith(Mono.error(new IllegalStateException("boom"))), 2)
		            .doOnEach(signals::add)
	                .doOnSubscribe(s -> subscribeCount.increment())
	                .doOnNext(v -> valueCount.increment())
	                .doOnRequest(r -> requestCount.increment())
	                .doOnComplete(completeCount::increment)
	                .doOnCancel(cancelCount::increment)
	                .doOnError(e -> errorCount.increment())
	                .doOnTerminate(terminateCount::increment)
	                .doAfterTerminate(afterTerminateCount::increment)
	                .subscribe(v -> {}, e -> {}); //error callback so that afterTerminate isn't swallowed

		assertThat(signals).as("signals").hasSize(4 + 2); //2x2 onNext, 2x1 onError
		assertThat(subscribeCount.longValue()).as("subscribe").isEqualTo(2); //1 per rail
		assertThat(valueCount.longValue()).as("values").isEqualTo(4);
		assertThat(requestCount.longValue()).as("request").isEqualTo(2); //1 per rail
		assertThat(completeCount.longValue()).as("complete").isEqualTo(0);
		assertThat(cancelCount.longValue()).as("cancel").isEqualTo(0);
		assertThat(errorCount.longValue()).as("errors").isEqualTo(2);
		assertThat(terminateCount.longValue()).as("terminate").isEqualTo(2); //1 per rail
		assertThat(afterTerminateCount.longValue()).as("afterTerminate").isEqualTo(2); //1 per rail
	}
	@Test
	public void testPeekCancel() {
		List<Signal> signals = Collections.synchronizedList(new ArrayList<>());
		LongAdder subscribeCount = new LongAdder();
		LongAdder valueCount = new LongAdder();
		LongAdder requestCount = new LongAdder();
		LongAdder completeCount = new LongAdder();
		LongAdder cancelCount = new LongAdder();
		LongAdder errorCount = new LongAdder();
		LongAdder terminateCount = new LongAdder();
		LongAdder afterTerminateCount = new LongAdder();

		ParallelFlux.from(Flux.range(1, 10), 2)
		            .doOnEach(signals::add)
	                .doOnSubscribe(s -> subscribeCount.increment())
	                .doOnNext(v -> valueCount.increment())
	                .doOnRequest(r -> requestCount.increment())
	                .doOnComplete(completeCount::increment)
	                .doOnCancel(cancelCount::increment)
	                .doOnError(e -> errorCount.increment())
	                .doOnTerminate(terminateCount::increment)
	                .doAfterTerminate(afterTerminateCount::increment)
	                .sequential().take(4).subscribe();

		assertThat(signals).as("signals").hasSize(4); //2x2 onNext (+ 2 non-represented cancels)
		assertThat(subscribeCount.longValue()).as("subscribe").isEqualTo(2); //1 per rail
		assertThat(valueCount.longValue()).as("values").isEqualTo(4);
		assertThat(requestCount.longValue()).as("request").isEqualTo(2); //1 per rail
		assertThat(completeCount.longValue()).as("complete").isEqualTo(0);
		assertThat(cancelCount.longValue()).as("cancel").isEqualTo(2);
		assertThat(errorCount.longValue()).as("errors").isEqualTo(0);
		//cancel don't trigger onTerminate/onAfterTerminate:
		assertThat(terminateCount.longValue()).as("terminate").isEqualTo(0);
		assertThat(afterTerminateCount.longValue()).as("afterTerminate").isEqualTo(0);
	}

	@Test
	public void testConcatMapPrefetch() {
		ParallelFlux<Integer> pf = ParallelFlux.from(Flux.range(1, 4), 2)
		                                      .concatMap(i -> Flux.just(i, 100 * i), 4);

		assertThat(pf.getPrefetch()).isEqualTo(4);
		StepVerifier.create(pf)
		            .expectNext(1, 100, 2, 200, 3, 300, 4, 400)
		            .verifyComplete();
	}

	@Test
	public void testConcatMapDelayError() {
		ParallelFlux<Integer> pf = ParallelFlux.from(Flux.range(1, 4), 2)
		                                       .concatMapDelayError(i -> {
		                                       	if (i == 1)
		                                       		return Mono.<Integer>error(new IllegalStateException("boom")).hide();
		                                       	return Flux.just(i, 100 * i);
		                                       });

		StepVerifier.create(pf)
		            .expectNext(2, 200, 3, 300, 4, 400)
		            .verifyErrorMessage("boom");
	}

	@Test
	public void testConcatMapDelayErrorPrefetch() {
		ParallelFlux<Integer> pf = ParallelFlux.from(Flux.range(1, 4), 2)
		                                       .concatMapDelayError(i -> {
			                                       if (i == 1)
				                                       return Mono.<Integer>error(new IllegalStateException("boom")).hide();
			                                       return Flux.just(i, 100 * i);
		                                       }, 4);

		assertThat(pf.getPrefetch()).isEqualTo(4);
		StepVerifier.create(pf)
		            .expectNext(2, 200, 3, 300, 4, 400)
		            .verifyErrorMessage("boom");
	}

	@Test
	public void testConcatMapDelayErrorPrefetchDelayUntilEnd() {
		ParallelFlux<Integer> pf = ParallelFlux.from(Flux.range(1, 4), 2)
		                                       .concatMapDelayError(i -> {
			                                       if (i == 1)
				                                       return Mono.error(new IllegalStateException("boom"));
			                                       return Flux.just(i, 100 * i);
		                                       }, false, 4);

		assertThat(pf.getPrefetch()).isEqualTo(4);
		StepVerifier.create(pf)
		            .verifyErrorMessage("boom");
	}

	@Test
	public void testFlatMapDelayError() {
		ParallelFlux<Integer> pf = ParallelFlux.from(Flux.range(1, 4), 2)
		                                       .flatMap(i -> {
			                                       if (i == 1)
				                                       return Mono.<Integer>error(new IllegalStateException("boom")).hide();
			                                       return Flux.just(i, 100 * i);
		                                       }, true);

		StepVerifier.create(pf)
		            .expectNext(2, 200, 3, 300, 4, 400)
		            .verifyErrorMessage("boom");
	}

	@Test
	public void testFlatMapDelayErrorMaxConcurrency() {
		ParallelFlux<Integer> pf = ParallelFlux.from(Flux.range(1, 4), 2)
		                                       .flatMap(i -> {
			                                       if (i == 1)
				                                       return Mono.<Integer>error(new IllegalStateException("boom")).hide();
			                                       return Flux.just(i, 100 * i);
		                                       }, true, 2);

		StepVerifier.create(pf)
		            .expectNext(2, 200, 3, 300, 4, 400)
		            .verifyErrorMessage("boom");
	}

	@Test
	public void testPublisherSubscribeUsesSequential() {
		LongAdder valueCount = new LongAdder();
		ParallelFlux<Integer> pf = ParallelFlux.from(Flux.range(1, 4), 2);
		pf.subscribe(new BaseSubscriber<Integer>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				requestUnbounded();
			}

			@Override
			protected void hookOnNext(Integer value) {
				valueCount.increment();
			}
		});

		assertThat(valueCount.intValue()).isEqualTo(4);
	}

	@Test
	public void collectSortedListBothEmpty() {
		List<Integer> result = ParallelFlux.sortedMerger(Collections.emptyList(),
				Collections.emptyList(),
				Integer::compareTo);

		assertThat(result)
				.isEmpty();
	}

	@Test
	public void collectSortedListRightLarger() {
		List<Integer> left = Arrays.asList(1, 3);
		List<Integer> right = Arrays.asList(2, 4, 5, 6);

		List<Integer> result = ParallelFlux.sortedMerger(left, right, Integer::compareTo);

		assertThat(result)
				.containsExactly(1, 2, 3, 4, 5, 6);
	}

	@Test
	public void collectSortedListLeftLarger() {
		List<Integer> left = Arrays.asList(2, 4, 5, 6);
		List<Integer> right = Arrays.asList(1, 3);

		List<Integer> result = ParallelFlux.sortedMerger(left, right, Integer::compareTo);

		assertThat(result)
				.containsExactly(1, 2, 3, 4, 5, 6);
	}

	@Test
	public void collectSortedListLeftEmpty() {
		List<Integer> left = Collections.emptyList();
		List<Integer> right = Arrays.asList(2, 4, 5, 6);

		List<Integer> result = ParallelFlux.sortedMerger(left, right, Integer::compareTo);

		assertThat(result)
				.containsExactly(2, 4, 5, 6);
	}

	@Test
	public void collectSortedListRightEmpty() {
		List<Integer> left = Arrays.asList(2, 4, 5, 6);
		List<Integer> right = Collections.emptyList();

		List<Integer> result = ParallelFlux.sortedMerger(left, right, Integer::compareTo);

		assertThat(result)
				.containsExactly(2, 4, 5, 6);
	}


	@Test
	public void testParallelism() throws Exception
	{
		Flux<Integer> flux = Flux.just(1, 2, 3);

		Set<String> threadNames = Collections.synchronizedSet(new TreeSet<>());
		AtomicInteger count = new AtomicInteger();

		CountDownLatch latch = new CountDownLatch(3);

		flux
				// Uncomment line below for failure
				.cache(1)
				.parallel(3)
				.runOn(afterTest.autoDispose(Schedulers.newBoundedElastic(4, 100, "TEST")))
				.subscribe(i ->
				{
					threadNames.add(Thread.currentThread()
					                      .getName());
					count.incrementAndGet();
					latch.countDown();

					tryToSleep(1000);
				});

		latch.await();

		assertThat(count.get()).as("Multithreaded count").isEqualTo(3);
		assertThat(threadNames).as("Multithreaded threads").hasSize(3);
	}

	@Test
	public void parallelSubscribeAndDispose() throws InterruptedException {
		AtomicInteger nextCount = new AtomicInteger();
		CountDownLatch cancelLatch = new CountDownLatch(1);
		TestPublisher<Integer> source = TestPublisher.create();

		Disposable d = source
				.flux()
				.parallel(3)
				.doOnCancel(cancelLatch::countDown)
				.subscribe(i -> nextCount.incrementAndGet());

		source.next(1, 2, 3);
		d.dispose();

		source.emit(4, 5, 6);

		boolean finished = cancelLatch.await(300, TimeUnit.MILLISECONDS);

		assertThat(finished).as("cancelled latch").isTrue();
		assertThat(d.isDisposed()).as("disposed").isTrue();
		assertThat(nextCount.get()).as("received count").isEqualTo(3);
	}

	@Test
	public void hooks() throws Exception {
		String key = UUID.randomUUID().toString();
		try {
			Hooks.onLastOperator(key, p -> new CorePublisher<Object>() {
				@Override
				public void subscribe(CoreSubscriber<? super Object> subscriber) {
					((CorePublisher<?>) p).subscribe(subscriber);
				}

				@Override
				public void subscribe(Subscriber<? super Object> s) {
					throw new IllegalStateException("Should not be called");
				}
			});

			List<Integer> results = new CopyOnWriteArrayList<>();
			CountDownLatch latch = new CountDownLatch(1);
			Flux.just(1, 2, 3)
			    .parallel()
			    .doOnNext(results::add)
			    .doOnComplete(latch::countDown)
			    .subscribe();

			latch.await(1, TimeUnit.SECONDS);

			assertThat(results).containsOnly(1, 2, 3);
		}
		finally {
			Hooks.resetOnLastOperator(key);
		}
	}

	@Test
	public void subscribeWithCoreSubscriber() throws Exception {
		List<Integer> results = new CopyOnWriteArrayList<>();
		CountDownLatch latch = new CountDownLatch(1);
		Flux.just(1, 2, 3).parallel().subscribe(new CoreSubscriber<Integer>() {
			@Override
			public void onSubscribe(Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(Integer integer) {
				results.add(integer);
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
			}

			@Override
			public void onComplete() {
				latch.countDown();
			}
		});

		latch.await(1, TimeUnit.SECONDS);

		assertThat(results).containsOnly(1, 2, 3);
	}

	// https://github.com/reactor/reactor-core/issues/1656
	@Test
	public void doOnEachContext() {
		List<String> results = new CopyOnWriteArrayList<>();
		Flux.just(1, 2, 3)
		    .parallel(3)
		    .doOnEach(s -> {
			    String valueFromContext = s.getContextView()
			                               .getOrDefault("test", null);
			    results.add(s + " " + valueFromContext);
		    })
		    .reduce(Integer::sum)
		    .contextWrite(Context.of("test", "Hello!"))
		    .block();

		assertThat(results).containsExactlyInAnyOrder(
				"onNext(1) Hello!",
				"onNext(2) Hello!",
				"onNext(3) Hello!",
				"onComplete() Hello!",
				"onComplete() Hello!",
				"onComplete() Hello!"
		);
	}

	private void tryToSleep(long value)
	{
		try
		{
			Thread.sleep(value);
		}
		catch(InterruptedException e)
		{
			e.printStackTrace();
		}
	}
}
