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
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.FluxConcatMap.ErrorMode;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxMergeSequentialTest {

	AssertSubscriber<Object> ts;
	AssertSubscriber<Object> tsBp;

	final Function<Integer, Flux<Integer>> toJust = Flux::just;

	final Function<Integer, Flux<Integer>> toRange = t -> Flux.range(t, 2);

	@BeforeEach
	public void before() {
		ts = new AssertSubscriber<>();
		tsBp = new AssertSubscriber<>(0L);
	}

	@Test
	public void normal() {
		StepVerifier.create(Flux.range(1, 5)
		                        .hide()
		                        .flatMapSequential(t -> Flux.range(t, 2)))
					.expectNoFusionSupport()
		            .expectNext(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		            .verifyComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = Flux.range(1, 5)
		                                   .hide()
		                                   .flatMapSequential(t -> Flux.range(t, 2))
		                                   .subscribeWith(AssertSubscriber.create(3));

		ts.assertValues(1, 2, 2);

		ts.request(1);

		ts.assertValues(1, 2, 2, 3);

		ts.request(1);

		ts.assertValues(1, 2, 2, 3, 3);

		ts.request(5);

		ts.assertComplete().assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
	}


	@Test
	public void normalDelayEnd() {
		Flux.range(1, 5)
		        .flatMapSequentialDelayError(t -> Flux.range(t, 2), 32, 32)
		        .subscribeWith(AssertSubscriber.create())
		        .assertComplete().assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
	}

	@Test
	public void normalDelayEndBackpressured() {
		AssertSubscriber<Integer> ts = Flux.range(1, 5)
		                                   .flatMapSequentialDelayError(t -> Flux.range(t, 2), 32, 32)
		                                   .subscribeWith(AssertSubscriber.create(3));

		ts.assertValues(1, 2, 2);

		ts.request(1);

		ts.assertValues(1, 2, 2, 3);

		ts.request(1);

		ts.assertValues(1, 2, 2, 3, 3);

		ts.request(5);

		ts.assertComplete().assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
	}

	@Test
	public void mainErrorsDelayEnd() {
		Sinks.Many<Integer> main = Sinks.unsafe().many().multicast().directBestEffort();
		final Sinks.Many<Integer> inner = Sinks.unsafe().many().multicast().directBestEffort();

		AssertSubscriber<Integer> ts = main.asFlux()
										   .flatMapSequentialDelayError(t -> inner.asFlux(), 32, 32)
		                                   .subscribeWith(AssertSubscriber.create());

		main.emitNext(1, FAIL_FAST);
		main.emitNext(2, FAIL_FAST);

		inner.emitNext(2, FAIL_FAST);

		ts.assertValues(2);

		main.emitError(new RuntimeException("Forced failure"), FAIL_FAST);

		ts.assertNoError();

		inner.emitNext(3, FAIL_FAST);
		inner.emitComplete(FAIL_FAST);

		ts.assertValues(2, 3, 2, 3)
		  .assertErrorMessage("Forced failure");
	}

	@Test
	public void mainErrorsImmediate() {
		Sinks.Many<Integer> main = Sinks.unsafe().many().multicast().directBestEffort();
		final Sinks.Many<Integer> inner = Sinks.unsafe().many().multicast().directBestEffort();

		AssertSubscriber<Integer> ts = main.asFlux().flatMapSequential(t -> inner.asFlux())
		                                   .subscribeWith(AssertSubscriber.create());

		main.emitNext(1, FAIL_FAST);
		main.emitNext(2, FAIL_FAST);

		inner.emitNext(2, FAIL_FAST);

		ts.assertValues(2);

		main.emitError(new RuntimeException("Forced failure"), FAIL_FAST);

		assertThat(inner.currentSubscriberCount()).as("inner has subscriber").isZero();

		inner.emitNext(3, FAIL_FAST);
		inner.emitComplete(FAIL_FAST);

		ts.assertValues(2).assertErrorMessage("Forced failure");
	}

	@Test
	public void longEager() {

		Flux.range(1, 2 * Queues.SMALL_BUFFER_SIZE)
		        .flatMapSequential(v -> Flux.just(1))
		        .subscribeWith(AssertSubscriber.create())
		        .assertValueCount(2 * Queues.SMALL_BUFFER_SIZE)
		        .assertNoError()
		        .assertComplete();
	}

	@Test
	public void testSimple() {
		Flux.range(1, 100).flatMapSequential(toJust).subscribe(ts);

		ts.assertNoError();
		ts.assertValueCount(100);
		ts.assertComplete();
	}

	@Test
	public void testSimple2() {
		Flux.range(1, 100).flatMapSequential(toRange).subscribe(ts);

		ts.assertNoError();
		ts.assertValueCount(200);
		ts.assertComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEagerness2() {
		final AtomicInteger count = new AtomicInteger();
		Flux<Integer> source = Flux.just(1).doOnNext(t -> count.getAndIncrement()).hide();

		Flux.mergeSequential(source, source).subscribe(tsBp);

		assertThat(count).hasValue(2);
		tsBp.assertNoError();
		tsBp.assertNotComplete();
		tsBp.assertNoValues();

		tsBp.request(Long.MAX_VALUE);

		tsBp.assertValueCount(count.get());
		tsBp.assertNoError();
		tsBp.assertComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEagerness3() {
		final AtomicInteger count = new AtomicInteger();
		Flux<Integer> source = Flux.just(1).doOnNext(t -> count.getAndIncrement()).hide();

		Flux.mergeSequential(source, source, source).subscribe(tsBp);

		assertThat(count).hasValue(3);
		tsBp.assertNoError();
		tsBp.assertNotComplete();
		tsBp.assertNoValues();

		tsBp.request(Long.MAX_VALUE);

		tsBp.assertValueCount(count.get());
		tsBp.assertNoError();
		tsBp.assertComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEagerness4() {
		final AtomicInteger count = new AtomicInteger();
		Flux<Integer> source = Flux.just(1).doOnNext(t -> count.getAndIncrement()).hide();

		Flux.mergeSequential(source, source, source, source).subscribe(tsBp);

		assertThat(count).hasValue(4);
		tsBp.assertNoError();
		tsBp.assertNotComplete();
		tsBp.assertNoValues();

		tsBp.request(Long.MAX_VALUE);

		tsBp.assertValueCount(count.get());
		tsBp.assertNoError();
		tsBp.assertComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEagerness5() {
		final AtomicInteger count = new AtomicInteger();
		Flux<Integer> source = Flux.just(1).doOnNext(t -> count.getAndIncrement()).hide();

		Flux.mergeSequential(source, source, source, source, source).subscribe(tsBp);

		assertThat(count).hasValue(5);
		tsBp.assertNoError();
		tsBp.assertNotComplete();
		tsBp.assertNoValues();

		tsBp.request(Long.MAX_VALUE);

		tsBp.assertValueCount(count.get());
		tsBp.assertNoError();
		tsBp.assertComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEagerness6() {
		final AtomicInteger count = new AtomicInteger();
		Flux<Integer> source = Flux.just(1).doOnNext(t -> count.getAndIncrement()).hide();

		Flux.mergeSequential(source, source, source, source, source, source).subscribe(tsBp);

		assertThat(count).hasValue(6);
		tsBp.assertNoError();
		tsBp.assertNotComplete();
		tsBp.assertNoValues();

		tsBp.request(Long.MAX_VALUE);

		tsBp.assertValueCount(count.get());
		tsBp.assertNoError();
		tsBp.assertComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEagerness7() {
		final AtomicInteger count = new AtomicInteger();
		Flux<Integer> source = Flux.just(1).doOnNext(t -> count.getAndIncrement()).hide();

		Flux.mergeSequential(source, source, source, source, source, source, source).subscribe(tsBp);

		assertThat(count).hasValue(7);
		tsBp.assertNoError();
		tsBp.assertNotComplete();
		tsBp.assertNoValues();

		tsBp.request(Long.MAX_VALUE);

		tsBp.assertValueCount(count.get());
		tsBp.assertNoError();
		tsBp.assertComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEagerness8() {
		final AtomicInteger count = new AtomicInteger();
		Flux<Integer> source = Flux.just(1).doOnNext(t -> count.getAndIncrement()).hide();

		Flux.mergeSequential(source, source, source, source, source, source, source, source).subscribe(tsBp);

		assertThat(count).hasValue(8);
		tsBp.assertNoError();
		tsBp.assertNotComplete();
		tsBp.assertNoValues();

		tsBp.request(Long.MAX_VALUE);

		tsBp.assertValueCount(count.get());
		tsBp.assertNoError();
		tsBp.assertComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEagerness9() {
		final AtomicInteger count = new AtomicInteger();
		Flux<Integer> source = Flux.just(1).doOnNext(t -> count.getAndIncrement()).hide();

		Flux.mergeSequential(source, source, source, source, source, source, source, source, source).subscribe(tsBp);

		assertThat(count).hasValue(9);
		tsBp.assertNoError();
		tsBp.assertNotComplete();
		tsBp.assertNoValues();

		tsBp.request(Long.MAX_VALUE);

		tsBp.assertValueCount(count.get());
		tsBp.assertNoError();
		tsBp.assertComplete();
	}

	@Test
	public void testMainError() {
		Flux.<Integer>error(new RuntimeException()).flatMapSequential(toJust).subscribe(ts);

		ts.assertNoValues();
		ts.assertError(RuntimeException.class);
		ts.assertNotComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInnerError() {
		Flux.mergeSequential(Flux.just(1), Flux.error(new RuntimeException())).subscribe(ts);

		ts.assertValues(1);
		ts.assertError(RuntimeException.class);
		ts.assertNotComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInnerEmpty() {
		Flux.mergeSequential(Flux.empty(), Flux.empty()).subscribe(ts);

		ts.assertNoValues();
		ts.assertNoError();
		ts.assertComplete();
	}

	@Test
	public void testMapperThrows() {
		Flux.just(1).flatMapSequential(t -> { throw new RuntimeException(); }).subscribe(ts);

		ts.assertNoValues();
		ts.assertNotComplete();
		ts.assertError(RuntimeException.class);
	}

	@Test
	public void testInvalidCapacityHint() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Flux.just(1).flatMapSequential(toJust, 0, Queues.SMALL_BUFFER_SIZE);
		});
	}

	@Test
	public void testInvalidMaxConcurrent() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Flux.just(1).flatMapSequential(toJust, Queues.SMALL_BUFFER_SIZE, 0);
		});
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testBackpressure() {
		Flux.mergeSequential(Flux.just(1), Flux.just(1)).subscribe(tsBp);

		tsBp.assertNoError();
		tsBp.assertNoValues();
		tsBp.assertNotComplete();

		tsBp.request(1);
		tsBp.assertValues(1);
		tsBp.assertNoError();
		tsBp.assertNotComplete();

		tsBp.request(1);
		tsBp.assertValues(1, 1);
		tsBp.assertNoError();
		tsBp.assertComplete();
	}

	@Test
	public void testAsynchronousRun() {
		Flux.range(1, 2).flatMapSequential(t -> Flux.range(1, 1000)
		                                            .subscribeOn(Schedulers.single())
		).publishOn(Schedulers.boundedElastic()).subscribe(ts);

		ts.await(Duration.ofSeconds(5));
		ts.assertNoError();
		ts.assertValueCount(2000);
	}

	@Test
	public void testReentrantWork() {
		final Sinks.Many<Integer> subject = Sinks.unsafe().many().multicast().directBestEffort();

		final AtomicBoolean once = new AtomicBoolean();

		subject.asFlux()
			   .flatMapSequential(Flux::just)
		       .doOnNext(t -> {
			       if (once.compareAndSet(false, true)) {
				       subject.emitNext(2, FAIL_FAST);
			       }
		       })
		       .subscribe(ts);

		subject.emitNext(1, FAIL_FAST);

		ts.assertNoError();
		ts.assertNotComplete();
		ts.assertValues(1, 2);
	}

	@Test
	public void testPrefetchIsBounded() {
		final AtomicInteger count = new AtomicInteger();

		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		Flux.just(1).hide()
		    .flatMapSequential(t -> Flux.range(1, Queues.SMALL_BUFFER_SIZE * 2)
		                                .doOnNext(t1 -> count.getAndIncrement())
		                                .hide())
		    .subscribe(ts);

		ts.assertNoError();
		ts.assertNoValues();
		ts.assertNotComplete();
		assertThat(count).hasValue(Queues.XS_BUFFER_SIZE);
	}

	@Test
	public void testMaxConcurrent5() {
		final List<Long> requests = new ArrayList<>();
		Flux.range(1, 100).doOnRequest(requests::add)
		    .flatMapSequential(toJust, 5, Queues.SMALL_BUFFER_SIZE)
		    .subscribe(ts);

		ts.assertNoError();
		ts.assertValueCount(100);
		ts.assertComplete();

		assertThat((long) requests.get(0)).isEqualTo(5);
		assertThat((long) requests.get(1)).isEqualTo(1);
		assertThat((long) requests.get(2)).isEqualTo(1);
		assertThat((long) requests.get(3)).isEqualTo(1);
		assertThat((long) requests.get(4)).isEqualTo(1);
		assertThat((long) requests.get(5)).isEqualTo(1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void maxConcurrencyAndPrefetch() {
		Flux<Integer> source = Flux.just(1);
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.mergeSequential(Arrays.asList(source, source, source), 1, 1)
		    .subscribe(ts);

		ts.assertValues(1, 1, 1);
		ts.assertNoError();
		ts.assertComplete();
	}

	@Test
	public void mergeSequentialPublisher() {
		Flux<Integer> source = Flux.just(1);
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.mergeSequential(Flux.just(source, source, source)).subscribe(ts);

		ts.assertValues(1, 1, 1);
		ts.assertNoError();
		ts.assertComplete();
	}

	@Test
	public void mergeSequentialMaxConcurrencyAndPrefetch() {
		Flux<Integer> source = Flux.just(1);
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.mergeSequential(Flux.just(source, source, source), 1, 1)
		    .subscribe(ts);

		ts.assertValues(1, 1, 1);
		ts.assertNoError();
		ts.assertComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void badPrefetch() throws Exception {
		Flux<Integer> source = Flux.just(1);
		try {
			Flux.mergeSequential(Arrays.asList(source, source, source), 1, -99);
		} catch (IllegalArgumentException ex) {
			assertThat(ex).hasMessage("prefetch > 0 required but it was -99");
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void mappingBadPrefetch() throws Exception {
		Flux<Integer> source = Flux.just(1);
		try {
			Flux.just(source, source, source).flatMapSequential(Flux.identityFunction(), 10, -99);
		} catch (IllegalArgumentException ex) {
			assertThat(ex).hasMessage("prefetch > 0 required but it was -99");
		}

	}

	@Test
	public void mergeSequentialZero() {
		Flux.mergeSequential(Collections.<Flux<Integer>>emptyList())
		        .subscribeWith(AssertSubscriber.create())
		        .assertComplete().assertValues();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void mergeSequentialOne() {
		Flux.mergeSequential(Arrays.asList(Flux.just(1)))
		        .subscribeWith(AssertSubscriber.create())
		        .assertComplete().assertValues(1);
	}

	@Test
	public void mergeSequentialTwo() {
		Flux.mergeSequential(Arrays.asList(Flux.just(1), Flux.just(2)))
		        .subscribeWith(AssertSubscriber.create())
		        .assertComplete().assertValues(1, 2);
	}

	@Test
	public void mergeSequentialTwoPrefetch() {
		StepVerifier.create(Flux.mergeSequential(128,
				Flux.just(1).concatWith(Flux.error(new Exception("test"))),
				Flux.just(2)))
		            .expectNext(1)
		            .verifyErrorMessage("test");
	}

	@Test
	public void mergeSequentialTwoDelayError() {
		StepVerifier.create(Flux.mergeSequentialDelayError(128,
				Flux.just(1).concatWith(Flux.error(new Exception("test"))),
				Flux.just(2)))
		            .expectNext(1, 2)
	                .verifyErrorMessage("test");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void mergeSequentialIterable() {
		Flux.mergeSequential(Arrays.asList(Flux.just(1), Flux.just(2)))
		        .subscribeWith(AssertSubscriber.create())
		        .assertComplete().assertValues(1, 2);
	}

	@Test
	public void mergeSequentialTwoDelayIterableError() {
		StepVerifier.create(Flux.mergeSequentialDelayError(
				Arrays.asList(Flux.just(1).concatWith(Flux.error(new Exception("test"))),
				Flux.just(2)), 128, 128))
		            .expectNext(1, 2)
		            .verifyErrorMessage("test");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void mergeSequentialPublisher2() {
		Flux.mergeSequential(Flux.just(Flux.just(1), Flux.just(2)))
		    .subscribeWith(AssertSubscriber.create())
		    .assertComplete().assertValues(1, 2);
	}

	@Test
	public void mergeSequentialTwoDelayPublisherError() {
		StepVerifier.create(Flux.mergeSequentialDelayError(
				Flux.just(Flux.just(1).concatWith(Flux.error(new Exception("test"))),
						Flux.just(2)), 128, 128))
		            .expectNext(1, 2)
		            .verifyErrorMessage("test");
	}

	@Test
	public void mergeSequentialLargeUnorderedEach100() {
		Scheduler scheduler = Schedulers.boundedElastic();
		AtomicBoolean comparisonFailure = new AtomicBoolean();
		long count = Flux.range(0, 500)
		                 .flatMapSequential(i -> {
			                 //ensure each pack of 100 is delayed in inverse order
			                 Duration sleep = Duration.ofMillis(600 - i % 100);
			                 return Mono.delay(sleep)
			                            .then(Mono.just(i))
			                            .subscribeOn(scheduler);
		                 })
		                 .zipWith(Flux.range(0, Integer.MAX_VALUE))
		                 .doOnNext(i -> {
			                 if (!Objects.equals(i.getT1(), i.getT2())) {
//				                 System.out.println(i);
				                 comparisonFailure.set(true);
			                 }
		                 })
		                 .count().block();

		assertThat(count).isEqualTo(500L);
		assertThat(comparisonFailure.get()).isFalse();
	}

	@Test
	public void mergeSequentialLargeBadQueueSize() {
		int prefetch = 32;
		int maxConcurrency = 256;
		Supplier<Queue<FluxMergeSequential.MergeSequentialInner<Integer>>> badQueueSupplier =
				Queues.get(Math.min(prefetch, maxConcurrency));

		FluxMergeSequential<Integer, Integer> fluxMergeSequential =
				new FluxMergeSequential<>(Flux.range(0, 500),
						Mono::just,
						maxConcurrency, prefetch, FluxConcatMap.ErrorMode.IMMEDIATE,
						badQueueSupplier);

		StepVerifier.create(fluxMergeSequential.zipWith(Flux.range(0, Integer.MAX_VALUE)))
		            .expectErrorMatches(e -> e instanceof IllegalStateException &&
		                e.getMessage().startsWith("Too many subscribers for fluxMergeSequential on item: ") &&
		                e.getMessage().endsWith("; subscribers: 32"))
		            .verify();
	}
	@Test
	public void mergeEmpty(){
		StepVerifier.create(Flux.mergeSequential())
		            .verifyComplete();
	}


	@Test
	public void mergeOne(){
		StepVerifier.create(Flux.mergeSequential(Flux.just(1)))
		            .expectNext(1)
		            .verifyComplete();
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void flatMapSequentialDelayErrorWithFluxError() {
		StepVerifier.create(
				Flux.just(
						Flux.just(1, 2),
						Flux.<Integer>error(new Exception("test")),
						Flux.just(3, 4))
				    .flatMapSequentialDelayError(f -> f, 4, 4))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void flatMapSequentialDelayErrorWithMonoError() {
		StepVerifier.create(
				Flux.just(
						Flux.just(1, 2),
						Mono.<Integer>error(new Exception("test")),
						Flux.just(3, 4))
				    .flatMapSequentialDelayError(f -> f, 4, 4))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void mergeSequentialDelayErrorWithFluxError() {
		StepVerifier.create(
				Flux.mergeSequentialDelayError(
						Flux.just(
								Flux.just(1, 2),
								Flux.error(new Exception("test")),
								Flux.just(3, 4))
				, 4, 4)
		)
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void mergeSequentialDelayErrorWithMonoError() {
		StepVerifier.create(
				Flux.mergeSequentialDelayError(
						Flux.just(
								Flux.just(1, 2),
								Mono.error(new Exception("test")),
								Flux.just(3, 4))
				, 4, 4)
		)
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	@Test
	public void cancellingSequentiallyMergedMonos() {
		AtomicInteger cancelCounter = new AtomicInteger(5);

		final Flux<Object> merge = Flux.mergeSequential(
				Mono.never().doOnCancel(() -> System.out.println("Cancelling #1, remaining " + cancelCounter.decrementAndGet())),
				Mono.never().doOnCancel(() -> System.out.println("Cancelling #2, remaining " + cancelCounter.decrementAndGet())),
				Mono.never().doOnCancel(() -> System.out.println("Cancelling #3, remaining " + cancelCounter.decrementAndGet())),
				Mono.never().doOnCancel(() -> System.out.println("Cancelling #4, remaining " + cancelCounter.decrementAndGet())),
				Mono.never().doOnCancel(() -> System.out.println("Cancelling #5, remaining " + cancelCounter.decrementAndGet())));

		merge.subscribe().dispose();

		assertThat(cancelCounter).as("cancellation remaining").hasValue(0);
	}

	@Test
	public void cancellingSequentiallyFlatMappedMonos() {
		AtomicInteger cancelCounter = new AtomicInteger(5);

		final Flux<Object> merge = Flux.range(1, 5)
				.flatMapSequential(i -> Mono.never()
				                            .doOnCancel(() -> System.out.println("Cancelling #" + i + ", remaining " + cancelCounter.decrementAndGet())));

		merge.subscribe().dispose();

		assertThat(cancelCounter).as("cancellation remaining").hasValue(0);
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.range(1, 5);
		FluxMergeSequential<Integer, Integer> test = new FluxMergeSequential<>(parent, t -> Flux.just(t), 3, 123, ErrorMode.END);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

    @Test
    public void scanMain() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxMergeSequential.MergeSequentialMain<Integer, Integer> test =
        		new FluxMergeSequential.MergeSequentialMain<>(actual, i -> Mono.just(i),
        				5, 123, ErrorMode.BOUNDARY, Queues.unbounded());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(5);
        test.subscribers.add(new FluxMergeSequential.MergeSequentialInner<>(test, 123));
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    public void scanInner() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxMergeSequential.MergeSequentialMain<Integer, Integer> main =
        		new FluxMergeSequential.MergeSequentialMain<Integer, Integer>(actual, i -> Mono.just(i),
        				5, 123, ErrorMode.IMMEDIATE, Queues.unbounded());
        FluxMergeSequential.MergeSequentialInner<Integer> inner =
        		new FluxMergeSequential.MergeSequentialInner<>(main, 123);
        Subscription parent = Operators.emptySubscription();
        inner.onSubscribe(parent);

        assertThat(inner.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        assertThat(inner.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(inner.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
        assertThat(inner.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        inner.queue = new ConcurrentLinkedQueue<>();
        inner.queue.add(1);
        assertThat(inner.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        assertThat(inner.scan(Scannable.Attr.ERROR)).isNull();
        assertThat(inner.scan(Scannable.Attr.TERMINATED)).isFalse();
        inner.queue.clear();
        inner.setDone();
        assertThat(inner.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(inner.scan(Scannable.Attr.CANCELLED)).isFalse();
        inner.cancel();
        assertThat(inner.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
