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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.core.publisher.FluxConcatMap.ErrorMode;
import reactor.core.publisher.FluxMergeSequential.MergeSequentialInner;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class FluxMergeSequentialTest {

	AssertSubscriber<Object> ts;
	AssertSubscriber<Object> tsBp;

	final Function<Integer, Flux<Integer>> toJust = Flux::just;

	final Function<Integer, Flux<Integer>> toRange = t -> Flux.range(t, 2);

	@Before
	public void before() {
		ts = new AssertSubscriber<>();
		tsBp = new AssertSubscriber<>(0L);
	}

	@Test
	public void normal() {
		StepVerifier.create(Flux.range(1, 5)
		                        .hide()
		                        .flatMapSequential(t -> Flux.range(t, 2)))
		            .expectNext(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		            .verifyComplete();
	}

	@Test
	public void normalFusedSync() {
		StepVerifier.create(Flux.range(1, 5)
		                        .flatMapSequential(t -> Flux.range(t, 2)))
		            .expectNext(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		            .verifyComplete();
	}

	@Test
	public void normalFusedAsync() {
		StepVerifier.create(Flux.range(1, 5)
		                        .subscribeWith(UnicastProcessor.create())
		                        .flatMapSequential(t -> Flux.range(t, 2)))
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
		DirectProcessor<Integer> main = DirectProcessor.create();
		final DirectProcessor<Integer> inner = DirectProcessor.create();

		AssertSubscriber<Integer> ts = main.flatMapSequentialDelayError(t -> inner, 32, 32)
		                                   .subscribeWith(AssertSubscriber.create());

		main.onNext(1);
		main.onNext(2);

		inner.onNext(2);

		ts.assertValues(2);

		main.onError(new RuntimeException("Forced failure"));

		ts.assertNoError();

		inner.onNext(3);
		inner.onComplete();

		ts.assertValues(2, 3, 2, 3)
		  .assertErrorMessage("Forced failure");
	}

	@Test
	public void mainErrorsImmediate() {
		DirectProcessor<Integer> main = DirectProcessor.create();
		final DirectProcessor<Integer> inner = DirectProcessor.create();

		AssertSubscriber<Integer> ts = main.flatMapSequential(t -> inner)
		                                   .subscribeWith(AssertSubscriber.create());

		main.onNext(1);
		main.onNext(2);

		inner.onNext(2);

		ts.assertValues(2);

		main.onError(new RuntimeException("Forced failure"));

		assertFalse("inner has subscribers?", inner.hasDownstreams());

		inner.onNext(3);
		inner.onComplete();

		ts.assertValues(2).assertErrorMessage("Forced failure");
	}

	@Test
	public void longEager() {

		Flux.range(1, 2 * QueueSupplier.SMALL_BUFFER_SIZE)
		        .flatMapSequential(v -> Flux.just(1))
		        .subscribeWith(AssertSubscriber.create())
		        .assertValueCount(2 * QueueSupplier.SMALL_BUFFER_SIZE)
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

		Assert.assertEquals(2, count.get());
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

		Assert.assertEquals(3, count.get());
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

		Assert.assertEquals(4, count.get());
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

		Assert.assertEquals(5, count.get());
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

		Assert.assertEquals(6, count.get());
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

		Assert.assertEquals(7, count.get());
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

		Assert.assertEquals(8, count.get());
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

		Assert.assertEquals(9, count.get());
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

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidCapacityHint() {
		Flux.just(1).flatMapSequential(toJust, 0, QueueSupplier.SMALL_BUFFER_SIZE);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidMaxConcurrent() {
		Flux.just(1).flatMapSequential(toJust, QueueSupplier.SMALL_BUFFER_SIZE, 0);
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
		).publishOn(Schedulers.elastic()).subscribe(ts);

		ts.await(Duration.ofSeconds(5));
		ts.assertNoError();
		ts.assertValueCount(2000);
	}

	@Test
	public void testReentrantWork() {
		final DirectProcessor<Integer> subject = DirectProcessor.create();

		final AtomicBoolean once = new AtomicBoolean();

		subject.flatMapSequential(Flux::just)
		       .doOnNext(t -> {
			       if (once.compareAndSet(false, true)) {
				       subject.onNext(2);
			       }
		       })
		       .subscribe(ts);

		subject.onNext(1);

		ts.assertNoError();
		ts.assertNotComplete();
		ts.assertValues(1, 2);
	}

	@Test
	public void testPrefetchIsBounded() {
		final AtomicInteger count = new AtomicInteger();

		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		Flux.just(1).hide()
		    .flatMapSequential(t -> Flux.range(1, QueueSupplier.SMALL_BUFFER_SIZE * 2)
		                                .doOnNext(t1 -> count.getAndIncrement())
		                                .hide())
		    .subscribe(ts);

		ts.assertNoError();
		ts.assertNoValues();
		ts.assertNotComplete();
		Assert.assertEquals(QueueSupplier.XS_BUFFER_SIZE, count.get());
	}

	@Test
	public void testMaxConcurrent5() {
		final List<Long> requests = new ArrayList<>();
		Flux.range(1, 100).doOnRequest(requests::add)
		    .flatMapSequential(toJust, 5, QueueSupplier.SMALL_BUFFER_SIZE)
		    .subscribe(ts);

		ts.assertNoError();
		ts.assertValueCount(100);
		ts.assertComplete();

		Assert.assertEquals(5, (long) requests.get(0));
		Assert.assertEquals(1, (long) requests.get(1));
		Assert.assertEquals(1, (long) requests.get(2));
		Assert.assertEquals(1, (long) requests.get(3));
		Assert.assertEquals(1, (long) requests.get(4));
		Assert.assertEquals(1, (long) requests.get(5));
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
			assertEquals("prefetch > 0 required but it was -99", ex.getMessage());
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void mappingBadPrefetch() throws Exception {
		Flux<Integer> source = Flux.just(1);
		try {
			Flux.just(source, source, source).flatMapSequential(Flux.identityFunction(), 10, -99);
		} catch (IllegalArgumentException ex) {
			assertEquals("prefetch > 0 required but it was -99", ex.getMessage());
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
		Scheduler scheduler = Schedulers.elastic();
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

		assertEquals(500L, count);
		assertFalse(comparisonFailure.get());
	}

	@Test
	public void mergeSequentialLargeBadQueueSize() {
		int prefetch = 32;
		int maxConcurrency = 256;
		Supplier<Queue<FluxMergeSequential.MergeSequentialInner<Integer>>> badQueueSupplier =
				QueueSupplier.get(Math.min(prefetch, maxConcurrency));

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

    @Test
    public void scanMain() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxMergeSequential.MergeSequentialMain<Integer, Integer> test =
        		new FluxMergeSequential.MergeSequentialMain<Integer, Integer>(actual, i -> Mono.just(i),
        				5, 123, ErrorMode.BOUNDARY, QueueSupplier.<MergeSequentialInner<Integer>>unbounded());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.BooleanAttr.DELAY_ERROR)).isTrue();
        test.requested = 35;
        assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(5);
        test.subscribers.add(new FluxMergeSequential.MergeSequentialInner<>(test, 123));
        assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);

        assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isSameAs(test.error);
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }

    @Test
    public void scanInner() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxMergeSequential.MergeSequentialMain<Integer, Integer> main =
        		new FluxMergeSequential.MergeSequentialMain<Integer, Integer>(actual, i -> Mono.just(i),
        				5, 123, ErrorMode.IMMEDIATE, QueueSupplier.<MergeSequentialInner<Integer>>unbounded());
        FluxMergeSequential.MergeSequentialInner<Integer> inner =
        		new FluxMergeSequential.MergeSequentialInner<>(main, 123);
        Subscription parent = Operators.emptySubscription();
        inner.onSubscribe(parent);

        assertThat(inner.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(main);
        assertThat(inner.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(inner.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(123);
        inner.queue = new ConcurrentLinkedQueue<>();
        inner.queue.add(1);
        assertThat(inner.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);

        assertThat(inner.scan(Scannable.ThrowableAttr.ERROR)).isNull();
        assertThat(inner.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        inner.queue.clear();
        inner.setDone();
        assertThat(inner.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

        assertThat(inner.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        inner.cancel();
        assertThat(inner.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}