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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

import static org.junit.Assert.*;

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
		Flux.range(1, 5)
		        .flatMapSequential(new Function<Integer, Publisher<Integer>>() {
			        @Override
			        public Publisher<Integer> apply(Integer t) {
				        return Flux.range(t, 2);
			        }
		        })
		        .subscribeWith(AssertSubscriber.create())
		        .assertComplete()
		    .assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = Flux.range(1, 5)
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
		        .flatMapSequential(t -> Flux.range(t, 2), true, 32, 32)
		        .subscribeWith(AssertSubscriber.create())
		        .assertComplete().assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
	}

	@Test
	public void normalDelayEndBackpressured() {
		AssertSubscriber<Integer> ts = Flux.range(1, 5)
		                                   .flatMapSequential(t -> Flux.range(t, 2), true, 32, 32)
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

		AssertSubscriber<Integer> ts = main.flatMapSequential(t -> inner, true, 32, 32)
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
		        .flatMapSequential(new Function<Integer, Publisher<Integer>>() {
			        @Override
			        public Publisher<Integer> apply(Integer v) {
				        return Flux.just(1);
			        }
		        })
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

		Flux.just(1).flatMapSequential(t -> Flux.range(1, QueueSupplier.SMALL_BUFFER_SIZE * 2)
		                                        .doOnNext(t1 -> count.getAndIncrement())
		                                        .hide())
		    .subscribe(ts);

		ts.assertNoError();
		ts.assertNoValues();
		ts.assertNotComplete();
		Assert.assertEquals(QueueSupplier.SMALL_BUFFER_SIZE, count.get());
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
	public void MaxConcurrencyAndPrefetch() {
		Flux<Integer> source = Flux.just(1);
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.mergeSequential(Arrays.asList(source, source, source), false, 1, 1)
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

		Flux.mergeSequential(Flux.just(source, source, source), false, 1, 1)
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
			Flux.mergeSequential(Arrays.asList(source, source, source), false, 1, -99);
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

	@SuppressWarnings("unchecked")
	@Test
	public void mergeSequentialTwo() {
		Flux.mergeSequential(Arrays.asList(Flux.just(1), Flux.just(2)))
		        .subscribeWith(AssertSubscriber.create())
		        .assertComplete().assertValues(1, 2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void mergeSequentialIterable() {
		Flux.mergeSequential(Arrays.asList(Flux.just(1), Flux.just(2)))
		        .subscribeWith(AssertSubscriber.create())
		        .assertComplete().assertValues(1, 2);
	}

}