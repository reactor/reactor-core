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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class ParallelFluxTest {

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
	public void reduceFull() {
		for (int i = 1;
		     i <= Runtime.getRuntime()
		                 .availableProcessors() * 2;
		     i++) {
			AssertSubscriber<Integer> ts = AssertSubscriber.create();

			Flux.range(1, 10)
			    .parallel(i)
			    .reduce((a, b) -> a + b)
			    .subscribe(ts);

			ts.assertValues(55);
		}
	}

	@Test
	public void parallelReduceFull() {
		int m = 100_000;
		for (int n = 1; n <= m; n *= 10) {
//            System.out.println(n);
			for (int i = 1;
			     i <= Runtime.getRuntime()
			                 .availableProcessors();
			     i++) {
//                System.out.println("  " + i);

				Scheduler scheduler = Schedulers.newParallel("test", i);

				try {
					AssertSubscriber<Long> ts = AssertSubscriber.create();

					Flux.range(1, n)
					    .map(v -> (long) v)
					    .parallel(i)
					    .runOn(scheduler)
					    .reduce((a, b) -> a + b)
					    .subscribe(ts);

					ts.await(Duration.ofSeconds(500));

					long e = ((long) n) * (1 + n) / 2;

					ts.assertValues(e);
				}
				finally {
					scheduler.dispose();
				}
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
	public void collect() {
		Supplier<List<Integer>> as = () -> new ArrayList<>();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux.range(1, 10)
		    .parallel()
		    .collect(as, (a, b) -> a.add(b))
		    .sequential()
		    .flatMapIterable(v -> v)
		    .log()
		    .subscribe(ts);

		ts.assertContainValues(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
		  .assertNoError()
		  .assertComplete();
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
	public void collectAsyncFused() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Scheduler scheduler = Schedulers.newParallel("test", 3);

		Flux.range(1, 100000)
		    .parallel(3)
		    .runOn(scheduler)
		    .collect(ArrayList::new, ArrayList::add)
		    .sequential()
		    .reduce(0, (a, b) -> a + b.size())
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));
		ts.assertValues(100_000)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void collectAsync() {
		Scheduler s = Schedulers.newParallel("test", 3);
		Supplier<List<Integer>> as = () -> new ArrayList<>();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 100000)
		    .hide()
		    .parallel(3)
		    .runOn(s)
		    .collect(as, (a, b) -> a.add(b))
		    .doOnNext(v -> System.out.println(v.size()))
		    .sequential()
		    .reduce(0, (a, b) -> a + b.size())
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));
		ts.assertValues(100_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void collectAsync2() {
		Scheduler s = Schedulers.newParallel("test", 3);
		Supplier<List<Integer>> as = () -> new ArrayList<>();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 100000)
		    .hide()
		    .publishOn(s)
		    .parallel(3)
		    .runOn(s)
		    .hide()
		    .collect(as, (a, b) -> a.add(b))
		    .doOnNext(v -> System.out.println(v.size()))
		    .sequential()
		    .reduce(0, (a, b) -> a + b.size())
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));
		ts.assertValues(100_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void collectAsync3() {
		Scheduler s = Schedulers.newParallel("test", 3);
		Supplier<List<Integer>> as = () -> new ArrayList<>();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 100000)
		    .hide()
		    .publishOn(s)
		    .parallel(3)
		    .runOn(s)
		    .filter(t -> true)
		    .collect(as, (a, b) -> a.add(b))
		    .doOnNext(v -> System.out.println(v.size()))
		    .groups()
		    .flatMap(v -> v)
		    .reduce(0, (a, b) -> b.size() + a)
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));
		ts.assertValues(100_000)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void collectAsync3Fused() {
		Scheduler s = Schedulers.newParallel("test", 3);
		Supplier<List<Integer>> as = () -> new ArrayList<>();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 100000)
		    .publishOn(s)
		    .parallel(3)
		    .runOn(s)
		    .collect(as, (a, b) -> a.add(b))
		    .doOnNext(v -> System.out.println(v.size()))
		    .groups()
		    .flatMap(v -> v)
		    .reduce(0, (a, b) -> b.size() + a)
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));
		ts.assertValues(100_000)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void collectAsync3Take() {
		Scheduler s = Schedulers.newParallel("test", 4);
		Supplier<List<Integer>> as = () -> new ArrayList<>();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 100000)
		    .publishOn(s)
		    .parallel(3)
		    .runOn(s)
		    .collect(as, (a, b) -> a.add(b))
		    .doOnNext(v -> System.out.println(v.size()))
		    .groups()
		    .flatMap(v -> v)
		    .reduce(0, (a, b) -> b.size() + a)
		    .subscribe(ts);

		ts.await(Duration.ofSeconds(5));
		ts.assertValues(100_000)
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
			                                 if (s.isOnNext())
				                                 values.add(s.get());
		                                 });

		//we use a lambda subscriber and latch to avoid using `sequential`
		CountDownLatch latch = new CountDownLatch(2);
		flux.subscribe(v -> {}, e -> latch.countDown(), latch::countDown);

		assertTrue(latch.await(2, TimeUnit.SECONDS));

		assertThat(signals.size(), is(5));
		assertThat("first onNext signal isn't first value", signals.get(0).get(), is(1));
		assertThat("second onNext signal isn't last value", signals.get(1).get(), is(2));
		assertTrue("onComplete for rail 1 expected", signals.get(2).isOnComplete());
		assertTrue("onComplete for rail 2 expected", signals.get(3).isOnComplete());
		assertTrue("onComplete for rail 3 expected", signals.get(4).isOnComplete());
		assertThat("1st onNext value unexpected", values.get(0), is(1));
		assertThat("2nd onNext value unexpected", values.get(1), is(2));
	}

	@Test
	public void testDoOnEachSignalWithError() throws InterruptedException {
		List<Signal<Integer>> signals = Collections.synchronizedList(new ArrayList<>(4));
		ParallelFlux<Integer> flux = Flux.<Integer>error(new IllegalArgumentException("boom"))
		                              .parallel(2)
		                              .runOn(Schedulers.parallel())
		                              .doOnEach(signals::add);

		//we use a lambda subscriber and latch to avoid using `sequential`
		CountDownLatch latch = new CountDownLatch(2);
		flux.subscribe(v -> {}, e -> latch.countDown(), latch::countDown);

		assertTrue(latch.await(2, TimeUnit.SECONDS));

		assertThat(signals.toString(), signals.size(), is(2));
		assertTrue("rail 1 onError expected", signals.get(0).isOnError());
		assertTrue("rail 2 onError expected", signals.get(1).isOnError());
		assertThat("plain exception rail 1 expected", signals.get(0).getThrowable().getMessage(), is("boom"));
		assertThat("plain exception rail 2 expected", signals.get(1).getThrowable().getMessage(), is("boom"));
	}

	@Test(expected = NullPointerException.class)
	public void testDoOnEachSignalNullConsumer() {
		Flux.just(1).parallel().doOnEach(null);
	}

	@Test
	public void testDoOnEachSignalToSubscriber() {
		AssertSubscriber<Integer> peekSubscriber = AssertSubscriber.create();
		ParallelFlux<Integer> flux = Flux.just(1, 2)
		                         .parallel(3)
		                         .doOnEach(s -> s.accept(peekSubscriber));

		//we use a lambda subscriber and latch to avoid using `sequential`
		flux.subscribe(v -> {});

		peekSubscriber.assertNotSubscribed();
		peekSubscriber.assertValues(1, 2);

		Assertions.assertThatExceptionOfType(AssertionError.class)
		          .isThrownBy(peekSubscriber::assertComplete)
		          .withMessage("Multiple completions: 3");
	}


	@Test
	public void transformGroup() {
		Set<Integer> values = new ConcurrentSkipListSet<>();

		Flux<Integer> flux = Flux.range(1, 10)
		                         .parallel(3)
		                         .runOn(Schedulers.parallel())
		                         .doOnNext(values::add)
		                         .transformGroup(p -> p.log("rail" + p.key())
		                                               .map(i -> (p.key() + 1) * 100 + i))
		                         .sequential();

		StepVerifier.create(flux.sort())
		            .assertNext(i -> Assertions.assertThat(i - 100).isBetween(1, 10))
		            .thenConsumeWhile(i -> i / 100 == 1)
		            .assertNext(i -> Assertions.assertThat(i - 200).isBetween(1, 10))
		            .thenConsumeWhile(i -> i / 100 == 2)
		            .assertNext(i -> Assertions.assertThat(i - 300).isBetween(1, 10))
		            .thenConsumeWhile(i -> i / 100 == 3)
		            .verifyComplete();

		Assertions.assertThat(values)
				.hasSize(10)
	            .contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}
}
