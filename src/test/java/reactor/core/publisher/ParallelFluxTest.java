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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.core.test.TestSubscriber;

public class ParallelFluxTest {

	@Test
	public void sequentialMode() {
		Flux<Integer> source = Flux.range(1, 1_000_000)
		                           .hide();
		for (int i = 1; i < 33; i++) {
			Flux<Integer> result = ParallelFlux.from(source, i)
			                                   .map(v -> v + 1)
			                                   .sequential();

			TestSubscriber<Integer> ts = TestSubscriber.create();

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

			TestSubscriber<Integer> ts = TestSubscriber.create();

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

				TestSubscriber<Integer> ts = TestSubscriber.create();

				result.subscribe(ts);

				ts.await(Duration.ofSeconds(10));

				ts.assertSubscribed()
				  .assertValueCount(1_000_000)
				  .assertComplete()
				  .assertNoError();
			}
			finally {
				scheduler.shutdown();
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

				TestSubscriber<Integer> ts = TestSubscriber.create();

				result.subscribe(ts);

				ts.await(Duration.ofSeconds(10));

				ts.assertSubscribed()
				  .assertValueCount(1_000_000)
				  .assertComplete()
				  .assertNoError();
			}
			finally {
				scheduler.shutdown();
			}
		}

	}

	@Test
	public void reduceFull() {
		for (int i = 1;
		     i <= Runtime.getRuntime()
		                 .availableProcessors() * 2;
		     i++) {
			TestSubscriber<Integer> ts = TestSubscriber.create();

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
					TestSubscriber<Long> ts = TestSubscriber.create();

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
					scheduler.shutdown();
				}
			}
		}
	}

	@Test
	public void collectSortedList() {
		TestSubscriber<List<Integer>> ts = TestSubscriber.create();

		Flux.just(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
		    .parallel()
		    .collectSortedList(Comparator.naturalOrder())
		    .subscribe(ts);

		ts.assertValues(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
	}

	@Test
	public void sorted() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

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

		TestSubscriber<Integer> ts = TestSubscriber.create();
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
		TestSubscriber<Integer> ts = TestSubscriber.create();

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
		TestSubscriber<Integer> ts = TestSubscriber.create();

		ParallelFlux.from(Flux.range(1, 5), Flux.range(6, 5))
		            .sequential()
		            .subscribe(ts);

		ts.assertContainValues(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void concatMapUnordered() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

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
		TestSubscriber<Integer> ts = TestSubscriber.create();

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
		TestSubscriber<Integer> ts = TestSubscriber.create();
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

		TestSubscriber<Integer> ts = TestSubscriber.create();

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

		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 100000)
		    .hide()
		    .publishOn(s)
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
	public void collectAsync3() {
		Scheduler s = Schedulers.newParallel("test", 3);
		Supplier<List<Integer>> as = () -> new ArrayList<>();

		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 100000)
		    .hide()
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
	public void collectAsync3Fused() {
		Scheduler s = Schedulers.newParallel("test", 3);
		Supplier<List<Integer>> as = () -> new ArrayList<>();

		TestSubscriber<Integer> ts = TestSubscriber.create();

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

		TestSubscriber<Integer> ts = TestSubscriber.create();

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
}
