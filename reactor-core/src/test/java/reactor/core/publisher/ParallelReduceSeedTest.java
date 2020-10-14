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
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.ParallelOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class ParallelReduceSeedTest extends ParallelOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.receive(4, i -> item(0));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.reduce(() -> item(0), (a, b) -> a))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.reduce(() -> "", (a, b) -> null)),

				scenario(f -> f.reduce(() -> null, (a, b) -> a + b))
		);
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
	public void failInitial() {
		Supplier<Integer> as = () -> {
			throw new RuntimeException("test");
		};

		StepVerifier.create(Flux.range(1, 10)
		                        .parallel(3)
		                        .reduce(as, (a, b) -> b + a))
		            .verifyErrorMessage("test");
	}

	@Test
	public void failCombination() {
		StepVerifier.create(Flux.range(1, 10)
		                        .parallel(3)
		                        .reduce(() -> 0, (a, b) -> {
		                        	throw new RuntimeException("test");
		                        }))
		            .verifyErrorMessage("test");
	}

	@Test
	public void testPrefetch() {
		assertThat(Flux.range(1, 10)
		               .parallel(3)
		               .reduce(() -> 0, (a, b) -> a + b)
		               .getPrefetch()).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void parallelism() {
		ParallelFlux<Integer> source = Flux.just(500, 300).parallel(10);
		ParallelReduceSeed<Integer, String> test = new ParallelReduceSeed<>(source, () -> "", (s, i) -> s + i);

		assertThat(test.parallelism())
				.isEqualTo(source.parallelism())
				.isEqualTo(10);
	}

	@Test
	public void scanOperator() {
		ParallelFlux<Integer> source = Flux.just(500, 300).parallel(10);
		ParallelReduceSeed<Integer, String> test = new ParallelReduceSeed<>(source, () -> "", (s, i) -> s + i);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		ParallelFlux<Integer> source = Flux.just(500, 300).parallel(10);

		LambdaSubscriber<String> subscriber = new LambdaSubscriber<>(null, e -> {
		}, null, null);

		ParallelReduceSeed.ParallelReduceSeedSubscriber<Integer, String> test = new ParallelReduceSeed.ParallelReduceSeedSubscriber<>(
				subscriber, "", (s, i) -> s + i);

		@SuppressWarnings("unchecked")
		final CoreSubscriber<Integer>[] testSubscribers = new CoreSubscriber[1];
		testSubscribers[0] = test;
		source.subscribe(testSubscribers);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(subscriber);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		test.state = Operators.MonoSubscriber.HAS_REQUEST_HAS_VALUE;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		test.state = Operators.MonoSubscriber.HAS_REQUEST_NO_VALUE;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();

		test.state = Operators.MonoSubscriber.NO_REQUEST_HAS_VALUE;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}
