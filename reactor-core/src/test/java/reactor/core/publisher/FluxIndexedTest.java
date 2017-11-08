/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import reactor.core.Fuseable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * @author Simon Baslé
 */
public class FluxIndexedTest extends FluxOperatorTest<Integer, Tuple2<Long, Integer>> {

	@Override
	protected Scenario<Integer, Tuple2<Long, Integer>> defaultScenarioOptions(
			Scenario<Integer, Tuple2<Long, Integer>> defaultOptions) {
		return super.defaultScenarioOptions(defaultOptions)
		            .producer(10, i -> i)
		            .receive(10, i -> Tuples.of((long) i, i));
	}

	@Override
	protected List<Scenario<Integer, Tuple2<Long, Integer>>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.indexed((i, v) -> {
					throw exception();
				})),

				scenario(f -> f.indexed((i, v) -> null))
		);
	}

	@Override
	protected List<Scenario<Integer, Tuple2<Long, Integer>>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.indexed((i, v) -> i))
		);
	}


	@Test
	public void apiDefaultNormal() {
		AtomicLong counter = new AtomicLong(2);
		StepVerifier.create(
				Flux.range(0, 1000)
					.indexed()
		)
		            .expectNext(Tuples.of(0L, 0))
		            .expectNextMatches(t -> t.getT1() == t.getT2().longValue())
		            .thenConsumeWhile(t -> t.getT1() == t.getT2().longValue(),
				            it -> counter.incrementAndGet())
		            .expectComplete()
		            .verify();

		assertThat(counter.get()).isEqualTo(1000);
	}

	@Test
	public void apiDefaultBackpressured() {
		AtomicLong counter = new AtomicLong(4);

		StepVerifier.create(
				Flux.range(0, 1000)
					.indexed()
		, 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenRequest(1)
		            .expectNext(Tuples.of(0L, 0))
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenRequest(3)
		            .expectNext(Tuples.of(1L, 1))
		            .expectNext(Tuples.of(2L, 2))
		            .expectNext(Tuples.of(3L, 3))
		            .thenRequest(Long.MAX_VALUE)
		            .thenConsumeWhile(t -> t.getT1() == t.getT2().longValue(),
				            it -> counter.incrementAndGet())
		            .verifyComplete();

		assertThat(counter.get()).isEqualTo(1000);
	}

	@Test
	public void apiCustomNormal() {
		AtomicLong counter = new AtomicLong(2);
		StepVerifier.create(
				Flux.range(0, 1000)
					.indexed((i, v) -> "#" + (i + 1))
		)
		            .expectNext(Tuples.of("#1", 0))
		            .expectNextMatches(t -> t.getT1().equals("#" + (t.getT2() + 1)))
		            .thenConsumeWhile(t -> t.getT1().equals("#" + (t.getT2() + 1)),
				            it -> counter.incrementAndGet())
		            .expectComplete()
		            .verify();

		assertThat(counter.get()).isEqualTo(1000);
	}

	@Test
	public void apiCustomBackpressured() {
		AtomicLong counter = new AtomicLong(4);

		StepVerifier.create(
				Flux.range(0, 1000)
				    .indexed((i, v) -> "#" + (i + 1))
		, 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenRequest(1)
		            .expectNext(Tuples.of("#1", 0))
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenRequest(3)
		            .expectNext(Tuples.of("#2", 1))
		            .expectNext(Tuples.of("#3", 2))
		            .expectNext(Tuples.of("#4", 3))
		            .thenRequest(Long.MAX_VALUE)
		            .thenConsumeWhile(t -> t.getT1().equals("#" + (t.getT2() + 1)),
				            it -> counter.incrementAndGet())
		            .verifyComplete();

		assertThat(counter.get()).isEqualTo(1000);
	}

	@Test
	public void sourceNull() {
		//noinspection ConstantConditions
		assertThatNullPointerException()
				.isThrownBy(() -> new FluxIndexed<>(null, (i, v) -> i))
				.withMessage(null);
	}

	@Test
	public void indexMapperNull() {
		Flux<String> source = Flux.just("foo", "bar");
		//noinspection ConstantConditions
		assertThatNullPointerException()
				.isThrownBy(() -> new FluxIndexed<>(source, null))
				.withMessage("indexMapper must be non null");
	}

	@Test
	public void indexMapperReturnsNull() {
		Flux<String> source = Flux.just("foo", "bar");
		Flux<Tuple2<Integer, String>> test = new FluxIndexed<>(source,
				(i, v) -> {
					if (i == 0L) return 0;
					return null;
				});

		StepVerifier.create(test)
		            .expectNext(Tuples.of(0, "foo"))
		            .verifyErrorMessage("indexMapper returned a null value at raw index 1 for value bar");
	}

	@Test
	public void indexMapperThrows() {
		Flux<String> source = Flux.just("foo", "bar");
		Flux<Tuple2<Integer, String>> test = new FluxIndexed<>(source,
				(i, v) -> {
					if (i == 0L) return 0;
					throw new IllegalStateException("boom-" + i);
				});

		StepVerifier.create(test)
		            .expectNext(Tuples.of(0, "foo"))
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("boom-1"));
	}
}