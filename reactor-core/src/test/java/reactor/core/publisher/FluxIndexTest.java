/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class FluxIndexTest extends FluxOperatorTest<Integer, Tuple2<Long, Integer>> {

	@Override
	protected Scenario<Integer, Tuple2<Long, Integer>> defaultScenarioOptions(
			Scenario<Integer, Tuple2<Long, Integer>> defaultOptions) {
		return super.defaultScenarioOptions(defaultOptions)
		            .fusionMode(Fuseable.NONE)
		            .producer(10, i -> i)
		            .receive(10, i -> Tuples.of((long) i, i));
	}

	@Override
	protected List<Scenario<Integer, Tuple2<Long, Integer>>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.index((i, v) -> {
					throw exception();
				})),

				scenario(f -> f.index((i, v) -> null))
		);
	}

	@Override
	protected List<Scenario<Integer, Tuple2<Long, Integer>>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(Flux::index),
				scenario(f -> f.index(Tuples::of))
		);
	}

	@Test
	public void defaultNormal() {
		AtomicLong counter = new AtomicLong(2);
		StepVerifier.create(
				Flux.range(0, 1000)
				    .hide()
					.index()
		)
		            .expectNoFusionSupport()
		            .expectNext(Tuples.of(0L, 0))
		            .expectNextMatches(t -> t.getT1() == t.getT2().longValue())
		            .thenConsumeWhile(t -> t.getT1() == t.getT2().longValue(),
				            it -> counter.incrementAndGet())
		            .expectComplete()
		            .verify();

		assertThat(counter).hasValue(1000);
	}

	@Test
	public void defaultBackpressured() {
		AtomicLong counter = new AtomicLong(4);

		StepVerifier.create(
				Flux.range(0, 1000)
				    .hide()
					.index()
		, 0)
		            .expectNoFusionSupport()
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

		assertThat(counter).hasValue(1000);
	}

	@Test
	public void defaultConditional() {
		AtomicLong counter = new AtomicLong(2);
		StepVerifier.create(
				Flux.range(0, 1000)
				    .hide()
				    .index()
				    .filter(it -> true)
		)
		            .expectNoFusionSupport()
		            .expectNext(Tuples.of(0L, 0))
		            .expectNextMatches(t -> t.getT1() == t.getT2().longValue())
		            .thenConsumeWhile(t -> t.getT1() == t.getT2().longValue(),
				            it -> counter.incrementAndGet())
		            .expectComplete()
		            .verify();

		assertThat(counter).hasValue(1000);
	}

	@Test
	public void customNormal() {
		AtomicLong counter = new AtomicLong(2);
		StepVerifier.create(
				Flux.range(0, 1000)
				    .hide()
					.index((i, v) -> Tuples.of("#" + (i + 1), v))
		)
		            .expectNoFusionSupport()
		            .expectNext(Tuples.of("#1", 0))
		            .expectNextMatches(t -> t.getT1().equals("#" + (t.getT2() + 1)))
		            .thenConsumeWhile(t -> t.getT1().equals("#" + (t.getT2() + 1)),
				            it -> counter.incrementAndGet())
		            .expectComplete()
		            .verify();

		assertThat(counter).hasValue(1000);
	}

	@Test
	public void customBackpressured() {
		AtomicLong counter = new AtomicLong(4);

		StepVerifier.create(
				Flux.range(0, 1000)
				    .hide()
				    .index((i, v) -> Tuples.of("#" + (i + 1), v))
		, 0)
		            .expectNoFusionSupport()
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

		assertThat(counter).hasValue(1000);
	}

	@Test
	public void customConditional() {
		AtomicLong counter = new AtomicLong(2);
		StepVerifier.create(
				Flux.range(0, 1000)
				    .hide()
					.index((i, v) -> Tuples.of("#" + (i + 1), v))
					.filter(it -> true)
		)
		            .expectNoFusionSupport()
		            .expectNext(Tuples.of("#1", 0))
		            .expectNextMatches(t -> t.getT1().equals("#" + (t.getT2() + 1)))
		            .thenConsumeWhile(t -> t.getT1().equals("#" + (t.getT2() + 1)),
				            it -> counter.incrementAndGet())
		            .expectComplete()
		            .verify();

		assertThat(counter).hasValue(1000);
	}

	@Test
	public void sourceNull() {
		//noinspection ConstantConditions
		assertThatNullPointerException()
				.isThrownBy(() -> new FluxIndex<>(null, (i, v) -> i))
				.withMessage(null);
	}

	@Test
	public void indexMapperNull() {
		Flux<String> source = Flux.just("foo", "bar");
		//noinspection ConstantConditions
		assertThatNullPointerException()
				.isThrownBy(() -> new FluxIndex<>(source, null))
				.withMessage("indexMapper must be non null");
	}

	@Test
	public void indexMapperReturnsNull() {
		Flux<String> source = Flux.just("foo", "bar");
		Flux<Tuple2<Integer, String>> test = new FluxIndex<>(source,
				(i, v) -> {
					if (i == 0L) return Tuples.of(0, v);
					return null;
				});

		StepVerifier.create(test)
		            .expectNext(Tuples.of(0, "foo"))
		            .verifyErrorMessage("indexMapper returned a null value at raw index 1 for value bar");
	}

	@Test
	public void indexMapperThrows() {
		Flux<String> source = Flux.just("foo", "bar");
		Flux<Tuple2<Integer, String>> test = new FluxIndex<>(source,
				(i, v) -> {
					if (i == 0L) return Tuples.of(0, v);
					throw new IllegalStateException("boom-" + i);
				});

		StepVerifier.create(test)
		            .expectNext(Tuples.of(0, "foo"))
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("boom-1"));
	}

	@Test
	public void doNotCallToString() {
		Flux<ThrowsOnToString> source = Flux.just(new ThrowsOnToString());
		Flux<Tuple2<Long, ThrowsOnToString>> test = new FluxIndex<>(source, Flux.tuple2Function());

		StepVerifier.create(test)
				.expectNextCount(1)
				.verifyComplete();
	}

	static class ThrowsOnToString {
		@Override
		public String toString() {
			throw new RuntimeException("should not be called");
		}
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxIndex<Integer, Tuple2<Long, Integer>> test = new FluxIndex<>(parent, Tuples::of);

	    assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber(){
		CoreSubscriber<Object> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxIndex.IndexSubscriber<Object, Tuple2<Long, Object>> test =
				new FluxIndex.IndexSubscriber<>(actual, Tuples::of);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
	    assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanConditionnalSubscriber(){
		@SuppressWarnings("unchecked")
		Fuseable.ConditionalSubscriber<Tuple2<Long, String>> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
		FluxIndex.IndexConditionalSubscriber<String, Tuple2<Long, String>> test =
				new FluxIndex.IndexConditionalSubscriber<>(actual, Tuples::of);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

}
