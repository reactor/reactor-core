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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoStreamCollectorTest {


	static class TestCollector<T, A, R> implements Collector<T, A, R> {

		private final Supplier<A>      supplier;
		private final BiConsumer<A, T> accumulator;
		private final Function<A, R>   finisher;

		TestCollector(Supplier<A> supplier, BiConsumer<A, T> accumulator, Function<A, R> finisher) {
			this.supplier = supplier;
			this.accumulator = accumulator;
			this.finisher = finisher;
		}

		@Override
		public Supplier<A> supplier() {
			return this.supplier;
		}

		@Override
		public BiConsumer<A, T> accumulator() {
			return this.accumulator;
		}

		@Override
		public BinaryOperator<A> combiner() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Function<A, R> finisher() {
			return this.finisher;
		}

		@Override
		public Set<Characteristics> characteristics() {
			return Collections.emptySet();
		}
	}

	@Test
	public void collectToList() {
		Mono<List<Integer>> source = Flux.range(1, 5).collect(Collectors.toList());

		for (int i = 0; i < 5; i++) {
			AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();
			source.subscribe(ts);

			ts.assertValues(Arrays.asList(1, 2, 3, 4, 5))
			.assertNoError()
			.assertComplete();
		}
	}

	@Test
	public void collectToSet() {
		Mono<Set<Integer>> source = Flux.just(1).repeat(5).collect(Collectors.toSet());

		for (int i = 0; i < 5; i++) {
			AssertSubscriber<Set<Integer>> ts = AssertSubscriber.create();
			source.subscribe(ts);

			ts.assertValues(Collections.singleton(1))
			.assertNoError()
			.assertComplete();
		}
	}

	@Test
	public void scanStreamCollectorSubscriber() {
		CoreSubscriber<List<String>>
				actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		Collector<String, ?, List<String>> collector = Collectors.toList();
		@SuppressWarnings("unchecked")
		BiConsumer<Integer, String> accumulator = (BiConsumer<Integer, String>) collector.accumulator();
		@SuppressWarnings("unchecked")
		Function<Integer, List<String>> finisher = (Function<Integer, List<String>>) collector.finisher();

		MonoStreamCollector.StreamCollectorSubscriber<String, Integer, List<String>> test = new MonoStreamCollector.StreamCollectorSubscriber<>(
				actual, 1, accumulator, finisher);
		Subscription parent = Operators.emptySubscription();

		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void discardValueAndIntermediateListElementsOnAccumulatorFailure() {
		Collector<Integer, List<Integer>, Set<Integer>> collector = new TestCollector<>(ArrayList::new, (l, i) -> {
			if (i == 2) throw new IllegalStateException("accumulator: boom");
			l.add(i);
		}, HashSet::new);

		Flux.range(1, 4)
		    .collect(collector)
		    .as(StepVerifier::create)
		    .expectErrorMessage("accumulator: boom")
		    .verifyThenAssertThat()
		    .hasDiscardedExactly(1, 2);
	}

	@Test
	public void discardValueAndIntermediateMapOnAccumulatorFailure() {
		Collector<Integer, Map<Integer, String>, Set<Integer>> collector = new TestCollector<>(HashMap::new, (m, i) -> {
			if (i == 2) throw new IllegalStateException("accumulator: boom");
			m.put(i, String.valueOf(i));
		}, Map::keySet);

		Flux.range(1, 4)
		    .collect(collector)
		    .as(StepVerifier::create)
		    .expectErrorMessage("accumulator: boom")
		    .verifyThenAssertThat()
		    .hasDiscardedExactly(Collections.singletonMap(1, "1"), 2);
	}

	@Test
	public void discardIntermediateListElementsOnError() {
		final Collector<Integer, ?, Collection<Integer>> collector = Collectors.toCollection(ArrayList::new);

		Mono<Collection<Integer>> test =
				Flux.range(1, 10)
				    .hide()
				    .map(i -> {
					    if (i == 5) {
						    throw new IllegalStateException("boom");
					    }
					    return i;
				    })
				    .collect(collector);

		StepVerifier.create(test)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3, 4);
	}

	@Test
	public void discardIntermediateListElementsOnCancel() {
		final Collector<Long, ?, Collection<Long>> collector = Collectors.toCollection(ArrayList::new);

		StepVerifier.withVirtualTime(() ->
				Flux.interval(Duration.ofMillis(100))
				    .take(10)
				    .collect(collector)
		)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(210))
		            .thenCancel()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(0L, 1L);
	}

	@Test
	public void discardIntermediateListElementsOnFinisherFailure() {
		Collector<Integer, List<Integer>, Set<Integer>> collector = new TestCollector<>(ArrayList::new, List::add, m -> { throw new IllegalStateException("finisher: boom"); });

		Mono<Set<Integer>> test =
				Flux.range(1, 4)
				    .hide()
				    .collect(collector);

		StepVerifier.create(test)
		            .expectErrorMessage("finisher: boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3, 4);
	}

	@Test
	public void discardIntermediateMapOnError() {
		Collector<Integer, ?, Map<Integer, String>> collector = Collectors.toMap(Function.identity(), String::valueOf);
		List<Object> discarded = new ArrayList<>();

		Mono<Map<Integer, String>> test =
				Flux.range(1, 10)
				    .hide()
				    .map(i -> {
					    if (i == 5) {
						    throw new IllegalStateException("boom");
					    }
					    return i;
				    })
				    .collect(collector)
				    .doOnDiscard(Object.class, discarded::add);

		StepVerifier.create(test)
		            .expectErrorMessage("boom")
		            .verify();

		assertThat(discarded).doesNotHaveAnyElementsOfTypes(Integer.class)
		                     .hasOnlyElementsOfType(Map.class)
		                     .hasSize(1);
		//noinspection unchecked
		assertThat(((Map) discarded.get(0)).keySet()).containsExactly(1, 2, 3, 4);
	}

	@Test
	public void discardIntermediateMapOnCancel() {
		Collector<Long, Map<Long, String>, Set<Long>> collector = new TestCollector<>(HashMap::new,
				(m, i) -> m.put(i, String.valueOf(i)), Map::keySet);
		List<Object> discarded = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Flux.interval(Duration.ofMillis(100))
				    .take(10)
				    .collect(collector)
				    .doOnDiscard(Object.class, discarded::add))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(210))
		            .thenCancel()
		            .verify();

		assertThat(discarded).doesNotHaveAnyElementsOfTypes(Long.class)
		                     .hasOnlyElementsOfType(Map.class)
		                     .hasSize(1);
		//noinspection unchecked
		assertThat((Map) discarded.get(0)).containsOnlyKeys(0L, 1L);
	}

	@Test
	public void discardIntermediateMapOnFinisherFailure() {
		Collector<Integer, Map<Integer, String>, Set<Integer>> collector = new TestCollector<>(HashMap::new,
				(m, i) -> m.put(i, String.valueOf(i)), m -> { throw new IllegalStateException("finisher: boom"); });
		List<Object> discarded = new ArrayList<>();

		Mono<Set<Integer>> test =
				Flux.range(1, 4)
				    .hide()
				    .collect(collector)
				    .doOnDiscard(Object.class, discarded::add);

		StepVerifier.create(test)
		            .expectErrorMessage("finisher: boom")
		            .verify();

		assertThat(discarded).doesNotHaveAnyElementsOfTypes(Integer.class, Set.class)
		                     .hasOnlyElementsOfType(Map.class)
		                     .hasSize(1);
		//noinspection unchecked
		assertThat(((Map) discarded.get(0))).containsOnlyKeys(1, 2, 3, 4);
	}

}
