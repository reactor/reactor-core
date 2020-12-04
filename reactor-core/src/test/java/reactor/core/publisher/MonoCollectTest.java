/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.MonoCollect.CollectSubscriber;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoCollectTest {

	static final Logger LOGGER = Loggers.getLogger(MonoCollectListTest.class);


	@Test
	public void nullSource() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoCollect<>(null, () -> 1, (a, b) -> {
			});
		});
	}

	@Test
	public void nullSupplier() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never().collect(null, (a, b) -> {
			});
		});
	}

	@Test
	public void nullAction() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never().collect(() -> 1, null);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<ArrayList<Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 10).collect(ArrayList<Integer>::new, ArrayList::add).subscribe(ts);

		ts.assertValues(new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<ArrayList<Integer>> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).collect(ArrayList<Integer>::new, ArrayList::add).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void supplierThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 10).collect(() -> {
			throw new RuntimeException("forced failure");
		}, (a, b) -> {
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorWith( e -> assertThat(e).hasMessageContaining("forced failure"))
		  .assertNotComplete();

	}

	@Test
	public void supplierReturnsNull() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 10).collect(() -> null, (a, b) -> {
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void actionThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 10).collect(() -> 1, (a, b) -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> assertThat(e).hasMessageContaining("forced failure"))
		  .assertNotComplete();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> source = Flux.just(1, 2, 3);
		MonoCollect<Integer, List<Integer>> test = new MonoCollect<>(source, ArrayList::new, (a, b) -> {});

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<List<String>> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		CollectSubscriber<String, List<String>> test = new CollectSubscriber<>(
				actual, (l, v) -> l.add(v), new ArrayList<>());
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void discardElementOnAccumulatorFailure() {
		Flux.range(1, 4)
		    .collect(ArrayList::new, (l, t) -> { throw new IllegalStateException("accumulator: boom"); })
		    .as(StepVerifier::create)
		    .expectErrorMessage("accumulator: boom")
		    .verifyThenAssertThat()
		    .hasDiscardedExactly(1);
	}

	@Test
	public void discardElementAndBufferOnAccumulatorLateFailure() {
		Flux.just(1, 2, 3, 4)
		    .hide()
		    .collect(ArrayList::new, (l, t) -> {
			    if (t == 3) {
				    throw new IllegalStateException("accumulator: boom");
			    }
			    l.add(t);
		    })
		    .as(StepVerifier::create)
		    .expectErrorMessage("accumulator: boom")
		    .verifyThenAssertThat()
		    .hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardElementAndBufferOnAccumulatorLateFailure_fused() {
		Flux.just(1, 2, 3, 4)
		    .collect(ArrayList::new, (l, t) -> {
			    if (t == 3) {
				    throw new IllegalStateException("accumulator: boom");
			    }
			    l.add(t);
		    })
		    .as(StepVerifier::create)
		    //WARNING: we need to request fusion so this expectFusion is important
		    .expectFusion(Fuseable.ASYNC)
		    .expectErrorMessage("accumulator: boom")
		    .verifyThenAssertThat()
		    .hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardListElementsOnError() {
		Mono<List<Integer>> test =
				Flux.range(1, 10)
				    .hide()
				    .map(i -> {
					    if (i == 5) {
						    throw new IllegalStateException("boom");
					    }
					    return i;
				    })
				    .collect(ArrayList::new, List::add);

		StepVerifier.create(test)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3, 4);
	}

	@Test
	public void discardListElementsOnCancel() {
		StepVerifier.withVirtualTime(() ->
				Flux.interval(Duration.ofMillis(100))
				    .take(10)
				    .collect(ArrayList::new, List::add)
		)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(210))
		            .thenCancel()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(0L, 1L);
	}

	@Test
	public void discardWholeArrayOnError() {
		List<Object> discarded = new ArrayList<>();
		AtomicInteger index = new AtomicInteger();

		Mono<Object[]> test =
				Flux.range(1, 10)
				    .hide()
				    .map(i -> {
					    if (i == 5) {
						    throw new IllegalStateException("boom");
					    }
					    return i;
				    })
				    .collect(() -> new Object[4], (container, element) -> container[index.getAndIncrement()] = element)
				    .doOnDiscard(Object.class, discarded::add);

		StepVerifier.create(test)
		            .expectErrorMessage("boom")
		            .verify();

		assertThat(discarded).doesNotHaveAnyElementsOfTypes(Integer.class)
		                     .hasOnlyElementsOfType(Object[].class)
		                     .hasSize(1);
		assertThat((Object[]) discarded.get(0)).containsExactly(1, 2, 3, 4);
	}

	@Test
	public void discardWholeArrayOnCancel() {
		List<Object> discarded = new ArrayList<>();
		AtomicInteger index = new AtomicInteger();

		StepVerifier.withVirtualTime(() ->
				Flux.interval(Duration.ofMillis(100))
				    .take(10)
				    .collect(() -> new Object[4], (container, element) -> container[index.getAndIncrement()] = element)
				    .doOnDiscard(Object.class, discarded::add))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(210))
		            .thenCancel()
		            .verify();

		assertThat(discarded).doesNotHaveAnyElementsOfTypes(Long.class)
		                     .hasOnlyElementsOfType(Object[].class)
		                     .hasSize(1);
		assertThat((Object[]) discarded.get(0)).containsExactly(0L, 1L, null, null);
	}

	@Test
	public void discardCancelNextRace() {
		AtomicInteger doubleDiscardCounter = new AtomicInteger();
		Context discardingContext = Operators.enableOnDiscard(null, o -> {
			AtomicBoolean ab = (AtomicBoolean) o;
			if (ab.getAndSet(true)) {
				doubleDiscardCounter.incrementAndGet();
				throw new RuntimeException("test");
			}
		});
		for (int i = 0; i < 100_000; i++) {
			AssertSubscriber<List<AtomicBoolean>> testSubscriber = new AssertSubscriber<>(discardingContext);
			CollectSubscriber<AtomicBoolean, List<AtomicBoolean>> subscriber =
					new CollectSubscriber<>(testSubscriber, List::add, new ArrayList<>());
			subscriber.onSubscribe(Operators.emptySubscription());

			AtomicBoolean extraneous = new AtomicBoolean(false);

			RaceTestUtils.race(subscriber::cancel,
					() -> subscriber.onNext(extraneous));

			testSubscriber.assertNoValues();
			if (!extraneous.get()) {
				LOGGER.info(""+subscriber.container);
			}
			assertThat(extraneous).as("released %d", i).isTrue();
		}
		LOGGER.info("discarded twice or more: {}", doubleDiscardCounter.get());
	}

	@Test
	public void discardCancelCompleteRace() {
		AtomicInteger doubleDiscardCounter = new AtomicInteger();
		Context discardingContext = Operators.enableOnDiscard(null, o -> {
			AtomicBoolean ab = (AtomicBoolean) o;
			if (ab.getAndSet(true)) {
				doubleDiscardCounter.incrementAndGet();
				throw new RuntimeException("test");
			}
		});
		for (int i = 0; i < 100_000; i++) {
			AssertSubscriber<List<AtomicBoolean>> testSubscriber = new AssertSubscriber<>(discardingContext);
			CollectSubscriber<AtomicBoolean, List<AtomicBoolean>> subscriber =
					new CollectSubscriber<>(testSubscriber, List::add, new ArrayList<>());
			subscriber.onSubscribe(Operators.emptySubscription());

			AtomicBoolean resource = new AtomicBoolean(false);
			subscriber.onNext(resource);

			RaceTestUtils.race(subscriber::cancel, subscriber::onComplete);

			if (testSubscriber.values().isEmpty()) {
				assertThat(resource).as("not completed and released %d", i).isTrue();
			}
		}
		LOGGER.info("discarded twice or more: {}", doubleDiscardCounter.get());
	}

	@Test
	// See https://github.com/reactor/reactor-core/issues/2519
	void cancelPropagatesEvenOnEmptySource() {
		AtomicBoolean cancel1 = new AtomicBoolean();
		AtomicBoolean cancel2 = new AtomicBoolean();

		Flux<?> publisher = Flux.never()
				.hide()
				.doOnCancel(() -> cancel1.set(true))
				.collectMultimap(Function.identity())
				//.hide()
				.doOnCancel(() -> cancel2.set(true))
				.flatMapIterable(Map::entrySet)
				;
		Disposable d = publisher.subscribe();
		d.dispose();

		SoftAssertions.assertSoftly(softly -> {
			softly.assertThat(cancel1).as("cancel1").isTrue();
			softly.assertThat(cancel2).as("cancel2").isTrue();
		});
	}
}
