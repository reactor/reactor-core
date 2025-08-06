/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.MockUtils;
import reactor.test.ParameterizedTestWithName;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FluxIterableTest {

	final Iterable<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

	@SuppressWarnings({"unchecked", "rawtypes"})
	static Stream<Function<Flux, Flux>> factory() {
		return Stream.of(new Function<Flux, Flux>() {
			@Override
			public Flux apply(Flux flux) {
				return flux;
			}

			@Override
			public String toString() {
				return "normal fast-path";
			}
		}, new Function<Flux, Flux>() {
			@Override
			public Flux apply(Flux flux) {
				return flux.filter(__ -> true);
			}

			@Override
			public String toString() {
				return "conditional fast-path";
			}
		}, new Function<Flux, Flux>() {
			@Override
			public Flux apply(Flux flux) {
				return flux.limitRate(1);
			}

			@Override
			public String toString() {
				return "fused";
			}
		}, new Function<Flux, Flux>() {
			@Override
			public Flux apply(Flux flux) {
				return flux.hide()
				           .limitRate(1);
			}

			@Override
			public String toString() {
				return "normal slow-path";
			}
		}, new Function<Flux, Flux>() {
			@Override
			public Flux apply(Flux flux) {
				return flux.filter(__ -> true)
				           .hide()
				           .limitRate(1);
			}

			@Override
			public String toString() {
				return "conditional slow-path";
			}
		}, new Function<Flux, Flux>() {
			@Override
			public Flux apply(Flux flux) {
				return flux.filter(__ -> true)
				           .limitRate(1);
			}

			@Override
			public String toString() {
				return "conditional-fused";
			}
		});
	}

	@ParameterizedTestWithName
	@MethodSource("factory")
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testFluxIterableEmptyCase(Function<Flux, Flux> fn) {
		Iterable<String> iterable = mock(Iterable.class);
		Mockito.when(iterable.spliterator())
		       .thenReturn(mock(Spliterator.class));

		StepVerifier.create(
				Flux.fromIterable(iterable)
					.as(fn)
				    .next()
            )
		    .expectSubscription()
		    .expectComplete()
		    .verify();
	}

	@ParameterizedTestWithName
	@MethodSource("factory")
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testFluxIterableErrorHasNext(Function<Flux, Flux> fn) {
		Iterable<String> iterable = mock(Iterable.class);
		Spliterator mock = mock(Spliterator.class);
		Mockito.when(iterable.spliterator())
		       .thenReturn(mock);

		when(mock.tryAdvance(any())).thenThrow();

		StepVerifier.create(
				            Flux.fromIterable(iterable)
				                .as(fn)
				                .next()
		            )
		            .expectSubscription()
		            .expectError()
		            .verify();
	}

	@Test
	//https://github.com/reactor/reactor-core/issues/3295
	public void useIterableOncePerSubscriber() {
		AtomicInteger calls = new AtomicInteger();
		Iterable<String> strings = () -> {
			calls.incrementAndGet();
			return Arrays.asList("hello").iterator();
		};
		StepVerifier.create(Flux.fromIterable(strings))
				.expectNext("hello")
				.verifyComplete();

		Assertions.assertThat(calls).hasValue(1);
	}

	@Test
	public void emptyIterable() {
		StepVerifier.create(Flux.never().zipWithIterable(new ArrayList<>()))
	                .verifyComplete();
	}

	@Test
	public void nullIterable() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never().zipWithIterable(null);
		});
	}

	@Test
	public void nullIterator() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>fromIterable(() -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.fromIterable(source)
		    .subscribe(ts);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.fromIterable(source)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressuredExact() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(10);

		Flux.fromIterable(source)
		    .subscribe(ts);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void iteratorReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.fromIterable(Arrays.asList(1, 2, 3, 4, 5, null, 7, 8, 9, 10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void lambdaIterableWithIterator() {
		final int max = 10;
		Iterable<Integer> iterable = () -> new Iterator<Integer>() {
			int i = 0;

			@Override
			public boolean hasNext() {
				return i < max;
			}

			@Override
			public Integer next() {
				return i++;
			}
		};

		StepVerifier.create(Flux.fromIterable(iterable), 0)
		            .expectSubscription()
		            .thenRequest(5)
		            .expectNext(0, 1, 2, 3, 4)
		            .thenRequest(5)
		            .expectNext(5, 6, 7, 8, 9)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void lambdaIterableWithList() {
		List<Integer> iterable = new ArrayList<>(10);
		iterable.add(0);
		iterable.add(1);
		iterable.add(2);
		iterable.add(3);
		iterable.add(4);
		iterable.add(5);
		iterable.add(6);
		iterable.add(7);
		iterable.add(8);
		iterable.add(9);

		StepVerifier.create(Flux.fromIterable(iterable), 0)
		            .expectSubscription()
		            .thenRequest(5)
		            .expectNext(0, 1, 2, 3, 4)
		            .thenRequest(5)
		            .expectNext(5, 6, 7, 8, 9)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void emptyMapped() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.fromIterable(Collections.<Integer>emptyList())
		    .map(v -> v + 1)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void scanOperator() {
		Iterable<String> collection = Arrays.asList("A", "B", "C");
		Iterable<Object> tuple = Tuples.of("A", "B");
		Iterable<String> other = () -> Arrays.asList("A", "B", "C", "D").iterator();

		FluxIterable<String> collectionFlux = new FluxIterable<>(collection);
		FluxIterable<Object> tupleFlux = new FluxIterable<>(tuple);
		FluxIterable<String> otherFlux = new FluxIterable<>(other);

		assertThat(collectionFlux.scan(Scannable.Attr.BUFFERED)).as("collection").isEqualTo(3);
		assertThat(collectionFlux.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(tupleFlux.scan(Scannable.Attr.BUFFERED)).as("tuple").isEqualTo(2);
		assertThat(tupleFlux.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(otherFlux.scan(Scannable.Attr.BUFFERED)).as("other").isEqualTo(0);
		assertThat(otherFlux.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscription() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, sub -> sub.request(100));
		FluxIterable.IterableSubscription<String> test =
				new FluxIterable.IterableSubscription<>(actual, Collections.singleton("test").spliterator(), true);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		test.request(123);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(123);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.clear();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanConditionalSubscription() {
		@SuppressWarnings("unchecked")
		Fuseable.ConditionalSubscriber<? super String> actual = mock(MockUtils.TestScannableConditionalSubscriber.class);
		Mockito.when(actual.currentContext()).thenReturn(Context.empty());
        FluxIterable.IterableSubscriptionConditional<String> test =
				new FluxIterable.IterableSubscriptionConditional<>(actual, Collections.singleton("test").spliterator(), true);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		test.request(123);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(123);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.clear();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	void infiniteGeneratorDoesntHangFusedDiscard() {
		class Generator implements Iterable<Integer> {

			final int seed;

			Generator(int seed) {
				this.seed = seed;
			}

			@NonNull
			@Override
			public Iterator<Integer> iterator() {
				return new Iterator<Integer>() {
					int value = seed;

					@Override
					public boolean hasNext() {
						return true;
					}

					@Override
					public Integer next() {
						return value++;
					}
				};
			}
		}

		Generator one = new Generator(1);

		//smoke test: this Iterable is indeed NOT SIZED
		assertThat(one.spliterator().hasCharacteristics(Spliterator.SIZED)).as("spliterator not sized").isFalse();

		AtomicInteger discardCount = new AtomicInteger();

		Flux.fromIterable(one)
		    .publishOn(Schedulers.single())
		    .take(10, false)
		    .doOnDiscard(Integer.class, i -> discardCount.incrementAndGet())
		    .blockLast(Duration.ofSeconds(1));

		assertThat(discardCount)
				.as("discardCount")
				.hasValue(0);
	}

	@Test
	@Timeout(5)
	void smokeTestIterableConditionalSubscriptionWithInfiniteIterable() {
		//this test is simulating a poll() loop over an infinite iterable with conditional fusion enabled

		AtomicInteger backingAtomic = new AtomicInteger();
		Context discardingContext = Operators.enableOnDiscard(Context.empty(), v -> { });

		@SuppressWarnings("unchecked")
		Fuseable.ConditionalSubscriber<Integer> testSubscriber = mock(Fuseable.ConditionalSubscriber.class);
		Mockito.when(testSubscriber.currentContext()).thenReturn(discardingContext);

		Spliterator<Integer> iterator = Spliterators.spliteratorUnknownSize(new Iterator<Integer>() {
			@Override
			public boolean hasNext() {
				//approximate infinite source with a large upper bound instead
				return backingAtomic.get() < 10_000;
			}

			@Override
			public Integer next() {
				return backingAtomic.incrementAndGet();
			}
		}, 0);


		FluxIterable.IterableSubscriptionConditional<Integer> subscription = new FluxIterable.IterableSubscriptionConditional<>(
				testSubscriber,
				iterator, false);

		subscription.cancel();

		//protected by @Timeout(5)
		Operators.onDiscardQueueWithClear(subscription, discardingContext, null);

		assertThat(backingAtomic).hasValue(0);
	}

	//see https://github.com/reactor/reactor-core/issues/2761
	@Test
	void fromIterableWithFailingIteratorNextInFusion() throws InterruptedException {
		CountDownLatch thrown = new CountDownLatch(1);
		Iterator<Integer> throwingIterator = new Iterator<Integer>() {
			int count = 0;

			@Override
			public boolean hasNext() {
				return count < 3;
			}

			@Override
			public Integer next() {
				if (++count > 2) {
					thrown.countDown();
					throw new RuntimeException("boom");
				} else {
					return count;
				}
			}
		};


		CompletableFuture<Throwable> error = new CompletableFuture<>();
		CountDownLatch terminated = new CountDownLatch(1);
		Subscriber<Integer> simpleAsyncSubscriber = new BaseSubscriber<Integer>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				request(1);
			}

			@Override
			protected void hookOnNext(Integer value) {
				// proceed on a different thread
				CompletableFuture.runAsync(() -> request(1));
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				error.complete(throwable); // expected to be called, but isn't
			}

			@Override
			protected void hookOnComplete() {
				error.complete(null); // not expected to happen
			}
		};

		Flux.fromIterable(() -> throwingIterator)
			.publishOn(Schedulers.boundedElastic())
			.doOnTerminate(terminated::countDown)
			.subscribe(simpleAsyncSubscriber);

		assertThat(thrown.await(3, TimeUnit.SECONDS)).isTrue();

		assertThat(terminated.await(2, TimeUnit.SECONDS))
			.withFailMessage("Pipeline should terminate")
			.isTrue();

		assertThat(error)
			.succeedsWithin(Duration.ofSeconds(2), InstanceOfAssertFactories.THROWABLE)
			.hasMessage("boom");
	}
}
