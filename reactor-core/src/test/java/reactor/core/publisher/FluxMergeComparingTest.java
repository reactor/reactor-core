/*
 * Copyright (c) 2021-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.*;

//see also FluxMergeOrderedTest for variant with delayError everywhere
@Timeout(5)
class FluxMergeComparingTest {

	//see https://github.com/reactor/reactor-core/issues/1958
	@Test
	void dealsWithPrefetchLargerThanSmallBufferSize() {
		int prefetch = Queues.SMALL_BUFFER_SIZE + 100;
		int range = prefetch * 2;
		Flux.mergeComparing(prefetch,
				Comparator.naturalOrder(),
				Flux.range(1, range).filter(i -> i % 2 == 0),
				Flux.range(1, range).filter(i -> i % 2 != 0))
		    .as(StepVerifier::create)
		    .expectNextCount(range)
		    .expectComplete()
		    .verify(Duration.ofSeconds(1));
	}

	@Test
	void reorderingAPI() {
		Flux<Integer> test = Flux.mergeComparing(Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7),
				Flux.just(2, 4, 6, 8, 10));

		StepVerifier.create(test)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 10)
		            .verifyComplete();
	}

	@Test
	void reorderingAPIWithDefaultPrefetch() {
		Flux<Integer> test = Flux.mergeComparing(Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6, 8, 10));

		assertThat(test.getPrefetch()).isEqualTo(Queues.SMALL_BUFFER_SIZE);

		StepVerifier.create(test)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 10)
		            .verifyComplete();
	}

	@Test
	void reorderingAPINaturalOrder() {
		Flux<Integer> test = Flux.mergeComparing(Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6, 8, 10));

		assertThat(test.getPrefetch()).isEqualTo(Queues.SMALL_BUFFER_SIZE);

		StepVerifier.create(test)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 10)
		            .verifyComplete();
	}

	@Test
	void reorderingAPISmallRequest() {
		Flux<Integer> test = Flux.mergeComparing(Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7),
				Flux.just(2, 4, 6, 8, 10));

		StepVerifier.create(test, 5)
		            .expectNext(1, 2, 3, 4)
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(5)
		            .expectNext(5, 6, 7, 8, 10)
		            .verifyComplete();
	}

	@Test
	void reorderingAPIZeroOrOneSource() {
		Flux<Integer> expectedZero = Flux.empty();
		Flux<Integer> testZero = Flux.mergeComparing(Comparator.naturalOrder());

		Flux<Integer> expectedOne = Flux.just(1, 2, 3);
		Flux<Integer> testOne = Flux.mergeComparing(Comparator.naturalOrder(), expectedOne);

		assertThat(testZero).isSameAs(expectedZero);
		assertThat(testOne).isSameAs(expectedOne);
	}

	static class Person {
		private final String name;

		Person(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	static class User extends Person {

		private final String login;

		User(String name, String login) {
			super(name);
			this.login = login;
		}

		public String getLogin() {
			return login;
		}
	}

	@Test
	void mergeComparingWithCombinesComparators() {
		Comparator<Person> nameComparator = Comparator.comparing(Person::getName);
		Comparator<User> loginComparator = Comparator.comparing(User::getLogin).reversed();

		Flux<User> a = Flux.just(new User("foo", "A"), new User("bob", "JustBob"));
		Flux<User> b = Flux.just(new User("foo", "B"));
		Flux<User> c = Flux.just(new User("foo", "C"));

		StepVerifier.create(a.mergeComparingWith(b, nameComparator)
		                     .mergeComparingWith(c, loginComparator)
		                     .map(User::getLogin))
		            .expectNext("C", "B", "A", "JustBob")
		            .verifyComplete();
	}

	@Test
	void mergeComparingWithDetectsSameReference() {
		Comparator<String> comparator = Comparator.comparingInt(String::length);

		final Flux<String> flux = Flux.just("AAAAA", "BBBB")
		                              .mergeComparingWith(Flux.just("DD", "CCC"), comparator)
		                              .mergeComparingWith(Flux.just("E"), comparator);

		assertThat(flux).isInstanceOf(FluxMergeComparing.class);
		assertThat(((FluxMergeComparing<String>) flux).valueComparator)
				.as("didn't combine comparator")
				.isSameAs(comparator);
	}

	@Test
	void mergeComparingWithDoesntCombineNaturalOrder() {
		final Flux<String> flux = Flux.just("AAAAA", "BBBB")
		                              .mergeComparingWith(Flux.just("DD", "CCC"), Comparator.naturalOrder())
		                              .mergeComparingWith(Flux.just("E"), Comparator.naturalOrder());

		assertThat(flux).isInstanceOf(FluxMergeComparing.class);
		assertThat(((FluxMergeComparing<String>) flux).valueComparator)
				.as("didn't combine naturalOrder()")
				.isSameAs(Comparator.naturalOrder());
	}

	@Test
	void considersOnlyLatestElementInEachSource() {
		final Flux<String> flux = Flux.mergeComparing(Comparator.comparingInt(String::length),
				Flux.just("AAAAA", "BBBB"),
				Flux.just("DD", "CCC"),
				Flux.just("E"));

		StepVerifier.create(flux)
		            .expectNext("E") // between E, DD and AAAAA => E, 3rd slot done
		            .expectNext("DD") // between DD and AAAAA => DD, replenish 2nd slot to CCC
		            .expectNext("CCC") // between CCC and AAAAA => CCC, 2nd slot done
		            .expectNext("AAAAA", "BBBB") // rest of first flux in 1st slot => AAAAA then BBBB
		            .verifyComplete();
	}

	@Test
	void reorderingByIndex() {
		List<Mono<Tuple2<Long, Integer>>> sourceList = Flux.range(1, 10)
		                                                   .index()
		                                                   .map(Mono::just)
		                                                   .collectList()
		                                                   .block();
		assertThat(sourceList).isNotNull();
		Collections.shuffle(sourceList);

		@SuppressWarnings("unchecked")
		Publisher<Tuple2<Long, Integer>>[] sources = sourceList.toArray(new Publisher[sourceList.size()]);

		Flux<Integer> test = new FluxMergeComparing<>(16, Comparator.comparing(Tuple2::getT1), false, true, sources)
				.map(Tuple2::getT2);

		StepVerifier.create(test)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	@Test
	void reorderingByIndexWithDelays() {
		List<Mono<Tuple2<Long, Integer>>> sourceList = Flux.range(1, 10)
		                                                   .index()
		                                                   .map(t2 -> {
			                                                   if (t2.getT1() < 4) {
				                                                   return Mono.just(t2).delayElement(Duration.ofMillis(500 - t2.getT2() * 100));
			                                                   }
			                                                   return Mono.just(t2);
		                                                   })
		                                                   .collectList()
		                                                   .block();
		assertThat(sourceList).isNotNull();
		Collections.shuffle(sourceList);

		@SuppressWarnings("unchecked")
		Publisher<Tuple2<Long, Integer>>[] sources = sourceList.toArray(new Publisher[sourceList.size()]);

		Flux<Integer> test = new FluxMergeComparing<>(16,
				Comparator.comparing(Tuple2::getT1), false, true, sources)
				.map(Tuple2::getT2);

		StepVerifier.create(test)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	@Test
	void prefetchZero() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new FluxMergeComparing<Integer>(0, Comparator.naturalOrder(), false, true))
				.withMessage("prefetch > 0 required but it was 0");
	}

	@Test
	void prefetchNegative() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new FluxMergeComparing<Integer>(-1, Comparator.naturalOrder(), false, true))
				.withMessage("prefetch > 0 required but it was -1");
	}

	@Test
	void getPrefetch() {
		FluxMergeComparing<Integer> fmo = new FluxMergeComparing<Integer>(123,
				Comparator.naturalOrder(), false, true);

		assertThat(fmo.getPrefetch()).isEqualTo(123);
	}

	@Test
	void nullSources() {
		assertThatNullPointerException()
				.isThrownBy(() -> new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
						(Publisher<Integer>[]) null))
				.withMessage("sources must be non-null");
	}

	@Test
	void mergeAdditionalSource() {
		Comparator<Integer> originalComparator = Comparator.naturalOrder();
		@SuppressWarnings("unchecked") //safe varargs
		FluxMergeComparing<Integer> fmo = new FluxMergeComparing<>(2,
				originalComparator, false, true,
				Flux.just(1, 2),
				Flux.just(3, 4));

		FluxMergeComparing<Integer> fmo2 = fmo.mergeAdditionalSource(Flux.just(5, 6), Comparator.naturalOrder());

		assertThat(fmo2).isNotSameAs(fmo);
		assertThat(fmo2.sources).startsWith(fmo.sources)
		                        .hasSize(3);
		assertThat(fmo.sources).hasSize(2);
		assertThat(fmo2.valueComparator)
				.as("same comparator detected and used")
				.isSameAs(originalComparator);

		StepVerifier.create(fmo2)
		            .expectNext(1, 2, 3, 4, 5, 6)
		            .verifyComplete();
	}

	@Test
	void mergeAdditionalSourcePreservesDelayErrorOfFirst() {
		Comparator<Integer> originalComparator = Comparator.naturalOrder();
		@SuppressWarnings("unchecked") //safe varargs
		FluxMergeComparing<Integer> fmo = new FluxMergeComparing<>(2,
				originalComparator, true, true,
				Flux.just(1, 2),
				Flux.just(3, 4));

		FluxMergeComparing<Integer> fmo2 = fmo.mergeAdditionalSource(Flux.just(5, 6), Comparator.naturalOrder());

		assertThat(fmo.delayError).as("delayError fmo").isTrue();
		assertThat(fmo2.delayError).as("delayError fmo2").isTrue();
	}

	@Test
	void scanOperator() {
		Flux<Integer> source1 = Flux.just(1).map(Function.identity()); //scannable
		Flux<Integer> source2 = Flux.just(2);

		@SuppressWarnings("unchecked") //safe varargs
		FluxMergeComparing<Integer> fmo = new FluxMergeComparing<>(123, Comparator.naturalOrder(), true, true, source1, source2);

		assertThat(fmo.scan(Scannable.Attr.PARENT)).isSameAs(source1);
		assertThat(fmo.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(fmo.scan(Scannable.Attr.DELAY_ERROR)).isTrue();

		//default value
		assertThat(fmo.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		assertThat(fmo.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	void scanMain() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, null, null, null);

		FluxMergeComparing.MergeOrderedMainProducer<Integer> test =
				new FluxMergeComparing.MergeOrderedMainProducer<Integer>(actual, Comparator.naturalOrder(), 123, 4, false, true);

		assertThat(test.scan(Scannable.Attr.ACTUAL))
				.isSameAs(actual)
				.isSameAs(test.actual());
		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isFalse();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		test.emitted = 2;
		test.requested = 10;
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(8);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancelled = 2;
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();

		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		test.error = new IllegalStateException("boom");
		assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);

		//default value
		assertThat(test.scan(Scannable.Attr.NAME)).isNull();
	}

	@Test
	void scanInner() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, null, null, null);
		FluxMergeComparing.MergeOrderedMainProducer<Integer> main =
				new FluxMergeComparing.MergeOrderedMainProducer<Integer>(actual, Comparator.naturalOrder(), 123, 4, false, true);

		FluxMergeComparing.MergeOrderedInnerSubscriber<Integer> test = new FluxMergeComparing.MergeOrderedInnerSubscriber<>(
				main, 123);
		Subscription sub = Operators.emptySubscription();
		test.onSubscribe(sub);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.actual()).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isFalse();

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.done = true;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		test.queue.offer(1);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

		//default value
		assertThat(test.scan(Scannable.Attr.NAME)).isNull();
	}

	@Test
	void mainSubscribersDifferentCountInners() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, null, null, null);

		FluxMergeComparing.MergeOrderedMainProducer<Integer> test =
				new FluxMergeComparing.MergeOrderedMainProducer<Integer>(actual, Comparator.naturalOrder(), 123, 4, false, true);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> {
					@SuppressWarnings("unchecked")
					final Publisher<Integer>[] sources = new Publisher[3];
					test.subscribe(sources);
				})
				.withMessage("must subscribe with 4 sources");
	}

	@Test
	void innerRequestAmountIgnoredAssumedOne() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, null, null, null);
		FluxMergeComparing.MergeOrderedMainProducer<Integer> main =
				new FluxMergeComparing.MergeOrderedMainProducer<Integer>(actual, Comparator.naturalOrder(), 123, 4, false, true);

		FluxMergeComparing.MergeOrderedInnerSubscriber<Integer> test = new FluxMergeComparing.MergeOrderedInnerSubscriber<>(
				main, 4);
		AtomicLong requested = new AtomicLong();
		Subscription sub = new Subscription() {
			@Override
			public void request(long n) {
				requested.addAndGet(n);
			}

			@Override
			public void cancel() {
				//NO-OP
			}
		};
		test.onSubscribe(sub);
		assertThat(requested).as("prefetch").hasValue(test.prefetch);
		assertThat(test.limit).isEqualTo(3);

		test.request(1000);
		assertThat(test.consumed).isEqualTo(1);

		test.request(1000);
		assertThat(test.consumed).isEqualTo(2);

		test.request(Long.MAX_VALUE);
		assertThat(test.consumed).isEqualTo(0);
		assertThat(requested).as("prefetch + replenish")
		                     .hasValue(test.prefetch + test.limit);
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void normal1() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1), Flux.just(2))
				.as(StepVerifier::create)
				.expectNext(1, 2)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void normal2() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6, 8))
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7, 8)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void normal3() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6))
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void normal4() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1, 3, 5, 7), Flux.just(1, 3, 5, 7))
				.as(StepVerifier::create)
				.expectNext(1, 1, 3, 3, 5, 5, 7, 7)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void normal1Hidden() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1).hide(), Flux.just(2).hide())
				.as(StepVerifier::create)
				.expectNext(1, 2)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void normal2Hidden() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1, 3, 5, 7).hide(), Flux.just(2, 4, 6, 8).hide())
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7, 8)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void normal3Hidden() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1, 3, 5, 7).hide(), Flux.just(2, 4, 6).hide())
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void normal4Hidden() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1, 3, 5, 7).hide(), Flux.just(1, 3, 5, 7).hide())
				.as(StepVerifier::create)
				.expectNext(1, 1, 3, 3, 5, 5, 7, 7)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void backpressure1() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1, 3, 5, 7).log("left")
				, Flux.just(2, 4, 6, 8).log("right"))
				.limitRate(1)
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7, 8)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void backpressure2() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1), Flux.just(2))
				.limitRate(1)
				.as(StepVerifier::create)
				.expectNext(1, 2)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void backpressure3() {
		new FluxMergeComparing<>(1, Comparator.naturalOrder(), false, true,
				Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6, 8))
				.limitRate(1)
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7, 8)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void take() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6, 8))
				.take(5)
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void firstErrorsDelayed() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), true, true,
				Flux.error(new IOException("boom")),
				Flux.just(2, 4, 6, 8))
				.as(StepVerifier::create)
				.expectNext(2, 4, 6, 8)
				.verifyError(IOException.class);
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void firstErrorsBackpressuredDelayed() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), true, true,
				Flux.error(new IOException("boom")),
				Flux.just(2, 4, 6, 8))
				.as(f -> StepVerifier.create(f, 0L))
				.thenRequest(4)
				.expectNext(2, 4, 6, 8)
				.verifyError(IOException.class);
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void secondErrorsDelayed() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), true, true,
				Flux.just(1, 3, 5, 7),
				Flux.error(new IOException("boom"))
		)
				.as(StepVerifier::create)
				.expectNext(1, 3, 5, 7)
				.verifyError(IOException.class);
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void secondErrorsBackpressuredDelayed() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), true, true,
				Flux.just(1, 3, 5, 7),
				Flux.error(new IOException("boom"))
		)
				.as(f -> StepVerifier.create(f, 0L))
				.thenRequest(4)
				.expectNext(1, 3, 5, 7)
				.verifyError(IOException.class);
	}

	@Test
	void bothErrorDelayed() {
		IOException firstError = new IOException("first");
		IOException secondError = new IOException("second");
		new FluxMergeComparing<Integer>(2, Comparator.naturalOrder(), true, true,
				Flux.error(firstError),
				Flux.error(secondError)
		)
				.as(StepVerifier::create)
				.consumeErrorWith(e -> assertThat(Exceptions.unwrapMultiple(e)).containsExactly(firstError, secondError))
				.verifyThenAssertThat()
				.hasNotDroppedErrors();
	}

	@Test
	void never() {
		new FluxMergeComparing<Integer>(2, Comparator.naturalOrder(), false, true,
				Flux.never(), Flux.never())
				.as(StepVerifier::create)
				.thenCancel()
				.verify();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void fusedThrowsInDrainLoopDelayed() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), true, true,
				Flux.just(1).map(v -> { throw new IllegalArgumentException("boom"); }),
				Flux.just(2, 3))
				.as(StepVerifier::create)
				.expectNext(2, 3)
				.verifyErrorMessage("boom");
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void fusedThrowsInPostEmissionCheckDelayed() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), true, true,
				Flux.just(1).map(v -> { throw new IllegalArgumentException("boom"); }),
				Flux.just(2, 3))
				.as(f -> StepVerifier.create(f, 0L))
				.thenRequest(2)
				.expectNext(2, 3)
				.verifyErrorMessage("boom");
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void nullInSourceArray() {
		assertThatNullPointerException()
				.isThrownBy(() -> {
					new FluxMergeComparing<>(2,
							Comparator.naturalOrder(), false, true,
							Flux.just(1), null);
				})
				.withMessage("sources[1] is null");
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void comparatorThrows() {
		new FluxMergeComparing<>(2,
				(a, b) -> { throw new IllegalArgumentException("boom"); }, false, true,
				Flux.just(1, 3), Flux.just(2, 4))
				.as(StepVerifier::create)
				.verifyErrorMessage("boom");
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	void naturalOrder() {
		new FluxMergeComparing<>(2, Comparator.naturalOrder(), false, true,
				Flux.just(1), Flux.just(2))
				.as(StepVerifier::create)
				.expectNext(1, 2)
				.verifyComplete();
	}

	@Test
	void mergeComparingNoErrorDelay() {
		Flux.mergeComparing(2, Comparator.naturalOrder(),
			Flux.error(new RuntimeException("boom")),
			Flux.<String>never())
			.as(StepVerifier::create)
			.verifyErrorMessage("boom");
	}

	@Test
	void mergeComparingNoErrorDelay2() {
		Flux.mergeComparing(1, Integer::compareTo,
				Flux.range(0, 20), 
				Flux.range(0, 20)
				.<Integer>handle((v, sink) -> {
					if (v < 2) {
						sink.next(v);
					} else {
						sink.error(new RuntimeException("boom"));
					}
				}))
				.as(StepVerifier::create)
				.expectNext(0, 0, 1, 1)
				.verifyErrorMessage("boom");
	}

	@Test
	void mergeComparingWithInheritErrorDelay() {
		Flux<String> fluxNoDelay = Flux.<String>error(new RuntimeException("boom"))
				.mergeComparingWith(Flux.never(), Comparator.naturalOrder());
		FluxMergeComparing<String> fmoNoDelay = (FluxMergeComparing<String>) fluxNoDelay;

		assertThat(fmoNoDelay.delayError).as("delayError").isFalse();

		Flux<String> fluxDelay = Flux.mergeComparingDelayError(2, Comparator.naturalOrder(),
				Flux.<String>error(new RuntimeException("boom")),
				Flux.never())
				.mergeComparingWith(Flux.never(), Comparator.naturalOrder());
		FluxMergeComparing<String> fmoDelay = (FluxMergeComparing<String>) fluxDelay;

		assertThat(fmoDelay.delayError).as("delayError fmoDelay").isTrue();
	}

	@Test
	void mergeComparingWithOnNonMergeSourceDefaultsToNoDelay() {
		Flux<String> flux = Flux.<String>error(new RuntimeException("boom"))
				.mergeComparingWith(Flux.never(), Comparator.naturalOrder());
		FluxMergeComparing<String> fmoNoDelay = (FluxMergeComparing<String>) flux;

		assertThat(fmoNoDelay.delayError).as("delayError").isFalse();
	}

	@Test
	void shouldEmitAsyncValuesAsTheyArrive() {
		StepVerifier.withVirtualTime(() -> {
	        Flux<Tuple2<String, Integer>> catsAreBetterThanDogs = Flux.just(
				"pickles", // 700
                "leela", // 1400
                "girl", // 2100
                "meatloaf", // 2800
                "henry" // 3500
                )
                .delayElements(Duration.ofMillis(700))
                .map(s -> Tuples.of(s, 0));

	        Flux<Tuple2<String, Integer>> poorDogs = Flux.just(
				"spot", // 300
	            "barley", // 600
	            "goodboy", // 900
	            "sammy", // 1200
	            "doug" // 1500
                )
	            .delayElements(Duration.ofMillis(300))
	            .map(s -> Tuples.of(s, 1));

	        return Flux.mergePriority(Comparator.comparing(Tuple2::getT2),
                       catsAreBetterThanDogs, poorDogs)
	                   .map(Tuple2::getT1);
        })
			.thenAwait(Duration.ofMillis(300))// 300
            .expectNext("spot")
            .thenAwait(Duration.ofMillis(300)) // 600
            .expectNext("barley")
            .thenAwait(Duration.ofMillis(100)) // 700
            .expectNext("pickles")
            .thenAwait(Duration.ofMillis(200)) // 900
            .expectNext("goodboy")
            .thenAwait(Duration.ofMillis(300)) // 1200
            .expectNext("sammy")
            .thenAwait(Duration.ofMillis(200)) // 1400
            .expectNext("leela")
            .thenAwait(Duration.ofMillis(100)) // 1500
            .expectNext("doug")
            .thenAwait(Duration.ofMillis(600)) // 2100
            .expectNext("girl")
            .thenAwait(Duration.ofMillis(700)) // 2800
            .expectNext("meatloaf")
            .thenAwait(Duration.ofMillis(700)) // 3500
            .expectNext("henry")
            .expectComplete()
            .verify(Duration.ofSeconds(5));
	}
}
