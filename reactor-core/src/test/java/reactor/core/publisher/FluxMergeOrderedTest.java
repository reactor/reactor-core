/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.*;

public class FluxMergeOrderedTest {

	@Test
	public void reorderingAPI() {
		Flux<Integer> test = Flux.mergeOrdered(Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7),
				Flux.just(2, 4, 6, 8, 10));

		StepVerifier.create(test)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 10)
		            .verifyComplete();
	}

	@Test
	public void reorderingAPIWithDefaultPrefetch() {
		Flux<Integer> test = Flux.mergeOrdered(Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6, 8, 10));

		assertThat(test.getPrefetch()).isEqualTo(Queues.SMALL_BUFFER_SIZE);

		StepVerifier.create(test)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 10)
		            .verifyComplete();
	}

	@Test
	public void reorderingAPINaturalOrder() {
		Flux<Integer> test = Flux.mergeOrdered(Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6, 8, 10));

		assertThat(test.getPrefetch()).isEqualTo(Queues.SMALL_BUFFER_SIZE);

		StepVerifier.create(test)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 10)
		            .verifyComplete();
	}

	@Test
	public void reorderingAPISmallRequest() {
		Flux<Integer> test = Flux.mergeOrdered(Comparator.naturalOrder(),
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
	public void reorderingAPIZeroOrOneSource() {
		Flux<Integer> expectedZero = Flux.empty();
		Flux<Integer> testZero = Flux.mergeOrdered(Comparator.naturalOrder());

		Flux<Integer> expectedOne = Flux.just(1, 2, 3);
		Flux<Integer> testOne = Flux.mergeOrdered(Comparator.naturalOrder(), expectedOne);

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
	public void mergeOrderedWithCombinesComparators() {
		Comparator<Person> nameComparator = Comparator.comparing(Person::getName);
		Comparator<User> loginComparator = Comparator.comparing(User::getLogin).reversed();

		Flux<User> a = Flux.just(new User("foo", "A"), new User("bob", "JustBob"));
		Flux<User> b = Flux.just(new User("foo", "B"));
		Flux<User> c = Flux.just(new User("foo", "C"));

		StepVerifier.create(a.mergeOrderedWith(b, nameComparator)
		                     .mergeOrderedWith(c, loginComparator)
		                     .map(User::getLogin))
		            .expectNext("C", "B", "A", "JustBob")
		            .verifyComplete();
	}

	@Test
	public void mergeOrderedWithDetectsSameReference() {
		Comparator<String> comparator = Comparator.comparingInt(String::length);

		final Flux<String> flux = Flux.just("AAAAA", "BBBB")
		                              .mergeOrderedWith(Flux.just("DD", "CCC"), comparator)
		                              .mergeOrderedWith(Flux.just("E"), comparator);

		assertThat(flux).isInstanceOf(FluxMergeOrdered.class);
		assertThat(((FluxMergeOrdered) flux).valueComparator)
				.as("didn't combine comparator")
				.isSameAs(comparator);
	}

	@Test
	public void mergeOrderedWithDoesntCombineNaturalOrder() {
		final Flux<String> flux = Flux.just("AAAAA", "BBBB")
		                              .mergeOrderedWith(Flux.just("DD", "CCC"), Comparator.naturalOrder())
		                              .mergeOrderedWith(Flux.just("E"), Comparator.naturalOrder());

		assertThat(flux).isInstanceOf(FluxMergeOrdered.class);
		assertThat(((FluxMergeOrdered) flux).valueComparator)
				.as("didn't combine naturalOrder()")
				.isSameAs(Comparator.naturalOrder());
	}

	@Test
	public void considersOnlyLatestElementInEachSource() {
		final Flux<String> flux = Flux.mergeOrdered(Comparator.comparingInt(String::length),
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
	public void reorderingByIndex() {
		List<Mono<Tuple2<Long, Integer>>> sourceList = Flux.range(1, 10)
		                                                   .index()
		                                                   .map(Mono::just)
		                                                   .collectList()
		                                                   .block();
		assertThat(sourceList).isNotNull();
		Collections.shuffle(sourceList);

		@SuppressWarnings("unchecked")
		Publisher<Tuple2<Long, Integer>>[] sources = sourceList.toArray(new Publisher[sourceList.size()]);

		Flux<Integer> test = new FluxMergeOrdered<>(16,
				Queues.small(), Comparator.comparing(Tuple2::getT1), sources)
				.map(Tuple2::getT2);

		StepVerifier.create(test)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	@Test
	public void reorderingByIndexWithDelays() {
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

		Flux<Integer> test = new FluxMergeOrdered<>(16,
				Queues.small(), Comparator.comparing(Tuple2::getT1), sources)
				.map(Tuple2::getT2);

		StepVerifier.create(test)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	@Test
	public void prefetchZero() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new FluxMergeOrdered<Integer>(0, Queues.small(), Comparator.naturalOrder()))
				.withMessage("prefetch > 0 required but it was 0");
	}

	@Test
	public void prefetchNegative() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new FluxMergeOrdered<Integer>(-1, Queues.small(), Comparator.naturalOrder()))
				.withMessage("prefetch > 0 required but it was -1");
	}

	@Test
	public void getPrefetch() {
		FluxMergeOrdered<Integer> fmo = new FluxMergeOrdered<Integer>(123,
				Queues.small(), Comparator.naturalOrder());

		assertThat(fmo.getPrefetch()).isEqualTo(123);
	}

	@Test
	public void nullSources() {
		assertThatNullPointerException()
				.isThrownBy(() -> new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
						(Publisher<Integer>[]) null))
				.withMessage("sources must be non-null");
	}

	@Test
	public void mergeAdditionalSource() {
		Comparator<Integer> originalComparator = Comparator.naturalOrder();
		@SuppressWarnings("unchecked")
		FluxMergeOrdered<Integer> fmo = new FluxMergeOrdered<>(2,
				Queues.small(),
				originalComparator,
				Flux.just(1, 2),
				Flux.just(3, 4));

		FluxMergeOrdered<Integer> fmo2 = fmo.mergeAdditionalSource(Flux.just(5, 6), Comparator.naturalOrder());

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
	public void scanOperator() {
		Flux<Integer> source1 = Flux.just(1).map(Function.identity()); //scannable
		Flux<Integer> source2 = Flux.just(2);

		@SuppressWarnings("unchecked") //safe varargs
		Scannable fmo = new FluxMergeOrdered<>(123, Queues.small(), Comparator.naturalOrder(), source1, source2);

		assertThat(fmo.scan(Scannable.Attr.PARENT)).isSameAs(source1);
		assertThat(fmo.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(fmo.scan(Scannable.Attr.DELAY_ERROR)).isTrue();

		//default value
		assertThat(fmo.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		assertThat(fmo.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanMain() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, null, null, null);

		FluxMergeOrdered.MergeOrderedMainProducer<Integer> test =
				new FluxMergeOrdered.MergeOrderedMainProducer<Integer>(actual, Comparator.naturalOrder(), 123, 4);

		assertThat(test.scan(Scannable.Attr.ACTUAL))
				.isSameAs(actual)
				.isSameAs(test.actual());
		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
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
	public void scanInner() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, null, null, null);
		FluxMergeOrdered.MergeOrderedMainProducer<Integer> main =
				new FluxMergeOrdered.MergeOrderedMainProducer<Integer>(actual, Comparator.naturalOrder(), 123, 4);

		FluxMergeOrdered.MergeOrderedInnerSubscriber<Integer> test = new FluxMergeOrdered.MergeOrderedInnerSubscriber<>(
				main, 123);
		Subscription sub = Operators.emptySubscription();
		test.onSubscribe(sub);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.actual()).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

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
	public void mainSubscribersDifferentCountInners() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, null, null, null);

		FluxMergeOrdered.MergeOrderedMainProducer<Integer> test =
				new FluxMergeOrdered.MergeOrderedMainProducer<Integer>(actual, Comparator.naturalOrder(), 123, 4);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> {
					@SuppressWarnings("unchecked")
					final Publisher<Integer>[] sources = new Publisher[3];
					test.subscribe(sources);
				})
				.withMessage("must subscribe with 4 sources");
	}

	@Test
	public void innerRequestAmountIgnoredAssumedOne() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, null, null, null);
		FluxMergeOrdered.MergeOrderedMainProducer<Integer> main =
				new FluxMergeOrdered.MergeOrderedMainProducer<Integer>(actual, Comparator.naturalOrder(), 123, 4);

		FluxMergeOrdered.MergeOrderedInnerSubscriber<Integer> test = new FluxMergeOrdered.MergeOrderedInnerSubscriber<>(
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
	public void normal1() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1), Flux.just(2))
				.as(StepVerifier::create)
				.expectNext(1, 2)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void normal2() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6, 8))
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7, 8)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void normal3() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6))
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void normal4() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7), Flux.just(1, 3, 5, 7))
				.as(StepVerifier::create)
				.expectNext(1, 1, 3, 3, 5, 5, 7, 7)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void normal1Hidden() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1).hide(), Flux.just(2).hide())
				.as(StepVerifier::create)
				.expectNext(1, 2)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void normal2Hidden() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7).hide(), Flux.just(2, 4, 6, 8).hide())
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7, 8)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void normal3Hidden() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7).hide(), Flux.just(2, 4, 6).hide())
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void normal4Hidden() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7).hide(), Flux.just(1, 3, 5, 7).hide())
				.as(StepVerifier::create)
				.expectNext(1, 1, 3, 3, 5, 5, 7, 7)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void backpressure1() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6, 8))
				.limitRate(1)
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7, 8)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void backpressure2() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1), Flux.just(2))
				.limitRate(1)
				.as(StepVerifier::create)
				.expectNext(1, 2)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void backpressure3() {
		new FluxMergeOrdered<>(1, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6, 8))
				.limitRate(1)
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5, 6, 7, 8)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void take() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7), Flux.just(2, 4, 6, 8))
				.take(5)
				.as(StepVerifier::create)
				.expectNext(1, 2, 3, 4, 5)
				.verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void firstErrorsDelayed() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.error(new IOException("boom")),
				Flux.just(2, 4, 6, 8))
				.as(StepVerifier::create)
				.expectNext(2, 4, 6, 8)
				.verifyError(IOException.class);
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void firstErrorsBackpressuredDelayed() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.error(new IOException("boom")),
				Flux.just(2, 4, 6, 8))
				.as(f -> StepVerifier.create(f, 0L))
				.thenRequest(4)
				.expectNext(2, 4, 6, 8)
				.verifyError(IOException.class);
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void secondErrorsDelayed() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7),
				Flux.error(new IOException("boom"))
		)
				.as(StepVerifier::create)
				.expectNext(1, 3, 5, 7)
				.verifyError(IOException.class);
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void secondErrorsBackpressuredDelayed() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1, 3, 5, 7),
				Flux.error(new IOException("boom"))
		)
				.as(f -> StepVerifier.create(f, 0L))
				.thenRequest(4)
				.expectNext(1, 3, 5, 7)
				.verifyError(IOException.class);
	}

	@Test
	public void bothErrorDelayed() {
		IOException firstError = new IOException("first");
		IOException secondError = new IOException("second");
		new FluxMergeOrdered<Integer>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.error(firstError),
				Flux.error(secondError)
		)
				.as(StepVerifier::create)
				.consumeErrorWith(e -> assertThat(Exceptions.unwrapMultiple(e)).containsExactly(firstError, secondError))
				.verifyThenAssertThat()
				.hasNotDroppedErrors();
	}

	@Test
	public void never() {
		new FluxMergeOrdered<Integer>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.never(), Flux.never())
				.as(StepVerifier::create)
				.thenCancel()
				.verify();
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void fusedThrowsInDrainLoopDelayed() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1).map(v -> { throw new IllegalArgumentException("boom"); }),
				Flux.just(2, 3))
				.as(StepVerifier::create)
				.expectNext(2, 3)
				.verifyErrorMessage("boom");
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void fusedThrowsInPostEmissionCheckDelayed() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1).map(v -> { throw new IllegalArgumentException("boom"); }),
				Flux.just(2, 3))
				.as(f -> StepVerifier.create(f, 0L))
				.thenRequest(2)
				.expectNext(2, 3)
				.verifyErrorMessage("boom");
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void nullInSourceArray() {
		assertThatNullPointerException()
				.isThrownBy(() -> {
					new FluxMergeOrdered<>(2, Queues.small(),
							Comparator.naturalOrder(),
							Flux.just(1), null);
				})
				.withMessage("sources[1] is null");
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void comparatorThrows() {
		new FluxMergeOrdered<>(2, Queues.small(),
				(a, b) -> { throw new IllegalArgumentException("boom"); },
				Flux.just(1, 3), Flux.just(2, 4))
				.as(StepVerifier::create)
				.verifyErrorMessage("boom");
	}

	@Test
	@SuppressWarnings("unchecked") //safe varargs
	public void naturalOrder() {
		new FluxMergeOrdered<>(2, Queues.small(), Comparator.naturalOrder(),
				Flux.just(1), Flux.just(2))
				.as(StepVerifier::create)
				.expectNext(1, 2)
				.verifyComplete();
	}

}
