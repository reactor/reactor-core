/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoZipTest {

	@Test
	public void allEmpty() {
		assertThat(Mono.zip(Mono.empty(), Mono.empty())
		                      .block()).isNull();
	}

	@Test
	public void allNonEmptyIterable() {
		assertThat(Mono.zip(Arrays.asList(Mono.just(1), Mono.just(2)),
				args -> (int) args[0] + (int) args[1])
		               .block()).isEqualTo(3);
	}

	@Test
	public void noSourcePublisherCombined() {
		assertThat(Mono.zip(args -> (int) args[0] + (int) args[1])
		               .block()).isNull();
	}

	@Test
	public void oneSourcePublisherCombined() {
		assertThat(Mono.zip(args -> (int) args[0], Mono.just(1))
		               .block()).isEqualTo(1);
	}

	@Test
	public void allEmptyDelay() {
		assertThat(Mono.zipDelayError(Mono.empty(), Mono.empty())
		                      .block()).isNull();
	}

	@Test
	public void noSourcePublisherCombinedDelay() {
		assertThat(Mono.zipDelayError(args -> (int) args[0] + (int) args[1])
		               .block()).isNull();
	}

	@Test
	public void oneSourcePublisherCombinedDelay() {
		assertThat(Mono.zipDelayError(args -> (int) args[0], Mono.just(1))
		               .block()).isEqualTo(1);
	}

	@Test
	public void nonEmptyPublisherCombinedDelay() {
		assertThat(Mono.zipDelayError(args -> (int) args[0] + (int) args[1],
				Mono.just(1),
				Mono.just(2))
		               .block()).isEqualTo(3);
	}

	@Test
	@Timeout(5)
	public void castCheck() {
		Mono<String[]> mono = Mono.zip(a -> Arrays.copyOf(a, a.length, String[].class),
				Mono.just("hello"),
				Mono.just("world"));
		mono.subscribe(System.out::println);
	}

	@Test//(timeout = 5000)
	public void all2NonEmpty() {
		assertThat(Mono.zip(Mono.delay(Duration.ofMillis(150)), Mono.delay(Duration.ofMillis(250))).block())
				.isEqualTo(Tuples.of(0L, 0L));
	}

	@Test
	public void allNonEmpty2() {
		assertThat(Mono.zip(args -> (int) args[0] + (int) args[1],
				Mono.just(1),
				Mono.just(2))
		               .block()).isEqualTo(3);
	}

	@Test
	public void someEmpty() {
		StepVerifier.withVirtualTime(() ->
				Mono.zip(Mono.delay(Duration.ofMillis(150)).then(), Mono.delay(Duration
						.ofMillis(250))))
		            .thenAwait(Duration.ofMillis(150))
		            .verifyComplete();
	}

	@Test//(timeout = 5000)
	public void allNonEmpty() {
		for (int i = 2; i < 7; i++) {
			Long[] result = new Long[i];
			Arrays.fill(result, 0L);

			@SuppressWarnings("unchecked") Mono<Long>[] monos = new Mono[i];
			for (int j = 0; j < i; j++) {
				monos[j] = Mono.delay(Duration.ofMillis(150 + 50 * j));
			}

			Object[] out = Mono.zip(a -> a, monos)
			                   .block();

			assertThat(out).isEqualTo(result);
		}
	}

	@Test
	public void pairWise() {
		Mono<Tuple2<Integer, String>> f = Mono.just(1)
		                                      .zipWith(Mono.just("test2"));

		assertThat(f).isInstanceOf(MonoZip.class);
		MonoZip<?, ?> s = (MonoZip<?, ?>) f;
		assertThat(s.sources).isNotNull();
		assertThat(s.sources).hasSize(2);

		f.subscribeWith(AssertSubscriber.create())
		 .assertValues(Tuples.of(1, "test2"))
		 .assertComplete();
	}

	@Test
	public void pairWise2() {
		Mono<Tuple2<Tuple2<Integer, String>, String>> f =
				Mono.zip(Mono.just(1), Mono.just("test"))
				    .zipWith(Mono.just("test2"));

		assertThat(f).isInstanceOf(MonoZip.class);
		MonoZip<?, ?> s = (MonoZip<?, ?>) f;
		assertThat(s.sources).isNotNull();
		assertThat(s.sources).hasSize(3);

		Mono<Tuple2<Integer, String>> ff = f.map(t -> Tuples.of(t.getT1()
		                                                         .getT1(),
				t.getT1()
				 .getT2() + t.getT2()));

		ff.subscribeWith(AssertSubscriber.create())
		  .assertValues(Tuples.of(1, "testtest2"))
		  .assertComplete();
	}

	@Test
	public void pairWise3() {
		Mono<Tuple2<Tuple2<Integer, String>, String>> f =
				Mono.zip(Arrays.asList(Mono.just(1), Mono.just("test")),
						obj -> Tuples.of((int) obj[0], (String) obj[1]))
				    .zipWith(Mono.just("test2"));

		assertThat(f).isInstanceOf(MonoZip.class);
		MonoZip<?, ?> s = (MonoZip<?, ?>) f;
		assertThat(s.sources).isNotNull();
		assertThat(s.sources).hasSize(2);

		Mono<Tuple2<Integer, String>> ff = f.map(t -> Tuples.of(t.getT1()
		                                                         .getT1(),
				t.getT1()
				 .getT2() + t.getT2()));

		ff.subscribeWith(AssertSubscriber.create())
		  .assertValues(Tuples.of(1, "testtest2"))
		  .assertComplete();
	}

	@Test
	public void zipMonoJust() {
		StepVerifier.create(Mono.zip(Mono.just(1), Mono.just(2)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipMonoJust3() {
		StepVerifier.create(Mono.zip(Mono.just(1), Mono.just(2), Mono.just(3)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipMonoJust4() {
		StepVerifier.create(Mono.zip(
				Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipMonoJust5() {
		StepVerifier.create(Mono.zip(
				Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipMonoJust6() {
		StepVerifier.create(Mono.zip(
				Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5),
				Mono.just(6)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5 && v.getT6() == 6).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipMonoJust7() {
		StepVerifier.create(Mono.zip(
				Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5),
				Mono.just(6),
				Mono.just(7)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5 && v.getT6() == 6 && v.getT7() == 7).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipMonoJust8() {
		StepVerifier.create(Mono.zip(
				Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5),
				Mono.just(6),
				Mono.just(7),
				Mono.just(8)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5 && v.getT6() == 6 && v.getT7() == 7 && v.getT8() == 8).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipMonoError() {
		StepVerifier.create(Mono.zip(Mono.<Integer>error(new Exception("test1")),
				Mono.<Integer>error(new Exception("test2"))))
		            .verifyErrorSatisfies(e -> assertThat(e).hasMessage("test1"));
	}

	@Test
	public void zipMonoCallable() {
		StepVerifier.create(Mono.zip(Mono.fromCallable(() -> 1), Mono.fromCallable(() -> 2)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipDelayErrorJustMono() {
		StepVerifier.create(Mono.zipDelayError(Mono.just(1), Mono.just(2)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipDelayErrorJustMono3() {
		StepVerifier.create(Mono.zipDelayError(Mono.just(1), Mono.just(2), Mono.just(3)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipDelayErrorMonoJust4() {
		StepVerifier.create(Mono.zipDelayError(
				Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipDelayErrorMonoJust5() {
		StepVerifier.create(Mono.zipDelayError(
				Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipDelayErrorMonoJust6() {
		StepVerifier.create(Mono.zipDelayError(
				Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5),
				Mono.just(6)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5 && v.getT6() == 6).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipDelayErrorMonoJust7() {
		StepVerifier.create(Mono.zipDelayError(
				Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5),
				Mono.just(6),
				Mono.just(7)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5 && v.getT6() == 6 && v.getT7() == 7).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipDelayErrorMonoJust8() {
		StepVerifier.create(Mono.zipDelayError(
				Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5),
				Mono.just(6),
				Mono.just(7),
				Mono.just(8)))
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5 && v.getT6() == 6 && v.getT7() == 7 && v.getT8() == 8).isTrue())
		            .verifyComplete();
	}

	@Test
	public void zipIterableDelayErrorCombinesErrors() {
		Exception boom1 = new NullPointerException("boom1");
		Exception boom2 = new IllegalArgumentException("boom2");

		StepVerifier.create(Mono.zipDelayError(
				Arrays.asList(Mono.just("foo"), Mono.<String>error(boom1), Mono.<String>error(boom2)),
				Tuples.fn3()))
		            .verifyErrorMatches(e -> e.getMessage().equals("Multiple exceptions") &&
				            e.getSuppressed()[0] == boom1 &&
				            e.getSuppressed()[1] == boom2);
	}

	@Test
	public void zipIterableDoesntCombineErrors() {
		Exception boom1 = new NullPointerException("boom1");
		Exception boom2 = new IllegalArgumentException("boom2");

		StepVerifier.create(Mono.zip(
				Arrays.asList(Mono.just("foo"), Mono.<String>error(boom1), Mono.<String>error(boom2)),
				Tuples.fn3()))
		            .verifyErrorMatches(e -> e == boom1);
	}

	@Test
	public void delayErrorEmptySourceErrorSource() {
		Mono<String> error = Mono.error(new IllegalStateException("boom"));
		Mono<String> empty = Mono.empty();

		StepVerifier.create(Mono.zipDelayError(error,empty))
		            .expectErrorMessage("boom")
		            .verify();
	}

	@Test
	public void delayErrorEmptySourceErrorTwoSource() {
		final IllegalStateException e1 = new IllegalStateException("boom1");
		final IllegalStateException e2 = new IllegalStateException("boom2");
		Mono<String> error1 = Mono.error(e1);
		Mono<String> error2 = Mono.error(e2);
		Mono<String> empty = Mono.empty();

		StepVerifier.create(Mono.zipDelayError(error1, empty, error2))
		            .expectErrorSatisfies(e -> assertThat(e)
				            .matches(Exceptions::isMultiple)
				            .hasSuppressedException(e1)
				            .hasSuppressedException(e2))
		            .verify();
	}

	@Test
	public void delayErrorEmptySources() {
		AtomicBoolean cancelled = new AtomicBoolean();
		Mono<String> empty1 = Mono.empty();
		Mono<String> empty2 = Mono.empty();
		Mono<String> empty3 = Mono.<String>empty().delaySubscription(Duration.ofMillis(500))
				.doOnCancel(() -> cancelled.set(true));

		StepVerifier.create(Mono.zipDelayError(empty1, empty2, empty3))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(400))
		            .verifyComplete();

		assertThat(cancelled).isFalse();
	}

	@Test
	public void emptySources() {
		AtomicBoolean cancelled = new AtomicBoolean();
		Mono<String> empty1 = Mono.empty();
		Mono<String> empty2 = Mono.empty();
		Mono<String> empty3 = Mono.<String>empty().delaySubscription(Duration.ofMillis(500))
				.doOnCancel(() -> cancelled.set(true));

		Duration d = StepVerifier.create(Mono.zip(empty1, empty2, empty3))
		            .verifyComplete();

		assertThat(cancelled).isTrue();
		assertThat(d).isLessThan(Duration.ofMillis(500));
	}

	@Test
	public void scanOperator() {
		MonoZip s = new MonoZip<>(true, z -> z);
		assertThat(s.scan(Scannable.Attr.DELAY_ERROR)).as("delayError").isTrue();
		assertThat(s.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanCoordinator() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoZip.ZipCoordinator<String> test = new MonoZip.ZipCoordinator<>(
				actual, 2, true, a -> String.valueOf(a[0]));

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(2);
		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void innerErrorIncrementsParentDone() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoZip.ZipCoordinator<String> parent = new MonoZip.ZipCoordinator<>(
				actual, 2, false, a -> String.valueOf(a[0]));
		MonoZip.ZipInner<String> test = new MonoZip.ZipInner<>(parent);

		assertThat(parent.done).isZero();

		test.onError(new IllegalStateException("boom"));

		assertThat(parent.done).isEqualTo(2);
		assertThat(parent.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanCoordinatorNotDoneUntilN() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoZip.ZipCoordinator<String> test = new MonoZip.ZipCoordinator<>(
				actual, 10, true, a -> String.valueOf(a[0]));

		test.done = 9;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();

		test.done = 10;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanWhenInner() {
		CoreSubscriber<? super String> actual = new LambdaMonoSubscriber<>(null, e ->
		{}, null, null);
		MonoZip.ZipCoordinator<String>
				coordinator = new MonoZip.ZipCoordinator<>(actual, 2, false, a -> null);
		MonoZip.ZipInner<String> test = new MonoZip.ZipInner<>(coordinator);
		Subscription innerSub = Operators.cancelledSubscription();
		test.onSubscribe(innerSub);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(innerSub);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(coordinator);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(coordinator.scan(Scannable.Attr.TERMINATED)).isFalse(); //done == 1
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		assertThat(coordinator.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();

	}

	@Test
	public void andAliasZipWith() {
		Mono<Tuple2<Integer, String>> and = Mono.just(1)
		                                        .zipWith(Mono.just("B"));

		Mono<Tuple2<Tuple2<Integer, String>, Integer>> zipWith = and.zipWith(Mono.just(3));

		StepVerifier.create(zipWith)
		            .expectNext(Tuples.of(Tuples.of(1, "B"), 3))
		            .verifyComplete();
	}

	@Test
	public void andCombinatorAliasZipWithCombinator() {
		Mono<String> and = Mono.just(1).zipWith(Mono.just("B"), (i, s) -> i + s);

		Mono<String> zipWith = and.zipWith(Mono.just(3), (s, i) -> s + i);

		StepVerifier.create(zipWith)
		            .expectNext("1B3")
		            .verifyComplete();
	}
}
