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
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuple5;
import reactor.util.function.Tuple6;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoWhenTest {

	@Test
	public void allEmpty() {
		Assert.assertNull(Mono.when(Mono.empty(), Mono.empty())
		                      .block());
	}

	@Test
	public void allEmptyPublisherIterable() {
		Assert.assertNull(Mono.when(Arrays.asList(Mono.empty(), Flux.empty()))
		                      .block());
	}

	@Test
	public void allEmptyPublisher() {
		assertThat(Mono.when(Mono.empty(), Flux.empty())
		               .block()).isNull();
	}

	@Test
	public void allNonEmptyIterable() {
		assertThat(Mono.when(Arrays.asList(Mono.just(1), Mono.just(2)),
				args -> (int) args[0] + (int) args[1])
		               .block()).isEqualTo(3);
	}

	@Test
	public void noSourcePublisherCombined() {
		assertThat(Mono.when(args -> (int) args[0] + (int) args[1])
		               .block()).isNull();
	}

	@Test
	public void oneSourcePublisherCombined() {
		assertThat(Mono.when(args -> (int) args[0], Mono.just(1))
		               .block()).isEqualTo(1);
	}

	@Test
	public void noSourcePublisher() {
		assertThat(Mono.when()
		               .block()).isNull();
	}

	@Test
	public void oneSourcePublisher() {
		assertThat(Mono.when(Flux.empty())
		               .block()).isNull();
	}

	@Test
	public void allEmptyDelay() {
		Assert.assertNull(Mono.whenDelayError(Mono.empty(), Mono.empty())
		                      .block());
	}

	@Test
	public void allEmptyPublisherDelay() {
		assertThat(Mono.whenDelayError(Mono.empty(), Flux.empty())
		               .block()).isNull();
	}

	@Test
	public void noSourcePublisherCombinedDelay() {
		assertThat(Mono.whenDelayError(args -> (int) args[0] + (int) args[1])
		               .block()).isNull();
	}

	@Test
	public void oneSourcePublisherCombinedDelay() {
		assertThat(Mono.whenDelayError(args -> (int) args[0], Mono.just(1))
		               .block()).isEqualTo(1);
	}

	@Test
	public void nonEmptyPublisherCombinedDelay() {
		assertThat(Mono.whenDelayError(args -> (int) args[0] + (int) args[1],
				Mono.just(1),
				Mono.just(2))
		               .block()).isEqualTo(3);
	}

	@Test
	public void noSourcePublisherDelay() {
		assertThat(Mono.whenDelayError()
		               .block()).isNull();
	}

	@Test
	public void oneSourcePublisherDelay() {
		assertThat(Mono.whenDelayError(Flux.empty())
		               .block()).isNull();
	}

	@Test(timeout = 5000)
	public void castCheck() {
		Mono<String[]> mono = Mono.when(a -> Arrays.copyOf(a, a.length, String[].class),
				Mono.just("hello"),
				Mono.just("world"));
		mono.subscribe(System.out::println);
	}

	@Test(timeout = 5000)
	public void someEmpty() {
		Assert.assertNull(Mono.when(Mono.empty(), Mono.delay(Duration.ofMillis(250)))
		                      .block());
	}

	@Test//(timeout = 5000)
	public void all2NonEmpty() {
		Assert.assertEquals(Tuples.of(0L, 0L),
				Mono.when(Mono.delay(Duration.ofMillis(150)), Mono.delay(Duration.ofMillis(250)))
				    .block());
	}

	@Test
	public void allNonEmpty2() {
		assertThat(Mono.when(args -> (int) args[0] + (int) args[1],
				Mono.just(1),
				Mono.just(2))
		               .block()).isEqualTo(3);
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

			Object[] out = Mono.when(a -> a, monos)
			                   .block();

			Assert.assertArrayEquals(result, out);
		}
	}

	@Test
	public void pairWise() {
		Mono<Tuple2<Integer, String>> f = Mono.just(1)
		                                      .and(Mono.just("test2"));

		Assert.assertTrue(f instanceof MonoWhen);
		MonoWhen<?, ?> s = (MonoWhen<?, ?>) f;
		Assert.assertTrue(s.sources != null);
		Assert.assertTrue(s.sources.length == 2);

		f.subscribeWith(AssertSubscriber.create())
		 .assertValues(Tuples.of(1, "test2"))
		 .assertComplete();
	}

	@Test
	public void pairWise2() {
		Mono<Tuple2<Tuple2<Integer, String>, String>> f =
				Mono.when(Mono.just(1), Mono.just("test"))
				    .and(Mono.just("test2"));

		Assert.assertTrue(f instanceof MonoWhen);
		MonoWhen<?, ?> s = (MonoWhen<?, ?>) f;
		Assert.assertTrue(s.sources != null);
		Assert.assertTrue(s.sources.length == 3);

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
				Mono.when(Arrays.asList(Mono.just(1), Mono.just("test")),
						obj -> Tuples.of((int) obj[0], (String) obj[1]))
				    .and(Mono.just("test2"));

		Assert.assertTrue(f instanceof MonoWhen);
		MonoWhen<?, ?> s = (MonoWhen<?, ?>) f;
		Assert.assertTrue(s.sources != null);
		Assert.assertTrue(s.sources.length == 2);

		Mono<Tuple2<Integer, String>> ff = f.map(t -> Tuples.of(t.getT1()
		                                                         .getT1(),
				t.getT1()
				 .getT2() + t.getT2()));

		ff.subscribeWith(AssertSubscriber.create())
		  .assertValues(Tuples.of(1, "testtest2"))
		  .assertComplete();
	}

	@Test
	public void whenMonoJust() {
		MonoProcessor<Tuple2<Integer, Integer>> mp = MonoProcessor.create();
		StepVerifier.create(Mono.when(Mono.just(1), Mono.just(2))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2).isTrue())
		            .verifyComplete();
	}

	@Test
	public void whenMonoJust3() {
		MonoProcessor<Tuple3<Integer, Integer, Integer>> mp = MonoProcessor.create();
		StepVerifier.create(Mono.when(Mono.just(1), Mono.just(2), Mono.just(3))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3).isTrue())
		            .verifyComplete();
	}

	@Test
	public void whenMonoJust4() {
		MonoProcessor<Tuple4<Integer, Integer, Integer, Integer>> mp =
				MonoProcessor.create();
		StepVerifier.create(Mono.when(Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4).isTrue())
		            .verifyComplete();
	}

	@Test
	public void whenMonoJust5() {
		MonoProcessor<Tuple5<Integer, Integer, Integer, Integer, Integer>> mp =
				MonoProcessor.create();
		StepVerifier.create(Mono.when(Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5).isTrue())
		            .verifyComplete();
	}

	@Test
	public void whenMonoJust6() {
		MonoProcessor<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> mp =
				MonoProcessor.create();
		StepVerifier.create(Mono.when(Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5),
				Mono.just(6))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5 && v.getT6() == 6).isTrue())
		            .verifyComplete();
	}

	@Test
	public void whenMonoError() {
		MonoProcessor<Tuple2<Integer, Integer>> mp = MonoProcessor.create();
		StepVerifier.create(Mono.when(Mono.<Integer>error(new Exception("test1")),
				Mono.<Integer>error(new Exception("test2")))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isTrue())
		            .then(() -> assertThat(mp.isSuccess()).isFalse())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .consumeErrorWith(e -> assertThat(e).hasMessage("test1"))
		            .verify();
	}

	@Test
	public void whenMonoCallable() {
		MonoProcessor<Tuple2<Integer, Integer>> mp = MonoProcessor.create();
		StepVerifier.create(Mono.when(Mono.fromCallable(() -> 1),
				Mono.fromCallable(() -> 2))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2).isTrue())
		            .verifyComplete();
	}

	@Test
	public void whenDelayJustMono() {
		MonoProcessor<Tuple2<Integer, Integer>> mp = MonoProcessor.create();
		StepVerifier.create(Mono.whenDelayError(Mono.just(1), Mono.just(2))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2).isTrue())
		            .verifyComplete();
	}

	@Test
	public void whenDelayJustMono3() {
		MonoProcessor<Tuple3<Integer, Integer, Integer>> mp = MonoProcessor.create();
		StepVerifier.create(Mono.whenDelayError(Mono.just(1), Mono.just(2), Mono.just(3))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3).isTrue())
		            .verifyComplete();
	}

	@Test
	public void whenDelayMonoJust4() {
		MonoProcessor<Tuple4<Integer, Integer, Integer, Integer>> mp =
				MonoProcessor.create();
		StepVerifier.create(Mono.whenDelayError(Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4).isTrue())
		            .verifyComplete();
	}

	@Test
	public void whenDelayMonoJust5() {
		MonoProcessor<Tuple5<Integer, Integer, Integer, Integer, Integer>> mp =
				MonoProcessor.create();
		StepVerifier.create(Mono.whenDelayError(Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5).isTrue())
		            .verifyComplete();
	}

	@Test
	public void whenDelayMonoJust6() {
		MonoProcessor<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> mp =
				MonoProcessor.create();
		StepVerifier.create(Mono.whenDelayError(Mono.just(1),
				Mono.just(2),
				Mono.just(3),
				Mono.just(4),
				Mono.just(5),
				Mono.just(6))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .assertNext(v -> assertThat(v.getT1() == 1 && v.getT2() == 2 && v.getT3() == 3 && v.getT4() == 4 && v.getT5() == 5 && v.getT6() == 6).isTrue())
		            .verifyComplete();
	}

	@Test
	public void whenIterableDelayErrorCombinesErrors() {
		Exception boom1 = new NullPointerException("boom1");
		Exception boom2 = new IllegalArgumentException("boom2");

		StepVerifier.create(Mono.whenDelayError(
				Arrays.asList(Mono.just("foo"), Mono.<String>error(boom1), Mono.<String>error(boom2)),
				Tuples.fn3()))
		            .verifyErrorMatches(e -> e.getMessage().equals("Multiple errors") &&
				            e.getSuppressed()[0] == boom1 &&
				            e.getSuppressed()[1] == boom2);
	}

	@Test
	public void whenIterableDoesntCombineErrors() {
		Exception boom1 = new NullPointerException("boom1");
		Exception boom2 = new IllegalArgumentException("boom2");

		StepVerifier.create(Mono.when(
				Arrays.asList(Mono.just("foo"), Mono.<String>error(boom1), Mono.<String>error(boom2)),
				Tuples.fn3()))
		            .verifyErrorMatches(e -> e == boom1);
	}

	@Test
	public void whenIterableDelayErrorPublishersVoidCombinesErrors() {
		Exception boom1 = new NullPointerException("boom1");
		Exception boom2 = new IllegalArgumentException("boom2");

		Iterable<Publisher<Void>> voidPublishers = Arrays.asList(
				Mono.<Void>empty(),
				Mono.<Void>error(boom1),
				Mono.<Void>error(boom2));

		StepVerifier.create(Mono.whenDelayError(voidPublishers))
		            .verifyErrorMatches(e -> e.getMessage().equals("Multiple errors") &&
				            e.getSuppressed()[0] == boom1 &&
				            e.getSuppressed()[1] == boom2);
	}

	@Test
	public void whenIterablePublishersVoidDoesntCombineErrors() {
		Exception boom1 = new NullPointerException("boom1");
		Exception boom2 = new IllegalArgumentException("boom2");

		Iterable<Publisher<Void>> voidPublishers = Arrays.asList(
				Mono.<Void>empty(),
				Mono.<Void>error(boom1),
				Mono.<Void>error(boom2));

		StepVerifier.create(Mono.when(voidPublishers))
		            .verifyErrorMatches(e -> e == boom1);
	}

	@Test
	public void scanCoordinator() {
		Subscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoWhen.WhenCoordinator<String> test = new MonoWhen.WhenCoordinator<>(
				actual, 2, true, a -> String.valueOf(a[0]));

		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(2);
		assertThat(test.scan(Scannable.BooleanAttr.DELAY_ERROR)).isTrue();

		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.signalError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse(); //done == 1
		test.signalError(new IllegalStateException("boom2")); // done == 2
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

	@Test
	public void scanCoordinatorNotDoneUntilN() {
		Subscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoWhen.WhenCoordinator<String> test = new MonoWhen.WhenCoordinator<>(
				actual, 10, true, a -> String.valueOf(a[0]));

		test.done = 9;
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();

		test.done = 10;
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanWhenInner() {
		Subscriber<? super String> actual = new LambdaMonoSubscriber<>(null, null, null, null);
		MonoWhen.WhenCoordinator<String> coordinator = new MonoWhen.WhenCoordinator<>(actual, 2, false, a -> null);
		MonoWhen.WhenInner<String> test = new MonoWhen.WhenInner<>(coordinator);
		Subscription innerSub = Operators.cancelledSubscription();
		test.onSubscribe(innerSub);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(innerSub);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(coordinator);

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();

		test.error = new IllegalStateException("boom");
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
	}
}
