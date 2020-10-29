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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSequenceEqualTest {

	@Test
	public void sequenceEquals() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two", "three"),
						Flux.just("one", "two", "three")))
		            .expectNext(Boolean.TRUE)
		            .verifyComplete();
	}

	@Test
	public void sequenceLongerLeft() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two", "three", "four"),
						Flux.just("one", "two", "three")))
		            .expectNext(Boolean.FALSE)
		            .verifyComplete();
	}

	@Test
	public void sequenceLongerRight() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two", "three"),
						Flux.just("one", "two", "three", "four")))
		            .expectNext(Boolean.FALSE)
		            .verifyComplete();
	}

	@Test
	public void sequenceErrorsLeft() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two").concatWith(Mono.error(new IllegalStateException())),
						Flux.just("one", "two", "three")))
		            .verifyError(IllegalStateException.class);
	}

	@Test
	public void sequenceErrorsRight() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two", "three"),
						Flux.just("one", "two").concatWith(Mono.error(new IllegalStateException()))))
		            .verifyError(IllegalStateException.class);
	}

	@Test
	public void sequenceErrorsBothPropagatesLeftError() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two", "three", "four").concatWith(Mono.error(new IllegalArgumentException("left"))).hide(),
						Flux.just("one", "two").concatWith(Mono.error(new IllegalArgumentException("right"))).hide()))
		            .verifyErrorMessage("left");
	}

	@Test
	public void sequenceErrorsBothPropagatesLeftErrorWithSmallRequest() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two", "three", "four")
						    .concatWith(Mono.error(new IllegalArgumentException("left")))
						    .hide(),
						Flux.just("one", "two")
						    .concatWith(Mono.error(new IllegalArgumentException("right")))
						    .hide(),
						Objects::equals, 1))
		            .verifyErrorMessage("right");
	}

	@Test
	public void sequenceEmptyLeft() {
		StepVerifier.create(Mono.sequenceEqual(
				Flux.empty(),
				Flux.just("one", "two", "three")))
		            .expectNext(Boolean.FALSE)
		            .verifyComplete();
	}

	@Test
	public void sequenceEmptyRight() {
		StepVerifier.create(Mono.sequenceEqual(
				Flux.just("one", "two", "three"),
				Flux.empty()))
		            .expectNext(Boolean.FALSE)
		            .verifyComplete();
	}

	@Test
	public void sequenceEmptyBoth() {
		StepVerifier.create(Mono.sequenceEqual(
				Flux.empty(),
				Flux.empty()))
		            .expectNext(Boolean.TRUE)
		            .verifyComplete();
	}

	@Test
	public void equalPredicateFailure() {
		StepVerifier.create(Mono.sequenceEqual(Mono.just("one"), Mono.just("one"),
						(s1, s2) -> { throw new IllegalStateException("boom"); }))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void largeSequence() {
		Flux<Integer> source = Flux.range(1, Queues.SMALL_BUFFER_SIZE * 4).subscribeOn(Schedulers.boundedElastic());

		StepVerifier.create(Mono.sequenceEqual(source, source))
		            .expectNext(Boolean.TRUE)
		            .expectComplete()
		            .verify(Duration.ofSeconds(5));
	}

	@Test
	public void syncFusedCrash() {
		Flux<Integer> source = Flux.range(1, 10).map(i -> { throw new IllegalArgumentException("boom"); });

		StepVerifier.create(Mono.sequenceEqual(source, Flux.range(1, 10).hide()))
		            .verifyErrorMessage("boom");

		StepVerifier.create(Mono.sequenceEqual(Flux.range(1, 10).hide(), source))
		            .verifyErrorMessage("boom");
	}


	@Test
	public void differenceCancelsBothSources() {
		AtomicBoolean sub1 = new AtomicBoolean();
		AtomicBoolean sub2 = new AtomicBoolean();

		Flux<Integer> source1 = Flux.range(1, 5).doOnCancel(() -> sub1.set(true));
		Flux<Integer> source2 = Flux.just(1, 2, 3, 7, 8).doOnCancel(() -> sub2.set(true));

		StepVerifier.create(Mono.sequenceEqual(source1, source2))
		            .expectNext(Boolean.FALSE)
		            .verifyComplete();

		assertThat(sub1.get()).as("left not cancelled").isTrue();
		assertThat(sub2.get()).as("right not cancelled").isTrue();
	}

	@Test
	public void cancelCancelsBothSources() {
		AtomicReference<Subscription> sub1 = new AtomicReference<>();
		AtomicReference<Subscription> sub2 = new AtomicReference<>();
		AtomicBoolean cancel1 = new AtomicBoolean();
		AtomicBoolean cancel2 = new AtomicBoolean();

		Flux<Integer> source1 = Flux.range(1, 5)
		                            .doOnSubscribe(sub1::set)
		                            .doOnCancel(() -> cancel1.set(true))
		                            .hide();
		Flux<Integer> source2 = Flux.just(1, 2, 3, 7, 8)
		                            .doOnSubscribe(sub2::set)
		                            .doOnCancel(() -> cancel2.set(true))
		                            .hide();

		Mono.sequenceEqual(source1, source2)
		    .subscribeWith(new LambdaSubscriber<>(System.out::println, Throwable::printStackTrace, null,
				    Subscription::cancel));

		assertThat(sub1.get()).as("left not subscribed").isNotNull();
		assertThat(cancel1.get()).as("left not cancelled").isTrue();
		assertThat(sub2.get()).as("right not subscribed").isNotNull();
		assertThat(cancel2.get()).as("right not cancelled").isTrue();
	}

	@Test
	public void doubleCancelCancelsOnce() {
		AtomicReference<Subscription> sub1 = new AtomicReference<>();
		AtomicReference<Subscription> sub2 = new AtomicReference<>();
		AtomicLong cancel1 = new AtomicLong();
		AtomicLong cancel2 = new AtomicLong();

		Flux<Integer> source1 = Flux.range(1, 5)
		                            .doOnSubscribe(sub1::set)
		                            .doOnCancel(cancel1::incrementAndGet)
		                            .hide();
		Flux<Integer> source2 = Flux.just(1, 2, 3, 7, 8)
		                            .doOnSubscribe(sub2::set)
		                            .doOnCancel(cancel2::incrementAndGet)
		                            .hide();

		Mono.sequenceEqual(source1, source2)
		    .subscribeWith(new LambdaSubscriber<>(System.out::println, Throwable::printStackTrace, null,
				    s -> { s.cancel(); s.cancel(); }));

		assertThat(sub1.get()).as("left not subscribed").isNotNull();
		assertThat(cancel1).hasValue(1);
		assertThat(sub2.get()).as("right not subscribed").isNotNull();
		assertThat(cancel2).hasValue(1);
	}

	@Test
	public void cancelCancelsBothSourcesIncludingNever() {
		AtomicReference<Subscription> sub1 = new AtomicReference<>();
		AtomicReference<Subscription> sub2 = new AtomicReference<>();
		AtomicBoolean cancel1 = new AtomicBoolean();
		AtomicBoolean cancel2 = new AtomicBoolean();

		Flux<Integer> source1 = Flux.range(1, 5)
		                            .doOnSubscribe(sub1::set)
		                            .doOnCancel(() -> cancel1.set(true))
		                            .hide();
		Flux<Integer> source2 = Flux.<Integer>never()
		                            .doOnSubscribe(sub2::set)
		                            .doOnCancel(() -> cancel2.set(true));

		Mono.sequenceEqual(source1, source2)
		    .subscribeWith(new LambdaSubscriber<>(System.out::println, Throwable::printStackTrace, null,
				    Subscription::cancel));

		assertThat(sub1.get()).as("left not subscribed").isNotNull();
		assertThat(cancel1.get()).as("left not cancelled").isTrue();
		assertThat(sub2.get()).as("right not subscribed").isNotNull();
		assertThat(cancel2.get()).as("right not cancelled").isTrue();
	}

	@Test
	public void subscribeInnerOnce() {
		LongAdder innerSub1 = new LongAdder();
		LongAdder innerSub2 = new LongAdder();

		Flux<Integer> source1 = Flux.range(1, 5)
		                            .doOnSubscribe((t) -> innerSub1.increment());
		Flux<Integer> source2 = Flux.just(1, 2, 3, 7, 8)
		                            .doOnSubscribe((t) -> innerSub2.increment());

		Mono.sequenceEqual(source1, source2)
		    .subscribe();

		assertThat(innerSub1.intValue()).as("left has been subscribed multiple times").isEqualTo(1);
		assertThat(innerSub2.intValue()).as("right has been subscribed multiple times").isEqualTo(1);
	}

	@Test
	public void scanOperator() {
		MonoSequenceEqual<Integer> s = new MonoSequenceEqual<>(Mono.just(1), Mono.just(2), (a, b) -> true, 123);
		assertThat(s.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(s.scan(Scannable.Attr.RUN_STYLE)).isEqualTo(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanCoordinator() {
		CoreSubscriber<Boolean> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoSequenceEqual.EqualCoordinator<String> test = new MonoSequenceEqual.EqualCoordinator<>(actual,
						123,
						Mono.just("foo"),
						Mono.just("bar"),
						(s1, s2) -> s1.equals(s2));

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<Boolean>
				actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoSequenceEqual.EqualCoordinator<String> coordinator = new MonoSequenceEqual.EqualCoordinator<>(actual,
						123,
						Mono.just("foo"),
						Mono.just("bar"),
						(s1, s2) -> s1.equals(s2));

		MonoSequenceEqual.EqualSubscriber<String> test = new MonoSequenceEqual.EqualSubscriber<>(
				coordinator, 456);
		test.queue.offer("foo");
		Subscription sub = Operators.cancelledSubscription();
		test.onSubscribe(sub);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(456);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(coordinator);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
	}

	//TODO multithreaded race between cancel and onNext, between cancel and drain, source overflow, error dropping to hook
}
