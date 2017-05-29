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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoDelayUntilTest {

	@Test
	public void testMonoValuedAndPublisherVoid() {
		Publisher<Void> voidPublisher = Mono.fromRunnable(() -> { });

		StepVerifier.create(Mono.just("foo").delayUntil(a -> voidPublisher))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testMonoEmptyAndPublisherVoid() {
		Publisher<Void> voidPublisher = Mono.fromRunnable(() -> { });
		StepVerifier.create(Mono.<String>empty().delayUntil(a -> voidPublisher))
		            .verifyComplete();
	}

	@Test
	public void triggerSequenceWithDelays() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .delayUntil(a -> Flux.just(1, 2, 3)
		                                                            .hide()
		                                                            .delayElements(Duration.ofMillis(500))))
		                                .expectSubscription()
		                                .expectNoEvent(Duration.ofMillis(1400))
		                                .thenAwait(Duration.ofMillis(100))
		                                .expectNext("foo")
		                                .verifyComplete();
	}

	@Test
	public void triggerSequenceHasMultipleValuesNotCancelled() {
		AtomicBoolean triggerCancelled = new AtomicBoolean();
		StepVerifier.create(Mono.just("foo")
		                        .delayUntil(
				                        a -> Flux.just(1, 2, 3).hide()
				                                 .doOnCancel(() -> triggerCancelled.set(true))))
		            .expectNext("foo")
		            .verifyComplete();
		assertThat(triggerCancelled.get()).isFalse();
	}

	@Test
	public void triggerSequenceHasSingleValueNotCancelled() {
		AtomicBoolean triggerCancelled = new AtomicBoolean();
		StepVerifier.create(Mono.just("foo")
		                        .delayUntil(
				                        a -> Mono.just(1)
				                                 .doOnCancel(() -> triggerCancelled.set(true))))
		            .expectNext("foo")
		            .verifyComplete();
		assertThat(triggerCancelled.get()).isFalse();
	}

	@Test
	public void triggerSequenceDoneFirst() {
		StepVerifier.withVirtualTime(() -> Mono.delay(Duration.ofSeconds(2))
		                                       .delayUntil(a -> Mono.just("foo")))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext(0L)
		            .verifyComplete();
	}

	@Test
	public void sourceHasError() {
		StepVerifier.create(Mono.<String>error(new IllegalStateException("boom"))
				.delayUntil(a -> Mono.just("foo")))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void triggerHasError() {
		StepVerifier.create(Mono.just("foo")
		                        .delayUntil(a -> Mono.<String>error(new IllegalStateException("boom"))))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void sourceAndTriggerHaveErrorsNotDelayed() {
		StepVerifier.create(Mono.<String>error(new IllegalStateException("boom1"))
				.delayUntil(a -> Mono.<Integer>error(new IllegalStateException("boom2"))))
		            .verifyErrorMessage("boom1");
	}

	@Test
	public void sourceAndTriggerHaveErrorsDelayedShortCircuits() {
		IllegalStateException boom1 = new IllegalStateException("boom1");
		IllegalStateException boom2 = new IllegalStateException("boom2");
		StepVerifier.create(new MonoDelayUntil<>(true,
				Mono.<String>error(boom1),
				a -> Mono.<Integer>error(boom2)))
		            .verifyErrorMessage("boom1");
	}

	@Test
	public void multipleTriggersWithErrorDelayed() {
		IllegalStateException boom1 = new IllegalStateException("boom1");
		IllegalStateException boom2 = new IllegalStateException("boom2");
		StepVerifier.create(new MonoDelayUntil<>(true,
				Mono.just("ok"), a -> Mono.<Integer>error(boom1))
				.delayUntilDelayError(a -> Mono.error(boom2))
		)
		            .verifyErrorMatches(e -> e.getMessage().equals("Multiple errors") &&
				            e.getSuppressed()[0] == boom1 &&
				            e.getSuppressed()[1] == boom2);
	}

	@Test
	public void testAPIDelayUntil() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .delayUntil(a -> Mono.delay(Duration.ofSeconds(2))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testAPIDelayUntilErrorsImmediately() {
		IllegalArgumentException boom = new IllegalArgumentException("boom");
		StepVerifier.create(Mono.error(boom)
		                        .delayUntil(a -> Mono.delay(Duration.ofSeconds(2))))
		            .expectErrorMessage("boom")
		            .verify(Duration.ofMillis(200)); //at least, less than 2s
	}

	@Test
	public void testAPIDelayUntilDelayErrorNoError() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .delayUntilDelayError(a -> Mono.delay(Duration.ofSeconds(2))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testAPIDelayUntilDelayErrorWaitsOtherTriggers() {
		IllegalArgumentException boom = new IllegalArgumentException("boom");

		StepVerifier.withVirtualTime(() -> Mono.just("ok")
		                                       .delayUntilDelayError(a -> Mono.error(boom))
		                                       .delayUntilDelayError(a -> Mono.delay(Duration.ofSeconds(2))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .verifyErrorMessage("boom");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAPIchainingCombines() {
		Mono<String> source = Mono.just("foo");

		Function<String, Flux<Integer>> generator1 = a -> Flux.just(1, 2, 3);
		Function<Object, Mono<Long>> generator2 = a -> Mono.delay(Duration.ofMillis(800));

		MonoDelayUntil<String> until1 = (MonoDelayUntil<String>) source.delayUntil(generator1);
		MonoDelayUntil<String> until2 = (MonoDelayUntil<String>) until1.delayUntilDelayError(generator2);
		MonoDelayUntil<String> until3 = (MonoDelayUntil<String>) until2.delayUntilOther(Mono.empty());
		MonoDelayUntil<String> until4 = (MonoDelayUntil<String>) until3.delayUntilOtherDelayError(Mono.empty());

		assertThat(until1)
				.isNotSameAs(until2)
				.isNotSameAs(until3)
				.isNotSameAs(until4);
		assertThat(until1.source)
				.isSameAs(until2.source)
				.isSameAs(until3.source)
				.isSameAs(until4.source);
		assertThat(until1.otherGenerators).containsExactly(generator1);
		assertThat(until2.otherGenerators).containsExactly(generator1, generator2);
		assertThat(until3.otherGenerators)
				.startsWith(generator1, generator2)
				.hasSize(3);
		assertThat(until4.otherGenerators)
				.startsWith(generator1, generator2)
				.hasSize(4);

		StepVerifier.create(until2)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(700))
		            .thenAwait(Duration.ofMillis(100))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAPIchainingCombinesUseAnyDelayError() {
		Mono<String> source = Mono.just("foo");

		Function<String, Mono<String>> generator1 = a -> Mono.error(new IllegalArgumentException("boom"));
		Function<Object, Mono<Long>> generator2 = a -> Mono.delay(Duration.ofMillis(800));

		MonoDelayUntil<String> until1 = (MonoDelayUntil<String>) source.delayUntil(generator1);
		MonoDelayUntil<String> until2 = (MonoDelayUntil<String>) until1.delayUntilDelayError(generator2);
		MonoDelayUntil<String> until3 = (MonoDelayUntil<String>) until2.delayUntilOther(Mono.empty());

		assertThat(until1)
				.isNotSameAs(until2)
				.isNotSameAs(until3);
		assertThat(until1.source)
				.isSameAs(until2.source)
				.isSameAs(until3.source);
		assertThat(until1.otherGenerators).containsExactly(generator1);
		assertThat(until2.otherGenerators).containsExactly(generator1, generator2);
		assertThat(until3.otherGenerators).startsWith(generator1, generator2)
		                                  .hasSize(3);

		assertThat(until1.delayError).isFalse();
		assertThat(until2.delayError).isTrue();
		assertThat(until3.delayError).isTrue();

		StepVerifier.create(until2)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(700))
		            .thenAwait(Duration.ofMillis(100))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void testAPIchainingCumulatesDelaysAfterValueGenerated() {
		AtomicInteger generator1Used = new AtomicInteger();
		AtomicInteger generator2Used = new AtomicInteger();

		Function<String, Mono<Long>> generator1 = a -> {
			generator1Used.incrementAndGet();
			return Mono.delay(Duration.ofMillis(400));
		};
		Function<Object, Mono<Long>> generator2 = a -> {
			generator2Used.incrementAndGet();
			return Mono.delay(Duration.ofMillis(800));
		};

		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                              .delayElement(Duration.ofSeconds(3))
		                              .delayUntil(generator1)
		                              .delayUntil(generator2))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(2900))
		            .then(() -> assertThat(generator1Used.get()).isZero())
		            .then(() -> assertThat(generator2Used.get()).isZero())
		            .expectNoEvent(Duration.ofMillis(100))
		            .then(() -> assertThat(generator1Used.get()).isEqualTo(1))
		            .then(() -> assertThat(generator2Used.get()).isEqualTo(0))
		            .expectNoEvent(Duration.ofMillis(400))
		            .then(() -> assertThat(generator2Used.get()).isEqualTo(1))
		            .expectNoEvent(Duration.ofMillis(800))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testChainingDelayUntilOtherWithLastTriggerError() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .delayElement(Duration.ofSeconds(3))
		                                       .delayUntilOtherDelayError(Mono.delay(Duration.ofMillis(800)))
		                                       .delayUntilOther(Mono.delay(Duration.ofMillis(200)))
		                                       .delayUntilOtherDelayError(Mono.error(new IllegalStateException("boom")))
		)
	                .thenAwait(Duration.ofSeconds(4))
	                .verifyErrorMessage("boom");
	}

	@Test
	public void immediateCancel() {
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Disposable s = Mono.just("foo")
		                   .delayUntilOther(Mono.just(1))
		                   .subscribe(value::set, error::set, () -> {}, Subscription::cancel);

		assertThat(value.get()).isNull();
		assertThat(error.get()).isNull(); //would be a NPE if trigger array wasn't pre-initialized
	}

	@Test
	public void scanCoordinator() {
		Subscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoDelayUntil.DelayUntilCoordinator<String> test = new MonoDelayUntil.DelayUntilCoordinator<>(
				actual, true, new Function[3]);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isNull();
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.DELAY_ERROR)).isTrue();
		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.done = 2;
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.done = 3;
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

	@Test
	public void scanTrigger() {
		Subscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoDelayUntil.DelayUntilCoordinator<String> main = new MonoDelayUntil.DelayUntilCoordinator<>(
				actual, false, new Function[3]);

		MonoDelayUntil.DelayUntilTrigger<String> test = new MonoDelayUntil.DelayUntilTrigger<>(main);

		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(main.scan(Scannable.BooleanAttr.DELAY_ERROR)).isFalse();

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(subscription);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(main);

		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();

		test.error = new IllegalStateException("boom");
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
	}
}