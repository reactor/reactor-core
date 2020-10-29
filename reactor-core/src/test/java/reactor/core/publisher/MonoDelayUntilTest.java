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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
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
	public void triggerThrows() {
		StepVerifier.create(Mono.just("foo")
		                        .delayUntil(a -> {throw new IllegalStateException("boom");}))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void sourceAndTriggerHaveErrorsNotDelayed() {
		StepVerifier.create(Mono.<String>error(new IllegalStateException("boom1"))
				.delayUntil(a -> Mono.<Integer>error(new IllegalStateException("boom2"))))
		            .verifyErrorMessage("boom1");
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

	@SuppressWarnings("unchecked")
	@Test
	public void testAPIchainingCombines() {
		Mono<String> source = Mono.just("foo");

		Function<String, Flux<Integer>> generator1 = a -> Flux.just(1, 2, 3);
		Function<Object, Mono<Long>> generator2 = a -> Mono.delay(Duration.ofMillis(800));

		MonoDelayUntil<String> until1 = (MonoDelayUntil<String>) source.delayUntil(generator1);
		MonoDelayUntil<String> until2 = (MonoDelayUntil<String>) until1.delayUntil(generator2);

		assertThat(until1)
				.isNotSameAs(until2)
				;
		assertThat(until1.source)
				.isSameAs(until2.source)
				;
		assertThat(until1.otherGenerators).containsExactly(generator1);
		assertThat(until2.otherGenerators).containsExactly(generator1, generator2);

		StepVerifier.create(until2)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(700))
		            .thenAwait(Duration.ofMillis(100))
		            .expectNext("foo")
		            .verifyComplete();
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
		            .then(() -> assertThat(generator1Used).hasValue(1))
		            .then(() -> assertThat(generator2Used).hasValue(0))
		            .expectNoEvent(Duration.ofMillis(400))
		            .then(() -> assertThat(generator2Used).hasValue(1))
		            .expectNoEvent(Duration.ofMillis(800))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void scanOperator(){
	    Mono<Integer> source = Mono.just(1);
		MonoDelayUntil<Integer> test = new MonoDelayUntil<>(source, i -> Mono.just(1));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanCoordinator() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		@SuppressWarnings("unchecked")
		Function<? super String, ? extends Publisher<?>>[] otherGenerators = new Function[3];
		MonoDelayUntil.DelayUntilCoordinator<String> test = new MonoDelayUntil.DelayUntilCoordinator<>(actual, otherGenerators);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PARENT)).isNull();
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.done = 2;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.done = 3;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanTrigger() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		@SuppressWarnings("unchecked")
		Function<? super String, ? extends Publisher<?>>[] otherGenerators = new Function[3];
		MonoDelayUntil.DelayUntilCoordinator<String> main = new MonoDelayUntil.DelayUntilCoordinator<>(
				actual, otherGenerators);

		MonoDelayUntil.DelayUntilTrigger<String> test = new MonoDelayUntil.DelayUntilTrigger<>(main);

		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(subscription);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();

		test.error = new IllegalStateException("boom");
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
	}

	@Test
	public void testNullPublisherFromMapper() {
		StepVerifier.create(Mono.just("foo").delayUntil(a -> null))
		            .expectErrorMessage("mapper returned null value")
					.verify();
	}
}
