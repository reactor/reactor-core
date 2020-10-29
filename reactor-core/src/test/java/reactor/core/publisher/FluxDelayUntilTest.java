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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxDelayUntilTest {

	@Test
	public void testFluxEmptyAndPublisherVoid() {
		Publisher<Void> voidPublisher = Mono.fromRunnable(() -> { });
		StepVerifier.create(Flux.<String>empty().delayUntil(a -> voidPublisher))
		            .verifyComplete();
	}

	@Test
	public void testFlux1AndPublisherVoid() {
		Publisher<Void> voidPublisher = Mono.fromRunnable(() -> { });

		StepVerifier.create(Flux.just("foo").delayUntil(a -> voidPublisher))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testFlux2AndPublisherVoid() {
		Publisher<Void> voidPublisher = Mono.fromRunnable(() -> { });

		StepVerifier.create(Flux.just("foo", "bar").delayUntil(a -> voidPublisher))
		            .expectNext("foo", "bar")
		            .verifyComplete();
	}

	@Test
	public void testFlux2DoesntReorderViaDelays() {
		StepVerifier.withVirtualTime(() ->
				Flux.just(100, 200, 300)
				    .delayUntil(v -> Mono.delay(Duration.ofMillis(400 - v)))
		)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(300))
		            .expectNext(100)
		            .expectNoEvent(Duration.ofMillis(200))
		            .expectNext(200)
		            .expectNoEvent(Duration.ofMillis(100))
		            .expectNext(300)
		            .verifyComplete();
	}

	@Test
	public void triggerSequenceWithDelays() {
		StepVerifier.withVirtualTime(() -> Flux.just("foo", "bar")
		                                       .delayUntil(a -> Flux.just(1, 2, 3).hide().delayElements(Duration.ofMillis(500))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1400))
		            .thenAwait(Duration.ofMillis(100))
		            .expectNext("foo")
		            .expectNoEvent(Duration.ofMillis(1500))
		            .expectNext("bar")
		            .verifyComplete();
	}

	@Test
	public void triggerSequenceHasMultipleValuesNotCancelled() {
		AtomicBoolean triggerCancelled = new AtomicBoolean();
		StepVerifier.create(Flux.just("foo")
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
		StepVerifier.create(Flux.just("foo")
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
		                                       .flatMapMany(Flux::just)
		                                       .delayUntil(a -> Mono.just("foo")))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext(0L)
		            .verifyComplete();
	}

	@Test
	public void sourceHasError() {
		StepVerifier.create(Flux.<String>error(new IllegalStateException("boom"))
				.delayUntil(a -> Mono.just("foo")))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void triggerHasError() {
		StepVerifier.create(Flux.just("foo")
		                        .delayUntil(a -> Mono.<String>error(new IllegalStateException("boom"))))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void sourceAndTriggerHaveErrorsNotDelayed() {
		StepVerifier.create(Flux.<String>error(new IllegalStateException("boom1"))
				.delayUntil(a -> Mono.<Integer>error(new IllegalStateException("boom2"))))
		            .verifyErrorMessage("boom1");
	}


	@Test
	public void testAPIDelayUntil() {
		StepVerifier.withVirtualTime(() -> Flux.just("foo")
		                                       .delayUntil(a -> Mono.delay(Duration.ofSeconds(2))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testAPIDelayUntilErrorsImmediately() {
		IllegalArgumentException boom = new IllegalArgumentException("boom");
		StepVerifier.create(Flux.error(boom)
		                        .delayUntil(a -> Mono.delay(Duration.ofSeconds(2))))
		            .expectErrorMessage("boom")
		            .verify(Duration.ofMillis(200)); //at least, less than 2s
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

		StepVerifier.withVirtualTime(() -> Flux.just("foo")
		                                       .delayElements(Duration.ofSeconds(3))
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
	public void immediateCancel() {
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Disposable s = Flux.just("foo", "bar")
		                   .delayUntil(v -> Mono.just(1))
		                   .subscribeWith(new LambdaSubscriber<>(value::set, error::set, () -> {}, Subscription::cancel));

		assertThat(value.get()).isNull();
		assertThat(error.get()).isNull(); //would be a NPE if trigger array wasn't pre-initialized
	}

	@Test
	public void isAlias() {
		assertThat(Flux.range(1, 10).delayUntil(a -> Mono.empty()))
				.isInstanceOf(FluxConcatMap.class);
	}

}
