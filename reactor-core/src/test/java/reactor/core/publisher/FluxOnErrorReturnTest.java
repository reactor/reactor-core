/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * @author Simon Baslé
 */
class FluxOnErrorReturnTest {

	@Test
	void fluxApiReturn_disallowsNulls() {
		assertThatNullPointerException().isThrownBy(() -> Flux.empty().onErrorReturn(null))
			.withMessage("fallbackValue must not be null");
		assertThatNullPointerException().isThrownBy(() -> Flux.empty().onErrorReturn(Throwable.class, null))
			.withMessage("fallbackValue must not be null");
		assertThatNullPointerException().isThrownBy(() -> Flux.empty().onErrorReturn(e -> true, null))
			.withMessage("fallbackValue must not be null");

		assertThatNullPointerException().isThrownBy(() -> Flux.empty().onErrorReturn((Class<Throwable>) null, "test"))
			.withMessage("type must not be null");
		assertThatNullPointerException().isThrownBy(() -> Flux.empty().onErrorReturn((Predicate<? super Throwable>) null, "test"))
			.withMessage("predicate must not be null");
	}

	@Test
	void fluxApiComplete_disallowsNulls() {
		assertThatNullPointerException().isThrownBy(() -> Flux.empty().onErrorComplete((Class<Throwable>) null))
			.withMessage("type must not be null");
		assertThatNullPointerException().isThrownBy(() -> Flux.empty().onErrorComplete((Predicate<? super Throwable>) null))
			.withMessage("predicate must not be null");
	}

	@Test
	void monoApiReturn_disallowsNulls() {
		assertThatNullPointerException().isThrownBy(() -> Mono.empty().onErrorReturn(null))
			.withMessage("fallbackValue must not be null");
		assertThatNullPointerException().isThrownBy(() -> Mono.empty().onErrorReturn(Throwable.class, null))
			.withMessage("fallbackValue must not be null");
		assertThatNullPointerException().isThrownBy(() -> Mono.empty().onErrorReturn(e -> true, null))
			.withMessage("fallbackValue must not be null");

		assertThatNullPointerException().isThrownBy(() -> Mono.empty().onErrorReturn((Class<Throwable>) null, "test"))
			.withMessage("type must not be null");
		assertThatNullPointerException().isThrownBy(() -> Mono.empty().onErrorReturn((Predicate<? super Throwable>) null, "test"))
			.withMessage("predicate must not be null");
	}

	@Test
	void monoApiComplete_disallowsNulls() {
		assertThatNullPointerException().isThrownBy(() -> Mono.empty().onErrorComplete((Class<Throwable>) null))
			.withMessage("type must not be null");
		assertThatNullPointerException().isThrownBy(() -> Mono.empty().onErrorComplete((Predicate<? super Throwable>) null))
			.withMessage("predicate must not be null");
	}

	@Test
	void onErrorReturnFluxApi() {
		IllegalStateException exception = new IllegalStateException("ignored");
		Flux<String> source = Flux.error(exception);

		StepVerifier.create(source.onErrorReturn("basic"))
			.expectNext("basic")
			.verifyComplete();

		StepVerifier.create(source.onErrorReturn(IllegalStateException.class, "class"))
			.expectNext("class")
			.verifyComplete();

		StepVerifier.create(source.onErrorReturn(e -> true, "predicate"))
			.expectNext("predicate")
			.verifyComplete();
	}

	@Test
	void onErrorReturnFluxApiWithNonMatchingErrors() {
		IllegalStateException exception = new IllegalStateException("expected");
		Flux<String> source = Flux.error(exception);

		StepVerifier.create(source.onErrorReturn(IllegalArgumentException.class, "class"))
			.verifyErrorMessage("expected");

		StepVerifier.create(source.onErrorReturn(e -> false, "predicate"))
			.verifyErrorMessage("expected");
	}

	@Test
	void onErrorCompleteFluxApi() {
		IllegalStateException exception = new IllegalStateException("ignored");
		Flux<String> source = Flux.error(exception);

		StepVerifier.create(source.onErrorComplete())
			.verifyComplete();

		StepVerifier.create(source.onErrorComplete(IllegalStateException.class))
			.verifyComplete();

		StepVerifier.create(source.onErrorComplete(e -> true))
			.verifyComplete();
	}

	@Test
	void onErrorCompleteFluxApiWithNonMatchingErrors() {
		IllegalStateException exception = new IllegalStateException("expected");
		Flux<String> source = Flux.error(exception);

		StepVerifier.create(source.onErrorComplete(IllegalArgumentException.class))
			.verifyErrorMessage("expected");

		StepVerifier.create(source.onErrorComplete(e -> false))
			.verifyErrorMessage("expected");
	}

	@Test
	void onErrorReturnMonoApi() {
		IllegalStateException exception = new IllegalStateException("ignored");
		Mono<String> source = Mono.error(exception);

		StepVerifier.create(source.onErrorReturn("basic"))
			.expectNext("basic")
			.verifyComplete();

		StepVerifier.create(source.onErrorReturn(IllegalStateException.class, "class"))
			.expectNext("class")
			.verifyComplete();

		StepVerifier.create(source.onErrorReturn(e -> true, "predicate"))
			.expectNext("predicate")
			.verifyComplete();
	}

	@Test
	void onErrorReturnMonoApiWithNonMatchingErrors() {
		IllegalStateException exception = new IllegalStateException("expected");
		Mono<String> source = Mono.error(exception);

		StepVerifier.create(source.onErrorReturn(IllegalArgumentException.class, "class"))
			.verifyErrorMessage("expected");

		StepVerifier.create(source.onErrorReturn(e -> false, "predicate"))
			.verifyErrorMessage("expected");
	}

	@Test
	void onErrorCompleteMonoApi() {
		IllegalStateException exception = new IllegalStateException("ignored");
		Mono<String> source = Mono.error(exception);

		StepVerifier.create(source.onErrorComplete())
			.verifyComplete();

		StepVerifier.create(source.onErrorComplete(IllegalStateException.class))
			.verifyComplete();

		StepVerifier.create(source.onErrorComplete(e -> true))
			.verifyComplete();
	}

	@Test
	void onErrorCompleteMonoApiWithNonMatchingErrors() {
		IllegalStateException exception = new IllegalStateException("expected");
		Mono<String> source = Mono.error(exception);

		StepVerifier.create(source.onErrorComplete(IllegalArgumentException.class))
			.verifyErrorMessage("expected");

		StepVerifier.create(source.onErrorComplete(e -> false))
			.verifyErrorMessage("expected");
	}

	@Test
	void scanFlux() {
		Flux<Integer> parent = Flux.just(1);
		FluxOnErrorReturn<Integer> test = new FluxOnErrorReturn<>(parent, null, null);

		assertThat(test.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).as("RUN_STYLE").isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	void scanMono() {
		Mono<Integer> parent = Mono.just(1);
		MonoOnErrorReturn<Integer> test = new MonoOnErrorReturn<>(parent, null, null);

		assertThat(test.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).as("RUN_STYLE").isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	void scanSubscriber() {
		@SuppressWarnings("unchecked")
		CoreSubscriber<Integer> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);

		FluxOnErrorReturn.ReturnSubscriber<Integer> test = new FluxOnErrorReturn.ReturnSubscriber<>(actual, null, null, false);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).as("TERMINATED pre-complete").isFalse();
		test.onComplete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).as("TERMINATED post-complete").isTrue();

		FluxOnErrorReturn.ReturnSubscriber.REQUESTED.set(test, 1L);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).as("TERMINATED pre-error").isFalse();
		test.onError(new IllegalStateException("expected"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).as("TERMINATED post-complete").isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).as("CANCELLED pre-cancel").isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).as("CANCELLED post-cancel").isTrue();
	}

	@Test
	void propagateOnNextOnComplete() {
		Flux<Integer> source = Flux.just(1, 2, 3).hide();

		Flux<Integer> test = new FluxOnErrorReturn<>(source, null, -1);

		StepVerifier.create(test)
			.expectNext(1, 2, 3)
			.verifyComplete();
	}

	@Test
	void propagateOnNextOnCompleteWithBackpressure() {
		Flux<Integer> source = Flux.just(1, 2, 3).hide();

		Flux<Integer> test = new FluxOnErrorReturn<>(source, null, -1);

		StepVerifier.create(test, 2)
			.expectNext(1, 2)
			.expectNoEvent(Duration.ofMillis(500))
			.thenRequest(2)
			.expectNext(3)
			.verifyComplete();
	}

	@Test
	void propagateOnNextOnCompleteWithExactRequest() {
		Flux<Integer> source = Flux.just(1, 2, 3).hide();

		Flux<Integer> test = new FluxOnErrorReturn<>(source, null, -1);

		StepVerifier.create(test, 3)
			.expectNext(1, 2,3)
			.verifyComplete();
	}

	@Test
	void propagateErrorIfItDoesntMatchPredicate() {
		IllegalStateException expected = new IllegalStateException("expected");
		Flux<Integer> source = Flux.concat(Flux.just(1, 2), Flux.error(expected));

		Flux<Integer> test = new FluxOnErrorReturn<>(source, IllegalArgumentException.class::isInstance, -1);

		StepVerifier.create(test)
			.expectNext(1, 2)
			.verifyErrorSatisfies(e -> assertThat(e).isSameAs(expected));
	}

	@Test
	void propagateErrorIfItDoesntMatchPredicateEvenWithZeroRequest() {
		IllegalStateException expected = new IllegalStateException("expected");
		Flux<Integer> source = Flux.concat(Flux.just(1, 2), Flux.error(expected));

		Flux<Integer> test = new FluxOnErrorReturn<>(source, IllegalArgumentException.class::isInstance, -1);

		StepVerifier.create(test, 2)
			.expectNext(1, 2)
			.verifyErrorSatisfies(e -> assertThat(e).isSameAs(expected));
	}

	@Test
	void completeIfNoFallback() {
		IllegalStateException ignored = new IllegalStateException("ignored");
		Flux<Integer> source = Flux.concat(Flux.just(1, 2), Flux.error(ignored));

		Flux<Integer> test = new FluxOnErrorReturn<>(source, null, null);

		StepVerifier.create(test)
			.expectNext(1, 2)
			.verifyComplete();
	}

	@Test
	void completeIfNoFallbackEvenWithExactRequest() {
		IllegalStateException ignored = new IllegalStateException("ignored");
		Flux<Integer> source = Flux.concat(Flux.just(1, 2), Flux.error(ignored));

		Flux<Integer> test = new FluxOnErrorReturn<>(source, null, null);

		StepVerifier.create(test, 2)
			.expectNext(1, 2)
			.verifyComplete();
	}


	@Test
	void produceFallbackValueWithUnboundedRequest() {
		IllegalStateException recovered = new IllegalStateException("recovered");
		Flux<Integer> source = Flux.concat(Flux.just(1, 2), Flux.error(recovered));

		Flux<Integer> test = new FluxOnErrorReturn<>(source, null, 100);

		StepVerifier.create(test)
			.expectNext(1, 2, 100)
			.verifyComplete();
	}

	@Test
	void produceFallbackValueWithJustEnoughRequest() {
		IllegalStateException recovered = new IllegalStateException("recovered");
		Flux<Integer> source = Flux.concat(Flux.just(1, 2), Flux.error(recovered));

		Flux<Integer> test = new FluxOnErrorReturn<>(source, null, 100);

		StepVerifier.create(test, 3)
			.expectNext(1, 2, 100)
			.verifyComplete();
	}

	@Test
	void dontProduceExtraneousFallbackValueWithUnderRequest() {
		IllegalStateException recovered = new IllegalStateException("recovered");
		Flux<Integer> source = Flux.concat(Flux.just(1, 2), Flux.error(recovered));

		Flux<Integer> test = new FluxOnErrorReturn<>(source, null, 100);

		StepVerifier.create(test, 2)
			.expectNext(1, 2)
			.verifyTimeout(Duration.ofSeconds(1));
	}

	@Test
	void produceFallbackWithIncrementalRequest() {
		IllegalStateException ignored = new IllegalStateException("ignored");
		Flux<Integer> source = Flux.concat(Flux.just(1, 2), Flux.error(ignored));

		Flux<Integer> test = new FluxOnErrorReturn<>(source, null, 100);

		StepVerifier.create(test, 2)
			.expectNext(1, 2)
			.expectNoEvent(Duration.ofSeconds(1))
			.thenRequest(1)
			.expectNext(100)
			.verifyComplete();
	}
}