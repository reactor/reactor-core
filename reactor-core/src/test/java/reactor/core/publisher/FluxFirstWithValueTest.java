/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FluxFirstWithValueTest {

	@Test
	void firstSourceEmittingValueIsChosen() {
		StepVerifier.withVirtualTime(() -> Flux.firstWithValue(
				Flux.just(1, 2, 3).delayElements(Duration.ofMillis(500L)),
				Flux.just(4, 5, 6).delayElements(Duration.ofMillis(1_000L))
		))
				.thenAwait(Duration.ofMillis(1_500L))
				.expectNext(1, 2, 3)
				.verifyComplete();
	}

	@Test
	void firstSourceEmittingValueIsChosenOverErrorOrCompleteEmpty() {
		StepVerifier.withVirtualTime(() -> Flux.firstWithValue(
				Flux.just(1, 2, 3).delayElements(Duration.ofMillis(500L)),
				Flux.error(new RuntimeException("Boom!")),
				Flux.empty(),
				Flux.never()
		))
				.thenAwait(Duration.ofMillis(1_500L))
				.expectNext(1, 2, 3)
				.verifyComplete();
	}

	@Test
	void singleErrorIsPropagatedAsIs() {
		StepVerifier.create(Flux.firstWithValue(Flux.error(new IllegalStateException("boom"))))
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("boom")
				            .hasNoCause()
				            .hasNoSuppressedExceptions())
		            .verify();
	}

	@Test
	void singleEmptyLeadsToComplete() {
		StepVerifier.create(Flux.firstWithValue(Flux.empty()))
		            .expectComplete()
		            .verify();
	}

	@Test
	void errorAndEmptySourcesTriggerNoSuchElementWithOneSuppressedCompositeOfTwo() {
		StepVerifier.create(Flux.firstWithValue(
				Flux.error(new IllegalStateException("boom")),
				Flux.empty()))
		            .expectErrorSatisfies(e -> {
			            assertThat(e)
					            .isInstanceOf(NoSuchElementException.class)
					            .hasMessage("All sources completed with error or without values")
					            .hasCauseInstanceOf(RuntimeException.class);
			            List<Throwable> composite = Exceptions.unwrapMultiple(e.getCause());
			            assertThat(composite)
					            .hasSize(2)
					            .extracting(Throwable::toString)
					            .containsExactly("java.lang.IllegalStateException: boom",
							            "java.util.NoSuchElementException: source at index 1 completed empty");
		            })
		            .verify();
	}

	@Test
	void twoErrorsTriggerNoSuchElementWithOneSuppressedCompositeOfBothErrors() {
		StepVerifier.create(Flux.firstWithValue(Flux.error(new IllegalStateException("boom1")), Flux.error(new IllegalArgumentException("boom2"))))
		            .expectErrorSatisfies(e -> {
			            assertThat(e)
					            .isInstanceOf(NoSuchElementException.class)
					            .hasMessage("All sources completed with error or without values")
					            .hasCauseInstanceOf(RuntimeException.class);
			            List<Throwable> composite = Exceptions.unwrapMultiple(e.getCause());
			            assertThat(composite)
					            .hasSize(2)
					            .extracting(Throwable::toString)
					            .containsExactly("java.lang.IllegalStateException: boom1",
							            "java.lang.IllegalArgumentException: boom2");
		            })
		            .verify();
	}

	@Test
	void twoEmptyTriggersNoSuchElementWithOneSuppressedCompositeOfTwoNoSuchElement() {
		StepVerifier.create(Flux.firstWithValue(Flux.empty(), Flux.empty()))
		            .expectErrorSatisfies(e -> {
			            assertThat(e)
					            .isInstanceOf(NoSuchElementException.class)
					            .hasMessage("All sources completed with error or without values")
					            .hasCauseInstanceOf(RuntimeException.class);
			            List<Throwable> composite = Exceptions.unwrapMultiple(e.getCause());
			            assertThat(composite)
					            .hasSize(2)
					            .extracting(Throwable::toString)
					            .containsExactly("java.util.NoSuchElementException: source at index 0 completed empty",
							            "java.util.NoSuchElementException: source at index 1 completed empty");
		            })
		            .verify();
	}

	@Test
	void firstNull() {
		assertThrows(NullPointerException.class, () -> Flux.firstWithValue(null, Flux.just(1), Flux.just(2)));
	}

	@Test
	void arrayNull() {
		assertThrows(NullPointerException.class, () -> Flux.firstWithValue(Flux.just(1), (Publisher<Integer>[]) null));
	}

	@Test
	void iterableNull() {
		assertThrows(NullPointerException.class, () -> Flux.firstWithValue((Iterable<Publisher<Integer>>) null));
	}

	@Test
	void requestAndCancelArePropagated() {
		TestPublisher<Integer> pub1 = TestPublisher.create();
		TestPublisher<Integer> pub2 = TestPublisher.create();

		StepVerifier.create(Flux.firstWithValue(pub1, pub2))
				.thenRequest(4)
				.then(() -> {
					pub1.emit(1, 2, 3, 4, 5).complete();
					pub2.emit(6, 7, 8, 9, 10).complete();
				})
				.expectNext(1, 2, 3, 4)
				.thenCancel()
				.verify(Duration.ofSeconds(1L));

		pub1.assertWasSubscribed();
		pub1.assertMaxRequested(4);
		pub1.assertCancelled();

		pub2.assertWasSubscribed();
		pub2.assertMaxRequested(4);
		pub2.assertWasCancelled();
	}

	@Test
	void singleNullSourceInVararg() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstWithValue(Mono.empty(), (Publisher<Object>) null)
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	void arrayOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstWithValue(Flux.never(), null, Flux.never())
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	void singleIterableNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstWithValue(Arrays.asList((Publisher<Object>) null))
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	void iterableOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstWithValue(Arrays.asList(Flux.never(),
				(Publisher<Object>) null,
				Flux.never()))
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	void pairWise() {
		Flux<Integer> firstValues = Flux.firstWithValue(Flux.just(1), Mono.just(2));
		Flux<Integer> orValues = Flux.firstWithValue(firstValues, Mono.just(3));

		assertThat(orValues).isInstanceOf(FluxFirstWithValue.class);
		assertThat(((FluxFirstWithValue<Integer>) orValues).array)
				.isNotNull()
				.hasSize(3);

		orValues.subscribeWith(AssertSubscriber.create())
				.assertValues(1)
				.assertComplete();
	}

	@Test
	void combineMoreThanOneAdditionalSource() {
		Flux<Integer> step1 = Flux.firstWithValue(Flux.just(1), Mono.just(2));
		Flux<Integer> step2 = Flux.firstWithValue(step1, Flux.just(3), Mono.just(4));

		assertThat(step2).isInstanceOfSatisfying(FluxFirstWithValue.class,
				ffv -> assertThat(ffv.array)
						.hasSize(4)
						.doesNotContainNull());
	}

	// See https://github.com/reactor/reactor-core/issues/2557
	@Test
	void protectAgainstSparseArray() {
		assertThatCode(() -> Flux.firstWithValue(Arrays.asList(Mono.just(1), Mono.empty())).blockFirst())
				.doesNotThrowAnyException();
	}

	@Test
	void scanOperator() {
		@SuppressWarnings("unchecked") FluxFirstWithValue<Integer>
				test = new FluxFirstWithValue<>(Flux.range(1, 10), Flux.range(11, 10));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {
		}, null, null);
		FluxFirstWithValue.RaceValuesCoordinator<String> parent = new FluxFirstWithValue.RaceValuesCoordinator<>(1);
		FluxFirstWithValue.FirstValuesEmittingSubscriber<String> test = new FluxFirstWithValue.FirstValuesEmittingSubscriber<>(actual, parent, 1);
		Subscription sub = Operators.emptySubscription();
		test.onSubscribe(sub);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		parent.cancelled = true;
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	void scanRaceCoordinator() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {
		}, null, null);
		FluxFirstWithValue.RaceValuesCoordinator<String> parent = new FluxFirstWithValue.RaceValuesCoordinator<>(1);
		FluxFirstWithValue.FirstValuesEmittingSubscriber<String> test = new FluxFirstWithValue.FirstValuesEmittingSubscriber<>(actual, parent, 1);
		Subscription sub = Operators.emptySubscription();
		test.onSubscribe(sub);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(parent.scan(Scannable.Attr.CANCELLED)).isFalse();
		parent.cancelled = true;
		assertThat(parent.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

}
