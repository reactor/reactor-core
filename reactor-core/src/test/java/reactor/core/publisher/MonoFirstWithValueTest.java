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

import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MonoFirstWithValueTest {

	@Test
	void firstSourceEmittingValueIsChosen() {
		StepVerifier.withVirtualTime(() -> Mono.firstWithValue(
				Mono.just(1).delayElement(Duration.ofMillis(500L)),
				Mono.just(2).delayElement(Duration.ofMillis(1_000L))
		))
				.thenAwait(Duration.ofMillis(1_500L))
				.expectNext(1)
				.verifyComplete();
	}

	@Test
	void firstSourceEmittingValueIsChosenOverErrorOrCompleteEmpty() {
		StepVerifier.withVirtualTime(() -> Mono.firstWithValue(
				Mono.just(1).delayElement(Duration.ofMillis(500L)),
				Mono.error(new RuntimeException("Boom!")),
				Mono.empty(),
				Mono.never()
		))
				.thenAwait(Duration.ofMillis(500L))
				.expectNext(1)
				.verifyComplete();
	}

	@Test
	void singleErrorIsPropagatedAsIs() {
		StepVerifier.create(Mono.firstWithValue(Mono.error(new IllegalStateException("boom"))))
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("boom")
				            .hasNoCause()
				            .hasNoSuppressedExceptions())
		            .verify();
	}

	@Test
	void singleEmptyLeadsToComplete() {
		StepVerifier.create(Mono.firstWithValue(Mono.empty()))
		            .expectComplete()
		            .verify();
	}

	@Test
	void errorAndEmptySourcesTriggerNoSuchElementWithOneSuppressedCompositeOfTwo() {
		StepVerifier.create(Mono.firstWithValue(
				Mono.error(new IllegalStateException("boom")),
				Mono.empty()
		))
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
		StepVerifier.create(Mono.firstWithValue(Mono.error(new IllegalStateException("boom1")), Mono.error(new IllegalArgumentException("boom2"))))
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
		StepVerifier.create(Mono.firstWithValue(Mono.empty(), Mono.empty()))
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
		assertThrows(NullPointerException.class, () -> Mono.firstWithValue(null, Mono.just(1), Mono.just(2)));
	}

	@Test
	void arrayNull() {
		assertThrows(NullPointerException.class, () -> Mono.firstWithValue(Mono.just(1),(Mono<Integer>[]) null));
	}

	@Test
	void iterableNull() {
		assertThrows(NullPointerException.class, () -> Mono.firstWithValue((Iterable<Mono<Integer>>) null));
	}

	@Test
	void cancelIsPropagated() {
		TestPublisher<Integer> pub1 = TestPublisher.create();
		TestPublisher<Integer> pub2 = TestPublisher.create();

		StepVerifier.create(Mono.firstWithValue(Mono.from(pub1), Mono.from(pub2)))
				.thenRequest(1)
				.then(() -> {
					pub1.emit(1 ).complete();
					pub2.emit(2).complete();
				})
				.expectNext(1)
				.thenCancel()
				.verify(Duration.ofSeconds(1L));

		pub1.assertWasSubscribed();
		pub1.assertMaxRequested(1);
		pub1.assertCancelled();

		pub2.assertWasSubscribed();
		pub2.assertMaxRequested(1);
		pub2.assertWasCancelled();
	}

	@Test
	void singleArrayNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstWithValue(Mono.empty(), (Mono<Object>) null)
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	void arrayOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstWithValue(Mono.never(), null, Mono.never())
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	void singleIterableNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstWithValue(Arrays.asList((Mono<Object>) null))
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	void iterableOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstWithValue(Arrays.asList(Mono.never(),
				null,
				Mono.never()))
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	// See https://github.com/reactor/reactor-core/issues/2557
	@Test
	void protectAgainstSparseArray() {
		assertThatCode(() -> Mono.firstWithValue(Arrays.asList(Mono.just(1), Mono.empty())).block())
				.doesNotThrowAnyException();
	}

	@Test
	void pairWise() {
		Mono<Integer> firstValue = Mono.firstWithValue(Mono.just(1), Mono.just(2));
		Mono<Integer> orValue = Mono.firstWithValue(firstValue, Mono.just(3));

		assertThat(orValue).isInstanceOf(MonoFirstWithValue.class);
		assertThat(((MonoFirstWithValue<Integer>) orValue).array)
				.isNotNull()
				.hasSize(3);

		orValue.subscribeWith(AssertSubscriber.create())
				.assertValues(1)
				.assertComplete();
	}

	@Test
	void combineMoreThanOneAdditionalSource() {
		Mono<Integer> step1 = Mono.firstWithValue(Mono.just(1), Mono.just(2));
		Mono<Integer> step2 = Mono.firstWithValue(step1, Mono.just(3), Mono.just(4));

		assertThat(step2).isInstanceOfSatisfying(MonoFirstWithValue.class,
				mfv -> assertThat(mfv.array)
						.hasSize(4)
						.doesNotContainNull());
	}

	@Test
	void scanOperator() {
		@SuppressWarnings("unchecked")
		MonoFirstWithValue<Integer> test = new MonoFirstWithValue<>(Mono.just(1), Mono.just(2));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}