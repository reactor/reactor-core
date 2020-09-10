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

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import java.time.Duration;
import java.util.Arrays;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class MonoFirstValuedTest {

	@Test
	public void firstSourceEmittingValueIsChosen() {
		StepVerifier.withVirtualTime(() -> Mono.firstValued(
				Mono.just(1).delayElement(Duration.ofMillis(500L)),
				Mono.just(2).delayElement(Duration.ofMillis(1_000L))
		))
				.thenAwait(Duration.ofMillis(1_500L))
				.expectNext(1)
				.verifyComplete();
	}

	@Test
	public void firstSourceEmittingValueIsChosenOverErrorOrCompleteEmpty() {
		StepVerifier.withVirtualTime(() -> Mono.firstValued(
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
	public void onlyErrorOrCompleteEmptyEmitsError() {
		StepVerifier.withVirtualTime(() -> Mono.firstValued(
				Mono.error(new RuntimeException("Boom!")),
				Mono.empty()
		))
				.expectErrorSatisfies(e -> {
					assertThat(e).isInstanceOf(NoSuchElementException.class);
					assertThat(e.getMessage()).isEqualTo("All sources completed with error or without values");
					Throwable throwable = e.getSuppressed()[0];
					assertThat(throwable.getSuppressed()[0].getMessage()).isEqualTo("Boom!");
					assertThat(throwable.getSuppressed()[1].getMessage())
							.isEqualTo("source at index 1 completed empty");

				})
				.verify();
	}

	@Test
	public void firstNull() {
		assertThrows(NullPointerException.class, () -> Mono.firstValued(null, Mono.just(1), Mono.just(2)));
	}

	@Test
	public void arrayNull() {
		assertThrows(NullPointerException.class, () -> Mono.firstValued(Mono.just(1),(Mono<Integer>[]) null));
	}

	@Test
	public void iterableNull() {
		assertThrows(NullPointerException.class, () -> Mono.firstValued((Iterable<Mono<Integer>>) null));
	}

	@Test
	public void cancelIsPropagated() {
		TestPublisher<Integer> pub1 = TestPublisher.create();
		TestPublisher<Integer> pub2 = TestPublisher.create();

		StepVerifier.create(Mono.firstValued(Mono.from(pub1), Mono.from(pub2)))
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
	public void singleArrayNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstValued(Mono.empty(), (Mono<Object>) null)
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void arrayOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstValued(Mono.never(), null, Mono.never())
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void singleIterableNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstValued(Arrays.asList((Mono<Object>) null))
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void iterableOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstValued(Arrays.asList(Mono.never(),
				null,
				Mono.never()))
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void pairWise() {
		Mono<Integer> firstValue = Mono.firstValued(Mono.just(1), Mono.just(2));
		Mono<Integer> orValue = Mono.firstValued(firstValue, Mono.just(3));

		assertThat(orValue).isInstanceOf(MonoFirstValued.class);
		assertThat(((MonoFirstValued<Integer>) orValue).array)
				.isNotNull()
				.hasSize(3);

		orValue.subscribeWith(AssertSubscriber.create())
				.assertValues(1)
				.assertComplete();
	}

	@Test
	public void combineMoreThanOneAdditionalSource() {
		Mono<Integer> step1 = Mono.firstValued(Mono.just(1), Mono.just(2));
		Mono<Integer> step2 = Mono.firstValued(step1, Mono.just(3), Mono.just(4));

		assertThat(step2).isInstanceOfSatisfying(MonoFirstValued.class,
				mfv -> assertThat(mfv.array)
						.hasSize(4)
						.doesNotContainNull());
	}

	@Test
	public void scanOperator() {
		MonoFirstValued<Integer> test = new MonoFirstValued<>(Mono.just(1), Mono.just(2));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}