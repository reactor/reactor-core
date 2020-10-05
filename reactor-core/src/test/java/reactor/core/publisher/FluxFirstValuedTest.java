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
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FluxFirstValuedTest {

	@Test
	public void firstSourceEmittingValueIsChosen() {
		StepVerifier.withVirtualTime(() -> Flux.firstValued(
				Flux.just(1, 2, 3).delayElements(Duration.ofMillis(500L)),
				Flux.just(4, 5, 6).delayElements(Duration.ofMillis(1_000L))
		))
				.thenAwait(Duration.ofMillis(1_500L))
				.expectNext(1, 2, 3)
				.verifyComplete();
	}

	@Test
	public void firstSourceEmittingValueIsChosenOverErrorOrCompleteEmpty() {
		StepVerifier.withVirtualTime(() -> Flux.firstValued(
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
	public void onlyErrorOrCompleteEmptyEmitsError() {
		StepVerifier.withVirtualTime(() -> Flux.firstValued(
				Flux.error(new RuntimeException("Boom!")),
				Flux.empty()
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
		assertThrows(NullPointerException.class, () -> Flux.firstValued(null, Flux.just(1), Flux.just(2)));
	}

	@Test
	public void arrayNull() {
		assertThrows(NullPointerException.class, () -> Flux.firstValued(Flux.just(1), (Publisher<Integer>[]) null));
	}

	@Test
	public void iterableNull() {
		assertThrows(NullPointerException.class, () -> Flux.firstValued((Iterable<Publisher<Integer>>) null));
	}

	@Test
	public void requestAndCancelArePropagated() {
		TestPublisher<Integer> pub1 = TestPublisher.create();
		TestPublisher<Integer> pub2 = TestPublisher.create();

		StepVerifier.create(Flux.firstValued(pub1, pub2))
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
	public void singleNullSourceInVararg() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstValued(Mono.empty(), (Publisher<Object>) null)
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void arrayOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstValued(Flux.never(), null, Flux.never())
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void singleIterableNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstValued(Arrays.asList((Publisher<Object>) null))
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void iterableOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstValued(Arrays.asList(Flux.never(),
				(Publisher<Object>) null,
				Flux.never()))
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void pairWise() {
		Flux<Integer> firstValues = Flux.firstValued(Flux.just(1), Mono.just(2));
		Flux<Integer> orValues = Flux.firstValued(firstValues, Mono.just(3));

		assertThat(orValues).isInstanceOf(FluxFirstValued.class);
		assertThat(((FluxFirstValued<Integer>) orValues).array)
				.isNotNull()
				.hasSize(3);

		orValues.subscribeWith(AssertSubscriber.create())
				.assertValues(1)
				.assertComplete();
	}

	@Test
	public void combineMoreThanOneAdditionalSource() {
		Flux<Integer> step1 = Flux.firstValued(Flux.just(1), Mono.just(2));
		Flux<Integer> step2 = Flux.firstValued(step1, Flux.just(3), Mono.just(4));

		assertThat(step2).isInstanceOfSatisfying(FluxFirstValued.class,
				ffv -> assertThat(ffv.array)
						.hasSize(4)
						.doesNotContainNull());
	}

	@Test
	public void scanOperator() {
		@SuppressWarnings("unchecked") FluxFirstValued<Integer>
				test = new FluxFirstValued<>(Flux.range(1, 10), Flux.range(11, 10));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {
		}, null, null);
		FluxFirstValued.RaceValuesCoordinator<String> parent = new FluxFirstValued.RaceValuesCoordinator<>(1);
		FluxFirstValued.FirstValuesEmittingSubscriber<String> test = new FluxFirstValued.FirstValuesEmittingSubscriber<>(actual, parent, 1);
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
	public void scanRaceCoordinator() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {
		}, null, null);
		FluxFirstValued.RaceValuesCoordinator<String> parent = new FluxFirstValued.RaceValuesCoordinator<>(1);
		FluxFirstValued.FirstValuesEmittingSubscriber<String> test = new FluxFirstValued.FirstValuesEmittingSubscriber<>(actual, parent, 1);
		Subscription sub = Operators.emptySubscription();
		test.onSubscribe(sub);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(parent.scan(Scannable.Attr.CANCELLED)).isFalse();
		parent.cancelled = true;
		assertThat(parent.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

}
