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
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import reactor.core.publisher.FluxBufferPredicate.Mode;
import reactor.test.StepVerifier;
import reactor.util.concurrent.QueueSupplier;

import static org.junit.Assert.assertFalse;

public class FluxWindowPredicateTest {

	@Test
	public void apiUntil() {
		StepVerifier.create(Flux.just("red", "green", "#", "orange", "blue", "#", "black", "white")
		                        .windowUntil(color -> color.equals("#"))
		                        .flatMap(Flux::materialize)
		                        .map(s -> s.isOnComplete() ? "WINDOW CLOSED" : s.get()))
		            .expectNext("red", "green", "#", "WINDOW CLOSED")
		            .expectNext("orange", "blue", "#", "WINDOW CLOSED")
		            .expectNext("black", "white", "WINDOW CLOSED")
	                .verifyComplete();
	}

	@Test
	public void apiUntilCutAfter() {
		StepVerifier.create(Flux.just("red", "green", "#", "orange", "blue", "#", "black", "white")
		                        .windowUntil(color -> color.equals("#"), false)
		                        .flatMap(Flux::materialize)
		                        .map(s -> s.isOnComplete() ? "WINDOW CLOSED" : s.get()))
		            .expectNext("red", "green", "#", "WINDOW CLOSED")
		            .expectNext("orange", "blue", "#", "WINDOW CLOSED")
		            .expectNext("black", "white", "WINDOW CLOSED")
	                .verifyComplete();
	}

	@Test
	public void apiUntilCutBefore() {
		StepVerifier.create(Flux.just("red", "green", "#", "orange", "blue", "#", "black", "white")
		                        .windowUntil(color -> color.equals("#"), true)
		                        .flatMap(Flux::materialize)
		                        .map(s -> s.isOnComplete() ? "WINDOW CLOSED" : s.get()))
		            .expectNext("red", "green", "WINDOW CLOSED", "#")
		            .expectNext("orange", "blue", "WINDOW CLOSED", "#")
		            .expectNext("black", "white", "WINDOW CLOSED")
	                .verifyComplete();
	}

	@Test
	public void apiWhile() {
		StepVerifier.create(Flux.just("red", "green", "#", "orange", "blue", "#", "black", "white")
		                        .windowWhile(color -> !color.equals("#"))
		                        .flatMap(Flux::materialize)
		                        .map(s -> s.isOnComplete() ? "WINDOW CLOSED" : s.get()))
		            .expectNext("red", "green", "WINDOW CLOSED")
		            .expectNext("orange", "blue", "WINDOW CLOSED")
		            .expectNext("black", "white", "WINDOW CLOSED")
	                .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void normalUntil() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntil = new FluxWindowPredicate<>(sp1, i -> i % 3 == 0,
				QueueSupplier.small(), Mode.UNTIL);

		StepVerifier.create(windowUntil.flatMap(Flux::materialize))
		            .expectSubscription()
		            .then(() -> sp1.onNext(1))
		            .expectNext(Signal.next(1))
				    .then(() -> sp1.onNext(2))
		            .expectNext(Signal.next(2))
				    .then(() -> sp1.onNext(3))
		            .expectNext(Signal.next(3), Signal.complete())
				    .then(() -> sp1.onNext(4))
				    .expectNext(Signal.next(4))
				    .then(() -> sp1.onNext(5))
				    .expectNext(Signal.next(5))
				    .then(() -> sp1.onNext(6))
				    .expectNext(Signal.next(6), Signal.complete())
				    .then(() -> sp1.onNext(7))
				    .expectNext(Signal.next(7))
				    .then(() -> sp1.onNext(8))
				    .expectNext(Signal.next(8))
				    .then(sp1::onComplete)
		            .expectNext(Signal.complete())
				    .verifyComplete();

		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void onCompletionBeforeLastBoundaryWindowEmitted() {
		Flux<Integer> source = Flux.just(1, 2);

		FluxWindowPredicate<Integer> windowUntil =
				new FluxWindowPredicate<>(source, i -> i >= 3, QueueSupplier.small(), Mode.UNTIL);

		FluxWindowPredicate<Integer> windowUntilOther =
				new FluxWindowPredicate<>(source, i -> i >= 3, QueueSupplier.small(), Mode.UNTIL_CUT_BEFORE);

		FluxWindowPredicate<Integer> windowWhile =
				new FluxWindowPredicate<>(source, i -> i < 3, QueueSupplier.small(), Mode.WHILE);

		StepVerifier.create(windowUntil.flatMap(Flux::collectList))
				.expectNext(Arrays.asList(1, 2))
				.expectComplete()
				.verify();

		StepVerifier.create(windowUntilOther.flatMap(Flux::collectList))
		            .expectNext(Arrays.asList(1, 2))
		            .expectComplete()
		            .verify();

		StepVerifier.create(windowWhile.flatMap(Flux::collectList))
		            .expectNext(Arrays.asList(1, 2))
		            .expectComplete()
		            .verify();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void mainErrorUntilIsPropagatedToBothWindowAndMain() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntil = new FluxWindowPredicate<>(
				sp1, i -> i % 3 == 0, QueueSupplier.small(), Mode.UNTIL);

		StepVerifier.create(windowUntil.flatMap(Flux::materialize))
		            .expectSubscription()
		            .then(() -> sp1.onNext(1))
		            .expectNext(Signal.next(1))
		            .then(() -> sp1.onNext(2))
		            .expectNext(Signal.next(2))
		            .then(() -> sp1.onNext(3))
		            .expectNext(Signal.next(3), Signal.complete())
		            .then(() -> sp1.onNext(4))
		            .expectNext(Signal.next(4))
		            .then(() -> sp1.onError(new RuntimeException("forced failure")))
		            //this is the error in the window:
		            .expectNextMatches(s -> s.isOnError() && s.getThrowable().getMessage().equals("forced failure"))
		            //this is the error in the main:
		            .expectErrorMessage("forced failure")
		            .verify();
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void predicateErrorUntil() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntil = new FluxWindowPredicate<>(
				sp1,
				i -> {
					if (i == 5) throw new IllegalStateException("predicate failure");
					return i % 3 == 0;
				}, QueueSupplier.small(), Mode.UNTIL);

		StepVerifier.create(windowUntil.flatMap(Flux::collectList))
					.expectSubscription()
					.then(() -> sp1.onNext(1))
					.then(() -> sp1.onNext(2))
					.then(() -> sp1.onNext(3))
					.expectNext(Arrays.asList(1, 2, 3))
					.then(() -> sp1.onNext(4))
					.then(() -> sp1.onNext(5))
					.expectErrorMessage("predicate failure")
					.verify();
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void normalUntilOther() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntilOther = new FluxWindowPredicate<>(sp1, i -> i % 3 == 0,
				QueueSupplier.small(), Mode.UNTIL_CUT_BEFORE);

		StepVerifier.create(windowUntilOther.flatMap(Flux::materialize))
				.expectSubscription()
				    .then(() -> sp1.onNext(1))
				    .expectNext(Signal.next(1))
				    .then(() -> sp1.onNext(2))
				    .expectNext(Signal.next(2))
				    .then(() -> sp1.onNext(3))
				    .expectNext(Signal.complete(), Signal.next(3))
				    .then(() -> sp1.onNext(4))
				    .expectNext(Signal.next(4))
				    .then(() -> sp1.onNext(5))
				    .expectNext(Signal.next(5))
				    .then(() -> sp1.onNext(6))
				    .expectNext(Signal.complete(), Signal.next(6))
				    .then(() -> sp1.onNext(7))
				    .expectNext(Signal.next(7))
				    .then(() -> sp1.onNext(8))
				    .expectNext(Signal.next(8))
				    .then(sp1::onComplete)
				    .expectNext(Signal.complete())
				    .verifyComplete();
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void mainErrorUntilOtherIsPropagatedToBothWindowAndMain() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntilOther =
				new FluxWindowPredicate<>(sp1, i -> i % 3 == 0, QueueSupplier.small(),
						Mode.UNTIL_CUT_BEFORE);

		StepVerifier.create(windowUntilOther.flatMap(Flux::materialize))
		            .expectSubscription()
		            .then(() -> sp1.onNext(1))
		            .expectNext(Signal.next(1))
		            .then(() -> sp1.onNext(2))
		            .expectNext(Signal.next(2))
		            .then(() -> sp1.onNext(3))
		            .expectNext(Signal.complete())
		            .expectNext(Signal.next(3))
		            .then(() -> sp1.onNext(4))
		            .expectNext(Signal.next(4))
		            .then(() -> sp1.onError(new RuntimeException("forced failure")))
		            //this is the error in the window:
		            .expectNextMatches(signal -> signal.isOnError() && signal.getThrowable().getMessage().equals("forced failure"))
		            //this is the error in the main:
		            .expectErrorMessage("forced failure")
		            .verify();
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void predicateErrorUntilOther() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntilOther =
				new FluxWindowPredicate<>(sp1,
				i -> {
					if (i == 5) throw new IllegalStateException("predicate failure");
					return i % 3 == 0;
				}, QueueSupplier.small(), Mode.UNTIL_CUT_BEFORE);

		StepVerifier.create(windowUntilOther.flatMap(Flux::collectList))
					.expectSubscription()
					.then(() -> sp1.onNext(1))
					.then(() -> sp1.onNext(2))
					.then(() -> sp1.onNext(3))
					.expectNext(Arrays.asList(1, 2))
					.then(() -> sp1.onNext(4))
					.then(() -> sp1.onNext(5))
					.expectErrorMessage("predicate failure")
					.verify();
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void normalWhile() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowWhile = new FluxWindowPredicate<>(
				sp1, i -> i % 3 != 0, QueueSupplier.small(),
				Mode.WHILE);

		StepVerifier.create(windowWhile.flatMap(Flux::materialize))
		            .expectSubscription()
		            .then(() -> sp1.onNext(1))
		            .expectNext(Signal.next(1))
		            .then(() -> sp1.onNext(2))
		            .expectNext(Signal.next(2))
		            .then(() -> sp1.onNext(3))
		            .expectNext(Signal.complete())
		            .then(() -> sp1.onNext(4))
		            .expectNext(Signal.next(4))
		            .then(() -> sp1.onNext(5))
		            .expectNext(Signal.next(5))
		            .then(() -> sp1.onNext(6))
		            .expectNext(Signal.complete())
		            .then(() -> sp1.onNext(7))
		            .expectNext(Signal.next(7))
		            .then(() -> sp1.onNext(8))
		            .expectNext(Signal.next(8))
		            .then(sp1::onComplete)
		            .expectNext(Signal.complete())
		            .verifyComplete();
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void normalWhileDoesntInitiallyMatch() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowWhile = new FluxWindowPredicate<>(
				sp1, i -> i % 3 == 0, QueueSupplier.small(), Mode.WHILE);

		StepVerifier.create(windowWhile.flatMap(Flux::collectList))
				.expectSubscription()
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(1))
				.then(() -> sp1.onNext(2))
				.then(() -> sp1.onNext(3))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(4))
				.expectNext(Arrays.asList(3)) //emission of 4 completes the list
				.then(() -> sp1.onNext(5))
				.then(() -> sp1.onNext(6))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(7)) // emission of 7 completes the list
				.expectNext(Arrays.asList(6))
				.then(() -> sp1.onNext(8))
				.then(() -> sp1.onNext(9))
				.expectNoEvent(Duration.ofMillis(10))
				.then(sp1::onComplete) // completion triggers completion of the window / list
			    .expectNext(Collections.singletonList(9))
				.expectComplete()
				.verify(Duration.ofSeconds(1));
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void normalWhileDoesntMatch() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowWhile = new FluxWindowPredicate<>(
				sp1, i -> i > 4, QueueSupplier.small(), Mode.WHILE);

		StepVerifier.create(windowWhile.flatMap(Flux::collectList).log())
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(10))
		            .then(() -> sp1.onNext(1))
		            .then(() -> sp1.onNext(2))
		            .then(() -> sp1.onNext(3))
		            .then(() -> sp1.onNext(4))
		            .expectNoEvent(Duration.ofMillis(10))
		            .then(() -> sp1.onNext(1))
		            .then(() -> sp1.onNext(2))
		            .then(() -> sp1.onNext(3))
		            .then(() -> sp1.onNext(4))
		            .expectNoEvent(Duration.ofMillis(10))
		            .then(sp1::onComplete)
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void mainErrorWhile() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowWhile = new FluxWindowPredicate<>(
				sp1, i -> i % 3 == 0, QueueSupplier.small(), Mode.WHILE);

		StepVerifier.create(windowWhile.flatMap(Flux::collectList))
		            .expectSubscription()
		            .then(() -> sp1.onNext(1))
		            .then(() -> sp1.onNext(2))
		            .then(() -> sp1.onNext(3))
		            .then(() -> sp1.onNext(4))
		            .expectNext(Arrays.asList(3))
		            .then(() -> sp1.onError(new RuntimeException("forced failure")))
		            .expectErrorMessage("forced failure")
		            .verify(Duration.ofMillis(100));
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	public void whileStartingSeveralSeparatorsDontCreateWindow() {
		StepVerifier.create(Flux.just("#")
		                        .repeat(10)
		                        .concatWith(Flux.just("other", "value"))
		                        .windowWhile(s -> !s.equals("#"))
		                        .flatMap(Flux::count)
		)
		            .expectNext(2L)
		            .verifyComplete();
	}

	@Test
	public void whileOnlySeparatorsGivesEmptySequence() {
		StepVerifier.create(Flux.just("#")
		                        .repeat(10)
		                        .windowWhile(s -> !s.equals("#")))
	                .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void predicateErrorWhile() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowWhile = new FluxWindowPredicate<>(
				sp1,
				i -> {
					if (i == 3) return true;
					if (i == 5) throw new IllegalStateException("predicate failure");
					return false;
				}, QueueSupplier.small(), Mode.WHILE);

		Flux<Signal<Integer>> flux = windowWhile.flatMap(Flux::materialize)
		                                        .log();

		StepVerifier.create(flux)
					.expectSubscription()
					.then(() -> sp1.onNext(1)) //ignored, window not emitted
					.then(() -> sp1.onNext(2)) //ignored, window not emitted
					.then(() -> sp1.onNext(3)) //window opens
					.expectNext(Signal.next(3))
					.then(() -> sp1.onNext(4)) //ignored, window closes
					.expectNext(Signal.complete())
					.then(() -> sp1.onNext(5)) //fails
					.expectErrorMessage("predicate failure")
					.verify(Duration.ofMillis(100));
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void queueSupplierThrows() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntil = new FluxWindowPredicate<>(
				sp1, i -> i % 3 == 0,
				() -> { throw new RuntimeException("supplier failure"); },
				Mode.UNTIL);

		assertFalse("sp1 has subscribers?", sp1.hasDownstreams());

		StepVerifier.create(windowUntil)
		            .then(() -> sp1.onNext(1))
		            .expectErrorMessage("supplier failure")
		            .verify();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void queueSupplierThrowsLater() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		int count[] = {1};
		FluxWindowPredicate<Integer> windowUntil = new FluxWindowPredicate<>(
				sp1, i -> i % 3 == 0,
				() -> {
					if (count[0]-- > 0) {
						return QueueSupplier.<Integer>small().get();
					}
					throw new RuntimeException("supplier failure");
				},
				Mode.UNTIL);

		assertFalse("sp1 has subscribers?", sp1.hasDownstreams());

		StepVerifier.create(windowUntil.flatMap(Flux::collectList))
		            .then(() -> sp1.onNext(1))
		            .then(() -> sp1.onNext(2))
		            .expectNoEvent(Duration.ofMillis(10))
		            .then(() -> sp1.onNext(3))
		            .expectNext(Arrays.asList(1, 2, 3))
		            .then(() -> sp1.onNext(4))
		            .expectErrorMessage("supplier failure")
		            .verify();
	}

	@Test
	public void queueSupplierReturnsNull() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntil = new FluxWindowPredicate<>(
				sp1, i -> i % 3 == 0,
				() -> null,
				Mode.UNTIL);

		assertFalse("sp1 has subscribers?", sp1.hasDownstreams());

		StepVerifier.create(windowUntil)
		            .then(() -> sp1.onNext(1))
		            .expectErrorMatches(t -> t instanceof NullPointerException &&
		                "The processorQueueSupplier returned a null queue".equals(t.getMessage()))
		            .verify();
	}

}