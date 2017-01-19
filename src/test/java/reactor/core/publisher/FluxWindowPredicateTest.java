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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.FluxBufferPredicate.Mode;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.concurrent.QueueSupplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

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
				    .then(() -> sp1.onNext(2))
		            .expectNext(Signal.next(1), Signal.next(2))
				    .then(() -> sp1.onNext(3))
		            .expectNext(Signal.next(3), Signal.complete())
				    .then(() -> sp1.onNext(4))
				    .then(() -> sp1.onNext(5))
				    .expectNext(Signal.next(4), Signal.next(5))
				    .then(() -> sp1.onNext(6))
				    .expectNext(Signal.next(6), Signal.complete())
				    .then(() -> sp1.onNext(7))
				    .then(() -> sp1.onNext(8))
				    .expectNext(Signal.next(7), Signal.next(8))
				    .then(sp1::onComplete)
		            .expectNext(Signal.complete())
				    .verifyComplete();

		assertFalse(sp1.hasDownstreams());
	}

//	@Test
//	@SuppressWarnings("unchecked")
//	public void onCompletionBeforeLastBoundaryBufferEmitted() {
//		Flux<Integer> source = Flux.just(1, 2);
//
//		FluxBufferPredicate<Integer, List<Integer>> bufferUntil =
//				new FluxBufferPredicate<>(source, i -> i >= 3, Flux.listSupplier(),
//						FluxBufferPredicate.Mode.UNTIL);
//
//		FluxBufferPredicate<Integer, List<Integer>> bufferUntilOther =
//				new FluxBufferPredicate<>(source, i -> i >= 3, Flux.listSupplier(),
//						FluxBufferPredicate.Mode.UNTIL_CUT_BEFORE);
//
//		FluxBufferPredicate<Integer, List<Integer>> bufferWhile =
//				new FluxBufferPredicate<>(source, i -> i < 3, Flux.listSupplier(),
//						FluxBufferPredicate.Mode.WHILE);
//
//		StepVerifier.create(bufferUntil)
//				.expectNext(Arrays.asList(1, 2))
//				.expectComplete()
//				.verify();
//
//		StepVerifier.create(bufferUntilOther)
//		            .expectNext(Arrays.asList(1, 2))
//		            .expectComplete()
//		            .verify();
//
//		StepVerifier.create(bufferWhile)
//		            .expectNext(Arrays.asList(1, 2))
//		            .expectComplete()
//		            .verify();
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void mainErrorUntil() {
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		FluxBufferPredicate<Integer, List<Integer>> bufferUntil = new FluxBufferPredicate<>(
//				sp1, i -> i % 3 == 0, Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL);
//
//		StepVerifier.create(bufferUntil)
//		            .expectSubscription()
//		            .then(() -> sp1.onNext(1))
//		            .then(() -> sp1.onNext(2))
//		            .then(() -> sp1.onNext(3))
//		            .expectNext(Arrays.asList(1, 2, 3))
//		            .then(() -> sp1.onNext(4))
//		            .then(() -> sp1.onError(new RuntimeException("forced failure")))
//		            .expectErrorMessage("forced failure")
//		            .verify();
//		assertFalse(sp1.hasDownstreams());
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void predicateErrorUntil() {
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		FluxBufferPredicate<Integer, List<Integer>> bufferUntil = new FluxBufferPredicate<>(
//				sp1,
//				i -> {
//					if (i == 5) throw new IllegalStateException("predicate failure");
//					return i % 3 == 0;
//				}, Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL);
//
//		StepVerifier.create(bufferUntil)
//					.expectSubscription()
//					.then(() -> sp1.onNext(1))
//					.then(() -> sp1.onNext(2))
//					.then(() -> sp1.onNext(3))
//					.expectNext(Arrays.asList(1, 2, 3))
//					.then(() -> sp1.onNext(4))
//					.then(() -> sp1.onNext(5))
//					.expectErrorMessage("predicate failure")
//					.verify();
//		assertFalse(sp1.hasDownstreams());
//	}

	@Test
	@SuppressWarnings("unchecked")
	public void normalUntilOther() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntilOther = new FluxWindowPredicate<>(sp1, i -> i % 3 == 0,
				QueueSupplier.small(), Mode.UNTIL_CUT_BEFORE);

		StepVerifier.create(windowUntilOther.flatMap(Flux::materialize))
				.expectSubscription()
				    .then(() -> sp1.onNext(1))
				    .then(() -> sp1.onNext(2))
				    .expectNext(Signal.next(1), Signal.next(2))
				    .then(() -> sp1.onNext(3))
				    .expectNext(Signal.complete(), Signal.next(3))
				    .then(() -> sp1.onNext(4))
				    .then(() -> sp1.onNext(5))
				    .expectNext(Signal.next(4), Signal.next(5))
				    .then(() -> sp1.onNext(6))
				    .expectNext(Signal.complete(), Signal.next(6))
				    .then(() -> sp1.onNext(7))
				    .then(() -> sp1.onNext(8))
				    .expectNext(Signal.next(7), Signal.next(8))
				    .then(sp1::onComplete)
				    .expectNext(Signal.complete())
				    .verifyComplete();
		assertFalse(sp1.hasDownstreams());
	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void mainErrorUntilOther() {
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		FluxBufferPredicate<Integer, List<Integer>> bufferUntilOther =
//				new FluxBufferPredicate<>(sp1, i -> i % 3 == 0, Flux.listSupplier(),
//						FluxBufferPredicate.Mode.UNTIL_CUT_BEFORE);
//
//		StepVerifier.create(bufferUntilOther)
//		            .expectSubscription()
//		            .then(() -> sp1.onNext(1))
//		            .then(() -> sp1.onNext(2))
//		            .then(() -> sp1.onNext(3))
//		            .expectNext(Arrays.asList(1, 2))
//		            .then(() -> sp1.onNext(4))
//		            .then(() -> sp1.onError(new RuntimeException("forced failure")))
//		            .expectErrorMessage("forced failure")
//		            .verify();
//		assertFalse(sp1.hasDownstreams());
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void predicateErrorUntilOther() {
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		FluxBufferPredicate<Integer, List<Integer>> bufferUntilOther =
//				new FluxBufferPredicate<>(sp1,
//				i -> {
//					if (i == 5) throw new IllegalStateException("predicate failure");
//					return i % 3 == 0;
//				}, Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL_CUT_BEFORE);
//
//		StepVerifier.create(bufferUntilOther)
//					.expectSubscription()
//					.then(() -> sp1.onNext(1))
//					.then(() -> sp1.onNext(2))
//					.then(() -> sp1.onNext(3))
//					.expectNext(Arrays.asList(1, 2))
//					.then(() -> sp1.onNext(4))
//					.then(() -> sp1.onNext(5))
//					.expectErrorMessage("predicate failure")
//					.verify();
//		assertFalse(sp1.hasDownstreams());
//	}
//
	@Test
	@SuppressWarnings("unchecked")
	public void normalWhile() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowWhile = new FluxWindowPredicate<>(
				sp1, i -> i % 3 != 0, QueueSupplier.small(),
				FluxBufferPredicate.Mode.WHILE);

		StepVerifier.create(windowWhile.flatMap(Flux::materialize))
		            .expectSubscription()
		            .then(() -> sp1.onNext(1))
		            .then(() -> sp1.onNext(2))
		            .expectNext(Signal.next(1), Signal.next(2))
		            .then(() -> sp1.onNext(3))
		            .expectNext(Signal.complete())
		            .then(() -> sp1.onNext(4))
		            .then(() -> sp1.onNext(5))
		            .expectNext(Signal.next(4), Signal.next(5))
		            .then(() -> sp1.onNext(6))
		            .expectNext(Signal.complete())
		            .then(() -> sp1.onNext(7))
		            .then(() -> sp1.onNext(8))
		            .expectNext(Signal.next(7), Signal.next(8))
		            .then(sp1::onComplete)
		            .expectNext(Signal.complete())
		            .verifyComplete();
		assertFalse(sp1.hasDownstreams());
	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void normalWhileDoesntInitiallyMatch() {
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		FluxBufferPredicate<Integer, List<Integer>> bufferWhile = new FluxBufferPredicate<>(
//				sp1, i -> i % 3 == 0, Flux.listSupplier(), FluxBufferPredicate.Mode.WHILE);
//
//		StepVerifier.create(bufferWhile)
//				.expectSubscription()
//				.expectNoEvent(Duration.ofMillis(10))
//				.then(() -> sp1.onNext(1))
//				.then(() -> sp1.onNext(2))
//				.then(() -> sp1.onNext(3))
//				.expectNoEvent(Duration.ofMillis(10))
//				.then(() -> sp1.onNext(4))
//				.expectNext(Arrays.asList(3)) //emission of 4 triggers the buffer emit
//				.then(() -> sp1.onNext(5))
//				.then(() -> sp1.onNext(6))
//				.expectNoEvent(Duration.ofMillis(10))
//				.then(() -> sp1.onNext(7)) // emission of 7 triggers the buffer emit
//				.expectNext(Arrays.asList(6))
//				.then(() -> sp1.onNext(8))
//				.then(() -> sp1.onNext(9))
//				.expectNoEvent(Duration.ofMillis(10))
//				.then(sp1::onComplete) // completion triggers the buffer emit
//			    .expectNext(Collections.singletonList(9))
//				.expectComplete()
//				.verify(Duration.ofSeconds(1));
//		assertFalse(sp1.hasDownstreams());
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void normalWhileDoesntMatch() {
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		FluxBufferPredicate<Integer, List<Integer>> bufferWhile = new FluxBufferPredicate<>(
//				sp1, i -> i > 4, Flux.listSupplier(), FluxBufferPredicate.Mode.WHILE);
//
//		StepVerifier.create(bufferWhile)
//		            .expectSubscription()
//		            .expectNoEvent(Duration.ofMillis(10))
//		            .then(() -> sp1.onNext(1))
//		            .then(() -> sp1.onNext(2))
//		            .then(() -> sp1.onNext(3))
//		            .then(() -> sp1.onNext(4))
//		            .expectNoEvent(Duration.ofMillis(10))
//		            .then(() -> sp1.onNext(1))
//		            .then(() -> sp1.onNext(2))
//		            .then(() -> sp1.onNext(3))
//		            .then(() -> sp1.onNext(4))
//		            .expectNoEvent(Duration.ofMillis(10))
//		            .then(sp1::onComplete)
//		            .expectComplete()
//		            .verify(Duration.ofSeconds(1));
//		assertFalse(sp1.hasDownstreams());
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void mainErrorWhile() {
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		FluxBufferPredicate<Integer, List<Integer>> bufferWhile = new FluxBufferPredicate<>(
//				sp1, i -> i % 3 == 0, Flux.listSupplier(), FluxBufferPredicate.Mode.WHILE);
//
//		StepVerifier.create(bufferWhile)
//		            .expectSubscription()
//		            .then(() -> sp1.onNext(1))
//		            .then(() -> sp1.onNext(2))
//		            .then(() -> sp1.onNext(3))
//		            .then(() -> sp1.onNext(4))
//		            .expectNext(Arrays.asList(3))
//		            .then(() -> sp1.onError(new RuntimeException("forced failure")))
//		            .expectErrorMessage("forced failure")
//		            .verify(Duration.ofMillis(100));
//		assertFalse(sp1.hasDownstreams());
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void predicateErrorWhile() {
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		FluxBufferPredicate<Integer, List<Integer>> bufferWhile = new FluxBufferPredicate<>(
//				sp1,
//				i -> {
//					if (i == 3) return true;
//					if (i == 5) throw new IllegalStateException("predicate failure");
//					return false;
//				}, Flux.listSupplier(), FluxBufferPredicate.Mode.WHILE);
//
//		StepVerifier.create(bufferWhile)
//					.expectSubscription()
//					.then(() -> sp1.onNext(1)) //ignored
//					.then(() -> sp1.onNext(2)) //ignored
//					.then(() -> sp1.onNext(3)) //buffered
//					.then(() -> sp1.onNext(4)) //ignored, emits buffer
//					.expectNext(Arrays.asList(3))
//					.then(() -> sp1.onNext(5)) //fails
//					.expectErrorMessage("predicate failure")
//					.verify(Duration.ofMillis(100));
//		assertFalse(sp1.hasDownstreams());
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void bufferSupplierThrows() {
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		FluxBufferPredicate<Integer, List<Integer>> bufferUntil = new FluxBufferPredicate<>(
//				sp1, i -> i % 3 == 0,
//				() -> { throw new RuntimeException("supplier failure"); },
//				FluxBufferPredicate.Mode.UNTIL);
//
//		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
//
//		StepVerifier.create(bufferUntil)
//		            .expectErrorMessage("supplier failure")
//		            .verify();
//	}
//
//	@Test
//	public void bufferSupplierThrowsLater() {
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		int count[] = {1};
//		FluxBufferPredicate<Integer, List<Integer>> bufferUntil = new FluxBufferPredicate<>(
//				sp1, i -> i % 3 == 0,
//				() -> {
//					if (count[0]-- > 0) {
//						return new ArrayList<>();
//					}
//					throw new RuntimeException("supplier failure");
//				},
//				FluxBufferPredicate.Mode.UNTIL);
//
//		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
//
//		StepVerifier.create(bufferUntil)
//		            .then(() -> sp1.onNext(1))
//		            .then(() -> sp1.onNext(2))
//		            .expectNoEvent(Duration.ofMillis(10))
//		            .then(() -> sp1.onNext(3))
//		            .expectErrorMessage("supplier failure")
//		            .verify();
//	}
//
//	@Test
//	public void bufferSupplierReturnsNull() {
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		FluxBufferPredicate<Integer, List<Integer>> bufferUntil = new FluxBufferPredicate<>(
//				sp1, i -> i % 3 == 0,
//				() -> null,
//				FluxBufferPredicate.Mode.UNTIL);
//
//		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
//
//		StepVerifier.create(bufferUntil)
//		            .then(() -> sp1.onNext(1))
//		            .expectErrorMatches(t -> t instanceof NullPointerException &&
//		                "The bufferSupplier returned a null initial buffer".equals(t.getMessage()))
//		            .verify();
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void multipleTriggersOfEmptyBufferKeepInitialBuffer() {
//		//this is best demonstrated with bufferWhile:
//		DirectProcessor<Integer> sp1 = DirectProcessor.create();
//		LongAdder bufferCount = new LongAdder();
//		Supplier<List<Integer>> bufferSupplier = () -> {
//			bufferCount.increment();
//			return new ArrayList<>();
//		};
//
//		FluxBufferPredicate<Integer, List<Integer>> bufferWhile = new
//				FluxBufferPredicate<>(
//				sp1, i -> i >= 10,
//				bufferSupplier,
//				FluxBufferPredicate.Mode.WHILE);
//
//		StepVerifier.create(bufferWhile)
//		            .then(() -> assertThat(bufferCount.intValue(), is(1)))
//		            .then(() -> sp1.onNext(1))
//		            .then(() -> sp1.onNext(2))
//		            .then(() -> sp1.onNext(3))
//		            .then(() -> assertThat(bufferCount.intValue(), is(1)))
//		            .expectNoEvent(Duration.ofMillis(10))
//		            .then(() -> sp1.onNext(10))
//		            .then(() -> sp1.onNext(11))
//		            .then(sp1::onComplete)
//		            .expectNext(Arrays.asList(10, 11))
//		            .then(() -> assertThat(bufferCount.intValue(), is(1)))
//		            .expectComplete()
//		            .verify();
//
//		assertThat(bufferCount.intValue(), is(1));
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void requestBounded() {
//		LongAdder requestCallCount = new LongAdder();
//		LongAdder totalRequest = new LongAdder();
//		Flux<Integer> source = Flux.range(1, 10).hide()
//		                           .doOnRequest(r -> requestCallCount.increment())
//		                           .doOnRequest(totalRequest::add);
//
//		StepVerifier.withVirtualTime( //start with a request for 1 buffer
//				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
//				Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL), 1)
//		            .expectSubscription()
//		            .expectNext(Arrays.asList(1, 2, 3))
//		            .expectNoEvent(Duration.ofSeconds(1))
//		            .thenRequest(2)
//		            .expectNext(Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9))
//		            .expectNoEvent(Duration.ofSeconds(1))
//		            .thenRequest(3)
//		            .expectNext(Collections.singletonList(10))
//		            .expectComplete()
//		            .verify();
//
//		assertThat(requestCallCount.intValue(), is(11)); //10 elements then the completion
//		assertThat(totalRequest.longValue(), is(11L)); //ignores the main requests
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void requestBoundedSeveralInitial() {
//		LongAdder requestCallCount = new LongAdder();
//		LongAdder totalRequest = new LongAdder();
//		Flux<Integer> source = Flux.range(1, 10).hide()
//		                           .doOnRequest(r -> requestCallCount.increment())
//		                           .doOnRequest(totalRequest::add);
//
//		StepVerifier.withVirtualTime( //start with a request for 2 buffers
//				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
//				Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL), 2)
//		            .expectSubscription()
//		            .expectNext(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6))
//		            .expectNoEvent(Duration.ofSeconds(1))
//		            .thenRequest(2)
//					.expectNext(Arrays.asList(7, 8, 9))
//		            .expectNext(Collections.singletonList(10))
//		            .expectComplete()
//		            .verify(Duration.ofSeconds(1));
//
//		assertThat(requestCallCount.intValue(), is(11)); //10 elements then the completion
//		assertThat(totalRequest.longValue(), is(11L)); //ignores the main requests
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void requestRemainingBuffersAfterBufferEmission() {
//		LongAdder requestCallCount = new LongAdder();
//		LongAdder totalRequest = new LongAdder();
//		Flux<Integer> source = Flux.range(1, 10).hide()
//		                           .doOnRequest(r -> requestCallCount.increment())
//		                           .doOnRequest(totalRequest::add);
//
//		StepVerifier.withVirtualTime( //start with a request for 3 buffers
//				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
//						Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL), 3)
//		            .expectSubscription()
//		            .expectNext(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9))
//		            .expectNoEvent(Duration.ofSeconds(1))
//		            .thenRequest(1)
//		            .expectNext(Collections.singletonList(10))
//		            .expectComplete()
//		            .verify(Duration.ofSeconds(1));
//
//		assertThat(requestCallCount.intValue(), is(11)); //10 elements then the completion
//		assertThat(totalRequest.longValue(), is(11L)); //ignores the main requests
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void requestUnboundedFromStartRequestsSourceOnce() {
//		LongAdder requestCallCount = new LongAdder();
//		LongAdder totalRequest = new LongAdder();
//		Flux<Integer> source = Flux.range(1, 10).hide()
//		                           .doOnRequest(r -> requestCallCount.increment())
//		                           .doOnRequest(totalRequest::add);
//
//		StepVerifier.withVirtualTime(//start with an unbounded request
//				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
//						Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL))
//		            .expectSubscription()
//		            .expectNext(Arrays.asList(1, 2, 3))
//		            .expectNext(Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9))
//		            .expectNext(Collections.singletonList(10))
//		            .expectComplete()
//		            .verify();
//
//		assertThat(requestCallCount.intValue(), is(1));
//		assertThat(totalRequest.longValue(), is(Long.MAX_VALUE)); //also unbounded
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void requestSwitchingToMaxRequestsSourceOnlyOnceMore() {
//		LongAdder requestCallCount = new LongAdder();
//		LongAdder totalRequest = new LongAdder();
//		Flux<Integer> source = Flux.range(1, 10).hide()
//		                           .doOnRequest(r -> requestCallCount.increment())
//		                           .doOnRequest(r -> totalRequest.add(r < Long.MAX_VALUE ? r : 1000L));
//
//		StepVerifier.withVirtualTime( //start with a single request
//				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
//						Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL), 1)
//		            .expectSubscription()
//		            .expectNext(Arrays.asList(1, 2, 3))
//		            .expectNoEvent(Duration.ofSeconds(1))
//		            .then(() -> assertThat(requestCallCount.intValue(), is(3)))
//		            .thenRequest(Long.MAX_VALUE)
//		            .expectNext(Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9))
//		            .expectNext(Collections.singletonList(10))
//		            .expectComplete()
//		            .verify();
//
//		assertThat(requestCallCount.intValue(), is(4));
//		assertThat(totalRequest.longValue(), is(1003L)); //the switch to unbounded is translated to 1000
//	}
//
//	@Test
//	@SuppressWarnings("unchecked")
//	public void requestBoundedConditionalFusesDemands() {
//		LongAdder requestCallCount = new LongAdder();
//		LongAdder totalRequest = new LongAdder();
//		Flux<Integer> source = Flux.range(1, 10)
//		                           .doOnRequest(r -> requestCallCount.increment())
//		                           .doOnRequest(totalRequest::add);
//
//		StepVerifier.withVirtualTime(
//				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
//						Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL), 1)
//		            .expectSubscription()
//		            .expectNext(Arrays.asList(1, 2, 3))
//		            .thenRequest(1)
//					.expectNext(Arrays.asList(4, 5, 6))
//		            .thenRequest(1)
//					.expectNext(Arrays.asList(7, 8, 9))
////		            .expectNoEvent(Duration.ofSeconds(1))
//		            .thenRequest(1)
//		            .expectNext(Collections.singletonList(10))
//		            .expectComplete()
//		            .verify();
//
//		//despite the 1 by 1 demand, only 1 fused request per buffer, 4 buffers
//		assertThat(requestCallCount.intValue(), is(4));
//		assertThat(totalRequest.longValue(), is(4L)); //ignores the main requests
//	}
//
//
//	@Test
//	public void testBufferPredicateUntilIncludesBoundaryLast() {
//		String[] colorSeparated = new String[]{"red", "green", "blue", "#", "green", "green", "#", "blue", "cyan"};
//
//		Flux<List<String>> colors = Flux
//				.fromArray(colorSeparated)
//				.bufferUntil(val -> val.equals("#"))
//				.log();
//
//		StepVerifier.create(colors)
//		            .consumeNextWith(l1 -> Assert.assertThat(l1, contains("red", "green", "blue", "#")))
//		            .consumeNextWith(l2 -> Assert.assertThat(l2, contains("green", "green", "#")))
//		            .consumeNextWith(l3 -> Assert.assertThat(l3, contains("blue", "cyan")))
//		            .expectComplete()
//		            .verify();
//	}
//
//	@Test
//	public void testBufferPredicateUntilIncludesBoundaryLastAfter() {
//		String[] colorSeparated = new String[]{"red", "green", "blue", "#", "green", "green", "#", "blue", "cyan"};
//
//		Flux<List<String>> colors = Flux
//				.fromArray(colorSeparated)
//				.bufferUntil(val -> val.equals("#"), false)
//				.log();
//
//		StepVerifier.create(colors)
//		            .consumeNextWith(l1 -> Assert.assertThat(l1, contains("red", "green", "blue", "#")))
//		            .consumeNextWith(l2 -> Assert.assertThat(l2, contains("green", "green", "#")))
//		            .consumeNextWith(l3 -> Assert.assertThat(l3, contains("blue", "cyan")))
//		            .expectComplete()
//		            .verify();
//	}
//
//	@Test
//	public void testBufferPredicateUntilCutBeforeIncludesBoundaryFirst() {
//		String[] colorSeparated = new String[]{"red", "green", "blue", "#", "green", "green", "#", "blue", "cyan"};
//
//		Flux<List<String>> colors = Flux
//				.fromArray(colorSeparated)
//				.bufferUntil(val -> val.equals("#"), true)
//				.log();
//
//		StepVerifier.create(colors)
//		            .thenRequest(1)
//		            .consumeNextWith(l1 -> Assert.assertThat(l1, contains("red", "green", "blue")))
//		            .consumeNextWith(l2 -> Assert.assertThat(l2, contains("#", "green", "green")))
//		            .consumeNextWith(l3 -> Assert.assertThat(l3, contains("#", "blue", "cyan")))
//		            .expectComplete()
//		            .verify();
//	}
//
//	@Test
//	public void testBufferPredicateWhileDoesntIncludeBoundary() {
//		String[] colorSeparated = new String[]{"red", "green", "blue", "#", "green", "green", "#", "blue", "cyan"};
//
//		Flux<List<String>> colors = Flux
//				.fromArray(colorSeparated)
//				.bufferWhile(val -> !val.equals("#"))
//				.log();
//
//		StepVerifier.create(colors)
//		            .consumeNextWith(l1 -> Assert.assertThat(l1, contains("red", "green", "blue")))
//		            .consumeNextWith(l2 -> Assert.assertThat(l2, contains("green", "green")))
//		            .consumeNextWith(l3 -> Assert.assertThat(l3, contains("blue", "cyan")))
//		            .expectComplete()
//		            .verify();
//	}
}