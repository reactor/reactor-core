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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.MemoryUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class FluxBufferPredicateTest {

	@Test
	@SuppressWarnings("unchecked")
	public void normalUntil() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferUntil = new FluxBufferPredicate<>(
				sp1, i -> i % 3 == 0, Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL);

		StepVerifier.create(bufferUntil)
				.expectSubscription()
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(1))
				.then(() -> sp1.onNext(2))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(3))
				.expectNext(Arrays.asList(1, 2, 3))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(4))
				.then(() -> sp1.onNext(5))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(6))
				.expectNext(Arrays.asList(4, 5, 6))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(7))
				.then(() -> sp1.onNext(8))
				.then(sp1::onComplete)
				.expectNext(Arrays.asList(7, 8))
				.expectComplete()
				.verify();
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void onCompletionBeforeLastBoundaryBufferEmitted() {
		Flux<Integer> source = Flux.just(1, 2);

		FluxBufferPredicate<Integer, List<Integer>> bufferUntil =
				new FluxBufferPredicate<>(source, i -> i >= 3, Flux.listSupplier(),
						FluxBufferPredicate.Mode.UNTIL);

		FluxBufferPredicate<Integer, List<Integer>> bufferUntilOther =
				new FluxBufferPredicate<>(source, i -> i >= 3, Flux.listSupplier(),
						FluxBufferPredicate.Mode.UNTIL_CUT_BEFORE);

		FluxBufferPredicate<Integer, List<Integer>> bufferWhile =
				new FluxBufferPredicate<>(source, i -> i < 3, Flux.listSupplier(),
						FluxBufferPredicate.Mode.WHILE);

		StepVerifier.create(bufferUntil)
				.expectNext(Arrays.asList(1, 2))
				.expectComplete()
				.verify();

		StepVerifier.create(bufferUntilOther)
		            .expectNext(Arrays.asList(1, 2))
		            .expectComplete()
		            .verify();

		StepVerifier.create(bufferWhile)
		            .expectNext(Arrays.asList(1, 2))
		            .expectComplete()
		            .verify();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void mainErrorUntil() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferUntil = new FluxBufferPredicate<>(
				sp1, i -> i % 3 == 0, Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL);

		StepVerifier.create(bufferUntil)
		            .expectSubscription()
		            .then(() -> sp1.onNext(1))
		            .then(() -> sp1.onNext(2))
		            .then(() -> sp1.onNext(3))
		            .expectNext(Arrays.asList(1, 2, 3))
		            .then(() -> sp1.onNext(4))
		            .then(() -> sp1.onError(new RuntimeException("forced failure")))
		            .expectErrorMessage("forced failure")
		            .verify();
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void predicateErrorUntil() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferUntil = new FluxBufferPredicate<>(
				sp1,
				i -> {
					if (i == 5) throw new IllegalStateException("predicate failure");
					return i % 3 == 0;
				}, Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL);

		StepVerifier.create(bufferUntil)
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
		FluxBufferPredicate<Integer, List<Integer>> bufferUntilOther = new FluxBufferPredicate<>(
				sp1, i -> i % 3 == 0, Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL_CUT_BEFORE);

		StepVerifier.create(bufferUntilOther)
				.expectSubscription()
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(1))
				.then(() -> sp1.onNext(2))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(3))
				.expectNext(Arrays.asList(1, 2))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(4))
				.then(() -> sp1.onNext(5))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(6))
				.expectNext(Arrays.asList(3, 4, 5))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(7))
				.then(() -> sp1.onNext(8))
				.then(sp1::onComplete)
				.expectNext(Arrays.asList(6, 7, 8))
				.expectComplete()
				.verify();
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void mainErrorUntilOther() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferUntilOther =
				new FluxBufferPredicate<>(sp1, i -> i % 3 == 0, Flux.listSupplier(),
						FluxBufferPredicate.Mode.UNTIL_CUT_BEFORE);

		StepVerifier.create(bufferUntilOther)
		            .expectSubscription()
		            .then(() -> sp1.onNext(1))
		            .then(() -> sp1.onNext(2))
		            .then(() -> sp1.onNext(3))
		            .expectNext(Arrays.asList(1, 2))
		            .then(() -> sp1.onNext(4))
		            .then(() -> sp1.onError(new RuntimeException("forced failure")))
		            .expectErrorMessage("forced failure")
		            .verify();
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void predicateErrorUntilOther() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferUntilOther =
				new FluxBufferPredicate<>(sp1,
				i -> {
					if (i == 5) throw new IllegalStateException("predicate failure");
					return i % 3 == 0;
				}, Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL_CUT_BEFORE);

		StepVerifier.create(bufferUntilOther)
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
	public void untilChangedNoRepetition() {
		StepVerifier.create(Flux.range(0, 5)
		                        .bufferUntilChanged())
		            .expectSubscription()
		            .expectNext(Collections.singletonList(0))
		            .expectNext(Collections.singletonList(1))
		            .expectNext(Collections.singletonList(2))
		            .expectNext(Collections.singletonList(3))
		            .expectNext(Collections.singletonList(4))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void untilChangedWithKeySelector() {
		StepVerifier.create(Flux.range(0, 5)
		                        .bufferUntilChanged(i -> i / 2))
		            .expectSubscription()
		            .expectNext(Arrays.asList(0, 1))
		            .expectNext(Arrays.asList(2, 3))
		            .expectNext(Collections.singletonList(4))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void untilChangedNoSharedState() {
		List<List<Integer>> buffers = new CopyOnWriteArrayList<>();
		List<Integer> list1 = Arrays.asList(1, 1, 1, 1);
		List<Integer> list2 = Arrays.asList(3, 3, 2, 2, 2, 2, 2, 2, 2, 2);
		AtomicBoolean first = new AtomicBoolean();

		Flux<List<Integer>> source = Flux.defer(() -> {
			if (first.compareAndSet(false, true)) {
				return Flux.fromIterable(list1).delayElements(Duration.ofMillis(200));
			}
			first.set(false);
			return Flux.fromIterable(list2).delayElements(Duration.ofMillis(50));
		})
				.bufferUntilChanged();

		Mono.when(
				source.map(buffers::add),
				source.map(buffers::add)
		).block();

		List<Integer> flattened = buffers.stream().flatMap(List::stream).collect(Collectors.toList());
		Assertions.assertThat(flattened).containsExactly(3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1);
	}

	@Test
	public void untilChangedSomeRepetition() {
		StepVerifier.create(Flux.just(1, 1, 2, 2, 3, 3, 1)
		                        .bufferUntilChanged())
		            .expectSubscription()
		            .expectNext(Arrays.asList(1, 1))
		            .expectNext(Arrays.asList(2, 2))
		            .expectNext(Arrays.asList(3, 3))
		            .expectNext(Arrays.asList(1))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void untilChangedDisposesStateOnComplete() {
		MemoryUtils.RetainedDetector retainedDetector = new MemoryUtils.RetainedDetector();
		Flux<List<AtomicInteger>> test =
				Flux.range(1, 100)
				    .map(AtomicInteger::new) // wrap integer with object to test gc
				    .map(retainedDetector::tracked)
				    .bufferUntilChanged();

		StepVerifier.create(test)
		            .expectNextCount(100)
		            .verifyComplete();

		System.gc();

		await()
				.atMost(2, TimeUnit.SECONDS)
				.untilAsserted(retainedDetector::assertAllFinalized);
	}

	@Test
	public void untilChangedDisposesStateOnError() {
		MemoryUtils.RetainedDetector retainedDetector = new MemoryUtils.RetainedDetector();
		Flux<List<AtomicInteger>> test =
				Flux.range(1, 100)
				    .map(AtomicInteger::new) // wrap integer with object to test gc
				    .map(retainedDetector::tracked)
				    .concatWith(Mono.error(new Throwable("expected")))
				    .bufferUntilChanged();


		StepVerifier.create(test)
		            .expectNextCount(99) // last buffer discarded onError
		            .verifyErrorMessage("expected");

		System.gc();

		await()
				.atMost(2, TimeUnit.SECONDS)
				.untilAsserted(retainedDetector::assertAllFinalized);
	}

	@Test
	public void untilChangedDisposesStateOnCancel() {
		MemoryUtils.RetainedDetector retainedDetector = new MemoryUtils.RetainedDetector();
		Flux<List<AtomicInteger>> test =
				Flux.range(1, 100)
				    .map(AtomicInteger::new) // wrap integer with object to test gc
				    .map(retainedDetector::tracked)
				    .concatWith(Mono.error(new Throwable("unexpected")))
				    .bufferUntilChanged()
				    .take(50);


		StepVerifier.create(test)
		            .expectNextCount(50)
		            .verifyComplete();

		System.gc();

		await()
				.atMost(2, TimeUnit.SECONDS)
				.untilAsserted(retainedDetector::assertAllFinalized);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void normalWhile() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferWhile = new FluxBufferPredicate<>(
				sp1, i -> i % 3 != 0, Flux.listSupplier(),
				FluxBufferPredicate.Mode.WHILE);

		StepVerifier.create(bufferWhile)
				.expectSubscription()
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(1))
				.then(() -> sp1.onNext(2))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(3))
				.expectNext(Arrays.asList(1, 2))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(4))
				.then(() -> sp1.onNext(5))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(6))
				.expectNext(Arrays.asList(4, 5))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(7))
				.then(() -> sp1.onNext(8))
				.then(sp1::onComplete)
				.expectNext(Arrays.asList(7, 8))
				.expectComplete()
				.verify();
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void normalWhileDoesntInitiallyMatch() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferWhile = new FluxBufferPredicate<>(
				sp1, i -> i % 3 == 0, Flux.listSupplier(), FluxBufferPredicate.Mode.WHILE);

		StepVerifier.create(bufferWhile)
				.expectSubscription()
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(1))
				.then(() -> sp1.onNext(2))
				.then(() -> sp1.onNext(3))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(4))
				.expectNext(Arrays.asList(3)) //emission of 4 triggers the buffer emit
				.then(() -> sp1.onNext(5))
				.then(() -> sp1.onNext(6))
				.expectNoEvent(Duration.ofMillis(10))
				.then(() -> sp1.onNext(7)) // emission of 7 triggers the buffer emit
				.expectNext(Arrays.asList(6))
				.then(() -> sp1.onNext(8))
				.then(() -> sp1.onNext(9))
				.expectNoEvent(Duration.ofMillis(10))
				.then(sp1::onComplete) // completion triggers the buffer emit
			    .expectNext(Collections.singletonList(9))
				.expectComplete()
				.verify(Duration.ofSeconds(1));
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void normalWhileDoesntMatch() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferWhile = new FluxBufferPredicate<>(
				sp1, i -> i > 4, Flux.listSupplier(), FluxBufferPredicate.Mode.WHILE);

		StepVerifier.create(bufferWhile)
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
		FluxBufferPredicate<Integer, List<Integer>> bufferWhile = new FluxBufferPredicate<>(
				sp1, i -> i % 3 == 0, Flux.listSupplier(), FluxBufferPredicate.Mode.WHILE);

		StepVerifier.create(bufferWhile)
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
	@SuppressWarnings("unchecked")
	public void predicateErrorWhile() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferWhile = new FluxBufferPredicate<>(
				sp1,
				i -> {
					if (i == 3) return true;
					if (i == 5) throw new IllegalStateException("predicate failure");
					return false;
				}, Flux.listSupplier(), FluxBufferPredicate.Mode.WHILE);

		StepVerifier.create(bufferWhile)
					.expectSubscription()
					.then(() -> sp1.onNext(1)) //ignored
					.then(() -> sp1.onNext(2)) //ignored
					.then(() -> sp1.onNext(3)) //buffered
					.then(() -> sp1.onNext(4)) //ignored, emits buffer
					.expectNext(Arrays.asList(3))
					.then(() -> sp1.onNext(5)) //fails
					.expectErrorMessage("predicate failure")
					.verify(Duration.ofMillis(100));
		assertFalse(sp1.hasDownstreams());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void bufferSupplierThrows() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferUntil = new FluxBufferPredicate<>(
				sp1, i -> i % 3 == 0,
				() -> { throw new RuntimeException("supplier failure"); },
				FluxBufferPredicate.Mode.UNTIL);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());

		StepVerifier.create(bufferUntil)
		            .expectErrorMessage("supplier failure")
		            .verify();
	}

	@Test
	public void bufferSupplierThrowsLater() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		int count[] = {1};
		FluxBufferPredicate<Integer, List<Integer>> bufferUntil = new FluxBufferPredicate<>(
				sp1, i -> i % 3 == 0,
				() -> {
					if (count[0]-- > 0) {
						return new ArrayList<>();
					}
					throw new RuntimeException("supplier failure");
				},
				FluxBufferPredicate.Mode.UNTIL);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());

		StepVerifier.create(bufferUntil)
		            .then(() -> sp1.onNext(1))
		            .then(() -> sp1.onNext(2))
		            .expectNoEvent(Duration.ofMillis(10))
		            .then(() -> sp1.onNext(3))
		            .expectErrorMessage("supplier failure")
		            .verify();
	}

	@Test
	public void bufferSupplierReturnsNull() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferUntil = new FluxBufferPredicate<>(
				sp1, i -> i % 3 == 0,
				() -> null,
				FluxBufferPredicate.Mode.UNTIL);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());

		StepVerifier.create(bufferUntil)
		            .then(() -> sp1.onNext(1))
		            .expectErrorMatches(t -> t instanceof NullPointerException &&
		                "The bufferSupplier returned a null initial buffer".equals(t.getMessage()))
		            .verify();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void multipleTriggersOfEmptyBufferKeepInitialBuffer() {
		//this is best demonstrated with bufferWhile:
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		LongAdder bufferCount = new LongAdder();
		Supplier<List<Integer>> bufferSupplier = () -> {
			bufferCount.increment();
			return new ArrayList<>();
		};

		FluxBufferPredicate<Integer, List<Integer>> bufferWhile = new
				FluxBufferPredicate<>(
				sp1, i -> i >= 10,
				bufferSupplier,
				FluxBufferPredicate.Mode.WHILE);

		StepVerifier.create(bufferWhile)
		            .then(() -> assertThat(bufferCount.intValue(), is(1)))
		            .then(() -> sp1.onNext(1))
		            .then(() -> sp1.onNext(2))
		            .then(() -> sp1.onNext(3))
		            .then(() -> assertThat(bufferCount.intValue(), is(1)))
		            .expectNoEvent(Duration.ofMillis(10))
		            .then(() -> sp1.onNext(10))
		            .then(() -> sp1.onNext(11))
		            .then(sp1::onComplete)
		            .expectNext(Arrays.asList(10, 11))
		            .then(() -> assertThat(bufferCount.intValue(), is(1)))
		            .expectComplete()
		            .verify();

		assertThat(bufferCount.intValue(), is(1));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void requestBounded() {
		LongAdder requestCallCount = new LongAdder();
		LongAdder totalRequest = new LongAdder();
		Flux<Integer> source = Flux.range(1, 10).hide()
		                           .doOnRequest(r -> requestCallCount.increment())
		                           .doOnRequest(totalRequest::add);

		StepVerifier.withVirtualTime( //start with a request for 1 buffer
				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
				Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL), 1)
		            .expectSubscription()
		            .expectNext(Arrays.asList(1, 2, 3))
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenRequest(2)
		            .expectNext(Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9))
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenRequest(3)
		            .expectNext(Collections.singletonList(10))
		            .expectComplete()
		            .verify();

		/*
			request pattern expected:

			BUF 1 => 1 buffer wanted
			  UP 1 => 1/3 1st buffer, propagated from above
			UP 1 => 2/3 1st buffer
			UP 1 => EMIT 1st buffer
			--- pause
			BUF 2 => 2 more buffers wanted
			  UP 2 => 2/3 2nd buffer
			UP 1 => EMIT 2nd buffer
			UP 1 => 1/3 3rd buffer
			UP 1 => 2/3 3rd buffer
			UP 1 => EMIT 3rd buffer
			--- pause
			BUF 3
			  UP 3 => 1/3 4th buffer then COMPLETED
			COMPLETED

			hence 9 request calls and request(12) total (2 extraneous due to last request(3))
		 */
		assertThat(requestCallCount.intValue(), is(9));
		assertThat(totalRequest.longValue(), is(12L));
	}

	@Test
	public void requestBoundedSeveralInitial() {
		LongAdder requestCallCount = new LongAdder();
		LongAdder totalRequest = new LongAdder();
		Flux<Integer> source = Flux.range(1, 10).hide()
		                           .doOnRequest(r -> requestCallCount.increment())
		                           .doOnRequest(totalRequest::add);

		StepVerifier.withVirtualTime( //start with a request for 2 buffers
				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
				Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL), 2)
		            .expectSubscription()
		            .expectNext(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6))
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenRequest(2)
					.expectNext(Arrays.asList(7, 8, 9))
		            .expectNext(Collections.singletonList(10))
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));

		/*
			upstream requesting pattern:
			2 => initial request, wants 2 buffers, upstream request propagated
			  up 2 => 2/3 1st buffer
			up 1 => EMIT 1st buffer
			up 1 => 1/3 2nd buffer
			up 1 => 2/3 2nd buffer
			up 1 => EMIT 2nd buffer
			--- pause
			2 => wants 2 more buffers, up 2
			 up 2 => 2/3 3rd buffer
			up 1 => EMIT 3rd buffer
			up 1 => 1/3 4th buffer
			up 1 => COMPLETED

			so 9 total upstream requests calls
			and 11 total requested
		 */
		assertThat(requestCallCount.intValue(), is(9));
		assertThat(totalRequest.longValue(), is(11L));
	}

	@Test
	public void requestRemainingBuffersAfterBufferEmission() {
		LongAdder requestCallCount = new LongAdder();
		LongAdder totalRequest = new LongAdder();
		Flux<Integer> source = Flux.range(1, 10).hide()
		                           .doOnRequest(r -> requestCallCount.increment())
		                           .doOnRequest(totalRequest::add);

		StepVerifier.withVirtualTime( //start with a request for 3 buffers
				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
						Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL), 3)
		            .expectSubscription()
		            .expectNext(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9))
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenRequest(1)
		            .expectNext(Collections.singletonList(10))
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));

		/*
			upstream requesting pattern:
			3 => initial request, wants 3 buffers, upstream request propagated
			  up 3 => 3/3, EMIT 1st buffer
			up 1 => 1/3 2nd buffer
			up 1 => 2/3 2nd buffer
			up 1 => EMIT 2nd buffer
			up 1 => 1/3 3rd buffer
			up 1 => 2/3 3rd buffer
			up 1 => EMIT 3rd buffer
			--- pause
			1 => want 1 more buffer
			  up 1 => 1/3 2nd buffer
			up 1 => COMPLETED

			so 9 total upstream requests calls
			and 11 total requested
		 */
		assertThat(requestCallCount.intValue(), is(9));
		assertThat(totalRequest.longValue(), is(11L));
	}

	// the 3 race condition tests below essentially validate that racing request vs onNext
	// don't lead to backpressure errors.
	@Test
	public void requestRaceWithOnNext() {
		AtomicLong requested = new AtomicLong();
		TestPublisher<Integer> testPublisher = TestPublisher.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferPredicate = new FluxBufferPredicate<>(
				testPublisher.flux().doOnRequest(requested::addAndGet),
				i -> true, ArrayList::new, FluxBufferPredicate.Mode.UNTIL);

		BaseSubscriber<List<Integer>> subscriber = new BaseSubscriber<List<Integer>>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				request(1);
			}
		};
		bufferPredicate.subscribe(subscriber);
		@SuppressWarnings("unchecked")
		final FluxBufferPredicate.BufferPredicateSubscriber<Integer, List<Integer>> bufferPredicateSubscriber =
				(FluxBufferPredicate.BufferPredicateSubscriber<Integer, List<Integer>>) subscriber.subscription;

		for (int i = 0; i < 10; i++) {
			final int value = i;
			RaceTestUtils.race(() -> subscriber.request(1), () -> testPublisher.next(value));
		}
		for (int i = 0; i < bufferPredicateSubscriber.requestedFromSource; i++) {
			testPublisher.next(100 + i);
		}
		testPublisher.complete();

		Assertions.assertThat(requested).as("total upstream request").hasValue(10 + 1);
	}

	@Test
	public void onNextRaceWithRequest() {
		AtomicLong requested = new AtomicLong();
		TestPublisher<Integer> testPublisher = TestPublisher.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferPredicate = new FluxBufferPredicate<>(
				testPublisher.flux().doOnRequest(requested::addAndGet),
				i -> true, ArrayList::new, FluxBufferPredicate.Mode.UNTIL);

		BaseSubscriber<List<Integer>> subscriber = new BaseSubscriber<List<Integer>>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				request(1);
			}
		};
		bufferPredicate.subscribe(subscriber);
		@SuppressWarnings("unchecked")
		final FluxBufferPredicate.BufferPredicateSubscriber<Integer, List<Integer>> bufferPredicateSubscriber =
				(FluxBufferPredicate.BufferPredicateSubscriber<Integer, List<Integer>>) subscriber.subscription;

		for (int i = 0; i < 10; i++) {
			final int value = i;
			RaceTestUtils.race(() -> testPublisher.next(value), () -> subscriber.request(1));
		}
		for (int i = 0; i < bufferPredicateSubscriber.requestedFromSource; i++) {
			testPublisher.next(100 + i);
		}
		testPublisher.complete();

		Assertions.assertThat(requested).as("total upstream request").hasValue(10 + 1);
	}

	@Test
	public void onNextRaceWithRequestOfTwo() {
		AtomicLong requested = new AtomicLong();
		TestPublisher<Integer> testPublisher = TestPublisher.create();
		FluxBufferPredicate<Integer, List<Integer>> bufferPredicate = new FluxBufferPredicate<>(
				testPublisher.flux().doOnRequest(requested::addAndGet),
				i -> true, ArrayList::new, FluxBufferPredicate.Mode.UNTIL);

		BaseSubscriber<List<Integer>> subscriber = new BaseSubscriber<List<Integer>>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				request(1);
			}
		};
		bufferPredicate.subscribe(subscriber);
		@SuppressWarnings("unchecked")
		final FluxBufferPredicate.BufferPredicateSubscriber<Integer, List<Integer>> bufferPredicateSubscriber =
				(FluxBufferPredicate.BufferPredicateSubscriber<Integer, List<Integer>>) subscriber.subscription;

		for (int i = 0; i < 10; i++) {
			final int value = i;
			RaceTestUtils.race(() -> testPublisher.next(value), () -> subscriber.request(2));
		}
		for (int i = 0; i < bufferPredicateSubscriber.requestedFromSource; i++) {
			testPublisher.next(100 + i);
		}
		testPublisher.complete();

		Assertions.assertThat(requested).as("total upstream request").hasValue(2 * 10 + 1);
	}

	@Test
	public void requestRaceWithOnNextLoops() {
		for (int i = 0; i < 100_000; i++) {
			requestRaceWithOnNext();
			onNextRaceWithRequest();
			onNextRaceWithRequestOfTwo();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void requestUnboundedFromStartRequestsSourceOnce() {
		LongAdder requestCallCount = new LongAdder();
		LongAdder totalRequest = new LongAdder();
		Flux<Integer> source = Flux.range(1, 10).hide()
		                           .doOnRequest(r -> requestCallCount.increment())
		                           .doOnRequest(totalRequest::add);

		StepVerifier.withVirtualTime(//start with an unbounded request
				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
						Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL))
		            .expectSubscription()
		            .expectNext(Arrays.asList(1, 2, 3))
		            .expectNext(Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9))
		            .expectNext(Collections.singletonList(10))
		            .expectComplete()
		            .verify();

		assertThat(requestCallCount.intValue(), is(1));
		assertThat(totalRequest.longValue(), is(Long.MAX_VALUE)); //also unbounded
	}

	@Test
	@SuppressWarnings("unchecked")
	public void requestSwitchingToMaxRequestsSourceOnlyOnceMore() {
		LongAdder requestCallCount = new LongAdder();
		LongAdder totalRequest = new LongAdder();
		Flux<Integer> source = Flux.range(1, 10).hide()
		                           .doOnRequest(r -> requestCallCount.increment())
		                           .doOnRequest(r -> totalRequest.add(r < Long.MAX_VALUE ? r : 1000L));

		StepVerifier.withVirtualTime( //start with a single request
				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
						Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL), 1)
		            .expectSubscription()
		            .expectNext(Arrays.asList(1, 2, 3))
		            .expectNoEvent(Duration.ofSeconds(1))
		            .then(() -> assertThat(requestCallCount.intValue(), is(3)))
		            .thenRequest(Long.MAX_VALUE)
		            .expectNext(Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9))
		            .expectNext(Collections.singletonList(10))
		            .expectComplete()
		            .verify();

		assertThat(requestCallCount.intValue(), is(4));
		assertThat(totalRequest.longValue(), is(1003L)); //the switch to unbounded is translated to 1000
	}

	@Test
	@SuppressWarnings("unchecked")
	public void requestBoundedConditionalFusesDemands() {
		LongAdder requestCallCount = new LongAdder();
		LongAdder totalRequest = new LongAdder();
		Flux<Integer> source = Flux.range(1, 10)
		                           .doOnRequest(r -> requestCallCount.increment())
		                           .doOnRequest(totalRequest::add);

		StepVerifier.withVirtualTime(
				() -> new FluxBufferPredicate<>(source, i -> i % 3 == 0,
						Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL), 1)
		            .expectSubscription()
		            .expectNext(Arrays.asList(1, 2, 3))
		            .thenRequest(1)
					.expectNext(Arrays.asList(4, 5, 6))
		            .thenRequest(1)
					.expectNext(Arrays.asList(7, 8, 9))
//		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenRequest(1)
		            .expectNext(Collections.singletonList(10))
		            .expectComplete()
		            .verify();

		//despite the 1 by 1 demand, only 1 fused request per buffer, 4 buffers
		assertThat(requestCallCount.intValue(), is(4));
		assertThat(totalRequest.longValue(), is(4L)); //ignores the main requests
	}


	@Test
	public void testBufferPredicateUntilIncludesBoundaryLast() {
		String[] colorSeparated = new String[]{"red", "green", "blue", "#", "green", "green", "#", "blue", "cyan"};

		Flux<List<String>> colors = Flux
				.fromArray(colorSeparated)
				.bufferUntil(val -> val.equals("#"))
				.log();

		StepVerifier.create(colors)
		            .consumeNextWith(l1 -> Assert.assertThat(l1, contains("red", "green", "blue", "#")))
		            .consumeNextWith(l2 -> Assert.assertThat(l2, contains("green", "green", "#")))
		            .consumeNextWith(l3 -> Assert.assertThat(l3, contains("blue", "cyan")))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void testBufferPredicateUntilIncludesBoundaryLastAfter() {
		String[] colorSeparated = new String[]{"red", "green", "blue", "#", "green", "green", "#", "blue", "cyan"};

		Flux<List<String>> colors = Flux
				.fromArray(colorSeparated)
				.bufferUntil(val -> val.equals("#"), false)
				.log();

		StepVerifier.create(colors)
		            .consumeNextWith(l1 -> Assert.assertThat(l1, contains("red", "green", "blue", "#")))
		            .consumeNextWith(l2 -> Assert.assertThat(l2, contains("green", "green", "#")))
		            .consumeNextWith(l3 -> Assert.assertThat(l3, contains("blue", "cyan")))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void testBufferPredicateUntilCutBeforeIncludesBoundaryFirst() {
		String[] colorSeparated = new String[]{"red", "green", "blue", "#", "green", "green", "#", "blue", "cyan"};

		Flux<List<String>> colors = Flux
				.fromArray(colorSeparated)
				.bufferUntil(val -> val.equals("#"), true)
				.log();

		StepVerifier.create(colors)
		            .thenRequest(1)
		            .consumeNextWith(l1 -> Assert.assertThat(l1, contains("red", "green", "blue")))
		            .consumeNextWith(l2 -> Assert.assertThat(l2, contains("#", "green", "green")))
		            .consumeNextWith(l3 -> Assert.assertThat(l3, contains("#", "blue", "cyan")))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void testBufferPredicateWhileDoesntIncludeBoundary() {
		String[] colorSeparated = new String[]{"red", "green", "blue", "#", "green", "green", "#", "blue", "cyan"};

		Flux<List<String>> colors = Flux
				.fromArray(colorSeparated)
				.bufferWhile(val -> !val.equals("#"))
				.log();

		StepVerifier.create(colors)
		            .consumeNextWith(l1 -> Assert.assertThat(l1, contains("red", "green", "blue")))
		            .consumeNextWith(l2 -> Assert.assertThat(l2, contains("green", "green")))
		            .consumeNextWith(l3 -> Assert.assertThat(l3, contains("blue", "cyan")))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void scanMain() {
		CoreSubscriber<? super List> actual = new LambdaSubscriber<>(null, e -> {}, null,
				sub -> sub.request(100));
		List<String> initialBuffer = Arrays.asList("foo", "bar");
		FluxBufferPredicate.BufferPredicateSubscriber<String, List<String>> test = new FluxBufferPredicate.BufferPredicateSubscriber<>(
				actual, initialBuffer, ArrayList::new, s -> s.length() < 5,
				FluxBufferPredicate.Mode.WHILE
		);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(2);
		Assertions.assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(100L);
		Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanMainCancelled() {
		CoreSubscriber<? super List> actual = new LambdaSubscriber<>(null, e -> {}, null,
				sub -> sub.request(100));
		List<String> initialBuffer = Arrays.asList("foo", "bar");
		FluxBufferPredicate.BufferPredicateSubscriber<String, List<String>> test = new FluxBufferPredicate.BufferPredicateSubscriber<>(
				actual, initialBuffer, ArrayList::new, s -> s.length() < 5,
				FluxBufferPredicate.Mode.WHILE
		);

		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void discardOnCancel() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatWith(Mono.never())
		                        .bufferUntil(i -> i > 10))
		            .thenAwait(Duration.ofMillis(10))
		            .thenCancel()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardOnPredicateError() {
		StepVerifier.create(Flux.just(1, 2, 3)
				.bufferUntil(i -> { if (i == 3) throw new IllegalStateException("boom"); else return false; }))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardOnError() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatWith(Mono.error(new IllegalStateException("boom")))
		                        .bufferUntil(i -> i > 10))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3);
	}

	//see https://github.com/reactor/reactor-core/issues/1937
	@Test
	public void testBufferUntilNoExtraRequestFromEmit() {
		Flux<List<Integer>> numbers = Flux.just(1, 2, 3)
		                                  .hide()
		                                  .bufferUntil(val -> true);

		StepVerifier.create(numbers, 1)
				.expectNext(Collections.singletonList(1))
				.thenRequest(1)
				.expectNext(Collections.singletonList(2))
				.thenAwait()
				.thenRequest(1)
				.expectNext(Collections.singletonList(3))
				.verifyComplete();
	}

	//see https://github.com/reactor/reactor-core/pull/2027
	@Test
	public void testBufferUntilNoExtraRequestFromConcurrentEmitAndRequest() {
		AtomicReference<Throwable> errorOrComplete = new AtomicReference<>();
		ConcurrentLinkedQueue<List<Integer>> buffers = new ConcurrentLinkedQueue<>();
		final BaseSubscriber<List<Integer>> subscriber = new BaseSubscriber<List<Integer>>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				request(1);
			}
			@Override
			protected void hookOnNext(List<Integer> value) {
				buffers.add(value);
				if (value.equals(Collections.singletonList(0))) {
					request(1);
				}
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				errorOrComplete.set(throwable);
			}

			@Override
			protected void hookFinally(SignalType type) {
				if (type != SignalType.ON_ERROR) {
					errorOrComplete.set(new IllegalArgumentException("Terminated with signal type " + type));
				}
			}
		};
		final CountDownLatch emitLatch = new CountDownLatch(1);
		Flux.just(0, 1, 2, 3, 4)
		    .bufferUntil(s -> {
			    if (s == 1) {
				    try {
					    Mono.fromRunnable(() -> {
						    subscriber.request(1);
						    emitLatch.countDown();
					    })
					        .subscribeOn(Schedulers.single())
					        .subscribe();
					    emitLatch.await();
				    } catch (InterruptedException e) {
					    throw Exceptions.propagate(e);
				    }
			    }
			    return true;
		    })
		    .subscribe((Subscriber<? super List<Integer>>) subscriber); //sync subscriber, no need to sleep/latch

		Assertions.assertThat(buffers).hasSize(3)
		          .allMatch(b -> b.size() == 1, "size one");
		//total requests: 3
		Assertions.assertThat(buffers.stream().flatMapToInt(l -> l.stream().mapToInt(Integer::intValue)))
		          .containsExactly(0,1,2);

		Assertions.assertThat(errorOrComplete.get()).isNull();

		//request the rest
		subscriber.requestUnbounded();

		Assertions.assertThat(errorOrComplete.get()).isInstanceOf(IllegalArgumentException.class)
		          .hasMessage("Terminated with signal type onComplete");
	}
}