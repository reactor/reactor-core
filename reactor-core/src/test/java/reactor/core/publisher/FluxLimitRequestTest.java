/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class FluxLimitRequestTest {

	@Test
	void apiCall() {
		LongAdder rCount = new LongAdder();
		final Flux<Integer> source = Flux.range(1, 100)
		                                 .doOnRequest(rCount::add);

		StepVerifier.create(source.take(3, true))
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		assertThat(rCount.longValue()).isEqualTo(3);
	}

	@Test
	void unboundedDownstreamRequest() {
		LongAdder rCount = new LongAdder();
		final Flux<Integer> source = Flux.range(1, 100)
		                                 .doOnRequest(rCount::add);

		Flux<Integer> test = new FluxLimitRequest<>(source, 3);

		StepVerifier.create(test, Long.MAX_VALUE)
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		assertThat(rCount.longValue())
				.as("total request should match the limitRequest")
				.isEqualTo(3);
	}

	@Test
	void boundedDownStreamRequestMatchesCap() {
		LongAdder rCount = new LongAdder();
		final Flux<Integer> source = Flux.range(1, 100)
		                                 .doOnRequest(rCount::add);

		Flux<Integer> test = new FluxLimitRequest<>(source, 10);

		StepVerifier.create(test, 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenRequest(4)
		            .expectNext(1, 2, 3, 4)
		            .then(() -> assertThat(rCount.longValue())
				            .as("request under cap should be propagated as is")
				            .isEqualTo(4))
		            .thenRequest(3)
		            .expectNext(5, 6, 7)
		            .thenRequest(3)
		            .expectNext(8, 9, 10)
		            .verifyComplete();

		assertThat(rCount.longValue())
				.as("total request should match the limitRequest")
				.isEqualTo(10);
	}

	@Test
	void boundedDownStreamRequestOverflowsCap() {
		List<Long> requests = new ArrayList<>();
		final Flux<Integer> source = Flux.range(1, 100)
		                                 .doOnRequest(requests::add);

		Flux<Integer> test = new FluxLimitRequest<>(source, 10);

		StepVerifier.create(test, 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .then(() -> assertThat(requests).as("no initial prefetch").isEmpty())
		            .thenRequest(4)
		            .expectNext(1, 2, 3, 4)
		            .then(() -> assertThat(requests)
				            .as("request under cap should be propagated as is")
				            .containsExactly(4L))
		            .thenRequest(18)
		            .expectNext(5, 6, 7,8, 9, 10)
		            .verifyComplete();

		assertThat(requests)
				.as("limitRequest should rebatch last request")
				.containsExactly(4L, 6L);
		assertThat(requests.stream().mapToLong(l -> l).sum())
				.as("total request should match the limitRequest")
				.isEqualTo(10);
	}

	@Test
	void extraneousSmallRequestsNotPropagatedAsZero() {
		List<Long> requests = new ArrayList<>();
		final Flux<Integer> source = Flux.range(1, 100)
		                                 .doOnRequest(requests::add);

		Flux<Integer> test = new FluxLimitRequest<>(source, 11);

		StepVerifier.create(test, 0)
		            .thenRequest(8)
		            .thenRequest(2)
		            .thenRequest(2)
		            .thenRequest(2)
		            .thenRequest(2)
		            .expectNextCount(11)
		            .verifyComplete();

		assertThat(requests)
				.as("limitRequest should not propagate extraneous requests as zeros")
				.containsExactly(8L, 2L, 1L);
	}

	@Test
	void largerSourceCancelled() {
		AtomicBoolean cancelled = new AtomicBoolean();

		Flux<Integer> test = Flux.range(1, 1000)
				.doOnCancel(() -> cancelled.set(true))
				.take(3, true);

		StepVerifier.create(test)
		            .expectNextCount(3)
		            .verifyComplete();

		assertThat(cancelled.get()).as("source is cancelled").isTrue();
	}

	@Test
	void takeCancelsOperatorAndSource() {
		AtomicBoolean sourceCancelled = new AtomicBoolean();
		AtomicBoolean operatorCancelled = new AtomicBoolean();
		LongAdder sourceRequested = new LongAdder();
		LongAdder operatorRequested = new LongAdder();

		Flux<Integer> test = Flux.range(1, 1000)
		                         .doOnCancel(() -> sourceCancelled.set(true))
		                         .doOnRequest(sourceRequested::add)
		                         .take(10, true)
		                         .doOnCancel(() -> operatorCancelled.set(true))
		                         .doOnRequest(operatorRequested::add)
		                         .take(3);

		StepVerifier.create(test)
		            .expectNextCount(3)
		            .verifyComplete();

		assertThat(operatorCancelled.get()).as("operator cancelled").isTrue();
		assertThat(operatorRequested.longValue()).as("operator request").isEqualTo(Long.MAX_VALUE);

		assertThat(sourceCancelled.get()).as("source cancelled").isTrue();
		assertThat(sourceRequested.longValue()).as("source request").isEqualTo(10);
	}

	@Test
	void noPrefetch() {
		assertThat(Flux.range(1, 10).take(3, true)
				.getPrefetch()).isZero();
	}

	@Test
	void errorAtCapNotPropagated() {
		TestPublisher<Integer> tp = TestPublisher.create();

		StepVerifier.create(tp.flux().take(3, true))
		            .then(() -> tp.next(1, 2, 3).error(new IllegalStateException("boom")))
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	void errorUnderCapPropagated() {
		TestPublisher<Integer> tp = TestPublisher.create();

		StepVerifier.create(tp.flux().take(4, true))
		            .then(() -> tp.next(1, 2, 3).error(new IllegalStateException("boom")))
		            .expectNext(1, 2, 3)
		            .verifyErrorMessage("boom");
	}

	@Test
	void completeUnderCap() {
		TestPublisher<Integer> tp = TestPublisher.create();

		StepVerifier.create(tp.flux().take(4, true))
		            .then(() -> tp.emit(1, 2, 3))
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	void nextSignalDespiteAllProducedIsDropped() {
		TestPublisher<Integer> tp = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE, TestPublisher.Violation.REQUEST_OVERFLOW);

		StepVerifier.create(tp.flux().take(3, true))
				.then(() -> tp.emit(1, 2, 3, 4))
				.expectNext(1, 2, 3)
				.expectComplete()
				.verifyThenAssertThat()
				.hasDropped(4);
	}

	@Test
	void completeSignalDespiteAllProducedNotPropagated() {
		TestPublisher<Integer> tp = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(tp.flux().take(3, true))
		            .then(() -> tp.emit(1, 2, 3))
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	void errorSignalDespiteAllProducedIsDropped() {
		TestPublisher<Integer> tp = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(tp.flux().take(3, true))
				.then(() -> tp.next(1, 2, 3).error(new IllegalStateException("boom")))
				.expectNext(1, 2, 3)
				.expectComplete()
				.verifyThenAssertThat()
				.hasDroppedErrorWithMessage("boom");
	}

	@Test
	void zeroCompletesImmediately() {
		StepVerifier.create(Flux.range(1, 100).take(0, true))
				.expectComplete()
				.verify(Duration.ofSeconds(1));
	}

	@Test
	void zeroDoesntEvenSubscribeToUpstream() {
		AtomicBoolean subscribed = new AtomicBoolean();
		StepVerifier.create(Flux.never().doOnSubscribe(sub -> subscribed.set(true)).take(0, true))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		assertThat(subscribed).isFalse();
	}

	@Test
	void negativeCapIsRejectedAtAssembly() {
		assertThatIllegalArgumentException().isThrownBy(() -> Flux.empty().take(-1, true))
				.withMessage("cap >= 0 required but it was -1");
	}

	@Test
	void scanOperator() {
		Flux<String> source = Flux.just("foo").map(Function.identity());
		FluxLimitRequest<String> operator = new FluxLimitRequest<>(source, 123);

		assertThat(operator.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(operator.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(123L);
		assertThat(operator.scan(Scannable.Attr.PREFETCH)).isEqualTo(0);
		assertThat(operator.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	void scanInner() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, null, null, null);
		FluxLimitRequest.FluxLimitRequestSubscriber<String> inner =
				new FluxLimitRequest.FluxLimitRequestSubscriber<>(actual, 2);
		Subscription s = Operators.emptySubscription();
		inner.onSubscribe(s);

		assertThat(inner.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(inner.scan(Scannable.Attr.PARENT)).isSameAs(s);
		assertThat(inner.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(inner.scan(Scannable.Attr.TERMINATED)).isFalse();

		inner.onNext("foo");
		inner.onNext("bar");
		assertThat(inner.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	void raceRequest() {
		List<Long> requests = Collections.synchronizedList(new ArrayList<>());
		final Flux<Integer> flux = Flux.range(1, 1000)
		                               .doOnRequest(requests::add)
		                               .take(81, true);
		BaseSubscriber<Integer> base = new BaseSubscriber<Integer>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
			}
		};
		flux.subscribe(base);

		for (int i = 0; i < 11; i++) {
			final int idx = i;
			RaceTestUtils.race(
					() -> base.request(idx % 2 == 0 ? 10 : 8),
					() -> base.request(8)
			);
		}

		assertThat(requests.stream().mapToLong(l -> l).sum())
				.as("total request should match the limitRequest")
				.isEqualTo(81);
		assertThat(requests.subList(0, requests.size() - 2))
				.allMatch(l -> l % 2 == 0, "all requests except last two are even");
		assertThat(requests)
				.filteredOn(l -> l % 2 == 1)
				.as("only one odd element toward end")
				.hasSize(1);
	}

}
