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
import java.util.Comparator;
import java.util.List;

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.test.publisher.TestPublisher.Violation.CLEANUP_ON_TERMINATE;

public class MonoCollectListTest {

	@Test
	public void aFluxCanBeSorted(){
		List<Integer> vals = Flux.just(43, 32122, 422, 321, 43, 443311)
		                         .collectSortedList()
		                         .block();

		assertThat(vals).containsExactly(43, 43, 321, 422, 32122, 443311);
	}

	@Test
	public void aFluxCanBeSorted2(){
		List<Integer> vals = Flux.just(1, 2, 3, 4)
		                         .collectSortedList(Comparator.reverseOrder())
		                         .block();

		assertThat(vals).containsExactly(4,3,2,1);
	}

	@Test
	public void aFluxCanBeSorted3(){
		StepVerifier.create(Flux.just(43, 32122, 422, 321, 43, 443311)
		                        .sort(Comparator.reverseOrder()))
		            .expectNext(443311, 32122, 422, 321, 43, 43)
		            .verifyComplete();
	}

	@Test
	public void aFluxCanBeSorted4(){
		StepVerifier.create(Flux.just(43, 32122, 422, 321, 43, 443311)
		                        .sort())
		            .expectNext(43, 43, 321, 422, 32122, 443311)
		            .verifyComplete();
	}

	@Test
	public void collectListOne() {
		StepVerifier.create(Flux.just(1)
		                        .collectList())
		            .assertNext(d -> assertThat(d).containsExactly(1))
	                .verifyComplete();

	}

	@Test
	public void collectListEmpty() {
		StepVerifier.create(Flux.empty()
		                        .collectList())
		            .assertNext(d -> assertThat(d).isEmpty())
	                .verifyComplete();

	}

	@Test
	public void collectListCallable() {
		StepVerifier.create(Mono.fromCallable(() -> 1)
		                        .flux()
		                        .collectList())
		            .assertNext(d -> assertThat(d).containsExactly(1))
	                .verifyComplete();

	}

	@Test
	public void collectListError() {
		StepVerifier.create(Flux.error(new Exception("test"))
		                        .collectList())
		            .verifyErrorMessage("test");
	}

	@Test
	public void collectListErrorHide() {
		StepVerifier.create(Flux.error(new Exception("test"))
		                        .hide()
		                        .collectList())
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/1523
	@Test
	public void protocolErrorsOnNext() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(CLEANUP_ON_TERMINATE);

		StepVerifier.create(testPublisher.flux().collectList())
		            .expectSubscription()
		            .then(() -> testPublisher.emit("foo"))
		            .then(() -> testPublisher.next("bar"))
		            .assertNext(l -> assertThat(l).containsExactly("foo"))
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped("bar");
	}

	//see https://github.com/reactor/reactor-core/issues/1523
	@Test
	public void protocolErrorsOnError() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(CLEANUP_ON_TERMINATE);

		StepVerifier.create(testPublisher.flux().collectList())
		            .expectSubscription()
		            .then(() -> testPublisher.emit("foo"))
		            .then(() -> testPublisher.error(new IllegalStateException("boom")))
		            .assertNext(l -> assertThat(l).containsExactly("foo"))
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedErrorOfType(IllegalStateException.class);
	}

	@Test
	public void scanBufferAllSubscriber() {
		CoreSubscriber<List<String>> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoCollectList.MonoBufferAllSubscriber<String, List<String>> test = new MonoCollectList.MonoBufferAllSubscriber<String, List<String>>(
				actual, new ArrayList<>());
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);


		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void discardOnError() {
		Mono<List<Integer>> test = Flux.range(1, 10)
		                               .hide()
		                               .map(i -> {
			                               if (i == 5) throw new IllegalStateException("boom");
			                               return i;
		                               })
		                               .collectList();

		StepVerifier.create(test)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3, 4);
	}

	@Test
	public void discardOnCancel() {
		Mono<List<Long>> test = Flux.interval(Duration.ofMillis(100))
		                            .take(10)
		                            .collectList();

		StepVerifier.create(test)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(210))
		            .thenCancel()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(0L, 1L);
	}

	@Test
	public void discardOnNextPredicateMiss() {
		StepVerifier.create(Flux.range(1, 10)
		                        .hide() //hide both avoid the fuseable AND tryOnNext usage
		                        .filter(i -> i % 2 == 0)
		)
		            .expectNextCount(5)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 3, 5, 7, 9);
	}

	@Test
	public void discardTryOnNextPredicateFail() {
		List<Object> discarded = new ArrayList<>();
		CoreSubscriber<Integer> actual = new AssertSubscriber<>(Operators.enableOnDiscard(null, discarded::add));
		FluxFilter.FilterSubscriber<Integer> subscriber =
				new FluxFilter.FilterSubscriber<>(actual, i -> { throw new IllegalStateException("boom"); });
		subscriber.onSubscribe(Operators.emptySubscription());

		subscriber.tryOnNext(1);

		assertThat(discarded).containsExactly(1);
	}

	@Test
	public void discardTryOnNextPredicateMiss() {
		List<Object> discarded = new ArrayList<>();
		CoreSubscriber<Integer> actual = new AssertSubscriber<>(Operators.enableOnDiscard(null, discarded::add));
		FluxFilter.FilterSubscriber<Integer> subscriber =
				new FluxFilter.FilterSubscriber<>(actual, i -> i % 2 == 0);
		subscriber.onSubscribe(Operators.emptySubscription());

		subscriber.tryOnNext(1);
		subscriber.tryOnNext(2);

		assertThat(discarded).containsExactly(1);
	}

	@Test
	public void discardConditionalOnNextPredicateFail() {
		StepVerifier.create(Flux.range(1, 10)
		                        .hide()
		                        .filter(i -> { throw new IllegalStateException("boom"); })
		                        .filter(i -> true)
		)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}

	@Test
	public void discardConditionalOnNextPredicateMiss() {
		StepVerifier.create(Flux.range(1, 10)
		                        .hide()
		                        .filter(i -> i % 2 == 0)
		                        .filter(i -> true)
		)
		            .expectNextCount(5)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 3, 5, 7, 9);
	}

	@Test
	public void discardConditionalTryOnNextPredicateFail() {
		List<Object> discarded = new ArrayList<>();
		Fuseable.ConditionalSubscriber<Integer> actual = new FluxPeekFuseableTest.ConditionalAssertSubscriber<>(
				Operators.enableOnDiscard(null, discarded::add));

		FluxFilter.FilterConditionalSubscriber<Integer> subscriber =
				new FluxFilter.FilterConditionalSubscriber<>(actual, i -> {
					throw new IllegalStateException("boom");
				});
		subscriber.onSubscribe(Operators.emptySubscription());

		subscriber.tryOnNext(1);

		assertThat(discarded).containsExactly(1);
	}

	@Test
	public void discardConditionalTryOnNextPredicateMiss() {
		List<Object> discarded = new ArrayList<>();
		Fuseable.ConditionalSubscriber<Integer> actual = new FluxPeekFuseableTest.ConditionalAssertSubscriber<>(
				Operators.enableOnDiscard(null, discarded::add));

		FluxFilter.FilterConditionalSubscriber<Integer> subscriber =
				new FluxFilter.FilterConditionalSubscriber<>(actual, i -> i % 2 == 0);
		subscriber.onSubscribe(Operators.emptySubscription());

		subscriber.tryOnNext(1);
		subscriber.tryOnNext(2);

		assertThat(discarded).containsExactly(1);
	}
}