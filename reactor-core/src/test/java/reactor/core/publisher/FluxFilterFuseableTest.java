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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.FluxFilterFuseable.FilterFuseableConditionalSubscriber;
import reactor.core.publisher.FluxFilterFuseable.FilterFuseableSubscriber;
import reactor.core.scheduler.Schedulers;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxFilterFuseableTest extends FluxOperatorTest<String, String> {

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxFilterFuseable<Integer> test = new FluxFilterFuseable<>(parent, e -> true);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

    @Test
    public void scanSubscriber() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FilterFuseableSubscriber<String> test = new FilterFuseableSubscriber<>(actual, t -> true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    public void scanConditionalSubscriber() {
        @SuppressWarnings("unchecked")
        Fuseable.ConditionalSubscriber<String> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
        FilterFuseableConditionalSubscriber<String> test = new FilterFuseableConditionalSubscriber<>(actual, t -> true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    public void failureStrategyResumeSyncFused() {
        Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
        try {
            StepVerifier.create(Flux.range(0, 2)
                                    .filter(i -> 4 / i == 4))
                        .expectFusion(Fuseable.ANY, Fuseable.SYNC)
                        .expectNext(1)
                        .expectComplete()
                        .verifyThenAssertThat()
                        .hasDroppedExactly(0)
                        .hasDroppedErrorWithMessage("/ by zero");
        }
        finally {
            Hooks.resetOnNextError();
        }
    }

    @Test
    public void failureStrategyResumeConditionalSyncFused() {
        Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
        try {
            StepVerifier.create(Flux.range(0, 2)
                                    .filter(i -> 4 / i == 4)
                                    .filter(i -> true))
                        .expectFusion(Fuseable.ANY, Fuseable.SYNC)
                        .expectNext(1)
                        .expectComplete()
                        .verifyThenAssertThat()
                        .hasDroppedExactly(0)
                        .hasDroppedErrorWithMessage("/ by zero");
        }
        finally {
            Hooks.resetOnNextError();
        }
    }

	@Test
	public void discardOnNextPredicateFail() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //range uses tryOnNext, so let's use just instead
		                        .filter(i -> { throw new IllegalStateException("boom"); })
		)
		            .expectFusion(Fuseable.NONE)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}

	@Test
	public void discardOnNextPredicateMiss() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //range uses tryOnNext, so let's use just instead
		                        .filter(i -> i % 2 == 0)
		)
		            .expectFusion(Fuseable.NONE)
		            .expectNextCount(5)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 3, 5, 7, 9);
	}

	@Test
	public void discardTryOnNextPredicateFail() {
		StepVerifier.create(Flux.range(1, 10) //range uses tryOnNext
		                        .filter(i -> { throw new IllegalStateException("boom"); })
		)
		            .expectFusion(Fuseable.NONE)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}

	@Test
	public void discardTryOnNextPredicateMiss() {
		StepVerifier.create(Flux.range(1, 10) //range uses tryOnNext
		                        .filter(i -> i % 2 == 0)
		)
		            .expectFusion(Fuseable.NONE)
		            .expectNextCount(5)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 3, 5, 7, 9);
	}

	@Test
	public void discardPollAsyncPredicateFail() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //range uses tryOnNext, so let's use just instead
		                        .publishOn(Schedulers.newSingle("discardPollAsync"), 1)
		                        .filter(i -> { throw new IllegalStateException("boom"); })
		)
		            .expectFusion(Fuseable.ASYNC)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscarded(1) //publishOn also might discard the rest
                            .hasDiscardedElementsMatching(list -> !list.contains(0))
			    .hasDiscardedElementsSatisfying(list -> assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
	}

	@Test
	public void discardPollAsyncPredicateMiss() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //range uses tryOnNext, so let's use just instead
		                        .publishOn(Schedulers.newSingle("discardPollAsync"))
		                        .filter(i -> i % 2 == 0)
		)
		            .expectFusion(Fuseable.ASYNC)
		            .expectNextCount(5)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 3, 5, 7, 9);
	}

	@Test
	public void discardPollSyncPredicateFail() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //range uses tryOnNext, so let's use just instead
		                        .filter(i -> { throw new IllegalStateException("boom"); })
		)
		            .expectFusion(Fuseable.SYNC)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}

	@Test
	public void discardPollSyncPredicateMiss() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //range uses tryOnNext, so let's use just instead
		                        .filter(i -> i % 2 == 0)
		)
		            .expectFusion(Fuseable.SYNC)
		            .expectNextCount(5)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 3, 5, 7, 9)
                            .hasDiscardedElementsMatching(list -> list.stream().allMatch(i -> (int)i % 2 != 0))
                            .hasDiscardedElementsSatisfying(list -> assertThat(list).containsExactly(1, 3, 5, 7, 9));
	}

	@Test
	public void discardConditionalOnNextPredicateFail() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //range uses tryOnNext, so let's use just instead
		                        .filter(i -> { throw new IllegalStateException("boom"); })
		                        .filter(i -> true)
		)
		            .expectFusion(Fuseable.NONE)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}

	@Test
	public void discardConditionalOnNextPredicateMiss() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //range uses tryOnNext, so let's use just instead
		                        .filter(i -> i % 2 == 0)
		                        .filter(i -> true)
		)
		            .expectFusion(Fuseable.NONE)
		            .expectNextCount(5)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 3, 5, 7, 9)
                            .hasDiscardedElementsMatching(list -> list.stream().allMatch(i -> (int)i % 2 != 0))
                            .hasDiscardedElementsSatisfying(list -> assertThat(list).containsExactly(1, 3, 5, 7, 9));
	}

	@Test
	public void discardConditionalTryOnNextPredicateFail() {
		StepVerifier.create(Flux.range(1, 10) //range uses tryOnNext
		                        .filter(i -> { throw new IllegalStateException("boom"); })
		                        .filter(i -> true)
		)
		            .expectFusion(Fuseable.NONE)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}

	@Test
	public void discardConditionalTryOnNextPredicateMiss() {
		StepVerifier.create(Flux.range(1, 10) //range uses tryOnNext
		                         .filter(i -> i % 2 == 0)
		                         .filter(i -> true)
		)
		             .expectFusion(Fuseable.NONE)
		             .expectNextCount(5)
		             .expectComplete()
		             .verifyThenAssertThat()
		             .hasDiscardedExactly(1, 3, 5, 7, 9)
                             .hasDiscardedElementsMatching(list -> list.stream().allMatch(i -> (int)i % 2 != 0))
                             .hasDiscardedElementsSatisfying(list -> assertThat(list).containsExactly(1, 3, 5, 7, 9));
	}

	@Test
	public void discardConditionalPollAsyncPredicateFail() {
		StepVerifier.create(Flux.range(1, 10) //range uses tryOnNext, so let's use just instead
		                        .publishOn(Schedulers.newSingle("discardPollAsync"))
		                        .filter(i -> { throw new IllegalStateException("boom"); })
		                        .filter(i -> true)
		)
		            .expectFusion(Fuseable.ASYNC)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscarded(1) //publishOn also discards the rest
			    .hasDiscardedElementsMatching(list -> !list.contains(0))
			    .hasDiscardedElementsSatisfying(list -> assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
	}

	@Test
	public void discardConditionalPollAsyncPredicateMiss() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //range uses tryOnNext, so let's use just instead
		                        .publishOn(Schedulers.newSingle("discardPollAsync"))
		                        .filter(i -> i % 2 == 0)
		                        .filter(i -> true)
		)
		            .expectFusion(Fuseable.ASYNC)
		            .expectNextCount(5)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 3, 5, 7, 9)
                            .hasDiscardedElementsMatching(list -> list.stream().allMatch(i -> (int)i % 2 != 0))
                            .hasDiscardedElementsSatisfying(list -> assertThat(list).containsExactly(1, 3, 5, 7, 9));
	}

	@Test
	public void discardConditionalPollSyncPredicateFail() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //range uses tryOnNext, so let's use just instead
		                        .filter(i -> { throw new IllegalStateException("boom"); })
		                        .filter(i -> true)
		)
		            .expectFusion(Fuseable.SYNC)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}

	@Test
	public void discardConditionalPollSyncPredicateMiss() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //range uses tryOnNext, so let's use just instead
		                        .filter(i -> i % 2 == 0)
		                        .filter(i -> true)
		)
		            .expectFusion(Fuseable.SYNC)
		            .expectNextCount(5)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 3, 5, 7, 9)
                            .hasDiscardedElementsMatching(list -> list.stream().allMatch(i -> (int)i % 2 != 0))
                            .hasDiscardedElementsSatisfying(list -> assertThat(list).containsExactly(1, 3, 5, 7, 9));
	}

}
