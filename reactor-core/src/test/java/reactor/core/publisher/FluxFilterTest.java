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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.FluxFilter.FilterConditionalSubscriber;
import reactor.core.publisher.FluxFilter.FilterSubscriber;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxFilterTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ANY);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.filter(d -> {
					throw exception();
				}))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.filter(d -> true)),

				scenario(f -> f.filter(d -> false))
					.receiverEmpty()
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return  Arrays.asList(
				scenario(f -> f.filter(d -> true))
		);
	}

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxFilter<Integer>(null, e -> true);
		});
	}

	@Test
	public void predicateNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never()
					.filter(null);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .filter(v -> v % 2 == 0)
		    .subscribe(ts);

		ts.assertValues(2, 4, 6, 8, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressuredRange() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(2);

		Flux.range(1, 10)
		    .filter(v -> v % 2 == 0)
		    .subscribe(ts);

		ts.assertValues(2, 4)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(2, 4, 6, 8, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressuredArray() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(2);

		Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		    .filter(v -> v % 2 == 0)
		    .subscribe(ts);

		ts.assertValues(2, 4)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(2, 4, 6, 8, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressuredIterable() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(2);

		Flux.fromIterable(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
		    .filter(v -> v % 2 == 0)
		    .subscribe(ts);

		ts.assertValues(2, 4)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(2, 4, 6, 8, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void predicateThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(2);

		Flux.range(1, 10)
		    .filter(v -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void syncFusion() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertValues(2, 4, 6, 8, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void asyncFusion() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Sinks.Many<Integer> up =
				Sinks.unsafe().many().unicast().onBackpressureBuffer(new ConcurrentLinkedQueue<>());

		up.asFlux()
		  .filter(v -> (v & 1) == 0)
		  .subscribe(ts);

		for (int i = 1; i < 11; i++) {
			up.emitNext(i, FAIL_FAST);
		}
		up.emitComplete(FAIL_FAST);

		ts.assertValues(2, 4, 6, 8, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void asyncFusionBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(1);

		Sinks.Many<Integer> up =
				Sinks.unsafe().many().unicast().onBackpressureBuffer(new ConcurrentLinkedQueue<>());

		Flux.just(1)
		    .hide()
		    .flatMap(w -> up.asFlux().filter(v -> (v & 1) == 0))
		    .subscribe(ts);

		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);

		ts.assertValues(2)
		  .assertNoError()
		  .assertNotComplete();

		up.emitComplete(FAIL_FAST);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void asyncFusionBackpressured2() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(1);

		Sinks.Many<Integer> up =
				Sinks.unsafe().many().unicast().onBackpressureBuffer(new ConcurrentLinkedQueue<>());

		Flux.just(1)
		    .hide()
		    .flatMap(w -> up.asFlux().filter(v -> (v & 1) == 0), false, 1, 1)
		    .subscribe(ts);

		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);

		ts.assertValues(2)
		  .assertNoError()
		  .assertNotComplete();

		up.emitComplete(FAIL_FAST);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxFilter<Integer> test = new FluxFilter<>(parent, e -> true);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

    @Test
    public void scanSubscriber() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FilterSubscriber<String> test = new FilterSubscriber<>(actual, t -> true);
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
        FilterConditionalSubscriber<String> test = new FilterConditionalSubscriber<>(actual, t -> true);
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
	public void failureStrategyResume() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			AtomicLong r = new AtomicLong();
			StepVerifier.create(Flux.range(0, 2)
			                        .doOnRequest(r::addAndGet)
			                        .hide()
			                        .filter(i -> 4 / i == 4), 1)
			            .expectNoFusionSupport()
			            .expectNext(1)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorWithMessage("/ by zero");

			assertThat(r.get()).as("amount requested").isEqualTo(2L);
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeTryOnNext() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			StepVerifier.create(Flux.range(0, 2)
			                        .distinctUntilChanged()
			                        .filter(i -> 4 / i == 4))
			            .expectNoFusionSupport()
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
	public void failureStrategyResumeConditional() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			AtomicLong r = new AtomicLong();
			StepVerifier.create(Flux.range(0, 2)
			                        .doOnRequest(r::addAndGet)
			                        .hide()
			                        .filter(i -> 4 / i == 4)
			                        .filter(i -> true), 1)
			            .expectNoFusionSupport()
			            .expectNext(1)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorWithMessage("/ by zero");

			assertThat(r.get()).as("amount requested").isEqualTo(2L);
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeConditionalTryOnNext() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			StepVerifier.create(Flux.range(0, 2)
			                        .distinctUntilChanged()
			                        .filter(i -> 4 / i == 4)
			                        .filter(i -> true))
			            .expectNoFusionSupport()
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
		StepVerifier.create(Flux.range(1, 10)
		                        .hide() //hide both avoid the fuseable AND tryOnNext usage
		                        .filter(i -> { throw new IllegalStateException("boom"); })
		)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
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
		CoreSubscriber<Integer> actual = new AssertSubscriber<>(
				Context.of(Hooks.KEY_ON_DISCARD, (Consumer<?>) discarded::add));
		FilterSubscriber<Integer> subscriber =
				new FilterSubscriber<>(actual, i -> { throw new IllegalStateException("boom"); });
		subscriber.onSubscribe(Operators.emptySubscription());

		subscriber.tryOnNext(1);

		assertThat(discarded).containsExactly(1);
	}

	@Test
	public void discardTryOnNextPredicateMiss() {
		List<Object> discarded = new ArrayList<>();
		CoreSubscriber<Integer> actual = new AssertSubscriber<>(
				Context.of(Hooks.KEY_ON_DISCARD, (Consumer<?>) discarded::add));
		FilterSubscriber<Integer> subscriber =
				new FilterSubscriber<>(actual, i -> i % 2 == 0);
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
						Context.of(Hooks.KEY_ON_DISCARD, (Consumer<?>) discarded::add));

		FilterConditionalSubscriber<Integer> subscriber =
				new FilterConditionalSubscriber<>(actual, i -> {
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
				Context.of(Hooks.KEY_ON_DISCARD, (Consumer<?>) discarded::add));

		FilterConditionalSubscriber<Integer> subscriber =
				new FilterConditionalSubscriber<>(actual, i -> i % 2 == 0);
		subscriber.onSubscribe(Operators.emptySubscription());

		subscriber.tryOnNext(1);
		subscriber.tryOnNext(2);

		assertThat(discarded).containsExactly(1);
	}
}
