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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxMapTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ANY);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.map(d -> {
					throw exception();
				})),

				scenario(f -> f.map(d -> null))

		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.map(d -> d))
		);
	}

	Flux<Integer> just = Flux.just(1);

	@Test
	public void nullSource() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxMap<Integer, Integer>(null, v -> v);
		});
	}

	@Test
	public void nullMapper() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			just.map(null);
		});
	}

	@Test
	public void simpleMapping() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		just.map(v -> v + 1)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertValues(2)
		  .assertComplete();
	}

	@Test
	public void simpleMappingBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		just.map(v -> v + 1)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(1);

		ts.assertNoError()
		  .assertValues(2)
		  .assertComplete();
	}

	@Test
	public void mapperThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		just.map(v -> {
			throw new RuntimeException("forced failure");
		})
		    .subscribe(ts);

		ts.assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNoValues()
		  .assertNotComplete();
	}

	@Test
	public void mapperReturnsNull() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		just.map(v -> null)
		    .subscribe(ts);

		ts.assertError(NullPointerException.class)
		  .assertNoValues()
		  .assertNotComplete();
	}

	@Test
	public void syncFusion() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .map(v -> v + 1)
		    .subscribe(ts);

		ts.assertValues(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void asyncFusion() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Sinks.Many<Integer> up = Sinks.unsafe().many().unicast().onBackpressureBuffer(new ConcurrentLinkedQueue<>());

		up.asFlux()
		  .map(v -> v + 1)
		  .subscribe(ts);

		for (int i = 1; i < 11; i++) {
			up.emitNext(i, FAIL_FAST);
		}
		up.emitComplete(FAIL_FAST);

		ts.assertValues(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
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
		    .flatMap(w -> up.asFlux().map(v -> v + 1))
		    .subscribe(ts);

		up.emitNext(1, FAIL_FAST);

		ts.assertValues(2)
		  .assertNoError()
		  .assertNotComplete();

		up.emitComplete(FAIL_FAST);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void mapFilter() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(0, 1_000_000)
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void mapFilterBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		Flux.range(0, 1_000_000)
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(250_000)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void hiddenMapFilter() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(0, 1_000_000)
		    .hide()
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void hiddenMapFilterBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		Flux.range(0, 1_000_000)
		    .hide()
		    .map(v -> v + 1)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(250_000)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void hiddenMapHiddenFilterBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		Flux.range(0, 1_000_000)
		    .hide()
		    .map(v -> v + 1)
		    .hide()
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(250_000)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(250_000);

		ts.assertValueCount(500_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxMap<Integer, String> test = new FluxMap<>(parent, v -> v.toString());

	    assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

    @Test
    public void scanSubscriber() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxMap.MapSubscriber<Integer, String> test = new FluxMap.MapSubscriber<>(actual, i -> String.valueOf(i));
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
        FluxMap.MapConditionalSubscriber<Integer, String> test = new FluxMap.MapConditionalSubscriber<>(actual, i -> String.valueOf(i));
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
	public void scanFuseableOperator(){
		FluxMapFuseable<Integer, String> test = new FluxMapFuseable<>(Flux.just(1), v -> v.toString());

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

    @Test
    public void scanFuseableSubscriber() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxMapFuseable.MapFuseableSubscriber<Integer, String> test =
        		new FluxMapFuseable.MapFuseableSubscriber<>(actual, i -> String.valueOf(i));
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
    public void scanFuseableConditionalSubscriber() {
	    @SuppressWarnings("unchecked")
	    Fuseable.ConditionalSubscriber<String> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
        FluxMapFuseable.MapFuseableConditionalSubscriber<Integer, String> test =
        		new FluxMapFuseable.MapFuseableConditionalSubscriber<>(actual, i -> String.valueOf(i));
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
	public void mapFailureStrategyResume() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);

		try {
			AtomicLong r = new AtomicLong();
			StepVerifier.create(Flux.just(0, 2)
			                        .doOnRequest(r::addAndGet)
			                        .hide()
			                        .map(i -> 4 / i), 1)
			            .expectNoFusionSupport()
			            .expectNext(2)
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
	public void mapFailureStrategyCustomResume() {
	    List<Object> valuesDropped = new ArrayList<>();
	    List<Throwable> errorsDropped = new ArrayList<>();
	    Hooks.onNextError(OnNextFailureStrategy.resume(
				(t, s) -> {
					errorsDropped.add(t);
					valuesDropped.add(s);
				}));

		try {
			AtomicLong r = new AtomicLong();
			StepVerifier.create(Flux.just(0, 2)
			                        .doOnRequest(r::addAndGet)
			                        .hide()
			                        .map(i -> 4 / i), 1)
			            .expectNoFusionSupport()
			            .expectNext(2)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasNotDroppedElements()
			            .hasNotDroppedErrors();

			assertThat(valuesDropped).containsExactly(0);
			assertThat(errorsDropped)
					.hasSize(1)
					.allSatisfy(e -> assertThat(e)
							.isInstanceOf(ArithmeticException.class)
							.hasMessage("/ by zero"));
			assertThat(r.get()).as("amount requested").isEqualTo(2L);

		}
		finally {
			Hooks.resetOnNextError();
		}
    }

	@Test
	public void mapTryOnNextFailureStrategyResume() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);

		try {
			StepVerifier.create(Flux.range(0, 2)
			                        //distinctUntilChanged is conditional but not fuseable
			                        .distinctUntilChanged()
			                        .map(i -> 4 / i)
			                        .filter(i -> true))
			            .expectNoFusionSupport()
			            .expectNext(4)
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
	public void mapFuseableFailureStrategyResume() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);

		try {
			AtomicLong r = new AtomicLong();
			StepVerifier.create(Flux.just(0, 2)
			                        .doOnRequest(r::addAndGet)
			                        .map(i -> 4 / i), 1)
			            .expectFusion()
			            .expectNext(2)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDropped(0)
			            .hasDroppedErrorWithMessage("/ by zero");

			assertThat(r.get()).as("async/no request").isEqualTo(0L);
		}
		finally {
			Hooks.resetOnNextError();
		}
    }

	@Test
	public void mapFuseableTryOnNextFailureStrategyResume() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);

		try {
			StepVerifier.create(Flux.range(0, 2)
			                        .map(i -> 4 / i)
			                        .filter(i -> true))
			            .expectFusion()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorWithMessage("/ by zero");
		}
		finally {
			Hooks.resetOnNextError();
		}
	}
}
