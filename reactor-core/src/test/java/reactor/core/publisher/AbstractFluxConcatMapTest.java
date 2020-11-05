/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
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
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Publisher;

import reactor.core.CorePublisher;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

@Timeout(5)
public abstract class AbstractFluxConcatMapTest extends FluxOperatorTest<String, String> {

	abstract int implicitPrefetchValue();

	@Override
	protected final Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.shouldHitDropNextHookAfterTerminate(false)
		                     .shouldHitDropErrorHookAfterTerminate(false)
		                     .prefetch(implicitPrefetchValue() == 0 ? -1 : implicitPrefetchValue());
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.concatMap(Flux::just, implicitPrefetchValue())),

				scenario(f -> f.concatMap(d -> Flux.just(d).hide(), implicitPrefetchValue())),

				scenario(f -> f.concatMap(d -> Flux.empty(), implicitPrefetchValue()))
						.receiverEmpty(),

				scenario(f -> f.concatMapDelayError(Flux::just, implicitPrefetchValue())),

				scenario(f -> f.concatMapDelayError(d -> Flux.just(d).hide(), implicitPrefetchValue())),

				scenario(f -> f.concatMapDelayError(d -> Flux.empty(), implicitPrefetchValue()))
						.receiverEmpty(),

				//scenarios with fromCallable(() -> null)
				scenario(f -> f.concatMap(d -> Mono.fromCallable(() -> null), implicitPrefetchValue()))
						.receiverEmpty(),

				scenario(f -> f.concatMapDelayError(d -> Mono.fromCallable(() -> null), implicitPrefetchValue()))
						.shouldHitDropErrorHookAfterTerminate(true)
						.receiverEmpty()
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(f -> f.concatMap(Flux::just, implicitPrefetchValue())),

				scenario(f -> f.concatMapDelayError(Flux::just, implicitPrefetchValue()))
						.shouldHitDropErrorHookAfterTerminate(true)
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.concatMap(d -> {
					throw exception();
				}, implicitPrefetchValue())),

				scenario(f -> f.concatMap(d -> null, implicitPrefetchValue())),

				scenario(f -> f.concatMapDelayError(d -> {
					throw exception();
				}, implicitPrefetchValue()))
						.shouldHitDropErrorHookAfterTerminate(true),

				scenario(f -> f.concatMapDelayError(d -> null, implicitPrefetchValue()))
						.shouldHitDropErrorHookAfterTerminate(true)
		);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .concatMap(v -> Flux.range(v, 2), implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .hide()
		    .concatMap(v -> Flux.range(v, 2), implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .concatMapDelayError(v -> Flux.range(v, 2), implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBoundary2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .hide()
		    .concatMapDelayError(v -> Flux.range(v, 2), implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRun() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000)
		    .concatMap(v -> Flux.range(v, 1000), implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRunJust() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000_000)
		    .concatMap(v -> Flux.just(v), implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRun2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000)
		    .hide()
		    .concatMap(v -> Flux.range(v, 1000), implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRunBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000)
		    .concatMapDelayError(v -> Flux.range(v, 1000), implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRunJustBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000_000)
		    .concatMapDelayError(v -> Flux.just(v), implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRunBoundary2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000)
		    .hide()
		    .concatMapDelayError(v -> Flux.range(v, 1000), implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	//see https://github.com/reactor/reactor-core/issues/1302
	@Test
	public void boundaryFusion() {
		Flux.range(1, 10000)
		    .publishOn(Schedulers.single())
		    .map(t -> Thread.currentThread().getName().contains("single-") ? "single" : ("BAD-" + t + Thread.currentThread().getName()))
		    .concatMap(Flux::just, implicitPrefetchValue())
		    .publishOn(Schedulers.boundedElastic())
		    .distinct()
		    .as(StepVerifier::create)
		    .expectFusion()
		    .expectNext("single")
		    .expectComplete()
		    .verify(Duration.ofSeconds(5));
	}

	//see https://github.com/reactor/reactor-core/issues/1302
	@Test
	public void boundaryFusionDelayError() {
		Flux.range(1, 10000)
		    .publishOn(Schedulers.single())
		    .map(t -> Thread.currentThread().getName().contains("single-") ? "single" : ("BAD-" + t + Thread.currentThread().getName()))
		    .concatMapDelayError(Flux::just, implicitPrefetchValue())
		    .publishOn(Schedulers.boundedElastic())
		    .distinct()
		    .as(StepVerifier::create)
		    .expectFusion()
		    .expectNext("single")
		    .expectComplete()
		    .verify(Duration.ofSeconds(5));
	}

	@Test
	public void singleSubscriberOnlyBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		source.asFlux().concatMapDelayError(v -> v == 1 ? source1.asFlux() : source2.asFlux(), implicitPrefetchValue())
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.emitNext(1, FAIL_FAST);

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isPositive();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();

		source1.emitNext(1, FAIL_FAST);
		source2.emitNext(10, FAIL_FAST);

		source1.emitComplete(FAIL_FAST);
		source.emitNext(2, FAIL_FAST);
		source.emitComplete(FAIL_FAST);

		source2.emitNext(2, FAIL_FAST);
		source2.emitComplete(FAIL_FAST);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isZero();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();
	}

	@Test
	public void mainErrorsImmediate() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		source.asFlux().concatMap(v -> v == 1 ? source1.asFlux() : source2.asFlux(), implicitPrefetchValue())
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.emitNext(1, FAIL_FAST);

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isPositive();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();

		source1.emitNext(1, FAIL_FAST);

		source.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isZero();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();
	}

	@Test
	public void mainErrorsBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		source.asFlux().concatMapDelayError(v -> v == 1 ? source1.asFlux() : source2.asFlux(), implicitPrefetchValue())
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.emitNext(1, FAIL_FAST);

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isPositive();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();

		source1.emitNext(1, FAIL_FAST);

		source.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		source1.emitNext(2, FAIL_FAST);
		source1.emitComplete(FAIL_FAST);

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isZero();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();
	}

	@Test
	public void innerErrorsImmediate() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		source.asFlux().concatMap(v -> v == 1 ? source1.asFlux() : source2.asFlux(), implicitPrefetchValue())
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.emitNext(1, FAIL_FAST);

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isPositive();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();

		source1.emitNext(1, FAIL_FAST);

		source1.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isZero();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();
	}

	@Test
	public void syncFusionMapToNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .map(v -> v == 2 ? null : v)
		    .concatMap(Flux::just, implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void syncFusionMapToNullFilter() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .map(v -> v == 2 ? null : v)
		    .filter(v -> true)
		    .concatMap(Flux::just, implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void asyncFusionMapToNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> up = Sinks.many().unicast().onBackpressureBuffer(Queues.<Integer>get(2).get());
		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		up.asFlux()
		  .map(v -> v == 2 ? null : v)
		  .concatMap(Flux::just, implicitPrefetchValue())
		  .subscribe(ts);

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void asyncFusionMapToNullFilter() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> up =
				Sinks.many().unicast().onBackpressureBuffer(Queues.<Integer>get(2).get());
		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		up.asFlux()
		  .map(v -> v == 2 ? null : v)
		  .filter(v -> true)
		  .concatMap(Flux::just, implicitPrefetchValue())
		  .subscribe(ts);

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void scalarAndRangeBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		@SuppressWarnings("unchecked") Publisher<Integer>[] sources =
				new Publisher[]{Flux.just(1), Flux.range(2, 3)};

		Flux.range(0, 2)
		    .concatMap(v -> sources[v], implicitPrefetchValue())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void publisherOfPublisherDelayError2() {
		StepVerifier.create(Flux.just(Flux.just(1, 2)
		                                  .concatWith(Flux.error(new Exception("test"))),
				Flux.just(3, 4))
		                        .concatMap(f -> f, implicitPrefetchValue()))
		            .expectNext(1, 2)
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void concatDelayErrorWithFluxError() {
		StepVerifier.create(
				Flux.concatDelayError(
						Flux.just(
								Flux.just(1, 2),
								Flux.error(new Exception("test")),
								Flux.just(3, 4)), implicitPrefetchValue()))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void concatDelayErrorWithMonoError() {
		Flux<CorePublisher<Integer>> sources = Flux.just(
				Flux.just(1, 2),
				Mono.error(new Exception("test")),
				Flux.just(3, 4)
		);
		StepVerifier.create(Flux.concatDelayError(sources, implicitPrefetchValue()))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	@Test
	public void errorModeContinueNullPublisher() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.<Integer>concatMap(f -> null, implicitPrefetchValue())
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
		            .expectNoFusionSupport()
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped(1, 2)
		            .hasDroppedErrors(2);
	}

	@Test
	public void errorModeContinueInternalError() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.concatMap(f -> {
					if(f == 1){
						return Mono.error(new NullPointerException());
					}
					else {
						return Mono.just(f);
					}
				}, implicitPrefetchValue())
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
		            .expectNoFusionSupport()
		            .expectNext(2)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped(1)
		            .hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueInternalErrorHidden() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.concatMap(f -> {
					if(f == 1){
						return Mono.<Integer>error(new NullPointerException()).hide();
					}
					else {
						return Mono.just(f);
					}
				}, implicitPrefetchValue())
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
		            .expectNoFusionSupport()
		            .expectNext(2)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueWithCallable() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.concatMap(f -> Mono.<Integer>fromRunnable(() -> {
					if(f == 1) {
						throw new ArithmeticException("boom");
					}
				}), implicitPrefetchValue())
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
		            .expectNoFusionSupport()
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped(1)
		            .hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueDelayErrors() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.concatMapDelayError(f -> {
					if(f == 1){
						return Mono.<Integer>error(new NullPointerException()).hide();
					}
					else {
						return Mono.just(f);
					}
				}, implicitPrefetchValue())
				.onErrorContinue(OnNextFailureStrategyTest::drop);


		StepVerifier.create(test)
		            .expectNoFusionSupport()
		            .expectNext(2)
		            .expectComplete()
		            .verifyThenAssertThat()
		            // When inner is not a Callable error value is not available.
		            .hasNotDroppedElements()
		            .hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueDelayErrorsWithCallable() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.concatMapDelayError(f -> {
					if(f == 1){
						return Mono.<Integer>error(new NullPointerException());
					}
					else {
						return Mono.just(f);
					}
				}, implicitPrefetchValue())
				.onErrorContinue(OnNextFailureStrategyTest::drop);


		StepVerifier.create(test)
		            .expectNoFusionSupport()
		            .expectNext(2)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped(1)
		            .hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueInternalErrorStopStrategy() {
		Flux<Integer> test = Flux
				.just(0, 1)
				.hide()
				.concatMap(f ->  Flux.range(f, 1).map(i -> 1/i).onErrorStop(), implicitPrefetchValue())
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueInternalErrorStopStrategyAsync() {
		Flux<Integer> test = Flux
				.just(0, 1)
				.hide()
				.concatMap(f ->  Flux.range(f, 1).publishOn(Schedulers.parallel()).map(i -> 1 / i).onErrorStop(), implicitPrefetchValue())
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueInternalErrorMono() {
		Flux<Integer> test = Flux
				.just(0, 1)
				.hide()
				.concatMap(f ->  Mono.just(f).map(i -> 1/i), implicitPrefetchValue())
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped(0)
		            .hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueInternalErrorMonoAsync() {
		Flux<Integer> test = Flux
				.just(0, 1)
				.hide()
				.concatMap(f ->  Mono.just(f).publishOn(Schedulers.parallel()).map(i -> 1/i), implicitPrefetchValue())
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped(0)
		            .hasDroppedErrors(1);
	}

	@Test
	public void discardOnDrainMapperError() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatMap(i -> { throw new IllegalStateException("boom"); }, implicitPrefetchValue()))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}

	@Test
	public void discardDelayedOnDrainMapperError() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatMapDelayError(i -> { throw new IllegalStateException("boom"); }, implicitPrefetchValue()))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardDelayedInnerOnDrainMapperError() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatMapDelayError(
		                        		i -> {
		                        			if (i == 2) {
						                        throw new IllegalStateException("boom");
					                        }
					                        return Mono.just(i);
	                                    },
				                        false,
				                        implicitPrefetchValue()
		                        ))
		            .expectNext(1)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(2);
	}

	@Test
	public void publisherOfPublisherDelayEnd() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2),
				Flux.just(3, 4)), false, implicitPrefetchValue()))
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}

	@Test
	public void publisherOfPublisherDelayEnd2() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2),
				Flux.just(3, 4)), true, implicitPrefetchValue()))
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}

	@Test
	public void publisherOfPublisherDelayEndNot3() {
		StepVerifier.create(Flux.just(Flux.just(1, 2)
		                                  .concatWith(Flux.error(new Exception("test"))),
				Flux.just(3, 4))
		                        .concatMapDelayError(f -> f, false, implicitPrefetchValue()))
		            .expectNext(1, 2)
		            .verifyErrorMessage("test");
	}

	@Test
	public void publisherOfPublisherDelayEndError() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2)
		                                                        .concatWith(Flux.error(new Exception(
				                                                        "test"))),
				Flux.just(3, 4)), false, implicitPrefetchValue()))
		            .expectNext(1, 2)
		            .verifyErrorMessage("test");
	}

	@Test
	public void publisherOfPublisherDelayEndError2() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2)
		                                                        .concatWith(Flux.error(new Exception(
				                                                        "test"))),
				Flux.just(3, 4)), true, implicitPrefetchValue()))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	@Test
	public void publisherOfPublisherDelayEnd3() {
		StepVerifier.create(Flux.just(Flux.just(1, 2)
		                                  .concatWith(Flux.error(new Exception("test"))),
				Flux.just(3, 4))
		                        .concatMapDelayError(f -> f, true, implicitPrefetchValue()))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	@Test
	public void innerErrorsBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		//gh-1101: default changed from BOUNDARY to END
		source.asFlux().concatMapDelayError(v -> v == 1 ? source1.asFlux() : source2.asFlux(), false, implicitPrefetchValue())
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.emitNext(1, FAIL_FAST);

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isPositive();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();

		source1.emitNext(1, FAIL_FAST);

		source1.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isZero();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();
	}

	@Test
	public void innerErrorsEnd() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		source.asFlux().concatMapDelayError(v -> v == 1 ? source1.asFlux() : source2.asFlux(), true, implicitPrefetchValue())
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.emitNext(1, FAIL_FAST);

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isPositive();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();

		source1.emitNext(1, FAIL_FAST);

		source1.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		source.emitNext(2, FAIL_FAST);

		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isPositive();

		source2.emitNext(2, FAIL_FAST);
		source2.emitComplete(FAIL_FAST);

		source.emitComplete(FAIL_FAST);

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isZero();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();
	}

	@Test
	public void publisherOfPublisherDelay() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2),
				Flux.just(3, 4)), implicitPrefetchValue()))
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}

	@Test
	public void publisherOfPublisherDelayError() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2).concatWith(Flux.error(new Exception("test"))),
				Flux.just(3, 4)), implicitPrefetchValue()))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void concatMapDelayErrorWithFluxError() {
		StepVerifier.create(
				Flux.just(
						Flux.just(1, 2),
						Flux.<Integer>error(new Exception("test")),
						Flux.just(3, 4))
				    .concatMapDelayError(f -> f, true, implicitPrefetchValue()))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void concatMapDelayErrorWithMonoError() {
		StepVerifier.create(
				Flux.just(
						Flux.just(1, 2),
						Mono.<Integer>error(new Exception("test")),
						Flux.just(3, 4))
				    .concatMapDelayError(f -> f, true, implicitPrefetchValue()))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	@Test
	public void publisherOfPublisher() {
		StepVerifier.create(Flux.concat(Flux.just(Flux.just(1, 2), Flux.just(3, 4)), implicitPrefetchValue()))
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}
}
