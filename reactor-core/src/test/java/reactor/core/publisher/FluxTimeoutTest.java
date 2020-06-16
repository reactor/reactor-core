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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxTimeoutTest {

	@Test
	public void noTimeout() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(), v -> Flux.never())
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void immediateTimeout() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.empty(), v -> Flux.never())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(TimeoutException.class);
	}

	@Test
	public void firstElemenetImmediateTimeout() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(), v -> Flux.empty())
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(TimeoutException.class);
	}

	//Fail
	//@Test
	public void immediateTimeoutResume() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.empty(), v -> Flux.never(), Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void firstElemenetImmediateResume() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(), v -> Flux.empty(), Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertValues(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oldTimeoutHasNoEffect() {
		FluxProcessor<Integer, Integer> source = Processors.more().multicastNoBackpressure();

		FluxProcessor<Integer, Integer> tp = Processors.more().multicastNoBackpressure();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		source.timeout(tp, v -> Flux.never(), Flux.range(1, 10))
		      .subscribe(ts);

		source.onNext(0);

		tp.onNext(1);

		source.onComplete();

		Assert.assertFalse("Timeout has subscribers?", tp.hasDownstreams());

		ts.assertValues(0)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oldTimeoutCompleteHasNoEffect() {
		FluxProcessor<Integer, Integer> source = Processors.more().multicastNoBackpressure();

		FluxProcessor<Integer, Integer> tp = Processors.more().multicastNoBackpressure();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		source.timeout(tp, v -> Flux.never(), Flux.range(1, 10))
		      .subscribe(ts);

		source.onNext(0);

		tp.onComplete();

		source.onComplete();

		Assert.assertFalse("Timeout has subscribers?", tp.hasDownstreams());

		ts.assertValues(0)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oldTimeoutErrorHasNoEffect() {
		FluxProcessor<Integer, Integer> source = Processors.more().multicastNoBackpressure();

		FluxProcessor<Integer, Integer> tp = Processors.more().multicastNoBackpressure();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		source.timeout(tp, v -> Flux.never(), Flux.range(1, 10))
		      .subscribe(ts);

		source.onNext(0);

		tp.onError(new RuntimeException("forced failure"));

		source.onComplete();

		Assert.assertFalse("Timeout has subscribers?", tp.hasDownstreams());

		ts.assertValues(0)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void itemTimeoutThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(), v -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void itemTimeoutReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(), v -> null)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void firstTimeoutError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.error(new RuntimeException("forced " + "failure")),
				    v -> Flux.never())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void itemTimeoutError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(),
				    v -> Flux.error(new RuntimeException("forced failure")))
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void timeoutRequested() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		FluxProcessor<Integer, Integer> source = Processors.more().multicastNoBackpressure();

		FluxProcessor<Integer, Integer> tp = Processors.more().multicastNoBackpressure();

		source.timeout(tp, v -> tp)
		      .subscribe(ts);

		tp.onNext(1);

		source.onNext(2);
		source.onComplete();

		ts.assertNoValues()
		  .assertError(TimeoutException.class)
		  .assertNotComplete();
	}

	Flux<Integer> scenario_timeoutCanBeBoundWithCallback() {
		return Flux.<Integer>never().timeout(Duration.ofMillis(500), Flux.just(-5));
	}

	@Test
	public void timeoutCanBeBoundWithCallback() {
		StepVerifier.withVirtualTime(this::scenario_timeoutCanBeBoundWithCallback)
		            .thenAwait(Duration.ofMillis(500))
		            .expectNext(-5)
		            .verifyComplete();
	}

	Flux<?> scenario_timeoutThrown() {
		return Flux.never()
		           .timeout(Duration.ofMillis(500));
	}

	@Test
	public void fluxPropagatesErrorUsingAwait() {
		StepVerifier.withVirtualTime(this::scenario_timeoutThrown)
		            .thenAwait(Duration.ofMillis(500))
		            .verifyError(TimeoutException.class);
	}

	Flux<Integer> scenario_timeoutCanBeBoundWithCallback2() {
		return Flux.<Integer>never().timeout(Duration.ofMillis(500), Flux.just(-5));
	}

	@Test
	public void timeoutCanBeBoundWithCallback2() {
		StepVerifier.withVirtualTime(this::scenario_timeoutCanBeBoundWithCallback2)
		            .thenAwait(Duration.ofMillis(500))
		            .expectNext(-5)
		            .verifyComplete();
	}

	Flux<?> scenario_timeoutThrown2() {
		return Flux.never()
		           .timeout(Duration.ofMillis(500));
	}

	@Test
	public void fluxPropagatesErrorUsingAwait2() {
		StepVerifier.withVirtualTime(this::scenario_timeoutThrown2)
		            .thenAwait(Duration.ofMillis(500))
		            .verifyError(TimeoutException.class);
	}

	Flux<?> scenario_timeoutThrown3() {
		return Flux.never()
		           .timeout(Duration.ofMillis(500), Schedulers.parallel());
	}

	@Test
	public void fluxPropagatesErrorUsingAwait3() {
		StepVerifier.withVirtualTime(this::scenario_timeoutThrown3)
		            .thenAwait(Duration.ofMillis(500))
		            .verifyError(TimeoutException.class);
	}

	@Test
	public void fluxTimeoutOther() {
		StepVerifier.create(Flux.never().timeout(Flux.just(1)))
		            .thenAwait(Duration.ofMillis(500))
		            .verifyError(TimeoutException.class);
	}

	//see https://github.com/reactor/reactor-core/issues/744
	@Test
	public void timeoutDropWhenNoCancelWithoutFallback() {
		for (int i = 0; i < 50; i++) {
			StepVerifier.withVirtualTime(
					() -> Flux.just("cat")
					          .delaySubscription(Duration.ofMillis(3))
					          // We cancel on another scheduler that won't do anything to force it to act like
					          // the event is already in flight
					          .cancelOn(Schedulers.fromExecutor(r -> {}))
					          .timeout(Duration.ofMillis(2))
			)
			            .thenAwait(Duration.ofSeconds(5))
			            .expectError(TimeoutException.class)
			            .verify();
		}
	}

	//see https://github.com/reactor/reactor-core/issues/744
	@Test
	public void timeoutDropWhenNoCancelWithFallback() {
		for (int i = 0; i < 50; i++) {
			StepVerifier.withVirtualTime(
					() -> Flux.just("cat")
					          .delaySubscription(Duration.ofMillis(3))
					          // We cancel on another scheduler that won't do anything to force it to act like
					          // the event is already in flight
					          .cancelOn(Schedulers.fromExecutor(r -> {}))
					          .timeout(Duration.ofMillis(2), Flux.just("dog").delayElements(Duration.ofMillis(5)))
			)
			            .thenAwait(Duration.ofSeconds(5))
			            .expectNext("dog")
			            .expectComplete()
			            .verify();
		}
	}

	@Test
	public void timeoutDurationMessageDefault() {
		StepVerifier.withVirtualTime(() -> Flux.never()
		                                       .timeout(Duration.ofHours(1)))
		            .thenAwait(Duration.ofHours(2))
		            .expectErrorMessage("Did not observe any item or terminal signal within " +
				            "3600000ms in 'source(FluxNever)' (and no fallback has been configured)")
		            .verify();
	}

	@Test
	public void timeoutDurationMessageWithName() {
		StepVerifier.withVirtualTime(() -> Flux.never()
				.name("Name")
				.timeout(Duration.ofHours(1)))
				.thenAwait(Duration.ofHours(2))
				.expectErrorMessage("Did not observe any item or terminal signal within " +
						"3600000ms in 'Name' (and no fallback has been configured)")
				.verify();
	}


	@Test
	public void timeoutNotDurationMessageFirstTimeout() {
		StepVerifier.create(Flux.never()
		                        .timeout(Mono.just("immediate")))
		            .expectErrorMessage("Did not observe any item or terminal signal within " +
				            "first signal from a Publisher in 'source(FluxNever)' (and no fallback has been configured)")
		            .verify();
	}

	@Test
	public void timeoutNotDurationMessageSecondTimeout() {
		AtomicBoolean generatorUsed = new AtomicBoolean();
		StepVerifier.create(Flux.concat(Mono.just("foo"), Mono.just("bar").delayElement(Duration.ofMillis(500)))
		                        .timeout(Mono.delay(Duration.ofMillis(100)),
				                        v -> {
					                        generatorUsed.set(true);
					                        return Mono.delay(Duration.ofMillis(100));
				                        }))
		            .expectNext("foo")
		            .expectErrorMessage("Did not observe any item or terminal signal within " +
				            "first signal from a Publisher in 'source(FluxConcatArray)' (and no fallback has been configured)")
		            .verify();

		assertThat(generatorUsed.get()).as("generator used").isTrue();
	}
}
