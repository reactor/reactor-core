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

import org.junit.Test;

import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class MonoTimeoutTest {

	@Test
	public void noTimeout() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .timeout(Mono.never())
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void immediateTimeout() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .timeout(Mono.empty())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(TimeoutException.class);
	}

	@Test
	public void firstTimeoutError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .timeout(Mono.error(new RuntimeException("forced " + "failure")))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void timeoutRequested() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		MonoProcessor<Integer> source = MonoProcessor.create();

		FluxProcessor<Integer, Integer> tp = Processors.more().multicastNoBackpressure();

		source.timeout(tp)
		      .subscribe(ts);

		tp.onNext(1);

		source.onNext(2);
		source.onComplete();

		ts.assertNoValues()
		  .assertError(TimeoutException.class)
		  .assertNotComplete();
	}


	Mono<Integer> scenario_timeoutCanBeBoundWithCallback() {
		return Mono.<Integer>never().timeout(Duration.ofMillis(500), Mono.just(-5));
	}

	@Test
	public void timeoutCanBeBoundWithCallback() {
		StepVerifier.withVirtualTime(this::scenario_timeoutCanBeBoundWithCallback)
		            .thenAwait(Duration.ofMillis(500))
		            .expectNext(-5)
		            .verifyComplete();
	}

	Mono<Integer> scenario_timeoutCanBeBoundWithCallback2() {
		return Mono.<Integer>never().timeout(Mono.delay(Duration.ofMillis(500)), Mono.just
				(-5));
	}

	@Test
	public void timeoutCanBeBoundWithCallback2() {
		StepVerifier.withVirtualTime(this::scenario_timeoutCanBeBoundWithCallback2)
		            .thenAwait(Duration.ofMillis(500))
		            .expectNext(-5)
		            .verifyComplete();
	}

	Mono<?> scenario_timeoutThrown() {
		return Mono.never()
		           .timeout(Duration.ofMillis(500));
	}

	@Test
	public void MonoPropagatesErrorUsingAwait() {
		StepVerifier.withVirtualTime(this::scenario_timeoutThrown)
		            .thenAwait(Duration.ofMillis(500))
		            .verifyError(TimeoutException.class);
	}

	@Test
	public void timeoutDurationMessageDefault() {
		StepVerifier.withVirtualTime(() -> Mono.never()
		                                       .timeout(Duration.ofHours(1)))
		            .thenAwait(Duration.ofHours(2))
		            .expectErrorMessage("Did not observe any item or terminal signal within " +
				            "3600000ms in 'source(MonoNever)' (and no fallback has been " +
				            "configured)")
		            .verify();
	}

	@Test
	public void timeoutDurationMessageWithName() {
		StepVerifier.withVirtualTime(() -> Mono.never()
		                                       .name("Name")
		                                       .timeout(Duration.ofHours(1)))
		            .thenAwait(Duration.ofHours(2))
		            .expectErrorMessage("Did not observe any item or terminal signal within " +
				            "3600000ms in 'Name' (and no fallback has been " +
				            "configured)")
		            .verify();
	}


	@Test
	public void timeoutNotDurationMessage() {
		StepVerifier.create(Mono.never()
		                        .timeout(Mono.just("immediate")))
		            .expectErrorMessage("Did not observe any item or terminal signal within " +
				            "first signal from a Publisher in 'source(MonoNever)' " +
				            "(and no fallback has been configured)")
		            .verify();
	}
}