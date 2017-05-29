/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;


public class FluxSkipTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.shouldAssertPostTerminateState(false)
				.shouldHitDropErrorHookAfterTerminate(false)
				.shouldHitDropNextHookAfterTerminate(false);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.skip(1)).receiveValues(item(1), item(2))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(scenario(f -> f.skip(1)));
	}

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxSkip<>(null, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void skipInvalid() {
		Flux.never().skip(-1);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .skip(5)
		    .subscribe(ts);

		ts.assertValues(6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .skip(5)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(6, 7)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void skipAll() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .skip(Long.MAX_VALUE)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void aFluxCanBeSkipped(){
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .skip(1)
		)
		            .expectNext("test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void aFluxCanBeSkippedZero(){
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .skip(0)
		)
		            .expectNext("test", "test2", "test3")
		            .verifyComplete();
	}

	@Test
    public void scanSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSkip.SkipSubscriber<Integer> test = new FluxSkip.SkipSubscriber<>(actual, 7);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
    }
}
