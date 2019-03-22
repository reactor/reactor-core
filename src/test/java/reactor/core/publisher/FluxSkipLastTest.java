/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxSkipLastTest extends FluxOperatorTest<String, String> {
	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.shouldAssertPostTerminateState(false)
		                     .shouldHitDropErrorHookAfterTerminate(false)
		                     .shouldHitDropNextHookAfterTerminate(false);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(

				scenario(f -> f.skipLast(1))
						.receiveValues(item(0) ,item(1))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(scenario(f -> f.skipLast(1)));
	}


	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxSkipLast<>(null, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeNumber() {
		Flux.never()
		    .skipLast(-1);
	}

	@Test
	public void skipNone() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .skipLast(0)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void skipNoneBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .skipLast(0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void skipSome() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .skipLast(3)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void skipSomeBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .skipLast(3)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(4);

		ts.assertValues(1, 2, 3, 4, 5, 6)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void skipAll() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .skipLast(20)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void skipAllBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .skipLast(20)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

}
