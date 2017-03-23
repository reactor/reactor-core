/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.Assert;
import org.junit.Test;
import reactor.test.publisher.ReduceOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

public class MonoReduceSeedTest extends ReduceOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.shouldHitDropNextHookAfterTerminate(false);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.reduce(item(0), (a, b) -> a))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.reduce(item(0), (a, b) -> null)),

				scenario(f -> f.reduceWith(() -> null, (a, b) -> a + b))
		);
	}

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new MonoReduceSeed<>(null, () -> 1, (a, b) -> (Integer) b);
	}

	@Test(expected = NullPointerException.class)
	public void supplierNull() {
		Flux.never()
		    .reduceWith(null, (a, b) -> b);
	}

	@Test(expected = NullPointerException.class)
	public void accumulatorNull() {
		Flux.never()
		    .reduceWith(() -> 1, null);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .reduceWith(() -> 0, (a, b) -> b)
		    .subscribe(ts);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .reduceWith(() -> 0, (a, b) -> b)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void supplierThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).<Integer>reduceWith(() -> {
			throw new RuntimeException("forced failure");
		}, (a, b) -> b).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> Assert.assertTrue(e.getMessage()
		                                           .contains("forced failure")));
	}

	@Test
	public void accumulatorThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).reduceWith(() -> 0, (a, b) -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> Assert.assertTrue(e.getMessage()
		                                           .contains("forced failure")));
	}

	@Test
	public void supplierReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).<Integer>reduceWith(() -> null, (a, b) -> b).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void accumulatorReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .reduceWith(() -> 0, (a, b) -> null)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

}
