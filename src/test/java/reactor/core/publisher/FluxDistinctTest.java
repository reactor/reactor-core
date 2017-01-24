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
import java.util.HashSet;
import java.util.List;

import org.junit.Test;
import reactor.core.Fuseable;
import reactor.test.subscriber.AssertSubscriber;

public class FluxDistinctTest extends AbstractFluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ANY);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorInOperatorCallback() {
		return Arrays.asList(
				scenario(f -> f.distinct(d -> {
					throw exception();
				})),

				scenario(f -> f.distinct(d -> null))
						.verifier(step -> step.verifyError(NullPointerException.class))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(f -> f.distinct()));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_threeNextAndComplete() {
		return Arrays.asList(
				scenario(f -> f.distinct()),

				scenario(f -> f.distinct()).finiteFlux(Flux.just(item(0),
						                           item(0),
						                           item(0)))
				                           .verifier(step -> step.expectNext(item(0))
				                                                 .verifyComplete())
		);
	}

	@Override
	protected int fusionModeThreadBarrierSupport() {
		return Fuseable.ANY;
	}

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxDistinct<>(null, k -> k, HashSet::new);
	}

	@Test(expected = NullPointerException.class)
	public void keyExtractorNull() {
		Flux.never().distinct(null);
	}

	@Test(expected = NullPointerException.class)
	public void collectionSupplierNull() {
		new FluxDistinct<>(Flux.never(), k -> k, null);
	}

	@Test
	public void allDistinct() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allDistinctBackpressured() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allDistinctHide() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .hide()
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allDistinctBackpressuredHide() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .hide()
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void someDistinct() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1, 2, 2, 3, 4, 5, 6, 1, 2, 7, 7, 8, 9, 9, 10, 10, 10)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void someDistinctBackpressured() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1, 2, 2, 3, 4, 5, 6, 1, 2, 7, 7, 8, 9, 9, 10, 10, 10)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allSame() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1, 1, 1, 1, 1, 1, 1, 1, 1)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allSameFusable() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(Fuseable.ANY);

		Flux.just(1, 1, 1, 1, 1, 1, 1, 1, 1)
		    .distinct(k -> k)
		    .filter(t -> true)
		    .map(t -> t)
		    .cache(4)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertFuseableSource()
		  .assertFusionEnabled()
		  .assertFusionMode(Fuseable.ASYNC)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allSameBackpressured() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1, 1, 1, 1, 1, 1, 1, 1, 1)
		    .distinct(k -> k)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void withKeyExtractorSame() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).distinct(k -> k % 3).subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void withKeyExtractorBackpressured() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).distinct(k -> k % 3).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void keyExtractorThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).distinct(k -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void collectionSupplierThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		new FluxDistinct<>(Flux.range(1, 10), k -> k, () -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void collectionSupplierReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		new FluxDistinct<>(Flux.range(1, 10), k -> k, () -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}
}
