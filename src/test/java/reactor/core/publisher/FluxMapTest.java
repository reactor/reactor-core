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
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Test;
import reactor.core.Fuseable;
import reactor.test.subscriber.AssertSubscriber;

public class FluxMapTest extends AbstractFluxOperatorTest<String, String>{

	@Override
	protected List<Scenario<String, String>> errorInOperatorCallback() {
		return Arrays.asList(
				Scenario.from(f -> f.map(d -> {
					throw new RuntimeException("test");
				}), Fuseable.ANY)
		);
	}

	@Override
	protected Flux<String> errorFromUpstreamFailure(Flux<String> f) {
		return f.map(d -> d);
	}

	Flux<Integer> just = Flux.just(1);

	@Test(expected = NullPointerException.class)
	public void nullSource() {
		new FluxMap<Integer, Integer>(null, v -> v);
	}

	@Test(expected = NullPointerException.class)
	public void nullMapper() {
		just.map(null);
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

		UnicastProcessor<Integer> up =
				UnicastProcessor.create(new ConcurrentLinkedQueue<>());

		up.map(v -> v + 1)
		  .subscribe(ts);

		for (int i = 1; i < 11; i++) {
			up.onNext(i);
		}
		up.onComplete();

		ts.assertValues(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void asyncFusionBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(1);

		UnicastProcessor<Integer> up =
				UnicastProcessor.create(new ConcurrentLinkedQueue<>());

		Flux.just(1)
		    .hide()
		    .flatMap(w -> up.map(v -> v + 1))
		    .subscribe(ts);

		up.onNext(1);

		ts.assertValues(2)
		  .assertNoError()
		  .assertNotComplete();

		up.onComplete();

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
}
