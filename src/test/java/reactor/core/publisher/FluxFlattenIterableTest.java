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
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;
import reactor.test.TestSubscriber;

public class FluxFlattenIterableTest {

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 5)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 5)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertNoEvents();

		ts.request(1);

		ts.assertIncomplete(1);

		ts.request(2);

		ts.assertIncomplete(1, 2, 2);

		ts.request(7);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalNoFusion() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 5)
		    .hide()
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressuredNoFusion() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 5)
		    .hide()
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertNoEvents();

		ts.request(1);

		ts.assertIncomplete(1);

		ts.request(2);

		ts.assertIncomplete(1, 2, 2);

		ts.request(7);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void longRunning() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		int n = 1_000_000;

		Flux.range(1, n)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValueCount(n * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void longRunningNoFusion() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		int n = 1_000_000;

		Flux.range(1, n)
		    .hide()
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValueCount(n * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void fullFusion() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		int n = 1_000_000;

		Flux.range(1, n)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .concatMap(Flux::just)
		    .subscribe(ts);

		ts.assertValueCount(n * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void just() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.just(1)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void empty() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.<Integer>empty().concatMapIterable(v -> Arrays.asList(v, v + 1))
		                     .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

}
