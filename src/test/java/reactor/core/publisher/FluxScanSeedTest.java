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

import org.junit.Test;
import reactor.test.TestSubscriber;

public class FluxScanSeedTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxScanSeed<>(null, () -> 1, (a, b) -> a);
	}

	@Test(expected = NullPointerException.class)
	public void initialValueNull() {
		Flux.never()
		    .scan(null, (a, b) -> a);
	}

	@Test(expected = NullPointerException.class)
	public void accumulatorNull() {
		Flux.never()
		    .scan(1, null);
	}

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .scan(0, (a, b) -> b)
		    .subscribe(ts);

		ts.assertValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 10)
		    .scan(0, (a, b) -> b)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(0, 1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(8);

		ts.assertValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void accumulatorThrows() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .scan(0, (a, b) -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertValues(0)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void accumulatorReturnsNull() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .scan(0, (a, b) -> null)
		    .subscribe(ts);

		ts.assertValues(0)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}
}
