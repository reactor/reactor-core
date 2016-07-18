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

public class FluxTakeUntilPredicateTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxTakeUntil<>(null, v -> true);
	}

	@Test(expected = NullPointerException.class)
	public void predicateNull() {
		new FluxTakeUntil<>(FluxNever.instance(), null);
	}

	@Test
	public void takeAll() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxTakeUntil<>(new FluxRange(1, 5), v -> false).subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeAllBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		new FluxTakeUntil<>(new FluxRange(1, 5), v -> false).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeSome() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxTakeUntil<>(new FluxRange(1, 5), v -> v == 3).subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeSomeBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		new FluxTakeUntil<>(new FluxRange(1, 5), v -> v == 3).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void stopImmediately() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxTakeUntil<>(new FluxRange(1, 5), v -> true).subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void stopImmediatelyBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		new FluxTakeUntil<>(new FluxRange(1, 5), v -> true).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void predicateThrows() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxTakeUntil<>(new FluxRange(1, 5), v -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

	}

}
