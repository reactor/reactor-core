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

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.subscriber.AssertSubscriber;

public class MonoCollectTest {

	@Test(expected = NullPointerException.class)
	public void nullSource() {
		new MonoCollect<>(null, () -> 1, (a, b) -> {
		});
	}

	@Test(expected = NullPointerException.class)
	public void nullSupplier() {
		Flux.never().collect(null, (a, b) -> {});
	}

	@Test(expected = NullPointerException.class)
	public void nullAction() {
		Flux.never().collect(() -> 1, null);
	}

	@Test
	public void normal() {
		AssertSubscriber<ArrayList<Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 10).collect(ArrayList<Integer>::new, (a, b) -> a.add(b)).subscribe(ts);

		ts.assertValues(new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<ArrayList<Integer>> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).collect(ArrayList<Integer>::new, ArrayList::add).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void supplierThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 10).collect(() -> {
			throw new RuntimeException("forced failure");
		}, (a, b) -> {
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")))
		  .assertNotComplete();

	}

	@Test
	public void supplierReturnsNull() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 10).collect(() -> null, (a, b) -> {
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void actionThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 10).collect(() -> 1, (a, b) -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")))
		  .assertNotComplete();

	}

}
