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

import org.junit.Assert;
import org.junit.Test;
import reactor.test.subscriber.TestSubscriber;

public class MonoReduceTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new MonoReduce<>(null, () -> 1, (a, b) -> (Integer) b);
	}

	@Test(expected = NullPointerException.class)
	public void supplierNull() {
		new MonoReduce<>(Mono.never(), null, (a, b) -> b);
	}

	@Test(expected = NullPointerException.class)
	public void accumulatorNull() {
		new MonoReduce<>(Mono.never(), () -> 1, null);
	}

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10).reduceWith(() -> 0, (a, b) -> b).subscribe(ts);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 10).reduceWith(() -> 0, (a, b) -> b).subscribe(ts);

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
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10).<Integer>reduceWith(() -> {
			throw new RuntimeException("forced failure");
		}, (a, b) -> b).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")));
	}

	@Test
	public void accumulatorThrows() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10).<Integer>reduceWith(() -> 0, (a, b) -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")));
	}

	@Test
	public void supplierReturnsNull() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10).<Integer>reduceWith(() -> null, (a, b) -> b).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void accumulatorReturnsNull() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10).reduceWith(() -> 0, (a, b) -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

}
