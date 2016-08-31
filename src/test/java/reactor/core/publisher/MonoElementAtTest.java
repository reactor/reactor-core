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
import reactor.test.TestSubscriber;

public class MonoElementAtTest {

	@Test(expected = NullPointerException.class)
	public void source1Null() {
		new MonoElementAt<>(null, 1);
	}

	@Test(expected = NullPointerException.class)
	public void source2Null() {
		new MonoElementAt<>(null, 1, () -> 1);
	}

	@Test(expected = NullPointerException.class)
	public void defaultSupplierNull() {
		new MonoElementAt<>(Mono.never(), 1, null);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void indexNegative1() {
		new MonoElementAt<>(Mono.never(), -1);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void indexNegative2() {
		new MonoElementAt<>(Mono.never(), -1, () -> 1);
	}

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10).elementAt(0).subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 10).elementAt(0).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal2() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10).elementAt(4).subscribe(ts);

		ts.assertValues(5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal5Backpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 10).elementAt(4).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal3() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10).elementAt(9).subscribe(ts);

		ts.assertValues(10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal3Backpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 10).elementAt(9).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void empty() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.<Integer>empty().elementAt(0).subscribe(ts);

		ts.assertNoValues()
		  .assertError(IndexOutOfBoundsException.class)
		  .assertNotComplete();
	}

	@Test
	public void emptyDefault() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.<Integer>empty().elementAt(0, () -> 20).subscribe(ts);

		ts.assertValues(20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void emptyDefaultBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.<Integer>empty().elementAt(0, () -> 20).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void nonEmptyDefault() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10).elementAt(20, () -> 20).subscribe(ts);

		ts.assertValues(20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void nonEmptyDefaultBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 10).elementAt(20, () -> 20).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void defaultReturnsNull() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.<Integer>empty().elementAt(0, () -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void defaultThrows() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.<Integer>empty().elementAt(0, () -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")))
		  .assertNotComplete();
	}
}
