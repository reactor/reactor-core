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

import java.util.NoSuchElementException;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.TestSubscriber;

public class MonoSingleTest {

	@Test(expected = NullPointerException.class)
	public void source1Null() {
		new MonoSingle<>(null);
	}

	@Test(expected = NullPointerException.class)
	public void source2Null() {
		new MonoSingle<>(null, () -> 1);
	}

	@Test(expected = NullPointerException.class)
	public void defaultSupplierNull() {
		new MonoSingle<>(Mono.never(), null);
	}

	@Test
	public void defaultReturnsNull() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.<Integer>empty().single(() -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void defaultThrows() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.<Integer>empty().single(() -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage()
		                                            .contains("forced failure")))
		  .assertNotComplete();
	}

	@Test
	public void normal() {

		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.just(1).single().subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.just(1).single().subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void empty() {

		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.<Integer>empty().single().subscribe(ts);

		ts.assertNoValues()
		  .assertError(NoSuchElementException.class)
		  .assertNotComplete();
	}

	@Test
	public void emptyDefault() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.<Integer>empty().single(() -> 1).subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void emptyDefaultBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.<Integer>empty().single(() -> 1).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void multi() {

		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10).single().subscribe(ts);

		ts.assertNoValues()
		  .assertError(IndexOutOfBoundsException.class)
		  .assertNotComplete();
	}

	@Test
	public void multiBackpressured() {

		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 10).single().subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertNoValues()
		  .assertError(IndexOutOfBoundsException.class)
		  .assertNotComplete();
	}

}
