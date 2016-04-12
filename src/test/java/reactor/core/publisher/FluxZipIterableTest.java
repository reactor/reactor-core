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

import org.junit.Assert;
import org.junit.Test;
import reactor.core.test.TestSubscriber;

public class FluxZipIterableTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxZipIterable<>(null, Collections.emptyList(), (a, b) -> a);
	}

	@Test(expected = NullPointerException.class)
	public void iterableNull() {
		new FluxZipIterable<>(Flux.never(), null, (a, b) -> a);
	}

	@Test(expected = NullPointerException.class)
	public void zipperNull() {
		new FluxZipIterable<>(Flux.never(), Collections.emptyList(), null);
	}
	
	@Test
	public void normalSameSize() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		new FluxZipIterable<>(new FluxRange(1, 5),
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(11, 22, 33, 44, 55)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void normalSameSizeBackpressured() {
		TestSubscriber<Integer> ts = new TestSubscriber<>(0);
		
		new FluxZipIterable<>(new FluxRange(1, 5),
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> a + b).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		ts.request(1);

		ts.assertValues(11)
		.assertNoError()
		.assertNotComplete();

		ts.request(2);

		ts.assertValues(11, 22, 33)
		.assertNoError()
		.assertNotComplete();

		ts.request(5);

		ts.assertValues(11, 22, 33, 44, 55)
		.assertComplete()
		.assertNoError();
	}
	
	@Test
	public void normalSourceShorter() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		new FluxZipIterable<>(new FluxRange(1, 4),
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(11, 22, 33, 44)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void normalOtherShorter() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		new FluxZipIterable<>(new FluxRange(1, 5),
				Arrays.asList(10, 20, 30, 40), (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(11, 22, 33, 44)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void sourceEmpty() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		new FluxZipIterable<>(new FluxRange(1, 0),
				Arrays.asList(10, 20, 30, 40), (a, b) -> a + b).subscribe(ts);
		
		ts.assertNoValues()
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void otherEmpty() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		new FluxZipIterable<>(new FluxRange(1, 5),
				Collections.<Integer>emptyList(), (a, b) -> a + b).subscribe(ts);
		
		ts.assertNoValues()
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void zipperReturnsNull() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		new FluxZipIterable<>(new FluxRange(1, 5),
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> (Integer)null).subscribe(ts);
		
		ts.assertNoValues()
		.assertNotComplete()
		.assertError(NullPointerException.class);
	}

	@Test
	public void iterableReturnsNull() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		new FluxZipIterable<>(new FluxRange(1, 5),
				() -> null, (a, b) -> a).subscribe(ts);
		
		ts.assertNoValues()
		.assertNotComplete()
		.assertError(NullPointerException.class);
	}

	@Test
	public void zipperThrowsNull() {
		TestSubscriber<Object> ts = new TestSubscriber<>();
		
		new FluxZipIterable<>(new FluxRange(1, 5),
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
		
		ts.assertNoValues()
		.assertNotComplete()
		.assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")));
	}

	@Test
	public void iterableThrowsNull() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		new FluxZipIterable<>(new FluxRange(1, 5),
				() -> { throw new RuntimeException("forced failure"); }, (a, b) -> a).subscribe(ts);
		
		ts.assertNoValues()
		.assertNotComplete()
		.assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")));
	}
	
}
