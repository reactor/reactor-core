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
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuples;

public class FluxZipIterableTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxZipIterable<>(null, Collections.emptyList(), (a, b) -> a);
	}

	@Test(expected = NullPointerException.class)
	public void iterableNull() {
		Flux.never()
		    .zipWithIterable(null, (a, b) -> a);
	}

	@Test(expected = NullPointerException.class)
	public void zipperNull() {
		Flux.never()
		    .zipWithIterable(Collections.emptyList(), null);
	}
	
	@Test
	public void normalSameSize() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(11, 22, 33, 44, 55)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void normalSameSizeBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .zipWithIterable(
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
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 4)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(11, 22, 33, 44)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void normalOtherShorter() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40), (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(11, 22, 33, 44)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void sourceEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 0)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40), (a, b) -> a + b).subscribe(ts);
		
		ts.assertNoValues()
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void otherEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				Collections.<Integer>emptyList(), (a, b) -> a + b).subscribe(ts);
		
		ts.assertNoValues()
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void zipperReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> (Integer)null).subscribe(ts);
		
		ts.assertNoValues()
		.assertNotComplete()
		.assertError(NullPointerException.class);
	}

	@Test
	public void iterableReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				() -> null, (a, b) -> a).subscribe(ts);
		
		ts.assertNoValues()
		.assertNotComplete()
		.assertError(NullPointerException.class);
	}

	@Test
	public void zipperThrowsNull() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				Arrays.asList(10, 20, 30, 40, 50), (a, b) -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
		
		ts.assertNoValues()
		.assertNotComplete()
		.assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")));
	}

	@Test
	public void iterableThrowsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .zipWithIterable(
				() -> { throw new RuntimeException("forced failure"); }, (a, b) -> a).subscribe(ts);
		
		ts.assertNoValues()
		.assertNotComplete()
		.assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void zipWithIterable(){
		StepVerifier.create(Flux.just(0).zipWithIterable(Arrays.asList(1, 2, 3)))
	                .expectNext(Tuples.of(0, 1))
	                .verifyComplete();
	}
	
}
