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
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class FluxConcatArrayTest {

	@Test(expected = NullPointerException.class)
	public void arrayNull() {
		Flux.concat((Publisher<Object>[]) null);
	}

	final Publisher<Integer> source = Flux.range(1, 3);

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.concat(source, source, source)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.concat(source, source, source)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(4);

		ts.assertValues(1, 2, 3, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oneSourceIsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.concat(source, null, source)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void singleSourceIsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.concat((Publisher<Integer>) null)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void scalarAndRangeBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1)
		    .concatWith(Flux.range(2, 3))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void errorDelayed() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.concatDelayError(
				Flux.range(1, 2),
				Flux.error(new RuntimeException("Forced failure")),
				Flux.range(3, 2))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("Forced failure")
		  .assertNotComplete();
	}

	@Test
	public void errorManyDelayed() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.concatDelayError(
				Flux.range(1, 2),
				Flux.error(new RuntimeException("Forced failure")),
				Flux.range(3, 2),
				Flux.error(new RuntimeException("Forced failure")),
				Flux.empty())
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4)
		  .assertError(Throwable.class)
		  .assertErrorMessage("Multiple exceptions")
		  .assertNotComplete();
	}

	@Test
	public void veryLongTake() {
		Flux.range(1, 1_000_000_000)
		    .concatWith(Flux.empty())
		    .take(10)
		    .subscribeWith(AssertSubscriber.create())
		    .assertComplete()
		    .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}

	@Test
	public void pairWise() {
		Flux<String> f = Flux.concat(Flux.just("test"), Flux.just("test2"))
		                     .concatWith(Flux.just("test3"));

		Assert.assertTrue(f instanceof FluxConcatArray);
		FluxConcatArray<String> s = (FluxConcatArray<String>) f;
		Assert.assertTrue(s.array != null);
		Assert.assertTrue(s.array.length == 3);

		StepVerifier.create(f)
	                .expectNext("test", "test2", "test3")
	                .verifyComplete();
	}


	@Test
	public void pairWise2() {
		Flux<String> f = Mono.just("test")
		                     .concatWith(Flux.just("test2"));

		Assert.assertTrue(f instanceof FluxConcatArray);
		FluxConcatArray<String> s = (FluxConcatArray<String>) f;
		Assert.assertTrue(s.array != null);
		Assert.assertTrue(s.array.length == 2);

		StepVerifier.create(f)
	                .expectNext("test", "test2")
	                .verifyComplete();
	}

	@Test
	public void thenMany(){
		StepVerifier.create(Flux.just(1, 2, 3).thenMany(Flux.just("test", "test2")))
	                .expectNext("test", "test2")
	                .verifyComplete();
	}


	@Test
	public void thenManyThenMany(){
		StepVerifier.create(Flux.just(1, 2, 3).thenMany(Flux.just("test", "test2"))
		                        .thenMany(Flux.just(1L, 2L)))
	                .expectNext(1L, 2L)
	                .verifyComplete();
	}

	@Test
	public void thenManySupplier(){
		StepVerifier.create(Flux.just(1, 2, 3).thenMany(() -> Flux.just("test", "test2")))
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void thenManyError(){
		StepVerifier.create(Flux.error(new Exception("test")).thenMany(() -> Flux.just(4, 5, 6)))
	                .verifyErrorMessage("test");
	}

}
