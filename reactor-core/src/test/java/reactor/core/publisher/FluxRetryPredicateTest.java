/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.Test;

import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

@SuppressWarnings("deprecation") //retry with Predicate are deprecated, the underlying implementation is going to be removed ultimately
public class FluxRetryPredicateTest {

	final Flux<Integer> source = Flux.concat(Flux.range(1, 5),
			Flux.error(new RuntimeException("forced failure 0")));

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxRetryPredicate<>(null, e -> true);
	}

	@Test(expected = NullPointerException.class)
	public void predicateNull() {
		Flux.never()
		    .retry((Predicate<? super Throwable>) null);
	}

	@Test
	public void normal() {
		int[] times = {1};

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		source.retry(e -> times[0]-- > 0)
		      .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure 0")
		  .assertNotComplete();
	}

	@Test
	public void normalBackpressured() {
		int[] times = {1};

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		source.retry(e -> times[0]-- > 0)
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure 0")
		  .assertNotComplete();
	}

	@Test
	public void dontRepeat() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		source.retry(e -> false)
		      .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure 0")
		  .assertNotComplete();
	}

	@Test
	public void predicateThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		source.retry(e -> {
			throw new RuntimeException("forced failure");
		})
		      .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void twoRetryNormal() {
		AtomicInteger i = new AtomicInteger();

		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .doOnNext(d -> {
			                        if (i.getAndIncrement() < 2) {
				                        throw new RuntimeException("test");
			                        }
		                        })
		                        .retry(e -> i.get() <= 2)
		                        .count())
		            .expectNext(3L)
		            .expectComplete()
		            .verify();
	}


	@Test
	public void twoRetryNormalSupplier() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		StepVerifier.create(Flux.defer(() -> Flux.just(i.incrementAndGet()))
		                        .doOnNext(v -> {
			                        if(v < 4) {
				                        throw new RuntimeException("test");
			                        }
			                        else {
				                        bool.set(false);
			                        }
		                        })
		                        .retry(3, e -> bool.get()))
		            .expectNext(4)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void twoRetryErrorSupplier() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		StepVerifier.create(Flux.defer(() -> Flux.just(i.incrementAndGet()))
		                        .doOnNext(v -> {
			                        if(v < 4) {
				                        if( v > 2){
					                        bool.set(false);
				                        }
				                        throw new RuntimeException("test");
			                        }
		                        })
		                        .retry(3, e -> bool.get()))
		            .verifyErrorMessage("test");
	}

	@Test
	public void twoRetryNormalSupplier3() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		StepVerifier.create(Flux.defer(() -> Flux.just(i.incrementAndGet()))
		                        .doOnNext(v -> {
			                        if(v < 4) {
				                        throw new RuntimeException("test");
			                        }
			                        else {
				                        bool.set(false);
			                        }
		                        })
		                        .retry(2, e -> bool.get()))
		            .verifyErrorMessage("test");
	}

	@Test
	public void twoRetryNormalSupplier2() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		StepVerifier.create(Flux.defer(() -> Flux.just(i.incrementAndGet()))
		                        .doOnNext(v -> {
			                        if(v < 4) {
				                        throw new RuntimeException("test");
			                        }
			                        else {
				                        bool.set(false);
			                        }
		                        })
		                        .retry(0, e -> bool.get()))
		            .expectNext(4)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void twoRetryErrorSupplier2() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		StepVerifier.create(Flux.defer(() -> Flux.just(i.incrementAndGet()))
		                        .doOnNext(v -> {
			                        if(v < 4) {
				                        if( v > 2){
					                        bool.set(false);
				                        }
				                        throw new RuntimeException("test");
			                        }
		                        })
		                        .retry(0, e -> bool.get()))
		            .verifyErrorMessage("test");
	}

}
