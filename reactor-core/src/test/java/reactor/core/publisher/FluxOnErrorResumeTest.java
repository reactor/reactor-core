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

import org.junit.Assert;
import org.junit.Test;

import reactor.core.Exceptions;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class FluxOnErrorResumeTest {
/*
	@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(FluxOnErrorResume.class);
		
		ctb.addRef("source", Flux.never());
		ctb.addRef("nextFactory", (Function<Throwable, Publisher<Object>>)e -> Flux.never());
		
		ctb.test();
	}*/

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .onErrorResume(v -> Flux.range(11, 10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .onErrorResume(v -> Flux.range(11, 10))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>error(new RuntimeException("forced failure")).onErrorResume(v -> Flux.range(
				11,
				10))
		                                                           .subscribe(ts);

		ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorFiltered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>error(new RuntimeException("forced failure")).onErrorResume(e -> e.getMessage()
		                                                                                    .equals("forced failure"),
				v -> Mono.just(2))
		                                                           .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorMap() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>error(new Exception()).onErrorMap(d -> new RuntimeException("forced" + " " + "failure"))
		                                    .subscribe(ts);

		ts.assertNoValues()
		  .assertError()
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void errorBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>error(new RuntimeException("forced failure")).onErrorResume(v -> Flux.range(
				11,
				10))
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(11, 12)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(11, 12, 13, 14, 15, 16, 17)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void someFirst() {
		FluxProcessor<Integer, Integer> tp = Processors.multicast();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		tp.onErrorResume(v -> Flux.range(11, 10))
		  .subscribe(ts);

		tp.onNext(1);
		tp.onNext(2);
		tp.onNext(3);
		tp.onNext(4);
		tp.onNext(5);
		tp.onError(new RuntimeException("forced failure"));

		ts.assertValues(1, 2, 3, 4, 5, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void someFirstBackpressured() {
		FluxProcessor<Integer, Integer> tp = Processors.multicast();

		AssertSubscriber<Integer> ts = AssertSubscriber.create(10);

		tp.onErrorResume(v -> Flux.range(11, 10))
		  .subscribe(ts);

		tp.onNext(1);
		tp.onNext(2);
		tp.onNext(3);
		tp.onNext(4);
		tp.onNext(5);
		tp.onError(new RuntimeException("forced failure"));

		ts.assertValues(1, 2, 3, 4, 5, 11, 12, 13, 14, 15)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void nextFactoryThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>error(new RuntimeException("forced failure")).onErrorResume(v -> {
			throw new RuntimeException("forced failure 2");
		})
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> Assert.assertTrue(e.getMessage()
		                                           .contains("forced failure 2")));
	}

	@Test
	public void nextFactoryReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>error(new RuntimeException("forced failure")).onErrorResume(v -> null)
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void errorPropagated() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Exception exception = new NullPointerException("forced failure");
		Flux.<Integer>error(exception).onErrorResume(v -> {
			throw Exceptions.propagate(v);
		})
		                              .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertErrorWith(e -> Assert.assertSame(exception, e));
	}

	@Test
	public void errorPublisherCanReturn() {
		StepVerifier.create(Flux.<String>error(new IllegalStateException("boo")).onErrorReturn(
				"recovered"))
		            .expectNext("recovered")
		            .verifyComplete();
	}

	@Test
	public void returnFromIterableError() {
		StepVerifier.create(Flux.range(1, 1000)
		                        .map(t -> {
			                        if (t == 3) {
				                        throw new RuntimeException("test");
			                        }
			                        return t;
		                        })
		                        .onErrorReturn(100_000))
		            .expectNext(1, 2, 100_000)
		            .verifyComplete();
	}

	@Test
	public void switchFromIterableError() {
		StepVerifier.create(Flux.range(1, 1000)
		                        .map(t -> {
			                        if (t == 3) {
				                        throw new RuntimeException("test");
			                        }
			                        return t;
		                        })
		                        .onErrorResume(e -> Flux.range(9999, 4)))
		            .expectNext(1, 2, 9999, 10000, 10001, 10002)
		            .verifyComplete();
	}

	@Test
	public void switchFromCreateError() {
		Flux<String> source = Flux.create(s -> {
					s.next("Three");
					s.next("Two");
					s.next("One");
					s.error(new Exception());
				});

		Flux<String> fallback = Flux.create(s -> {
					s.next("1");
					s.next("2");
					s.complete();
				});

		StepVerifier.create(source.onErrorResume(e -> fallback))
		            .expectNext("Three", "Two", "One", "1", "2")
		            .verifyComplete();
	}
	@Test
	public void switchFromCreateError2() {
		Flux<String> source = Flux.create(s -> {
					s.next("Three");
					s.next("Two");
					s.next("One");
					s.error(new Exception());
				});

		StepVerifier.create(source.onErrorReturn("Zero"))
		            .expectNext("Three", "Two", "One", "Zero")
		            .verifyComplete();
	}

	static final class TestException extends Exception {}

	@Test
	public void mapError() {
		StepVerifier.create(Flux.<Integer>error(new TestException())
				.onErrorMap(TestException.class, e -> new Exception("test")))
				.verifyErrorMessage("test");
	}

	@Test
	public void onErrorResumeErrorPredicate() {
		StepVerifier.create(Flux.<Integer>error(new TestException())
				.onErrorResume(TestException.class, e -> Mono.just(1)))
				.expectNext(1)
				.verifyComplete();
	}

	@Test
	public void onErrorResumeErrorPredicateNot() {
		StepVerifier.create(Flux.<Integer>error(new TestException())
				.onErrorResume(RuntimeException.class, e -> Mono.just(1)))
				.verifyError(TestException.class);
	}

	@Test
	public void onErrorReturnErrorPredicate() {
		StepVerifier.create(Flux.<Integer>error(new TestException())
				.onErrorReturn(TestException.class, 1))
				.expectNext(1)
				.verifyComplete();
	}

	@Test
	public void onErrorReturnErrorPredicateNot() {
		StepVerifier.create(Flux.<Integer>error(new TestException())
				.onErrorReturn(RuntimeException.class, 1))
				.verifyError(TestException.class);
	}
	@Test
	public void onErrorReturnErrorPredicate2() {
		StepVerifier.create(Flux.<Integer>error(new TestException())
				.onErrorReturn(TestException.class::isInstance, 1))
				.expectNext(1)
				.verifyComplete();
	}

	@Test
	public void onErrorReturnErrorPredicateNot2() {
		StepVerifier.create(Flux.<Integer>error(new TestException())
				.onErrorReturn(RuntimeException.class::isInstance, 1))
				.verifyError(TestException.class);
	}
}
