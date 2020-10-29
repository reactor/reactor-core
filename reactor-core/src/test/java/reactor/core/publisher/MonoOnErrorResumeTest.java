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

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoOnErrorResumeTest {

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .onErrorResume(v -> Mono.just(2))
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.just(1)
		    .onErrorResume(v -> Mono.just(2))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>error(new RuntimeException("forced failure")).onErrorResume(v -> Mono.just(
				2))
		                                                           .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void error2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>error(new RuntimeException("forced failure")).onErrorReturn(2)
		                                                           .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorFiltered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>error(new RuntimeException("forced failure"))
				.onErrorResume(e -> e.getMessage().equals("forced failure"), v -> Mono.just(2))
				.subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorFiltered2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>error(new RuntimeException("forced failure"))
				.onErrorReturn(e -> e.getMessage().equals("forced failure"), 2)
				.subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorFiltered3() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>error(new RuntimeException("forced failure"))
				.onErrorReturn(RuntimeException.class, 2)
				.subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorMap() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>error(new Exception()).onErrorMap(d -> new RuntimeException("forced" +
				" " +
				"failure"))
		                                    .subscribe(ts);

		ts.assertNoValues()
		  .assertError()
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void errorBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.<Integer>error(new RuntimeException("forced failure")).onErrorResume(v -> Mono.just(
				2))
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(2)
		  .assertComplete();
	}

	@Test
	public void nextFactoryThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.<Integer>error(new RuntimeException("forced failure")).onErrorResume(v -> {
			throw new RuntimeException("forced failure 2");
		})
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> assertThat(e).hasMessageContaining("forced failure 2"));
	}

	@Test
	public void nextFactoryReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.<Integer>error(new RuntimeException("forced failure")).onErrorResume(v -> null)
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	static final class TestException extends Exception {}

	@Test
	public void mapError() {
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.onErrorMap(TestException.class, e -> new Exception("test")))
		            .verifyErrorMessage("test");
	}

	@Test
	public void otherwiseErrorFilter() {
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.onErrorResume(TestException.class, e -> Mono.just(1)))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void otherwiseErrorUnfilter() {
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.onErrorResume(RuntimeException.class, e -> Mono.just(1)))
		            .verifyError(TestException.class);
	}

	@Test
	public void otherwiseReturnErrorFilter() {
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.onErrorReturn(TestException.class, 1))
		            .expectNext(1)
		            .verifyComplete();
	}


	@Test
	public void otherwiseReturnErrorFilter2() {
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.onErrorReturn(TestException.class::isInstance, 1))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void otherwiseReturnErrorUnfilter() {
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.onErrorReturn(RuntimeException.class, 1))
		            .verifyError(TestException.class);
	}

	@Test
	public void otherwiseReturnErrorUnfilter2() {
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.onErrorReturn(RuntimeException.class::isInstance, 1))
		            .verifyError(TestException.class);
	}

	@Test
	public void scanOperator(){
	    MonoOnErrorResume<String> test = new MonoOnErrorResume<>(Mono.just("foo"), e -> Mono.just("bar"));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
