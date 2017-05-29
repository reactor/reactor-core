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
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

public class FluxSkipWhileTest extends FluxOperatorTest<String, String> {

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(scenario(f -> f.skipWhile(d -> {
				throw exception();
			})
		)
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.skipWhile(item(0)::equals))
						.receiveValues(item(1), item(2))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(scenario(f -> f.skipWhile(item(0)::equals)));
	}


	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxSkipWhile<>(null, v -> true);
	}

	@Test(expected = NullPointerException.class)
	public void predicateNull() {
		Flux.never()
		    .skipWhile(null);
	}

	@Test
	public void skipNone() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .skipWhile(v -> false)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void skipNoneBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .skipWhile(v -> false)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void skipSome() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .skipWhile(v -> v < 3)
		    .subscribe(ts);

		ts.assertValues(3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void skipSomeBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .skipWhile(v -> v < 3)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(3, 4)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void skipAll() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .skipWhile(v -> true)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void skipAllBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .skipWhile(v -> true)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void predicateThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .skipWhile(v -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

	}

	@Test
	public void aFluxCanBeSkippedWhile(){
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .skipWhile("test"::equals)
		)
		            .expectNext("test2", "test3")
		            .verifyComplete();
	}

	@Test
    public void scanSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSkipWhile.SkipWhileSubscriber<Integer> test = new FluxSkipWhile.SkipWhileSubscriber<>(actual, i -> true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }
}
