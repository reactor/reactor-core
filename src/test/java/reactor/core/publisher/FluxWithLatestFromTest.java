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

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class FluxWithLatestFromTest {


	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxWithLatestFrom<>(null, Flux.never(), (a, b) -> a);
	}

	@Test(expected = NullPointerException.class)
	public void otherNull() {
		Flux.never()
		    .withLatestFrom(null, (a, b) -> a);
	}

	@Test(expected = NullPointerException.class)
	public void combinerNull() {
		Flux.never()
		    .withLatestFrom(Flux.never(), null);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .withLatestFrom(Flux.just(10), (a, b) -> a + b)
		    .subscribe
		  (ts);

		ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .withLatestFrom(Flux.just(10), (a, b) -> a + b)
		    .subscribe
		  (ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(11, 12)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(5);

		ts.assertValues(11, 12, 13, 14, 15, 16, 17)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void otherIsNever() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .withLatestFrom(Flux.<Integer>empty(), (a, b) -> a + b)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void otherIsEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .withLatestFrom(Flux.<Integer>empty(), (a, b) -> a + b)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void combinerReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .withLatestFrom(Flux.just(10), (a, b) -> (Integer) null)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void combinerThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).<Integer, Integer>withLatestFrom(Flux.just(10),
		  (a, b) -> {
			  throw new RuntimeException("forced failure");
		  }).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")));
	}


	@Test
	public void combineLatest2Null() {
		StepVerifier.create(Flux.just(1).withLatestFrom(Flux.just(2), (a, b) ->
				null))
		            .verifyError(NullPointerException.class);
	}

	@Test
    public void scanMainSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxWithLatestFrom.WithLatestFromSubscriber<Integer, Integer, Integer> test =
        		new FluxWithLatestFrom.WithLatestFromSubscriber<>(actual, (i, j) -> i + j);
        Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
		Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxWithLatestFrom.WithLatestFromSubscriber<Integer, Integer, Integer> main =
        		new FluxWithLatestFrom.WithLatestFromSubscriber<>(actual, (i, j) -> i + j);
        FluxWithLatestFrom.WithLatestFromOtherSubscriber<Integer> test =
        		new FluxWithLatestFrom.WithLatestFromOtherSubscriber<>(main);
        Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(main);
    }
}
