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
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

public class FluxTakeUntilPredicateTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxTakeUntil<>(null, v -> true);
	}

	@Test(expected = NullPointerException.class)
	public void predicateNull() {
		Flux.never()
		    .takeUntil(null);
	}

	@Test
	public void takeAll() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .takeUntil(v -> false)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeAllBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .takeUntil(v -> false)
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
	public void takeSome() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .takeUntil(v -> v == 3)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeSomeBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .takeUntil(v -> v == 3)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void stopImmediately() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .takeUntil(v -> true)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void stopImmediatelyBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .takeUntil(v -> true)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void predicateThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .takeUntil(v -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

	}

	@Test
    public void scanPredicateSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxTakeUntil.TakeUntilPredicateSubscriber<Integer> test =
        		new FluxTakeUntil.TakeUntilPredicateSubscriber<>(actual, i -> true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }
}
