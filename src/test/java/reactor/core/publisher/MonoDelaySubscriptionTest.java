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

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.test.TestSubscriber;

public class MonoDelaySubscriptionTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new MonoDelaySubscription<>(null, Mono.never());
	}

	@Test(expected = NullPointerException.class)
	public void otherNull() {
		Mono.never().delaySubscription((Publisher<?>)null);
	}

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Mono.just(1)
		    .delaySubscription(Mono.just(1))
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Mono.just(1)
		    .delaySubscription(Mono.just(1))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void manyTriggered() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Mono.just(1)
		    .delaySubscription(Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void manyTriggeredBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Mono.just(1)
		    .delaySubscription(Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void emptyTrigger() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Mono.just(1)
		    .delaySubscription(Mono.empty())
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void emptyTriggerBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Mono.just(1)
		    .delaySubscription(Mono.empty())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void neverTriggered() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Mono.just(1)
		    .delaySubscription(Mono.never())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();
	}


}
