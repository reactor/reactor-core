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
import reactor.core.Exceptions;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class FluxTakeTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxTake<>(null, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void numberIsInvalid() {
		Flux.never()
		    .take(-1);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .take(5)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .take(5)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeZero() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .take(0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeOverflowAttempt() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Publisher<Integer> p = s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext(1);
			s.onNext(2);
			try {
				s.onNext(3);
			}
			catch(RuntimeException re){
				Assert.assertTrue("cancelled", Exceptions.isCancel(re));
				return;
			}
			Assert.fail();
		};

		Flux.from(p)
		    .take(2)
		    .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void aFluxCanBeLimited(){
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(2)
		)
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

}
