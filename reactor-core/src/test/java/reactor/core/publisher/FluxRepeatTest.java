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

import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class FluxRepeatTest {

	@Test(expected = IllegalArgumentException.class)
	public void timesInvalid() {
		Flux.never()
		    .repeat(-1);
	}

	@Test
	public void zeroRepeat() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .repeat(0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oneRepeat() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .repeat(1)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oneRepeatBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .repeat(1)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(3);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void twoRepeat() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .repeat(2)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void twoRepeatNormal() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .repeat(2)
		                        .count())
		            .expectNext(6L)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void twoRepeatBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .repeat(2)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(4);

		ts.assertValues(1, 2, 3, 4, 5, 1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);
		ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void repeatInfinite() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .repeat()
		    .take(9)
		    .subscribe(ts);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2, 1)
		  .assertComplete()
		  .assertNoError();
	}



	@Test
	public void twoRepeatNormalSupplier() {
		AtomicBoolean bool = new AtomicBoolean(true);

		StepVerifier.create(Flux.range(1, 4)
		                        .repeat(2, bool::get))
		            .expectNext(1, 2, 3, 4)
		            .expectNext(1, 2, 3, 4)
		            .expectNext(1, 2, 3, 4)
		            .then(() -> bool.set(false))
		            .verifyComplete();
	}

	@Test
	public void twoRepeatNormalSupplier2() {
		AtomicBoolean bool = new AtomicBoolean(false);

		StepVerifier.create(Flux.range(1, 4)
		                        .repeat(0, bool::get))
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}

	@Test
	public void twoRepeatNormalSupplier3() {
		AtomicBoolean bool = new AtomicBoolean(true);

		StepVerifier.create(Flux.range(1, 4)
		                        .repeat(2, bool::get))
		            .expectNext(1, 2, 3, 4)
		            .expectNext(1, 2, 3, 4)
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}

	@Test
	public void twoRepeatNormalSupplier4() {
		AtomicBoolean bool = new AtomicBoolean(false);

		StepVerifier.create(Flux.range(1, 4)
		                        .repeat(2, bool::get))
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}
}
