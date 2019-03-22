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

import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class MonoRepeatTest {

	@Test(expected = IllegalArgumentException.class)
	public void timesInvalid() {
		Mono.never()
		    .repeat(-1);
	}

	@Test
	public void zeroRepeat() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger i = new AtomicInteger();
		Mono.fromCallable(i::incrementAndGet)
		    .repeat(0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oneRepeat() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger i = new AtomicInteger();
		Mono.fromCallable(i::incrementAndGet)
		    .repeat(1)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oneRepeatBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		AtomicInteger i = new AtomicInteger();
		Mono.fromCallable(i::incrementAndGet)
		    .repeat(1)
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
	public void twoRepeat() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger i = new AtomicInteger();
		Mono.fromCallable(i::incrementAndGet)
		    .repeat(2)
		    .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void twoRepeatNormal() {
		AtomicInteger i = new AtomicInteger();

		StepVerifier.create(Mono.fromCallable(i::incrementAndGet)
		                        .repeat(2)
		                        .count())
		            .expectNext(2L)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void twoRepeatNormalSupplier() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		StepVerifier.create(Mono.fromCallable(i::incrementAndGet)
		                        .repeat(2, bool::get))
		            .expectNext(1, 2)
		            .expectNext(3)
		            .then(() -> bool.set(false))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void twoRepeatBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		AtomicInteger i = new AtomicInteger();
		Mono.fromCallable(i::incrementAndGet)
		    .repeat(2)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1, 2)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void repeatInfinite() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger i = new AtomicInteger();
		Mono.fromCallable(i::incrementAndGet)
		    .repeat()
		    .take(9)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9)
		  .assertComplete()
		  .assertNoError();
	}
}
