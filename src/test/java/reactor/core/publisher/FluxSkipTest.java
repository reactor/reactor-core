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
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class FluxSkipTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxSkip<>(null, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void skipInvalid() {
		new FluxSkip<>(Flux.never(), -1);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .skip(5)
		    .subscribe(ts);

		ts.assertValues(6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .skip(5)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(6, 7)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void skipAll() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .skip(Long.MAX_VALUE)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void aFluxCanBeSkipped(){
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .skip(1)
		)
		            .expectNext("test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void aFluxCanBeSkippedZero(){
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .skip(0)
		)
		            .expectNext("test", "test2", "test3")
		            .verifyComplete();
	}
}
