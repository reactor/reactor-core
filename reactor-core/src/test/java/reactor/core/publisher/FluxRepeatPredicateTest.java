/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.List;

import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxRepeatPredicateTest {

	@Test(expected = NullPointerException.class)
	public void predicateNull() {
		Flux.never()
		    .repeat(null);
	}

	@Test
	public void normal() {
		int[] times = {1};

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .repeat(() -> times[0]-- > 0)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		int[] times = {1};

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .repeat(() -> times[0]-- > 0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void dontRepeat() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .repeat(() -> false)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void predicateThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .repeat(() -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void repeatPredicateAndMaxZero() {
		StepVerifier.create(Flux.just(1)
		                        .repeat(0, () -> false))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void repeatPredicateAndMaxOne() {
		StepVerifier.create(Flux.just(1)
				.repeat(1, () -> true))
		            .expectNext(1, 1)
		            .verifyComplete();
	}

	@Test
	public void repeatPredicateAndMaxOneAndPredicateFail() {
		StepVerifier.create(Flux.just(1)
				.repeat(1, () -> false))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void repeatAndRepeatWithNeutralPredicateAreOffByOne() {
		final List<Integer> simpleRepeat = Flux.just(1)
		                                       .repeat(3)
		                                       .collectList()
		                                       .block();

		final List<Integer> predicateRepeat = Flux.just(1)
		                                          .repeat(3, () -> true)
		                                          .collectList()
		                                          .block();

		assertThat(predicateRepeat)
				.containsAll(simpleRepeat)
				.hasSize(simpleRepeat.size() + 1)
				.endsWith(1);
	}
}
