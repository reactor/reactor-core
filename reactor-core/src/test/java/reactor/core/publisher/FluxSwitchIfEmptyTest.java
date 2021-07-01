/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxSwitchIfEmptyTest {

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxSwitchIfEmpty<>(null, Flux.never());
		});
	}

	@Test
	public void otherNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never()
					.switchIfEmpty(null);
		});
	}

	@Test
	public void nonEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1, 2, 3, 4, 5)
		    .switchIfEmpty(Flux.just(10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void nonEmptyBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1, 2, 3, 4, 5)
		    .switchIfEmpty(Flux.just(10))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

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
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>empty()
		    .switchIfEmpty(Flux.just(10))
		    .subscribe(ts);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void emptyBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>empty()
		    .switchIfEmpty(Flux.just(10))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();
	}

}
