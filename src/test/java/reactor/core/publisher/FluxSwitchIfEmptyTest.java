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
import reactor.test.TestSubscriber;

public class FluxSwitchIfEmptyTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxSwitchIfEmpty<>(null, FluxNever.instance());
	}

	@Test(expected = NullPointerException.class)
	public void otherNull() {
		new FluxSwitchIfEmpty<>(FluxNever.instance(), null);
	}

	@Test
	public void nonEmpty() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxSwitchIfEmpty<>(Flux.just(1, 2, 3, 4, 5), new FluxJust<>(10)).subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void nonEmptyBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		new FluxSwitchIfEmpty<>(Flux.just(1, 2, 3, 4, 5), new FluxJust<>(10)).subscribe(ts);

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
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxSwitchIfEmpty<>(Flux.empty(), new FluxJust<>(10)).subscribe(ts);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void emptyBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		new FluxSwitchIfEmpty<>(Flux.empty(), new FluxJust<>(10)).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();
	}

}
