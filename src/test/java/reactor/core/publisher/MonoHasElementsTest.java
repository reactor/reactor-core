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
import reactor.core.test.TestSubscriber;

public class MonoHasElementsTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new MonoHasElements<>(null);
	}

	@Test
	public void emptySource() {
		TestSubscriber<Boolean> ts = TestSubscriber.create();

		Mono.empty().hasElement().subscribe(ts);

		ts.assertValues(false)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void emptySourceBackpressured() {
		TestSubscriber<Boolean> ts = TestSubscriber.create(0);

		Mono.empty().hasElement().subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(1);

		ts.assertValues(false)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void nonEmptySource() {
		TestSubscriber<Boolean> ts = TestSubscriber.create();

		Flux.range(1, 10).hasElements().subscribe(ts);

		ts.assertValues(true)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void nonEmptySourceBackpressured() {
		TestSubscriber<Boolean> ts = TestSubscriber.create(0);

		Flux.range(1, 10).hasElements().subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(1);

		ts.assertValues(true)
		  .assertComplete()
		  .assertNoError();
	}
}
