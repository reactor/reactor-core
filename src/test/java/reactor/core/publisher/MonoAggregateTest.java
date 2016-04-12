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
import reactor.core.test.TestSubscriber;

public class MonoAggregateTest {

	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(MonoAggregate.class);

		ctb.addRef("source", Mono.never());
		ctb.addRef("aggregator", (BiFunction<Object, Object, Object>) (a, b) -> b);

		ctb.test();
	}
*/
	@Test
	public void normal() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		Flux.range(1, 10)
		       .reduce((a, b) -> a + b)
		       .subscribe(ts);

		ts.assertValues(55)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		TestSubscriber<Integer> ts = new TestSubscriber<>(0L);

		Flux.range(1, 10)
		       .reduce((a, b) -> a + b)
		       .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(55)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void single() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		Flux.just(1)
		       .reduce((a, b) -> a + b)
		       .subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void empty() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		Flux.<Integer>empty().reduce((a, b) -> a + b)
		                        .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void error() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		Flux.<Integer>error(new RuntimeException("forced failure")).reduce((a, b) -> a + b)
		                                                              .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> Assert.assertTrue(e.getMessage()
		                                       .contains("forced failure")))
		  .assertNotComplete();
	}

	@Test
	public void aggregatorThrows() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		Flux.range(1, 10)
		       .reduce((a, b) -> {
			      throw new RuntimeException("forced failure");
		      })
		       .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> Assert.assertTrue(e.getMessage()
		                                           .contains("forced failure")))
		  .assertNotComplete();
	}

	@Test
	public void aggregatorReturnsNull() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		Flux.range(1, 10)
		       .reduce((a, b) -> null)
		       .subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

}
