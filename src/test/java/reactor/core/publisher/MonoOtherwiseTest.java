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

public class MonoOtherwiseTest {
/*
	@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(FluxResume.class);
		
		ctb.addRef("source", FluxNever.instance());
		ctb.addRef("nextFactory", (Function<Throwable, Publisher<Object>>)e -> FluxNever.instance());
		
		ctb.test();
	}*/

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Mono.just(1)
		    .otherwise(v -> Mono.just(2))
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Mono.just(1)
		    .otherwise(v -> Mono.just(2))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void error() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Mono.<Integer>error(new RuntimeException("forced failure")).otherwise(v -> Mono.just(
				2))
		                                                           .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorFiltered() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Mono.<Integer>error(new RuntimeException("forced failure"))
				.otherwise(e -> e.getMessage().equals("forced failure"), v -> Mono.just(2))
				.subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorMap() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Mono.<Integer>error(new Exception()).mapError(d -> new RuntimeException("forced" +
				" " +
				"failure"))
		                                    .subscribe(ts);

		ts.assertNoValues()
		  .assertError()
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void errorBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Mono.<Integer>error(new RuntimeException("forced failure")).otherwise(v -> Mono.just(
				2))
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(2)
		  .assertComplete();
	}

	@Test
	public void nextFactoryThrows() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Mono.<Integer>error(new RuntimeException("forced failure")).otherwise(v -> {
			throw new RuntimeException("forced failure 2");
		})
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> Assert.assertTrue(e.getMessage()
		                                           .contains("forced failure 2")));
	}

	@Test
	public void nextFactoryReturnsNull() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Mono.<Integer>error(new RuntimeException("forced failure")).otherwise(v -> null)
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

}
