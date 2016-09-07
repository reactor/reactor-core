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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import reactor.test.TestSubscriber;

public class FluxRepeatWhenTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxRepeatWhen<>(null, v -> v);
	}

	@Test(expected = NullPointerException.class)
	public void whenFactoryNull() {
		Flux.never()
		    .repeatWhen(null);
	}

	@Test
	public void coldRepeater() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.just(1)
		    .repeatWhen(v -> Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertValues(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void coldRepeaterBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> Flux.range(1, 5))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2, 1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void coldEmpty() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> Flux.empty())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void coldError() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> Flux.error(new RuntimeException("forced " + "failure")))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void whenFactoryThrows() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 2)
		    .repeatWhen(v -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

	}

	@Test
	public void whenFactoryReturnsNull() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxRepeatWhen<>(Flux.range(1, 2), v -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();

	}

	@Test
	public void repeaterErrorsInResponse() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 2)
		    .repeatWhen(v -> new FluxMap<>(v, a -> {
			    throw new RuntimeException("forced failure");
		    }))
		    .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

	}

	@Test
	public void retryAlways() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> v)
		    .subscribe(ts);

		ts.request(8);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();
	}

	@Test
	public void retryAlwaysScalar() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		AtomicInteger count = new AtomicInteger();

		Flux.just(1)
		    .map(d -> count.incrementAndGet())
		    .repeatWhen(v -> v)
		    .subscribe(ts);

		ts.request(8);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8)
		  .assertNoError()
		  .assertNotComplete();
	}

	@Test
	public void retryWithVolumeCondition() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> v.takeWhile(n -> n > 0))
		    .subscribe(ts);

		ts.request(8);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();
	}

}
