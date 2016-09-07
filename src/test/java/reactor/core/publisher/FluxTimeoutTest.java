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

import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.TestSubscriber;

public class FluxTimeoutTest {

	@Test
	public void noTimeout() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(), v -> Flux.never())
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void immediateTimeout() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.empty(), v -> Flux.never())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(TimeoutException.class);
	}

	@Test
	public void firstElemenetImmediateTimeout() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(), v -> Flux.empty())
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(TimeoutException.class);
	}

	//Fail
	//@Test
	public void immediateTimeoutResume() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.empty(), v -> Flux.never(), Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void firstElemenetImmediateResume() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(), v -> Flux.empty(), Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertValues(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oldTimeoutHasNoEffect() {
		DirectProcessor<Integer> source = DirectProcessor.create();

		DirectProcessor<Integer> tp = DirectProcessor.create();

		TestSubscriber<Integer> ts = TestSubscriber.create();

		source.timeout(tp, v -> Flux.never(), Flux.range(1, 10))
		      .subscribe(ts);

		source.onNext(0);

		tp.onNext(1);

		source.onComplete();

		Assert.assertFalse("Timeout has subscribers?", tp.hasDownstreams());

		ts.assertValues(0)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oldTimeoutCompleteHasNoEffect() {
		DirectProcessor<Integer> source = DirectProcessor.create();

		DirectProcessor<Integer> tp = DirectProcessor.create();

		TestSubscriber<Integer> ts = TestSubscriber.create();

		source.timeout(tp, v -> Flux.never(), Flux.range(1, 10))
		      .subscribe(ts);

		source.onNext(0);

		tp.onComplete();

		source.onComplete();

		Assert.assertFalse("Timeout has subscribers?", tp.hasDownstreams());

		ts.assertValues(0)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oldTimeoutErrorHasNoEffect() {
		DirectProcessor<Integer> source = DirectProcessor.create();

		DirectProcessor<Integer> tp = DirectProcessor.create();

		TestSubscriber<Integer> ts = TestSubscriber.create();

		source.timeout(tp, v -> Flux.never(), Flux.range(1, 10))
		      .subscribe(ts);

		source.onNext(0);

		tp.onError(new RuntimeException("forced failure"));

		source.onComplete();

		Assert.assertFalse("Timeout has subscribers?", tp.hasDownstreams());

		ts.assertValues(0)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void itemTimeoutThrows() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(), v -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void itemTimeoutReturnsNull() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(), v -> null)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void firstTimeoutError() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.error(new RuntimeException("forced " + "failure")),
				    v -> Flux.never())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void itemTimeoutError() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .timeout(Flux.never(),
				    v -> Flux.error(new RuntimeException("forced failure")))
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void timeoutRequested() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		DirectProcessor<Integer> source = DirectProcessor.create();

		DirectProcessor<Integer> tp = DirectProcessor.create();

		source.timeout(tp, v -> tp)
		      .subscribe(ts);

		tp.onNext(1);

		source.onNext(2);
		source.onComplete();

		ts.assertNoValues()
		  .assertError(TimeoutException.class)
		  .assertNotComplete();
	}
}
