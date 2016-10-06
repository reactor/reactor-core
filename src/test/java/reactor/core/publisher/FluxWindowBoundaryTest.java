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
import org.reactivestreams.Publisher;
import reactor.test.subscriber.AssertSubscriber;

public class FluxWindowBoundaryTest {

	static <T> AssertSubscriber<T> toList(Publisher<T> windows) {
		AssertSubscriber<T> ts = AssertSubscriber.create();
		windows.subscribe(ts);
		return ts;
	}

	@SafeVarargs
	static <T> void expect(AssertSubscriber<Flux<T>> ts, int index, T... values) {
		toList(ts.values()
		         .get(index)).assertValues(values)
		                     .assertComplete()
		                     .assertNoError();
	}

	@Test
	public void normal() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.window(sp2)
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.onNext(1);
		sp1.onNext(2);
		sp1.onNext(3);

		sp2.onNext(1);

		sp1.onNext(4);
		sp1.onNext(5);

		sp1.onComplete();

		ts.assertValueCount(2);

		expect(ts, 0, 1, 2, 3);
		expect(ts, 1, 4, 5);

		ts.assertNoError()
		  .assertComplete();

		Assert.assertFalse("sp1 has subscribers", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers", sp1.hasDownstreams());
	}

	@Test
	public void normalOtherCompletes() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.window(sp2)
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.onNext(1);
		sp1.onNext(2);
		sp1.onNext(3);

		sp2.onNext(1);

		sp1.onNext(4);
		sp1.onNext(5);

		sp2.onComplete();

		ts.assertValueCount(2);

		expect(ts, 0, 1, 2, 3);
		expect(ts, 1, 4, 5);

		ts.assertNoError()
		  .assertComplete();

		Assert.assertFalse("sp1 has subscribers", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers", sp1.hasDownstreams());
	}

	@Test
	public void mainError() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.window(sp2)
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.onNext(1);
		sp1.onNext(2);
		sp1.onNext(3);

		sp2.onNext(1);

		sp1.onNext(4);
		sp1.onNext(5);

		sp1.onError(new RuntimeException("forced failure"));

		ts.assertValueCount(2);

		expect(ts, 0, 1, 2, 3);

		toList(ts.values()
		         .get(1)).assertValues(4, 5)
		                 .assertError(RuntimeException.class)
		                 .assertErrorMessage("forced failure")
		                 .assertNotComplete();

		ts.assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers", sp1.hasDownstreams());
	}

	@Test
	public void otherError() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.window(sp2)
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.onNext(1);
		sp1.onNext(2);
		sp1.onNext(3);

		sp2.onNext(1);

		sp1.onNext(4);
		sp1.onNext(5);

		sp2.onError(new RuntimeException("forced failure"));

		ts.assertValueCount(2);

		expect(ts, 0, 1, 2, 3);

		toList(ts.values()
		         .get(1)).assertValues(4, 5)
		                 .assertError(RuntimeException.class)
		                 .assertErrorMessage("forced failure")
		                 .assertNotComplete();

		ts.assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers", sp1.hasDownstreams());
	}

}
