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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.test.TestSubscriber;

public class FluxWindowStartEndTest {

	static <T> TestSubscriber<T> toList(Publisher<T> windows) {
		TestSubscriber<T> ts = TestSubscriber.create();
		windows.subscribe(ts);
		return ts;
	}

	@SafeVarargs
	static <T> void expect(TestSubscriber<Flux<T>> ts, int index, T... values) {
		toList(ts.values()
		         .get(index)).assertValues(values)
		                     .assertComplete()
		                     .assertNoError();
	}

	@Test
	public void normal() {
		TestSubscriber<Flux<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();
		DirectProcessor<Integer> sp4 = DirectProcessor.create();

		sp1.window(sp2, v -> v == 1 ? sp3 : sp4)
		   .subscribe(ts);

		sp1.onNext(1);

		sp2.onNext(1);

		sp1.onNext(2);

		sp2.onNext(2);

		sp1.onNext(3);

		sp3.onNext(1);

		sp1.onNext(4);

		sp4.onNext(1);

		sp1.onComplete();

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 2, 3);
		expect(ts, 1, 3, 4);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp3 has subscribers?", sp3.hasDownstreams());
		Assert.assertFalse("sp4 has subscribers?", sp4.hasDownstreams());
	}

	@Test
	public void normalStarterEnds() {
		TestSubscriber<Flux<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();
		DirectProcessor<Integer> sp4 = DirectProcessor.create();

		sp1.window(sp2, v -> v == 1 ? sp3 : sp4)
		   .subscribe(ts);

		sp1.onNext(1);

		sp2.onNext(1);

		sp1.onNext(2);

		sp2.onNext(2);

		sp1.onNext(3);

		sp3.onNext(1);

		sp1.onNext(4);

		sp4.onNext(1);

		sp2.onComplete();

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 2, 3);
		expect(ts, 1, 3, 4);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp3 has subscribers?", sp3.hasDownstreams());
		Assert.assertFalse("sp4 has subscribers?", sp4.hasDownstreams());
	}

	@Test
	public void oneWindowOnly() {
		TestSubscriber<Flux<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();
		DirectProcessor<Integer> sp4 = DirectProcessor.create();

		sp1.window(sp2, v -> v == 1 ? sp3 : sp4)
		   .subscribe(ts);

		sp2.onNext(1);
		sp2.onComplete();

		sp1.onNext(1);
		sp1.onNext(2);
		sp1.onNext(3);

		sp3.onComplete();

		sp1.onNext(4);

		ts.assertValueCount(1)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 1, 2, 3);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp3 has subscribers?", sp3.hasDownstreams());
		Assert.assertFalse("sp4 has subscribers?", sp4.hasDownstreams());
	}

}
