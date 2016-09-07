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
import reactor.test.TestSubscriber;

public class FluxSwitchMapTest {

	@Test
	public void noswitch() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.switchMap(v -> sp2)
		   .subscribe(ts);

		sp1.onNext(1);

		sp2.onNext(10);
		sp2.onNext(20);
		sp2.onNext(30);
		sp2.onNext(40);
		sp2.onComplete();

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertNotComplete();

		sp1.onComplete();

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void noswitchBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.switchMap(v -> sp2)
		   .subscribe(ts);

		sp1.onNext(1);

		sp2.onNext(10);
		sp2.onNext(20);
		sp2.onNext(30);
		sp2.onNext(40);
		sp2.onComplete();

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10, 20)
		  .assertNoError()
		  .assertNotComplete();

		sp1.onComplete();

		ts.assertValues(10, 20)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void doswitch() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();

		sp1.switchMap(v -> v == 1 ? sp2 : sp3)
		   .subscribe(ts);

		sp1.onNext(1);

		sp2.onNext(10);
		sp2.onNext(20);

		sp1.onNext(2);

		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());

		sp2.onNext(30);
		sp3.onNext(300);
		sp2.onNext(40);
		sp3.onNext(400);
		sp2.onComplete();
		sp3.onComplete();

		ts.assertValues(10, 20, 300, 400)
		  .assertNoError()
		  .assertNotComplete();

		sp1.onComplete();

		ts.assertValues(10, 20, 300, 400)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void mainCompletesBefore() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.switchMap(v -> sp2)
		   .subscribe(ts);

		sp1.onNext(1);
		sp1.onComplete();

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.onNext(10);
		sp2.onNext(20);
		sp2.onNext(30);
		sp2.onNext(40);
		sp2.onComplete();

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void mainError() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.switchMap(v -> sp2)
		   .subscribe(ts);

		sp1.onNext(1);
		sp1.onError(new RuntimeException("forced failure"));

		sp2.onNext(10);
		sp2.onNext(20);
		sp2.onNext(30);
		sp2.onNext(40);
		sp2.onComplete();

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void innerError() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.switchMap(v -> sp2)
		   .subscribe(ts);

		sp1.onNext(1);

		sp2.onNext(10);
		sp2.onNext(20);
		sp2.onNext(30);
		sp2.onNext(40);
		sp2.onError(new RuntimeException("forced failure"));

		ts.assertValues(10, 20, 30, 40)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
	}

	@Test
	public void mapperThrows() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();

		sp1.switchMap(v -> {
			throw new RuntimeException("forced failure");
		})
		   .subscribe(ts);

		sp1.onNext(1);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void mapperReturnsNull() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();

		sp1.switchMap(v -> null)
		   .subscribe(ts);

		sp1.onNext(1);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}
}
