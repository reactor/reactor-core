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
import reactor.test.TestSubscriber;

public class FluxSampleFirstTest {

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();

		sp1.sampleFirst(v -> v == 1 ? sp2 : sp3)
		   .subscribe(ts);

		sp1.onNext(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(2);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		sp2.onNext(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(3);

		ts.assertValues(1, 3)
		  .assertNoError()
		  .assertNotComplete();

		sp1.onComplete();

		ts.assertValues(1, 3)
		  .assertNoError()
		  .assertComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp3.hasDownstreams());
	}

	@Test
	public void mainError() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();

		sp1.sampleFirst(v -> v == 1 ? sp2 : sp3)
		   .subscribe(ts);

		sp1.onNext(1);
		sp1.onError(new RuntimeException("forced failure"));

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp3.hasDownstreams());
	}

	@Test
	public void throttlerError() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();

		sp1.sampleFirst(v -> v == 1 ? sp2 : sp3)
		   .subscribe(ts);

		sp1.onNext(1);
		sp2.onError(new RuntimeException("forced failure"));

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp3.hasDownstreams());
	}

	@Test
	public void throttlerThrows() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();

		sp1.sampleFirst(v -> {
			throw new RuntimeException("forced failure");
		})
		   .subscribe(ts);

		sp1.onNext(1);

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
	}

	@Test
	public void throttlerReturnsNull() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();

		sp1.sampleFirst(v -> null)
		   .subscribe(ts);

		sp1.onNext(1);

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
	}

}
