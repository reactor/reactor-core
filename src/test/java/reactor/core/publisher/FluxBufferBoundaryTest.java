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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.TestSubscriber;

public class FluxBufferBoundaryTest {

	@Test
	public void normal() {
		TestSubscriber<List<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.buffer(sp2)
		   .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(1);
		sp1.onNext(2);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.onNext(1);

		ts.assertValues(Arrays.asList(1, 2))
		  .assertNoError()
		  .assertNotComplete();

		sp2.onNext(2);

		ts.assertValues(Arrays.asList(1, 2))
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(3);
		sp1.onNext(4);

		sp2.onComplete();

		ts.assertValues(Arrays.asList(1, 2), Arrays.asList(3, 4))
		  .assertNoError()
		  .assertComplete();

		sp1.onNext(5);
		sp1.onNext(6);
		sp1.onComplete();

		ts.assertValues(Arrays.asList(1, 2), Arrays.asList(3, 4))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void mainError() {
		TestSubscriber<List<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.buffer(sp2)
		   .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(1);
		sp1.onNext(2);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.onNext(1);

		ts.assertValues(Arrays.asList(1, 2))
		  .assertNoError()
		  .assertNotComplete();

		sp1.onError(new RuntimeException("forced failure"));

		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());

		sp2.onNext(2);

		ts.assertValues(Arrays.asList(1, 2))
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		sp2.onComplete();

		ts.assertValues(Arrays.asList(1, 2))
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void otherError() {
		TestSubscriber<List<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.buffer(sp2)
		   .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(1);
		sp1.onNext(2);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.onNext(1);

		ts.assertValues(Arrays.asList(1, 2))
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(3);

		sp2.onError(new RuntimeException("forced failure"));

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());

		ts.assertValues(Arrays.asList(1, 2))
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		sp2.onComplete();

		ts.assertValues(Arrays.asList(1, 2))
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void bufferSupplierThrows() {
		TestSubscriber<List<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.buffer(sp2, (Supplier<List<Integer>>) () -> {
			throw new RuntimeException("forced failure");
		})
		   .subscribe(ts);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void bufferSupplierThrowsLater() {
		TestSubscriber<List<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		int count[] = {1};

		sp1.buffer(sp2, (Supplier<List<Integer>>) () -> {
			if (count[0]-- > 0) {
				return new ArrayList<>();
			}
			throw new RuntimeException("forced failure");
		})
		   .subscribe(ts);

		sp1.onNext(1);
		sp1.onNext(2);

		sp2.onNext(1);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void bufferSupplierReturnsNUll() {
		TestSubscriber<List<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.buffer(sp2, (Supplier<List<Integer>>) () -> null)
		   .subscribe(ts);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

}
