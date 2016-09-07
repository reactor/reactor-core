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
import reactor.test.TestSubscriber;

public class FluxOnBackpressureLatestTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxOnBackpressureLatest<>(null);
	}

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 10).onBackpressureLatest().subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void backpressured() {
		DirectProcessor<Integer> tp = DirectProcessor.create();

		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		tp.onBackpressureLatest().subscribe(ts);

		tp.onNext(1);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		tp.onNext(2);

		ts.request(1);

		ts.assertValues(2)
		  .assertNoError()
		  .assertNotComplete();

		tp.onNext(3);
		tp.onNext(4);

		ts.request(2);

		ts.assertValues(2, 4)
		  .assertNoError()
		  .assertNotComplete();

		tp.onNext(5);
		tp.onComplete();

		ts.assertValues(2, 4, 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void error() {
		DirectProcessor<Integer> tp = DirectProcessor.create();

		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		tp.onBackpressureLatest().subscribe(ts);

		tp.onError(new RuntimeException("forced failure"));

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void backpressureWithDrop() {
		DirectProcessor<Integer> tp = DirectProcessor.create();

		TestSubscriber<Integer> ts = new TestSubscriber<Integer>(0) {
			@Override
			public void onNext(Integer t) {
				super.onNext(t);
				if (t == 2) {
					tp.onNext(3);
				}
			}
		};

		tp.onBackpressureLatest()
		  .subscribe(ts);

		tp.onNext(1);
		tp.onNext(2);

		ts.request(1);

		ts.assertValues(2)
		  .assertNoError()
		  .assertNotComplete();

	}
}
