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
import java.util.function.Supplier;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.test.TestSubscriber;

public class FluxWindowTest {

	// javac can't handle these inline and fails with type inference error
	final Supplier<Queue<Integer>>                   pqs = ConcurrentLinkedQueue::new;
	final Supplier<Queue<UnicastProcessor<Integer>>> oqs = ConcurrentLinkedQueue::new;

	@Test(expected = NullPointerException.class)
	public void source1Null() {
		new FluxWindow<>(null, 1, pqs);
	}

	@Test(expected = NullPointerException.class)
	public void source2Null() {
		new FluxWindow<>(null, 1, 2, pqs, oqs);
	}

	@Test(expected = NullPointerException.class)
	public void processorQueue1Null() {
		new FluxWindow<>(Flux.never(), 1, null);
	}

	@Test(expected = NullPointerException.class)
	public void processorQueue2Null() {
		new FluxWindow<>(Flux.never(), 1, 1, null, oqs);
	}

	@Test(expected = NullPointerException.class)
	public void overflowQueueNull() {
		new FluxWindow<>(Flux.never(), 1, 1, pqs, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void size1Invalid() {
		Flux.never()
		    .window(0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void size2Invalid() {
		Flux.never()
		    .window(0, 2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void skipInvalid() {
		Flux.never()
		    .window(1, 0);
	}

	@Test
	public void processorQueue1ReturnsNull() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		new FluxWindow<>(Flux.range(1, 10), 1, 1, () -> null, oqs).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void processorQueue2ReturnsNull() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		new FluxWindow<>(Flux.range(1, 10), 1, 2, () -> null, oqs).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void overflowQueueReturnsNull() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		new FluxWindow<>(Flux.range(1, 10), 2, 1, pqs, () -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	static <T> Supplier<T> throwing() {
		return () -> {
			throw new RuntimeException("forced failure");
		};
	}

	@Test
	public void processorQueue1Throws() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		new FluxWindow<>(Flux.range(1, 10), 1, throwing()).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void processorQueue2Throws() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		new FluxWindow<>(Flux.range(1, 10), 1, 2, throwing(), oqs).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void overflowQueueThrows() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		new FluxWindow<>(Flux.range(1, 10), 2, 1, pqs, throwing()).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	static <T> TestSubscriber<T> toList(Publisher<T> windows) {
		TestSubscriber<T> ts = TestSubscriber.create();
		windows.subscribe(ts);
		return ts;
	}

	@Test
	public void exact() {
		TestSubscriber<Publisher<Integer>> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .window(3)
		    .subscribe(ts);

		ts.assertValueCount(4)
		  .assertComplete()
		  .assertNoError();

		toList(ts.values()
		         .get(0)).assertValues(1, 2, 3)
		                 .assertComplete()
		                 .assertNoError();

		toList(ts.values()
		         .get(1)).assertValues(4, 5, 6)
		                 .assertComplete()
		                 .assertNoError();

		toList(ts.values()
		         .get(2)).assertValues(7, 8, 9)
		                 .assertComplete()
		                 .assertNoError();

		toList(ts.values()
		         .get(3)).assertValues(10)
		                 .assertComplete()
		                 .assertNoError();
	}

	@Test
	public void exactBackpressured() {
		TestSubscriber<Publisher<Integer>> ts = TestSubscriber.create(0L);

		Flux.range(1, 10)
		    .window(3)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValueCount(1)
		  .assertNotComplete()
		  .assertNoError();

		toList(ts.values()
		         .get(0)).assertValues(1, 2, 3)
		                 .assertComplete()
		                 .assertNoError();

		ts.request(1);

		ts.assertValueCount(2)
		  .assertNotComplete()
		  .assertNoError();

		toList(ts.values()
		         .get(1)).assertValues(4, 5, 6)
		                 .assertComplete()
		                 .assertNoError();

		ts.request(1);

		ts.assertValueCount(3)
		  .assertNotComplete()
		  .assertNoError();

		toList(ts.values()
		         .get(2)).assertValues(7, 8, 9)
		                 .assertComplete()
		                 .assertNoError();

		ts.request(1);

		ts.assertValueCount(4)
		  .assertComplete()
		  .assertNoError();

		toList(ts.values()
		         .get(3)).assertValues(10)
		                 .assertComplete()
		                 .assertNoError();
	}

	@Test
	public void exactWindowCount() {
		TestSubscriber<Publisher<Integer>> ts = TestSubscriber.create();

		Flux.range(1, 9).window(3).subscribe(ts);

		ts.assertValueCount(3)
		  .assertComplete()
		  .assertNoError();

		toList(ts.values()
		         .get(0)).assertValues(1, 2, 3)
		                 .assertComplete()
		                 .assertNoError();

		toList(ts.values()
		         .get(1)).assertValues(4, 5, 6)
		                 .assertComplete()
		                 .assertNoError();

		toList(ts.values()
		         .get(2)).assertValues(7, 8, 9)
		                 .assertComplete()
		                 .assertNoError();
	}

	@Test
	public void skip() {
		TestSubscriber<Publisher<Integer>> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .window(2, 3)
		    .subscribe(ts);

		ts.assertValueCount(4)
		  .assertComplete()
		  .assertNoError();

		toList(ts.values()
		         .get(0)).assertValues(1, 2)
		                 .assertComplete()
		                 .assertNoError();

		toList(ts.values()
		         .get(1)).assertValues(4, 5)
		                 .assertComplete()
		                 .assertNoError();

		toList(ts.values()
		         .get(2)).assertValues(7, 8)
		                 .assertComplete()
		                 .assertNoError();

		toList(ts.values()
		         .get(3)).assertValues(10)
		                 .assertComplete()
		                 .assertNoError();
	}

	@Test
	public void skipBackpressured() {
		TestSubscriber<Publisher<Integer>> ts = TestSubscriber.create(0L);

		Flux.range(1, 10)
		    .window(2, 3)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValueCount(1)
		  .assertNotComplete()
		  .assertNoError();

		toList(ts.values()
		         .get(0)).assertValues(1, 2)
		                 .assertComplete()
		                 .assertNoError();

		ts.request(1);

		ts.assertValueCount(2)
		  .assertNotComplete()
		  .assertNoError();

		toList(ts.values()
		         .get(1)).assertValues(4, 5)
		                 .assertComplete()
		                 .assertNoError();

		ts.request(1);

		ts.assertValueCount(3)
		  .assertNotComplete()
		  .assertNoError();

		toList(ts.values()
		         .get(2)).assertValues(7, 8)
		                 .assertComplete()
		                 .assertNoError();

		ts.request(1);

		ts.assertValueCount(4)
		  .assertComplete()
		  .assertNoError();

		toList(ts.values()
		         .get(3)).assertValues(10)
		                 .assertComplete()
		                 .assertNoError();
	}

	@SafeVarargs
	static <T> void expect(TestSubscriber<Publisher<T>> ts, int index, T... values) {
		toList(ts.values()
		         .get(index)).assertValues(values)
		                     .assertComplete()
		                     .assertNoError();
	}

	@Test
	public void overlap() {
		TestSubscriber<Publisher<Integer>> ts = TestSubscriber.create();

		Flux.range(1, 10)
		    .window(3, 1)
		    .subscribe(ts);

		ts.assertValueCount(10)
		  .assertComplete()
		  .assertNoError();

		expect(ts, 0, 1, 2, 3);
		expect(ts, 1, 2, 3, 4);
		expect(ts, 2, 3, 4, 5);
		expect(ts, 3, 4, 5, 6);
		expect(ts, 4, 5, 6, 7);
		expect(ts, 5, 6, 7, 8);
		expect(ts, 6, 7, 8, 9);
		expect(ts, 7, 8, 9, 10);
		expect(ts, 8, 9, 10);
		expect(ts, 9, 10);
	}

	@Test
	public void overlapBackpressured() {
		TestSubscriber<Publisher<Integer>> ts = TestSubscriber.create(0L);

		Flux.range(1, 10)
		    .window(3, 1)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		for (int i = 0; i < 10; i++) {
			ts.request(1);

			ts.assertValueCount(i + 1)
			  .assertNoError();

			if (i == 9) {
				ts.assertComplete();
			}
			else {
				ts.assertNotComplete();
			}

			switch (i) {
				case 9:
					expect(ts, 9, 10);
					break;
				case 8:
					expect(ts, 8, 9, 10);
					break;
				case 7:
					expect(ts, 7, 8, 9, 10);
					break;
				case 6:
					expect(ts, 6, 7, 8, 9);
					break;
				case 5:
					expect(ts, 5, 6, 7, 8);
					break;
				case 4:
					expect(ts, 4, 5, 6, 7);
					break;
				case 3:
					expect(ts, 3, 4, 5, 6);
					break;
				case 2:
					expect(ts, 2, 3, 4, 5);
					break;
				case 1:
					expect(ts, 1, 2, 3, 4);
					break;
				case 0:
					expect(ts, 0, 1, 2, 3);
					break;
			}
		}
	}

	@Test
	public void exactError() {
		TestSubscriber<Publisher<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp = DirectProcessor.create();

		sp.window(2, 2)
		  .subscribe(ts);

		ts.assertValueCount(0)
		  .assertNotComplete()
		  .assertNoError();

		sp.onNext(1);
		sp.onError(new RuntimeException("forced failure"));

		ts.assertValueCount(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		toList(ts.values()
		         .get(0)).assertValues(1)
		                 .assertNotComplete()
		                 .assertError(RuntimeException.class)
		                 .assertErrorMessage("forced failure");
	}

	@Test
	public void skipError() {
		TestSubscriber<Publisher<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp = DirectProcessor.create();

		sp.window(2, 3)
		  .subscribe(ts);

		ts.assertValueCount(0)
		  .assertNotComplete()
		  .assertNoError();

		sp.onNext(1);
		sp.onError(new RuntimeException("forced failure"));

		ts.assertValueCount(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		toList(ts.values()
		         .get(0)).assertValues(1)
		                 .assertNotComplete()
		                 .assertError(RuntimeException.class)
		                 .assertErrorMessage("forced failure");
	}

	@Test
	public void skipInGapError() {
		TestSubscriber<Publisher<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp = DirectProcessor.create();

		sp.window(1, 3)
		  .subscribe(ts);

		ts.assertValueCount(0)
		  .assertNotComplete()
		  .assertNoError();

		sp.onNext(1);
		sp.onNext(2);
		sp.onError(new RuntimeException("forced failure"));

		ts.assertValueCount(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		expect(ts, 0, 1);
	}

	@Test
	public void overlapError() {
		TestSubscriber<Publisher<Integer>> ts = TestSubscriber.create();

		DirectProcessor<Integer> sp = DirectProcessor.create();

		sp.window(2, 1)
		  .subscribe(ts);

		ts.assertValueCount(0)
		  .assertNotComplete()
		  .assertNoError();

		sp.onNext(1);
		sp.onError(new RuntimeException("forced failure"));

		ts.assertValueCount(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		toList(ts.values()
		         .get(0)).assertValues(1)
		                 .assertNotComplete()
		                 .assertError(RuntimeException.class)
		                 .assertErrorMessage("forced failure");
	}
}
