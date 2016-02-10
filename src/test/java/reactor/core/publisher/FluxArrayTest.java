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
import reactor.core.test.TestSubscriber;

public class FluxArrayTest {

	static Flux<Integer> range(int start, int count) {
		return Flux.create(s -> {
			int c = s.context()
			          .getAndIncrement();

			if (c == start + count) {
				s.onComplete();
			}
			else {
				s.onNext(c);
				if(c + 1 == start + count){
					s.onComplete();
				}
			}
		}, s -> new AtomicInteger(start));
	}

	@Test(expected = NullPointerException.class)
	public void arrayNull() {
		new FluxArray<>((Integer[]) null);
	}

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		new FluxArray<>(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		TestSubscriber<Integer> ts = new TestSubscriber<>(0);

		new FluxArray<>(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(5);

		ts.assertNoError()
		  .assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete();

		ts.request(10);

		ts.assertNoError()
		  .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete();
	}

	@Test
	public void normalBackpressuredExact() {
		TestSubscriber<Integer> ts = new TestSubscriber<>(10);

		new FluxArray<>(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete();

		ts.request(10);

		ts.assertNoError()
		  .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete();
	}

	@Test
	public void arrayContainsNull() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		new FluxArray<>(1, 2, 3, 4, 5, null, 7, 8, 9, 10).subscribe(ts);

		ts.assertError(NullPointerException.class)
		  .assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete();
	}
}
