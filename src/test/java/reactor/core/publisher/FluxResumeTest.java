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
import reactor.core.test.TestSubscriber;

public class FluxResumeTest {
/*
	@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(FluxResume.class);
		
		ctb.addRef("source", FluxNever.instance());
		ctb.addRef("nextFactory", (Function<Throwable, Publisher<Object>>)e -> FluxNever.instance());
		
		ctb.test();
	}*/

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		new FluxResume<>(Flux.range(1, 10), v -> Flux.range(11, 10)).subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		TestSubscriber<Integer> ts = new TestSubscriber<>(0);

		new FluxResume<>(Flux.range(1, 10), v -> Flux.range(11, 10)).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void error() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		new FluxResume<>(Flux.error(new RuntimeException("forced failure")), v -> Flux.range
		  (11, 10)).subscribe(ts);

		ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorBackpressured() {
		TestSubscriber<Integer> ts = new TestSubscriber<>(0);

		new FluxResume<>(Flux.error(new RuntimeException("forced failure")), v -> Flux.range
		  (11, 10)).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(11, 12)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(11, 12, 13, 14, 15, 16, 17)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void someFirst() {
		EmitterProcessor<Integer> tp = EmitterProcessor.create();

		tp.connect();

		TestSubscriber<Integer> ts = new TestSubscriber<>();

		new FluxResume<>(tp, v -> Flux.range(11, 10)).subscribe(ts);

		tp.onNext(1);
		tp.onNext(2);
		tp.onNext(3);
		tp.onNext(4);
		tp.onNext(5);
		tp.onError(new RuntimeException("forced failure"));

		ts.assertValues(1, 2, 3, 4, 5, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void someFirstBackpressured() {
		EmitterProcessor<Integer> tp = EmitterProcessor.create();

		tp.connect();

		TestSubscriber<Integer> ts = new TestSubscriber<>(10);

		new FluxResume<>(tp, v -> Flux.range(11, 10)).subscribe(ts);

		tp.onNext(1);
		tp.onNext(2);
		tp.onNext(3);
		tp.onNext(4);
		tp.onNext(5);
		tp.onError(new RuntimeException("forced failure"));

		ts.assertValues(1, 2, 3, 4, 5, 11, 12, 13, 14, 15)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void nextFactoryThrows() {
		TestSubscriber<Integer> ts = new TestSubscriber<>(0);

		new FluxResume<Integer>(Flux.error(new RuntimeException("forced failure")), v -> {
			throw new RuntimeException("forced failure 2");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure 2")));
	}

	@Test
	public void nextFactoryReturnsNull() {
		TestSubscriber<Integer> ts = new TestSubscriber<>(0);

		new FluxResume<Integer>(Flux.error(new RuntimeException("forced failure")), v -> null)
		  .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

}
