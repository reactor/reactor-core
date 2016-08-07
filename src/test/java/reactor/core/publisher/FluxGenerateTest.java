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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;

import reactor.core.Fuseable;
import reactor.test.TestSubscriber;

public class FluxGenerateTest {

	@Test(expected = NullPointerException.class)
	public void stateSupplierNull() {
		new FluxGenerate<>(null, (s, o) -> s, s -> {
		});
	}

	@Test(expected = NullPointerException.class)
	public void generatorNull() {
		new FluxGenerate<>(() -> 1, null, s -> {
		});
	}

	@Test(expected = NullPointerException.class)
	public void stateConsumerNull() {
		new FluxGenerate<>(() -> 1, (s, o) -> s, null);
	}

	@Test
	public void generateEmpty() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxGenerate<Integer, Void>((s, o) -> {
			o.complete();
			return s;
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void generateJust() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxGenerate<Integer, Void>((s, o) -> {
			o.next(1);
			o.complete();
			return s;
		}).subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void generateError() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxGenerate<Integer, Void>((s, o) -> {
			o.error(new RuntimeException("forced failure"));
			return s;
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		;
	}


	@Test
	public void generateJustBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		new FluxGenerate<Integer, Void>((s, o) -> {
			o.next(1);
			o.complete();
			return s;
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void generateRange() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxGenerate<Integer, Integer>(() -> 1, (s, o) -> {
			if (s < 11) {
				o.next(s);
			} else {
				o.complete();
			}
			return s + 1;
		}).subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void generateRangeBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		new FluxGenerate<Integer, Integer>(() -> 1, (s, o) -> {
			if (s < 11) {
				o.next(s);
			} else {
				o.complete();
			}
			return s + 1;
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void stateSupplierThrows() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxGenerate<Integer, Integer>(() -> {
			throw new RuntimeException("forced failure");
		}, (s, o) -> {
			o.next(1);
			return s;
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class);
	}

	@Test
	public void generatorThrows() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxGenerate<Integer, Integer>((s, o) -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void generatorMultipleOnErrors() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxGenerate<Integer, Integer>((s, o) -> {
			o.error(new RuntimeException("forced failure"));
			o.error(new RuntimeException("forced failure"));
			return s;
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void generatorMultipleOnCompletes() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxGenerate<Integer, Integer>((s, o) -> {
			o.complete();
			o.complete();
			return s;
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void generatorMultipleOnNexts() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new FluxGenerate<Integer, Integer>((s, o) -> {
			o.next(1);
			o.next(1);
			return s;
		}).subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(IllegalStateException.class);
	}

	@Test
	public void stateConsumerCalled() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		AtomicInteger stateConsumer = new AtomicInteger();

		new FluxGenerate<Integer, Integer>(() -> 1, (s, o) -> {
			o.complete();
			return s;
		}, stateConsumer::set).subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();

		Assert.assertEquals(1, stateConsumer.get());
	}

	@Test
	public void iterableSource() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		new FluxGenerate<Integer, Iterator<Integer>>(
		  () -> list.iterator(),
		  (s, o) -> {
			  if (s.hasNext()) {
				  o.next(s.next());
			  } else {
				  o.complete();
			  }
			  return s;
		  }).subscribe(ts);

		ts.assertValueSequence(list)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void iterableSourceBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		new FluxGenerate<Integer, Iterator<Integer>>(
		  () -> list.iterator(),
		  (s, o) -> {
			  if (s.hasNext()) {
				  o.next(s.next());
			  } else {
				  o.complete();
			  }
			  return s;
		  }).subscribe(ts);

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
		ts.assertValueSequence(list)
		  .assertComplete()
		  .assertNoError();
	}
	
	@Test
	public void fusion() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		ts.requestedFusionMode(Fuseable.ANY);
		
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		Flux.<Integer, Iterator<Integer>>generate(
		  () -> list.iterator(),
		  (s, o) -> {
			  if (s.hasNext()) {
				  o.next(s.next());
			  } else {
				  o.complete();
			  }
			  return s;
		  }).subscribe(ts);
		
		ts.assertFuseableSource()
		.assertFusionMode(Fuseable.SYNC)
		.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		;
	}

	@Test
	public void fusionBoundary() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		ts.requestedFusionMode(Fuseable.ANY | Fuseable.THREAD_BARRIER);
		
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		Flux.<Integer, Iterator<Integer>>generate(list::iterator,
		  (s, o) -> {
			  if (s.hasNext()) {
				  o.next(s.next());
			  } else {
				  o.complete();
			  }
			  return s;
		  }).subscribe(ts);
		
		ts.assertFuseableSource()
		.assertFusionMode(Fuseable.NONE)
		.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		;
	}

}
