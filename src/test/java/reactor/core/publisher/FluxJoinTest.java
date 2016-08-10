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

import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.Test;
import reactor.test.TestSubscriber;

public class FluxJoinTest {

	final BiFunction<Integer, Integer, Integer> add = (t1, t2) -> t1 + t2;

	<T> Function<Integer, Flux<T>> just(final Flux<T> publisher) {
		return t1 -> publisher;
	}

	@Test
	public void normal1() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

		Flux<Integer> m =
				source1.join(source2, just(Flux.never()), just(Flux.never()), add);

		m.subscribe(ts);

		source1.onNext(1);
		source1.onNext(2);
		source1.onNext(4);

		source2.onNext(16);
		source2.onNext(32);
		source2.onNext(64);

		source1.onComplete();
		source2.onComplete();

		ts.assertValues(17, 18, 20, 33, 34, 36, 65, 66, 68)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normal1WithDuration() {
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

		DirectProcessor<Integer> duration1 = new DirectProcessor<>();

		Flux<Integer> m = source1.join(source2, just(duration1), just(Flux.never()), add);
		m.subscribe(ts);

		source1.onNext(1);
		source1.onNext(2);
		source2.onNext(16);

		duration1.onNext(1);

		source1.onNext(4);
		source1.onNext(8);

		source1.onComplete();
		source2.onComplete();

		ts.assertValues(17, 18, 20, 24)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void normal2() {
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

		Flux<Integer> m =
				source1.join(source2, just(Flux.never()), just(Flux.never()), add);

		m.subscribe(ts);

		source1.onNext(1);
		source1.onNext(2);
		source1.onComplete();

		source2.onNext(16);
		source2.onNext(32);
		source2.onNext(64);

		source2.onComplete();

		ts.assertValues(17, 18, 33, 34, 65, 66)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void leftThrows() {
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

		Flux<Integer> m =
				source1.join(source2, just(Flux.never()), just(Flux.never()), add);

		m.subscribe(ts);

		source2.onNext(1);
		source1.onError(new RuntimeException("Forced failure"));

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void rightThrows() {
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

		Flux<Integer> m =
				source1.join(source2, just(Flux.never()), just(Flux.never()), add);

		m.subscribe(ts);

		source1.onNext(1);
		source2.onError(new RuntimeException("Forced failure"));

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void leftDurationThrows() {
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

		Flux<Integer> duration1 = Flux.error(new RuntimeException("Forced failure"));

		Flux<Integer> m = source1.join(source2, just(duration1), just(Flux.never()), add);
		m.subscribe(ts);

		source1.onNext(1);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void rightDurationThrows() {
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

		Flux<Integer> duration1 = Flux.error(new RuntimeException("Forced failure"));

		Flux<Integer> m = source1.join(source2, just(Flux.never()), just(duration1), add);
		m.subscribe(ts);

		source2.onNext(1);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void leftDurationSelectorThrows() {
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

		Function<Integer, Flux<Integer>> fail = t1 -> {
			throw new RuntimeException("Forced failure");
		};

		Flux<Integer> m = source1.join(source2, fail, just(Flux.never()), add);
		m.subscribe(ts);

		source1.onNext(1);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void rightDurationSelectorThrows() {
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

		Function<Integer, Flux<Integer>> fail = t1 -> {
			throw new RuntimeException("Forced failure");
		};

		Flux<Integer> m = source1.join(source2, just(Flux.never()), fail, add);
		m.subscribe(ts);

		source2.onNext(1);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}

	@Test
	public void resultSelectorThrows() {
		TestSubscriber<Object> ts = TestSubscriber.create();
		DirectProcessor<Integer> source1 = new DirectProcessor<>();
		DirectProcessor<Integer> source2 = new DirectProcessor<>();

		BiFunction<Integer, Integer, Integer> fail = (t1, t2) -> {
			throw new RuntimeException("Forced failure");
		};

		Flux<Integer> m =
				source1.join(source2, just(Flux.never()), just(Flux.never()), fail);
		m.subscribe(ts);

		source1.onNext(1);
		source2.onNext(2);

		ts.assertErrorMessage("Forced failure")
		  .assertNotComplete()
		  .assertNoValues();
	}
}
