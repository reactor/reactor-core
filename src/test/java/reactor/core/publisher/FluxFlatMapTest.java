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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.TestSubscriber;

public class FluxFlatMapTest {

	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(FluxFlatMap.class);
		
		ctb.addRef("source", FluxNever.instance());
		ctb.addRef("mapper", (Function<Object, Publisher<Object>>)v -> FluxNever.instance());
		ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
		ctb.addInt("maxConcurrency", 1, Integer.MAX_VALUE);
		ctb.addRef("mainQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
		ctb.addRef("innerQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
		
		ctb.test();
	}*/

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		
		Flux.range(1, 1000).flatMap(v -> Flux.range(v, 2)).subscribe(ts);
		
		ts.assertValueCount(2000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);
		
		Flux.range(1, 1000).flatMap(v -> Flux.range(v, 2)).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		ts.request(1000);

		ts.assertValueCount(1000)
		.assertNoError()
		.assertNotComplete();

		ts.request(1000);

		ts.assertValueCount(2000)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void mainError() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.<Integer>error(new RuntimeException("forced failure"))
		.flatMap(v -> new FluxJust<>(v)).subscribe(ts);
		
		ts.assertNoValues()
		.assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")))
		.assertNotComplete();
	}

	@Test
	public void innerError() {
		TestSubscriber<Object> ts = TestSubscriber.create(0);

		new FluxJust<>(1).flatMap(v -> Flux.error(new RuntimeException("forced failure"))).subscribe(ts);
		
		ts.assertNoValues()
		.assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")))
		.assertNotComplete();
	}

	@Test
	public void normalQueueOpt() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		
		Flux.range(1, 1000).flatMap(v -> new FluxArray<>(v, v + 1)).subscribe(ts);
		
		ts.assertValueCount(2000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalQueueOptBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);
		
		Flux.range(1, 1000).flatMap(v -> new FluxArray<>(v, v + 1)).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		ts.request(1000);

		ts.assertValueCount(1000)
		.assertNoError()
		.assertNotComplete();

		ts.request(1000);

		ts.assertValueCount(2000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void nullValue() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		
		Flux.range(1, 1000).flatMap(v -> new FluxArray<>((Integer)null)).subscribe(ts);
		
		ts.assertNoValues()
		.assertError(NullPointerException.class)
		.assertNotComplete();
	}

	@Test
	public void mainEmpty() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.<Integer>empty().flatMap(v -> new FluxJust<>(v)).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void innerEmpty() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

		Flux.range(1, 1000).flatMap(v -> Flux.<Integer>empty()).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void flatMapOfJust() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		
		Flux.range(1, 1000).flatMap(Flux::just).subscribe(ts);
		
		ts.assertValueCount(1000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void flatMapOfMixed() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		
		Flux.range(1, 1000).flatMap(
				v -> v % 2 == 0 ? Flux.just(v) : Flux.fromIterable(Arrays.asList(v)))
		.subscribe(ts);
		
		ts.assertValueCount(1000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void flatMapOfMixedBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);
		
		Flux.range(1, 1000).flatMap(v -> v % 2 == 0 ? Flux.just(v) : Flux.fromIterable(Arrays.asList(v))).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		ts.request(500);

		ts.assertValueCount(500)
		.assertNoError()
		.assertNotComplete();

		ts.request(500);

		ts.assertValueCount(1000)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void flatMapOfMixedBackpressured1() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);
		
		Flux.range(1, 1000).flatMap(v -> v % 2 == 0 ? Flux.just(v) : Flux.fromIterable(Arrays.asList(v))).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		ts.request(500);

		ts.assertValueCount(500)
		.assertNoError()
		.assertNotComplete();

		ts.request(501);

		ts.assertValueCount(1000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void flatMapOfJustBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);
		
		Flux.range(1, 1000).flatMap(Flux::just).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		ts.request(500);

		ts.assertValueCount(500)
		.assertNoError()
		.assertNotComplete();

		ts.request(500);

		ts.assertValueCount(1000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void flatMapOfJustBackpressured1() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);
		
		Flux.range(1, 1000).flatMap(Flux::just).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		ts.request(500);

		ts.assertValueCount(500)
		.assertNoError()
		.assertNotComplete();

		ts.request(501);

		ts.assertValueCount(1000)
		.assertNoError()
		.assertComplete();
	}


	@Test
	public void testMaxConcurrency1() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 1_000_000).flatMap(Flux::just, 1, 32).subscribe(ts);
		
		ts.assertValueCount(1_000_000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void singleSubscriberOnly() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		
		AtomicInteger emission = new AtomicInteger();
		
		Flux<Integer> source = Flux.range(1, 2).doOnNext(v -> emission.getAndIncrement());
		
		EmitterProcessor<Integer> source1 = EmitterProcessor.create();
		EmitterProcessor<Integer> source2 = EmitterProcessor.create();

		source.flatMap(v -> v == 1 ? source1 : source2, 1, 32).subscribe(ts);

		source1.connect();
		source2.connect();
		
		Assert.assertEquals(1, emission.get());
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		Assert.assertTrue("source1 no subscribers?", source1.downstreamCount() != 0);
		Assert.assertFalse("source2 has subscribers?", source2.downstreamCount() != 0);
		
		source1.onNext(1);
		source2.onNext(10);
		
		source1.onComplete();
		
		source2.onNext(2);
		source2.onComplete();
		
		ts.assertValues(1, 2)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void flatMapUnbounded() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		
		AtomicInteger emission = new AtomicInteger();
		
		Flux<Integer> source = Flux.range(1, 1000).doOnNext(v -> emission.getAndIncrement());
		
		EmitterProcessor<Integer> source1 = EmitterProcessor.create();
		EmitterProcessor<Integer> source2 = EmitterProcessor.create();
		
		source.flatMap(v -> v == 1 ? source1 : source2, false, Integer.MAX_VALUE, 32).subscribe(ts);

		source1.connect();
		source2.connect();

		Assert.assertEquals(1000, emission.get());
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		Assert.assertTrue("source1 no subscribers?", source1.downstreamCount() != 0);
		Assert.assertTrue("source2 no  subscribers?", source2.downstreamCount() != 0);
		
		source1.onNext(1);
		source1.onComplete();
		
		source2.onNext(2);
		source2.onComplete();
		
		ts.assertValueCount(1000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void syncFusionIterable() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		
		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			list.add(i);
		}
		
		Flux.range(1, 1000).flatMap(v -> Flux.fromIterable(list)).subscribe(ts);
		
		ts.assertValueCount(1_000_000)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void syncFusionRange() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		
		Flux.range(1, 1000).flatMap(v -> Flux.range(v, 1000)).subscribe(ts);
		
		ts.assertValueCount(1_000_000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void syncFusionArray() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		
		Integer[] array = new Integer[1000];
		Arrays.fill(array, 777);
		
		Flux.range(1, 1000).flatMap(v -> Flux.fromArray(array)).subscribe(ts);
		
		ts.assertValueCount(1_000_000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void innerMapSyncFusion() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 1000).flatMap(v -> Flux.range(1, 1000).map(w -> w + 1)).subscribe(ts);

		ts.assertValueCount(1_000_000)
		.assertNoError()
		.assertComplete();
	}


}
