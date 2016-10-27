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

import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class FluxZipTest {

	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(FluxZip.class);
		
		ctb.addRef("sources", new Publisher[0]);
		ctb.addRef("sourcesIterable", Collections.emptyList());
		ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
		ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
		ctb.addRef("zipper", (Function<Object[], Object>)v -> v);
		
		ctb.test();
	}
	*/
	@Test
	public void sameLength() {
		
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux<Integer> source = Flux.fromIterable(Arrays.asList(1, 2));
		source.zipWith(source, (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(2, 4)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void sameLengthOptimized() {
		
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux<Integer> source = Flux.just(1, 2);
		source.zipWith(source, (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(2, 4)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void sameLengthBackpressured() {
		
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);
		
		Flux<Integer> source = Flux.fromIterable(Arrays.asList(1, 2));
		source.zipWith(source, (a, b) -> a + b).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		ts.request(1);

		ts.assertValues(2)
		.assertNoError()
		.assertNotComplete();

		ts.request(2);
		
		ts.assertValues(2, 4)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void sameLengthOptimizedBackpressured() {
		
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);
		
		Flux<Integer> source = Flux.just(1, 2);
		source.zipWith(source, (a, b) -> a + b).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		ts.request(1);

		ts.assertValues(2)
		.assertNoError()
		.assertNotComplete();

		ts.request(2);
		
		ts.assertValues(2, 4)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void differentLength() {
		
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux<Integer> source1 = Flux.fromIterable(Arrays.asList(1, 2));
		Flux<Integer> source2 = Flux.just(1, 2, 3);
		source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(2, 4)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void differentLengthOpt() {
		
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux<Integer> source1 = Flux.fromIterable(Arrays.asList(1, 2));
		Flux<Integer> source2 = Flux.just(1, 2, 3);
		source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(2, 4)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void emptyNonEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux<Integer> source1 = Flux.fromIterable(Collections.emptyList());
		Flux<Integer> source2 = Flux.just(1, 2, 3);
		source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void nonEmptyAndEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux<Integer> source1 = Flux.just(1, 2, 3);
		Flux<Integer> source2 = Flux.fromIterable(Collections.emptyList());
		source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void scalarNonScalar() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux<Integer> source1 = Flux.just(1);
		Flux<Integer> source2 = Flux.just(1, 2, 3);
		source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(2)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void scalarNonScalarBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);
		
		Flux<Integer> source1 = Flux.just(1);
		Flux<Integer> source2 = Flux.just(1, 2, 3);
		source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		ts.request(1);
		
		ts.assertValues(2)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void scalarNonScalarOpt() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux<Integer> source1 = Flux.just(1);
		Flux<Integer> source2 = Flux.just(1, 2, 3);
		source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(2)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void scalarScalar() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux<Integer> source1 = Flux.just(1);
		Flux<Integer> source2 = Flux.just(1);
		source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(2)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void emptyScalar() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux<Integer> source1 = Flux.empty();
		Flux<Integer> source2 = Flux.just(1);
		source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
		
		ts.assertNoValues()
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void syncFusionMapToNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.fromIterable(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
		    .zipWith(Flux.fromIterable(Arrays.asList(1, 2)).map(v -> v == 2 ? null : v), (a, b) -> a + b).subscribe(ts);
		
		ts.assertValues(2)
		.assertError(NullPointerException.class)
		.assertNotComplete();
	}

	@Test
	public void pairWise() {
		Flux<Tuple2<Tuple2<Integer, String>, String>> f =
				Flux.zip(Flux.just(1), Flux.just("test"))
				    .zipWith(Flux.just("test2"));

		Assert.assertTrue(f instanceof FluxZip);
		FluxZip<?, ?> s = (FluxZip<?, ?>) f;
		Assert.assertTrue(s.sources != null);
		Assert.assertTrue(s.sources.length == 3);

		Flux<Tuple2<Integer, String>> ff = f.map(t -> Tuples.of(t.getT1()
		                                                         .getT1(),
				t.getT1()
				 .getT2() + t.getT2()));

		ff.subscribeWith(AssertSubscriber.create())
		 .assertValues(Tuples.of(1, "testtest2"))
		 .assertComplete();
	}
}
