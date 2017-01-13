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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.MultiReceiver;
import reactor.core.Trackable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuple7;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;

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
	public void iterableWithCombinatorHasCorrectLength() {
		Flux<Integer> flux1 = Flux.just(1);
		Flux<Integer> flux2 = Flux.just(2);
		Flux<Integer> flux3 = Flux.just(3);
		Flux<Integer> flux4 = Flux.just(4);
		List<Object> expected = Arrays.asList(1, 2, 3, 4);

		Flux<List<Object>> zipped = Flux.zip(Arrays.asList(flux1, flux2, flux3, flux4),
				Arrays::asList);

		StepVerifier.create(zipped)
		            .consumeNextWith(t -> Assertions.assertThat(t).containsExactlyElementsOf(expected))
		            .expectComplete()
		            .verify();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void iterableWithoutCombinatorReturnsCorrectTuple() {
		Flux<Integer> flux1 = Flux.just(1);
		Flux<Integer> flux2 = Flux.just(2);
		Flux<Integer> flux3 = Flux.just(3);
		Flux<Integer> flux4 = Flux.just(4);
		List<Object> expected = Arrays.asList(1, 2, 3, 4);

		Flux<Tuple2> zipped = Flux.zip(Arrays.asList(flux1, flux2, flux3, flux4));

		StepVerifier.create(zipped)
		            .consumeNextWith(t -> Assertions.assertThat(t)
		                                            .containsExactlyElementsOf(expected)
		                                            .isExactlyInstanceOf(Tuple4.class))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void publisherOfPublishersUsesCorrectTuple() {
		Flux<Flux<Integer>> map = Flux.range(1, 7)
		                              .map(Flux::just);

		Flux<Tuple2> zipped = Flux.zip(map, t -> t);
		StepVerifier.create(zipped)
		            .consumeNextWith(t -> Assertions.assertThat((Iterable<?>) t)
				            .isEqualTo(Tuples.of(1, 2, 3, 4, 5, 6, 7))
				            .isExactlyInstanceOf(Tuple7.class))
		            .expectComplete()
		            .verify();
	}

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

	@Test
	public void pairWise3() {
		AtomicLong ref = new AtomicLong();
		Flux<Tuple2<Tuple2<Integer, String>, String>> f =
				Flux.zip(Flux.just(1), Flux.just("test"))
				    .zipWith(Flux.just("test2").hide().doOnRequest(ref::set), 1);

		Assert.assertTrue(f instanceof FluxZip);
		FluxZip<?, ?> s = (FluxZip<?, ?>) f;
		Assert.assertTrue(s.sources != null);
		Assert.assertTrue(s.sources.length == 2);

		Flux<Tuple2<Integer, String>> ff = f.map(t -> Tuples.of(t.getT1()
		                                                         .getT1(),
				t.getT1()
				 .getT2() + t.getT2()));

		ff.subscribeWith(AssertSubscriber.create())
		 .assertValues(Tuples.of(1, "testtest2"))
		 .assertComplete();
		Assert.assertTrue(ref.get() == 1);
	}

	@Test
	public void pairWise2() {
		Flux<Tuple2<Tuple2<Integer, String>, String>> f =
				Flux.zip(Arrays.asList(Flux.just(1), Flux.just("test")), obj -> Tuples.of((int)obj[0], (String)obj[1]))
				    .zipWith(Flux.just("test2"));

		Assert.assertTrue(f instanceof FluxZip);
		FluxZip<?, ?> s = (FluxZip<?, ?>) f;
		Assert.assertTrue(s.sources != null);
		Assert.assertTrue(s.sources.length == 2);

		Flux<Tuple2<Integer, String>> ff = f.map(t -> Tuples.of(t.getT1()
		                                                         .getT1(),
				t.getT1()
				 .getT2() + t.getT2()));

		ff.subscribeWith(AssertSubscriber.create())
		  .assertValues(Tuples.of(1, "testtest2"))
		  .assertComplete();
	}


	@Test
	public void multipleStreamValuesCanBeZipped() {
//		"Multiple Stream"s values can be zipped"
//		given: "source composables to merge, buffer and tap"
		EmitterProcessor<Integer> source1 = EmitterProcessor.<Integer>create().connect();
		EmitterProcessor<Integer> source2 = EmitterProcessor.<Integer>create().connect();
		Flux<Integer> zippedFlux = Flux
				.zip(source1, source2, (t1, t2) -> t1 + t2);
		AtomicReference<Integer> tap = new AtomicReference<>();
		zippedFlux.subscribe(it -> tap.set(it));

//		when: "the sources accept a value"
		source1.onNext(1);
		source2.onNext(2);
		source2.onNext(3);
		source2.onNext(4);

//		then: "the values are all collected from source1 flux"
		assertThat(tap.get()).isEqualTo(3);

//		when: "the sources accept the missing value"
		source2.onNext(5);
		source1.onNext(6);

//		then: "the values are all collected from source1 flux"
		assertThat(tap.get()).isEqualTo(9);
	}

	@Test
	public void multipleIterableStreamValuesCanBeZipped() {
//		"Multiple iterable Stream"s values can be zipped"
//		given: "source composables to zip, buffer and tap"
		Flux<Integer> odds = Flux.just(1, 3, 5, 7, 9);
		Flux<Integer> even = Flux.just(2, 4, 6);

//		when: "the sources are zipped"
		Flux<List<Integer>> zippedFlux = Flux.zip(odds,
				even,
				(t1, t2) -> Arrays.asList(t1, t2));
		Mono<List<List<Integer>>> tap = zippedFlux
		                                          .collectList();

//		then: "the values are all collected from source1 flux"
		assertThat(tap.block()).containsExactly(
				Arrays.asList(1, 2),
				Arrays.asList(3, 4),
				Arrays.asList(5, 6));

//		when: "the sources are zipped in a flat map"
		zippedFlux = odds.flatMap(it -> Flux.zip(Flux.just(it), even, (t1, t2) -> Arrays.asList(t1, t2)));
		tap = zippedFlux.collectList();


//		then: "the values are all collected from source1 flux"
		assertThat(tap.block()).containsExactly(
				Arrays.asList(1, 2),
				Arrays.asList(3, 2),
				Arrays.asList(5, 2),
				Arrays.asList(7, 2),
				Arrays.asList(9, 2));
	}

	@Test
	public void zip() {
		StepVerifier.create(Flux.zip(obj -> (int) obj[0], Flux.just(1)))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void zipEmpty() {
		StepVerifier.create(Flux.zip(obj -> (int) obj[0]))
		            .verifyComplete();
	}

	@Test
	public void zipHide() {
		StepVerifier.create(Flux.zip(obj -> (int) obj[0],
				Flux.just(1)
				    .hide()))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void zip2() {
		StepVerifier.create(Flux.zip(Flux.just(1), Flux.just(2), (a, b) -> a))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void zip3() {
		StepVerifier.create(Flux.zip(Flux.just(1), Flux.just(2), Flux.just(3)))
		            .expectNext(Tuples.of(1, 2, 3))
		            .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void zip4() {
		StepVerifier.create(Flux.zip(Flux.just(1),
				Flux.just(2),
				Flux.just(3),
				Flux.just(4)))
		            .expectNext(Tuples.of(1, 2, 3, 4))
		            .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void zip5() {
		StepVerifier.create(Flux.zip(Flux.just(1),
				Flux.just(2),
				Flux.just(3),
				Flux.just(4),
				Flux.just(5)))
		            .expectNext(Tuples.of(1, 2, 3, 4, 5))
		            .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void zip6() {
		StepVerifier.create(
				Flux.zip(Flux.just(1),
						Flux.just(2),
						Flux.just(3),
						Flux.just(4),
						Flux.just(5),
						Flux.just(6))).expectNext(Tuples.of(1, 2, 3, 4, 5, 6))
		                              .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createZipWithPrefetch() {
		Flux<Integer>[] list = new Flux[]{Flux.just(1), Flux.just(2)};
		Flux<Integer> f = Flux.zip(obj -> 0, 123, list);
		assertThat(f.getPrefetch()).isEqualTo(123);

		assertThat(f instanceof Trackable).isTrue();
		Trackable t = (Trackable)f;
		assertThat(t.getCapacity()).isEqualTo(123);

		assertThat(f instanceof MultiReceiver).isTrue();
		MultiReceiver mr = (MultiReceiver)f;
		assertThat(mr.upstreamCount()).isEqualTo(2);

		Iterator<?> it = mr.upstreams();
		assertThat(it.next()).isEqualTo(list[0]);
		assertThat(it.next()).isEqualTo(list[1]);
		assertThat(it.hasNext()).isFalse();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createZipWithPrefetchIterable() {
		List<Flux<Integer>> list = Arrays.asList(Flux.just(1), Flux.just(2));
		Flux<Integer> f = Flux.zip(list, 123, obj -> 0);
		assertThat(f.getPrefetch()).isEqualTo(123);

		assertThat(f instanceof Trackable).isTrue();
		Trackable t = (Trackable)f;
		assertThat(t.getCapacity()).isEqualTo(123);

		assertThat(f instanceof MultiReceiver).isTrue();
		MultiReceiver mr = (MultiReceiver)f;
		assertThat(mr.upstreamCount()).isEqualTo(-1);

		Iterator<?> it = mr.upstreams();
		assertThat(it.next()).isEqualTo(list.get(0));
		assertThat(it.next()).isEqualTo(list.get(1));
		assertThat(it.hasNext()).isFalse();
	}

	@Test(expected = IllegalArgumentException.class)
	public void failPrefetch() {
		Flux.zip(obj -> 0, -1, Flux.just(1), Flux.just(2));
	}

	@Test(expected = IllegalArgumentException.class)
	public void failPrefetchIterable() {
		Flux.zip(Arrays.asList(Flux.just(1), Flux.just(2)), -1, obj -> 0);
	}

	@Test
	public void failIterableNull() {
		StepVerifier.create(Flux.zip(Arrays.asList(Flux.just(1), null), obj -> 0))
		            .verifyError(NullPointerException.class);
	}

	@Test
	public void failIterableCallable() {
		StepVerifier.create(Flux.zip(Arrays.asList(Flux.just(1), Mono.fromCallable(() -> {
			throw new Exception("test");
		})), obj -> 0))
		            .verifyErrorMessage("test");
	}

	@Test
	public void prematureCompleteIterableCallableNull() {
		StepVerifier.create(Flux.zip(Arrays.asList(Flux.just(1),
				Mono.fromCallable(() -> null)), obj -> 0))
		            .verifyComplete(); //FIXME Should fail ?
	}

	@Test
	public void failNull() {
		StepVerifier.create(Flux.zip(obj -> 0, Flux.just(1), null))
		            .verifyError(NullPointerException.class);
	}

	@Test
	public void failCombinedNull() {
		StepVerifier.create(Flux.zip(obj -> null, Flux.just(1), Flux.just(2)))
		            .verifyError(NullPointerException.class);
	}

	@Test
	public void failCombinedError() {
		StepVerifier.create(Flux.zip(obj -> {
			throw new RuntimeException("test");
		}, 123, Flux.just(1), Flux.just(2), Flux.just(3)))
		            .verifyErrorMessage("test");
	}

	@Test
	public void failCallable() {
		StepVerifier.create(Flux.zip(obj -> 0, Flux.just(1), Mono.fromCallable(() -> {
			throw new Exception("test");
		})))
		            .verifyErrorMessage("test");
	}

	@Test
	public void prematureCompleteCallableNull() {
		StepVerifier.create(Flux.zip(obj -> 0,
				Flux.just(1),
				Mono.fromCallable(() -> null)))
		            .verifyComplete(); //FIXME Should fail ?
	}

	@Test
	public void prematureCompleteEmpty() {
		StepVerifier.create(Flux.zip(obj -> 0))
		            .verifyComplete();
	}

	@Test
	public void prematureCompleteIterableEmpty() {
		StepVerifier.create(Flux.zip(Arrays.asList(), obj -> 0))
		            .verifyComplete();
	}

	@Test
	public void moreThan8() {
		StepVerifier.create(Flux.zip(Arrays.asList(Flux.just(1),
				Flux.just(2),
				Flux.just(3),
				Flux.just(4),
				Flux.just(5),
				Flux.just(6),
				Flux.just(7),
				Flux.just(8),
				Flux.just(9)), obj -> (int) obj[8]))
		            .expectNext(9)
		            .verifyComplete();
	}

	@Test
	public void moreThan8Hide() {
		StepVerifier.create(Flux.zip(Arrays.asList(Flux.just(1)
		                                               .hide(),
				Flux.just(2)
				    .hide(),
				Flux.just(3)
				    .hide(),
				Flux.just(4)
				    .hide(),
				Flux.just(5)
				    .hide(),
				Flux.just(6)
				    .hide(),
				Flux.just(7)
				    .hide(),
				Flux.just(8)
				    .hide(),
				Flux.just(9)
				    .hide()), obj -> (int) obj[8]))
		            .expectNext(9)
		            .verifyComplete();
	}
}
