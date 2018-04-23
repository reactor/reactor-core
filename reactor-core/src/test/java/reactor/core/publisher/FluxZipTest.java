/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.publisher.PublisherProbe;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuple7;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxZipTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.shouldHitDropNextHookAfterTerminate(false)
		                     .droppedError(new RuntimeException("test"));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(

				scenario(f -> f.zipWith(Flux.just(1, 2, 3), 3, (a, b) -> a))
					.prefetch(3),

				scenario(f -> f.zipWith(Flux.<String>error(exception()),
						(a, b) -> a)).shouldHitDropErrorHookAfterTerminate(false),

				scenario(f -> f.zipWith(Flux.<String>error(exception()).hide(),
						(a, b) -> a)));
	}

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

		Flux<List<Object>> zipped =
				Flux.zip(Arrays.asList(flux1, flux2, flux3, flux4), Arrays::asList);

		StepVerifier.create(zipped)
		            .consumeNextWith(t -> Assertions.assertThat(t)
		                                            .containsExactlyElementsOf(expected))
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
		                                            .isEqualTo(Tuples.of(1,
				                                            2,
				                                            3,
				                                            4,
				                                            5,
				                                            6,
				                                            7))
		                                            .isExactlyInstanceOf(Tuple7.class))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void sameLength() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> source = Flux.fromIterable(Arrays.asList(1, 2));
		source.zipWith(source, (a, b) -> a + b)
		      .subscribe(ts);

		ts.assertValues(2, 4)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void sameLengthOptimized() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> source = Flux.just(1, 2);
		source.zipWith(source, (a, b) -> a + b)
		      .subscribe(ts);

		ts.assertValues(2, 4)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void sameLengthBackpressured() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> source = Flux.fromIterable(Arrays.asList(1, 2));
		source.zipWith(source, (a, b) -> a + b)
		      .subscribe(ts);

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
		source.zipWith(source, (a, b) -> a + b)
		      .subscribe(ts);

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
		source1.zipWith(source2, (a, b) -> a + b)
		       .subscribe(ts);

		ts.assertValues(2, 4)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void differentLengthOpt() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> source1 = Flux.fromIterable(Arrays.asList(1, 2));
		Flux<Integer> source2 = Flux.just(1, 2, 3);
		source1.zipWith(source2, (a, b) -> a + b)
		       .subscribe(ts);

		ts.assertValues(2, 4)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void emptyNonEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> source1 = Flux.fromIterable(Collections.emptyList());
		Flux<Integer> source2 = Flux.just(1, 2, 3);
		source1.zipWith(source2, (a, b) -> a + b)
		       .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void nonEmptyAndEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> source1 = Flux.just(1, 2, 3);
		Flux<Integer> source2 = Flux.fromIterable(Collections.emptyList());
		source1.zipWith(source2, (a, b) -> a + b)
		       .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void scalarNonScalar() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> source1 = Flux.just(1);
		Flux<Integer> source2 = Flux.just(1, 2, 3);
		source1.zipWith(source2, (a, b) -> a + b)
		       .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void scalarNonScalarBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> source1 = Flux.just(1);
		Flux<Integer> source2 = Flux.just(1, 2, 3);
		source1.zipWith(source2, (a, b) -> a + b)
		       .subscribe(ts);

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
		source1.zipWith(source2, (a, b) -> a + b)
		       .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void scalarScalar() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> source1 = Flux.just(1);
		Flux<Integer> source2 = Flux.just(1);
		source1.zipWith(source2, (a, b) -> a + b)
		       .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void emptyScalar() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> source1 = Flux.empty();
		Flux<Integer> source2 = Flux.just(1);
		source1.zipWith(source2, (a, b) -> a + b)
		       .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void syncFusionMapToNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.fromIterable(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
		    .zipWith(Flux.fromIterable(Arrays.asList(1, 2))
		                 .map(v -> v == 2 ? null : v), (a, b) -> a + b)
		    .subscribe(ts);

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
	public void nonPairWisePairWise() {
		Flux<Tuple2<Tuple3<Integer, String, String>, String>> f =
				Flux.zip(Flux.just(1), Flux.just("test"), Flux.just("test0"))
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
	public void pairWise3() {
		AtomicLong ref = new AtomicLong();
		Flux<Tuple2<Tuple2<Integer, String>, String>> f =
				Flux.zip(Flux.just(1), Flux.just("test"))
				    .zipWith(Flux.just("test2")
				                 .hide()
				                 .doOnRequest(ref::set), 1);

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
				Flux.zip(Arrays.asList(Flux.just(1), Flux.just("test")),
						obj -> Tuples.of((int) obj[0], (String) obj[1]))
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
		EmitterProcessor<Integer> source1 = EmitterProcessor.create();
		EmitterProcessor<Integer> source2 = EmitterProcessor.create();
		Flux<Integer> zippedFlux = Flux.zip(source1, source2, (t1, t2) -> t1 + t2);
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
		Flux<List<Integer>> zippedFlux =
				Flux.zip(odds, even, (t1, t2) -> Arrays.asList(t1, t2));
		Mono<List<List<Integer>>> tap = zippedFlux.collectList();

//		then: "the values are all collected from source1 flux"
		assertThat(tap.block()).containsExactly(Arrays.asList(1, 2),
				Arrays.asList(3, 4),
				Arrays.asList(5, 6));

//		when: "the sources are zipped in a flat map"
		zippedFlux = odds.flatMap(it -> Flux.zip(Flux.just(it),
				even,
				(t1, t2) -> Arrays.asList(t1, t2)));
		tap = zippedFlux.collectList();

//		then: "the values are all collected from source1 flux"
		assertThat(tap.block()).containsExactly(Arrays.asList(1, 2),
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
		StepVerifier.create(Flux.zip(Flux.just(1),
				Flux.just(2),
				Flux.just(3),
				Flux.just(4),
				Flux.just(5),
				Flux.just(6)))
		            .expectNext(Tuples.of(1, 2, 3, 4, 5, 6))
		            .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createZipWithPrefetch() {
		Flux<Integer>[] list = new Flux[]{Flux.just(1), Flux.just(2)};
		Flux<Integer> f = Flux.zip(obj -> 0, 123, list);
		assertThat(f.getPrefetch()).isEqualTo(123);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createZipWithPrefetchIterable() {
		List<Flux<Integer>> list = Arrays.asList(Flux.just(1), Flux.just(2));
		Flux<Integer> f = Flux.zip(list, 123, obj -> 0);
		assertThat(f.getPrefetch()).isEqualTo(123);
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
		            .verifyComplete();
	}

	@Test
	public void prematureCompleteIterableEmptyScalarSource() {
		StepVerifier.create(Flux.zip(Arrays.asList(Flux.just(1), Mono.empty()), obj -> 0))
		            .verifyComplete();
	}

	@Test //FIXME use Violation.NO_CLEANUP_ON_TERMINATE
	public void failDoubleNext() {
		Hooks.onNextDropped(c -> {
		});
		StepVerifier.create(Flux.zip(obj -> 0, Flux.just(1), Flux.just(2), s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext(2);
			s.onNext(3);
		}))
		            .thenCancel()
		            .verify();
		Hooks.resetOnNextDropped();
	}

	@Test //FIXME use Violation.NO_CLEANUP_ON_TERMINATE
	public void ignoreDoubleComplete() {
		StepVerifier.create(Flux.zip(obj -> 0, Flux.just(1), Flux.never(), s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
			s.onComplete();
		}))
		            .verifyComplete();
	}

	@Test //FIXME use Violation.NO_CLEANUP_ON_TERMINATE
	public void failDoubleError() {
		try {
			StepVerifier.create(Flux.zip(obj -> 0, Flux.just(1), Flux.never(), s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(new Exception("test"));
				s.onError(new Exception("test2"));
			}))
			            .verifyErrorMessage("test");
			Assert.fail();
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).hasMessage("test2");
		}
	}

	@Test //FIXME use Violation.NO_CLEANUP_ON_TERMINATE
	public void failDoubleError3() {
		try {
			StepVerifier.create(Flux.zip(obj -> 0,
					Flux.just(1)
					    .hide(),
					Flux.never(),
					s -> {
						s.onSubscribe(Operators.emptySubscription());
						s.onError(new Exception("test"));
						s.onError(new Exception("test2"));
					}))
			            .verifyErrorMessage("test");
			Assert.fail();
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).hasMessage("test2");
		}
	}

	@Test //FIXME use Violation.NO_CLEANUP_ON_TERMINATE
	public void failDoubleErrorSilent() {
		Hooks.onErrorDropped(e -> {
		});
		StepVerifier.create(Flux.zip(obj -> 0, Flux.just(1), Flux.never(), s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onError(new Exception("test"));
			s.onError(new Exception("test2"));
		}))
		            .verifyErrorMessage("test");
		Hooks.resetOnErrorDropped();
	}

	@Test //FIXME use Violation.NO_CLEANUP_ON_TERMINATE
	public void failDoubleErrorHide() {
		try {
			StepVerifier.create(Flux.zip(obj -> 0,
					Flux.just(1)
					    .hide(),
					Flux.never(),
					s -> {
						s.onSubscribe(Operators.emptySubscription());
						s.onError(new Exception("test"));
						s.onError(new Exception("test2"));
					}))
			            .verifyErrorMessage("test");
			Assert.fail();
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).hasMessage("test2");
		}
	}

	@Test
	public void failDoubleTerminalPublisher() {
		DirectProcessor<Integer> d1 = DirectProcessor.create();
		Hooks.onErrorDropped(e -> {
		});
		try {
			StepVerifier.create(Flux.zip(obj -> 0, Flux.just(1), d1, s -> {
				CoreSubscriber<?> a =
						((DirectProcessor.DirectInner) d1.inners().findFirst().get())
								.actual;

				s.onSubscribe(Operators.emptySubscription());
				s.onComplete();
				a.onError(new Exception("test"));
			}))
			            .verifyComplete();
		}
		finally {
			Hooks.resetOnErrorDropped();
		}
	}

	@Test //FIXME use Violation.NO_CLEANUP_ON_TERMINATE
	public void failDoubleError2() {
		try {
			StepVerifier.create(Flux.zip(obj -> 0,
					Flux.just(1)
					    .hide(),
					Flux.never(),
					s -> {
						s.onSubscribe(Operators.emptySubscription());
						s.onError(new Exception("test"));
						s.onError(new Exception("test2"));
					}))
			            .verifyErrorMessage("test");
			Assert.fail();
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).hasMessage("test2");
		}
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
	public void failCombinedNullHide() {
		StepVerifier.create(Flux.zip(obj -> null,
				Flux.just(1),
				Flux.just(2)
				    .hide()))
		            .verifyError(NullPointerException.class);
	}

	@Test
	public void failCombinedNullHideAll() {
		StepVerifier.create(Flux.zip(obj -> null,
				Flux.just(1)
				    .hide(),
				Flux.just(2)
				    .hide()))
		            .verifyError(NullPointerException.class);
	}

	@Test
	public void ignoreRequestZeroHideAll() {
		StepVerifier.create(Flux.zip(obj -> (int) obj[0] + (int) obj[1],
				Flux.just(1)
				    .hide(),
				Flux.just(2)
				    .hide()), 0)
		            .consumeSubscriptionWith(s -> s.request(0))
		            .thenRequest(1)
		            .expectNext(3)
		            .verifyComplete();
	}

	@Test
	public void failCombinedFusedError() {
		StepVerifier.create(Flux.zip(obj -> 0, Flux.just(1, null), Flux.just(2, 3)), 0)
		            .thenRequest(1)
		            .expectNext(0)
		            .verifyError(NullPointerException.class);
	}

	@Test
	public void backpressuredAsyncFusedCancelled() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		StepVerifier.create(Flux.zip(obj -> (int) obj[0] + (int) obj[1],
				1,
				up,
				Flux.just(2, 3, 5)), 0)
		            .then(() -> up.onNext(1))
		            .thenRequest(1)
		            .expectNext(3)
		            .then(() -> up.onNext(2))
		            .thenRequest(1)
		            .expectNext(5)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void backpressuredAsyncFusedCancelled2() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		StepVerifier.create(Flux.zip(obj -> (int) obj[0] + (int) obj[1],
				1,
				up,
				Flux.just(2, 3, 5)), 0)
		            .then(() -> up.onNext(1))
		            .thenRequest(3)
		            .expectNext(3)
		            .then(() -> up.onNext(2))
		            .expectNext(5)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void backpressuredAsyncFusedError() {
		Hooks.onErrorDropped(c -> {
			assertThat(c).hasMessage("test2");
		});
		try {
			UnicastProcessor<Integer> up = UnicastProcessor.create();
			StepVerifier.create(Flux.zip(obj -> (int) obj[0] + (int) obj[1],
					1,
					up,
					Flux.just(2, 3, 5)), 0)
			            .then(() -> up.onNext(1))
			            .thenRequest(1)
			            .expectNext(3)
			            .then(() -> up.onNext(2))
			            .thenRequest(1)
			            .expectNext(5)
			            .then(() -> up.actual.onError(new Exception("test")))
			            .then(() -> up.actual.onError(new Exception("test2")))
			            .verifyErrorMessage("test");
		}
		finally {
			Hooks.resetOnErrorDropped();
		}
	}

	@Test
	public void backpressuredAsyncFusedErrorHideAll() {
		Hooks.onErrorDropped(c -> {
			assertThat(c).hasMessage("test2");
		});
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		try {
			StepVerifier.create(Flux.zip(obj -> (int) obj[0] + (int) obj[1], 1, up, s -> {
				assertThat(((FluxZip.ZipInner) up.actual).parent.subscribers[1].done).isFalse();
				Flux.just(2, 3, 5)
				    .subscribe(s);
			})
			                        .hide(), 0)
			            .then(() -> up.onNext(1))
			            .thenRequest(1)
			            .expectNext(3)
			            .then(() -> up.onNext(2))
			            .thenRequest(1)
			            .expectNext(5)
			            .then(() -> assertThat(((FluxZip.ZipInner) up.actual).done).isFalse())
			            .then(() -> up.actual.onError(new Exception("test")))
			            .then(() -> assertThat(((FluxZip.ZipInner) up.actual).done).isTrue())
			            .then(() -> up.actual.onError(new Exception("test2")))
			            .verifyErrorMessage("test");
		}
		finally {
			Hooks.resetOnErrorDropped();
		}
	}

	@Test
	public void backpressuredAsyncFusedComplete() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		StepVerifier.create(Flux.zip(obj -> (int) obj[0] + (int) obj[1],
				1,
				up,
				Flux.just(2, 3, 5)), 0)
		            .then(() -> up.onNext(1))
		            .thenRequest(1)
		            .expectNext(3)
		            .then(() -> up.onNext(2))
		            .thenRequest(1)
		            .expectNext(5)
		            .then(() -> up.onComplete())
		            .verifyComplete();
	}

	@Test
	public void failCombinedError() {
		StepVerifier.create(Flux.zip(obj -> {
			throw new RuntimeException("test");
		}, 123, Flux.just(1), Flux.just(2), Flux.just(3)))
		            .verifyErrorMessage("test");
	}

	@Test
	public void failCombinedErrorHide() {
		StepVerifier.create(Flux.zip(obj -> {
					throw new RuntimeException("test");
				},
				123,
				Flux.just(1)
				    .hide(),
				Flux.just(2)
				    .hide(),
				Flux.just(3)))
		            .verifyErrorMessage("test");
	}

	@Test
	public void failCombinedErrorHideAll() {
		StepVerifier.create(Flux.zip(obj -> {
					throw new RuntimeException("test");
				},
				123,
				Flux.just(1)
				    .hide(),
				Flux.just(2)
				    .hide(),
				Flux.just(3)
				    .hide()))
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
		            .verifyComplete();
	}

	@Test
	public void prematureCompleteCallableNullHide() {
		StepVerifier.create(Flux.zip(obj -> 0,
				Flux.just(1)
				    .hide(),
				Mono.fromCallable(() -> null)))
		            .verifyComplete();
	}

	@Test
	public void prematureCompleteCallableNullHideAll() {
		StepVerifier.create(Flux.zip(obj -> 0,
				Flux.just(1)
				    .hide(),
				Mono.fromCallable(() -> null)
				    .hide()))
		            .verifyComplete();
	}

	@Test
	public void prematureCompleteEmptySource() {
		StepVerifier.create(Flux.zip(obj -> 0, Flux.just(1), Mono.empty()))
		            .verifyComplete();
	}

	@Test
	public void prematureCompleteEmptySourceHide() {
		StepVerifier.create(Flux.zip(obj -> 0,
				Flux.just(1)
				    .hide(),
				Mono.empty()))
		            .verifyComplete();
	}

	@Test
	public void prematureCompleteSourceEmpty() {
		StepVerifier.create(Flux.zip(obj -> 0,
				Flux.just(1),
				Mono.empty()
				    .hide()))
		            .verifyComplete();
	}

	@Test
	public void prematureCompleteSourceEmptyDouble() {
		DirectProcessor<Integer> d = DirectProcessor.create();
		StepVerifier.create(Flux.zip(obj -> 0, d, s -> {
			CoreSubscriber<?> a =
					((DirectProcessor.DirectInner) d.inners().findFirst().get())
							.actual;

			Operators.complete(s);

			a.onComplete();
		}, Mono.just(1)))
		            .verifyComplete();
	}

	@Test
	public void prematureCompleteSourceError() {
		StepVerifier.create(Flux.zip(obj -> 0,
				Flux.just(1),
				Mono.error(new Exception("test"))))
		            .verifyErrorMessage("test");
	}

	@Test
	public void prematureCompleteSourceErrorHide() {
		StepVerifier.create(Flux.zip(obj -> 0,
				Flux.just(1)
				    .hide(),
				Mono.error(new Exception("test"))))
		            .verifyErrorMessage("test");
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
				Flux.just(9)),
				obj -> (int) obj[0] + (int) obj[1] + (int) obj[2] + (int) obj[3] + (int) obj[4] + (int) obj[5] + (int) obj[6] + (int) obj[7] + (int) obj[8]))
		            .expectNext(45)
		            .verifyComplete();
	}

	@Test
	public void size8LikeInternalBuffer() {
		StepVerifier.create(Flux.zip(Arrays.asList(Flux.just(1),
				Flux.just(2),
				Flux.just(3),
				Flux.just(4),
				Flux.just(5),
				Flux.just(6),
				Flux.just(7),
				Flux.just(8)),
				obj -> (int) obj[0] + (int) obj[1] + (int) obj[2] + (int) obj[3] + (int) obj[4] + (int) obj[5] + (int) obj[6] + (int) obj[7]))
		            .expectNext(36)
		            .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void cancelled() {
		AtomicReference<FluxZip.ZipSingleCoordinator> ref = new AtomicReference<>();

		StepVerifier.create(Flux.zip(obj -> (int) obj[0] + (int) obj[1] + (int) obj[2],
				1,
				Flux.just(1, 2),
				Flux.defer(() -> {
					ref.get()
					   .cancel();
					return Flux.just(3);
				}),
				Flux.just(3))
		                        .doOnSubscribe(s -> {
			                        assertThat(s instanceof FluxZip.ZipSingleCoordinator).isTrue();
			                        ref.set((FluxZip.ZipSingleCoordinator) s);
			                        assertInnerSubscriberBefore(ref.get());
		                        }), 0)
		            .then(() -> assertThat(ref.get()
		                                      .scan(Scannable.Attr.BUFFERED)).isEqualTo(3))
		            .then(() -> assertThat(ref.get()
		                                      .scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE))
		            .then(() -> assertThat(ref.get()
		                                      .inners()).hasSize(3))
		            .thenCancel()
		            .verify();

		assertInnerSubscriber(ref.get());
	}

	@SuppressWarnings("unchecked")
	void assertInnerSubscriberBefore(FluxZip.ZipSingleCoordinator c) {
		FluxZip.ZipSingleSubscriber s = (FluxZip.ZipSingleSubscriber) c.inners()
		                                                               .findFirst()
		                                                               .get();

		assertThat(s.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		assertThat(s.scan(Scannable.Attr.CANCELLED)).isFalse();
	}

	@SuppressWarnings("unchecked")
	void assertInnerSubscriber(FluxZip.ZipSingleCoordinator c) {
		FluxZip.ZipSingleSubscriber s = (FluxZip.ZipSingleSubscriber) c.inners()
		                                                               .findFirst()
		                                                               .get();

		assertThat(s.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
		assertThat(s.scan(Scannable.Attr.CANCELLED)).isTrue();

		Hooks.onNextDropped(v -> {
		});
		s.onNext(0);
		Hooks.resetOnNextDropped();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void cancelledHide() {
		AtomicReference<FluxZip.ZipCoordinator> ref = new AtomicReference<>();

		StepVerifier.create(Flux.zip(obj -> (int) obj[0] + (int) obj[1] + (int) obj[2],
				123,
				Flux.just(1, 2)
				    .hide(),
				Flux.defer(() -> {
					ref.get()
					   .cancel();
					return Flux.just(3);
				}),
				Flux.just(3)
				    .hide())
		                        .doOnSubscribe(s -> {
			                        assertThat(s instanceof FluxZip.ZipCoordinator).isTrue();
			                        ref.set((FluxZip.ZipCoordinator) s);
			                        assertInnerSubscriberBefore(ref.get());
		                        }), 0)
		            .then(() -> assertThat(ref.get()
		                                      .inners().count()).isEqualTo(3))
		            .then(() -> assertThat(ref.get()
		                                      .scan(Scannable.Attr.ERROR)).isNull())
		            .then(() -> assertThat(ref.get()
		                                      .scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L))
		            .thenCancel()
		            .verify();

		assertInnerSubscriber(ref.get());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void delayedCancelledHide() {
		AtomicReference<FluxZip.ZipCoordinator> ref = new AtomicReference<>();

		StepVerifier.create(Flux.zip(obj -> (int) obj[0] + (int) obj[1] + (int) obj[2],
				123,
				Flux.just(1, 2)
				    .hide(),
				Flux.defer(() -> {
					ref.get()
					   .cancel();
					assertThat(ref.get()
					              .scan(Scannable.Attr.CANCELLED)).isTrue();
					assertThat(ref.get()
					              .scanOrDefault(Scannable.Attr.TERMINATED, false));
					return Flux.just(3);
				}),
				Flux.just(3)
				    .hide())
		                        .doOnSubscribe(s -> {
			                        assertThat(s instanceof FluxZip.ZipCoordinator).isTrue();
			                        ref.set((FluxZip.ZipCoordinator) s);
			                        assertInnerSubscriberBefore(ref.get());
		                        }), 0)
		            .then(() -> assertThat(ref.get()
		                                      .inners()).hasSize(3))
		            .then(() -> assertThat(ref.get()
		                                      .scan(Scannable.Attr.ERROR)).isNull())
		            .then(() -> assertThat(ref.get()
		                                      .scan(Scannable.Attr
				                                      .REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L))
		            .thenCancel()
		            .verify();

		assertInnerSubscriber(ref.get());
	}

	@SuppressWarnings("unchecked")
	void assertInnerSubscriberBefore(FluxZip.ZipCoordinator c) {
		FluxZip.ZipInner s = (FluxZip.ZipInner) c.inners()
		                                         .iterator()
		                                         .next();

		assertThat(s.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(s.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		assertThat(s.scan(Scannable.Attr.CANCELLED)).isFalse();
	}

	@SuppressWarnings("unchecked")
	void assertInnerSubscriber(FluxZip.ZipCoordinator c) {
		FluxZip.ZipInner s = (FluxZip.ZipInner) c.inners()
		                                         .iterator()
		                                         .next();

		assertThat(s.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(s.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(c.inners()).hasSize(3);
		assertThat(s.scan(Scannable.Attr.CANCELLED)).isTrue();
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
				    .hide()),
				obj -> (int) obj[0] + (int) obj[1] + (int) obj[2] + (int) obj[3] + (int) obj[4] + (int) obj[5] + (int) obj[6] + (int) obj[7] + (int) obj[8]))
		            .expectNext(45)
		            .verifyComplete();
	}

	@Test
	public void seven() {
		StepVerifier.create(Flux.zip(Arrays.asList(Flux.just(1),
				Flux.just(2),
				Flux.just(3),
				Flux.just(4)
				    .hide(),
				Flux.just(5)
				    .hide(),
				Flux.just(6)
				    .hide(),
				Flux.just(7)
				    .hide()),
				obj -> (int) obj[0] + (int) obj[1] + (int) obj[2] + (int) obj[3] + (int) obj[4] + (int) obj[5] + (int) obj[6]))
		            .expectNext(28)
		            .verifyComplete();
	}

	@Test
	public void scanOperator() {
		FluxZip s = new FluxZip<>(Flux.just(1), Flux.just(2), Tuples::of, Queues.small(), 123, true);
		assertThat(s.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(s.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
	}

	@Test
    public void scanCoordinator() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxZip.ZipCoordinator<Integer, Integer> test = new FluxZip.ZipCoordinator<Integer, Integer>(actual,
				i -> 5, 123, Queues.unbounded(), 345, true);

        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        test.requested = 35;
        Assertions.assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);

        Assertions.assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
        Assertions.assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
        test.error = new IllegalStateException("boom");
        Assertions.assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanInner() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxZip.ZipCoordinator<Integer, Integer> main = new FluxZip.ZipCoordinator<Integer, Integer>(actual,
				i -> 5, 123, Queues.unbounded(), 345, true);
		FluxZip.ZipInner<Integer> test = new FluxZip.ZipInner<>(main, 234, 1, Queues.unbounded());

        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        Assertions.assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(234);
        test.queue = new ConcurrentLinkedQueue<>();
        test.queue.offer(67);
        Assertions.assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.queue.clear();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }


	@Test
    public void scanSingleCoordinator() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxZip.ZipSingleCoordinator<Integer, Integer> test =
				new FluxZip.ZipSingleCoordinator<Integer, Integer>(actual, new Object[1], 1, i -> 5,
						true, null, false);

        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        Assertions.assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
        Assertions.assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();

        test.wip = 1;
        Assertions.assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.wip = 0;
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanSingleSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxZip.ZipSingleCoordinator<Integer, Integer> main =
				new FluxZip.ZipSingleCoordinator<Integer, Integer>(actual, new Object[1], 1, i -> 5,
						true, null, false);
        FluxZip.ZipSingleSubscriber<Integer> test = new FluxZip.ZipSingleSubscriber<>(main, 0);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        test.onNext(7);
        Assertions.assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    @Test
    public void zipDelayErrorScalarPartialCallableError() {
		AtomicReference<SignalType> fluxNotScalarDone = new AtomicReference<>();
		AtomicInteger fluxNotScalarNext = new AtomicInteger();
	    final Flux<Integer> fluxError = Flux.error(new IllegalStateException("boom"));
	    final Flux<Integer> fluxNotScalar = Flux.range(1, 2)
	                                            .doFinally(fluxNotScalarDone::set)
	                                            .doOnNext(n -> fluxNotScalarNext.incrementAndGet())
	                                            .hide();

	    final Flux<Tuple2<Integer, Integer>> test =
			    Flux.zipDelayError(fluxError, fluxNotScalar, Tuples::of);

	    StepVerifier.create(test)
	                .verifyErrorMessage("boom");

	    assertThat(fluxNotScalarDone.get()).isNotNull().isEqualTo(SignalType.ON_COMPLETE);
	    assertThat(fluxNotScalarNext).hasValue(2);
    }

    @Test
    public void zipDelayErrorScalarPartialNormalError() {
		AtomicInteger count = new AtomicInteger();
	    final Flux<Integer> fluxError = Flux.<Integer>error(new IllegalStateException("boom")).hide();
	    final Mono<Integer> fluxScalar = Mono.fromCallable(count::incrementAndGet);

	    final Flux<Tuple2<Integer, Integer>> test =
			    Flux.zipDelayError(fluxError, fluxScalar, Tuples::of);

	    StepVerifier.create(test)
	                .verifyErrorMessage("boom");

	    assertThat(count).hasValue(1);
    }

    @Test
    public void zipDelayErrorScalarPartialCallableEmpty() {
		AtomicReference<SignalType> fluxNotScalarDone = new AtomicReference<>();
		AtomicInteger fluxNotScalarNext = new AtomicInteger();
	    final Flux<Integer> fluxScalar = Flux.empty();
	    final Flux<Integer> fluxNotScalar = Flux.range(1, 2)
	                                            .doFinally(fluxNotScalarDone::set)
	                                            .doOnNext(n -> fluxNotScalarNext.incrementAndGet())
	                                            .hide();

	    final Flux<Tuple2<Integer, Integer>> test =
			    Flux.zipDelayError(fluxScalar, fluxNotScalar, Tuples::of);

	    StepVerifier.create(test)
	                .verifyComplete();

	    assertThat(fluxNotScalarNext).hasValue(2);
	    assertThat(fluxNotScalarDone.get()).isNotNull().isEqualTo(SignalType.ON_COMPLETE);
    }

    @Test
    public void zipDelayErrorScalarPartialNormalEmpty() {
	    AtomicInteger count = new AtomicInteger();
	    final Flux<Integer> fluxEmpty = Flux.<Integer>empty().hide();
	    final Mono<Integer> fluxScalar = Mono.fromCallable(count::incrementAndGet);

	    final Flux<Tuple2<Integer, Integer>> test =
			    Flux.zipDelayError(fluxEmpty, fluxScalar, Tuples::of);

	    StepVerifier.create(test)
	                .verifyComplete();

	    assertThat(count).hasValue(1);
    }

    @Test
    public void zipDelayErrorScalarFullWithErrorAndValue() {
	    AtomicInteger count = new AtomicInteger();
	    final Flux<Integer> fluxScalarError = Flux.error(new IllegalStateException("boom"));
	    final Mono<Integer> fluxScalar = Mono.fromCallable(count::incrementAndGet);

	    final Flux<Tuple2<Integer, Integer>> test =
			    Flux.zipDelayError(fluxScalarError, fluxScalar, Tuples::of);

	    StepVerifier.create(test)
	                .verifyErrorMessage("boom");

	    assertThat(count).hasValue(1);
    }

    @Test
    public void zipDelayErrorScalarFullWithEmptyAndError() {
		AtomicInteger count = new AtomicInteger();
	    final Mono<Integer> fluxScalarEmpty = Mono.fromRunnable(count::incrementAndGet);
	    final Flux<Integer> fluxScalarError = Flux.error(new IllegalStateException("boom"));

	    final Flux<Tuple2<Integer, Integer>> test =
			    Flux.zipDelayError(fluxScalarEmpty, fluxScalarError, Tuples::of);

	    StepVerifier.create(test)
	                .verifyErrorMessage("boom");

	    assertThat(count).hasValue(1);
    }

    @Test
    public void zipDelayErrorScalarFullWithEmptyAndValue() {
	    AtomicInteger count = new AtomicInteger();
	    final Flux<Integer> fluxScalarEmpty = Flux.empty();
	    final Mono<Integer> fluxScalar = Mono.fromCallable(count::incrementAndGet);

	    final Flux<Tuple2<Integer, Integer>> test =
			    Flux.zipDelayError(fluxScalarEmpty, fluxScalar, Tuples::of);

	    StepVerifier.create(test)
	                .verifyComplete();

	    assertThat(count).hasValue(1);
    }

    @Test
	public void zipDelayErrorNoScalarFirstImmediateErrors() {
		AtomicInteger nextCount = new AtomicInteger();
		PublisherProbe<Integer> error = PublisherProbe.of(Flux.error(new IllegalStateException("boom")));
		PublisherProbe<Integer> value = PublisherProbe.of(Flux.range(1, 2)
		                                                      .doOnNext(n -> nextCount.incrementAndGet()));

		StepVerifier.create(Flux.zipDelayError(error.flux(), value.flux(), Tuples::of))
		            .verifyErrorMessage("boom");

		error.assertWasSubscribed();
		value.assertWasSubscribed();
		assertThat(nextCount).hasValue(2);
		value.assertWasNotCancelled();
    }

    @Test
	public void zipDelayErrorNoScalarFirstEmpty() {
	    AtomicInteger nextCount = new AtomicInteger();
	    PublisherProbe<Integer> empty = PublisherProbe.of(Flux.empty());
	    PublisherProbe<Integer> value = PublisherProbe.of(Flux.range(1, 2)
	                                                          .doOnNext(n -> nextCount.incrementAndGet()));

	    StepVerifier.create(Flux.zipDelayError(empty.flux(), value.flux(), Tuples::of))
	                .verifyComplete();

	    empty.assertWasRequested();
	    value.assertWasRequested();
	    assertThat(nextCount).hasValue(2);
		value.assertWasNotCancelled();
    }

    @Test
	public void zipDelayErrorNoScalarFirstValueThenErrors() {
	    AtomicInteger firstNextCount = new AtomicInteger();
	    AtomicInteger secondNextCount = new AtomicInteger();
	    PublisherProbe<Integer> valueThenError = PublisherProbe.of(
			    Flux.concat(Flux.just(1), Flux.error(new IllegalStateException("boom")))
			        .doOnNext(n -> firstNextCount.incrementAndGet())
	    );
	    PublisherProbe<Integer> value = PublisherProbe.of(Flux.range(1, 2)
	                                                          .doOnNext(n -> secondNextCount.incrementAndGet()));

	    StepVerifier.create(Flux.zipDelayError(valueThenError.flux(), value.flux(), (a, b) -> a + b))
	                .expectNext(2)
	                .verifyErrorMessage("boom");

	    valueThenError.assertWasSubscribed();
	    value.assertWasSubscribed();
	    assertThat(firstNextCount).hasValue(1);
	    assertThat(secondNextCount).hasValue(2);
	    value.assertWasNotCancelled();
    }

    @Test
	public void zipDelayErrorNoScalarFirstValueThenEmpty() {
	    AtomicInteger firstNextCount = new AtomicInteger();
	    AtomicInteger secondNextCount = new AtomicInteger();
	    PublisherProbe<Integer> valueThenEmpty = PublisherProbe.of(
			    Flux.concat(Flux.just(1), Flux.empty())
			        .doOnNext(n -> firstNextCount.incrementAndGet())
	    );
	    PublisherProbe<Integer> value = PublisherProbe.of(Flux.range(1, 2)
	                                                          .doOnNext(n -> secondNextCount.incrementAndGet()));

	    StepVerifier.create(Flux.zipDelayError(valueThenEmpty.flux(), value.flux(), (a, b) -> a + b))
	                .expectNext(2)
	                .verifyComplete();

	    valueThenEmpty.assertWasSubscribed();
	    value.assertWasSubscribed();
	    assertThat(firstNextCount).hasValue(1);
	    assertThat(secondNextCount).hasValue(2);
	    value.assertWasNotCancelled();
    }

    @Test
	public void zipDelayErrorNoScalarFirstValueThenEmpty4() {
	    AtomicInteger firstNextCount = new AtomicInteger();
	    AtomicInteger secondNextCount = new AtomicInteger();
	    AtomicInteger thirdNextCount = new AtomicInteger();
	    AtomicInteger fourthNextCount = new AtomicInteger();

	    PublisherProbe<Integer> valueThenEmpty = PublisherProbe.of(Flux.range(1, 2)
	                                                                   .doOnNext(n -> firstNextCount.incrementAndGet()));
	    PublisherProbe<Integer> value1 = PublisherProbe.of(Flux.range(10, 3)
	                                                          .doOnNext(n -> secondNextCount.incrementAndGet()));
	    PublisherProbe<Integer> value2 = PublisherProbe.of(Flux.range(100, 4)
	                                                           .log()
	                                                           .doOnNext(n -> thirdNextCount.incrementAndGet()));
	    PublisherProbe<Integer> value3 = PublisherProbe.of(Flux.range(1000, 5)
	                                                          .doOnNext(n -> fourthNextCount.incrementAndGet()));

	    @SuppressWarnings("unchecked")
	    FluxZip<Integer, Integer> zip = new FluxZip<Integer, Integer>(
			    new Publisher[] {valueThenEmpty.flux(), value1.flux(), value2.flux(), value3.flux()},
			    o -> ((Integer) o[0] + (Integer) o[1] + (Integer) o[2] + (Integer) o[3]),
			    Queues.small(), 10, true);

	    StepVerifier.create(zip.log())
	                .expectNext(1111)
	                .expectNext(1115)
	                .verifyComplete();

	    valueThenEmpty.assertWasSubscribed();
	    value1.assertWasSubscribed();
	    value2.assertWasSubscribed();
	    value3.assertWasSubscribed();

	    value1.assertWasNotCancelled();
	    value2.assertWasNotCancelled();
	    value3.assertWasNotCancelled();

	    assertThat(firstNextCount).as("first").hasValue(2);
	    assertThat(secondNextCount).as("second").hasValue(3);
	    assertThat(thirdNextCount).as("third").hasValue(4);
	    assertThat(fourthNextCount).as("fourth").hasValue(5);
    }

    @Test
	public void zipDelayErrorNoScalarAllValued() {
	    PublisherProbe<Integer> first = PublisherProbe.of(Flux.range(1, 2));
	    PublisherProbe<Integer> second = PublisherProbe.of(Flux.range(10, 2));

	    StepVerifier.create(Flux.zipDelayError(first.flux(), second.flux(), (a, b) -> a + b))
	                .expectNext(11, 13)
	                .verifyComplete();

	    first.assertWasRequested();
	    second.assertWasRequested();

	    first.assertWasNotCancelled();
	    second.assertWasNotCancelled();
    }

    @Test
	public void zipDelayErrorPartialScalarAllValued() {
	    Flux<Integer> first = Flux.just(1);
	    PublisherProbe<Integer> second = PublisherProbe.of(Flux.just(10));

	    StepVerifier.create(Flux.zipDelayError(first, second.flux(), (a, b) -> a + b))
	                .expectNext(11)
	                .verifyComplete();

	    second.assertWasCancelled(); //single subscriber always cancels, but there was 1 element emitted
    }

    @Test
	public void zipDelayErrorFullScalarAllValued() {
	    Flux<Integer> first = Flux.just(1);
	    Flux<Integer> second = Flux.just(10);

	    StepVerifier.create(Flux.zipDelayError(first, second, (a, b) -> a + b))
	                .expectNext(11)
	                .verifyComplete();
    }

}
