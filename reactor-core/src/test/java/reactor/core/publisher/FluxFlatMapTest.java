/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxFlatMapTest {

	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(FluxFlatMap.class);

		ctb.addRef("source", Flux.never());
		ctb.addRef("mapper", (Function<Object, Publisher<Object>>)v -> Flux.never());
		ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
		ctb.addInt("maxConcurrency", 1, Integer.MAX_VALUE);
		ctb.addRef("mainQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
		ctb.addRef("innerQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());

		ctb.test();
	}*/

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000).flatMap(v -> Flux.range(v, 2)).subscribe(ts);

		ts.assertValueCount(2000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

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
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>error(new RuntimeException("forced failure"))
		.flatMap(v -> Flux.just(v)).subscribe(ts);

		ts.assertNoValues()
		.assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")))
		.assertNotComplete();
	}

	@Test
	public void innerError() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(0);

		Flux.just(1).flatMap(v -> Flux.error(new RuntimeException("forced failure"))).subscribe(ts);

		ts.assertNoValues()
		.assertError(RuntimeException.class)
		  .assertErrorWith( e -> Assert.assertTrue(e.getMessage().contains("forced failure")))
		.assertNotComplete();
	}

	@Test
	public void normalQueueOpt() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000).flatMap(v -> Flux.just(v, v + 1)).subscribe(ts);

		ts.assertValueCount(2000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalQueueOptBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 1000).flatMap(v -> Flux.just(v, v + 1)).subscribe(ts);

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
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000).flatMap(v -> Flux.just((Integer)null)).subscribe(ts);

		ts.assertNoValues()
		.assertError(NullPointerException.class)
		.assertNotComplete();
	}

	@Test
	public void mainEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>empty().flatMap(v -> Flux.just(v)).subscribe(ts);

		ts.assertNoValues()
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void innerEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 1000).flatMap(v -> Flux.<Integer>empty()).subscribe(ts);

		ts.assertNoValues()
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void flatMapOfJust() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000).flatMap(Flux::just).subscribe(ts);

		ts.assertValueCount(1000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void flatMapOfMixed() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000).flatMap(
				v -> v % 2 == 0 ? Flux.just(v) : Flux.fromIterable(Arrays.asList(v)))
		.subscribe(ts);

		ts.assertValueCount(1000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void flatMapOfMixedBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

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
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

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
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

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
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

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
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1_000_000).flatMap(Flux::just, 1, 32).subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void testMaxConcurrency2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1_000_000).flatMap(Flux::just, 64).subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void singleSubscriberOnly() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger emission = new AtomicInteger();

		Flux<Integer> source = Flux.range(1, 2).doOnNext(v -> emission.getAndIncrement());

		EmitterProcessor<Integer> source1 = EmitterProcessor.create();
		EmitterProcessor<Integer> source2 = EmitterProcessor.create();

		source.flatMap(v -> v == 1 ? source1 : source2, 1, 32).subscribe(ts);

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

		ts.assertValues(1, 10, 2)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void flatMapUnbounded() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger emission = new AtomicInteger();

		Flux<Integer> source = Flux.range(1, 1000).doOnNext(v -> emission.getAndIncrement());

		EmitterProcessor<Integer> source1 = EmitterProcessor.create();
		EmitterProcessor<Integer> source2 = EmitterProcessor.create();

		source.flatMap(v -> v == 1 ? source1 : source2, Integer.MAX_VALUE, 32).subscribe(ts);

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
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

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
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000).flatMap(v -> Flux.range(v, 1000)).subscribe(ts);

		ts.assertValueCount(1_000_000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void syncFusionArray() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Integer[] array = new Integer[1000];
		Arrays.fill(array, 777);

		Flux.range(1, 1000).flatMap(v -> Flux.fromArray(array)).subscribe(ts);

		ts.assertValueCount(1_000_000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void innerMapSyncFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000).flatMap(v -> Flux.range(1, 1000).map(w -> w + 1)).subscribe(ts);

		ts.assertValueCount(1_000_000)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void defaultPrefetch() {
		assertThat(Flux.just(1, 2, 3)
		               .flatMap(Flux::just)
		               .getPrefetch()).isEqualTo(Queues.XS_BUFFER_SIZE);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failMaxConcurrency() {
		Flux.just(1, 2, 3)
		    .flatMap(Flux::just, -1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failPrefetch() {
		Flux.just(1, 2, 3)
		    .flatMap(Flux::just, 128, -1);
	}

	@Test
	public void failCallable() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMap(d -> Mono.fromCallable(() -> {
			                        throw new Exception("test");
		                        })))
		            .verifyErrorMessage("test");
	}

	@Test
	public void failMap() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMap(d -> {
			                        throw new RuntimeException("test");
		                        }))
		            .verifyErrorMessage("test");
	}

	@Test
	public void failPublisher() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMap(d -> Flux.error(new Exception("test"))))
		            .verifyErrorMessage("test");
	}

	@Test
	public void failPublisherDelay() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMapDelayError(d -> Flux.error(new Exception("test")).hide(),
				                        1,
				                        1))
		            .verifyErrorMessage("Multiple exceptions");
	}

	@Test
	public void completePublisherDelayCancel() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMapDelayError(d -> Flux.<Integer>empty().hide(),
				                        1,
				                        1))
		            .thenCancel()
					.verify();
	}

	@Test
	public void completePublisherDelay() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMapDelayError(d -> Flux.<Integer>empty().hide(),
				                        1,
				                        1))
		            .verifyComplete();
	}

	@Test
	public void failNull() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMap(d -> null))
		            .verifyError(NullPointerException.class);
	}

	@Test
	public void failScalarCallable() {
		StepVerifier.create(Mono.fromCallable(() -> {
			throw new Exception("test");
		})
		                        .flatMapMany(Flux::just))
		            .verifyErrorMessage("test");
	}

	@Test
	public void failScalarMap() {
		StepVerifier.create(Mono.just(1)
		                        .flatMapMany(f -> {
			                        throw new RuntimeException("test");
		                        }))
		            .verifyErrorMessage("test");
	}

	@Test
	public void failScalarMapNull() {
		StepVerifier.create(Mono.just(1)
		                        .flatMapMany(f -> null))
		            .verifyError(NullPointerException.class);
	}

	@Test
	public void failScalarMapCallableError() {
		StepVerifier.create(Mono.just(1)
		                        .flatMapMany(f -> Mono.fromCallable(() -> {
			                        throw new Exception("test");
		                        })))
		            .verifyErrorMessage("test");
	}

	@Test
	public void prematureScalarMapCallableNullComplete() {
		StepVerifier.create(Mono.just(1)
		                        .flatMapMany(f -> Mono.fromCallable(() -> null)))
		            .verifyComplete();
	}

	@Test
	public void prematureScalarMapCallableEmptyComplete() {
		StepVerifier.create(Mono.just(1)
		                        .flatMapMany(f -> Mono.empty()))
		            .verifyComplete();
	}

	@Test
	public void prematureScalarMapCallableJust() {
		StepVerifier.create(Mono.just(1)
		                        .flatMapMany(f -> Mono.fromCallable(() -> 2)))
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	public void prematureCompleteMaxPrefetch() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMap(f -> Flux.empty(), Integer.MAX_VALUE))
		            .verifyComplete();
	}

	@Test
	public void prematureCancel() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMap(Flux::just))
		            .expectNext(1, 2, 3)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void prematureCancel2() {
		StepVerifier.create(Flux.range(1, 10000)
		                        .flatMap(Flux::just, 2)
		                        .cancelOn(Schedulers.single()), 1)
		            .expectNext(1)
		            .thenRequest(2)
		            .expectNext(2, 3)
		            .thenCancel()
		            .verify();
	}

	@Test //FIXME use Violation.NO_CLEANUP_ON_TERMINATE
	public void failNextOnTerminated() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();

		Hooks.onNextDropped(c -> {
			assertThat(c).isEqualTo(2);
		});
		StepVerifier.create(up.flatMap(Flux::just))
		            .then(() -> {
			            up.onNext(1);
			            CoreSubscriber<? super Integer> a = up.actual;
			            up.onComplete();
			            a.onNext(2);
		            })
		            .expectNext(1)
					.verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	public void ignoreDoubleComplete() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
			s.onComplete();
		}).flatMap(Flux::just))
		            .verifyComplete();
	}


	@Test
	public void ignoreConcurrentHiddenComplete() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext(1);
		}).flatMap(f -> Flux.just(f).hide()))
		            .thenCancel()
		            .verify();
	}

	@Test
	public void ignoreDoubleOnSubscribe() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
		}).flatMap(Flux::just))
		            .verifyComplete();
	}

	@Test
	public void ignoreDoubleOnSubscribeInner() {
		StepVerifier.create(Flux.just(1).hide()
		                        .flatMap(f -> Flux.from(s -> {
			                        s.onSubscribe(Operators.emptySubscription());
			                        s.onSubscribe(Operators.emptySubscription());
			                        s.onComplete();
		                        })))
		            .verifyComplete();
	}

	@Test
	public void failDoubleError() {
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(new Exception("test"));
				s.onError(new Exception("test2"));
			}).flatMap(Flux::just))
			            .verifyErrorMessage("test");
			Assert.fail();
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).hasMessage("test2");
		}
	}


	@Test //FIXME use Violation.NO_CLEANUP_ON_TERMINATE
	public void failDoubleErrorTerminated() {
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				Exceptions.terminate(FluxFlatMap.FlatMapMain.ERROR, (FluxFlatMap.FlatMapMain) s);
				s.onError(new Exception("test"));
			}).flatMap(Flux::just))
			            .verifyErrorMessage("test");
			Assert.fail();
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).hasMessage("test");
		}
	}

	@Test //FIXME use Violation.NO_CLEANUP_ON_TERMINATE
	public void failDoubleErrorTerminatedInner() {
		try {
			StepVerifier.create(Flux.just(1).hide().flatMap(f -> Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				Exceptions.terminate(FluxFlatMap.FlatMapMain.ERROR,( (FluxFlatMap
						.FlatMapInner) s).parent);
				s.onError(new Exception("test"));
			})))
			            .verifyErrorMessage("test");
			Assert.fail();
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).hasMessage("test");
		}
	}

	@Test
	public void failOverflowScalar() {
		TestPublisher<Integer> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

		StepVerifier.create(ts.flux()
		                      .flatMap(Flux::just, 1), 0)
		            .then(() -> ts.emit(1, 2))
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void failOverflow() {
		TestPublisher<Integer> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .flatMap(f -> ts, 2, 1), 0)
		            .then(() -> ts.emit(1, 2))
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void failOverflowCancelledWhileActive() {
		TestPublisher<Integer> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);
		AtomicReference<FluxFlatMap.FlatMapMain> ref = new AtomicReference<>();
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .flatMap(f -> ts, 2, 1), 0)
		            .consumeSubscriptionWith(s -> ref.set((FluxFlatMap.FlatMapMain)s))
		            .then(() -> {
			            ref.get().wip = 1;
			            ts.emit(1, 2);
			            ref.get().drainLoop();
		            })
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void failOverflowScalarThenError() {
		AtomicBoolean set = new AtomicBoolean();
		Hooks.onErrorDropped(e -> {
			assertThat(Exceptions.isOverflow(e)).isTrue();
			set.set(true);
		});
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext(1);
			Exceptions.terminate(FluxFlatMap.FlatMapMain.ERROR, (FluxFlatMap.FlatMapMain) s);
			s.onNext(2);
			((FluxFlatMap.FlatMapMain)s).error = null;
			s.onError(new Exception("test"));
		})
		                      .flatMap(Flux::just, 1), 0)
		            .verifyErrorMessage("test");
		Hooks.resetOnErrorDropped();
		assertThat(set.get()).isTrue();
	}

	@Test
	public void failOverflowWhileActiveScalar() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext(1);
			((FluxFlatMap.FlatMapMain)s).wip = 1; //simulate concurrent active
			s.onNext(2);
			((FluxFlatMap.FlatMapMain)s).drainLoop();
		})
		                      .flatMap(Flux::just, 1), 0)
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void failOverflowWhileActiveScalarThenError() {
		AtomicBoolean set = new AtomicBoolean();
		Hooks.onErrorDropped(e -> {
			assertThat(Exceptions.isOverflow(e)).isTrue();
			set.set(true);
		});
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext(1);
			Exceptions.terminate(FluxFlatMap.FlatMapMain.ERROR, (FluxFlatMap.FlatMapMain) s);
			((FluxFlatMap.FlatMapMain)s).wip = 1; //simulate concurrent active
			s.onNext(2);
			s.onNext(3);
			((FluxFlatMap.FlatMapMain)s).error = null;
			((FluxFlatMap.FlatMapMain)s).drainLoop();
			s.onError(new Exception("test"));
		})
		                        .flatMap(Flux::just, 1), 1)
		            .expectNext(1)
		            .verifyErrorMessage("test");
		Hooks.resetOnErrorDropped();
		assertThat(set.get()).isTrue();
	}

	@Test //FIXME use Violation.NO_CLEANUP_ON_TERMINATE
	public void failDoubleErrorSilent() {
		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage("test2");
		});
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onError(new Exception("test"));
			s.onError(new Exception("test2"));
		}).flatMap(Flux::just))
		            .verifyErrorMessage("test");
		Hooks.resetOnErrorDropped();
	}

	@Test
	public void suppressFusionIfExpected() {
		StepVerifier.create(Mono.just(1)
		                        .flatMap(d -> Mono.just(d)
		                                       .hide()))
		            .consumeSubscriptionWith(s -> {
			            assertThat(s).isInstanceOf(FluxHide.SuppressFuseableSubscriber.class);
		            })
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void ignoreRequestZeroThenRequestOneByOne() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMap(f -> Flux.just(f * 2)), 0)
		            .consumeSubscriptionWith(s -> s.request(0))
		            .thenRequest(1)
		            .expectNext(2)
		            .thenRequest(1)
		            .expectNext(4)
		            .thenRequest(1)
		            .expectNext(6)
		            .verifyComplete();
	}

	@Test
	public void suppressFusionIfExpectedError() {
		StepVerifier.create(Mono.just(1)
		                        .flatMap(d -> Mono.error(new Exception("test"))
		                                       .hide()))
		            .consumeSubscriptionWith(s -> {
			            assertThat(s).isInstanceOf(FluxHide.SuppressFuseableSubscriber.class);
		            })
		            .verifyErrorMessage("test");
	}

	@Test
	public void backpressuredScalarThenCancel(){
		StepVerifier.create(Flux.just(1, 2, 3).flatMap(Flux::just), 0)
	                .thenRequest(2)
	                .expectNext(1, 2)
	                .thenCancel()
	                .verify();
	}

	@Test
	public void delayedUnboundedScalarThenCancel() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMap(Flux::just), 0)
		            .thenAwait()
		            .thenRequest(Long.MAX_VALUE)
		            .expectNext(1, 2, 3)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void delayedBackpressuredScalar() {
		StepVerifier.create(Flux.range(1, 1000)
		                        .flatMap(Flux::just), 0)
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext(1, 2)
		            .thenRequest(1)
		            .expectNext(3)
		            .thenRequest(Long.MAX_VALUE)
		            .expectNextCount(997)
		            .verifyComplete();
	}

	Flux<Integer> scenario_backpressuredThenCancel() {
		return Flux.just(1, 2, 3)
		           .flatMap(f -> Flux.range(1, 10)
		                             .delayElements(Duration.ofMillis(10L)))
		           .hide();
	}

	@Test
	public void backpressuredThenCancel() {
		StepVerifier.withVirtualTime(this::scenario_backpressuredThenCancel, 0)
		            .thenRequest(2)
		            .thenAwait(Duration.ofSeconds(1))
		            .expectNext(1, 1)
		            .thenAwait(Duration.ofSeconds(1))
		            .thenRequest(1)
		            .expectNext(2)
		            .thenCancel()
		            .verify();
	}

	void assertBeforeOnSubscribeState(InnerOperator s) {
		assertThat(s.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);
		assertThat(s.scan(Scannable.Attr.PREFETCH)).isEqualTo(1);
		assertThat(s.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(s.scanOrDefault(Scannable.Attr.CANCELLED, false)).isFalse();
	}

	void assertAfterOnSubscribeState(InnerOperator s) {
		assertThat(s.scan(Scannable.Attr.ERROR)).isNull();
		assertThat(s.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(1L);
		assertThat(s.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(s.scanOrDefault(Scannable.Attr.CANCELLED, false)).isFalse();
	}

	void assertAfterOnNextInnerState(InnerConsumer s) {
		assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
	}

	void assertAfterOnNextInnerState2(InnerConsumer s) {
		assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
	}

	void assertAfterOnCompleteInnerState(InnerConsumer s) {
		assertThat(s.scan(Scannable.Attr.TERMINATED)).isFalse();
	}

	void assertAfterOnCompleteInnerState2(InnerConsumer s) {
		assertThat(s.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
	}

	@SuppressWarnings("unchecked")
	void assertAfterOnSubscribeInnerState(InnerProducer s) {
		assertThat(s.inners()).hasSize(1);
	}

	void assertBeforeOnSubscribeInnerState(InnerConsumer s) {
		assertThat(s.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(s.scan(Scannable.Attr.PREFETCH)).isEqualTo(32);
		assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
	}

	@Test
	public void assertOnSubscribeStateMainAndInner() {
		StepVerifier.create(Flux.from(s -> {
			assertBeforeOnSubscribeState((FluxFlatMap.FlatMapMain) s);
			s.onSubscribe(Operators.emptySubscription());
			assertAfterOnSubscribeState((FluxFlatMap.FlatMapMain) s);
			s.onNext(1);
			assertThat(((FluxFlatMap.FlatMapMain) s).actual()).isNotNull();
			assertThat(((FluxFlatMap.FlatMapMain) s).scan(Scannable.Attr.CANCELLED)).isTrue();
			s.onComplete();
		})
		                        .flatMap(f -> Flux.from(s -> {
			                        assertBeforeOnSubscribeInnerState((FluxFlatMap.FlatMapInner) s);
			                        s.onSubscribe(Operators.emptySubscription());
			                        assertAfterOnSubscribeInnerState(((FluxFlatMap
					                        .FlatMapInner) s).parent);
			                        s.onNext(f);
			                        s.onNext(f);
			                        assertAfterOnNextInnerState(((FluxFlatMap.FlatMapInner) s));
			                        assertAfterOnCompleteInnerState(((FluxFlatMap.FlatMapInner) s));
			                        assertThat(((FluxFlatMap.FlatMapInner)s).scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
			                        s.onComplete();
			                        assertAfterOnCompleteInnerState(((FluxFlatMap.FlatMapInner) s));
		                        }), 1), 1)
		            .expectNext(1)
		            .thenCancel()
		.verify();
	}

	@Test
	public void testFairness() {
		StepVerifier.create(Flux.merge(1,
				Flux.range(1, 10)
				    .delayElements(Duration.ofMillis(10), Schedulers.single())
				    .log(),
				Flux.range(1, 10)
				    .delayElements(Duration.ofMillis(20), Schedulers.single())
				    .log()), 0)
		            .thenRequest(5)
		            .expectNextCount(5)
		            .thenRequest(5)
		            .expectNextCount(5)
		            .thenRequest(5)
		            .expectNextCount(5)
		            .thenRequest(5)
		            .expectNextCount(5)
//		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
//		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                    .verifyComplete();
	}

	@Test
	public void noInnerReordering() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);
		FluxFlatMap.FlatMapMain<Publisher<Integer>, Integer> fmm =
				new FluxFlatMap.FlatMapMain<>(ts,
						Function.identity(),
						false,
						128,
						Queues.get(128),
						128,
						Queues.get(128));

		fmm.onSubscribe(Operators.emptySubscription());

		EmitterProcessor<Integer> ps = EmitterProcessor.create();

		fmm.onNext(ps);

		ps.onNext(1);

		Operators.addCap(FluxFlatMap.FlatMapMain.REQUESTED, fmm, 2L);

		ps.onNext(2);

		fmm.drain();

		ts.assertValues(1, 2);
	}

	@Test
	public void noOuterScalarReordering() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);
		FluxFlatMap.FlatMapMain<Publisher<Integer>, Integer> fmm =
				new FluxFlatMap.FlatMapMain<>(ts,
						Function.identity(),
						false,
						128,
						Queues.get(128),
						128,
						Queues.get(128));

		fmm.onSubscribe(Operators.emptySubscription());

		fmm.onNext(Flux.just(1));

		Operators.addCap(FluxFlatMap.FlatMapMain.REQUESTED, fmm, 2L);

		fmm.onNext(Flux.just(2));

		fmm.drain();

		ts.assertValues(1, 2);
	}

	@Test
	public void assertOnSubscribeStateMainAndInner2() {
		StepVerifier.create(Flux.from(s -> {
			assertBeforeOnSubscribeState((FluxFlatMap.FlatMapMain) s);
			s.onSubscribe(Operators.emptySubscription());
			assertAfterOnSubscribeState((FluxFlatMap.FlatMapMain) s);
			s.onNext(1);
			assertThat(((FluxFlatMap.FlatMapMain) s).actual()).isNotNull();
			assertThat(((FluxFlatMap.FlatMapMain) s).scan(Scannable.Attr.CANCELLED)).isFalse();
			s.onComplete();
			assertThat(((FluxFlatMap.FlatMapMain) s).scan(Scannable.Attr.TERMINATED)).isTrue();
		})
		                        .flatMap(f -> Flux.from(s -> {
			                        assertBeforeOnSubscribeInnerState((FluxFlatMap.FlatMapInner) s);
			                        s.onSubscribe(Operators.emptySubscription());
			                        assertAfterOnSubscribeInnerState(((FluxFlatMap
					                        .FlatMapInner) s).parent);
			                        s.onNext(f);
			                        assertAfterOnNextInnerState2(((FluxFlatMap
					                        .FlatMapInner) s));
			                        s.onComplete();
			                        assertAfterOnCompleteInnerState2(((FluxFlatMap.FlatMapInner) s));
		                        }), 1), 1)
		            .expectNext(1)
		            .verifyComplete();
	}


	@Test
	public void assertOnSubscribeStateMainAndInner3() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .flatMap(f -> Flux.from(s -> {
			                        s.onSubscribe(Operators.emptySubscription());
			                        s.onComplete();
			                        assertAfterOnCompleteInnerState2(((FluxFlatMap.FlatMapInner) s));
		                        }), 1), 1)
		            .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void assertOnSubscribeStateMainScalar() {
		AtomicReference<FluxFlatMap.FlatMapMain> ref = new AtomicReference<>();
		StepVerifier.create(Flux.from(s -> {
			assertBeforeOnSubscribeState((FluxFlatMap.FlatMapMain) s);
			s.onSubscribe(Operators.emptySubscription());
			assertAfterOnSubscribeState((FluxFlatMap.FlatMapMain) s);
			s.onNext(1);
		})
		                        .flatMap(Flux::just, 1), 1)
		            .consumeSubscriptionWith(s -> ref.set((FluxFlatMap.FlatMapMain)s))
		            .expectNext(1)
		            .then(() -> {
						FluxFlatMap.FlatMapMain s = ref.get();
			            assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
			            s.onNext(2);
			            assertThat(s.actual()).isNotNull();
			            assertThat(s.scan(Scannable.Attr.CANCELLED)).isFalse();
			            assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
			            assertThat(s.scan(Scannable.Attr.TERMINATED)).isFalse();
			            s.onComplete();
			            assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
			            assertThat(s.scan(Scannable.Attr.TERMINATED)).isFalse();
		            })
		            .thenRequest(1)
		            .then(() -> {
			            FluxFlatMap.FlatMapMain s = ref.get();
			            assertThat(s.scan(Scannable.Attr.TERMINATED)).isTrue();
			            assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		            })
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	public void ignoreSyncFusedRequest() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .flatMap(f -> Flux.range(f, 2), 1), 0)
		            .thenRequest(1)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void asyncInnerFusion() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .flatMap(f -> up, 1))
		            .then(() -> up.onNext(1))
		            .then(() -> up.onNext(2))
		            .then(() -> up.onNext(3))
		            .then(() -> up.onNext(4))
		            .then(() -> up.onComplete())
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}

	@Test
	public void failAsyncInnerFusion() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .flatMap(f -> up, 1))
		            .then(() -> up.onNext(1))
		            .then(() -> up.onNext(2))
		            .then(() -> up.onError(new Exception("test")))
		            .expectNext(1, 2)
		            .verifyErrorMessage("test");
	}

	@Test
	public void failsyncInnerFusion() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .flatMap(f -> Flux.just(1, 2, null), 1))
		            .expectNext(1, 2)
		            .verifyError(NullPointerException.class);
	}

	@Test
	public void onOperatorError() {
		AtomicReference<Object> errorValue = new AtomicReference<Object>();
		Hooks.onOperatorError((error, d) -> {
			errorValue.set(d);
			return error;
		});

		Flux<Integer> f1 = Mono.just(1).flatMapMany(i -> Flux.error(new Exception("test")));
		StepVerifier.create(f1).verifyErrorMessage("test");
		assertThat(errorValue.get()).isEqualTo(1);

//		Flux<Integer> f2 = Mono.just(2).flatMapMany(i -> {
//			throw new RuntimeException("test");
//		});
//		StepVerifier.create(f2).verifyErrorMessage("test");
//		assertThat(errorValue.get()).isEqualTo(2);
//
//		Flux<Integer> f3 = Flux.just(3, 6, 9).flatMap(i -> Flux.error(new Exception("test")));
//		StepVerifier.create(f3).verifyErrorMessage("test");
//		assertThat(errorValue.get()).isEqualTo(3);
//
//		Flux<Integer> f4 = Flux.just(4, 8, 12).flatMap(i -> {
//			throw new RuntimeException("test");
//		});
//		StepVerifier.create(f4).verifyErrorMessage("test");
//		assertThat(errorValue.get()).isEqualTo(4);

		Hooks.resetOnOperatorError();
	}

	@Test
	public void prematureInnerCancel() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5)
		                        .hide()
		                        .flatMap(f -> Flux.from(s -> {
		                        	s.onSubscribe(Operators.emptySubscription());
		                        	s.onNext(1);
		                        	s.onNext(2);
		                        	if(f > 1) {
				                        s.onComplete();
			                        }
		                        })))
		            .expectNext(1, 2)
		            .expectNext(1, 2)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void normalInnerArrayMoreThanDefaultArraySize4() {
		TestPublisher<Integer> ts = TestPublisher.create();
		TestPublisher<Integer> ts2 = TestPublisher.create();
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		                        .hide()
		                        .flatMap(f -> f < 5 ? ts : ts2), 0)
		            .then(() -> ts.next(1, 2))
		            .thenRequest(2)
		            .expectNext(1, 2)
		            .thenRequest(2)
		            .expectNext(1, 2)
		            .thenRequest(2)
		            .expectNext(1, 2)
		            .thenRequest(2)
		            .expectNext(1, 2)
		            .then(() -> ts2.complete())
		            .then(() -> ts.emit(3))
		            .thenRequest(4)
		            .expectNext(3, 3, 3, 3)
		            .verifyComplete();
	}

	@Test
	public void prematureCancelInnerArrayMoreThanDefaultArraySize4() {
		AtomicReference<Subscription> x = new AtomicReference<>();
		StepVerifier.create(Flux.just(1, 2, 3, 4)
		                        .hide()
		                        .flatMap(f -> Flux.from(s -> {
			                        s.onSubscribe(Operators.emptySubscription());
			                        s.onNext(1);
			                        s.onNext(2);
		                        })), 0)
		            .consumeSubscriptionWith(x::set)
		            .thenAwait()
		            .thenRequest(5)
		            .expectNext(1, 2)
		            .expectNext(1, 2)
		            .expectNext(1)
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext(2, 1)
		            .thenRequest(1)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void flatMapDelayErrors() throws Exception {
		AtomicInteger onNextSignals = new AtomicInteger();

		StepVerifier.create(Flux.range(0, 5).hide()
		                        .flatMapDelayError(i -> Mono.just(i)
		                                          .doOnNext(e -> onNextSignals.incrementAndGet())
		                                          .handle((s1, sink) -> {
			                                          if (s1 == 1 || s1 == 3) {
				                                          sink.error(new RuntimeException("Error: " + s1));
			                                          }
			                                          else {
				                                          sink.next(s1);
			                                          }
		                                          })
		                                          .subscribeOn(Schedulers.parallel()),
				                        4, 4)
		                        .retry(1))
		            .recordWith(ArrayList::new)
		            .expectNextCount(3)
		            .consumeRecordedWith(c ->  {
		            	assertThat(c).containsExactlyInAnyOrder(0, 2, 4);
		            	c.clear();
		            })
		            .expectNextCount(3)
		            .consumeRecordedWith(c ->  assertThat(c).containsExactlyInAnyOrder(0, 2, 4))
		            .verifyError();

		assertThat(onNextSignals.get()).isEqualTo(10);
	}


	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void delayErrorWithFluxError() {
		StepVerifier.create(
				Flux.just(
						Flux.just(1, 2),
						Flux.<Integer>error(new Exception("test")),
						Flux.just(3, 4))
				    .flatMapDelayError(f -> f, 4, 4))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void delayErrorWithMonoError() {
		StepVerifier.create(
				Flux.just(
						Flux.just(1, 2),
						Mono.<Integer>error(new Exception("test")),
						Flux.just(3, 4))
				    .flatMapDelayError(f -> f, 4, 4))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

    @Test
    public void scanMain() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFlatMap.FlatMapMain<Integer, Integer> test = new FluxFlatMap.FlatMapMain<>(actual,
                i -> Mono.just(i), true, 5, Queues.<Integer>unbounded(), 789,  Queues.<Integer>get(789));
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(5);

        test.scalarQueue = new ConcurrentLinkedQueue<>();
        test.scalarQueue.add(1);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
        assertThat(test.scan(Scannable.Attr.LARGE_BUFFERED)).isEqualTo(1L);
        assertThat(test.scan(Scannable.Attr.ERROR)).isNull();

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        test.scalarQueue.clear();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    @Test
   public void scanMainLargeBuffered() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFlatMap.FlatMapMain<Integer, Integer> test = new FluxFlatMap.FlatMapMain<>(actual,
                i -> Mono.just(i), true, 5, Queues.<Integer>unbounded(), 789,  Queues.<Integer>get(789));


        test.scalarQueue = new ConcurrentLinkedQueue<>();
        test.scalarQueue.add(1);
        test.scalarQueue.add(2);
        test.scalarQueue.add(3);
        test.size = Integer.MAX_VALUE;

        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(Integer.MIN_VALUE);
        assertThat(test.scan(Scannable.Attr.LARGE_BUFFERED)).isEqualTo(Integer.MAX_VALUE + 3L);
    }

    @Test
    public void scanInner() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFlatMap.FlatMapMain<Integer, Integer> main = new FluxFlatMap.FlatMapMain<>(actual,
                i -> Mono.just(i), true, 5, Queues.<Integer>unbounded(), 789,  Queues.<Integer>get(789));
        FluxFlatMap.FlatMapInner<Integer> inner = new FluxFlatMap.FlatMapInner<>(main, 123);
        Subscription parent = Operators.emptySubscription();
        inner.onSubscribe(parent);

        assertThat(inner.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        assertThat(inner.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(inner.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
        inner.queue = new ConcurrentLinkedQueue<>();
        inner.queue.add(5);
        assertThat(inner.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        assertThat(inner.scan(Scannable.Attr.TERMINATED)).isFalse();
        inner.onError(new IllegalStateException("boom"));
        assertThat(main.scan(Scannable.Attr.ERROR)).hasMessage("boom");
        inner.queue.clear();
        assertThat(inner.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(inner.scan(Scannable.Attr.CANCELLED)).isFalse();
        inner.cancel();
        assertThat(inner.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
	@Ignore
	public void progressiveRequest() {
		TestPublisher<Integer> tp = TestPublisher.create();
		StepVerifier.create(tp.flux()
		                      .flatMap(d -> Flux.just(d)
		                                        .hide()), 1)
		            .then(() -> tp.next(1))
		            .expectNext(1)
		            .thenRequest(2)
		            .then(() -> tp.next(2))
		            .expectNext(2)
		            .thenRequest(Long.MAX_VALUE)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void errorModeContinueNullPublisher() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.<Integer>flatMap(f -> null)
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
		            .expectNoFusionSupport()
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped(1, 2)
		            .hasDroppedErrors(2);
	}


	@Test
	public void errorModeContinueInternalError() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.flatMap(f -> {
					if(f == 1){
						return Mono.error(new NullPointerException());
					}
					else {
						return Mono.just(f);
					}
				})
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
				.expectNoFusionSupport()
				.expectNext(2)
				.expectComplete()
				.verifyThenAssertThat()
				.hasDropped(1)
				.hasDroppedErrors(1);
	}


	@Test
	public void errorModeContinueInternalErrorHidden() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.flatMap(f -> {
					if(f == 1){
						return Mono.<Integer>error(new NullPointerException()).hide();
					}
					else {
						return Mono.just(f);
					}
				})
				.doOnNext(i -> i++)
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
				.expectNoFusionSupport()
				.expectNext(2)
				.expectComplete()
				.verifyThenAssertThat()
				.hasNotDroppedElements()
				.hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueWithCallable() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.flatMap(f -> Mono.<Integer>fromRunnable(() -> {
					if(f == 1) {
						throw new ArithmeticException("boom");
					}
				}))
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
				.expectNoFusionSupport()
				.expectComplete()
				.verifyThenAssertThat()
				.hasDropped(1)
				.hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueDelayErrors() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.flatMapDelayError(f -> {
					if(f == 1){
						return Mono.<Integer>error(new NullPointerException()).hide();
					}
					else {
						return Mono.just(f);
					}
				}, Queues.SMALL_BUFFER_SIZE, Queues.XS_BUFFER_SIZE)
				.onErrorContinue(OnNextFailureStrategyTest::drop);


		StepVerifier.create(test)
				.expectNoFusionSupport()
				.expectNext(2)
				.expectComplete()
				.verifyThenAssertThat()
				// When inner is not a Callable error value is not available.
				.hasNotDroppedElements()
				.hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueDelayErrorsWithCallable() {
		Flux<Integer> test = Flux
				.just(1, 2)
				.hide()
				.flatMapDelayError(f -> {
					if(f == 1){
						return Mono.<Integer>error(new NullPointerException());
					}
					else {
						return Mono.just(f);
					}
				}, Queues.SMALL_BUFFER_SIZE, Queues.XS_BUFFER_SIZE)
				.onErrorContinue(OnNextFailureStrategyTest::drop);


		StepVerifier.create(test)
				.expectNoFusionSupport()
				.expectNext(2)
				.expectComplete()
				.verifyThenAssertThat()
				.hasDropped(1)
				.hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueInternalErrorStopStrategy() {
		Flux<Integer> test = Flux
				.just(0, 1)
				.hide()
				.flatMap(f ->  Flux.range(f, 1).map(i -> 1/i).onErrorStop())
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
				.expectNoFusionSupport()
				.expectNext(1)
				.expectComplete()
				.verifyThenAssertThat()
				.hasNotDroppedElements()
				.hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueInternalErrorStopStrategyAsync() {
		Flux<Integer> test = Flux
				.just(0, 1)
				.hide()
				.flatMap(f ->  Flux.range(f, 1).publishOn(Schedulers.parallel()).map(i -> 1/i).onErrorStop())
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
				.expectNoFusionSupport()
				.expectNext(1)
				.expectComplete()
				.verifyThenAssertThat()
				.hasNotDroppedElements()
				.hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueInternalErrorMono() {
		Flux<Integer> test = Flux
				.just(0, 1)
				.hide()
				.flatMap(f ->  Mono.just(f).map(i -> 1/i))
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
				.expectNoFusionSupport()
				.expectNext(1)
				.expectComplete()
				.verifyThenAssertThat()
				.hasDropped(0)
				.hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueInternalErrorMonoAsync() {
		Flux<Integer> test = Flux
				.just(0, 1)
				.hide()
				.flatMap(f ->  Mono.just(f).publishOn(Schedulers.parallel()).map(i -> 1/i))
				.onErrorContinue(OnNextFailureStrategyTest::drop);

		StepVerifier.create(test)
				.expectNoFusionSupport()
				.expectNext(1)
				.expectComplete()
				.verifyThenAssertThat()
				.hasDropped(0)
				.hasDroppedErrors(1);
	}

	@Test
	public void errorModeContinueScalarSourceFails() {
		StringBuffer msg = new StringBuffer();
		Flux.error(new IllegalStateException("boom"))
				.flatMap(v -> Flux.just("value" + v))
				.onErrorContinue(IllegalStateException.class,
						(ex, elem) -> msg.append(elem)
						                 .append(" skipped, reason: ")
						                 .append(ex.getMessage()))
				.as(StepVerifier::create)
				.verifyComplete();

		assertThat(msg).contains("null skipped, reason: boom");
	}

	@Test
	public void errorModeContinueScalarSourceMapperFails() {
		StringBuffer msg = new StringBuffer();
		Flux.just(42)
				.<String>flatMap(v -> {
					throw new IllegalStateException("boom");
				})
				.onErrorContinue(IllegalStateException.class,
						(ex, elem) -> msg.append(elem)
						                 .append(" skipped, reason: ")
						                 .append(ex.getMessage()))
				.as(StepVerifier::create)
				.verifyComplete();

		assertThat(msg).contains("42 skipped, reason: boom");
	}

	@Test
	public void errorModeContinueScalarSourceMappedCallableFails() {
		StringBuffer msg = new StringBuffer();
		Flux.just(42)
		    .flatMap(v -> Flux.error(new IllegalStateException("boom")))
		    .onErrorContinue(IllegalStateException.class,
				    (ex, elem) -> msg.append(elem)
				                     .append(" skipped, reason: ")
				                     .append(ex.getMessage()))
		    .as(StepVerifier::create)
		    .verifyComplete();

		assertThat(msg).contains("42 skipped, reason: boom");
	}

	@Test
	public void errorModeContinueLargerThanConcurrencySourceMappedCallableFails() {
		AtomicInteger continued = new AtomicInteger();
		Flux.range(1, 500)
		    .flatMap(v -> Flux.error(new IllegalStateException("boom #" + v)), 203)
		    .onErrorContinue(IllegalStateException.class, (ex, elem) -> continued.incrementAndGet())
		    .as(StepVerifier::create)
		    .expectComplete()
		    .verify(Duration.ofSeconds(1));

		assertThat(continued).hasValue(500);
	}

	@Test
	public void noWrappingOfCheckedExceptions() {
		Flux.just("single")
		    .flatMap(x -> Mono.error(new NoSuchMethodException()))
		    .as(StepVerifier::create)
		    .expectError(NoSuchMethodException.class)
		    .verify();

		Flux.just("a", "b")
		    .flatMap(x -> Mono.error(new NoSuchMethodException()))
		    .as(StepVerifier::create)
		    .expectError(NoSuchMethodException.class)
		    .verify();
	}

	@Test
	public void noWrappingOfCheckedExceptions_hide() {
		Flux.just("single")
		    .hide()
		    .flatMap(x -> Mono.error(new NoSuchMethodException()))
		    .as(StepVerifier::create)
		    .expectError(NoSuchMethodException.class)
		    .verify();

		Flux.just("a", "b")
		    .hide()
		    .flatMap(x -> Mono.error(new NoSuchMethodException()))
		    .as(StepVerifier::create)
		    .expectError(NoSuchMethodException.class)
		    .verify();
	}
}
