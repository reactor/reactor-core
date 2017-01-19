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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.Fuseable;
import reactor.core.Receiver;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Fuseable.ASYNC;
import static reactor.core.Fuseable.SYNC;

public class FluxHandleTest extends AbstractFluxOperatorTest<String, String> {

	@Override
	protected List<Scenario<String, String>> scenarios_threeNextAndComplete() {
		return Arrays.asList(
				Scenario.from(f -> f.handle((s, d) -> {
					if (multiItem(2).equals(s)) {
						d.complete();
					}
					else {
						d.next(s);
					}
				}), Fuseable.ANY, step -> step.expectNext(multiItem(0), multiItem(1))
				                              .verifyComplete()),

				Scenario.from(f -> f.handle((s, d) -> {
					if (!multiItem(2).equals(s)) {
						d.next(s);
					}
				}), Fuseable.ANY, step -> step.expectNext(multiItem(0), multiItem(1))
				                              .verifyComplete()),

				Scenario.from(f -> f.handle((s, d) -> {
					if (multiItem(2).equals(s)) {
						d.complete();
					}
					else if (multiItem(1).equals(s)) {
						d.next(s);
					}
				}), Fuseable.ANY, step -> step.expectNext(multiItem(1)).verifyComplete())
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorInOperatorCallback() {
		return Arrays.asList(
				Scenario.from(f -> f.handle((s, d) -> {
					throw exception();
				}), Fuseable.ANY),

				Scenario.from(f -> f.handle((s, d) -> d.error(exception())),
						Fuseable.ANY),

				Scenario.from(f -> f.handle((s, d) -> {
					d.next(multiItem(0));
					d.next(multiItem(1));
				}), Fuseable.ANY, step -> step.verifyError(IllegalStateException.class)),

				Scenario.from(f -> f.handle((s, d) -> {
					d.next(null);
				}), Fuseable.ANY, step -> step.verifyError(NullPointerException.class))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				Scenario.from(f -> f.handle((data, s) -> {})),

				Scenario.from(f -> f.handle((data, s) -> {
					if (multiItem(2).equals(data)) {
						s.complete();
					}
					else {
						s.next(data);
					}
				})),

				Scenario.from(f -> f.handle((data, s) -> {
					if (!multiItem(2).equals(data)) {
						s.next(data);
					}
				}))
		);
	}

	@Test
	public void normal() {
		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(2, 4, 6, 8, 10));

		Flux.range(1, 5)
		    .handle((v, s) -> s.next(v * 2))
		    .subscribeWith(AssertSubscriber.create())
		    .assertContainValues(expectedValues)
		    .assertNoError()
		    .assertComplete();
	}

	@Test
	public void normalHide() {
		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(2, 4, 6, 8, 10));

		Flux.range(1, 5)
		    .hide()
		    .handle((v, s) -> s.next(v * 2))
		    .subscribeWith(AssertSubscriber.create())
		    .assertContainValues(expectedValues)
		    .assertNoError()
		    .assertComplete();
	}

	@Test
	public void filterNullMapResult() {
		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(4, 8));

		Flux.range(1, 5)
		    .handle((v, s) -> {
			    if (v % 2 == 0) {
				    s.next(v * 2);
			    }
		    })
		    .subscribeWith(AssertSubscriber.create())
		    .assertContainValues(expectedValues)
		    .assertNoError()
		    .assertComplete();
	}

	@Test
	public void normalSyncFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(2, 4, 6, 8, 10));
		ts.requestedFusionMode(SYNC);

		Flux.range(1, 5).<Integer>handle((v, s) -> s.next(v * 2)).subscribe(ts);

		ts.assertContainValues(expectedValues)
		  .assertNoError()
		  .assertComplete()
		  .assertFuseableSource()
		  .assertFusionMode(SYNC);
	}

	@Test
	public void normalAsyncFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(ASYNC);

		Flux.range(1,
				5).<Integer>handle((v, s) -> s.next(v * 2)).publishOn(Schedulers.single())
		                                                   .subscribe(ts);

		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(2, 4, 6, 8, 10));
		ts.await()
		  .assertContainValues(expectedValues)
		  .assertNoError()
		  .assertComplete()
		  .assertFuseableSource()
		  .assertFusionMode(ASYNC);
	}

	@Test
	public void filterNullMapResultSyncFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(SYNC);

		Flux.range(1, 5).<Integer>handle((v, s) -> {
			if (v % 2 == 0) {
				s.next(v * 2);
			}
		}).subscribe(ts);

		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(4, 8));
		ts.assertContainValues(expectedValues)
		  .assertNoError()
		  .assertComplete()
		  .assertFuseableSource()
		  .assertFusionMode(SYNC);
	}

	@Test
	public void filterNullMapResultAsyncFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(ASYNC);

		Flux.range(1, 5).<Integer>handle((v, s) -> {
			if (v % 2 == 0) {
				s.next(v * 2);
			}
		}).publishOn(Schedulers.single())
		  .subscribe(ts);

		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(4, 8));
		ts.await()
		  .assertContainValues(expectedValues)
		  .assertNoError()
		  .assertComplete()
		  .assertFuseableSource()
		  .assertFusionMode(ASYNC);
	}

	@Test
	public void errorSignal() {

		int data = 1;
		Exception exception = new IllegalStateException();

		final AtomicReference<Throwable> throwableInOnOperatorError =
				new AtomicReference<>();
		final AtomicReference<Object> dataInOnOperatorError = new AtomicReference<>();

		Hooks.onOperatorError((t, d) -> {
			throwableInOnOperatorError.set(t);
			dataInOnOperatorError.set(d);
			return t;
		});

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(data).<Integer>handle((v, s) -> s.error(exception)).subscribe(ts);

		ts.await()
		  .assertNoValues()
		  .assertError(IllegalStateException.class)
		  .assertNotComplete();

		Assert.assertSame(throwableInOnOperatorError.get(), exception);
		Assert.assertSame(dataInOnOperatorError.get(), data);
	}

	@Test
	public void errorPropagated() {

		int data = 1;
		IllegalStateException exception = new IllegalStateException();

		final AtomicReference<Throwable> throwableInOnOperatorError =
				new AtomicReference<>();
		final AtomicReference<Object> dataInOnOperatorError = new AtomicReference<>();

		Hooks.onOperatorError((t, d) -> {
			throwableInOnOperatorError.set(t);
			dataInOnOperatorError.set(d);
			return t;
		});

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(data).<Integer>handle((v, s) -> {
			throw exception;
		}).subscribe(ts);

		ts.await()
		  .assertNoValues()
		  .assertError(IllegalStateException.class)
		  .assertNotComplete();

		Assert.assertSame(throwableInOnOperatorError.get(), exception);
		Assert.assertSame(dataInOnOperatorError.get(), data);
	}

	@Test
	public void handleConditionalFusedCancelBoth() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .as(this::passThrough)
		                        .filter(t -> true), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}

	@Test
	public void handleCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .as(this::passThrough), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}


	@Test
	public void handleConditionalFusedCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .as(this::passThrough), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}



	@Test
	public void handleFusedCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .as(this::passThrough), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}

	@Test
	public void handleBackpressuredBothConditional() {
		TestPublisher<String> ts = TestPublisher.create();

		StepVerifier.create(ts.flux()
		                      .as(this::filterTest2), 0)
		            .thenRequest(2)
		            .then(() -> ts.next("test0", "test1"))
		            .expectNext("test0", "test1")
		            .thenRequest(1)
		            .then(() -> ts.next("test2"))
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
	public void handleBackpressuredSourceConditional() {
		TestPublisher<String> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .as(this::filterTest2)
		                      .filter(d -> true), 0)
		            .thenRequest(2)
		            .then(() -> ts.next("test0", "test1"))
		            .expectNext("test0", "test1")
		            .thenRequest(1)
		            .then(() -> ts.next("test2"))
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
	public void handleBackpressuredTargetConditional() {
		TestPublisher<String> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .hide()
		                      .as(this::filterTest2)
		                      .filter(d -> true), 0)
		            .thenRequest(2)
		            .then(() -> ts.next("test0", "test1"))
		            .expectNext("test0", "test1")
		            .thenRequest(1)
		            .then(() -> ts.next("test2"))
		            .expectNext("test2")
		            .verifyComplete();
	}

	Flux<String> passThrough(Flux<String> f) {
		return f.handle((a, b) -> b.next(a));
	}



	Flux<String> filterTest2(Flux<String> f) {
		return f.handle((a, b) -> {
			b.next(a);
			if ("test2".equals(a)) {
				b.complete();
			}
		});
	}


	@Test
	public void noFusionOnConditionalThreadBarrier() {
		StepVerifier.create(Flux.just("test", "test2")
		                        .as(this::passThrough)
		                        .distinct())
		            .expectFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER, Fuseable.NONE)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void prematureCompleteFusedSync() {
		StepVerifier.create(Flux.just("test")
		                        .as(this::passThrough)
		                        .filter(t -> true))
		            .expectFusion(Fuseable.SYNC)
		            .expectNext("test")
		            .verifyComplete();
	}

	@Test
	public void dropHandleFusedSync() {
		StepVerifier.create(Flux.just("test", "test2")
		                        .handle((data, s) -> {
		                        })
		                        .filter(t -> true))
		            .expectFusion(Fuseable.SYNC)
		            .verifyComplete();
	}



	@Test
	@SuppressWarnings("unchecked")
	public void handleFusedStateTargetAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		testUnicastSource(up);
		StepVerifier.create(up.handle((s, d) -> {
			d.complete();
		}))
		            .consumeSubscriptionWith(s -> {
			            Fuseable.QueueSubscription<String> qs =
					            ((Fuseable.QueueSubscription<String>) s);
			            qs.requestFusion(ASYNC);
			            assertThat(qs.size()).isEqualTo(3);
			            assertThat(qs.poll()).isNull();
			            assertThat(qs.poll()).isNull();
			            assertThat(qs.size()).isEqualTo(2);
			            qs.clear();
			            assertThat(qs.size()).isEqualTo(0);
		            })
		            .thenCancel()
		            .verify();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void handleFusedStateTargetConditionalAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		testUnicastSource(up);
		StepVerifier.create(up.handle((s, d) -> {
			d.complete();
		})
		                      .filter(t -> true))
		            .consumeSubscriptionWith(s -> {
			            Fuseable.QueueSubscription<String> qs =
					            ((Fuseable.QueueSubscription<String>) ((Receiver) s).upstream());
			            qs.requestFusion(ASYNC);
			            assertThat(qs.size()).isEqualTo(3);
			            assertThat(qs.poll()).isNull();
			            assertThat(qs.poll()).isNull();
			            assertThat(qs.size()).isEqualTo(2);
			            qs.clear();
			            assertThat(qs.size()).isEqualTo(0);
		            })
		            .thenCancel()
		            .verify();
	}
}
