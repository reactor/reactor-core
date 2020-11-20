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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static reactor.core.Fuseable.*;

public class FluxHandleTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(ASYNC);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.handle((s, d) -> {
					if (item(2).equals(s)) {
						d.complete();
					}
					else {
						d.next(s);
					}
				})).receiveValues(item(0), item(1)),

				scenario(f -> f.handle((s, d) -> {
					if (item(1).equals(s)) {
						d.complete();
					}
					else {
						d.next(s);
					}
				})).receiveValues(item(0)),

				scenario(f -> f.handle((s, d) -> {
					if (!item(2).equals(s)) {
						d.next(s);
					}
				})).receiveValues(item(0), item(1)),

				scenario(f -> f.handle((s, d) -> {
					if (item(2).equals(s)) {
						d.complete();
					}
					else if (item(1).equals(s)) {
						d.next(s);
					}
				})).receiveValues(item(1))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.handle((s, d) -> {
					throw exception();
				})),

				scenario(f -> f.handle((s, d) -> d.error(exception()))),

				scenario(f -> f.handle((s, d) -> {
					d.next(item(0));
					d.next(item(1));
				})),

				scenario(f -> f.handle((s, d) -> {
					d.next(null);
				}))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(f -> f.handle((data, s) -> {})),

				scenario(f -> f.handle((data, s) -> {
					if (item(2).equals(data)) {
						s.complete();
					}
					else {
						s.next(data);
					}
				})),

				scenario(f -> f.handle((data, s) -> {
					if (!item(2).equals(data)) {
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

		Flux.just(data).
				<Integer>handle((v, s) -> s.error(exception))
				.subscribe(ts);

		ts.await()
		  .assertNoValues()
		  .assertError(IllegalStateException.class)
		  .assertNotComplete();

		assertThat(exception).isSameAs(throwableInOnOperatorError.get());
		assertThat(data).isSameAs(dataInOnOperatorError.get());
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

		assertThat(exception).isSameAs(throwableInOnOperatorError.get());
		assertThat(data).isSameAs(dataInOnOperatorError.get());
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
	public void prematureCompleteFusedSync() {
		StepVerifier.create(Flux.just("test")
		                        .as(this::passThrough)
		                        .filter(t -> true))
		            .expectFusion(SYNC)
		            .expectNext("test")
		            .verifyComplete();
	}

	@Test
	public void dropHandleFusedSync() {
		StepVerifier.create(Flux.just("test", "test2")
		                        .handle((data, s) -> {
		                        })
		                        .filter(t -> true))
		            .expectFusion(SYNC)
		            .verifyComplete();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxHandle<Integer, ?> test = new FluxHandle<>(parent, (t, s) -> { });

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxHandleFuseable<Integer, ?> test = new FluxHandleFuseable<>(parent, (t, s) -> { });

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

    @Test
    public void scanSubscriber() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxHandle.HandleSubscriber<String, String> test = new FluxHandle.HandleSubscriber<>(actual, (a, b) -> {});
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.error = new IllegalStateException("boom");
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        test.onComplete();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    public void scanConditionalSubscriber() {
	    @SuppressWarnings("unchecked")
	    Fuseable.ConditionalSubscriber<? super Object> subscriber = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
        FluxHandle.HandleConditionalSubscriber<String, String> test =
        		new FluxHandle.HandleConditionalSubscriber<>(subscriber, (a, b) -> {});
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(subscriber);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.error = new IllegalStateException("boom");
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        test.onComplete();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    public void scanFuseableSubscriber() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxHandleFuseable.HandleFuseableSubscriber<String, String> test =
        		new FluxHandleFuseable.HandleFuseableSubscriber<>(actual, (a, b) -> {});
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.error = new IllegalStateException("boom");
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        test.onComplete();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    public void scanFuseableConditionalSubscriber() {
	    @SuppressWarnings("unchecked")
	    Fuseable.ConditionalSubscriber<? super Object> subscriber = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
	    FluxHandleFuseable.HandleFuseableConditionalSubscriber<String, String> test =
        		new FluxHandleFuseable.HandleFuseableConditionalSubscriber<>(subscriber, (a, b) -> {});
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(subscriber);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.error = new IllegalStateException("boom");
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        test.onComplete();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }


	@Test
	public void contextTest() {
		StepVerifier.create(Flux.just("foo")
		                        .handle((d, s) -> s.next(s.currentContext()
		                                               .get(AtomicInteger.class)
		                                               .incrementAndGet()))
		                        .repeat(9)
		                        .contextWrite(ctx -> ctx.put(AtomicInteger.class,
				                        new AtomicInteger())))
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	@Test
	public void contextTestHide() {
		StepVerifier.create(Flux.just("foo")
		                        .hide()
		                        .handle((d, s) -> s.next(s.currentContext()
		                                               .get(AtomicInteger.class)
		                                               .incrementAndGet()))
		                        .repeat(9)
		                        .contextWrite(ctx -> ctx.put(AtomicInteger.class,
				                        new AtomicInteger())))
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	@Test
	public void contextTestFilter() {
		StepVerifier.create(Flux.just("foo")
		                        .handle((d, s) -> s.next(s.currentContext()
		                                               .get(AtomicInteger.class)
		                                               .incrementAndGet()))
		                        .filter(d -> true)
		                        .repeat(9)
		                        .contextWrite(ctx -> ctx.put(AtomicInteger.class,
				                        new AtomicInteger())))
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}
	@Test
	public void contextTestFilterHide() {
		StepVerifier.create(Flux.just("foo")
		                        .handle((d, s) -> s.next(s.currentContext()
		                                               .get(AtomicInteger.class)
		                                               .incrementAndGet()))
		                        .filter(d -> true)
		                        .repeat(9)
		                        .contextWrite(ctx -> ctx.put(AtomicInteger.class,
				                        new AtomicInteger())))
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	@Test
	public void nextAfterCompleteNormal() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .handle((v, sink) -> {
			                        sink.complete();
			                        sink.next(2);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		            .hasMessage("Cannot emit after a complete or error"));
	}

	@Test
	public void nextAfterErrorNormal() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .handle((v, sink) -> {
			                        sink.error(new NullPointerException("boom"));
			                        sink.next(2);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot emit after a complete or error"));
	}


	@Test
	public void errorAfterCompleteNormal() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .handle((v, sink) -> {
			                        sink.complete();
			                        sink.error(new NullPointerException("boom"));
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot error after a complete or error"));
	}


	@Test
	public void completeAfterErrorNormal() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .handle((v, sink) -> {
			                        sink.error(new NullPointerException("boom"));
			                        sink.complete();
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot complete after a complete or error"));
	}

	@Test
	public void nextAfterNextNormal() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .handle((v, sink) -> {
			                        sink.next(v);
			                        sink.next(v + 1);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot emit more than one data"));
	}

	@Test
	public void nextAfterCompleteNormalConditional() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .filter(i -> true)
		                        .handle((v, sink) -> {
			                        sink.complete();
			                        sink.next(2);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		            .hasMessage("Cannot emit after a complete or error"));
	}

	@Test
	public void nextAfterErrorNormalConditional() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .filter(i -> true)
		                        .handle((v, sink) -> {
			                        sink.error(new NullPointerException("boom"));
			                        sink.next(2);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot emit after a complete or error"));
	}


	@Test
	public void errorAfterCompleteNormalConditional() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .filter(i -> true)
		                        .handle((v, sink) -> {
			                        sink.complete();
			                        sink.error(new NullPointerException("boom"));
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot error after a complete or error"));
	}


	@Test
	public void completeAfterErrorNormalConditional() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .filter(i -> true)
		                        .handle((v, sink) -> {
			                        sink.error(new NullPointerException("boom"));
			                        sink.complete();
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot complete after a complete or error"));
	}

	@Test
	public void nextAfterNextNormalConditional() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .filter(i -> true)
		                        .handle((v, sink) -> {
			                        sink.next(v);
			                        sink.next(v + 1);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot emit more than one data"));
	}

	@Test
	public void nextAfterCompleteFused() {
		StepVerifier.create(Flux.just(1)
		                        .handle((v, sink) -> {
			                        sink.complete();
			                        sink.next(2);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		            .hasMessage("Cannot emit after a complete or error"));
	}

	@Test
	public void nextAfterErrorFused() {
		StepVerifier.create(Flux.just(1)
		                        .handle((v, sink) -> {
			                        sink.error(new NullPointerException("boom"));
			                        sink.next(2);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot emit after a complete or error"));
	}

    @Test
    public void runtimeExceptionFused() {
        // Check issue that for fuseable and Exception thrown inside handle it did not propagate signal -> hanging flux
        //  and throws IllegalStateException: Timeout on blocking read
        assertThatNullPointerException().isThrownBy(() -> {
            Flux.range(0, 10)
                .handle((v, sink) -> {
                    throw new NullPointerException("boom");
                })
                .blockLast(Duration.ofSeconds(1));
        }).withMessage("boom");
    }

	@Test
	public void errorAfterCompleteFused() {
		StepVerifier.create(Flux.just(1)
		                        .handle((v, sink) -> {
			                        sink.complete();
			                        sink.error(new NullPointerException("boom"));
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot error after a complete or error"));
	}


	@Test
	public void completeAfterErrorFused() {
		StepVerifier.create(Flux.just(1)
		                        .handle((v, sink) -> {
			                        sink.error(new NullPointerException("boom"));
			                        sink.complete();
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot complete after a complete or error"));
	}

	@Test
	public void nextAfterNextFused() {
		StepVerifier.create(Flux.just(1)
		                        .handle((v, sink) -> {
			                        sink.next(v);
			                        sink.next(v + 1);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot emit more than one data"));
	}

	@Test
	public void nextAfterCompleteFusedConditional() {
		StepVerifier.create(Flux.just(1)
		                        .filter(i -> true)
		                        .handle((v, sink) -> {
			                        sink.complete();
			                        sink.next(2);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		            .hasMessage("Cannot emit after a complete or error"));
	}

	@Test
	public void nextAfterErrorFusedConditional() {
		StepVerifier.create(Flux.just(1)
		                        .filter(i -> true)
		                        .handle((v, sink) -> {
			                        sink.error(new NullPointerException("boom"));
			                        sink.next(2);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot emit after a complete or error"));
	}


	@Test
	public void errorAfterCompleteFusedConditional() {
		StepVerifier.create(Flux.just(1)
		                        .filter(i -> true)
		                        .handle((v, sink) -> {
			                        sink.complete();
			                        sink.error(new NullPointerException("boom"));
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot error after a complete or error"));
	}

	@Test
	public void completeAfterErrorFusedConditional() {
		StepVerifier.create(Flux.just(1)
		                        .filter(i -> true)
		                        .handle((v, sink) -> {
			                        sink.error(new NullPointerException("boom"));
			                        sink.complete();
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot complete after a complete or error"));
	}

	@Test
	public void nextAfterNextFusedConditional() {
		StepVerifier.create(Flux.just(1)
		                        .filter(i -> true)
		                        .handle((v, sink) -> {
			                        sink.next(v);
			                        sink.next(v + 1);
		                        }))
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Cannot emit more than one data"));
	}

	@Test
	public void failureStrategyResumeExceptionThrown() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			AtomicLong r = new AtomicLong();
			StepVerifier.create(Flux.range(0, 2)
			                        .doOnRequest(r::addAndGet)
			                        .hide()
			                        .handle((i, sink) -> sink.next(4 / i)), 1)
			            .expectNoFusionSupport()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorWithMessage("/ by zero");

			assertThat(r.get()).as("amount requested").isEqualTo(2L);
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeExceptionSignalled() {
		Throwable error = new Throwable();
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			AtomicLong r = new AtomicLong();
			StepVerifier.create(Flux.range(0, 2)
			                        .doOnRequest(r::addAndGet)
			                        .hide()
			                        .handle((i, sink) -> {
				                        if (i == 0) {
					                        sink.error(error);
				                        }
				                        else {
					                        sink.next(4 / i);
				                        }
			                        }), 1)
			            .expectNoFusionSupport()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorMatching(throwable -> error == throwable);

			assertThat(r.get()).as("amount requested").isEqualTo(2L);
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeTryOnNextExceptionThrown() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			StepVerifier.create(Flux.range(0, 2)
			                        .distinctUntilChanged()
			                        .handle((i, sink) -> sink.next(4 / i)), 1)
			            .expectNoFusionSupport()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorWithMessage("/ by zero");
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeTryOnNextExceptionSignalled() {
		Throwable error = new Throwable();
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			StepVerifier.create(Flux.range(0, 2)
			                        .distinctUntilChanged()
			                        .handle((i, sink) -> {
				                        if (i == 0) {
					                        sink.error(error);
				                        }
				                        else {
					                        sink.next(4 / i);
				                        }
			                        }), 1)
			            .expectNoFusionSupport()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorMatching(throwable -> error == throwable);
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeConditionalExceptionThrown() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			AtomicLong r = new AtomicLong();
			StepVerifier.create(Flux.range(0, 2)
			                        .doOnRequest(r::addAndGet)
			                        .hide()
			                        .handle((i, sink) -> sink.next(4 / i))
			                        .filter(i -> true), 1)
			            .expectNoFusionSupport()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorWithMessage("/ by zero");

			assertThat(r.get()).as("amount requested").isEqualTo(2L);
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeConditionalExceptionSignalled() {
		Throwable error = new Throwable();
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			AtomicLong r = new AtomicLong();
			StepVerifier.create(Flux.range(0, 2)
			                        .doOnRequest(r::addAndGet)
			                        .hide()
			                        .handle((i, sink) -> {
				                        if (i == 0) {
					                        sink.error(error);
				                        }
				                        else {
					                        sink.next(4 / i);
				                        }
			                        })
			                        .filter(i -> true), 1)
			            .expectNoFusionSupport()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorMatching(throwable -> error == throwable);

			assertThat(r.get()).as("amount requested").isEqualTo(2L);
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeConditionalTryOnNextExceptionThrown() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			StepVerifier.create(Flux.range(0, 2)
			                        .distinctUntilChanged()
			                        .handle((i, sink) -> sink.next(4 / i))
			                        .filter(i -> true))
			            .expectNoFusionSupport()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorWithMessage("/ by zero");
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeConditionalTryOnNextExceptionSignalled() {
		Throwable error = new Throwable();
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			StepVerifier.create(Flux.range(0, 2)
			                        .distinctUntilChanged()
			                        .handle((i, sink) -> {
				                        if (i == 0) {
					                        sink.error(error);
				                        }
				                        else {
					                        sink.next(4 / i);
				                        }
			                        })
			                        .filter(i -> true))
			            .expectNoFusionSupport()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorMatching(throwable -> error == throwable);
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeExceptionThrownFuseable() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			StepVerifier.create(Flux.range(0, 2)
			                        .handle((i, sink) -> sink.next(4 / i)), 1)
			            .expectFusion()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorWithMessage("/ by zero");
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeExceptionSignalledFuesable() {
		Throwable error = new Throwable();
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			StepVerifier.create(Flux.range(0, 2)
			                        .handle((i, sink) -> {
				                        if (i == 0) {
					                        sink.error(error);
				                        }
				                        else {
					                        sink.next(4 / i);
				                        }
			                        }), 1)
			            .expectFusion()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorMatching(throwable -> error == throwable);
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeConditionalExceptionThrownFuseable() {
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			StepVerifier.create(Flux.range(0, 2)
			                        .handle((i, sink) -> sink.next(4 / i))
			                        .filter(i -> true), 1)
			            .expectFusion()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorWithMessage("/ by zero");
		}
		finally {
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void failureStrategyResumeConditionalExceptionSignalledFuseable() {
		Throwable error = new Throwable();
		Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
		try {
			StepVerifier.create(Flux.range(0, 2)
			                        .handle((i, sink) -> {
				                        if (i == 0) {
					                        sink.error(error);
				                        }
				                        else {
					                        sink.next(4 / i);
				                        }
			                        })
			                        .filter(i -> true), 1)
			            .expectFusion()
			            .expectNext(4)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly(0)
			            .hasDroppedErrorMatching(throwable -> error == throwable);
		}
		finally {
			Hooks.resetOnNextError();
		}
	}
}
