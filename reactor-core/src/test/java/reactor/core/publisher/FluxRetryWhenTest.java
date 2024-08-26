/*
 * Copyright (c) 2016-2024 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.LongAssert;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;
import reactor.util.context.ContextView;
import reactor.util.function.Tuple2;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;

public class FluxRetryWhenTest {

	Flux<Integer> justError = Flux.concat(Flux.just(1),
			Flux.error(new RuntimeException("forced failure 0")));

	Flux<Integer> rangeError = Flux.concat(Flux.range(1, 2),
			Flux.error(new RuntimeException("forced failure 0")));

	@Test
	// https://github.com/reactor/reactor-core/issues/3314
	void ensuresContextIsRestoredInRetryFunctions() {
		PublisherProbe<Void> doBeforeRetryProbe = PublisherProbe.empty();
		AtomicReference<ContextView> capturedContext = new AtomicReference<>();

		RetrySpec spec = Retry.max(1)
		                      .doBeforeRetryAsync(
				                      retrySignal ->
						                      Mono.deferContextual(cv -> {
							                      capturedContext.set(cv);
												  return doBeforeRetryProbe.mono();
						                      })
		                      );

		Context context = Context.of("test", "test");

        Mono.defer(new Supplier<Mono<?>>() {
				int index = 0;

                @Override
                public Mono<?> get() {
					if (index++ == 0) {
						return Mono.error(new RuntimeException());
					} else {
						return Mono.just("someValue");
					}
                }
            })
            .retryWhen(spec)
            .contextWrite(context)
            .as(StepVerifier::create)
            .expectNext("someValue")
            .verifyComplete();

		doBeforeRetryProbe.assertWasSubscribed();
		assertThat(capturedContext).hasValueMatching(c -> c.hasKey("test"));
	}

	@Test
	//https://github.com/reactor/reactor-core/issues/3253
	public void shouldFailWhenOnErrorContinueEnabled() {
		Mono.create(sink -> {
					throw new RuntimeException("blah");
				})
				.retryWhen(Retry.indefinitely().filter(t -> false))
				.onErrorContinue((e, o) -> {})
				.as(StepVerifier::create)
				.expectError()
				.verify(Duration.ofSeconds(10));
	}

	@Test
	//https://github.com/reactor/reactor-core/issues/3253
	public void shouldWorkAsExpected() {
		Mono.just(1)
			.map(v -> { // ensure original context is propagated
				throw new RuntimeException("boom");
			})
			.retryWhen(Retry.indefinitely().filter(t -> false))
			.onErrorContinue((e, o) -> {})
			.as(StepVerifier::create)
			.expectComplete()
			.verify(Duration.ofSeconds(10));
	}

	@Test
	public void dontRepeat() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		rangeError.retryWhen(Retry.indefinitely().filter(e -> false))
		      .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure 0")
		  .assertNotComplete();
	}

	@Test
	public void predicateThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		rangeError
				.retryWhen(
						Retry.indefinitely()
						     .filter(e -> {
							     throw new RuntimeException("forced failure");
						     })
				)
				.subscribe(ts);

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void twoRetryNormal() {
		AtomicInteger i = new AtomicInteger();

		Mono<Long> source = Flux
				.just("test", "test2", "test3")
				.doOnNext(d -> {
					if (i.getAndIncrement() < 2) {
						throw new RuntimeException("test");
					}
				})
				.retryWhen(Retry.indefinitely().filter(e -> i.get() <= 2))
				.count();

		StepVerifier.create(source)
		            .expectNext(3L)
		            .expectComplete()
		            .verify();
	}


	@Test
	public void twoRetryNormalSupplier() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		Flux<Integer> source = Flux.defer(() -> {
			return Flux.defer(() -> Flux.just(i.incrementAndGet()))
			           .doOnNext(v -> {
				           if (v < 4) {
					           throw new RuntimeException("test");
				           }
				           else {
					           bool.set(false);
				           }
			           })
			           .retryWhen(Retry.indefinitely().filter(Flux.countingPredicate(e -> bool.get(), 3)));
		});

		StepVerifier.create(source)
		            .expectNext(4)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void twoRetryErrorSupplier() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		Flux<Integer> source = Flux.defer(() -> {
			return Flux.defer(() -> Flux.just(i.incrementAndGet()))
			           .doOnNext(v -> {
				           if (v < 4) {
					           if (v > 2) {
						           bool.set(false);
					           }
					           throw new RuntimeException("test");
				           }
			           })
			           .retryWhen(Retry.indefinitely().filter(Flux.countingPredicate(e -> bool.get(), 3)));
		});

		StepVerifier.create(source)
		            .verifyErrorMessage("test");
	}

	@Test
	public void twoRetryNormalSupplier3() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		Flux<Integer> source = Flux.defer(() -> {
			return Flux.defer(() -> Flux.just(i.incrementAndGet()))
			           .doOnNext(v -> {
				           if (v < 4) {
					           throw new RuntimeException("test");
				           }
				           else {
					           bool.set(false);
				           }
			           })
			           .retryWhen(Retry.indefinitely().filter(Flux.countingPredicate(e -> bool.get(), 2)));
		});

		StepVerifier.create(source)
		            .verifyErrorMessage("test");
	}

	@Test
	public void twoRetryNormalSupplier2() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		Flux<Integer> source = Flux.defer(() -> {
			return Flux.defer(() -> Flux.just(i.incrementAndGet()))
			           .doOnNext(v -> {
				           if (v < 4) {
					           throw new RuntimeException("test");
				           }
				           else {
					           bool.set(false);
				           }
			           })
			           .retryWhen(Retry.indefinitely().filter(Flux.countingPredicate(e -> bool.get(), 0)));
		});

		StepVerifier.create(source)
		            .expectNext(4)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void twoRetryErrorSupplier2() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		Flux<Integer> source = Flux.defer(() -> {
			return Flux.defer(() -> Flux.just(i.incrementAndGet()))
			           .doOnNext(v -> {
				           if (v < 4) {
					           if (v > 2) {
						           bool.set(false);
					           }
					           throw new RuntimeException("test");
				           }
			           })
			           .retryWhen(Retry.indefinitely().filter(Flux.countingPredicate(e -> bool.get(), 0)));
		});

		StepVerifier.create(source)
		            .verifyErrorMessage("test");
	}

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxRetryWhen<>(null, Retry.from(v -> v));
		});
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void whenRetrySignalFactoryNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never().retryWhen((Retry) null);
		});
	}

	@Test
	public void cancelsOther() {
		AtomicBoolean cancelled = new AtomicBoolean();
		Flux<Integer> when = Flux.range(1, 10)
		                         .doOnCancel(() -> cancelled.set(true));

		StepVerifier.create(justError.retryWhen(Retry.from(other -> when)))
		            .thenCancel()
		            .verify();

		assertThat(cancelled.get()).isTrue();
	}

	@Test
	public void cancelTwiceCancelsOtherOnce() {
		AtomicInteger cancelled = new AtomicInteger();
		Flux<Integer> when = Flux.range(1, 10)
		                         .doOnCancel(cancelled::incrementAndGet);

		justError.retryWhen(Retry.from(other -> when))
		         .subscribe(new BaseSubscriber<Integer>() {
			         @Override
			         protected void hookOnSubscribe(Subscription subscription) {
				         subscription.request(1);
				         subscription.cancel();
				         subscription.cancel();
			         }
		         });

		assertThat(cancelled).hasValue(1);
	}

	@Test
	public void directOtherErrorPreventsSubscribe() {
		AtomicBoolean sourceSubscribed = new AtomicBoolean();
		AtomicBoolean sourceCancelled = new AtomicBoolean();
		Flux<Integer> source = justError
		                           .doOnSubscribe(sub -> sourceSubscribed.set(true))
		                           .doOnCancel(() -> sourceCancelled.set(true));

		Flux<Integer> retry = source.retryWhen(Retry.from(other -> Mono.error(new IllegalStateException("boom"))));

		StepVerifier.create(retry)
		            .expectSubscription()
		            .verifyErrorMessage("boom");

		assertThat(sourceSubscribed.get()).isFalse();
		assertThat(sourceCancelled.get()).isFalse();
	}

	@Test
	public void lateOtherErrorCancelsSource() {
		AtomicBoolean sourceSubscribed = new AtomicBoolean();
		AtomicBoolean sourceCancelled = new AtomicBoolean();
		AtomicInteger count = new AtomicInteger();
		Flux<Integer> source = justError
		                           .doOnSubscribe(sub -> sourceSubscribed.set(true))
		                           .doOnCancel(() -> sourceCancelled.set(true));


		Flux<Integer> retry = source.retryWhen(Retry.from(other -> other.flatMap(l ->
				count.getAndIncrement() == 0 ? Mono.just(l) : Mono.<Long>error(new IllegalStateException("boom")))));

		StepVerifier.create(retry)
		            .expectSubscription()
		            .expectNext(1)
		            .expectNext(1)
		            .verifyErrorMessage("boom");

		assertThat(sourceSubscribed.get()).isTrue();
		assertThat(sourceCancelled.get()).isTrue();
	}

	@Test
	public void directOtherEmptyPreventsSubscribeAndCompletes() {
		AtomicBoolean sourceSubscribed = new AtomicBoolean();
		AtomicBoolean sourceCancelled = new AtomicBoolean();
		Flux<Integer> source = justError
		                           .doOnSubscribe(sub -> sourceSubscribed.set(true))
		                           .doOnCancel(() -> sourceCancelled.set(true));

		Flux<Integer> retry = source.retryWhen(Retry.from(other -> Flux.empty()));

		StepVerifier.create(retry)
		            .expectSubscription()
		            .verifyComplete();

		assertThat(sourceSubscribed.get()).isFalse();
		assertThat(sourceCancelled.get()).isFalse();
	}

	@Test
	public void lateOtherEmptyCancelsSourceAndCompletes() {
		AtomicBoolean sourceSubscribed = new AtomicBoolean();
		AtomicBoolean sourceCancelled = new AtomicBoolean();
		Flux<Integer> source = justError
		                           .doOnSubscribe(sub -> sourceSubscribed.set(true))
		                           .doOnCancel(() -> sourceCancelled.set(true));

		Flux<Integer> retry = source.retryWhen(Retry.from(other -> other.take(1, false)));

		StepVerifier.create(retry)
		            .expectSubscription()
		            .expectNext(1) //original
		            .expectNext(1) //retry
		            .verifyComplete(); //retry terminated

		assertThat(sourceSubscribed.get()).isTrue();
		assertThat(sourceCancelled.get()).isTrue();
	}

	@Test
	public void coldRepeater() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		justError.retryWhen(Retry.from(other -> Flux.range(1, 10)))
		         .subscribe(ts);

		ts.assertValues(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void coldRepeaterBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		rangeError.retryWhen(Retry.from(other -> Flux.range(1, 5)))
		          .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2, 1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void coldEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		rangeError.retryWhen(Retry.from(other -> Flux.empty()))
		          .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void coldError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		rangeError.retryWhen(Retry.from(other -> Flux.error(new RuntimeException("forced failure"))))
		          .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void whenFactoryThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		rangeError.retryWhen(Retry.from(other -> {
			throw new RuntimeException("forced failure");
		}))
		          .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void whenFactoryReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		rangeError.retryWhen(Retry.from(other -> null))
		          .subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void retryErrorsInResponse() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		rangeError.retryWhen(Retry.from(v -> v.map(a -> {
			throw new RuntimeException("forced failure");
		})))
		          .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

	}

	@Test
	public void retryAlways() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		rangeError.retryWhen(Retry.from(v -> v))
		          .subscribe(ts);

		ts.request(8);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();
	}

	Flux<String> linearRetryScenario() {
		AtomicInteger i = new AtomicInteger();
		return Flux.<String>create(s -> {
			if (i.incrementAndGet() == 4) {
				s.next("hey");
				s.complete();
			}
			else {
				s.error(new RuntimeException("test " + i));
			}
		}).retryWhen(Retry
				.max(3)
				.doBeforeRetry(rs -> System.out.println(rs.copy()))
				.doBeforeRetryAsync(rs -> Mono.delay(Duration.ofSeconds(rs.totalRetries() + 1)).then())
		);
	}

	Flux<String> fixedDelaysRetryScenario() {
		AtomicInteger i = new AtomicInteger();
		return Flux.<String>create(s -> {
			if (i.incrementAndGet() == 4) {
				s.next("hey");
				s.complete();
			}
			else {
				s.error(new RuntimeException("test " + i));
			}
		}).retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(3))
				.doBeforeRetry(rs -> System.out.println(rs.copy()))
		);
	}

	@Test
	public void linearRetry() {
		StepVerifier.withVirtualTime(this::linearRetryScenario)
		            .thenAwait(Duration.ofSeconds(6))
		            .expectNext("hey")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void fixedDelaysRetry() {
		StepVerifier.withVirtualTime(this::fixedDelaysRetryScenario)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(3 * 3))
		            .expectNext("hey")
		            .expectComplete()
		            .verify();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxRetryWhen<Integer> test = new FluxRetryWhen<>(parent, Retry.from(other -> Flux.just(2)));

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanMainSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxRetryWhen.RetryWhenMainSubscriber<Integer> test =
        		new FluxRetryWhen.RetryWhenMainSubscriber<>(actual, null, Flux.empty(), Context.empty());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.requested = 35;
        Assertions.assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxRetryWhen.RetryWhenMainSubscriber<Integer> main =
        		new FluxRetryWhen.RetryWhenMainSubscriber<>(actual, null, Flux.empty(), Context.empty());
        FluxRetryWhen.RetryWhenOtherSubscriber test = new FluxRetryWhen.RetryWhenOtherSubscriber();
        test.main = main;

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main.otherArbiter);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }


	@Test
	public void inners() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		Sinks.Many<Retry.RetrySignal> signaller = Sinks.unsafe().many().multicast().directBestEffort();
		Flux<Integer> when = Flux.empty();
		FluxRetryWhen.RetryWhenMainSubscriber<Integer> main = new FluxRetryWhen
				.RetryWhenMainSubscriber<>(actual, signaller, when, Context.empty());

		List<Scannable> inners = main.inners().collect(Collectors.toList());

		assertThat(inners).containsExactly((Scannable) signaller, main.otherArbiter);
	}

	@Test
	public void retryContextExposedOnRetrySignal() {
		AtomicInteger i = new AtomicInteger();

		AtomicBoolean needsRollback = new AtomicBoolean();
		ContextView ctx = Context.of("needsRollback", needsRollback);

		Mono<Long> source = Flux
				.just("test", "test2", "test3")
				.doOnNext(d -> {
					if (i.getAndIncrement() < 2) {
						assertThat(needsRollback.compareAndSet(false, true)).as("needsRollback").isTrue();
						throw new RuntimeException("test");
					}
				})
				.retryWhen(Retry.indefinitely()
						.withRetryContext(ctx)
						.doBeforeRetry(rs -> {
							AtomicBoolean atomic = rs.retryContextView().get("needsRollback");
							assertThat(atomic.compareAndSet(true, false)).as("needsRollback").isTrue();
						}))
				.count();

		StepVerifier.create(source)
				.expectNext(3L)
				.expectComplete()
				.verify();

	}

	@Test
	public void retryWhenContextTrigger_MergesOriginalContext() {
		final int RETRY_COUNT = 3;
		List<Integer> retriesLeft = Collections.synchronizedList(new ArrayList<>(4));
		List<ContextView> contextPerRetry = Collections.synchronizedList(new ArrayList<>(4));

		Flux<Object> retryWithContext =
				Flux.error(new IllegalStateException("boom"))
				    .doOnEach(sig -> {
					    retriesLeft.add(sig.getContextView().get("retriesLeft"));
					    if (!sig.isOnNext()) {
						    contextPerRetry.add(sig.getContextView());
					    }
				    })
				    .retryWhen(Retry.from(retrySignalFlux -> retrySignalFlux.handle((rs, sink) -> {
	                    ContextView ctxView = sink.contextView();
	                    int rl = ctxView.getOrDefault("retriesLeft", 0);
	                    if (rl > 0) {
		                    sink.next(Context.of("retriesLeft", rl - 1));
	                    }
	                    else {
		                    sink.error(Exceptions.retryExhausted("retries exhausted", rs.failure()));
	                    }
                    })))
				    .contextWrite(Context.of("retriesLeft", RETRY_COUNT))
					.contextWrite(Context.of("thirdPartyContext", "present"));

		StepVerifier.create(retryWithContext)
		            .expectErrorSatisfies(e -> assertThat(e).matches(Exceptions::isRetryExhausted, "isRetryExhausted")
		                                                    .hasMessage("retries exhausted")
		                                                    .hasCause(new IllegalStateException("boom")))
		            .verify(Duration.ofSeconds(1));

		assertThat(retriesLeft).containsExactly(3, 2, 1, 0);
		assertThat(contextPerRetry).allMatch(ctx -> ctx.hasKey("thirdPartyContext"));
	}

	@Test
	public void fluxRetryRandomBackoff() {
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Flux.concat(Flux.range(0, 2), Flux.error(exception))
				    .retryWhen(Retry
						    .backoff(4, Duration.ofMillis(100))
						    .maxBackoff(Duration.ofMillis(2000))
						    .jitter(0.1)
				    )
				    .elapsed()
				    .doOnNext(elapsed -> { if (elapsed.getT2() == 0) elapsedList.add(elapsed.getT1());} )
				    .map(Tuple2::getT2)
		)
		            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
		            .expectNext(0, 1) //normal output
		            .expectNext(0, 1, 0, 1, 0, 1, 0, 1) //4 retry attempts
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

		assertThat(elapsedList).hasSize(5);
		assertThat(elapsedList, LongAssert.class).first()
				.isEqualTo(0L);
		assertThat(elapsedList, LongAssert.class).element(1)
				.isCloseTo(100, Percentage.withPercentage(10));
		assertThat(elapsedList, LongAssert.class).element(2)
				.isCloseTo(200, Percentage.withPercentage(10));
		assertThat(elapsedList, LongAssert.class).element(3)
				.isCloseTo(400, Percentage.withPercentage(10));
		assertThat(elapsedList, LongAssert.class).element(4)
				.isCloseTo(800, Percentage.withPercentage(10));
	}

	@Test
	public void fluxRetryRandomBackoffDefaultJitter() {
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Flux.concat(Flux.range(0, 2), Flux.error(exception))
				    .retryWhen(Retry
						    .backoff(4, Duration.ofMillis(100))
						    .maxBackoff(Duration.ofMillis(2000))
				    )
				    .elapsed()
				    .doOnNext(elapsed -> { if (elapsed.getT2() == 0) elapsedList.add(elapsed.getT1());} )
				    .map(Tuple2::getT2)
		)
		            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
		            .expectNext(0, 1) //normal output
		            .expectNext(0, 1, 0, 1, 0, 1, 0, 1) //4 retry attempts
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

		assertThat(elapsedList).hasSize(5);
		assertThat(elapsedList, LongAssert.class).first()
				.isEqualTo(0L);
		assertThat(elapsedList, LongAssert.class).element(1)
				.isCloseTo(100, Percentage.withPercentage(50));
		assertThat(elapsedList, LongAssert.class).element(2)
				.isCloseTo(200, Percentage.withPercentage(50));
		assertThat(elapsedList, LongAssert.class).element(3)
				.isCloseTo(400, Percentage.withPercentage(50));
		assertThat(elapsedList, LongAssert.class).element(4)
				.isCloseTo(800, Percentage.withPercentage(50));
	}

	@Test
	public void fluxRetryRandomBackoffDefaultMaxDuration() {
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Flux.concat(Flux.range(0, 2), Flux.error(exception))
				    .retryWhen(Retry.backoff(4, Duration.ofMillis(100)))
				    .elapsed()
				    .doOnNext(elapsed -> { if (elapsed.getT2() == 0) elapsedList.add(elapsed.getT1());} )
				    .map(Tuple2::getT2)
		)
		            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
		            .expectNext(0, 1) //normal output
		            .expectNext(0, 1, 0, 1, 0, 1, 0, 1) //4 retry attempts
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

		assertThat(elapsedList).hasSize(5);
		assertThat(elapsedList, LongAssert.class).first()
				.isEqualTo(0L);
		assertThat(elapsedList, LongAssert.class).element(1)
				.isCloseTo(100, Percentage.withPercentage(50));
		assertThat(elapsedList, LongAssert.class).element(2)
				.isCloseTo(200, Percentage.withPercentage(50));
		assertThat(elapsedList, LongAssert.class).element(3)
				.isCloseTo(400, Percentage.withPercentage(50));
		assertThat(elapsedList, LongAssert.class).element(4)
				.isCloseTo(800, Percentage.withPercentage(50));
	}

	@Test
	public void fluxRetryRandomBackoffMaxDurationWithMultiplier() {
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				            Flux.concat(Flux.range(0, 2), Flux.error(exception))
				                .retryWhen(Retry.backoff(4, Duration.ofMillis(100))
				                                .multiplier(5))
				                .elapsed()
				                .doOnNext(elapsed -> { if (elapsed.getT2() == 0) elapsedList.add(elapsed.getT1());} )
				                .map(Tuple2::getT2)
		            )
		            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
		            .expectNext(0, 1) //normal output
		            .expectNext(0, 1, 0, 1, 0, 1, 0, 1) //4 retry attempts
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

		assertThat(elapsedList).hasSize(5);
		assertThat(elapsedList, LongAssert.class).first()
		                                         .isEqualTo(0L);
		assertThat(elapsedList, LongAssert.class).element(1)
		                                         .isCloseTo(100, Percentage.withPercentage(50));
		assertThat(elapsedList, LongAssert.class).element(2)
		                                         .isCloseTo(500, Percentage.withPercentage(50));
		assertThat(elapsedList, LongAssert.class).element(3)
		                                         .isCloseTo(2500, Percentage.withPercentage(50));
		assertThat(elapsedList, LongAssert.class).element(4)
		                                         .isCloseTo(12500, Percentage.withPercentage(50));
	}

	@Test
	public void fluxRetryRandomBackoff_maxBackoffShaves() {
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Flux.concat(Flux.range(0, 2), Flux.error(exception))
				    .retryWhen(Retry
						    .backoff(4, Duration.ofMillis(100))
						    .maxBackoff(Duration.ofMillis(220))
						    .jitter(0.9)
				    )
				    .elapsed()
				    .doOnNext(elapsed -> { if (elapsed.getT2() == 0) elapsedList.add(elapsed.getT1());} )
				    .map(Tuple2::getT2)
		)
		            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
		            .expectNext(0, 1) //normal output
		            .expectNext(0, 1, 0, 1, 0, 1, 0, 1) //4 retry attempts
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

		assertThat(elapsedList).hasSize(5);
		assertThat(elapsedList, LongAssert.class)
				.first()
				.isEqualTo(0L);
		assertThat(elapsedList, LongAssert.class)
				.element(1)
				.isGreaterThanOrEqualTo(100) //min backoff
				.isCloseTo(100, Percentage.withPercentage(90));
		assertThat(elapsedList, LongAssert.class)
				.element(2)
				.isCloseTo(200, Percentage.withPercentage(90))
				.isGreaterThanOrEqualTo(100)
				.isLessThanOrEqualTo(220);
		assertThat(elapsedList, LongAssert.class)
				.element(3)
				.isGreaterThanOrEqualTo(100)
				.isLessThanOrEqualTo(220);
		assertThat(elapsedList, LongAssert.class)
				.element(4)
				.isGreaterThanOrEqualTo(100)
				.isLessThanOrEqualTo(220);
	}

	@Test
	public void fluxRetryRandomBackoff_minBackoffFloor() {
		for (int i = 0; i < 50; i++) {
			Exception exception = new IOException("boom retry loop #" + i);
			List<Long> elapsedList = new ArrayList<>();

			StepVerifier.withVirtualTime(() ->
					Flux.concat(Flux.range(0, 2), Flux.error(exception))
					    .retryWhen(Retry
							    .backoff(1, Duration.ofMillis(100))
							    .maxBackoff(Duration.ofMillis(2000))
							    .jitter(0.9)
					    )
					    .elapsed()
					    .doOnNext(elapsed -> { if (elapsed.getT2() == 0) elapsedList.add(elapsed.getT1());} )
					    .map(Tuple2::getT2)
			)
			            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
			            .expectNext(0, 1) //normal output
			            .expectNext(0, 1) //1 retry attempts
			            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
			                                                    .hasMessage("Retries exhausted: 1/1")
			                                                    .hasCause(exception))
			            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

			assertThat(elapsedList).hasSize(2);
			assertThat(elapsedList, LongAssert.class)
					.first()
					.isEqualTo(0L);
			assertThat(elapsedList, LongAssert.class)
					.element(1)
					.isGreaterThanOrEqualTo(100) //min backoff
					.isCloseTo(100, Percentage.withPercentage(90));
		}
	}

	@Test
	public void fluxRetryRandomBackoff_noRandomness() {
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Flux.concat(Flux.range(0, 2), Flux.error(exception))
				    .retryWhen(Retry
						    .backoff(4, Duration.ofMillis(100))
						    .maxBackoff(Duration.ofMillis(2000))
						    .jitter(0)
				    )
				    .elapsed()
				    .doOnNext(elapsed -> { if (elapsed.getT2() == 0) elapsedList.add(elapsed.getT1());} )
				    .map(Tuple2::getT2)
		)
		            .thenAwait(Duration.ofMinutes(1)) //ensure whatever the jittered delay that we have time to fit 4 retries
		            .expectNext(0, 1) //normal output
		            .expectNext(0, 1, 0, 1, 0, 1, 0, 1) //4 retry attempts
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofSeconds(1)); //vts test shouldn't even take that long

		assertThat(elapsedList).containsExactly(0L, 100L, 200L, 400L, 800L);
	}

	@Test
	public void fluxRetryRandomBackoffNoArithmeticException() {
		final Duration EXPLICIT_MAX = Duration.ofSeconds(100_000);
		final Duration INIT = Duration.ofSeconds(10);

		StepVerifier.withVirtualTime(() -> {
			RetryBackoffSpec retryBuilder = Retry
					//with pure exponential, 80 retries would overflow Duration's capacity
					.backoff(80, INIT)
					.maxBackoff(EXPLICIT_MAX)
					.jitter(0d);

			return Flux.error(new IllegalStateException("boom"))
			    .retryWhen(retryBuilder);
		})
		            .expectSubscription()
		            .thenAwait(Duration.ofNanos(Long.MAX_VALUE))
		            .expectErrorSatisfies(e -> assertThat(e).hasMessage("Retries exhausted: 80/80")
		                                                    .isInstanceOf(IllegalStateException.class)
		                                                    .hasCause(new IllegalStateException("boom"))
		            )
		            .verify();
	}

	@Test
	public void fluxRetryBackoffWithSpecificScheduler() {
		VirtualTimeScheduler backoffScheduler = VirtualTimeScheduler.create();

		Exception exception = new IOException("boom retry");

		StepVerifier.create(
				Flux.concat(Flux.range(0, 2), Flux.error(exception))
				    .retryWhen(Retry
						    .backoff(4, Duration.ofHours(1))
						    .maxBackoff(Duration.ofHours(1))
						    .jitter(0)
						    .scheduler(backoffScheduler)
				    )
		)
		            .expectNext(0, 1) //normal output
		            .expectNoEvent(Duration.ofMillis(100))
		            .then(() -> backoffScheduler.advanceTimeBy(Duration.ofHours(4)))
		            .expectNext(0, 1, 0, 1, 0, 1, 0, 1) //4 retry attempts
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("Retries exhausted: 4/4")
		                                                    .hasCause(exception))
		            .verify(Duration.ofMillis(100)); //test should only take roughly the expectNoEvent time
	}

	@Test
	public void fluxRetryBackoffRetriesOnGivenScheduler() {
		//the fluxRetryBackoffWithSpecificScheduler above is not suitable to verify the retry scheduler, as VTS is akin to immediate()
		//and doesn't really change the Thread
		Scheduler backoffScheduler = Schedulers.newSingle("backoffScheduler");
		String main = Thread.currentThread().getName();
		final IllegalStateException exception = new IllegalStateException("boom");
		List<String> threadNames = new ArrayList<>(4);
		try {
			StepVerifier.create(Flux.concat(Flux.range(0, 2), Flux.error(exception))
			                        .doOnError(e -> threadNames.add(Thread.currentThread().getName().replaceAll("-\\d+", "")))
			                        .retryWhen(Retry
					                        .backoff(2, Duration.ofMillis(10))
					                        .maxBackoff(Duration.ofMillis(100))
					                        .jitter(0.5d)
					                        .scheduler(backoffScheduler)
			                        )
			)
			            .expectNext(0, 1, 0, 1, 0, 1)
			            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
			                                                    .hasMessage("Retries exhausted: 2/2")
			                                                    .hasCause(exception))
			            .verify(Duration.ofMillis(200));

			assertThat(threadNames)
					.as("retry runs on backoffScheduler")
					.containsExactly(main, "backoffScheduler", "backoffScheduler");
		}
		finally {
			backoffScheduler.dispose();
		}
	}

	@Test
	public void backoffFunctionNotTransient() {
		Flux<Integer> source = transientErrorSource();

		Retry retryFunction =
				Retry.backoff(2, Duration.ZERO)
				     .maxBackoff(Duration.ofMillis(100))
				     .jitter(0d)
				     .transientErrors(false);

		new FluxRetryWhen<>(source, retryFunction)
				.as(StepVerifier::create)
				.expectNext(3, 4)
				.expectErrorMessage("Retries exhausted: 2/2")
				.verify(Duration.ofSeconds(2));
	}

	@Test
	public void backoffFunctionTransient() {
		Flux<Integer> source = transientErrorSource();

		Retry retryFunction =
				Retry.backoff(2, Duration.ZERO)
				     .maxBackoff(Duration.ofMillis(100))
				     .jitter(0d)
				     .transientErrors(true);

		new FluxRetryWhen<>(source, retryFunction)
				.as(StepVerifier::create)
				.expectNext(3, 4, 7, 8, 11, 12)
				.expectComplete()
				.verify(Duration.ofSeconds(2));
	}

	@Test
	public void simpleFunctionTransient() {
		Flux<Integer> source = transientErrorSource();

		Retry retryFunction =
				Retry.max(2)
				     .transientErrors(true);

		new FluxRetryWhen<>(source, retryFunction)
				.as(StepVerifier::create)
				.expectNext(3, 4, 7, 8, 11, 12)
				.expectComplete()
				.verify(Duration.ofSeconds(2));
	}

	@Test
	public void gh1978() {
		final int elementPerCycle = 3;
		final int stopAfterCycles = 10;
		Flux<Long> source =
				Flux.generate(() -> new AtomicLong(0), (counter, s) -> {
					long currentCount = counter.incrementAndGet();
					if (currentCount % elementPerCycle == 0) {
						s.error(new RuntimeException("Error!"));
					}
					else {
						s.next(currentCount);
					}
					return counter;
				});

		List<Long> pauses = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				source.retryWhen(Retry
						.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
						.maxBackoff(Duration.ofMinutes(1))
						.jitter(0d)
						.transientErrors(true)
				)
				      .take(stopAfterCycles * elementPerCycle, false)
				      .elapsed()
				      .map(Tuple2::getT1)
				      .doOnNext(pause -> { if (pause > 500) pauses.add(pause / 1000); })
		)
		            .thenAwait(Duration.ofHours(1))
		            .expectNextCount(stopAfterCycles * elementPerCycle)
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));

		assertThat(pauses).allMatch(p -> p == 1, "pause is constantly 1s");
	}

	public static Flux<Integer> transientErrorSource() {
		AtomicInteger count = new AtomicInteger();
		return Flux.generate(sink -> {
			int step = count.incrementAndGet();
			switch (step) {
				case 1:
				case 2:
				case 5:
				case 6:
				case 9:
				case 10:
					sink.error(new IllegalStateException("failing on step " + step));
					break;
				case 3: //should reset
				case 4: //should NOT reset
				case 7: //should reset
				case 8: //should NOT reset
				case 11: //should reset
					sink.next(step);
					break;
				case 12:
					sink.next(step); //should NOT reset
					sink.complete();
					break;
				default:
					sink.complete();
					break;
			}
		});
	}

	@Test
	public void retryWhenThrowableCompanionIsComparableToRetryWhenRetryFromFunction() {
		AtomicInteger sourceHelper = new AtomicInteger();
		Flux<Integer> source = Flux.create(sink -> {
			if (sourceHelper.getAndIncrement() == 3) {
				sink.next(1).next(2).next(3).complete();
			}
			else {
				sink.error(new IllegalStateException("boom"));
			}
		});

		StepVerifier.withVirtualTime(() -> source.retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(3))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(3 * 3))
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		sourceHelper.set(0);
		StepVerifier.withVirtualTime(() -> source.retryWhen(
				Retry.from(companion -> companion.delayElements(Duration.ofSeconds(3))))
		)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(3 * 3))
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	public void retryWhenWithThrowableFunction() {
		AtomicInteger sourceHelper = new AtomicInteger();
		Flux<Integer> source = Flux.create(sink -> {
			if (sourceHelper.getAndIncrement() == 3) {
				sink.next(1).next(2).next(3).complete();
			}
			else {
				sink.error(new IllegalStateException("boom"));
			}
		});

		Function<Flux<Throwable>, Flux<Long>> throwableBased = throwableFlux -> throwableFlux.index().map(Tuple2::getT1);

		StepVerifier.create(source.retryWhen(Retry.withThrowable(throwableBased)))
		            .expectSubscription()
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	void gh2488() {
		for (int i = 0; i < 1_000; i++) {
			AtomicInteger sourceHelper = new AtomicInteger();
			Flux.just("hello")
			    .doOnNext(m -> {
				    if (sourceHelper.getAndIncrement() < 9) {
					    throw new RuntimeException("Boom!");
				    }
			    })
			    .retryWhen(Retry.fixedDelay(10, Duration.ofNanos(1)))
			    .blockFirst(Duration.ofSeconds(1));
		}
	}

}
