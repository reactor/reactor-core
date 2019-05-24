/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.LongAssert;
import org.assertj.core.data.Percentage;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxRetryWhenTest {

	Flux<Integer> justError = Flux.concat(Flux.just(1),
			Flux.error(new RuntimeException("forced failure 0")));

	Flux<Integer> rangeError = Flux.concat(Flux.range(1, 2),
			Flux.error(new RuntimeException("forced failure 0")));

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxRetryWhen<>(null, v -> v);
	}

	@Test(expected = NullPointerException.class)
	public void whenFactoryNull() {
		Flux.never()
		    .retryWhen(null);
	}

	@Test
	public void cancelsOther() {
		AtomicBoolean cancelled = new AtomicBoolean();
		Flux<Integer> when = Flux.range(1, 10)
		                         .doOnCancel(() -> cancelled.set(true));

		StepVerifier.create(justError.retryWhen(other -> when))
		            .thenCancel()
		            .verify();

		assertThat(cancelled.get()).isTrue();
	}

	@Test
	public void cancelTwiceCancelsOtherOnce() {
		AtomicInteger cancelled = new AtomicInteger();
		Flux<Integer> when = Flux.range(1, 10)
		                         .doOnCancel(cancelled::incrementAndGet);

		justError.retryWhen(other -> when)
		         .subscribe(new BaseSubscriber<Integer>() {
			         @Override
			         protected void hookOnSubscribe(Subscription subscription) {
				         subscription.request(1);
				         subscription.cancel();
				         subscription.cancel();
			         }
		         });

		assertThat(cancelled.get()).isEqualTo(1);
	}

	@Test
	public void directOtherErrorPreventsSubscribe() {
		AtomicBoolean sourceSubscribed = new AtomicBoolean();
		AtomicBoolean sourceCancelled = new AtomicBoolean();
		Flux<Integer> source = justError
		                           .doOnSubscribe(sub -> sourceSubscribed.set(true))
		                           .doOnCancel(() -> sourceCancelled.set(true));

		Flux<Integer> retry = source.retryWhen(other -> Mono.error(new IllegalStateException("boom")));

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


		Flux<Integer> retry = source.retryWhen(other -> other.flatMap(l ->
				count.getAndIncrement() == 0 ? Mono.just(l) : Mono.<Long>error(new IllegalStateException("boom"))));

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

		Flux<Integer> retry = source.retryWhen(other -> Flux.empty());

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

		Flux<Integer> retry = source.retryWhen(other -> other.take(1));

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

		justError.retryWhen(v -> Flux.range(1, 10))
		         .subscribe(ts);

		ts.assertValues(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void coldRepeaterBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		rangeError.retryWhen(v -> Flux.range(1, 5))
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

		rangeError.retryWhen(v -> Flux.empty())
		          .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void coldError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		rangeError.retryWhen(v -> Flux.error(new RuntimeException("forced failure")))
		          .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void whenFactoryThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		rangeError.retryWhen(v -> {
			throw new RuntimeException("forced failure");
		})
		          .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

	}

	@Test
	public void whenFactoryReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		rangeError.retryWhen(v -> null)
		          .subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();

	}

	@Test
	public void retryErrorsInResponse() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		rangeError.retryWhen(v -> v.map(a -> {
			throw new RuntimeException("forced failure");
		}))
		          .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

	}

	@Test
	public void retryAlways() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		rangeError.retryWhen(v -> v)
		          .subscribe(ts);

		ts.request(8);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();
	}

	Flux<String> exponentialRetryScenario() {
		AtomicInteger i = new AtomicInteger();
		return Flux.<String>create(s -> {
			if (i.incrementAndGet() == 4) {
				s.next("hey");
			}
			else {
				s.error(new RuntimeException("test " + i));
			}
		}).retryWhen(repeat -> repeat.zipWith(Flux.range(1, 3), (t1, t2) -> t2)
		                             .flatMap(time -> Mono.delay(Duration.ofSeconds(time))));
	}

	@Test
	public void exponentialRetry() {
		StepVerifier.withVirtualTime(this::exponentialRetryScenario)
		            .thenAwait(Duration.ofSeconds(6))
		            .expectNext("hey")
		            .expectComplete()
		            .verify();
	}

	@Test
    public void scanMainSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxRetryWhen.RetryWhenMainSubscriber<Integer> test =
        		new FluxRetryWhen.RetryWhenMainSubscriber<>(actual, null, Flux.empty());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
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
        		new FluxRetryWhen.RetryWhenMainSubscriber<>(actual, null, Flux.empty());
        FluxRetryWhen.RetryWhenOtherSubscriber test = new FluxRetryWhen.RetryWhenOtherSubscriber();
        test.main = main;

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main.otherArbiter);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
    }


	@Test
	public void inners() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		CoreSubscriber<Throwable> signaller = new LambdaSubscriber<>(null, e -> {}, null, null);
		Flux<Integer> when = Flux.empty();
		FluxRetryWhen.RetryWhenMainSubscriber<Integer> main = new FluxRetryWhen
				.RetryWhenMainSubscriber<>(actual, signaller, when);

		List<Scannable> inners = main.inners().collect(Collectors.toList());

		assertThat(inners).containsExactly((Scannable) signaller, main.otherArbiter);
	}

	@Test
	public void retryWhenContextTrigger_ReplacesOriginalContext() {
		final int RETRY_COUNT = 3;
		List<Integer> retriesLeft = Collections.synchronizedList(new ArrayList<>(4));
		List<Context> contextPerRetry = Collections.synchronizedList(new ArrayList<>(4));

		Flux<Object> retryWithContext =
				Flux.error(new IllegalStateException("boom"))
				    .doOnEach(sig -> {
					    retriesLeft.add(sig.getContext().get("retriesLeft"));
					    if (!sig.isOnNext()) {
						    contextPerRetry.add(sig.getContext());
					    }
				    })
				    .retryWhen(errorFlux -> errorFlux.flatMap(e -> Mono.subscriberContext().map(ctx -> Tuples.of(e, ctx)))
				                                     .flatMap(t2 -> {
					                                     Throwable e = t2.getT1();
					                                     Context ctx = t2.getT2();
					                                     int rl = ctx.getOrDefault("retriesLeft", 0);
					                                     if (rl > 0) {
						                                     return Mono.just(Context.of("retriesLeft", rl - 1));
					                                     } else {
						                                     return Mono.<Context>error(new IllegalStateException("retries exhausted", e));
					                                     }
				                                     })
				    )
				    .subscriberContext(Context.of("retriesLeft", RETRY_COUNT))
					.subscriberContext(Context.of("thirdPartyContext", "present"));

		StepVerifier.create(retryWithContext)
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("retries exhausted")
		                                                    .hasCause(new IllegalStateException("boom")))
		            .verify(Duration.ofSeconds(1));

		assertThat(retriesLeft).containsExactly(3, 2, 1, 0);
		assertThat(contextPerRetry)
				.first()
				.matches(ctx  -> ctx.hasKey("thirdPartyContext"));
		assertThat(contextPerRetry.subList(1, contextPerRetry.size() - 1))
				.noneMatch(ctx  -> ctx.hasKey("thirdPartyContext"));
	}

	@Test
	public void retryWhenContextTrigger_OriginalContextManuallyUpdated() {
		final int RETRY_COUNT = 3;
		List<Integer> retriesLeft = Collections.synchronizedList(new ArrayList<>(4));
		List<Context> contextPerRetry = Collections.synchronizedList(new ArrayList<>(4));

		Flux<Object> retryWithContext =
				Flux.error(new IllegalStateException("boom"))
				    .doOnEach(sig -> {
					    retriesLeft.add(sig.getContext().get("retriesLeft"));
					    if (!sig.isOnNext()) {
						    contextPerRetry.add(sig.getContext());
					    }
				    })
				    .retryWhen(errorFlux -> errorFlux.flatMap(e -> Mono.subscriberContext().map(ctx -> Tuples.of(e, ctx)))
				                                     .flatMap(t2 -> {
					                                     Throwable e = t2.getT1();
					                                     Context ctx = t2.getT2();
					                                     int rl = ctx.getOrDefault("retriesLeft", 0);
					                                     if (rl > 0) {
						                                     return Mono.just(ctx.put("retriesLeft", rl - 1));
					                                     } else {
						                                     return Mono.<Context>error(new IllegalStateException("retries exhausted", e));
					                                     }
				                                     })
				    )
				    .subscriberContext(Context.of("retriesLeft", RETRY_COUNT))
				    .subscriberContext(Context.of("thirdPartyContext", "present"));

		StepVerifier.create(retryWithContext)
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("retries exhausted")
		                                                    .hasCause(new IllegalStateException("boom")))
		            .verify(Duration.ofSeconds(1));

		assertThat(retriesLeft).containsExactly(3, 2, 1, 0);
		assertThat(contextPerRetry)
				.allMatch(ctx  -> ctx.hasKey("thirdPartyContext"));
	}

	@Test
	public void fluxRetryRandomBackoff() {
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Flux.concat(Flux.range(0, 2), Flux.error(exception))
				    .retryBackoff(4, Duration.ofMillis(100), Duration.ofMillis(2000), 0.1)
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
				    .retryBackoff(4, Duration.ofMillis(100), Duration.ofMillis(2000))
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
				    .log()
				    .retryBackoff(4, Duration.ofMillis(100))
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
	public void fluxRetryRandomBackoff_maxBackoffShaves() {
		Exception exception = new IOException("boom retry");
		List<Long> elapsedList = new ArrayList<>();

		StepVerifier.withVirtualTime(() ->
				Flux.concat(Flux.range(0, 2), Flux.error(exception))
				    .retryBackoff(4, Duration.ofMillis(100), Duration.ofMillis(220), 0.9)
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
					    .retryBackoff(1, Duration.ofMillis(100), Duration.ofMillis(2000), 0.9)
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
				    .retryBackoff(4, Duration.ofMillis(100), Duration.ofMillis(2000), 0)
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
			Function<Flux<Throwable>, Publisher<Long>> backoffFunction = FluxRetryWhen.randomExponentialBackoffFunction(
					80, //with pure exponential, this amount of retries would overflow Duration's capacity
					INIT,
					EXPLICIT_MAX,
					0d,
					Schedulers.parallel());

			return Flux.error(new IllegalStateException("boom"))
			    .retryWhen(backoffFunction);
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
	public void fluxRetryBackoffWithGivenScheduler() {
		VirtualTimeScheduler backoffScheduler = VirtualTimeScheduler.create();

		Exception exception = new IOException("boom retry");

		StepVerifier.create(
				Flux.concat(Flux.range(0, 2), Flux.error(exception))
				    .log()
				    .retryBackoff(4, Duration.ofHours(1), Duration.ofHours(1), 0, backoffScheduler)
				.doOnNext(i -> System.out.println(i + " on " + Thread.currentThread().getName()))
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
		//the fluxRetryBackoffWithGivenScheduler above is not suitable to verify the retry scheduler, as VTS is akin to immediate()
		//and doesn't really change the Thread
		Scheduler backoffScheduler = Schedulers.newSingle("backoffScheduler");
		String main = Thread.currentThread().getName();
		final IllegalStateException exception = new IllegalStateException("boom");
		List<String> threadNames = new ArrayList<>(4);
		try {
			StepVerifier.create(Flux.concat(Flux.range(0, 2), Flux.error(exception))
			                        .doOnError(e -> threadNames.add(Thread.currentThread().getName().replaceAll("-\\d+", "")))
			                        .retryBackoff(2, Duration.ofMillis(10), Duration.ofMillis(100), 0.5d, backoffScheduler)

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
}
