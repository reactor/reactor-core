/*
 * Copyright (c) 2018-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.Scannable.Attr;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxSwitchOnFirstTest {

    // see https://github.com/reactor/reactor-core/pull/2765
    @Test
    void switchOnFirstWithInnerFlatMapErroring() {
        Flux<Integer> switchOnFirstDoubleFlatMap = Flux
            .just(1, 2, 3)
            .switchOnFirst((signal, innerFlux) -> {
                    int minimum = signal.get();
                    return signal.hasValue()
                        ? innerFlux
                        .flatMap(input -> input < minimum
                            ? Mono.error(new IllegalArgumentException("too small!"))
                            : Mono.just(minimum))
                        .flatMap(value -> Mono.error(new IllegalArgumentException("this fails!")))
                        : innerFlux;
                }
            );

        StepVerifier.create(switchOnFirstDoubleFlatMap)
            .expectErrorSatisfies(e -> assertThat(e)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("this fails!"))
            .verifyThenAssertThat()
            .hasNotDroppedErrors();
    }

    @Test
    public void shouldNotSubscribeTwice() {
        Throwable[] throwables = new Throwable[1];
        CountDownLatch latch = new CountDownLatch(1);
        StepVerifier.create(Flux.just(1L)
                    .switchOnFirst((s, f) -> {
                        RaceTestUtils.race(
                            () -> f.subscribe(__ -> {}, t -> {
                                throwables[0] = t;
                                latch.countDown();
                            },  latch::countDown),
                            () -> f.subscribe(__ -> {}, t -> {
                                throwables[0] = t;
                                latch.countDown();
                            },  latch::countDown)
                        );

                        return Flux.empty();
                    }, false))
                    .expectSubscription()
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));


        assertThat(throwables[0])
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessage("FluxSwitchOnFirst allows only one Subscriber");
    }

    @Test
    public void shouldNotSubscribeTwiceConditional() {
        Throwable[] throwables = new Throwable[1];
        CountDownLatch latch = new CountDownLatch(1);
        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    RaceTestUtils.race(
                                            () -> f.subscribe(__ -> {}, t -> {
                                                throwables[0] = t;
                                                latch.countDown();
                                            },  latch::countDown),
                                            () -> f.subscribe(__ -> {}, t -> {
                                                throwables[0] = t;
                                                latch.countDown();
                                            },  latch::countDown)
                                    );

                                    return Flux.empty();
                                }, false)
                                .filter(e -> true)
                    )
                    .expectSubscription()
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));


        assertThat(throwables[0])
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessage("FluxSwitchOnFirst allows only one Subscriber");
    }

    @Test
    public void shouldNotSubscribeTwiceWhenCanceled() {
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch nextLatch = new CountDownLatch(1);
        StepVerifier.create(Flux.just(1L)
                                .doOnComplete(() -> {
                                    try {
                                        if (!latch.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("latch didn't complete in 5s");
                                    }
                                    catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                                .hide()
                                .publishOn(Schedulers.parallel())
                                .cancelOn(NoOpsScheduler.INSTANCE)
                                .doOnCancel(latch::countDown)
                                .switchOnFirst((s, f) -> f)
                                .doOnSubscribe(s ->
                                    Schedulers.boundedElastic()
                                            .schedule(() -> {
                                                try {
                                                    nextLatch.await();
                                                } catch (InterruptedException e) {
                                                    e.printStackTrace();
                                                }
                                                s.cancel();
                                            })
                                )
                                .doOnNext(t -> nextLatch.countDown())
        )
                    .expectSubscription()
                    .expectNext(1L)
                    .expectNoEvent(Duration.ofMillis(200))
                    .thenCancel()
                    .verifyThenAssertThat(Duration.ofSeconds(20))
                    .hasNotDroppedErrors();
    }

    @Test
    public void shouldNotSubscribeTwiceConditionalWhenCanceled() {
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch nextLatch = new CountDownLatch(1);
        StepVerifier.create(Flux.just(1L)
                                .doOnComplete(() -> {
                                    try {
                                        if (!latch.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("latch didn't complete in 5s");
                                    }
                                    catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                                .hide()
                                .publishOn(Schedulers.parallel())
                                .cancelOn(NoOpsScheduler.INSTANCE)
                                .doOnCancel(latch::countDown)
                                .switchOnFirst((s, f) -> f)
                                .filter(e -> true)
                                .doOnSubscribe(s ->
                                    Schedulers.boundedElastic()
                                        .schedule(() -> {
                                            try {
                                                nextLatch.await();
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }
                                            s.cancel();
                                        })
                                )
                                .doOnNext(t -> nextLatch.countDown())
        )
                    .expectSubscription()
                    .expectNext(1L)
                    .expectNoEvent(Duration.ofMillis(200))
                    .thenCancel()
                    .verifyThenAssertThat(Duration.ofSeconds(5))
                    .hasNotDroppedErrors();
    }

    @Test
    public void shouldSendOnErrorSignalConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        RuntimeException error = new RuntimeException();
        StepVerifier.create(Flux.<Long>error(error)
                .switchOnFirst((s, f) -> {
                    first[0] = s;

                    return f;
                })
                .filter(e -> true)
        )
                    .expectSubscription()
                    .expectError(RuntimeException.class)
                    .verify(Duration.ofSeconds(5));


        assertThat(first).containsExactly(Signal.error(error));
    }

    @Test
    public void shouldSendOnNextSignalConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;

                                    return f;
                                })
                                .filter(e -> true)
                    )
                    .expectSubscription()
                    .expectNext(1L)
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));


        assertThat((long) first[0].get()).isEqualTo(1L);
    }

    @Test
    public void shouldSendOnErrorSignalWithDelaySubscription() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        RuntimeException error = new RuntimeException();
        StepVerifier.create(Flux.<Long>error(error)
                .switchOnFirst((s, f) -> {
                    first[0] = s;

                    return f.delaySubscription(Duration.ofMillis(100));
                }))
                    .expectSubscription()
                    .expectError(RuntimeException.class)
                    .verify(Duration.ofSeconds(5));


        assertThat(first).containsExactly(Signal.error(error));
    }

    @Test
    public void shouldSendOnCompleteSignalWithDelaySubscription() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.<Long>empty()
                .switchOnFirst((s, f) -> {
                    first[0] = s;

                    return f.delaySubscription(Duration.ofMillis(100));
                }))
                    .expectSubscription()
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));


        assertThat(first).containsExactly(Signal.complete());
    }

    @Test
    public void shouldSendOnErrorSignal() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        RuntimeException error = new RuntimeException();
        StepVerifier.create(Flux.<Long>error(error)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;

                                    return f;
                                }))
                    .expectSubscription()
                    .expectError(RuntimeException.class)
                    .verify(Duration.ofSeconds(5));


        assertThat(first).containsExactly(Signal.error(error));
    }

    @Test
    public void shouldSendOnNextSignal() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;

                                    return f;
                                }))
                    .expectSubscription()
                    .expectNext(1L)
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));


        assertThat((long) first[0].get()).isEqualTo(1L);
    }


    @Test
    public void shouldSendOnNextAsyncSignal() {
        for (int i = 0; i < 10000; i++) {
            @SuppressWarnings("unchecked") Signal<? extends Long>[] first = new Signal[1];

            StepVerifier.create(Flux.just(1L)
                                    .switchOnFirst((s, f) -> {
                                        first[0] = s;

                                        return f.subscribeOn(Schedulers.boundedElastic());
                                    }))
                        .expectSubscription()
                        .expectNext(1L)
                        .expectComplete()
                        .verify(Duration.ofSeconds(5));

            assertThat((long) first[0].get())
                      .isEqualTo(1L);
        }
    }

    @Test
    public void shouldSendOnNextAsyncSignalConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;

                                    return f.subscribeOn(Schedulers.boundedElastic());
                                })
                                .filter(p -> true)
                    )
                    .expectSubscription()
                    .expectNext(1L)
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));


        assertThat((long) first[0].get()).isEqualTo(1L);
    }

    @Test
    public void shouldNeverSendIncorrectRequestSizeToUpstream() throws InterruptedException {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        AtomicLong capture = new AtomicLong();
        ArrayList<Long> requested = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Flux<Long> switchTransformed = publisher.flux()
                                                .doOnRequest(requested::add)
                                                .switchOnFirst((first, innerFlux) -> innerFlux);

        publisher.next(1L);
        publisher.complete();

        switchTransformed.subscribeWith(new LambdaSubscriber<>(capture::set, __ -> {}, latch::countDown, s -> s.request(1)));

        latch.await(5, TimeUnit.SECONDS);

        assertThat(capture).hasValue(1L);
        assertThat(requested).containsExactly(1L);
    }

    @Test
    public void shouldNeverSendIncorrectRequestSizeToUpstreamConditional() throws InterruptedException {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        AtomicLong capture = new AtomicLong();
        ArrayList<Long> requested = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Flux<Long> switchTransformed = publisher.flux()
                                                .doOnRequest(requested::add)
                                                .switchOnFirst((first, innerFlux) -> innerFlux)
                                                .filter(e -> true);

        publisher.next(1L);
        publisher.complete();

        switchTransformed.subscribeWith(new LambdaSubscriber<>(capture::set, __ -> {}, latch::countDown, s -> s.request(1)));

        latch.await(5, TimeUnit.SECONDS);

        assertThat(capture).hasValue(1L);
        assertThat(requested).containsExactly(1L);
    }

    @Test
    public void shouldBeRequestedOneFromUpstreamTwiceInCaseOfConditional() throws InterruptedException {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        ArrayList<Long> capture = new ArrayList<>();
        ArrayList<Long> requested = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Flux<Long> switchTransformed = publisher.flux()
                                                .doOnRequest(requested::add)
                                                .switchOnFirst((first, innerFlux) -> innerFlux)
                                                .filter(e -> false);

        publisher.next(1L);
        publisher.complete();

        switchTransformed.subscribeWith(new LambdaSubscriber<>(capture::add, __ -> {}, latch::countDown, s -> s.request(1)));

        latch.await(5, TimeUnit.SECONDS);

        assertThat(capture).isEmpty();
        assertThat(requested).containsExactly(1L, 1L);
    }

    @Test
    public void shouldBeRequestedExactlyOneAndThenLongMaxValue() throws InterruptedException {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        ArrayList<Long> capture = new ArrayList<>();
        ArrayList<Long> requested = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Flux<Long> switchTransformed = publisher.flux()
                                                .doOnRequest(requested::add)
                                                .switchOnFirst((first, innerFlux) -> innerFlux);

        publisher.next(1L);
        publisher.complete();

        switchTransformed.subscribe(capture::add, __ -> {}, latch::countDown);

        latch.await(5, TimeUnit.SECONDS);

        assertThat(capture).containsExactly(1L);
        assertThat(requested).containsExactly(1L, Long.MAX_VALUE);
    }

    @Test
    public void shouldBeRequestedExactlyOneAndThenLongMaxValueConditional() throws InterruptedException {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        ArrayList<Long> capture = new ArrayList<>();
        ArrayList<Long> requested = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Flux<Long> switchTransformed = publisher.flux()
                                                .doOnRequest(requested::add)
                                                .switchOnFirst((first, innerFlux) -> innerFlux);

        publisher.next(1L);
        publisher.complete();

        switchTransformed.subscribe(capture::add, __ -> {}, latch::countDown);

        latch.await(5, TimeUnit.SECONDS);

        assertThat(capture).containsExactly(1L);
        assertThat(requested).containsExactly(1L, Long.MAX_VALUE);
    }

    @Test
    public void shouldReturnCorrectContextOnEmptySource() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        Flux<Long> switchTransformed = Flux.<Long>empty()
                .switchOnFirst((f, innerFlux) -> {
                    first[0] = f;
                    innerFlux.subscribe();

                    return Flux.<Long>empty();
                })
                .contextWrite(Context.of("a", "c"))
                .contextWrite(Context.of("c", "d"));

        StepVerifier.create(switchTransformed, 0)
                    .expectSubscription()
                    .thenRequest(1)
                    .expectAccessibleContext()
                    .contains("a", "c")
                    .contains("c", "d")
                    .then()
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));

        assertThat(first).containsExactly(Signal.complete(Context.of("a", "c").put("c", "d")));
    }

    @Test
    public void shouldReturnCorrectContextIfLoosingChain() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        Flux<Long> switchTransformed = Flux.<Long>empty()
                .switchOnFirst((f, innerFlux) -> {
                    first[0] = f;
                    return innerFlux;
                })
                .contextWrite(Context.of("a", "c"))
                .contextWrite(Context.of("c", "d"));

        StepVerifier.create(switchTransformed, 0)
                .expectSubscription()
                .thenRequest(1)
                .expectAccessibleContext()
                .contains("a", "c")
                .contains("c", "d")
                .then()
                .expectComplete()
                .verify(Duration.ofSeconds(5));

        assertThat(first).containsExactly(Signal.complete(Context.of("a", "c").put("c", "d")));
    }

    @Test
    public void shouldNotFailOnIncorrectPublisherBehavior() {
        TestPublisher<Long> publisher =
                TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
        Flux<Long> switchTransformed = publisher.flux()
                                                .switchOnFirst((first, innerFlux) -> innerFlux.contextWrite(Context.of("a", "b")));

        StepVerifier.create(new Flux<Long>() {
            @Override
            public void subscribe(CoreSubscriber<? super Long> actual) {
                switchTransformed.subscribe(actual);
                publisher.next(1L);
            }
        }, 0)
                    .thenRequest(1)
                    .expectNext(1L)
                    .thenRequest(1)
                    .then(() -> publisher.next(2L))
                    .expectNext(2L)
                    .then(() -> publisher.error(new RuntimeException()))
                    .then(() -> publisher.error(new RuntimeException()))
                    .then(() -> publisher.error(new RuntimeException()))
                    .then(() -> publisher.error(new RuntimeException()))
                    .expectError()
                    .verifyThenAssertThat(Duration.ofSeconds(5))
                    .hasDroppedErrors(3)
                    .tookLessThan(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    // Since context is immutable, with switchOnFirst it should not be mutable as well. Upstream should observe downstream
    // Inner should be able to access downstreamContext but should not modify upstream context after the first element
    public void shouldNotBeAbleToAccessUpstreamContext() {
        TestPublisher<Long> publisher = TestPublisher.createCold();

        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst(
                                                      (first, innerFlux) -> innerFlux.map(String::valueOf)
                                                                                     .contextWrite(Context.of("a", "b"))
                                                  )
                                                  .contextWrite(Context.of("a", "c"))
                                                  .contextWrite(Context.of("c", "d"));

        publisher.next(1L);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .thenRequest(1)
                    .then(() -> publisher.next(2L))
                    .expectNext("2")
                    .expectAccessibleContext()
                    .contains("a", "c")
                    .contains("c", "d")
                    .then()
                    .then(publisher::complete)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    public void shouldNotHangWhenOneElementUpstream() {
        TestPublisher<Long> publisher = TestPublisher.createCold();

        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) ->
                                                      innerFlux.map(String::valueOf)
                                                               .contextWrite(Context.of("a", "b"))
                                                  )
                                                  .contextWrite(Context.of("a", "c"))
                                                  .contextWrite(Context.of("c", "d"));

        publisher.next(1L);
        publisher.complete();

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    public void backpressureTest() {
        TestPublisher<Long> publisher = TestPublisher.createCold();
        AtomicLong requested = new AtomicLong();

        Flux<String> switchTransformed = publisher.flux()
                                                  .doOnRequest(requested::addAndGet)
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf));

        publisher.next(1L);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .thenRequest(1)
                    .then(() -> publisher.next(2L))
                    .expectNext("2")
                    .then(publisher::complete)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();

        assertThat(requested).hasValue(2L);
    }

    @Test
    public void backpressureConditionalTest() {
        Flux<Integer> publisher = Flux.range(0, 10000);
        AtomicLong requested = new AtomicLong();

        Flux<String> switchTransformed = publisher.doOnRequest(requested::addAndGet)
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf))
                                                  .filter(e -> false);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        assertThat(requested).hasValue(2L);

    }

    @Test
    public void backpressureHiddenConditionalTest() {
        Flux<Integer> publisher = Flux.range(0, 10000);
        AtomicLong requested = new AtomicLong();

        Flux<String> switchTransformed = publisher.doOnRequest(requested::addAndGet)
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf)
                                                                                                .hide())
                                                  .filter(e -> false);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        assertThat(requested).hasValue(10001L);
    }

    @Test
    public void backpressureDrawbackOnConditionalInTransformTest() {
        Flux<Integer> publisher = Flux.range(0, 10000);
        AtomicLong requested = new AtomicLong();

        Flux<String> switchTransformed = publisher.doOnRequest(requested::addAndGet)
                                                  .switchOnFirst((first, innerFlux) -> innerFlux
                                                          .map(String::valueOf)
                                                          .filter(e -> false));

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        assertThat(requested).hasValue(10001L);
    }

    @Test
    public void shouldErrorOnOverflowTest() {
        TestPublisher<Long> publisher = TestPublisher.createColdNonBuffering();

        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf));

        publisher.next(1L);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .then(() -> publisher.next(2L))
                    .expectErrorSatisfies(t -> assertThat(t)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("Can't deliver value due to lack of requests")
                    )
                    .verify(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    public void shouldPropagateonCompleteCorrectly() {
        Flux<String> switchTransformed = Flux.empty()
                                             .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf));

        StepVerifier.create(switchTransformed)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));
    }

    @Test
    public void shouldPropagateOnCompleteWithMergedElementsCorrectly() {
        Flux<String> switchTransformed = Flux.empty()
                                             .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf)
                                                                                           .mergeWith(Flux.just("1", "2", "3")));

        StepVerifier.create(switchTransformed)
                    .expectNext("1", "2", "3")
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));
    }

    @Test
    public void shouldPropagateErrorCorrectly() {
        Flux<String> switchTransformed = Flux.error(new RuntimeException("hello"))
                                             .transform(flux -> new FluxSwitchOnFirst<>(
                                                     flux,
                                                     (first, innerFlux) -> innerFlux.map(
                                                             String::valueOf), true));

        StepVerifier.create(switchTransformed)
                    .expectErrorMessage("hello")
                    .verify(Duration.ofSeconds(10));
    }

    @Test
    public void shouldBeAbleToBeCancelledProperly() throws InterruptedException {
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        TestPublisher<Integer> publisher = TestPublisher.createCold();
        Flux<String> switchTransformed = publisher.flux()
                .doOnCancel(latch2::countDown)
                .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf))
                .doOnCancel(() -> {
                    try {
                        if (!latch1.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("latch didn't complete in 5s");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                })
                .cancelOn(Schedulers.boundedElastic());

        publisher.next(1);

        StepVerifier stepVerifier = StepVerifier.create(switchTransformed, 0)
                .thenCancel()
                .verifyLater();

        latch1.countDown();
        stepVerifier.verify(Duration.ofSeconds(10));

        assertThat(latch2.await(1, TimeUnit.SECONDS)).isTrue();

        Instant endTime = Instant.now().plusSeconds(5);
        while (!publisher.wasCancelled()) {
            if (endTime.isBefore(Instant.now())) {
                break;
            }
        }
        publisher.assertCancelled();
        publisher.assertWasRequested();
    }

    @Test
    public void shouldBeAbleToBeCancelledProperly2() {
        TestPublisher<Integer> publisher = TestPublisher.createCold();
        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) ->
                                                      innerFlux
                                                          .map(String::valueOf)
                                                          .take(1, false)
                                                  );

        publisher.next(1);
        publisher.next(2);
        publisher.next(3);
        publisher.next(4);

        StepVerifier.create(switchTransformed, 1)
                    .expectNext("1")
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertCancelled();
        publisher.assertWasRequested();
    }

    @Test
    public void shouldBeAbleToBeCancelledProperly3() {
        TestPublisher<Integer> publisher = TestPublisher.createCold();
        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) ->
                                                          innerFlux
                                                                  .map(String::valueOf)
                                                  )
                                                  .take(1, false);

        publisher.next(1);
        publisher.next(2);
        publisher.next(3);
        publisher.next(4);

        StepVerifier.create(switchTransformed, 1)
                    .expectNext("1")
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

        publisher.assertCancelled();
        publisher.assertWasRequested();
    }

    @Test
    public void shouldBeAbleToCatchDiscardedElement() {
        TestPublisher<Integer> publisher = TestPublisher.create();
        Integer[] discarded = new Integer[1];
        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf))
                                                  .doOnDiscard(Integer.class, e -> discarded[0] = e);

        StepVerifier.create(switchTransformed, 0)
                    .expectSubscription()
                    .then(() -> publisher.next(1))
                    .thenCancel()
                    .verify(Duration.ofSeconds(10));

        publisher.assertCancelled();
        publisher.assertWasRequested();

        assertThat(discarded).containsExactly(1);
    }

    @Test
    public void shouldBeAbleToCatchDiscardedElementInCaseOfConditional() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        TestPublisher<Integer> publisher = TestPublisher.create();
        int[] discarded = new int[1];
        Flux<String> switchTransformed = publisher.flux()
                .doOnCancel(latch2::countDown)
                .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf))
                .filter(t -> true)
                .doOnDiscard(Integer.class, e -> discarded[0] = e)
                .doOnCancel(() -> {
                    try {
                        if (!latch.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("latch didn't complete in 5s");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                })
                .cancelOn(Schedulers.boundedElastic());

        StepVerifier stepVerifier = StepVerifier.create(switchTransformed, 0)
                .expectSubscription()
                .then(() -> publisher.next(1))
                .thenCancel()
                .verifyLater();

        latch.countDown();
        stepVerifier.verify(Duration.ofSeconds(1));
        assertThat(latch2.await(1, TimeUnit.SECONDS)).isTrue();

        Instant endTime = Instant.now().plusSeconds(5);
        while (discarded[0] == 0) {
            if (endTime.isBefore(Instant.now())) {
                break;
            }
        }

        publisher.assertCancelled();
        publisher.assertWasRequested();

        assertThat(discarded).containsExactly(1);
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnNextDuringSwitching() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        Optional<?> expectedCause = Optional.of(1L);

        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;
                                    throw new NullPointerException();
                                }))
                    .expectSubscription()
                    .expectError(NullPointerException.class)
                    .verifyThenAssertThat(Duration.ofSeconds(5))
                    .hasOperatorErrorsSatisfying(c ->
                            assertThat(c)
                                    .singleElement()
                                    .satisfies(t -> {
                                        assertThat(t.getT1()).containsInstanceOf(NullPointerException.class);
                                        assertThat(t.getT2()).isEqualTo(expectedCause);
                                    })
                    );


        assertThat((long) first[0].get()).isEqualTo(1L);
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnErrorDuringSwitching() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        NullPointerException npe = new NullPointerException("suppressed");
        RuntimeException error = new RuntimeException("main");
        StepVerifier.create(Flux.<Long>error(error)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;
                                    throw npe;
                                }))
                    .expectSubscription()
                    .consumeErrorWith((e) -> Assertions.assertThat(e)
                                                       .isExactlyInstanceOf(RuntimeException.class)
                                                       .hasMessage("main")
                                                       .hasSuppressedException(npe))
                    .verify();


        assertThat(first).containsExactly(Signal.error(error));
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnCompleteDuringSwitching() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.<Long>empty()
                            .switchOnFirst((s, f) -> {
                                first[0] = s;
                                throw new NullPointerException();
                            })
                    )
                    .expectSubscription()
                    .expectError(NullPointerException.class)
                    .verify();


        assertThat(first).containsExactly(Signal.complete());
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnNextDuringSwitchingConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Integer>[] first = new Signal[1];
        Optional<?> expectedCause = Optional.of(1);

        StepVerifier
            .create(
                Flux.range(1, 100)
                    .switchOnFirst((s, f) -> {
                        first[0] = s;
                        throw new NullPointerException();
                    })
                    .filter(__ -> true)
            )
            .expectSubscription()
            .expectError(NullPointerException.class)
            .verifyThenAssertThat()
            .hasOperatorErrorsSatisfying(c ->
                assertThat(c)
                        .singleElement()
                        .satisfies(t -> {
                            assertThat(t.getT1()).containsInstanceOf(NullPointerException.class);
                            assertThat(t.getT2()).isEqualTo(expectedCause);
                        })
            );


        assertThat((long) first[0].get()).isEqualTo(1L);
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnErrorDuringSwitchingConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        NullPointerException npe = new NullPointerException("suppressed");
        RuntimeException error = new RuntimeException("main");
        StepVerifier.create(Flux.<Long>error(error)
                                .switchOnFirst((s, f) -> {
	                                first[0] = s;
	                                throw npe;
                                })
                                .filter(__ -> true))
                    .expectSubscription()
                    .consumeErrorWith(e -> Assertions.assertThat(e)
                                                     .isExactlyInstanceOf(RuntimeException.class)
                                                     .hasMessage("main")
                                                     .hasSuppressedException(npe))
                    .verify();


        assertThat(first).containsExactly(Signal.error(error));
    }

    @Test
    public void shouldReturnNormallyIfExceptionIsThrownOnCompleteDuringSwitchingConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.<Long>empty()
                .switchOnFirst((s, f) -> {
                    first[0] = s;
                    throw new NullPointerException();
                }).filter(__ -> true)
        )
                .expectSubscription()
                .expectError(NullPointerException.class)
                .verify();


        assertThat(first).containsExactly(Signal.complete());
    }

    @Test
    public void sourceSubscribedOnce() {
        AtomicInteger subCount = new AtomicInteger();
        Flux<Integer> source = Flux.range(1, 10)
                                   .hide()
                                   .doOnSubscribe(subscription -> subCount.incrementAndGet());

        StepVerifier.create(source.switchOnFirst((s, f) -> f.filter(v -> v % 2 == s.get())))
                    .expectNext(1, 3, 5, 7, 9)
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));

        assertThat(subCount).hasValue(1);
    }

    @Test
    public void checkHotSource() {
        Sinks.Many<Long> processor = Sinks.many().replay().limit(1);

        processor.emitNext(1L, FAIL_FAST);
        processor.emitNext(2L, FAIL_FAST);
        processor.emitNext(3L, FAIL_FAST);


        StepVerifier.create(processor.asFlux().switchOnFirst((s, f) -> f.filter(v -> v % s.get() == 0)))
                    .expectNext(3L)
                    .then(() -> {
                        processor.emitNext(4L, FAIL_FAST);
                        processor.emitNext(5L, FAIL_FAST);
                        processor.emitNext(6L, FAIL_FAST);
                        processor.emitNext(7L, FAIL_FAST);
                        processor.emitNext(8L, FAIL_FAST);
                        processor.emitNext(9L, FAIL_FAST);
                        processor.emitComplete(FAIL_FAST);
                    })
                    .expectNext(6L, 9L)
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherComplete() {
        Sinks.Many<Long> testPublisher = Sinks.many().multicast().onBackpressureBuffer();

        testPublisher.emitNext(1L, FAIL_FAST);

        StepVerifier.create(testPublisher.asFlux().switchOnFirst((s, f) -> Flux.empty()))
                    .expectSubscription()
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));

        assertThat(Scannable.from(testPublisher).scan(Attr.CANCELLED)).isTrue();
    }

    @Test
    public void shouldNotCancelSourceOnUnrelatedPublisherComplete() {
        Sinks.Many<Long> testPublisher = Sinks.many().multicast().onBackpressureBuffer();

        testPublisher.emitNext(1L, FAIL_FAST);

        StepVerifier.create(testPublisher.asFlux().switchOnFirst((s, f) -> Flux.empty(), false))
                .expectSubscription()
                .expectComplete()
                .verify(Duration.ofSeconds(5));

        assertThat(Scannable.from(testPublisher).scan(Attr.CANCELLED)).isFalse();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherError() {
        Sinks.Many<Long> testPublisher = Sinks.many().multicast().onBackpressureBuffer();

        testPublisher.emitNext(1L, FAIL_FAST);

        StepVerifier.create(testPublisher.asFlux().switchOnFirst((s, f) -> Flux.error(new RuntimeException("test"))))
                    .expectSubscription()
                    .expectErrorSatisfies(t ->
                        assertThat(t)
                                  .hasMessage("test")
                                  .isExactlyInstanceOf(RuntimeException.class)
                    )
                    .verify(Duration.ofSeconds(5));

        assertThat(Scannable.from(testPublisher).scan(Attr.CANCELLED)).isTrue();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherCancel() {
        TestPublisher<Long> testPublisher = TestPublisher.create();

        StepVerifier.create(testPublisher.flux().switchOnFirst((s, f) -> Flux.error(new RuntimeException("test"))))
                    .expectSubscription()
                    .thenCancel()
                    .verify(Duration.ofSeconds(5));

        assertThat(testPublisher.wasCancelled()).isTrue();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherCompleteConditional() {
        Sinks.Many<Long> testPublisher = Sinks.many().multicast().onBackpressureBuffer();

        testPublisher.emitNext(1L, FAIL_FAST);

        StepVerifier.create(testPublisher.asFlux().switchOnFirst((s, f) -> Flux.empty().delaySubscription(Duration.ofMillis(10))).filter(__ -> true))
                    .then(() -> {
                        List<? extends Scannable> subs = Scannable.from(testPublisher).inners().collect(Collectors.toList());
                        assertThat(subs)
                                  .hasSize(1)
                                  .first()
                                  .extracting(psi -> psi.scan(Attr.ACTUAL))
                                  .isInstanceOf(Fuseable.ConditionalSubscriber.class);
                    })
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));

        assertThat(Scannable.from(testPublisher).scan(Attr.CANCELLED)).isTrue();
    }

    @Test
    public void shouldNotCancelSourceOnUnrelatedPublisherCompleteConditional() {
        Sinks.Many<Long> testPublisher = Sinks.many().multicast().onBackpressureBuffer();

        testPublisher.emitNext(1L, FAIL_FAST);

        StepVerifier.create(testPublisher.asFlux().switchOnFirst((s, f) -> Flux.empty().delaySubscription(Duration.ofMillis(10)), false).filter(__ -> true))
                .then(() -> {
                    List<? extends Scannable> subs = Scannable.from(testPublisher).inners().collect(Collectors.toList());
                    assertThat(subs)
                              .hasSize(1)
                              .first()
                              .extracting(psi -> psi.scan(Attr.ACTUAL))
                              .isInstanceOf(Fuseable.ConditionalSubscriber.class);
                })
                .expectComplete()
                .verify(Duration.ofSeconds(5));

        assertThat(Scannable.from(testPublisher).scan(Attr.CANCELLED)).isFalse();
    }

    @Test
    public void shouldCancelInnerSubscriptionImmediatelyUpOnReceivingIfDownstreamIsAlreadyCancelledConditional() {
        VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.getOrSet();
        TestPublisher<Long> testPublisher = TestPublisher.create();
        TestPublisher<Long> testPublisherInner = TestPublisher.create();

        try {
            StepVerifier
                .create(
                    testPublisher
                        .flux()
                        .switchOnFirst((s, f) ->
                            testPublisherInner
                                .flux()
                                .transform(Operators.lift((__, cs) -> new BaseSubscriber<Long>() {
                                    @Override
                                    protected void hookOnSubscribe(Subscription subscription) {
                                        Schedulers.parallel().schedule(() -> cs.onSubscribe(this), 1, TimeUnit.SECONDS);
                                    }
                                })),
                            false
                        )
                        .filter(__ -> true)
                )
                .expectSubscription()
                .then(() -> testPublisher.next(1L))
                .thenCancel()
                .verify(Duration.ofSeconds(5));

            assertThat(testPublisher.wasCancelled()).isTrue();
            assertThat(testPublisherInner.wasCancelled()).isFalse();
            virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(1000));
            assertThat(testPublisherInner.wasCancelled()).isTrue();
        } finally {
            VirtualTimeScheduler.reset();
        }
    }

    @Test
    public void shouldCancelInnerSubscriptionImmediatelyUpOnReceivingIfDownstreamIsAlreadyCancelled() {
        VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.getOrSet();
        TestPublisher<Long> testPublisher = TestPublisher.create();
        TestPublisher<Long> testPublisherInner = TestPublisher.create();

        try {
            StepVerifier
                    .create(
                            testPublisher
                                    .flux()
                                    .switchOnFirst((s, f) ->
                                                    testPublisherInner
                                                            .flux()
                                                            .transform(Operators.lift((__, cs) -> new BaseSubscriber<Long>() {
                                                                @Override
                                                                protected void hookOnSubscribe(Subscription subscription) {
                                                                    Schedulers.parallel().schedule(() -> cs.onSubscribe(this), 1, TimeUnit.SECONDS);
                                                                }
                                                            })),
                                            false
                                    )
                    )
                    .expectSubscription()
                    .then(() -> testPublisher.next(1L))
                    .thenCancel()
                    .verify(Duration.ofSeconds(5));

            assertThat(testPublisher.wasCancelled()).isTrue();
            assertThat(testPublisherInner.wasCancelled()).isFalse();
            virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(1000));
            assertThat(testPublisherInner.wasCancelled()).isTrue();
        } finally {
            VirtualTimeScheduler.reset();
        }
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherErrorConditional() {
        Sinks.Many<Long> testPublisher = Sinks.many().multicast().onBackpressureBuffer();

        testPublisher.emitNext(1L, FAIL_FAST);

        StepVerifier.create(testPublisher.asFlux().switchOnFirst((s, f) -> Flux.error(new RuntimeException("test")).delaySubscription(Duration.ofMillis(10))).filter(__ -> true))
                    .then(() -> {
                        List<? extends Scannable> subs = Scannable.from(testPublisher).inners().collect(Collectors.toList());
                        assertThat(subs)
                                  .hasSize(1)
                                  .first()
                                  .extracting(psi -> psi.scan(Attr.ACTUAL))
                                  .isInstanceOf(Fuseable.ConditionalSubscriber.class);
                    })
                    .expectErrorSatisfies(t ->
                            assertThat(t)
                                      .hasMessage("test")
                                      .isExactlyInstanceOf(RuntimeException.class)
                    )
                    .verify(Duration.ofSeconds(5));

        assertThat(Scannable.from(testPublisher).scan(Attr.CANCELLED)).isTrue();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherCancelConditional() {
        Sinks.Many<Long> testPublisher = Sinks.many().multicast().onBackpressureBuffer();

        testPublisher.emitNext(1L, FAIL_FAST);

        StepVerifier.create(testPublisher.asFlux().switchOnFirst((s, f) -> Flux.error(new RuntimeException("test")).delaySubscription(Duration.ofMillis(10))).filter(__ -> true))
                    .then(() -> {
                        List<? extends Scannable> subs = Scannable.from(testPublisher).inners().collect(Collectors.toList());
                        assertThat(subs)
                                  .hasSize(1)
                                  .first()
                                  .extracting(psi -> psi.scan(Attr.ACTUAL))
                                  .isInstanceOf(Fuseable.ConditionalSubscriber.class);
                    })
                    .thenAwait(Duration.ofMillis(50))
                    .thenCancel()
                    .verify();

        assertThat(Scannable.from(testPublisher).scan(Attr.CANCELLED)).isTrue();
    }

    @Test
    public void shouldCancelUpstreamBeforeFirst() {
        Sinks.Many<Long> testPublisher = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.create(testPublisher.asFlux().switchOnFirst((s, f) -> Flux.error(new RuntimeException("test"))))
                .thenAwait(Duration.ofMillis(50))
                .thenCancel()
                .verify(Duration.ofSeconds(2));

        assertThat(Scannable.from(testPublisher).scan(Attr.CANCELLED)).isTrue();
    }

    @Test
    public void shouldContinueWorkingRegardlessTerminalOnDownstream() {
        TestPublisher<Long> testPublisher = TestPublisher.create();

        @SuppressWarnings("unchecked")
        Flux<Long>[] intercepted = new Flux[1];

        StepVerifier.create(testPublisher.flux().switchOnFirst((s, f) -> {
            intercepted[0] = f;
            return Flux.just(2L);
        }, false))
                .expectSubscription()
                .then(() -> testPublisher.next(1L))
                .expectNext(2L)
                .expectComplete()
                .verify(Duration.ofSeconds(2));

        assertThat(testPublisher.wasCancelled()).isFalse();

        StepVerifier.create(intercepted[0])
                .expectSubscription()
                .expectNext(1L)
                .then(testPublisher::complete)
                .expectComplete()
                .verify(Duration.ofSeconds(1));
    }

    @Test
     public void shouldCancelSourceOnOnDownstreamTerminal() {
        TestPublisher<Long> testPublisher = TestPublisher.create();

        StepVerifier.create(testPublisher.flux().switchOnFirst((s, f) -> Flux.just(1L), true))
                .expectSubscription()
                .then(() -> testPublisher.next(1L))
                .expectNext(1L)
                .expectComplete()
                .verify(Duration.ofSeconds(2));

        assertThat(testPublisher.wasCancelled()).isTrue();
    }

	@Test
	void shouldErrorWhenResubscribingAfterCancel() {
		TestPublisher<Long> testPublisher = TestPublisher.create();

		AtomicReference<Flux<Long>> intercepted = new AtomicReference<>();

		Flux<Flux<Long>> flux =
				testPublisher.flux().switchOnFirst((s, f) -> {
					intercepted.set(f);
					// Due to wrapping we can avoid subscribing to f and subscribe later
					// (re-subscribing is not allowed)
					return Mono.just(f);
				});

		StepVerifier.create(flux)
		            .expectSubscription()
		            .then(() -> testPublisher.next(1L))
					.thenCancel()
		            .verify(Duration.ofSeconds(2));

		Flux<Long> switchOnFirstMain = intercepted.get();

		StepVerifier.create(switchOnFirstMain,
				            StepVerifierOptions.create()
				                               .scenarioName("Expect immediate onError from SwitchOnFirstMain as the outer operator has completed"))
		            .expectError(IllegalStateException.class)
		            .verify(Duration.ofSeconds(1));
	}

    @Test
    public void scanOperator(){
    	Flux<Integer> parent = Flux.just(1);
        FluxSwitchOnFirst<Integer, Integer> test = new FluxSwitchOnFirst<>(parent, (s, f) -> Flux.empty(), false);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

    @Test
    public void scanMain(){
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSwitchOnFirst.SwitchOnFirstMain<Integer, Integer> test =
                new FluxSwitchOnFirst.SwitchOnFirstMain<>(actual, (s, f) -> Flux.empty(), false);

        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    @Test
    public void scanMainConditional(){
        @SuppressWarnings("unchecked")
        Fuseable.ConditionalSubscriber<String> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
        FluxSwitchOnFirst.SwitchOnFirstConditionalMain<String, String> test =
                new FluxSwitchOnFirst.SwitchOnFirstConditionalMain<>(actual, (s, f) -> Flux.empty(), false);

        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    public void scanSubscriber(){
        @SuppressWarnings("unchecked")
        Fuseable.ConditionalSubscriber<String> delegate = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
        FluxSwitchOnFirst.SwitchOnFirstConditionalMain<String, String> parent =
                new FluxSwitchOnFirst.SwitchOnFirstConditionalMain<>(delegate, (s, f) -> Flux.empty(), false);
        FluxSwitchOnFirst.SwitchOnFirstControlSubscriber<String> test = new FluxSwitchOnFirst.SwitchOnFirstControlSubscriber<>(parent, delegate, false);

        Subscription sub = Operators.emptySubscription();
        test.onSubscribe(sub);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(delegate);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

    @Test
    public void scanConditionnalSubscriber(){
        @SuppressWarnings("unchecked")
        Fuseable.ConditionalSubscriber<String> delegate = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
        FluxSwitchOnFirst.SwitchOnFirstConditionalMain<String, String> main =
                new FluxSwitchOnFirst.SwitchOnFirstConditionalMain<>(delegate, (s, f) -> Flux.empty(), false);
        FluxSwitchOnFirst.SwitchOnFirstConditionalControlSubscriber<String> test = new FluxSwitchOnFirst.SwitchOnFirstConditionalControlSubscriber<>(main, delegate, false);

        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(delegate);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

    private static final class NoOpsScheduler implements Scheduler {

        static final NoOpsScheduler INSTANCE = new NoOpsScheduler();

        private NoOpsScheduler() {}

        @Override
        public Disposable schedule(Runnable task) {
            return Disposables.composite();
        }

        @Override
        public Worker createWorker() {
            return NoOpsWorker.INSTANCE;
        }

        static final class NoOpsWorker implements Worker {

            static final NoOpsWorker INSTANCE = new NoOpsWorker();

            @Override
            public Disposable schedule(Runnable task) {
                return Disposables.never();
            }

            @Override
            public void dispose() {

            }
        };
    }

    /**
     * A subscriber used solely for triggering the various side effects of hooks, if set.
     * In particular, does not {@link Subscription#request(long) request} any data by default.
     */
    private static final class SideEffectSubscriber<T> extends BaseSubscriber<T> {

        private final Consumer<T> onNext;
        private final Consumer<Throwable> onError;
        private final Runnable onComplete;
        private final Consumer<Subscription> onSubscribe;

        private SideEffectSubscriber(@Nullable Consumer<T> onNext, @Nullable Consumer<Throwable> onError, @Nullable Runnable onComplete, @Nullable Consumer<Subscription> onSubscribe) {
            this.onNext = onNext;
            this.onError = onError;
            this.onComplete = onComplete;
            this.onSubscribe = onSubscribe;
        }

        @Override
        protected void hookOnComplete() {
            if (onComplete != null) {
                onComplete.run();
            }
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            if (onSubscribe != null) {
                onSubscribe.accept(subscription);
            }
        }

        @Override
        protected void hookOnNext(T value) {
            if (onNext != null) {
                onNext.accept(value);
            }
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            if (onError != null) {
                onError.accept(throwable);
            }
        }
    }
}
