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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.assertj.core.api.Assumptions;
import org.mockito.ArgumentCaptor;
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
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxSwitchOnFirstTest {

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
                    }))
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
                                })
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
                                                          .take(1)
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
                                                  .take(1);

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
    public void shouldBeAbleToCancelSubscription() throws InterruptedException {
        Flux<Long> publisher = Flux.just(1L);
        ArrayList<Integer> capturedElementsNumber = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            final ArrayList<Throwable> dropped = new ArrayList<>();
            final AtomicLong requested = new AtomicLong();
            final CountDownLatch latch = new CountDownLatch(1);
            final AssertSubscriber<Long> assertSubscriber = new AssertSubscriber<>(Context.of(Hooks.KEY_ON_ERROR_DROPPED, (Consumer<Throwable>) dropped::add), 0);
            final Flux<Long> switchTransformed = publisher
                                                    .doOnRequest(requested::addAndGet)
                                                    .doOnCancel(latch::countDown)
                                                    .switchOnFirst((first, innerFlux) -> innerFlux.doOnComplete(latch::countDown));

            switchTransformed.subscribe(assertSubscriber);

            RaceTestUtils.race(assertSubscriber::cancel, () -> assertSubscriber.request(1));

            assertThat(latch.await(500, TimeUnit.SECONDS)).isTrue();

            capturedElementsNumber.add(assertSubscriber.values().size());
        }

        Assumptions.assumeThat(capturedElementsNumber).contains(0);
        Assumptions.assumeThat(capturedElementsNumber).contains(1);
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
                                  .hasOnlyOneElementSatisfying(t -> {
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

        NullPointerException npe = new NullPointerException();
        RuntimeException error = new RuntimeException();
        StepVerifier.create(Flux.<Long>error(error)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;
                                    throw npe;
                                }))
                    .expectSubscription()
                    .verifyError(NullPointerException.class);


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
                    .verifyThenAssertThat()
                    .hasOperatorErrorMatching(t -> {
                        assertThat(t)
                                  .isInstanceOf(NullPointerException.class);
                        return true;
                    });


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
                        .hasOnlyOneElementSatisfying(t -> {
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

        NullPointerException npe = new NullPointerException();
        RuntimeException error = new RuntimeException();
        StepVerifier.create(Flux.<Long>error(error)
                .switchOnFirst((s, f) -> {
                    first[0] = s;
                    throw npe;
                }).filter(__ -> true))
                .expectSubscription()
                .verifyError(NullPointerException.class);


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
                .verifyThenAssertThat()
                .hasOperatorErrorMatching(t -> {
                    assertThat(t)
                            .isInstanceOf(NullPointerException.class);
                    return true;
                });


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
    public void racingTest() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            @SuppressWarnings("unchecked")
            CoreSubscriber<? super Integer>[] subscribers = new CoreSubscriber[1];
            Subscription[] downstreamSubscriptions = new Subscription[1];
            Subscription[] innerSubscriptions = new Subscription[1];


            AtomicLong requested = new AtomicLong();

            Flux.range(0, 3)
                    .doOnRequest(requested::addAndGet)
                    .switchOnFirst((s, f) -> new Flux<Integer>() {

                        @Override
                        public void subscribe(CoreSubscriber<? super Integer> actual) {
                            subscribers[0] = actual;
                            f.subscribe(new SideEffectSubscriber<>(actual::onNext, actual::onError, actual::onComplete, (s) -> innerSubscriptions[0] = s));
                        }
                    })
                    .subscribe(new SideEffectSubscriber<>(null, null, null, s -> downstreamSubscriptions[0] = s));

            CoreSubscriber<? super Integer> subscriber = subscribers[0];
            Subscription downstreamSubscription = downstreamSubscriptions[0];
            Subscription innerSubscription = innerSubscriptions[0];
            downstreamSubscription.request(1);

            RaceTestUtils.race(() -> subscriber.onSubscribe(innerSubscription), () -> downstreamSubscription.request(1));

            assertThat(requested).hasValue(2);
        }
    }

    @Test
    public void racingConditionalTest() {
        for (int i = 0; i < 1000; i++) {
            @SuppressWarnings("unchecked")
            CoreSubscriber<? super Integer>[] subscribers = new CoreSubscriber[1];
            Subscription[] downstreamSubscriptions = new Subscription[1];
            Subscription[] innerSubscriptions = new Subscription[1];


            AtomicLong requested = new AtomicLong();

            Flux.range(0, 3)
                .doOnRequest(requested::addAndGet)
                .switchOnFirst((s, f) -> new Flux<Integer>() {

                    @Override
                    public void subscribe(CoreSubscriber<? super Integer> actual) {
                        subscribers[0] = actual;
                        f.subscribe(new Fuseable.ConditionalSubscriber<Integer>() {
                            @SuppressWarnings("unchecked")
                            @Override
                            public boolean tryOnNext(Integer integer) {
                                return ((Fuseable.ConditionalSubscriber<? super Integer>)actual).tryOnNext(integer);
                            }

                            @Override
                            public void onSubscribe(Subscription s) {
                                innerSubscriptions[0] = s;
                            }

                            @Override
                            public void onNext(Integer integer) {
                                actual.onNext(integer);
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                actual.onError(throwable);
                            }

                            @Override
                            public void onComplete() {
                                actual.onComplete();
                            }
                        });
                    }
                })
                .filter(__ -> true)
                .subscribeWith(new SideEffectSubscriber<>(null, null, null, s -> downstreamSubscriptions[0] = s));

            CoreSubscriber<? super Integer> subscriber = subscribers[0];
            Subscription downstreamSubscription = downstreamSubscriptions[0];
            Subscription innerSubscription = innerSubscriptions[0];
            downstreamSubscription.request(1);

            RaceTestUtils.race(() -> subscriber.onSubscribe(innerSubscription), () -> downstreamSubscription.request(1));

            assertThat(requested).hasValue(2);
        }
    }

    @Test
    public void racingInnerSubscribeAndOuterCancelTest() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            @SuppressWarnings("unchecked")
            CoreSubscriber<? super Integer>[] subscribers = new CoreSubscriber[1];
            @SuppressWarnings("unchecked")
            FluxSwitchOnFirst.SwitchOnFirstMain<Integer,Integer>[] sofSubscriber = new FluxSwitchOnFirst.SwitchOnFirstMain[1];
            @SuppressWarnings("unchecked")
            Flux<Integer>[] innerFlux = new Flux[1];


            AtomicLong requested = new AtomicLong();
            ArrayList<Throwable> dropped = new ArrayList<>();
            AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>(Context.of(Hooks.KEY_ON_ERROR_DROPPED, (Consumer<Throwable>) dropped::add), 0);

            Flux.range(0, 3)
                    .doOnRequest(requested::addAndGet)
                    .transform(Operators.<Integer, Integer>lift((__, cs) -> {
                        @SuppressWarnings("unchecked")
                        FluxSwitchOnFirst.SwitchOnFirstMain<Integer, Integer> sofCs = (FluxSwitchOnFirst.SwitchOnFirstMain<Integer,Integer>) cs;
                        sofSubscriber[0] = sofCs;
                        return cs;
                    }))
                    .switchOnFirst((s, f) -> new Flux<Integer>() {
                        @Override
                        public void subscribe(CoreSubscriber<? super Integer> actual) {
                            subscribers[0] = actual;
                            innerFlux[0] = f;
                        }
                    })
                    .subscribe(assertSubscriber);

            Flux<Integer> f = innerFlux[0];
            CoreSubscriber<? super Integer> subscriber = subscribers[0];
            assertSubscriber.request(1);

            RaceTestUtils.race(() -> f.subscribe(subscriber), () -> assertSubscriber.cancel());

            assertThat(sofSubscriber[0].inner).isEqualTo(Operators.EMPTY_SUBSCRIBER);

            // if cancel first then the upstream observes request(1) and request(1) if cancel later then only a single request
            assertThat(requested.get()).isBetween(1L, 2L);

            assertSubscriber.assertNoError();

            if (dropped.size() > 0) {
                assertThat(dropped)
                        .hasSize(1)
                        .first()
                        .isInstanceOf(CancellationException.class);
            }
            dropped.clear();
        }
    }

    @Test
    public void racingInnerSubscribeAndOuterCancelConditionalTest() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            @SuppressWarnings("unchecked")
            CoreSubscriber<? super Integer>[] subscribers = new CoreSubscriber[1];
            @SuppressWarnings("unchecked")
            FluxSwitchOnFirst.SwitchOnFirstConditionalMain<Integer, Integer>[] sofSubscriber = new FluxSwitchOnFirst.SwitchOnFirstConditionalMain[1];
            @SuppressWarnings("unchecked")
            Flux<Integer>[] innerFlux = new Flux[1];

            ArrayList<Throwable> dropped = new ArrayList<>();
            AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>(Context.of(Hooks.KEY_ON_ERROR_DROPPED, (Consumer<Throwable>) dropped::add), 0);

            AtomicLong requested = new AtomicLong();

            Flux.range(0, 3)
                    .doOnRequest(requested::addAndGet)
                    .transform(Operators.<Integer, Integer>lift((__, cs) -> {
                        @SuppressWarnings("unchecked")
                        FluxSwitchOnFirst.SwitchOnFirstConditionalMain<Integer, Integer> sofCs = (FluxSwitchOnFirst.SwitchOnFirstConditionalMain<Integer,Integer>) cs;
                        sofSubscriber[0] = sofCs;
                        return cs;
                    }))
                    .switchOnFirst((s, f) -> new Flux<Integer>() {
                        @Override
                        public void subscribe(CoreSubscriber<? super Integer> actual) {
                            subscribers[0] = actual;
                            innerFlux[0] = f;
                        }
                    })
                    .filter(__ -> true)
                    .subscribe(assertSubscriber);

            Flux<Integer> f = innerFlux[0];
            CoreSubscriber<? super Integer> subscriber = subscribers[0];
            assertSubscriber.request(1);

            RaceTestUtils.race(() -> f.subscribe(subscriber), () -> assertSubscriber.cancel());

            assertThat(sofSubscriber[0].inner).isEqualTo(Operators.EMPTY_SUBSCRIBER);

            // if cancel first then the upstream observes request(1) and request(1) if cancel later then only a single request
            assertThat(requested.get()).isBetween(1L, 2L);

            assertSubscriber.assertNoError();

            if (dropped.size() > 0) {
                assertThat(dropped)
                        .hasSize(1)
                        .first()
                        .isInstanceOf(CancellationException.class);
            }
            dropped.clear();
        }
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void unitRequestRacingTest() {
        @SuppressWarnings("unchecked")
        BiFunction<FluxSwitchOnFirst.AbstractSwitchOnFirstMain, CoreSubscriber, InnerOperator>[] factories = new BiFunction[] {
                (parent, assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstControlSubscriber((FluxSwitchOnFirst.AbstractSwitchOnFirstMain) parent, (CoreSubscriber) assertSubscriber, true),
                (parent, assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstConditionalControlSubscriber((FluxSwitchOnFirst.AbstractSwitchOnFirstMain) parent, (Fuseable.ConditionalSubscriber) assertSubscriber, true)
        };
        for (BiFunction<FluxSwitchOnFirst.AbstractSwitchOnFirstMain, CoreSubscriber, InnerOperator> factory : factories) {
            for (int i = 0; i < 10000; i++) {
                FluxSwitchOnFirst.AbstractSwitchOnFirstMain mockParent = Mockito.mock(FluxSwitchOnFirst.AbstractSwitchOnFirstMain.class);
                Mockito.doNothing().when(mockParent).request(Mockito.anyLong());
                Mockito.doNothing().when(mockParent).cancel();
                Subscription mockSubscription = Mockito.mock(Subscription.class);
                ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);
                Mockito.doNothing().when(mockSubscription).request(longArgumentCaptor.capture());
                Mockito.doNothing().when(mockSubscription).cancel();
                AssertSubscriber<Object> subscriber = AssertSubscriber.create(0);
                InnerOperator switchOnFirstControlSubscriber = factory.apply(mockParent, Operators.toConditionalSubscriber(subscriber));

                switchOnFirstControlSubscriber.request(10);
                RaceTestUtils.race(() -> switchOnFirstControlSubscriber.request(10), () -> switchOnFirstControlSubscriber.onSubscribe(mockSubscription), Schedulers.parallel());

                assertThat(longArgumentCaptor.getAllValues().size()).isBetween(1, 2);
                if (longArgumentCaptor.getAllValues().size() == 1) {
                    assertThat(longArgumentCaptor.getValue()).isEqualTo(20L);
                }
                else if (longArgumentCaptor.getAllValues().size() == 2) {
                    assertThat(longArgumentCaptor.getAllValues()).containsExactly(10L, 10L);
                }
                else {
                    fail("Unexpected number of calls");
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void unitRequestsAreSerialTest() {
        @SuppressWarnings("unchecked")
        BiFunction<FluxSwitchOnFirst.AbstractSwitchOnFirstMain, CoreSubscriber, InnerOperator>[] factories = new BiFunction[] {
                (parent, assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstControlSubscriber((FluxSwitchOnFirst.AbstractSwitchOnFirstMain) parent, (CoreSubscriber) assertSubscriber, true),
                (parent, assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstConditionalControlSubscriber((FluxSwitchOnFirst.AbstractSwitchOnFirstMain) parent, (Fuseable.ConditionalSubscriber) assertSubscriber, true)
        };
        for (BiFunction<FluxSwitchOnFirst.AbstractSwitchOnFirstMain, CoreSubscriber, InnerOperator> factory : factories) {
            for (int i = 0; i < 100000; i++) {
                long[] valueHolder = new long[] { 0 };
                FluxSwitchOnFirst.AbstractSwitchOnFirstMain mockParent = Mockito.mock(FluxSwitchOnFirst.AbstractSwitchOnFirstMain.class);
                Mockito.doNothing().when(mockParent).request(Mockito.anyLong());
                Mockito.doNothing().when(mockParent).cancel();
                Subscription mockSubscription = Mockito.mock(Subscription.class);
                Mockito.doAnswer((a) -> valueHolder[0] += (long) a.getArgument(0)).when(mockSubscription).request(Mockito.anyLong());
                Mockito.doNothing().when(mockSubscription).cancel();
                AssertSubscriber<Object> subscriber = AssertSubscriber.create(0);
                InnerOperator switchOnFirstControlSubscriber = factory.apply(mockParent, Operators.toConditionalSubscriber(subscriber));

                switchOnFirstControlSubscriber.request(10);
                RaceTestUtils.race(() -> {
                            switchOnFirstControlSubscriber.request(10);
                            switchOnFirstControlSubscriber.request(10);
                            switchOnFirstControlSubscriber.request(10);
                            switchOnFirstControlSubscriber.request(10);
                        },
                        () -> switchOnFirstControlSubscriber.onSubscribe(mockSubscription),
                        Schedulers.parallel());

                switchOnFirstControlSubscriber.request(10);
                assertThat(valueHolder[0])
                          .isEqualTo(60L);
                mockSubscription.toString();
            }
        }
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void unitCancelRacingTest() {
        @SuppressWarnings("unchecked")
        BiFunction<FluxSwitchOnFirst.AbstractSwitchOnFirstMain, CoreSubscriber, InnerOperator>[] factories = new BiFunction[] {
                (parent, assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstControlSubscriber((FluxSwitchOnFirst.AbstractSwitchOnFirstMain) parent, (CoreSubscriber) assertSubscriber, true),
                (parent, assertSubscriber) -> new FluxSwitchOnFirst.SwitchOnFirstConditionalControlSubscriber((FluxSwitchOnFirst.AbstractSwitchOnFirstMain) parent, (Fuseable.ConditionalSubscriber) assertSubscriber, true)
        };
        for (BiFunction<FluxSwitchOnFirst.AbstractSwitchOnFirstMain, CoreSubscriber, InnerOperator> factory : factories) {
            for (int i = 0; i < 10000; i++) {
                FluxSwitchOnFirst.AbstractSwitchOnFirstMain mockParent = Mockito.mock(FluxSwitchOnFirst.AbstractSwitchOnFirstMain.class);
                Mockito.doNothing().when(mockParent).request(Mockito.anyLong());
                Mockito.doNothing().when(mockParent).cancel();
                Subscription mockSubscription = Mockito.mock(Subscription.class);
                ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);
                Mockito.doNothing().when(mockSubscription).request(longArgumentCaptor.capture());
                Mockito.doNothing().when(mockSubscription).cancel();
                AssertSubscriber<Object> subscriber = AssertSubscriber.create(0);
                InnerOperator switchOnFirstControlSubscriber = factory.apply(mockParent, Operators.toConditionalSubscriber(subscriber));

                switchOnFirstControlSubscriber.request(10);
                RaceTestUtils.race(() -> switchOnFirstControlSubscriber.cancel(), () -> switchOnFirstControlSubscriber.onSubscribe(mockSubscription), Schedulers.parallel());

                assertThat(longArgumentCaptor.getAllValues().size()).isBetween(0, 1);
                Mockito.verify(mockParent).cancel();
                if (longArgumentCaptor.getAllValues().size() == 1) {
                    assertThat(longArgumentCaptor.getValue()).isEqualTo(10L);
                }
                else if (longArgumentCaptor.getAllValues().size() > 1) {
                    fail("Unexpected number of calls");
                }
            }
        }
    }

    @Test
    public void onCompleteAndRequestRacingTest() {
        Long signal = 1L;
        @SuppressWarnings("unchecked")
        Function<CoreSubscriber<Object>, FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object>>[] factories = new Function[2];
        factories[0] = assertSubscriber -> new FluxSwitchOnFirst.SwitchOnFirstMain<>(assertSubscriber, (s, f) -> f, true);
        factories[1] = assertSubscriber -> new FluxSwitchOnFirst.SwitchOnFirstConditionalMain<>((Fuseable.ConditionalSubscriber<Object>) assertSubscriber, (s, f) -> f, true);

        for (Function<CoreSubscriber<Object>, FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object>> factory : factories) {
            for (int i = 0; i < 1000; i++) {
                Subscription mockSubscription = Mockito.mock(Subscription.class);
                ArgumentCaptor<Long> requestCaptor = ArgumentCaptor.forClass(Long.class);
                Mockito.doNothing().when(mockSubscription).request(requestCaptor.capture());
                Mockito.doNothing().when(mockSubscription).cancel();
                AssertSubscriber<Object> assertSubscriber = AssertSubscriber.create(0);
                CoreSubscriber<? super Object> conditionalAssert = Operators.toConditionalSubscriber(assertSubscriber);
                FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object> switchOnFirstMain = factory.apply(conditionalAssert);

                switchOnFirstMain.onSubscribe(mockSubscription);
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(1L)));
                Mockito.clearInvocations(mockSubscription);
                switchOnFirstMain.onNext(signal);
                RaceTestUtils.race(() -> switchOnFirstMain.onComplete(), () -> switchOnFirstMain.request(55));
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(54L) || argument.equals(55L)));
                assertSubscriber.assertSubscribed()
                        .awaitAndAssertNextValues(signal)
                        .await(Duration.ofSeconds(5))
                        .assertComplete();
            }
        }
    }

    @Test
    public void onErrorAndRequestRacingTest() {
        Long signal = 1L;
        RuntimeException ex = new RuntimeException();
        @SuppressWarnings("unchecked")
        Function<CoreSubscriber<Object>, FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object>>[] factories = new Function[2];
        factories[0] = assertSubscriber -> new FluxSwitchOnFirst.SwitchOnFirstMain<>(assertSubscriber, (s, f) -> f, true);
        factories[1] = assertSubscriber -> new FluxSwitchOnFirst.SwitchOnFirstConditionalMain<>((Fuseable.ConditionalSubscriber<Object>) assertSubscriber, (s, f) -> f, true);

        for (Function<CoreSubscriber<Object>, FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object,Object>> factory : factories) {
            for (int i = 0; i < 1000; i++) {
                Subscription mockSubscription = Mockito.mock(Subscription.class);
                ArgumentCaptor<Long> requestCaptor = ArgumentCaptor.forClass(Long.class);
                Mockito.doNothing().when(mockSubscription).request(requestCaptor.capture());
                Mockito.doNothing().when(mockSubscription).cancel();
                AssertSubscriber<Object> assertSubscriber = AssertSubscriber.create(0);
                CoreSubscriber<? super Object> conditionalAssert = Operators.toConditionalSubscriber(assertSubscriber);
                FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object> switchOnFirstMain = factory.apply(conditionalAssert);

                switchOnFirstMain.onSubscribe(mockSubscription);
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(1L)));
                Mockito.clearInvocations(mockSubscription);
                switchOnFirstMain.onNext(signal);
                RaceTestUtils.race(() -> switchOnFirstMain.onError(ex), () -> switchOnFirstMain.request(55));
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(54L) || argument.equals(55L)));
                assertSubscriber.assertSubscribed()
                                .awaitAndAssertNextValues(signal)
                                .await(Duration.ofSeconds(5))
                                .assertErrorWith(t -> assertThat(t).isEqualTo(ex));
            }
        }
    }

    @Test
    public void cancelAndRequestRacingWithOnCompleteAfterTest() {
        Long signal = 1L;
        @SuppressWarnings("unchecked")
        Function<CoreSubscriber<Object>, FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object>>[] factories = new Function[2];
        factories[0] = assertSubscriber -> new FluxSwitchOnFirst.SwitchOnFirstMain<>(assertSubscriber, (s, f) -> f, true);
        factories[1] = assertSubscriber -> new FluxSwitchOnFirst.SwitchOnFirstConditionalMain<>((Fuseable.ConditionalSubscriber<Object>) assertSubscriber, (s, f) -> f, true);

        for (Function<CoreSubscriber<Object>, FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object>> factory : factories){
            for (int i = 0; i < 1000; i++) {
                Subscription mockSubscription = Mockito.mock(Subscription.class);
                ArgumentCaptor<Long> requestCaptor = ArgumentCaptor.forClass(Long.class);
                AtomicReference<Object> discarded = new AtomicReference<>();
                Mockito.doNothing().when(mockSubscription).request(requestCaptor.capture());
                Mockito.doNothing().when(mockSubscription).cancel();
                AssertSubscriber<Object> assertSubscriber = new AssertSubscriber<>(Context.of(Hooks.KEY_ON_DISCARD, (Consumer<Object>) o -> assertThat(discarded.getAndSet(o)).isNull()), 0L);

                Fuseable.ConditionalSubscriber<? super Object> assertConditional = Operators.toConditionalSubscriber(assertSubscriber);
                FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object> switchOnFirstMain = factory.apply(assertConditional);

                switchOnFirstMain.onSubscribe(mockSubscription);
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(1L)));
                Mockito.clearInvocations(mockSubscription);
                switchOnFirstMain.onNext(signal);
                RaceTestUtils.race(() -> switchOnFirstMain.cancel(), () -> switchOnFirstMain.request(55));
                switchOnFirstMain.onComplete();
                assertSubscriber.assertNotTerminated();
                Object discardedValue = discarded.get();
                if (discardedValue == null) {
                    assertSubscriber.awaitAndAssertNextValues(signal);
                } else {
                    assertThat(discardedValue).isEqualTo(signal);
                }

                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(54L) || argument.equals(55L)));
            }
        }
    }

    @Test
    public void cancelAndRequestRacingOnErrorAfterTest() {
        Long signal = 1L;
        @SuppressWarnings("unchecked")
        Function<CoreSubscriber<Object>, FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object>>[] factories = new Function[2];
        factories[0] = assertSubscriber -> new FluxSwitchOnFirst.SwitchOnFirstMain<>(assertSubscriber, (s, f) -> f, true);
        factories[1] = assertSubscriber -> new FluxSwitchOnFirst.SwitchOnFirstConditionalMain<>((Fuseable.ConditionalSubscriber<Object>) assertSubscriber, (s, f) -> f, true);

        for (Function<CoreSubscriber<Object>, FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object>> factory : factories) {
            for (int i = 0; i < 1000; i++) {
                Subscription mockSubscription = Mockito.mock(Subscription.class);
                ArgumentCaptor<Long> requestCaptor = ArgumentCaptor.forClass(Long.class);
                AtomicReference<Object> discarded = new AtomicReference<>();
                AtomicReference<Object> discardedError = new AtomicReference<>();
                Mockito.doNothing().when(mockSubscription).request(requestCaptor.capture());
                Mockito.doNothing().when(mockSubscription).cancel();
                AssertSubscriber<Object> assertSubscriber = new AssertSubscriber<>(Context.of(
                        Hooks.KEY_ON_DISCARD, (Consumer<Object>) o -> assertThat(discarded.getAndSet(o)).isNull(),
                        Hooks.KEY_ON_ERROR_DROPPED, (Consumer<Object>) o -> assertThat(discardedError.getAndSet(o)).isNull()
                ), 0L);
                CoreSubscriber<? super Object> conditionalAssert = Operators.toConditionalSubscriber(assertSubscriber);
                FluxSwitchOnFirst.AbstractSwitchOnFirstMain<Object, Object> switchOnFirstMain = factory.apply(conditionalAssert);


                switchOnFirstMain.onSubscribe(mockSubscription);
                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(1L)));
                Mockito.clearInvocations(mockSubscription);
                switchOnFirstMain.onNext(signal);
                RaceTestUtils.race(() -> switchOnFirstMain.cancel(), () -> switchOnFirstMain.request(55));
                switchOnFirstMain.onError(new NullPointerException());
                assertSubscriber.assertNotTerminated();
                assertThat(discardedError.get()).isInstanceOf(NullPointerException.class);
                Object discardedValue = discarded.get();
                if (discardedValue == null) {
                    assertSubscriber.awaitAndAssertNextValues(signal);
                } else {
                    assertThat(discardedValue).isEqualTo(signal);
                }

                Mockito.verify(mockSubscription).request(Mockito.longThat(argument -> argument.equals(54L) || argument.equals(55L)));
            }
        }
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
