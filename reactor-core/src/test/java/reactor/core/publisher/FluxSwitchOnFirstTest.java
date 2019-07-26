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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;

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
                    .verify();


        Assertions.assertThat(throwables[0])
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
                    .verify();


        Assertions.assertThat(throwables[0])
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessage("FluxSwitchOnFirst allows only one Subscriber");
    }

    @Test
    public void shouldNotSubscribeTwiceWhenCanceled() {
        CountDownLatch latch = new CountDownLatch(1);
        StepVerifier.create(Flux.just(1L)
                                .doOnComplete(() -> {
                                    try {
                                        latch.await();
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
                                    Schedulers
                                            .single()
                                            .schedule(s::cancel, 10, TimeUnit.MILLISECONDS)
                                )
        )
                    .expectSubscription()
                    .expectNext(1L)
                    .expectNoEvent(Duration.ofMillis(200))
                    .thenCancel()
                    .verifyThenAssertThat()
                    .hasNotDroppedErrors();
    }

    @Test
    public void shouldNotSubscribeTwiceConditionalWhenCanceled() {
        CountDownLatch latch = new CountDownLatch(1);
        StepVerifier.create(Flux.just(1L)
                                .doOnComplete(() -> {
                                    try {
                                        latch.await();
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
                                    Schedulers
                                        .single()
                                        .schedule(s::cancel, 10, TimeUnit.MILLISECONDS)
                                )
        )
                    .expectSubscription()
                    .expectNext(1L)
                    .expectNoEvent(Duration.ofMillis(200))
                    .thenCancel()
                    .verifyThenAssertThat()
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
                    .verify();


        Assertions.assertThat(first).containsExactly(Signal.error(error));
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
                    .verify();


        Assertions.assertThat((long) first[0].get()).isEqualTo(1L);
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
                    .verify();


        Assertions.assertThat(first).containsExactly(Signal.error(error));
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
                    .verify();


        Assertions.assertThat(first).containsExactly(Signal.complete());
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
                    .verify();


        Assertions.assertThat(first).containsExactly(Signal.error(error));
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
                    .verify();


        Assertions.assertThat((long) first[0].get()).isEqualTo(1L);
    }


    @Test
    public void shouldSendOnNextAsyncSignal() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;

                                    return f.subscribeOn(Schedulers.elastic());
                                }))
                    .expectSubscription()
                    .expectNext(1L)
                    .expectComplete()
                    .verify();


        Assertions.assertThat((long) first[0].get()).isEqualTo(1L);
    }

    @Test
    public void shouldSendOnNextAsyncSignalConditional() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        StepVerifier.create(Flux.just(1L)
                                .switchOnFirst((s, f) -> {
                                    first[0] = s;

                                    return f.subscribeOn(Schedulers.elastic());
                                })
                                .filter(p -> true)
                    )
                    .expectSubscription()
                    .expectNext(1L)
                    .expectComplete()
                    .verify();


        Assertions.assertThat((long) first[0].get()).isEqualTo(1L);
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

        Assertions.assertThat(capture.get()).isEqualTo(1L);
        Assertions.assertThat(requested).containsExactly(1L);
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

        Assertions.assertThat(capture.get()).isEqualTo(1L);
        Assertions.assertThat(requested).containsExactly(1L);
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

        Assertions.assertThat(capture).isEmpty();
        Assertions.assertThat(requested).containsExactly(1L, 1L);
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

        Assertions.assertThat(capture).containsExactly(1L);
        Assertions.assertThat(requested).containsExactly(1L, Long.MAX_VALUE);
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

        Assertions.assertThat(capture).containsExactly(1L);
        Assertions.assertThat(requested).containsExactly(1L, Long.MAX_VALUE);
    }

    @Test
    public void shouldReturnCorrectContextOnEmptySource() {
        @SuppressWarnings("unchecked")
        Signal<? extends Long>[] first = new Signal[1];

        Flux<Long> switchTransformed = Flux.<Long>empty()
                .switchOnFirst((f, innerFlux) -> {
                    first[0] = f;
                    return innerFlux;
                })
                .subscriberContext(Context.of("a", "c"))
                .subscriberContext(Context.of("c", "d"));

        StepVerifier.create(switchTransformed, 0)
                    .expectSubscription()
                    .thenRequest(1)
                    .expectAccessibleContext()
                    .contains("a", "c")
                    .contains("c", "d")
                    .then()
                    .expectComplete()
                    .verify();

        Assertions.assertThat(first).containsExactly(Signal.complete(Context.of("a", "c").put("c", "d")));
    }

    @Test
    public void shouldNotFailOnIncorrectPublisherBehavior() {
        TestPublisher<Long> publisher =
                TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
        Flux<Long> switchTransformed = publisher.flux()
                                                .switchOnFirst((first, innerFlux) -> innerFlux.subscriberContext(Context.of("a", "b")));

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
                    .verifyThenAssertThat()
                    .hasDroppedErrors(3)
                    .tookLessThan(Duration.ofSeconds(10));

        publisher.assertWasRequested();
        publisher.assertNoRequestOverflow();
    }

    @Test
    public void shouldBeAbleToAccessUpstreamContext() {
        TestPublisher<Long> publisher = TestPublisher.createCold();

        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst(
                                                      (first, innerFlux) -> innerFlux.map(String::valueOf)
                                                                                     .subscriberContext(Context.of("a", "b"))
                                                  )
                                                  .subscriberContext(Context.of("a", "c"))
                                                  .subscriberContext(Context.of("c", "d"));

        publisher.next(1L);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .thenRequest(1)
                    .then(() -> publisher.next(2L))
                    .expectNext("2")
                    .expectAccessibleContext()
                    .contains("a", "b")
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
                                                               .subscriberContext(Context.of("a", "b"))
                                                  )
                                                  .subscriberContext(Context.of("a", "c"))
                                                  .subscriberContext(Context.of("c", "d"));

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

        Assertions.assertThat(requested.get()).isEqualTo(2L);
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

        Assertions.assertThat(requested.get()).isEqualTo(2L);

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

        Assertions.assertThat(requested.get()).isEqualTo(10001L);
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

        Assertions.assertThat(requested.get()).isEqualTo(10001L);
    }

    @Test
    public void shouldErrorOnOverflowTest() {
        TestPublisher<Long> publisher = TestPublisher.createCold();

        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf));

        publisher.next(1L);

        StepVerifier.create(switchTransformed, 0)
                    .thenRequest(1)
                    .expectNext("1")
                    .then(() -> publisher.next(2L))
                    .expectErrorSatisfies(t -> Assertions
                        .assertThat(t)
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
                                                             String::valueOf)));

        StepVerifier.create(switchTransformed)
                    .expectErrorMessage("hello")
                    .verify(Duration.ofSeconds(10));
    }

    @Test
    public void shouldBeAbleToBeCancelledProperly() {
        TestPublisher<Integer> publisher = TestPublisher.createCold();
        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf));

        publisher.next(1);

        StepVerifier.create(switchTransformed, 0)
                    .thenCancel()
                    .verify(Duration.ofSeconds(10));

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
        TestPublisher<Integer> publisher = TestPublisher.createCold();
        Integer[] discarded = new Integer[1];
        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf))
                                                  .doOnDiscard(Integer.class, e -> discarded[0] = e);

        publisher.next(1);

        StepVerifier.create(switchTransformed, 0)
                    .thenCancel()
                    .verify(Duration.ofSeconds(10));

        publisher.assertCancelled();
        publisher.assertWasRequested();

        Assertions.assertThat(discarded).containsExactly(1);
    }

    @Test
    public void shouldBeAbleToCatchDiscardedElementInCaseOfConditional() {
        TestPublisher<Integer> publisher = TestPublisher.createCold();
        Integer[] discarded = new Integer[1];
        Flux<String> switchTransformed = publisher.flux()
                                                  .switchOnFirst((first, innerFlux) -> innerFlux.map(String::valueOf))
                                                  .filter(t -> true)
                                                  .doOnDiscard(Integer.class, e -> discarded[0] = e);

        publisher.next(1);

        StepVerifier.create(switchTransformed, 0)
                    .thenCancel()
                    .verify(Duration.ofSeconds(10));

        publisher.assertCancelled();
        publisher.assertWasRequested();

        Assertions.assertThat(discarded).contains(1);
    }

    @Test
    public void shouldBeAbleToCancelSubscription() throws InterruptedException {
        Flux<Long> publisher = Flux.just(1L);
        for (int j = 0; j < 100; j++) {
            ArrayList<Long> capturedElements = new ArrayList<>();
            ArrayList<Boolean> capturedCompletions = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                AtomicLong captureElement = new AtomicLong(0L);
                AtomicBoolean captureCompletion = new AtomicBoolean(false);
                AtomicLong requested = new AtomicLong();
                CountDownLatch latch = new CountDownLatch(1);
                Flux<Long> switchTransformed = publisher.doOnRequest(requested::addAndGet)
                                                        .doOnCancel(latch::countDown)
                                                        .switchOnFirst((first, innerFlux) -> innerFlux);

                switchTransformed.subscribeWith(new LambdaSubscriber<>(
                    captureElement::set,
                    __ -> { },
                    () -> captureCompletion.set(true),
                    s -> ForkJoinPool.commonPool().execute(() ->
                        RaceTestUtils.race(s::cancel, () -> s.request(1), Schedulers.parallel())
                    )
                ));

                Assertions.assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
                capturedElements.add(captureElement.get());
                capturedCompletions.add(captureCompletion.get());
            }

            Assertions.assertThat(capturedElements).contains(0L);
            Assertions.assertThat(capturedCompletions).contains(false);
        }
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
                    .verifyThenAssertThat()
                    .hasOperatorErrorsSatisfying(c ->
                        Assertions.assertThat(c)
                                  .hasOnlyOneElementSatisfying(t -> {
                                      Assertions.assertThat(t.getT1()).containsInstanceOf(NullPointerException.class);
                                      Assertions.assertThat(t.getT2()).isEqualTo(expectedCause);
                                  })
                    );


        Assertions.assertThat((long) first[0].get()).isEqualTo(1L);
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


        Assertions.assertThat(first).containsExactly(Signal.error(error));
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
                        Assertions.assertThat(t)
                                  .isInstanceOf(NullPointerException.class);
                        return true;
                    });


        Assertions.assertThat(first).containsExactly(Signal.complete());
    }

    @Test
    public void sourceSubscribedOnce() {
        AtomicInteger subCount = new AtomicInteger();
        Flux<Integer> source = Flux.range(1, 10)
                                   .hide()
                                   .doOnSubscribe(subscription -> subCount.incrementAndGet());

        StepVerifier.create(source.switchOnFirst((s, f) -> f.filter(v -> v % 2 == s.get())))
                    .expectNext(1, 3, 5, 7, 9)
                    .verifyComplete();

        Assertions.assertThat(subCount).hasValue(1);
    }

    @Test
    public void checkHotSource() {
        ReplayProcessor<Long> processor = ReplayProcessor.create(1);

        processor.onNext(1L);
        processor.onNext(2L);
        processor.onNext(3L);


        StepVerifier.create(processor.switchOnFirst((s, f) -> f.filter(v -> v % s.get() == 0)))
                    .expectNext(3L)
                    .then(() -> {
                        processor.onNext(4L);
                        processor.onNext(5L);
                        processor.onNext(6L);
                        processor.onNext(7L);
                        processor.onNext(8L);
                        processor.onNext(9L);
                        processor.onComplete();
                    })
                    .expectNext(6L, 9L)
                    .verifyComplete();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherComplete() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.empty()))
                    .expectSubscription()
                    .verifyComplete();

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherError() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.error(new RuntimeException("test"))))
                    .expectSubscription()
                    .verifyErrorSatisfies(t ->
                        Assertions.assertThat(t)
                                  .hasMessage("test")
                                  .isExactlyInstanceOf(RuntimeException.class)
                    );

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherCancel() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.error(new RuntimeException("test"))))
                    .expectSubscription()
                    .thenCancel()
                    .verify();

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherCompleteConditional() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.empty().delaySubscription(Duration.ofMillis(10))).filter(__ -> true))
                    .then(() -> {
                        FluxPublish.PubSubInner<Long>[] subs = testPublisher.subscribers;
                        Assertions.assertThat(subs).hasSize(1);
                        Assertions.assertThat(subs[0])
                                  .extracting(psi -> psi.actual)
                                  .isInstanceOf(Fuseable.ConditionalSubscriber.class);
                    })
                    .verifyComplete();

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherErrorConditional() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.error(new RuntimeException("test")).delaySubscription(Duration.ofMillis(10))).filter(__ -> true))
                    .then(() -> {
                        FluxPublish.PubSubInner<Long>[] subs = testPublisher.subscribers;
                        Assertions.assertThat(subs).hasSize(1);
                        Assertions.assertThat(subs[0])
                                  .extracting(psi -> psi.actual)
                                  .isInstanceOf(Fuseable.ConditionalSubscriber.class);
                    })
                    .verifyErrorSatisfies(t ->
                            Assertions.assertThat(t)
                                      .hasMessage("test")
                                      .isExactlyInstanceOf(RuntimeException.class)
                    );

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
    }

    @Test
    public void shouldCancelSourceOnUnrelatedPublisherCancelConditional() {
        EmitterProcessor<Long> testPublisher = EmitterProcessor.create();

        testPublisher.onNext(1L);

        StepVerifier.create(testPublisher.switchOnFirst((s, f) -> Flux.error(new RuntimeException("test")).delaySubscription(Duration.ofMillis(10))).filter(__ -> true))
                    .then(() -> {
                        FluxPublish.PubSubInner<Long>[] subs = testPublisher.subscribers;
                        Assertions.assertThat(subs).hasSize(1);
                        Assertions.assertThat(subs[0])
                                  .extracting(psi -> psi.actual)
                                  .isInstanceOf(Fuseable.ConditionalSubscriber.class);
                    })
                    .thenAwait(Duration.ofMillis(50))
                    .thenCancel()
                    .verify();

        Assertions.assertThat(testPublisher.isCancelled()).isTrue();
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
}
