package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;


public class FluxLimitRateTest {

    @Test
    public void emptyTest() {

    }

    public static abstract class TestBase {
        ArrayList<Long> observedRequests;

        @Before
        public void setUp() {
            observedRequests = new ArrayList<>();
        }

        <T> Flux<T> addRequestObservation(Flux<T> in) {
            return in.doOnRequest(observedRequests::add);
        }

        void assertObservedRequestsExactly(Long... requests) {
            Assertions.assertThat(observedRequests)
                      .containsExactly(requests);
        }

        abstract Flux<Integer> source(int cnt);

        boolean checkSequence() {
            return true;
        }

        Flux<Integer> modify(Flux<Integer> in, int cnt) {
            return in;
        }

        @Test
        public void testThatRequest1WillBePropagatedUpstream() {
            Flux<Integer> source = source(1)
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 128)
                    .transform(s -> this.modify(s, 1));

            StepVerifier.create(fluxLimitRate, 0)
                        .thenRequest(1)
                        .expectNextCount(1)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));

            assertObservedRequestsExactly(1L);
        }

        @Test
        public void testThatRequestWillBePropagatedUpstream1() {
            int cnt = 192;
            Flux<Integer> source = source(cnt)
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 64)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate)
                        .expectNextCount(cnt)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));

            assertObservedRequestsExactly(128L, 64L, 64L, 64L);
            //                                  ------^------
            //                                     192 / 64 == 3 (which means there is 3
            //                                     request(64))
        }

        @Test
        public void testThatRequestWillBePropagatedUpstream2() {
            int cnt = 192;
            Flux<Integer> source = source(cnt)
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 64)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate, cnt)
                        .expectNextCount(cnt)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));

            assertObservedRequestsExactly(128L, 64L);
        }



        @Test
        public void testThatRequestWillBePropagatedUpstream3() {
            int cnt = 192;
            Flux<Integer> source = source(cnt)
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 64)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate, 128)
                        .expectNextCount(128)
                        .thenRequest(32)
                        .expectNextCount(32)
                        .thenRequest(Long.MAX_VALUE)
                        .expectNextCount(32)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));

            assertObservedRequestsExactly(128L, 32L, 64L, 64L);
        }

        @Test
        public void testThatRequest256WillBePropagatedToUpstreamWithLimitedRate() {
            int cnt = 256;
            Flux<Integer> source = source(cnt)
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 128)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate, 0)
                        .thenRequest(cnt)
                        .expectNextCount(cnt)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));

            assertObservedRequestsExactly(128L, 96L, 32L);
        }

        @Test
        public void testThatRequest256WillBePropagatedToUpstreamWithLimitedRateInFewSteps() {
            int cnt = 256;
            Flux<Integer> source = source(cnt)
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 128)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate, 10) // cause request(10)
                        .expectNextCount(5)
                        .thenRequest(128) // cause request(118) 128 - 10 == 118
                        // once 96 is received it cause another request(10) to fulfil
                        // general request from downstream with rate limiting
                        .expectNextCount(133)
                        .expectNoEvent(Duration.ofMillis(10))
                        .thenRequest(Long.MAX_VALUE) // cause request in size 86 cuze 10 of
                        // lowTid was fulfilled previously
                        // so the next request(96) happens once another total 96 elements
                        // were delivered
                        .expectNextCount(118)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));

            assertObservedRequestsExactly(
                10L,
                118L,
                10L,
                86L,
                96L
            );
        }

        @Test
        public void testThatRequestInRandomFashionWillBePropagatedToUpstreamWithLimitedRateInFewSteps()
            throws InterruptedException {
            final int cnt = 100000;
            Flux<Integer> range = source(cnt);
            Flux<Integer> source = range
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 128)
                    .transform(s -> this.modify(s, cnt));

            CountDownLatch latch = new CountDownLatch(1);
            ArrayList<Integer> collected = new ArrayList<>();
            SignalType[] finalSignal = new SignalType[1];

            fluxLimitRate
                .subscribe(new BaseSubscriber<Integer>() {
                    Disposable requester;
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        requester = Flux
                            .range(0, cnt)
                            .concatMap(i -> Mono.delay(Duration.ofMillis(ThreadLocalRandom.current().nextInt(1, 100))).thenReturn(i))
                            .subscribe(
                                new Consumer<Integer>() {
                                    int count = cnt;

                                    @Override
                                    public void accept(Integer __) {
                                        int random = ThreadLocalRandom.current().nextInt(1, 512);

                                        long request = Math.min(random, count);

                                        count -= request;

                                        subscription.request(request);
                                    }
                                });
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        collected.add(value);
                    }

                    @Override
                    protected void hookFinally(SignalType type) {
                        finalSignal[0] = type;
                        requester.dispose();
                        latch.countDown();
                    }
                });

            Assertions.assertThat(latch.await(10, TimeUnit.MINUTES))
                      .isTrue();

            Assertions.assertThat(finalSignal)
                      .containsExactly(SignalType.ON_COMPLETE);
            if (checkSequence()) {
                Assertions.assertThat(collected)
                          .containsSequence(range.toIterable());
            }
            Assertions.assertThat(observedRequests)
                      .allMatch(r -> r <= 128);
        }


        @Test
        public void testThatRequestInRandomFashionWillBePropagatedToUpstreamWithLimitedRateInFewSteps2()
            throws InterruptedException {
            final int cnt = 100000;
            Flux<Integer> range = source(cnt);
            Flux<Integer> source =
                range
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 32)
                    .transform(s -> this.modify(s, cnt));

            CountDownLatch latch = new CountDownLatch(1);
            ArrayList<Integer> collected = new ArrayList<>();
            SignalType[] finalSignal = new SignalType[1];

            fluxLimitRate
                .subscribe(new BaseSubscriber<Integer>() {
                    Disposable requester;
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        requester = Flux
                            .range(0, cnt)
                            .concatMap(i -> Mono.delay(Duration.ofMillis(ThreadLocalRandom.current().nextInt(1, 100))).thenReturn(i))
                            .subscribe(
                                new Consumer<Integer>() {
                                    int count = cnt;

                                    @Override
                                    public void accept(Integer __) {
                                        int random = ThreadLocalRandom.current().nextInt(1, 512);

                                        long request = Math.min(random, count);

                                        count -= request;

                                        subscription.request(request);
                                    }
                                });
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        collected.add(value);
                    }

                    @Override
                    protected void hookFinally(SignalType type) {
                        finalSignal[0] = type;
                        requester.dispose();
                        latch.countDown();
                    }
                });

            Assertions.assertThat(latch.await(10, TimeUnit.MINUTES))
                      .isTrue();

            Assertions.assertThat(finalSignal)
                      .containsExactly(SignalType.ON_COMPLETE);
            if (checkSequence()) {
                Assertions.assertThat(collected)
                          .containsSequence(range.toIterable());
            }
            Assertions.assertThat(observedRequests)
                      .allMatch(r -> r <= 128);
        }

        @Test
        public void testThatRequestInRandomFashionWillBePropagatedToUpstreamWithLimitedRateInFewSteps3()
            throws InterruptedException {
            int cnt = 100000;
            Flux<Integer> range = source(cnt);
            Flux<Integer> source =
                range
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 0)
                    .transform(s -> this.modify(s, cnt));

            CountDownLatch latch = new CountDownLatch(1);
            ArrayList<Integer> collected = new ArrayList<>();
            SignalType[] finalSignal = new SignalType[1];

            fluxLimitRate
                .subscribe(new BaseSubscriber<Integer>() {
                    Disposable requester;
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        requester = Flux
                            .range(0, cnt)
                            .concatMap(i -> Mono.delay(Duration.ofMillis(ThreadLocalRandom.current().nextInt(1, 100))).thenReturn(i))
                            .subscribe(
                                new Consumer<Integer>() {
                                    int count = cnt;

                                    @Override
                                    public void accept(Integer __) {
                                        int random = ThreadLocalRandom.current().nextInt(1, 512);

                                        long request = Math.min(random, count);

                                        count -= request;

                                        subscription.request(request);
                                    }
                                });
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        collected.add(value);
                    }

                    @Override
                    protected void hookFinally(SignalType type) {
                        finalSignal[0] = type;
                        requester.dispose();
                        latch.countDown();
                    }
                });

            Assertions.assertThat(latch.await(10, TimeUnit.MINUTES))
                      .isTrue();

            Assertions.assertThat(finalSignal)
                      .containsExactly(SignalType.ON_COMPLETE);
            if (checkSequence()) {
                Assertions.assertThat(collected)
                          .containsSequence(range.toIterable());
            }
            Assertions.assertThat(observedRequests)
                      .allMatch(r -> r <= 128);
        }

        @Test
        public void testThatRequestLongMaxValueWillBeDeliveredInSeparateChunks() {
            int cnt = 10000000;
            Flux<Integer> source = source(cnt)
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 128)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate)
                        .expectNextCount(cnt)
                        .expectComplete()
                        .verify(Duration.ofMillis(30000));

            Assertions.assertThat(observedRequests)
                      .first()
                      .isEqualTo(128L);

            Assertions.assertThat(observedRequests.subList(1, observedRequests.size()))
                      .allMatch(r -> r == 96L);
        }

        @Test
        public void testThatRequestLongMaxWithIntegerMaxValuePrefetchWillBeDeliveredAsLongMaxValue() {
            int cnt = 10000000;
            Flux<Integer> source = source(cnt)
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, Integer.MAX_VALUE, Integer.MAX_VALUE)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate)
                        .expectNextCount(cnt)
                        .expectComplete()
                        .verify(Duration.ofMillis(30000));
        }

        @Test
        public void testThatRequestLongMax1() {
            int cnt = 10000000;
            Flux<Integer> source = source(cnt)
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 1)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate)
                        .expectNextCount(cnt)
                        .expectComplete()
                        .verify(Duration.ofMillis(30000));
        }

        @Test
        public void testThatRequestLongMax2() {
            int cnt = 10000000;
            Flux<Integer> source = source(cnt)
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 32)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate)
                        .expectNextCount(cnt)
                        .expectComplete()
                        .verify(Duration.ofMillis(30000));
        }

        @Test
        public void testThatRequestLongMax3() {
            int cnt = 10000000;
            Flux<Integer> source = source(cnt)
                    .subscribeOn(Schedulers.parallel())
                    .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 100)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate)
                        .expectNextCount(cnt)
                        .expectComplete()
                        .verify(Duration.ofMillis(30000));
        }
    }

    public static class PlainFluxLimitRateTest extends TestBase {
        @Override
        Flux<Integer> source(int cnt) {
            return Flux.range(0, cnt);
        }
    }

    public static class AsyncFluxLimitRateTest extends TestBase {
        @Override
        Flux<Integer> source(int cnt) {
            return Flux.range(0, cnt)
                .subscribeOn(Schedulers.elastic());
        }
    }

    @Ignore("Because FluxRepeat breaks conditional chain")
    public static class ConditionalFluxLimitRateTest extends TestBase {

        @Override
        boolean checkSequence() {
            return false;
        }

        @Override
        Flux<Integer> source(int cnt) {
            return Flux.range(0, Integer.MAX_VALUE)
                       .repeat();
        }

        @Override
        Flux<Integer> modify(Flux<Integer> in, int cnt) {
            AtomicInteger counter = new AtomicInteger();
            return in
                .filter(e -> ThreadLocalRandom.current().nextBoolean())
                .takeUntil(i -> counter.incrementAndGet() == cnt);
        }
    }

    public static class BrokenConditionalFluxLimitRateTest extends TestBase {

        @Override
        boolean checkSequence() {
            return false;
        }

        @Override
        Flux<Integer> source(int cnt) {
            return Flux.range(0, Integer.MAX_VALUE)
                       .repeat()
                       .hide();
        }

        @Override
        Flux<Integer> modify(Flux<Integer> in, int cnt) {
            AtomicInteger counter = new AtomicInteger();
            return in
                .filter(e -> ThreadLocalRandom.current().nextBoolean())
                .takeUntil(i -> counter.incrementAndGet() == cnt);
        }

        @Test
        public void testThatRequest1WillBePropagatedUpstream() {
            Flux<Integer> source = source(1)
                .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 128)
                    .transform(s -> this.modify(s, 1));

            StepVerifier.create(fluxLimitRate, 0)
                        .thenRequest(1)
                        .expectNextCount(1)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));


            Assertions.assertThat(observedRequests)
                      .allMatch(r -> r <= 128);
        }

        @Test
        public void testThatRequestWillBePropagatedUpstream1() {
            int cnt = 192;
            Flux<Integer> source = source(cnt)
                .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 64)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate)
                        .expectNextCount(cnt)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));


            Assertions.assertThat(observedRequests)
                      .allMatch(r -> r <= 128 && r >= 64);
            //                                  ------^------
            //                                     192 / 64 == 3 (which means there is 3
            //                                     request(64))
        }

        @Test
        public void testThatRequestWillBePropagatedUpstream2() {
            int cnt = 192;
            Flux<Integer> source = source(cnt)
                .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 64)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate, cnt)
                        .expectNextCount(cnt)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));

            Assertions.assertThat(observedRequests)
                      .allMatch(r -> r <= 128);
        }

        @Test
        public void testThatRequestWillBePropagatedUpstream3() {
            int cnt = 192;
            Flux<Integer> source = source(cnt)
                .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 64)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate, 128)
                        .expectNextCount(128)
                        .thenRequest(32)
                        .expectNextCount(32)
                        .thenRequest(Long.MAX_VALUE)
                        .expectNextCount(32)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));

            Assertions.assertThat(observedRequests)
                      .allMatch(r -> r <= 128);
        }

        @Test
        public void testThatRequest256WillBePropagatedToUpstreamWithLimitedRate() {
            int cnt = 256;
            Flux<Integer> source = source(cnt)
                .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 128)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate, 0)
                        .thenRequest(cnt)
                        .expectNextCount(cnt)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));

            Assertions.assertThat(observedRequests)
                      .allMatch(r -> r <= 128);
        }

        @Test
        public void testThatRequest256WillBePropagatedToUpstreamWithLimitedRateInFewSteps() {
            int cnt = 256;
            Flux<Integer> source = source(cnt)
                .transform(this::addRequestObservation);

            Flux<Integer> fluxLimitRate =
                new FluxLimitRate<>(source, 128, 128)
                    .transform(s -> this.modify(s, cnt));

            StepVerifier.create(fluxLimitRate, 10) // cause request(10)
                        .expectNextCount(5)
                        .thenRequest(128) // cause request(118) 128 - 10 == 118
                        // once 96 is received it cause another request(10) to fulfil
                        // general request from downstream with rate limiting
                        .expectNextCount(133)
                        .expectNoEvent(Duration.ofMillis(10))
                        .thenRequest(Long.MAX_VALUE) // cause request in size 86 cuze 10 of
                        // lowTid was fulfilled previously
                        // so the next request(96) happens once another total 96 elements
                        // were delivered
                        .expectNextCount(118)
                        .expectComplete()
                        .verify(Duration.ofMillis(1000));

            Assertions.assertThat(observedRequests)
                      .allMatch(r -> r <= 128);
        }
    }
}