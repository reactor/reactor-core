package reactor.core.publisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.reactivestreams.Publisher;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.concurrent.Queues;

@RunWith(Parameterized.class)
public class OnDiscardShouldNotLeakTest {

    //add DiscardScenarios here to test more operators
    private static DiscardScenario[] SCENARIOS = new DiscardScenario[] {
                DiscardScenario.allFluxSourceArray("merge", 4, Flux::merge),
                DiscardScenario.fluxSource("onBackpressureBuffer", 1, Flux::onBackpressureBuffer),
                DiscardScenario.rawSource("flatMapInner", 1, raw -> Flux.just(1).flatMap(f -> raw)),
                DiscardScenario.fluxSource("flatMap", 1, main -> main.flatMap(f -> Mono.just(f).hide().flux())),
                DiscardScenario.fluxSource("flatMapIterable", 1, f -> f.flatMapIterable(Arrays::asList)),
                DiscardScenario.fluxSource("publishOnDelayErrors", 1, f -> f.publishOn(Schedulers.immediate())),
                DiscardScenario.fluxSource("publishOnImmediateErrors", 1, f -> f.publishOn(Schedulers.immediate(), false, Queues.SMALL_BUFFER_SIZE))
    };

    private static boolean[][] CONDITIONAL_AND_FUSED = new boolean[][] {
            { false, false },
            { true, false },
            { false, true },
            { true, true }
    };

    @Parameterized.Parameters(name = " {2} | conditional={0}, fused={1}")
    public static Collection<Object[]> data() {
        List<Object[]> parameters = new ArrayList<>(CONDITIONAL_AND_FUSED.length * SCENARIOS.length);
        for (DiscardScenario scenario : SCENARIOS) {
            for (boolean[] booleans : CONDITIONAL_AND_FUSED) {
                parameters.add(new Object[] { booleans[0], booleans[1], scenario });
            }
        }
        return parameters;
    }

    private final boolean conditional;
    private final boolean fused;
    private final DiscardScenario discardScenario;
    
    private Scheduler scheduler;

    @Before
    public void setUp() {
        scheduler = Schedulers.newParallel(discardScenario.scenarioDescription + "DiscardScheduler", discardScenario.subscriptionsNumber + 1);
        scheduler.start();
    }

    @After
    public void tearDown() {
        Hooks.resetOnNextDropped();
        Hooks.resetOnErrorDropped();
        Hooks.resetOnNextError();
        Hooks.resetOnOperatorError();
        scheduler.dispose();
    }

    public OnDiscardShouldNotLeakTest(boolean conditional, boolean fused, DiscardScenario discardScenario) {
        this.conditional = conditional;
        this.fused = fused;
        this.discardScenario = discardScenario;
    }

    @Test
    public void ensureMultipleSubscribersSupportWithNoLeaksWhenRacingCancelAndOnNextAndRequest() {
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        int subscriptionsNumber = discardScenario.subscriptionsNumber;
        for (int i = 0; i < 10000; i++) {
            int[] index = new int[] {subscriptionsNumber};
            TestPublisher<Tracked<?>>[] testPublishers = new TestPublisher[subscriptionsNumber];

            for (int i1 = 0; i1 < subscriptionsNumber; i1++) {
                testPublishers[i1] = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            }

            Publisher<Tracked<?>> source = discardScenario.producePublisherFromSources(testPublishers[0], Arrays.copyOfRange(testPublishers, 1, testPublishers.length));

            if (conditional) {
                if (source instanceof Flux) {
                    source = ((Flux<Tracked<?>>)source).filter(t -> true);
                } else {
                    source = ((Mono<Tracked<?>>)source).filter(t -> true);
                }
            }

            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
            if (fused) {
                assertSubscriber.requestedFusionMode(Fuseable.ANY);
            }
            source.subscribe(assertSubscriber);

            if (subscriptionsNumber == 1) {
                Tracked<Integer> value = new Tracked<>(1);
                RaceTestUtils.race(() -> RaceTestUtils.race(assertSubscriber::cancel, () -> assertSubscriber.request(Long.MAX_VALUE), scheduler), () -> testPublishers[0].next(value), scheduler);
            } else {
                int startIndex = --index[0];
                Tracked<Integer> value1 = new Tracked<>(startIndex);
                int secondIndex = --index[0];
                Tracked<Integer> value2 = new Tracked<>(secondIndex);
                Runnable action = () -> RaceTestUtils.race(() -> testPublishers[startIndex].next(value1), () -> testPublishers[secondIndex].next(value2), scheduler);

                while (index[0] > 0) {
                    int nextIndex = --index[0];
                    Tracked<Integer> nextValue = new Tracked<>(nextIndex);
                    Runnable nextAction = action;
                    action = () -> RaceTestUtils.race(nextAction, () -> testPublishers[nextIndex].next(nextValue), scheduler);
                }
                RaceTestUtils.race(() -> RaceTestUtils.race(assertSubscriber::cancel, () -> assertSubscriber.request(Long.MAX_VALUE), scheduler), action, scheduler);
            }

            List<Tracked<?>> values = assertSubscriber.values();
            values.forEach(Tracked::release);

            Tracked.assertNoLeaks();
        }
    }

    @Test
    public void ensureMultipleSubscribersSupportWithNoLeaksWhenPopulatedQueueRacingCancelAndOnNextAndRequest() {
        int subscriptionsNumber = discardScenario.subscriptionsNumber;
        Assumptions.assumeThat(subscriptionsNumber)
                .isGreaterThan(1);
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        for (int i = 0; i < 10000; i++) {
            int[] index = new int[] {subscriptionsNumber};
            TestPublisher<Tracked<?>>[] testPublishers = new TestPublisher[subscriptionsNumber];

            for (int i1 = 0; i1 < subscriptionsNumber; i1++) {
                testPublishers[i1] = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            }

            Publisher<Tracked<?>> source = discardScenario.producePublisherFromSources(testPublishers[0], Arrays.copyOfRange(testPublishers, 1, testPublishers.length));

            if (conditional) {
                if (source instanceof Flux) {
                    source = ((Flux<Tracked<?>>)source).filter(t -> true);
                } else {
                    source = ((Mono<Tracked<?>>)source).filter(t -> true);
                }
            }

            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
            if (fused) {
                assertSubscriber.requestedFusionMode(Fuseable.ANY);
            }
            source.subscribe(assertSubscriber);
            int startIndex = --index[0];
            Tracked<String> value11 = new Tracked<>(startIndex+"1");
            Tracked<String> value12 = new Tracked<>(startIndex+"2");
            Tracked<String> value13 = new Tracked<>(startIndex+"3");
            Tracked<String> value14 = new Tracked<>(startIndex+"4");
            int secondIndex = --index[0];
            Tracked<String> value21 = new Tracked<>(secondIndex+"1");
            Tracked<String> value22 = new Tracked<>(secondIndex+"2");
            Tracked<String> value23 = new Tracked<>(secondIndex+"3");
            Tracked<String> value24 = new Tracked<>(secondIndex+"4");
            Runnable action = () -> RaceTestUtils.race(() -> testPublishers[startIndex].next(value11, value12, value13, value14), () -> testPublishers[secondIndex].next(value21, value22, value23, value24), scheduler);

            while (index[0] > 0) {
                int nextIndex = --index[0];
                Tracked<String> nextValue1 = new Tracked<>(nextIndex+"1");
                Tracked<String> nextValue2 = new Tracked<>(nextIndex+"2");
                Tracked<String> nextValue3 = new Tracked<>(nextIndex+"3");
                Tracked<String> nextValue4 = new Tracked<>(nextIndex+"4");
                Runnable nextAction = action;
                action = () -> RaceTestUtils.race(nextAction, () -> testPublishers[nextIndex].next(nextValue1, nextValue2, nextValue3, nextValue4), scheduler);
            }
            RaceTestUtils.race(() -> RaceTestUtils.race(assertSubscriber::cancel, () -> assertSubscriber.request(Long.MAX_VALUE), scheduler), action, scheduler);
            List<Tracked<?>> values = assertSubscriber.values();
            values.forEach(Tracked::release);

            Tracked.assertNoLeaks();
        }
    }

    @Test
    public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnNext() {
        Assumptions.assumeThat(discardScenario.subscriptionsNumber).isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        for (int i = 0; i < 10000; i++) {
            TestPublisher<Tracked<?>> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            Publisher<Tracked<?>> source = discardScenario.producePublisherFromSources(testPublisher);

            if (conditional) {
                if (source instanceof Flux) {
                    source = ((Flux<Tracked<?>>)source).filter(t -> true);
                } else {
                    source = ((Mono<Tracked<?>>)source).filter(t -> true);
                }
            }

            Scannable scannable = Scannable.from(source);
            Integer prefetch = scannable.scan(Scannable.Attr.PREFETCH);

            Assumptions.assumeThat(prefetch)
                    .isNotZero();

            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
            if (fused) {
                assertSubscriber.requestedFusionMode(Fuseable.ANY);
            }
            source.subscribe(assertSubscriber);

            testPublisher.next(new Tracked<>(1));
            testPublisher.next(new Tracked<>(2));

            Tracked<Integer> value3 = new Tracked<>(3);
            Tracked<Integer> value4 = new Tracked<>(4);
            Tracked<Integer> value5 = new Tracked<>(5);

            RaceTestUtils.race(assertSubscriber::cancel, () -> {
                testPublisher.next(value3);
                testPublisher.next(value4);
                testPublisher.next(value5);
            }, scheduler);

            List<Tracked<?>> values = assertSubscriber.values();
            values.forEach(Tracked::release);

            Tracked.assertNoLeaks();
        }
    }

    @Test
    public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnComplete() {
        Assumptions.assumeThat(discardScenario.subscriptionsNumber)
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        for (int i = 0; i < 10000; i++) {
            TestPublisher<Tracked<?>> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            Publisher<Tracked<?>> source = discardScenario.producePublisherFromSources(testPublisher);

            if (conditional) {
                if (source instanceof Flux) {
                    source = ((Flux<Tracked<?>>)source).filter(t -> true);
                } else {
                    source = ((Mono<Tracked<?>>)source).filter(t -> true);
                }
            }

            Scannable scannable = Scannable.from(source);
            Integer prefetch = scannable.scan(Scannable.Attr.PREFETCH);

            Assumptions.assumeThat(prefetch)
                    .isNotZero();

            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
            if (fused) {
                assertSubscriber.requestedFusionMode(Fuseable.ANY);
            }
            source.subscribe(assertSubscriber);

            testPublisher.next(new Tracked<>(1));
            testPublisher.next(new Tracked<>(2));
            testPublisher.next(new Tracked<>(3));
            testPublisher.next(new Tracked<>(4));

            RaceTestUtils.race(assertSubscriber::cancel, () -> testPublisher.complete(), scheduler);

            List<Tracked<?>> values = assertSubscriber.values();
            values.forEach(Tracked::release);

            Tracked.assertNoLeaks();
        }
    }

    @Test
    public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnError() {
        Assumptions.assumeThat(discardScenario.subscriptionsNumber)
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        for (int i = 0; i < 10000; i++) {
            TestPublisher<Tracked<?>> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            @SuppressWarnings("unchecked")
            Publisher<Tracked<?>> source = discardScenario.producePublisherFromSources(testPublisher);

            if (conditional) {
                if (source instanceof Flux) {
                    source = ((Flux<Tracked<?>>)source).filter(t -> true);
                } else {
                    source = ((Mono<Tracked<?>>)source).filter(t -> true);
                }
            }

            Scannable scannable = Scannable.from(source);
            Integer prefetch = scannable.scan(Scannable.Attr.PREFETCH);

            Assumptions.assumeThat(prefetch)
                    .isNotZero();

            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
            if (fused) {
                assertSubscriber.requestedFusionMode(Fuseable.ANY);
            }
            source.subscribe(assertSubscriber);

            testPublisher.next(new Tracked<>(1));
            testPublisher.next(new Tracked<>(2));
            testPublisher.next(new Tracked<>(3));
            testPublisher.next(new Tracked<>(4));

            RaceTestUtils.race(assertSubscriber::cancel, () -> testPublisher.error(new RuntimeException("test")), scheduler);

            List<Tracked<?>> values = assertSubscriber.values();
            values.forEach(Tracked::release);
            if (assertSubscriber.isTerminated()) {
              // has a chance to error with rejected exception
              assertSubscriber.assertError();
            }

            Tracked.assertNoLeaks();
        }
    }

    @Test
    public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOverflowError() {
        Assumptions.assumeThat(discardScenario.subscriptionsNumber)
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        for (int i = 0; i < 10000; i++) {
            TestPublisher<Tracked<?>> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            @SuppressWarnings("unchecked")
            Publisher<Tracked<?>> source = discardScenario.producePublisherFromSources(testPublisher);

            if (conditional) {
                if (source instanceof Flux) {
                    source = ((Flux<Tracked<?>>)source).filter(t -> true);
                } else {
                    source = ((Mono<Tracked<?>>)source).filter(t -> true);
                }
            }

            Scannable scannable = Scannable.from(source);
            Integer capacity = scannable.scan(Scannable.Attr.CAPACITY);
            Integer prefetch = Math.min(scannable.scan(Scannable.Attr.PREFETCH), capacity == 0 ? Integer.MAX_VALUE : capacity);

            Assumptions.assumeThat(prefetch)
                    .isNotZero();
            Assumptions.assumeThat(prefetch)
                    .isNotEqualTo(Integer.MAX_VALUE);
            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
            if (fused) {
                assertSubscriber.requestedFusionMode(Fuseable.ANY);
            }
            source.subscribe(assertSubscriber);

            for (int j = 0; j < prefetch - 1; j++) {
                testPublisher.next(new Tracked<>(j));
            }

            Tracked<Integer> lastValue = new Tracked<>(prefetch - 1);
            Tracked<Integer> overflowValue1 = new Tracked<>(prefetch);
            Tracked<Integer> overflowValue2 = new Tracked<>(prefetch + 1);

            RaceTestUtils.race(assertSubscriber::cancel, () -> {
                testPublisher.next(lastValue);
                testPublisher.next(overflowValue1);
                testPublisher.next(overflowValue2);
            });

            List<Tracked<?>> values = assertSubscriber.values();
            values.forEach(Tracked::release);
            if (assertSubscriber.isTerminated()) {
                // has a chance to error with rejected exception
                assertSubscriber.assertError();
            }

            Tracked.assertNoLeaks();
        }
    }

    @Test
    public void ensureNoLeaksPopulatedQueueAndRacingCancelAndRequest() {
        Assumptions.assumeThat(discardScenario.subscriptionsNumber)
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        for (int i = 0; i < 10000; i++) {
            TestPublisher<Tracked<?>> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            @SuppressWarnings("unchecked")
            Publisher<Tracked<?>> source = discardScenario.producePublisherFromSources(testPublisher);

            if (conditional) {
                if (source instanceof Flux) {
                    source = ((Flux<Tracked<?>>)source).filter(t -> true);
                } else {
                    source = ((Mono<Tracked<?>>)source).filter(t -> true);
                }
            }

            Scannable scannable = Scannable.from(source);
            Integer prefetch = scannable.scan(Scannable.Attr.PREFETCH);

            Assumptions.assumeThat(prefetch)
                    .isNotZero();

            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
            if (fused) {
                assertSubscriber.requestedFusionMode(Fuseable.ANY);
            }
            source.subscribe(assertSubscriber);

            testPublisher.next(new Tracked<>(1));
            testPublisher.next(new Tracked<>(2));
            testPublisher.next(new Tracked<>(3));
            testPublisher.next(new Tracked<>(4));

            RaceTestUtils.race(assertSubscriber::cancel, () -> assertSubscriber.request(Long.MAX_VALUE), scheduler);

            List<Tracked<?>> values = assertSubscriber.values();
            values.forEach(Tracked::release);

            Tracked.assertNoLeaks();
        }
    }

    public static final class Tracked<T> extends AtomicBoolean {

        static final Queue<Tracked<?>> tracked = new ConcurrentLinkedQueue<>();


        static void safeRelease(Object t) {
            if (t instanceof Tracked) {
                ((Tracked<?>) t).release();
            }
        }

        static void assertNoLeaks() {
            try {
                Assertions.assertThat(tracked)
                        .allMatch(Tracked::isReleased);
            } finally {
                tracked.clear();
            }
        }

        final T value;

        public Tracked(T value) {
            this.value = value;

            // track value
            tracked.add(this);
        }

        public Tracked<T> release() {
            set(true);
            return this;
        }

        public boolean isReleased() {
            return get();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tracked<?> tracked = (Tracked<?>) o;

            return value != null ? value.equals(tracked.value) : tracked.value == null;
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "Tracked{" +
                    " value=" + value +
                    " released=" + get() +
                    " }";
        }
    }

    static class DiscardScenario {

        static DiscardScenario rawSource(String desc, int subs, Function<TestPublisher<Tracked<?>>, Publisher<Tracked<?>>> rawToPublisherProducer) {
            return new DiscardScenario(desc, subs, (main, others) -> rawToPublisherProducer.apply(main));
        }

        static DiscardScenario fluxSource(String desc, int subs, Function<Flux<Tracked<?>>, Publisher<Tracked<?>>> fluxToPublisherProducer) {
            return new DiscardScenario(desc, subs, (main, others) -> fluxToPublisherProducer.apply(main.flux()));
        }

        static DiscardScenario allFluxSourceArray(String desc, int subs,
                Function<Flux<Tracked<?>>[], Publisher<Tracked<?>>> producer) {
            return new DiscardScenario(desc, subs, (main, others) -> {
                @SuppressWarnings("unchecked")
                Flux<Tracked<?>>[] inners = new Flux[subs];
                inners[0] = main.flux();
                for (int i = 1; i < subs; i++) {
                    inners[i] = others[i - 1].flux();
                }
                return producer.apply(inners);
            });
        }

        final String scenarioDescription;
        final int subscriptionsNumber;

        private final BiFunction<TestPublisher<Tracked<?>>, TestPublisher<Tracked<?>>[], Publisher<Tracked<?>>>
                publisherProducer;

        DiscardScenario(String description, int subscriptionsNumber, Function<TestPublisher<Tracked<?>>, Publisher<Tracked<?>>> simplePublisherProducer) {
            this.scenarioDescription = description;
            this.subscriptionsNumber = subscriptionsNumber;
            this.publisherProducer = (main, others) -> simplePublisherProducer.apply(main);
        }

        DiscardScenario(String description, int subscriptionsNumber, BiFunction<TestPublisher<Tracked<?>>, TestPublisher<Tracked<?>>[], Publisher<Tracked<?>>> publisherProducer) {
            this.scenarioDescription = description;
            this.subscriptionsNumber = subscriptionsNumber;
            this.publisherProducer = publisherProducer;
        }
        
        Publisher<Tracked<?>> producePublisherFromSources(TestPublisher<Tracked<?>> mainSource, TestPublisher<Tracked<?>>... otherSources) {
            return publisherProducer.apply(mainSource, otherSources);
        }

        @Override
        public String toString() {
            return scenarioDescription;
        }
    }
}