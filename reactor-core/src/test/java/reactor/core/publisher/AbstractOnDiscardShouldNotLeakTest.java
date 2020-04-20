package reactor.core.publisher;

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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(Parameterized.class)
public abstract class AbstractOnDiscardShouldNotLeakTest {

    @Parameterized.Parameters
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
            { false, false },
            { true, false },
            { false, true },
            { true, true }
        });
    }

    private final boolean conditional;
    private final boolean fused;
    
    private Scheduler scheduler;

    public AbstractOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        this.conditional = conditional;
        this.fused = fused;
    }

    protected abstract Publisher<Tracked<?>> transform(TestPublisher<Tracked<?>> main, TestPublisher<Tracked<?>>... additional);

    int subscriptionsNumber() {
        return 1;
    }
    
    @Before
    public void setUp() {
        scheduler = Schedulers.newParallel("testScheduler", subscriptionsNumber() + 1);
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

    @Test
    public void ensureMultipleSubscribersSupportWithNoLeaksWhenRacingCancelAndOnNextAndRequest() {
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        int subscriptionsNumber = subscriptionsNumber();
        for (int i = 0; i < 10000; i++) {
            int[] index = new int[] {subscriptionsNumber};
            TestPublisher<Tracked<?>>[] testPublishers = new TestPublisher[subscriptionsNumber];

            for (int i1 = 0; i1 < subscriptionsNumber; i1++) {
                testPublishers[i1] = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            }

            Publisher<Tracked<?>> source = transform(testPublishers[0], Arrays.copyOfRange(testPublishers, 1, testPublishers.length));

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
        scheduler.dispose();
    }

    @Test
    public void ensureMultipleSubscribersSupportWithNoLeaksWhenPopulatedQueueRacingCancelAndOnNextAndRequest() {
        int subscriptionsNumber = subscriptionsNumber();
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

            Publisher<Tracked<?>> source = transform(testPublishers[0], Arrays.copyOfRange(testPublishers, 1, testPublishers.length));

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
        scheduler.dispose();
    }

    @Test
    public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnNext() {
        Assumptions.assumeThat(subscriptionsNumber())
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        Scheduler scheduler = Schedulers.newParallel("testScheduler", subscriptionsNumber() + 1);
        scheduler.start();
        for (int i = 0; i < 10000; i++) {
            TestPublisher<Tracked<?>> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            Publisher<Tracked<?>> source = transform(testPublisher);

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
        scheduler.dispose();
    }

    @Test
    public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnComplete() {
        Assumptions.assumeThat(subscriptionsNumber())
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        Scheduler scheduler = Schedulers.newParallel("testScheduler", subscriptionsNumber() + 1);
        scheduler.start();
        for (int i = 0; i < 10000; i++) {
            TestPublisher<Tracked<?>> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            Publisher<Tracked<?>> source = transform(testPublisher);

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
        scheduler.dispose();
    }

    @Test
    public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOnError() {
        Assumptions.assumeThat(subscriptionsNumber())
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        Scheduler scheduler = Schedulers.newParallel("testScheduler", subscriptionsNumber() + 1);
        scheduler.start();
        for (int i = 0; i < 10000; i++) {
            TestPublisher<Tracked<?>> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            Publisher<Tracked<?>> source = transform(testPublisher);

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
        scheduler.dispose();
    }

    @Test
    public void ensureNoLeaksPopulatedQueueAndRacingCancelAndOverflowError() {
        Assumptions.assumeThat(subscriptionsNumber())
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        Scheduler scheduler = Schedulers.newParallel("testScheduler", subscriptionsNumber() + 1);
        scheduler.start();
        for (int i = 0; i < 10000; i++) {
            TestPublisher<Tracked<?>> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            Publisher<Tracked<?>> source = transform(testPublisher);

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
        scheduler.dispose();
    }

    @Test
    public void ensureNoLeaksPopulatedQueueAndRacingCancelAndRequest() {
        Assumptions.assumeThat(subscriptionsNumber())
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        Hooks.onErrorDropped(e -> {});
        Hooks.onOperatorError((e,v) -> null);
        for (int i = 0; i < 10000; i++) {
            TestPublisher<Tracked<?>> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.REQUEST_OVERFLOW);
            Publisher<Tracked<?>> source = transform(testPublisher);

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
        scheduler.dispose();
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
}