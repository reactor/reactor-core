package reactor.core.publisher;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class AbstractOnDiscardShouldNotLeakTest {

    abstract Publisher<Tracked<?>> transform(Flux<Tracked<?>> upstream);

    int subscriptionsNumber() {
        return 1;
    }

    @Test
    public void ensureNoLeaksOnCancelOnNext1() {
        Hooks.onNextDropped(Tracked::safeRelease);
        Scheduler scheduler = Schedulers.newParallel("testScheduler", subscriptionsNumber());
        scheduler.start();
        for (int i = 0; i < 10000; i++) {
            int[] index = new int[] { 0 };
            FluxSink<Tracked<?>> sink[] = new FluxSink[subscriptionsNumber()];
            Publisher<Tracked<?>> source = transform(Flux.create(s -> {
                sink[index[0]++] = s;
            }, FluxSink.OverflowStrategy.IGNORE));

            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease));
            source.subscribe(assertSubscriber);

            if (subscriptionsNumber() == 1) {
                Tracked<Integer> value = new Tracked<>(1);
                RaceTestUtils.race(assertSubscriber::cancel, () -> sink[0].next(value), scheduler);
            } else {
                int startIndex = --index[0];
                Tracked<Integer> value1 = new Tracked<>(startIndex);
                int secondIndex = --index[0];
                Tracked<Integer> value2 = new Tracked<>(secondIndex);
                Runnable action = () -> RaceTestUtils.race(() -> sink[startIndex].next(value1), () -> sink[secondIndex].next(value2), scheduler);

                while (index[0] > 0) {
                    int nextIndex = --index[0];
                    Tracked<Integer> nextValue = new Tracked<>(nextIndex);
                    Runnable nextAction = action;
                    action = () -> RaceTestUtils.race(nextAction, () -> sink[nextIndex].next(nextValue), scheduler);
                }
                RaceTestUtils.race(assertSubscriber::cancel, action, scheduler);
            }

            List<Tracked<?>> values = assertSubscriber.values();
            values.forEach(Tracked::release);

            Tracked.assertNoLeaks();
        }
    }

    @Test
    public void ensureNoLeaksOnCancelOnNext2() {
        Assumptions.assumeThat(subscriptionsNumber())
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        for (int i = 0; i < 10000; i++) {
            FluxSink<Tracked<?>> sink[] = new FluxSink[1];
            Publisher<Tracked<?>> source = transform(Flux.create(s -> {
                sink[0] = s;
            }, FluxSink.OverflowStrategy.IGNORE));

            Scannable scannable = Scannable.from(source);
            Integer prefetch = scannable.scan(Scannable.Attr.PREFETCH);

            Assumptions.assumeThat(prefetch)
                    .isNotZero();

            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
            source.subscribe(assertSubscriber);

            sink[0].next(new Tracked<>(1));
            sink[0].next(new Tracked<>(2));
            sink[0].next(new Tracked<>(3));
            sink[0].next(new Tracked<>(4));

            Tracked<Integer> value5 = new Tracked<>(5);

            RaceTestUtils.race(assertSubscriber::cancel, () -> sink[0].next(value5));

            List<Tracked<?>> values = assertSubscriber.values();
            values.forEach(Tracked::release);

            Tracked.assertNoLeaks();
        }
    }

    @Test
    public void ensureNoLeaksOnCancelOnNext3() {
        Assumptions.assumeThat(subscriptionsNumber())
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        for (int i = 0; i < 10000; i++) {
            FluxSink<Tracked<?>> sink[] = new FluxSink[1];
            Publisher<Tracked<?>> source = transform(Flux.create(s -> {
                sink[0] = s;
            }, FluxSink.OverflowStrategy.IGNORE));

            Scannable scannable = Scannable.from(source);
            Integer prefetch = scannable.scan(Scannable.Attr.PREFETCH);

            Assumptions.assumeThat(prefetch)
                    .isNotZero();

            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
            source.subscribe(assertSubscriber);

            sink[0].next(new Tracked<>(1));
            sink[0].next(new Tracked<>(2));
            sink[0].next(new Tracked<>(3));
            sink[0].next(new Tracked<>(4));

            RaceTestUtils.race(assertSubscriber::cancel, () -> sink[0].complete());

            List<Tracked<?>> values = assertSubscriber.values();
            values.forEach(Tracked::release);

            Tracked.assertNoLeaks();
        }
    }

    @Test
    public void ensureNoLeaksOnCancelOnNext4() {
        Assumptions.assumeThat(subscriptionsNumber())
                .isOne();
        Hooks.onNextDropped(Tracked::safeRelease);
        for (int i = 0; i < 10000; i++) {
            FluxSink<Tracked<?>> sink[] = new FluxSink[1];
            Publisher<Tracked<?>> source = transform(Flux.create(s -> {
                sink[0] = s;
            }, FluxSink.OverflowStrategy.IGNORE));

            Scannable scannable = Scannable.from(source);
            Integer prefetch = scannable.scan(Scannable.Attr.PREFETCH);

            Assumptions.assumeThat(prefetch)
                    .isNotZero();

            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease), 0);
            source.subscribe(assertSubscriber);

            sink[0].next(new Tracked<>(1));
            sink[0].next(new Tracked<>(2));
            sink[0].next(new Tracked<>(3));
            sink[0].next(new Tracked<>(4));

            RaceTestUtils.race(assertSubscriber::cancel, () -> assertSubscriber.request(Long.MAX_VALUE));

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
}
