package reactor.core.publisher;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class AbstractOnDiscardShouldNotLeakTest {

    abstract Publisher<Tracked<?>> transform(Flux<Tracked<?>> upstream);

    @Test
    public void ensureNoLeaksOnCancelOnNext() {
        for (int i = 0; i < 10000; i++) {
            FluxSink<Tracked<?>> sink[] = new FluxSink[1];
            Publisher<Tracked<?>> source = transform(Flux.create(s -> {
                sink[0] = s;
            }, FluxSink.OverflowStrategy.IGNORE));

            AssertSubscriber<Tracked<?>> assertSubscriber = new AssertSubscriber<>(Operators.enableOnDiscard(null, Tracked::safeRelease));
            source.subscribe(assertSubscriber);

            Tracked<Integer> value = new Tracked<>(1);

            RaceTestUtils.race(assertSubscriber::cancel, () -> sink[0].next(value));

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
