package reactor.core.publisher;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;
import org.openjdk.jcstress.infra.results.I_Result;

import java.util.concurrent.atomic.AtomicInteger;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public class SinksManyReplayLatestStressTest {
    final StressSubscriber<String> target = new StressSubscriber<>();

    final SinkManyReplayProcessor<String> sink = new SinkManyReplayProcessor<>(
        new FluxReplay.SingletonReplayBuffer<>()
    );

    @JCStressTest
    @Outcome(id = {"1"}, expect = ACCEPTABLE, desc = "single emission")
    @Outcome(id = {"2"}, expect = ACCEPTABLE, desc = "two emissions")
    @State
    public static class TryEmitNextStressTest extends SinksManyReplayLatestStressTest {

        @Actor
        public void first() {
            sink.tryEmitNext("Hello");
        }

        @Actor
        public void second() {
            sink.tryEmitNext("Hello");
        }

        @Actor
        public void subscriber() {
            sink.subscribe(target);
        }

        @Arbiter
        public void arbiter(I_Result r) {
            r.r1 = target.onNextCalls.get();
        }
    }

    @JCStressTest
    @Outcome(id = {"6, 63"}, expect = ACCEPTABLE, desc = "six subscribers, 64-1 result")
    @State
    public static class SubscriberCountStressTest extends SinksManyReplayLatestStressTest {

        AtomicInteger count = new AtomicInteger();

        @Actor
        public void first() {
            sink.tryEmitNext("Hello");
        }

        @Actor
        public void one() {
            sink.subscribe(s -> count.addAndGet(1));
        }

        @Actor
        public void two() {
            sink.subscribe(s -> count.addAndGet(2));
        }

        @Actor
        public void three() {
            sink.subscribe(s -> count.addAndGet(4));
        }

        @Actor
        public void four() {
            sink.subscribe(s -> count.addAndGet(8));
        }

        @Actor
        public void five() {
            sink.subscribe(s -> count.addAndGet(16));
        }

        @Actor
        public void six() {
            sink.subscribe(s -> count.addAndGet(32));
        }

        @Arbiter
        public void arbiter(II_Result r) {
            r.r1 = sink.currentSubscriberCount();
            r.r2 = count.get();
        }
    }


    @JCStressTest
    @Outcome(id = {"0, 0"}, expect = ACCEPTABLE, desc = "complete first")
    @Outcome(id = {"0, 1"}, expect = ACCEPTABLE, desc = "subscriber 1 before complete")
    @Outcome(id = {"0, 2"}, expect = ACCEPTABLE, desc = "subscriber 2 before complete")
    @Outcome(id = {"0, 3"}, expect = ACCEPTABLE, desc = "both subscribe before complete")
    @State
    public static class SubscriberCountCompleteStressTest extends SinksManyReplayLatestStressTest {

        AtomicInteger count = new AtomicInteger();

        @Actor
        public void first() {
            sink.tryEmitNext("Hello");
        }

        @Actor
        public void completer() {
            sink.tryEmitComplete();
        }


        @Actor
        public void one() {
            sink.subscribe(s -> count.addAndGet(1));
        }

        @Actor
        public void two() {
            sink.subscribe(s -> count.addAndGet(2));
        }

        @Arbiter
        public void arbiter(II_Result r) {
            r.r1 = sink.currentSubscriberCount();
            r.r2 = count.get();
        }
    }
}
