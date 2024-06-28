package reactor.core.publisher;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;


import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;

public class SinksManyReplayLatestStressTest {

    @JCStressTest
    @Outcome(id = {"6"},    expect = ACCEPTABLE, desc = "all signals go through")
    @Outcome(               expect = ACCEPTABLE_INTERESTING, desc = "Signals are lost before replay")
    @State
    public static class FluxReplaySizeBoundWriteStressTest {
        final StressSubscriber<String> target = new StressSubscriber<>();

        final SinkManyReplayProcessor<String> sink = new SinkManyReplayProcessor<>(
                new FluxReplay.ArraySizeBoundReplayBuffer<>(1)
        );

        public FluxReplaySizeBoundWriteStressTest() {
            // subscribe before start
            sink.subscribe(target);
        }


        @Actor
        public void one() {
            sink.tryEmitNext("Hello");
        }

        @Actor
        public void two() {
            sink.tryEmitNext("Hello");
        }

        @Actor
        public void three() {
            sink.tryEmitNext("Hello");
        }

        @Actor
        public void four() {
            sink.tryEmitNext("Hello");
        }

        @Actor
        public void five() {
            sink.tryEmitNext("Hello");
        }

        @Actor
        public void six() {
            sink.tryEmitNext("Hello");
        }

        @Arbiter
        public void arbiter(I_Result r) {
            r.r1 = target.onNextCalls.get();
        }
    }
}
