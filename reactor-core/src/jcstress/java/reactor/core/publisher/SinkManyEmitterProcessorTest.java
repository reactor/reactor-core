package reactor.core.publisher;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.I_Result;
import org.openjdk.jcstress.infra.results.LI_Result;

import static org.openjdk.jcstress.annotations.Expect.*;

public class SinkManyEmitterProcessorTest {

    @JCStressTest
    @Outcome(id = {"0"}, expect = ACCEPTABLE, desc = "Queue is empty")
    @Outcome(id = {"1"}, expect = ACCEPTABLE_INTERESTING, desc = "Queue is not empty")
    @State
    public static class ParallelSubscribersStressTest {

        final SinkManyEmitterProcessor<Integer> sink = new SinkManyEmitterProcessor(true, 1);

        @Actor
        public void subscribeAndCancel() {
            sink.subscribe().dispose();
        }

        @Actor
        public void tryEmitNext1(LI_Result r) {
            sink.tryEmitNext(1);
        }

        @Arbiter
        public void arbiter(I_Result result) {
            result.r1 = sink.queue.size();
        }
    }
}
