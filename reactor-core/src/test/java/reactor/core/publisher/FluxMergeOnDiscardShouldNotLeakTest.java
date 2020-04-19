package reactor.core.publisher;

import reactor.util.concurrent.Queues;

public class FluxMergeOnDiscardShouldNotLeakTest extends AbstractOnDiscardShouldNotLeakTest {

    public FluxMergeOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    Flux<Tracked<?>> transform(Flux<Tracked<?>> upstream) {
        Flux<Tracked<?>>[] inners = new Flux[subscriptionsNumber()];
        for (int i = 0; i < subscriptionsNumber(); i++) {
            inners[i] = upstream;
        }
        return Flux.merge(inners);
    }

    @Override
    int subscriptionsNumber() {
        return Queues.XS_BUFFER_SIZE;
    }
}
