package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.util.concurrent.Queues;

public class FluxMergeOnDiscardShouldNotLeakTest extends AbstractFluxOnDiscardShouldNotLeakTest {

    public FluxMergeOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    protected Publisher<Tracked<?>> transform(Flux<Tracked<?>> main, Flux<Tracked<?>>... additional) {
        Flux<Tracked<?>>[] inners = new Flux[subscriptionsNumber()];
        inners[0] = main;
        for (int i = 1; i < subscriptionsNumber(); i++) {
            inners[i] = additional[i - 1];
        }
        return Flux.merge(inners);
    }

    @Override
    int subscriptionsNumber() {
        return 4;
    }
}
