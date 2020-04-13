package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.util.concurrent.Queues;

public class FluxMergeOnDiscardShouldNotLeakTest extends AbstractOnDiscardShouldNotLeakTest {

    @Override
    Publisher<Tracked<?>> transform(Flux<Tracked<?>> upstream) {
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
