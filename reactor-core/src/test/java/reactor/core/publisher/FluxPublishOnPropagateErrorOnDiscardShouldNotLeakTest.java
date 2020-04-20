package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public class FluxPublishOnPropagateErrorOnDiscardShouldNotLeakTest extends AbstractFluxOnDiscardShouldNotLeakTest {

    public FluxPublishOnPropagateErrorOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    protected Publisher<Tracked<?>> transform(Flux<Tracked<?>> main, Flux<Tracked<?>>... additional) {
        return main
                .publishOn(Schedulers.immediate(), false, Queues.SMALL_BUFFER_SIZE);
    }
}
