package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.scheduler.Schedulers;

public class FluxPublishOnOnDiscardShouldNotLeakTest extends AbstractFluxOnDiscardShouldNotLeakTest {

    public FluxPublishOnOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    protected Publisher<Tracked<?>> transform(Flux<Tracked<?>> main, Flux<Tracked<?>>... additional) {
        return main
                .publishOn(Schedulers.immediate());
    }
}
