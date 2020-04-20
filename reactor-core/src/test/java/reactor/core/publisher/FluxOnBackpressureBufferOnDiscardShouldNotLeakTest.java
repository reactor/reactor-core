package reactor.core.publisher;

import org.reactivestreams.Publisher;

public class FluxOnBackpressureBufferOnDiscardShouldNotLeakTest extends AbstractFluxOnDiscardShouldNotLeakTest {

    public FluxOnBackpressureBufferOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    protected Publisher<Tracked<?>> transform(Flux<Tracked<?>> main, Flux<Tracked<?>>... additional) {
        return main
                .onBackpressureBuffer();
    }
}
