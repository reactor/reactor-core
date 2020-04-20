package reactor.core.publisher;

import org.reactivestreams.Publisher;

public class FluxFlatMapInnerOnDiscardShouldNotLeakTest extends AbstractFluxOnDiscardShouldNotLeakTest {

    public FluxFlatMapInnerOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    protected Publisher<Tracked<?>> transform(Flux<Tracked<?>> main, Flux<Tracked<?>>... additional) {
        return Flux.just(1)
                .flatMap(f -> main);
    }
}
