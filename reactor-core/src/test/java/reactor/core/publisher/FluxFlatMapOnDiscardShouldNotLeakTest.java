package reactor.core.publisher;

import org.reactivestreams.Publisher;

public class FluxFlatMapOnDiscardShouldNotLeakTest extends AbstractFluxOnDiscardShouldNotLeakTest {

    public FluxFlatMapOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    protected Publisher<Tracked<?>> transform(Flux<Tracked<?>> main, Flux<Tracked<?>>... additional) {
        return main
                .flatMap(f -> Mono.just(f).hide().flux());
    }
}
