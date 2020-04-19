package reactor.core.publisher;

public class FluxFlatMapOnDiscardShouldNotLeakTest extends AbstractOnDiscardShouldNotLeakTest {

    public FluxFlatMapOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    Flux<Tracked<?>> transform(Flux<Tracked<?>> upstream) {
        return upstream
                .flatMap(f -> Mono.just(f).hide().flux());
    }
}
