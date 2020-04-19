package reactor.core.publisher;

import java.util.Arrays;

public class FluxFlatMapIterableOnDiscardShouldNotLeakTest extends AbstractOnDiscardShouldNotLeakTest {

    public FluxFlatMapIterableOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    Flux<Tracked<?>> transform(Flux<Tracked<?>> upstream) {
        return upstream
                .flatMapIterable(Arrays::asList);
    }
}
