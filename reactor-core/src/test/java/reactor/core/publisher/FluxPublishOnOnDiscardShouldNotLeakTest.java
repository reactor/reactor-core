package reactor.core.publisher;

import reactor.core.scheduler.Schedulers;

public class FluxPublishOnOnDiscardShouldNotLeakTest extends AbstractOnDiscardShouldNotLeakTest {

    public FluxPublishOnOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    Flux<Tracked<?>> transform(Flux<Tracked<?>> upstream) {
        return upstream
                .publishOn(Schedulers.immediate());
    }
}
