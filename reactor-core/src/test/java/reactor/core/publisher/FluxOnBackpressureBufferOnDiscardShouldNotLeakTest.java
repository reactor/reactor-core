package reactor.core.publisher;

public class FluxOnBackpressureBufferOnDiscardShouldNotLeakTest extends AbstractOnDiscardShouldNotLeakTest {

    public FluxOnBackpressureBufferOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    Flux<Tracked<?>> transform(Flux<Tracked<?>> upstream) {
        return upstream
                .onBackpressureBuffer();
    }
}
