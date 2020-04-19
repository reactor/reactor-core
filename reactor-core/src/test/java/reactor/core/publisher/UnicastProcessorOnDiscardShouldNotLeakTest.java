package reactor.core.publisher;

public class UnicastProcessorOnDiscardShouldNotLeakTest extends AbstractOnDiscardShouldNotLeakTest {

    public UnicastProcessorOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    Flux<Tracked<?>> transform(Flux<Tracked<?>> upstream) {
        return upstream
                .subscribeWith(UnicastProcessor.create());
    }
}
