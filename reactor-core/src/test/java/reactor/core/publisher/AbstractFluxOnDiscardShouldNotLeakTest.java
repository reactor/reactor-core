package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.test.publisher.TestPublisher;

import java.util.Arrays;

public abstract class AbstractFluxOnDiscardShouldNotLeakTest extends AbstractOnDiscardShouldNotLeakTest {

    public AbstractFluxOnDiscardShouldNotLeakTest(boolean conditional, boolean fused) {
        super(conditional, fused);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Publisher<Tracked<?>> transform(TestPublisher<Tracked<?>> main, TestPublisher<Tracked<?>>... additional) {
        return transform(main.flux(), Arrays.stream(additional).map(TestPublisher::flux).toArray(Flux[]::new));
    }

    protected abstract Publisher<Tracked<?>> transform(Flux<Tracked<?>> main, Flux<Tracked<?>>... additional);
}
