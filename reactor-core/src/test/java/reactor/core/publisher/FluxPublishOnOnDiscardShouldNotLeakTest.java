package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.scheduler.Schedulers;

public class FluxPublishOnOnDiscardShouldNotLeakTest extends AbstractOnDiscardShouldNotLeakTest {

    @Override
    Publisher<Tracked<?>> transform(Flux<Tracked<?>> upstream) {
        return upstream
                .publishOn(Schedulers.immediate());
    }
}
