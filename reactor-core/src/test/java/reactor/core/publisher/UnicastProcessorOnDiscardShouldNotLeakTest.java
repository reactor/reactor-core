package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.scheduler.Schedulers;

public class UnicastProcessorOnDiscardShouldNotLeakTest extends AbstractOnDiscardShouldNotLeakTest {

    @Override
    Publisher<Tracked<?>> transform(Flux<Tracked<?>> upstream) {
        return upstream
                .subscribeWith(UnicastProcessor.create());
    }
}
