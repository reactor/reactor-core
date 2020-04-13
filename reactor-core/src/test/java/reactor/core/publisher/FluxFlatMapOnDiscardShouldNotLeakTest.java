package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public class FluxFlatMapOnDiscardShouldNotLeakTest extends AbstractOnDiscardShouldNotLeakTest {

    @Override
    Publisher<Tracked<?>> transform(Flux<Tracked<?>> upstream) {
        return upstream
                .flatMap(f -> Mono.just(f).hide().flux());
    }
}
