package reactor.core.publisher.tck;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

@Test
public class FluxSwitchOnFirstVerification extends PublisherVerification<Integer> {

    public FluxSwitchOnFirstVerification() {
        super(new TestEnvironment());
    }

    @Override
    public Publisher<Integer> createPublisher(long elements) {
        return Flux.range(0, Integer.MAX_VALUE < elements ? Integer.MAX_VALUE : (int) elements)
                   .switchOnFirst((first, innerFlux) -> innerFlux);
    }

    @Override
    public Publisher<Integer> createFailedPublisher() {
        return Flux.<Integer>error(new RuntimeException())
                   .switchOnFirst((first, innerFlux) -> innerFlux);
    }

}