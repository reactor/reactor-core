package reactor.core.publisher;

import org.junit.Test;

import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

public class MonoFlatMapTest {

    @Test
    public void normalHidden() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();
        
        Mono.just(1).hide().flatMap(v -> Mono.just(2).hide()).subscribe(ts);
        
        ts.assertValues(2)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void cancel() {
        TestPublisher<String> cancelTester = TestPublisher.create();

        MonoProcessor<Integer> processor = cancelTester.mono()
                                                       .flatMap(s -> Mono.just(s.length()))
                                                       .toProcessor();
        processor.subscribe();
        processor.cancel();

        cancelTester.assertCancelled();
    }
}
