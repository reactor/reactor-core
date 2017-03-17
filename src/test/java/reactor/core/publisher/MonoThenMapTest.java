package reactor.core.publisher;

import org.junit.Test;

import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

public class MonoThenMapTest {

    @Test
    public void normalHidden() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();
        
        Mono.just(1).hide().then(v -> Mono.just(2).hide()).subscribe(ts);
        
        ts.assertValues(2)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void cancel() {
        TestPublisher<String> cancelTester = TestPublisher.create();

        cancelTester.mono()
                    .then(s -> Mono.just(s.length()))
                    .subscribe()
                    .cancel();

        cancelTester.assertCancelled();
    }
}
