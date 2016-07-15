package reactor.core.publisher;

import org.junit.Test;

import reactor.test.TestSubscriber;

public class MonoThenApplyTest {

    @Test
    public void normalHidden() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        Mono.just(1).hide().then(v -> Mono.just(2).hide()).subscribe(ts);
        
        ts.assertValues(2)
        .assertComplete()
        .assertNoError();
    }
}
