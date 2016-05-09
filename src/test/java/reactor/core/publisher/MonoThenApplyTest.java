package reactor.core.publisher;

import org.junit.Test;

import reactor.core.test.TestSubscriber;

public class MonoThenApplyTest {

    @Test
    public void normalHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Mono.just(1).hide().then(v -> Mono.just(2).hide()).subscribe(ts);
        
        ts.assertValues(2)
        .assertComplete()
        .assertNoError();
    }
}
