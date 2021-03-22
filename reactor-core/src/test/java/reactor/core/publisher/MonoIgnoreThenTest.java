package reactor.core.publisher;


import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoIgnoreThenTest {

    @Test
    public void scanOperator() {
        MonoIgnoreThen<String> test = new MonoIgnoreThen<>(new Publisher[]{Mono.just("foo")}, Mono.just("bar"));

        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

    @Test
    public void scanThenIgnoreMain() {
        AssertSubscriber<String> actual = new AssertSubscriber<>();
        MonoIgnoreThen.ThenIgnoreMain<String> test = new MonoIgnoreThen.ThenIgnoreMain<>(actual, new Publisher[]{Mono.just("foo")}, Mono.just("bar"));

        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

}