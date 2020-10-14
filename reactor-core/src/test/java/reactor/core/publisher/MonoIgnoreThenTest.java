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

    @Test
    public void scanThenIgnoreInner() {
        AssertSubscriber<String> actual = new AssertSubscriber<>();
        MonoIgnoreThen.ThenIgnoreMain<String> main =
                new MonoIgnoreThen.ThenIgnoreMain<>(actual, new Publisher[]{Mono.just("foo")}, Mono.just("bar"));
        MonoIgnoreThen.ThenIgnoreInner test = new MonoIgnoreThen.ThenIgnoreInner(main);

        Subscription innerSubscription = Operators.emptySubscription();
        test.onSubscribe(innerSubscription);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(innerSubscription);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }


    @Test
    public void scanThenAcceptInner() {
        AssertSubscriber<String> actual = new AssertSubscriber<>();
        MonoIgnoreThen.ThenIgnoreMain<String> main =
                new MonoIgnoreThen.ThenIgnoreMain<>(actual, new Publisher[]{Mono.just("foo")}, Mono.just("bar"));
        MonoIgnoreThen.ThenAcceptInner<String> test = new MonoIgnoreThen.ThenAcceptInner<>(main);

        Subscription innerSubscription = Operators.emptySubscription();
        test.onSubscribe(innerSubscription);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(innerSubscription);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

}