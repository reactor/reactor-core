package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxSourceMonoTest {

    @Test
    public void scanOperatorWithSyncSource(){
        Mono<String> source = Mono.just("Foo");
        FluxSourceMono<String> test = new FluxSourceMono<>(source);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

    @Test
    public void scanOperatorWithAsyncSource(){
        MonoDelayElement<String> source = new MonoDelayElement<>(Mono.empty(), 1, TimeUnit.SECONDS, Schedulers.immediate());

        FluxSourceMono<String> test = new FluxSourceMono<>(source);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
    }

    @Test
    public void scanFuseableOperatorWithSyncSource(){
        Mono<String> source = Mono.just("Foo");
        FluxSourceMonoFuseable<String> test = new FluxSourceMonoFuseable<>(source);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

    @Test
    public void scanFuseableOperatorWithAsyncSource(){
        MonoDelayElement<String> source = new MonoDelayElement<>(Mono.empty(), 1, TimeUnit.SECONDS, Schedulers.immediate());

        FluxSourceMonoFuseable<String> test = new FluxSourceMonoFuseable<>(source);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
    }

}