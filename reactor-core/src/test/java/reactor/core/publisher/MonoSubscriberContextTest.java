package reactor.core.publisher;


import org.junit.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSubscriberContextTest {

    @Test
    public void scanOperator(){
        MonoSubscriberContext<String> test = new MonoSubscriberContext(Mono.just(1), c -> c);

        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }
}