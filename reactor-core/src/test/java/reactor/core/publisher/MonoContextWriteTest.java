package reactor.core.publisher;


import org.junit.jupiter.api.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoContextWriteTest {

    @Test
    public void scanOperator(){
        MonoContextWrite<Integer> test = new MonoContextWrite<>(Mono.just(1), c -> c);

        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }
}