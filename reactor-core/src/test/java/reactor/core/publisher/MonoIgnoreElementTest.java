package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoIgnoreElementTest {

    @Test
    public void scanOperator(){
        MonoIgnoreElement<Integer> test = new MonoIgnoreElement<>(Mono.just(1));

        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

}