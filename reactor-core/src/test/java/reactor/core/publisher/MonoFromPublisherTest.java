package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoFromPublisherTest {

	@Test
	public void scanOperator(){
		MonoFromPublisher<String> test = new MonoFromPublisher<>(Flux.just("foo", "bar"));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}