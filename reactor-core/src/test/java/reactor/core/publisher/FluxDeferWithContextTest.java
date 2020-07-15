package reactor.core.publisher;

import org.junit.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.*;

public class FluxDeferWithContextTest {

	@Test
	public void scanOperator(){
		FluxDeferWithContext<Integer> test = new FluxDeferWithContext<>(context -> Flux.just(1));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}