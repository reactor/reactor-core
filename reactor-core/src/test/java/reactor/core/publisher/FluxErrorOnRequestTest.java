package reactor.core.publisher;

import org.junit.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxErrorOnRequestTest {

	@Test
	public void scanOperator(){
	    FluxErrorOnRequest test = new FluxErrorOnRequest(new IllegalStateException());

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}