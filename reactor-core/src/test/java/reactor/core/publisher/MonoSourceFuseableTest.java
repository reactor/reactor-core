package reactor.core.publisher;

import org.junit.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSourceFuseableTest {

	@Test
	public void scanOperator(){
		Mono<String> source = Mono.just("foo");
		MonoSourceFuseable<String> test = new MonoSourceFuseable(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}