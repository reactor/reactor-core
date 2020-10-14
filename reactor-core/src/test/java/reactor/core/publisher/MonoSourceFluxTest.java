package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSourceFluxTest {

	@Test
	public void scanOperator(){
		Flux<String> source = Flux.just("foo", "bar");
		MonoSourceFlux<String> test = new MonoSourceFlux<>(source);

	    assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableOperator(){
		Flux<String> source = Flux.just("foo", "bar");
		MonoSourceFluxFuseable<String> test = new MonoSourceFluxFuseable<>(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}