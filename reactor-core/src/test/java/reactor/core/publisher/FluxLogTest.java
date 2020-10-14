package reactor.core.publisher;


import org.junit.jupiter.api.Test;
import reactor.core.Scannable;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxLogTest {

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		SignalLogger<Integer> log = new SignalLogger<>(parent, "category", Level.INFO, true);
		FluxLog<Integer> test = new FluxLog<>(parent, log);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableOperator(){
		Flux<Integer> parent = Flux.just(1);
		SignalLogger<Integer> log = new SignalLogger<>(parent, "category", Level.INFO, true);
		FluxLogFuseable<Integer> test = new FluxLogFuseable<>(parent, log);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}