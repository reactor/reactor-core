package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.*;

public class ParallelDoOnEachTest {

	@Test
	public void scanOperator(){
		ParallelFlux<Integer> parent = Flux.just(1).parallel(2);
		ParallelDoOnEach<Integer> test = new ParallelDoOnEach<>(parent, (k, v) -> {}, (k, v) -> {}, v -> {});

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Queues.SMALL_BUFFER_SIZE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}