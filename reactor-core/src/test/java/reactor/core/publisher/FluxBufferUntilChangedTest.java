package reactor.core.publisher;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import reactor.test.StepVerifier;

public class FluxBufferUntilChangedTest {

	@Test
	public void noRepetition() {
		StepVerifier.create(Flux.range(0, 5)
		                        .bufferUntilChanged())
		            .expectSubscription()
		            .expectNext(Collections.singletonList(0))
		            .expectNext(Collections.singletonList(1))
		            .expectNext(Collections.singletonList(2))
		            .expectNext(Collections.singletonList(3))
		            .expectNext(Collections.singletonList(4))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void withKeySelector() {
		StepVerifier.create(Flux.range(0, 5)
		                        .bufferUntilChanged(i -> i / 2))
		            .expectSubscription()
		            .expectNext(Arrays.asList(0, 1))
		            .expectNext(Arrays.asList(2, 3))
		            .expectNext(Collections.singletonList(4))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void someRepetition() {
		StepVerifier.create(Flux.just(1, 1, 2, 2, 3, 3, 1)
		                        .bufferUntilChanged())
		            .expectSubscription()
		            .expectNext(Arrays.asList(1, 1))
		            .expectNext(Arrays.asList(2, 2))
		            .expectNext(Arrays.asList(3, 3))
		            .expectNext(Arrays.asList(1))
		            .expectComplete()
		            .verify();
	}


}