package reactor.core.publisher;

import org.junit.jupiter.api.Test;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

class FluxDeferContextualTest {

	@Test
	void transformDeferredWithContext() {
		final Flux<String> flux = Flux
				.just("foo")
				.transformDeferredContextual((s, ctx) -> s.map(v -> v + " for " + ctx.getOrDefault("requestId", "NO ID")))
				.contextWrite(Context.of("unrelatedKey", true));

		flux.contextWrite(Context.of("requestId", "aA1"))
		    .as(StepVerifier::create)
		    .expectNext("foo for aA1")
		    .verifyComplete();

		flux.contextWrite(Context.of("requestId", "bB2"))
		    .as(StepVerifier::create)
		    .expectNext("foo for bB2")
		    .verifyComplete();

		flux.as(StepVerifier::create)
		    .expectNext("foo for NO ID")
		    .verifyComplete();
	}

	@Test
	void scanOperator(){
		FluxDeferContextual<Integer> test = new FluxDeferContextual<>(context -> Flux.just(1));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}