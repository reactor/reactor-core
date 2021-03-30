package reactor.core.publisher;

import java.util.NoSuchElementException;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import reactor.core.Fuseable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class MonoSingleMonoTest {

	@Test
	public void callableEmpty() {
		StepVerifier.create(Mono.empty().single())
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(NoSuchElementException.class)
				            .hasMessage("Source was a (constant) empty"));
	}

	@Test
	public void callableValued() {
		StepVerifier.create(Mono.just("foo").single())
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void normalEmpty() {
		StepVerifier.create(Mono.empty().hide().single())
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(NoSuchElementException.class)
				            .hasMessage("Source was empty"));
	}

	@Test
	public void normalValued() {
		StepVerifier.create(Mono.just("foo").hide().single())
		            .expectNext("foo")
		            .verifyComplete();
	}

	// see https://github.com/reactor/reactor-core/issues/2663
	@Test
	void fusionMonoSingleMonoDoesntTriggerFusion() {
		Mono<Integer> fusedCase = Mono
				.just(1)
				.map(Function.identity())
				.single();

		assertThat(fusedCase)
				.as("fusedCase assembly check")
				.isInstanceOf(MonoSingleMono.class)
				.isNotInstanceOf(Fuseable.class);

		assertThatCode(() -> fusedCase.filter(v -> true).block())
				.as("fusedCase fused")
				.doesNotThrowAnyException();
	}
}
