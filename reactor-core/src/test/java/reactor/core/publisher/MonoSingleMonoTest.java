package reactor.core.publisher;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

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

	@Test
	public void scanOperator(){
	    MonoSingleMono<String> test = new MonoSingleMono<>(Mono.just("foo"));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
