package reactor.test;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class SpotlessDemonstrationTest {

	/*
	 * There are several whitespace issues with this class:
	 *  - tests are separated with variations of blank lines that contains whitespace
	 *  - some indentation has spaces in multiples of 4, which should be replaced with tabs
	 *  - there are unused imports
	 *  - the file doesn't have a license header and doesn't end with a blank line
	 */


	@Test
	void withTooMuchAlignmentSpaces() {
		Mono.defer(() -> Mono.just(1)
		                     .doOnTerminate(() -> { } ))
			.subscribe();
	}

	@Test
	void withASingleExtraAlignmentSpaces() {
		Mono.defer(() -> Mono.just(1)
							 .doOnTerminate(() -> { } ))
			.subscribe();
	}

	@Test
	void keepsSpacesAsIsAfterActualStartOfLine() {
		Mono.defer(() -> Mono.just(1)
							 .doOnTerminate(() -> {    } ))
			.subscribe();
		final String      var1 = "";
		final String variable2 = "";
	}
}