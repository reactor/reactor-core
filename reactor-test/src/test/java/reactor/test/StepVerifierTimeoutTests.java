/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.test;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class StepVerifierTimeoutTests {

	@Test
	public void verifyThenAssert_failsAfterCustomTimeout() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() ->
						StepVerifier.create(Mono.delay(Duration.ofMillis(150)))
						            .expectComplete()
						            .verifyThenAssertThat(Duration.ofMillis(50)))
				.withMessageStartingWith("VerifySubscriber timed out");
	}

	@Test
	public void verifyThenAssertUsesCustomTimeout() {
		try {
			StepVerifier.setDefaultTimeout(Duration.ofMillis(50));

			Duration longerThanDefaultTimeout = Duration.ofMillis(150);
			StepVerifier.create(Mono.delay(longerThanDefaultTimeout))
			            .expectNext(0L)
			            .expectComplete()
			            .verifyThenAssertThat(Duration.ofMillis(250));
		} finally {
			StepVerifier.resetDefaultTimeout();
		}
	}
}
