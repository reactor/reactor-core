/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class StepVerifierDefaultTimeoutTests {

	@BeforeAll
	public static void init() {
		StepVerifier.setDefaultTimeout(Duration.ofMillis(100));
	}

	@AfterAll
	public static void clean() {
		StepVerifier.resetDefaultTimeout();
	}

	@Test
	public void verifyUsesDefaultTimeout() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() ->
						StepVerifier.create(Mono.delay(Duration.ofMillis(150)))
						            .expectComplete()
						            .verify())
				.withMessageStartingWith("VerifySubscriber timed out");
	}

	@Test
	public void verifyThenAssertUsesDefaultTimeout() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() ->
						StepVerifier.create(Mono.delay(Duration.ofMillis(150)))
						            .expectComplete()
						            .verifyThenAssertThat())
				.withMessageStartingWith("VerifySubscriber timed out");
	}

	@Test
	public void verifyCompleteUsesDefaultTimeout() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() ->
						StepVerifier.create(Mono.delay(Duration.ofMillis(150)))
						            .verifyComplete())
				.withMessageStartingWith("VerifySubscriber timed out");
	}

	@Test
	public void verifyErrorUsesDefaultTimeout() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() ->
						StepVerifier.create(Mono.delay(Duration.ofMillis(150)))
						            .verifyError())
				.withMessageStartingWith("VerifySubscriber timed out");
	}

	@Test
	public void verifyErrorMessageUsesDefaultTimeout() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() ->
						StepVerifier.create(Mono.delay(Duration.ofMillis(150)))
						            .verifyErrorMessage("ignored"))
				.withMessageStartingWith("VerifySubscriber timed out");
	}

	@Test
	public void verifyErrorMatchesUsesDefaultTimeout() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() ->
						StepVerifier.create(Mono.delay(Duration.ofMillis(150)))
						            .verifyErrorMatches(ignore -> true))
				.withMessageStartingWith("VerifySubscriber timed out");
	}

	@Test
	public void verifyErrorClassUsesDefaultTimeout() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() ->
						StepVerifier.create(Mono.delay(Duration.ofMillis(150)))
						            .verifyError(NullPointerException.class))
				.withMessageStartingWith("VerifySubscriber timed out");
	}
}
