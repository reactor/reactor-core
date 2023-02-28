/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;

import io.micrometer.context.ContextRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

public class FluxRetryWhenTest {

	//https://github.com/reactor/reactor-core/issues/3361
	@Test
	void ensuresContextPropagationIsAvailableWithinRetry() {
		ContextRegistry globalRegistry = ContextRegistry.getInstance();

		globalRegistry.registerThreadLocalAccessor("MDC",
				MDC::getCopyOfContextMap,
				MDC::setContextMap,
				MDC::clear);
		Hooks.enableAutomaticContextPropagation();

		String givenContextKey = "contextKey";
		String givenContextValue = "contextValue";

		MDC.put(givenContextKey, givenContextValue);

		Exception expectedException = new Exception("expectedException");
		Exception tooManyRetriesException = new Exception("tooManyRetriesException");
		Mono.error(expectedException)
		    .retryWhen(Retry.backoff(2, Duration.ofMillis(1))
		                    .filter(t -> {
			                    Assertions.assertThat(MDC.get(givenContextKey))
			                              .isEqualTo(givenContextValue);
			                    return false;
		                    })
		                    .doBeforeRetry(signal -> Assertions.assertThat(MDC.get(
				                                                       givenContextKey))
		                                                       .isEqualTo(
				                                                       givenContextValue))
		                    .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> tooManyRetriesException))
		    .onErrorComplete(expectedException::equals)
		    .contextCapture()
		    .as(StepVerifier::create)
		    .verifyComplete();
	}
}
