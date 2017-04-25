/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import reactor.test.StepVerifier;

public class MonoRetryWhenTest {


	Mono<String> exponentialRetryScenario() {
		AtomicInteger i = new AtomicInteger();
		return Mono.<String>create(s -> {
			if (i.incrementAndGet() == 4) {
				s.success("hey");
			}
			else {
				s.error(new RuntimeException("test " + i));
			}
		}).retryWhen(repeat -> repeat.zipWith(Flux.range(1, 3), (t1, t2) -> t2)
		                             .flatMap(time -> Mono.delay(Duration.ofSeconds(time))));
	}

	@Test
	public void exponentialRetry() {
		StepVerifier.withVirtualTime(this::exponentialRetryScenario)
		            .thenAwait(Duration.ofSeconds(6))
		            .expectNext("hey")
		            .expectComplete()
		            .verify();
	}
}