/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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

import static org.assertj.core.api.Assertions.assertThat;

public class MonoCacheTimeTest {

	@Test
	public void expireAfterTtlNormal() throws InterruptedException {
		AtomicInteger subCount = new AtomicInteger();
		Mono<Integer> source = Mono.defer(() -> Mono.just(subCount.incrementAndGet()));

		Mono<Integer> cached = source.cache(Duration.ofMillis(100))
		                             .hide();

		StepVerifier.create(cached)
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .as("first subscription caches 1")
		            .verifyComplete();

		Thread.sleep(110);

		StepVerifier.create(cached)
		            .expectNext(2)
		            .as("cached value should expire")
		            .verifyComplete();
	}

	@Test
	public void doesntResubscribeNormal() {
		AtomicInteger subCount = new AtomicInteger();
		Mono<Integer> source = Mono.defer(() -> Mono.just(subCount.incrementAndGet()));

		Mono<Integer> cached = source.cache(Duration.ofMillis(100))
		                             .hide();

		StepVerifier.create(cached)
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .as("first subscription caches 1")
		            .verifyComplete();

		StepVerifier.create(cached)
		            .expectNext(1)
		            .as("second subscription uses cache")
		            .verifyComplete();
	}

}