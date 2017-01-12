/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.function.Tuple2;

public class FluxCacheTest {

	@Test
	public void cacheFlux() {
		try {
			VirtualTimeScheduler vts = VirtualTimeScheduler.enable(false);

			Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
			                                         .delayMillis(1000)
			                                         .cache()
			                                         .elapsed();

			StepVerifier.create(source)
			            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
			            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
			            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
			            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
			            .verifyComplete();

			StepVerifier.create(source)
			            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
			            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 1)
			            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
			            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
			            .verifyComplete();
		}
		finally {
			VirtualTimeScheduler.reset();
		}
	}

	@Test
	public void cacheFluxTTL() {
		try {
			VirtualTimeScheduler vts = VirtualTimeScheduler.enable(false);

			Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
			                                         .delayMillis(1000)
			                                         .cache(Duration.ofMillis(2000))
			                                         .elapsed();

			StepVerifier.create(source)
			            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
			            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
			            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
			            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
			            .verifyComplete();

			StepVerifier.create(source)
			            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
			            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
			            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
			            .verifyComplete();
		}
		finally {
			VirtualTimeScheduler.reset();
		}
	}

	@Test
	public void cacheFluxHistoryTTL() {
		try {
			VirtualTimeScheduler vts = VirtualTimeScheduler.enable(false);

			Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
			                                         .delayMillis(1000)
			                                         .cache(2, Duration.ofMillis(2000))
			                                         .elapsed();

			StepVerifier.create(source)
			            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
			            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
			            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
			            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
			            .verifyComplete();

			StepVerifier.create(source)
			            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
			            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
			            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
			            .verifyComplete();
		}
		finally {
			VirtualTimeScheduler.reset();
		}
	}
}
