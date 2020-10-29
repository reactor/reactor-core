/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.publisher;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

public class FluxTimestampTest {

	Flux<Tuple2<Long, String>> scenario_aFluxCanBeTimestamped(){
		return Flux.just("test")
		           .timestamp();
	}

	@Test
	public void aFluxCanBeTimestamped(){
		StepVerifier.withVirtualTime(this::scenario_aFluxCanBeTimestamped, 0)
		            .thenAwait(Duration.ofSeconds(2))
		            .thenRequest(1)
		            .expectNextMatches(t -> t.getT1() == 2000 && t.getT2().equals("test"))
		            .verifyComplete();
	}
}
