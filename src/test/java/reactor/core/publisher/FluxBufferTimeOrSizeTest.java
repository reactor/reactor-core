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
import java.util.List;

import org.junit.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxBufferTimeOrSizeTest {

	Flux<List<Integer>> scenario_bufferWithTimeoutAccumulateOnTimeOrSize() {
		return Flux.range(1, 6)
		           .delayElements(Duration.ofMillis(300))
		           .buffer(5, Duration.ofMillis(2000));
	}

	@Test
	public void bufferWithTimeoutAccumulateOnTimeOrSize() {
		StepVerifier.withVirtualTime(this::scenario_bufferWithTimeoutAccumulateOnTimeOrSize)
		            .thenAwait(Duration.ofMillis(1500))
		            .assertNext(s -> assertThat(s).containsExactly(1, 2, 3, 4, 5))
		            .thenAwait(Duration.ofMillis(2000))
		            .assertNext(s -> assertThat(s).containsExactly(6))
		            .verifyComplete();
	}

	Flux<List<Integer>> scenario_bufferWithTimeoutAccumulateOnTimeOrSize2() {
		return Flux.range(1, 6)
		           .delayElements(Duration.ofMillis(300))
		           .bufferMillis(5, 2000);
	}

	@Test
	public void bufferWithTimeoutAccumulateOnTimeOrSize2() {
		StepVerifier.withVirtualTime(this::scenario_bufferWithTimeoutAccumulateOnTimeOrSize2)
		            .thenAwait(Duration.ofMillis(1500))
		            .assertNext(s -> assertThat(s).containsExactly(1, 2, 3, 4, 5))
		            .thenAwait(Duration.ofMillis(2000))
		            .assertNext(s -> assertThat(s).containsExactly(6))
		            .verifyComplete();
	}
}