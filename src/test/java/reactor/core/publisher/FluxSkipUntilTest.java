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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxSkipUntilTest extends FluxOperatorTest<String, String> {

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(scenario(f -> f.skipUntil(d -> {
					throw exception();
				})
				)
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.skipUntil(item(1)::equals))
				.receiveValues(item(1), item(2))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(scenario(f -> f.skipUntil(item(1)::equals)));
	}

	@Test
	public void normalHidden() {
		StepVerifier.create(Flux.range(1, 10)
		                        .hide()
		                        .skipUntil(v -> v > 4))
		            .expectNext(5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}

	@Test
	public void normal() {
		StepVerifier.create(Flux.range(1, 10)
		                        .skipUntil(v -> v > 4))
		            .expectNext(5, 6, 7, 8, 9, 10)
		            .verifyComplete();
	}
}