/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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

package reactor.tools.agent;

import org.junit.Test;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorDebugAgentTest {

	static {
		ReactorDebugAgent.init();

		// Since ReactorDebugAgentTest is already loaded, we need to re-process it
		ReactorDebugAgent.processExistingClasses();
	}

	@Test
	public void shouldAddAssemblyInfo() {
		int baseline = getBaseline();
		Flux<Integer> flux = Flux.just(1);

		assertThat(Scannable.from(flux).stepName())
				.startsWith("Flux.just ⇢ reactor.tools.agent.ReactorDebugAgentTest.shouldAddAssemblyInfo(ReactorDebugAgentTest.java:" + (baseline + 1));
	}

	@Test
	public void shouldWorkWithMethodChains() {
		int baseline = getBaseline();
		Flux<Integer> flux = Flux.just(1).map(it -> it);

		assertThat(Scannable.from(flux).stepName())
				.startsWith("Flux.map ⇢ reactor.tools.agent.ReactorDebugAgentTest.shouldWorkWithMethodChains(ReactorDebugAgentTest.java:" + (baseline + 1));
	}

	private int getBaseline() {
		return new Exception().getStackTrace()[1].getLineNumber();
	}
}