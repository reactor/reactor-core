/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
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

package reactor.tools.agent;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test assumes that the transformation is applied at build time
 * and does not manually install it.
 */
public class ReactorDebugBuildPluginTest {

	@Test
	public void shouldAddAssemblyInfo() {
		int baseline = getBaseline();
		Flux<Integer> flux = Flux.just(1);

		assertThat(Scannable.from(flux).stepName())
				.startsWith("Flux.just ⇢ at reactor.tools.agent.ReactorDebugBuildPluginTest.shouldAddAssemblyInfo(ReactorDebugBuildPluginTest.java:" + (baseline + 1));
	}

	private static int getBaseline() {
		return new Exception().getStackTrace()[1].getLineNumber();
	}
}
