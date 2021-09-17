/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package demo;

import org.junit.jupiter.api.Test;

import reactor.core.Scannable;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

/**
 * This test assumes that the transformation is applied at build time
 * and does not manually install it.
 */
public class SomeClassTest {

	@Test
	public void shouldAddAssemblyInfo() {
		//the first level is instantiated from the production code, which is instrumented
		Flux<Integer> flux = new SomeClass().obtainFlux();
		//the second level is instantiated here in test code, which isn't instrumented
		flux = flux.map(i -> i);

		Scannable scannable = Scannable.from(flux);

		assertThat(scannable.steps())
			.hasSize(2)
			.endsWith("map")
			.first(STRING).startsWith("Flux.just ⇢ at demo.SomeClass.obtainFlux(SomeClass.java:");
	}
}
