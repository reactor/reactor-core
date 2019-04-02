/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test;

import java.util.function.Function;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StepVerifierOptionsTest {

	@Test
	public void valueFormatterDefaultNull() {
		StepVerifierOptions options = StepVerifierOptions.create();

		assertThat(options.getValueFormatter()).isNull();
	}

	@Test
	public void valueFormatterCanSetNull() {
		Function<Object, String> formatter = ValueFormatters.forClass(Object.class, o -> o.getClass().getSimpleName());
		final StepVerifierOptions options = StepVerifierOptions.create().valueFormatter(formatter);

		assertThat(options.getValueFormatter()).as("before remove").isSameAs(formatter);

		options.valueFormatter(null);

		assertThat(options.getValueFormatter()).as("after remove").isNull();
	}

	@Test
	public void valueFormatterSetterReplaces() {
		Function<Object, String> formatter1 = ValueFormatters.forClass(Object.class, o -> o.getClass().getSimpleName());
		Function<Object, String> formatter2 = ValueFormatters.forClass(Object.class, o -> o.getClass().getSimpleName());

		final StepVerifierOptions options = StepVerifierOptions.create().valueFormatter(formatter1);

		assertThat(options.getValueFormatter()).as("before replace").isSameAs(formatter1);

		options.valueFormatter(formatter2);

		assertThat(options.getValueFormatter()).as("after replace").isSameAs(formatter2);
	}

}