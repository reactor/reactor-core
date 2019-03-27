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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StepVerifierOptionsTest {

	@Test
	public void valueFormatterCatchAllChangesFromNoOp() {
		StepVerifierOptions options = StepVerifierOptions.create();

		assertThat(options.getValueFormatter()).isSameAs(StepVerifierOptions.OBJECT_FORMATTER_NONE);

		options.valueFormatterCatchAll(String::valueOf);

		assertThat(options.getValueFormatter()).isNotSameAs(StepVerifierOptions.OBJECT_FORMATTER_NONE);
	}

	@Test
	public void valueFormatterSpecificChangesFromNoOp() {
		StepVerifierOptions options = StepVerifierOptions.create();

		assertThat(options.getValueFormatter()).isSameAs(StepVerifierOptions.OBJECT_FORMATTER_NONE);

		options.valueFormatter(String.class, String::valueOf);

		assertThat(options.getValueFormatter()).isNotSameAs(StepVerifierOptions.OBJECT_FORMATTER_NONE);
	}

	@Test
	public void valueFormatterNoUnwrapChangesFromNoOp() {
		StepVerifierOptions options = StepVerifierOptions.create();

		assertThat(options.getValueFormatter()).isSameAs(StepVerifierOptions.OBJECT_FORMATTER_NONE);

		options.valueFormatterUnwrapSignalNext(false);

		assertThat(options.getValueFormatter()).isNotSameAs(StepVerifierOptions.OBJECT_FORMATTER_NONE);
	}

}