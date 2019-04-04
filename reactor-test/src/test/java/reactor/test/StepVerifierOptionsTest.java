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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import reactor.core.publisher.Signal;

import static org.assertj.core.api.Assertions.assertThat;

public class StepVerifierOptionsTest {

	@Test
	public void valueFormatterDefaultNull() {
		StepVerifierOptions options = StepVerifierOptions.create();

		assertThat(options.getValueFormatter()).isNull();
	}

	@Test
	public void valueFormatterCanSetNull() {
		ValueFormatters.ToStringConverter formatter = ValueFormatters.forClass(Object.class, o -> o.getClass().getSimpleName());
		final StepVerifierOptions options = StepVerifierOptions.create().valueFormatter(formatter);

		assertThat(options.getValueFormatter()).as("before remove").isSameAs(formatter);

		options.valueFormatter(null);

		assertThat(options.getValueFormatter()).as("after remove").isNull();
	}

	@Test
	public void valueFormatterSetterReplaces() {
		ValueFormatters.ToStringConverter formatter1 = ValueFormatters.forClass(Object.class, o -> o.getClass().getSimpleName());
		ValueFormatters.ToStringConverter formatter2 = ValueFormatters.forClass(Object.class, o -> o.getClass().getSimpleName());

		final StepVerifierOptions options = StepVerifierOptions.create().valueFormatter(formatter1);

		assertThat(options.getValueFormatter()).as("before replace").isSameAs(formatter1);

		options.valueFormatter(formatter2);

		assertThat(options.getValueFormatter()).as("after replace").isSameAs(formatter2);
	}

	@Test
	public void extractorReplacesExisting() {
		List<ValueFormatters.Extractor> defaults;
		List<ValueFormatters.Extractor> custom;
		StepVerifierOptions options = StepVerifierOptions.create();

		defaults = new ArrayList<>(options.getExtractors());

		options.extractor(new ValueFormatters.Extractor<Signal>() {
			@Override
			public Class<Signal> getTargetClass() {
				return Signal.class;
			}

			@Override
			public boolean matches(Signal value) {
				return false;
			}

			@Override
			public String prefix(Signal original) {
				return "signal(";
			}

			@Override
			public String suffix(Signal original) {
				return ")";
			}

			@Override
			public Stream<Object> explode(Signal original) {
				return Stream.of(original.getType());
			}
		});

		custom = new ArrayList<>(options.getExtractors());

		assertThat(defaults.stream().map(ValueFormatters.Extractor::getTargetClass))
				.as("same class-matching order")
				.containsExactlyElementsOf(custom.stream().map(ValueFormatters.Extractor::getTargetClass).collect(Collectors.toList()));

		assertThat(defaults).as("not same extractors").doesNotContainSequence(custom);
	}

}