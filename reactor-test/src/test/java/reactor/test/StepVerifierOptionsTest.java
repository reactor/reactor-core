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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.Test;

import reactor.core.publisher.Signal;
import reactor.test.ValueFormatters.Extractor;

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
	public void extractorsDefaultAtEnd() {
		StepVerifierOptions options = StepVerifierOptions.create();

		options.extractor(new Extractor<String>() {
			@Override
			public Class<String> getTargetClass() {
				return String.class;
			}

			@Override
			public Stream<Object> explode(String original) {
				return Arrays.stream(original.split(" "));
			}
		});

		assertThat(options.getExtractors()
		                  .stream()
		                  .map(e -> e.getTargetClass().getSimpleName()))
				.containsExactly("String", "Signal", "Iterable", "Object[]");
	}

	@Test
	public void extractorReplacingDefaultMovesUp() {
		StepVerifierOptions options = StepVerifierOptions.create();

		options.extractor(new Extractor<Signal>() {
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
		options.extractor(new Extractor<String>() {
			@Override
			public Class<String> getTargetClass() {
				return String.class;
			}

			@Override
			public Stream<Object> explode(String original) {
				return Arrays.stream(original.split(" "));
			}
		});

		assertThat(options.getExtractors()
		                  .stream()
		                  .map(e -> e.getTargetClass().getSimpleName()))
				.containsExactly("Signal", "String", "Iterable", "Object[]");
	}

	@Test
	public void extractorReplacingCustomInPlace() {
		StepVerifierOptions options = StepVerifierOptions.create();
		Extractor<String> extractorV1 = new Extractor<String>() {
			@Override
			public Class<String> getTargetClass() {
				return String.class;
			}

			@Override
			public Stream<Object> explode(String original) {
				return Arrays.stream(original.split(" "));
			}
		};
		Extractor<String> extractorV2 = new Extractor<String>() {
			@Override
			public Class<String> getTargetClass() {
				return String.class;
			}

			@Override
			public Stream<Object> explode(String original) {
				return Arrays.stream(original.split(""));
			}
		};

		options.extractor(extractorV1)
		       .extractor(extractorV2);

		assertThat(options.getExtractors()
		                  .stream()
		                  .map(e -> e.getTargetClass().getSimpleName()))
				.containsExactly("String", "Signal", "Iterable", "Object[]");

		assertThat(options.getExtractors())
				.first()
				.isSameAs(extractorV2);
	}

	@Test
	public void getExtractorsIsCopy() {
		StepVerifierOptions options = StepVerifierOptions.create();
		options.extractor(new Extractor<String>() {
			@Override
			public Class<String> getTargetClass() {
				return String.class;
			}

			@Override
			public Stream<Object> explode(String original) {
				return Arrays.stream(original.split(" "));
			}
		});

		Collection<Extractor<?>> extractors1 = options.getExtractors();
		Collection<Extractor<?>> extractors2 = options.getExtractors();

		assertThat(extractors1).isNotSameAs(extractors2);

		extractors1.clear();

		assertThat(extractors1).isEmpty();
		assertThat(extractors2).isNotEmpty();
	}

}