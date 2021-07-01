/*
 * Copyright (c) 2019-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.test;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Signal;
import reactor.test.ValueFormatters.Extractor;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class StepVerifierOptionsTest {

	@Test
	void valueFormatterDefaultNull() {
		StepVerifierOptions options = StepVerifierOptions.create();

		assertThat(options.getValueFormatter()).isNull();
	}

	@Test
	void valueFormatterCanSetNull() {
		ValueFormatters.ToStringConverter formatter = ValueFormatters.forClass(Object.class, o -> o.getClass().getSimpleName());
		final StepVerifierOptions options = StepVerifierOptions.create().valueFormatter(formatter);

		assertThat(options.getValueFormatter()).as("before remove").isSameAs(formatter);

		options.valueFormatter(null);

		assertThat(options.getValueFormatter()).as("after remove").isNull();
	}

	@Test
	void valueFormatterSetterReplaces() {
		ValueFormatters.ToStringConverter formatter1 = ValueFormatters.forClass(Object.class, o -> o.getClass().getSimpleName());
		ValueFormatters.ToStringConverter formatter2 = ValueFormatters.forClass(Object.class, o -> o.getClass().getSimpleName());

		final StepVerifierOptions options = StepVerifierOptions.create().valueFormatter(formatter1);

		assertThat(options.getValueFormatter()).as("before replace").isSameAs(formatter1);

		options.valueFormatter(formatter2);

		assertThat(options.getValueFormatter()).as("after replace").isSameAs(formatter2);
	}

	@Test
	void extractorsDefaultAtEnd() {
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
	void extractorReplacingDefaultMovesUp() {
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
	void extractorReplacingCustomInPlace() {
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
	void getExtractorsIsCopy() {
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

	@Test
	@SuppressWarnings("rawtypes")
	void copy() {
		Extractor<Signal> customExtractor1 = new Extractor<Signal>() {
			@Override
			public Class<Signal> getTargetClass() {
				return Signal.class;
			}

			@Override
			public boolean matches(Signal value) {
				return value.isOnNext() && value.hasValue();
			}

			@Override
			public Stream<Object> explode(Signal original) {
				return Stream.of("CUSTOM1", original.get());
			}
		};
		Extractor<Signal> customExtractor2 = new Extractor<Signal>() {
			@Override
			public Class<Signal> getTargetClass() {
				return Signal.class;
			}

			@Override
			public boolean matches(Signal value) {
				return value.isOnNext() && value.hasValue();
			}

			@Override
			public Stream<Object> explode(Signal original) {
				return Stream.of("CUSTOM2", original.get());
			}
		};

		StepVerifierOptions options = StepVerifierOptions.create()
				.initialRequest(123L)
				.withInitialContext(Context.of("example", true))
				.scenarioName("scenarioName")
				.extractor(customExtractor1)
				.checkUnderRequesting(false)
				.valueFormatter(ValueFormatters.forClass(Signal.class, s -> "SIGNAL"))
				.virtualTimeSchedulerSupplier(VirtualTimeScheduler::create);

		StepVerifierOptions copy = options.copy();

		assertThat(copy)
				.as("deep copy")
				.isNotSameAs(options)
				.usingRecursiveComparison()
				.isEqualTo(options);

		assertThat(copy.extractorMap)
				.as("extractorMap not shared")
				.isNotSameAs(options.extractorMap)
				.containsOnlyKeys(Signal.class)
				.containsEntry(Signal.class, customExtractor1);

		copy.initialRequest(234L)
				.withInitialContext(Context.of("exampleSame", false))
				.scenarioName("scenarioName2")
				.checkUnderRequesting(true)
				.extractor(customExtractor2)
				.valueFormatter(ValueFormatters.forClass(Signal.class, s -> "SIGNAL2"))
				.virtualTimeSchedulerSupplier(() -> VirtualTimeScheduler.create(false));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> assertThat(copy)
						.usingRecursiveComparison()
						.isEqualTo(options)
				)
				.as("post mutation")
				.withMessageContainingAll(
						"when recursively comparing field by field, but found the following 5 differences:",
						"field/property 'checkUnderRequesting' differ:",
						"field/property 'initialContext.key' differ:",
						"field/property 'initialContext.value' differ:",
						"field/property 'initialRequest' differ:",
						"field/property 'scenarioName' differ:"
				);
		assertThat(copy.extractorMap)
				.as("post mutation extractorMap")
				.containsOnlyKeys(Signal.class)
				.doesNotContainEntry(Signal.class, customExtractor1)
				.containsEntry(Signal.class, customExtractor2);
		assertThat(copy.getValueFormatter()).as("valueFormatter").isNotSameAs(options.getValueFormatter());
		assertThat(copy.getVirtualTimeSchedulerSupplier()).as("vts supplier").isNotSameAs(options.getVirtualTimeSchedulerSupplier());
	}

}