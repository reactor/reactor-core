/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.metrics.micrometer;

import java.util.Arrays;
import java.util.List;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Simon Baslé
 */
@Deprecated //the classes under test are Deprecated
class CoreMetricsDisabledTest {

	@Test
	void coreMetricsReportsInstrumentationUnavailable() {
		assertThat(Metrics.isInstrumentationAvailable()).as("isInstrumentationAvailable").isFalse();
	}

	@Test
	void coreMicrometerConfigurationUseRegistryThrows() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
			.isThrownBy(() -> Metrics.MicrometerConfiguration.useRegistry(new SimpleMeterRegistry()))
			.as("useRegistry")
			.withMessage("When reactor-metrics-micrometer is on the classpath, the MicrometerConfiguration registry is never used");
	}

	@Test
	void coreMicrometerConfigurationGetRegistryThrows() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
			.isThrownBy(Metrics.MicrometerConfiguration::getRegistry)
			.as("useRegistry")
			.withMessage("When reactor-metrics-micrometer is on the classpath, the MicrometerConfiguration registry is never used");
	}

	@Test
	void coreMetricsOperatorsNoOp() {
		List<String> classNames = Arrays.asList(
			Flux.empty().hide().metrics().getClass().getSimpleName(),
			Flux.just(1, 2, 3).metrics().getClass().getSimpleName(),
			Mono.empty().hide().metrics().getClass().getSimpleName(),
			Mono.fromCallable(() -> 123).metrics().getClass().getSimpleName()
		);

		assertThat(classNames).doesNotContain("FluxMetrics", "FluxMetricsFuseable", "MonoMetrics", "MonoMetricsFuseable");
	}

}
