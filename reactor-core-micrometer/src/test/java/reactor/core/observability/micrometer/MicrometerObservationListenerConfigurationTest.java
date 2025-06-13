/*
 * Copyright (c) 2022-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.observability.micrometer;

import io.micrometer.common.KeyValues;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.CsvSource;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.test.ParameterizedTestWithName;
import org.jspecify.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class MicrometerObservationListenerConfigurationTest {

	@ParameterizedTestWithName
	@CsvSource(value = {
		",",
		"someName,",
		",someTag",
		"someName,someTag"
	})
	void fromFlux(@Nullable String name, @Nullable String tag) {
		ObservationRegistry expectedRegistry = ObservationRegistry.create();

		Flux<Integer> flux = Flux.just(1, 2, 3);

		if (name != null) {
			flux = flux.name(name);
		}
		if (tag != null) {
			flux = flux.tag("tag", tag);
		}

	MicrometerObservationListenerConfiguration configuration = MicrometerObservationListenerConfiguration.fromFlux(flux, expectedRegistry);

		assertThat(configuration.registry).as("registry").isSameAs(expectedRegistry);
		assertThat(configuration.isMono).as("isMono").isFalse();

		assertThat(configuration.sequenceName)
			.as("sequenceName")
			.isEqualTo(name == null ? "reactor.observation" : name);

		if (tag == null) {
			assertThat(configuration.commonKeyValues.stream().map(t -> t.getKey() + "=" + t.getValue()))
				.as("commonKeyValues without additional KeyValue")
				.containsExactly("reactor.type" + "=Flux");
		}
		else {
			assertThat(configuration.commonKeyValues.stream().map(t -> t.getKey() + "=" + t.getValue()))
				.as("commonKeyValues")
				.containsExactlyInAnyOrder("reactor.type" + "=Flux", "tag="+tag);
		}
	}

	@ParameterizedTestWithName
	@CsvSource(value = {
		",",
		"someName,",
		",someTag",
		"someName,someTag"
	})
	void fromMono(@Nullable String name, @Nullable String tag) {
		ObservationRegistry expectedRegistry = ObservationRegistry.create();

		Mono<Integer> mono = Mono.just(1);

		if (name != null) {
			mono = mono.name(name);
		}
		if (tag != null) {
			mono = mono.tag("tag", tag);
		}

		MicrometerObservationListenerConfiguration configuration = MicrometerObservationListenerConfiguration.fromMono(mono, expectedRegistry);

		assertThat(configuration.registry).as("registry").isSameAs(expectedRegistry);
		assertThat(configuration.isMono).as("isMono").isTrue();

		assertThat(configuration.sequenceName)
			.as("sequenceName")
			.isEqualTo(name == null ? "reactor.observation": name);

		if (tag == null) {
			assertThat(configuration.commonKeyValues.stream().map(t -> t.getKey() + "=" + t.getValue()))
				.as("commonKeyValues without additional KeyValue")
				.containsExactly("reactor.type" + "=Mono");
		}
		else {
			assertThat(configuration.commonKeyValues.stream().map(t -> t.getKey() + "=" + t.getValue()))
				.as("commonKeyValues")
				.containsExactlyInAnyOrder("reactor.type" + "=Mono", "tag="+tag);
		}
	}

	// == NB: this configuration reuses the MicrometerObservationListenerConfiguration#resolveName method, not tested here

	@Test
	void resolveKeyValues_notSet() {
		KeyValues defaultKeyValues = KeyValues.of("common1", "commonValue1");
		Flux<Integer> flux = Flux.just(1);

		KeyValues resolvedKeyValues = MicrometerObservationListenerConfiguration.resolveKeyValues(flux, defaultKeyValues);

		assertThat(resolvedKeyValues.stream().map(Object::toString))
			.containsExactly("keyValue(common1=commonValue1)");
	}

	@Test
	void resolveKeyValues_setRightAbove() {
		KeyValues defaultKeyValues = KeyValues.of("common1", "commonValue1");
		Flux<Integer> flux = Flux
			.just(1)
			.tag("k1", "v1");

		KeyValues resolvedKeyValues = MicrometerObservationListenerConfiguration.resolveKeyValues(flux, defaultKeyValues);

		assertThat(resolvedKeyValues.stream().map(Object::toString)).containsExactlyInAnyOrder(
			"keyValue(common1=commonValue1)",
			"keyValue(k1=v1)"
		);
	}

	@Test
	void resolveKeyValues_setHigherAbove() {
		KeyValues defaultKeyValues = KeyValues.of("common1", "commonValue1");
		Flux<Integer> flux = Flux
			.just(1)
			.tag("k1", "v1")
			.filter(i -> i % 2 == 0)
			.map(i -> i + 10);

		KeyValues resolvedKeyValues = MicrometerObservationListenerConfiguration.resolveKeyValues(flux, defaultKeyValues);

		assertThat(resolvedKeyValues.stream().map(Object::toString)).containsExactlyInAnyOrder(
			"keyValue(common1=commonValue1)",
			"keyValue(k1=v1)"
		);
	}

	@Test
	void resolveKeyValues_multipleScatteredKeyValuesSetAbove() {
		KeyValues defaultKeyValues = KeyValues.of("common1", "commonValue1");
		Flux<Integer> flux = Flux.just(1)
			.tag("k1", "v1")
			.filter(i -> i % 2 == 0)
			.tag("k2", "v2")
			.map(i -> i + 10);

		KeyValues resolvedKeyValues = MicrometerObservationListenerConfiguration.resolveKeyValues(flux, defaultKeyValues);

		assertThat(resolvedKeyValues.stream().map(Object::toString)).containsExactlyInAnyOrder(
			"keyValue(common1=commonValue1)",
			"keyValue(k1=v1)",
			"keyValue(k2=v2)"
		);
	}

	@Test
	void resolveKeyValues_multipleScatteredKeyValuesSetAboveWithDeduplication() {
		KeyValues defaultKeyValues = KeyValues.of("common1", "commonValue1");
		Flux<Integer> flux = Flux.just(1)
			.tag("k1", "v1")
			.tag("k2", "oldV2")
			.filter(i -> i % 2 == 0)
			.tag("k2", "v2")
			.map(i -> i + 10);

		KeyValues resolvedKeyValues = MicrometerObservationListenerConfiguration.resolveKeyValues(flux, defaultKeyValues);

		assertThat(resolvedKeyValues.stream().map(Object::toString)).containsExactly(
			"keyValue(common1=commonValue1)",
			"keyValue(k1=v1)",
			"keyValue(k2=v2)"
		);
	}

	@Test
	void resolveKeyValues_notScannable() {
		KeyValues defaultKeyValues = KeyValues.of("common1", "commonValue1");
		Publisher<Object> publisher = Operators::complete;

		KeyValues resolvedKeyValues = MicrometerObservationListenerConfiguration.resolveKeyValues(publisher, defaultKeyValues);

		assertThat(resolvedKeyValues.stream().map(Object::toString)).containsExactly("keyValue(common1=commonValue1)");
	}
}