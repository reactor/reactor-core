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

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.CsvSource;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.test.ParameterizedTestWithName;
import reactor.test.util.TestLogger;
import org.jspecify.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class MicrometerMeterListenerConfigurationTest {

	@ParameterizedTestWithName
	@CsvSource(value = {
		",",
		"someName,",
		",someTag",
		"someName,someTag"
	})
	void fromFlux(@Nullable String name, @Nullable String tag) {
		MeterRegistry expectedRegistry = new SimpleMeterRegistry();
		Clock expectedClock = Clock.SYSTEM;

		Flux<Integer> flux = Flux.just(1, 2, 3);

		if (name != null) {
			flux = flux.name(name);
		}
		if (tag != null) {
			flux = flux.tag("tag", tag);
		}

	MicrometerMeterListenerConfiguration configuration = MicrometerMeterListenerConfiguration.fromFlux(flux, expectedRegistry);

		assertThat(configuration.registry.config().clock()).as("clock").isSameAs(expectedClock);
		assertThat(configuration.registry).as("registry").isSameAs(expectedRegistry);
		assertThat(configuration.isMono).as("isMono").isFalse();

		assertThat(configuration.sequenceName)
			.as("sequenceName")
			.isEqualTo(name == null ? Micrometer.DEFAULT_METER_PREFIX : name);

		if (tag == null) {
			assertThat(configuration.commonTags.stream().map(t -> t.getKey() + "=" + t.getValue()))
				.as("commonTags without additional tag")
				.containsExactly("type=Flux");
		}
		else {
			assertThat(configuration.commonTags.stream().map(t -> t.getKey() + "=" + t.getValue()))
				.as("commonTags")
				.containsExactlyInAnyOrder("type=Flux", "tag="+tag);
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
		MeterRegistry expectedRegistry = new SimpleMeterRegistry();
		Clock expectedClock = Clock.SYSTEM;

		Mono<Integer> mono = Mono.just(1);

		if (name != null) {
			mono = mono.name(name);
		}
		if (tag != null) {
			mono = mono.tag("tag", tag);
		}

		MicrometerMeterListenerConfiguration configuration = MicrometerMeterListenerConfiguration.fromMono(mono, expectedRegistry);

		assertThat(configuration.registry.config().clock()).as("clock").isSameAs(expectedClock);
		assertThat(configuration.registry).as("registry").isSameAs(expectedRegistry);
		assertThat(configuration.isMono).as("isMono").isTrue();

		assertThat(configuration.sequenceName)
			.as("sequenceName")
			.isEqualTo(name == null ? Micrometer.DEFAULT_METER_PREFIX : name);

		if (tag == null) {
			assertThat(configuration.commonTags.stream().map(t -> t.getKey() + "=" + t.getValue()))
				.as("commonTags without additional tag")
				.containsExactly("type=Mono");
		}
		else {
			assertThat(configuration.commonTags.stream().map(t -> t.getKey() + "=" + t.getValue()))
				.as("commonTags")
				.containsExactlyInAnyOrder("type=Mono", "tag="+tag);
		}
	}

	@Test
	void resolveName_notSet() {
		String defaultName = "ANONYMOUS";
		TestLogger logger = new TestLogger(false);
		Flux<Integer> flux = Flux.just(1);

		String resolvedName = MicrometerMeterListenerConfiguration.resolveName(flux, logger, defaultName);

		assertThat(resolvedName).isEqualTo(defaultName);
		assertThat(logger.getOutContent() + logger.getErrContent()).as("logs").isEmpty();
	}

	@Test
	void resolveName_setRightAbove() {
		TestLogger logger = new TestLogger(false);
		Flux<Integer> flux = Flux.just(1).name("someName");

		String resolvedName = MicrometerMeterListenerConfiguration.resolveName(flux, logger, "UNEXPECTED");

		assertThat(resolvedName).isEqualTo("someName");
		assertThat(logger.getOutContent() + logger.getErrContent()).as("logs").isEmpty();
	}

	@Test
	void resolveName_setHigherAbove() {
		TestLogger logger = new TestLogger(false);
		Flux<Integer> flux = Flux.just(1).name("someName").filter(i -> i % 2 == 0).map(i -> i + 10);

		String resolvedName = MicrometerMeterListenerConfiguration.resolveName(flux, logger, "UNEXPECTED");

		assertThat(resolvedName).isEqualTo("someName");
		assertThat(logger.getOutContent() + logger.getErrContent()).as("logs").isEmpty();
	}

	@Test
	void resolveName_notScannable() {
		String defaultName = "ANONYMOUS";
		TestLogger testLogger = new TestLogger(false);
		Publisher<Object> publisher = Operators::complete;

		String resolvedName = MicrometerMeterListenerConfiguration.resolveName(publisher, testLogger, defaultName);

		assertThat(resolvedName).as("resolved name").isEqualTo(defaultName);
		assertThat(testLogger.getErrContent()).contains("Attempting to activate metrics but the upstream is not Scannable. You might want to use `name()` (and optionally `tags()`) right before this listener");
	}

	@Test
	void resolveTags_notSet() {
		Tags defaultTags = Tags.of("common1", "commonValue1");
		Flux<Integer> flux = Flux.just(1);

		Tags resolvedTags = MicrometerMeterListenerConfiguration.resolveTags(flux, defaultTags);

		assertThat(resolvedTags.stream().map(Object::toString))
			.containsExactly("tag(common1=commonValue1)");
	}

	@Test
	void resolveTags_setRightAbove() {
		Tags defaultTags = Tags.of("common1", "commonValue1");
		Flux<Integer> flux = Flux
			.just(1)
			.tag("k1", "v1");

		Tags resolvedTags = MicrometerMeterListenerConfiguration.resolveTags(flux, defaultTags);

		assertThat(resolvedTags.stream().map(Object::toString)).containsExactlyInAnyOrder(
			"tag(common1=commonValue1)",
			"tag(k1=v1)"
		);
	}

	@Test
	void resolveTags_setHigherAbove() {
		Tags defaultTags = Tags.of("common1", "commonValue1");
		Flux<Integer> flux = Flux
			.just(1)
			.tag("k1", "v1")
			.filter(i -> i % 2 == 0)
			.map(i -> i + 10);

		Tags resolvedTags = MicrometerMeterListenerConfiguration.resolveTags(flux, defaultTags);

		assertThat(resolvedTags.stream().map(Object::toString)).containsExactlyInAnyOrder(
			"tag(common1=commonValue1)",
			"tag(k1=v1)"
		);
	}

	@Test
	void resolveTags_multipleScatteredTagsSetAbove() {
		Tags defaultTags = Tags.of("common1", "commonValue1");
		Flux<Integer> flux = Flux.just(1)
			.tag("k1", "v1")
			.filter(i -> i % 2 == 0)
			.tag("k2", "v2")
			.map(i -> i + 10);

		Tags resolvedTags = MicrometerMeterListenerConfiguration.resolveTags(flux, defaultTags);

		assertThat(resolvedTags.stream().map(Object::toString)).containsExactlyInAnyOrder(
			"tag(common1=commonValue1)",
			"tag(k1=v1)",
			"tag(k2=v2)"
		);
	}

	@Test
	void resolveTags_multipleScatteredTagsSetAboveWithDeduplication() {
		Tags defaultTags = Tags.of("common1", "commonValue1");
		Flux<Integer> flux = Flux.just(1)
			.tag("k1", "v1")
			.tag("k2", "oldV2")
			.filter(i -> i % 2 == 0)
			.tag("k2", "v2")
			.map(i -> i + 10);

		Tags resolvedTags = MicrometerMeterListenerConfiguration.resolveTags(flux, defaultTags);

		assertThat(resolvedTags.stream().map(Object::toString)).containsExactly(
			"tag(common1=commonValue1)",
			"tag(k1=v1)",
			"tag(k2=v2)"
		);
	}

	@Test
	void resolveTags_notScannable() {
		Tags defaultTags = Tags.of("common1", "commonValue1");
		Publisher<Object> publisher = Operators::complete;

		Tags resolvedTags = MicrometerMeterListenerConfiguration.resolveTags(publisher, defaultTags);

		assertThat(resolvedTags.stream().map(Object::toString)).containsExactly("tag(common1=commonValue1)");
	}
}