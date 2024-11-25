/*
 * Copyright (c) 2022-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.List;
import java.util.stream.Collectors;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.ObservationRegistry;
import org.reactivestreams.Publisher;

import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * A companion configuration object for {@link MicrometerObservationListener} that serves as the state created by
 * {@link MicrometerObservationListenerFactory}.
 *
 * @author Simon Baslé
 */
final class MicrometerObservationListenerConfiguration {

	static final KeyValues DEFAULT_KV_FLUX = KeyValues.of(MicrometerObservationListenerDocumentation.ObservationTags.TYPE.asString(), "Flux");
	static final KeyValues DEFAULT_KV_MONO = KeyValues.of(MicrometerObservationListenerDocumentation.ObservationTags.TYPE.asString(), "Mono");

	private static final Logger LOGGER = Loggers.getLogger(MicrometerObservationListenerConfiguration.class);

	static MicrometerObservationListenerConfiguration fromFlux(Flux<?> source, ObservationRegistry observationRegistry) {
		KeyValues defaultKeyValues = DEFAULT_KV_FLUX;
		final String name = MicrometerMeterListenerConfiguration.resolveName(source, LOGGER, MicrometerObservationListenerDocumentation.ANONYMOUS.getName());
		final KeyValues keyValues = resolveKeyValues(source, defaultKeyValues);

		return new MicrometerObservationListenerConfiguration(name, keyValues, observationRegistry, false);
	}

	static MicrometerObservationListenerConfiguration fromMono(Mono<?> source, ObservationRegistry observationRegistry) {
		KeyValues defaultKeyValues = DEFAULT_KV_MONO;
		final String name = MicrometerMeterListenerConfiguration.resolveName(source, LOGGER, MicrometerObservationListenerDocumentation.ANONYMOUS.getName());
		final KeyValues keyValues = resolveKeyValues(source, defaultKeyValues);

		return new MicrometerObservationListenerConfiguration(name, keyValues, observationRegistry, true);
	}

	/**
	 * Extract the "tags" from the upstream as {@link KeyValues}.
	 *
	 * @param source the upstream
	 *
	 * @return a {@link KeyValues} collection
	 */
	static KeyValues resolveKeyValues(Publisher<?> source, KeyValues tags) {
		Scannable scannable = Scannable.from(source);

		if (scannable.isScanAvailable()) {
			// `KeyValues#and` deduplicates tags by key, retaining the last value as required.
			List<KeyValue> discoveredTags = scannable.tags()
				.map(e -> KeyValue.of(e.getT1(), e.getT2()))
				.collect(Collectors.toList());
			return tags.and(discoveredTags);
		}

		return tags;
	}

	final KeyValues commonKeyValues;
	final boolean   isMono;
	final String  sequenceName;

	final ObservationRegistry registry;

	MicrometerObservationListenerConfiguration(String sequenceName, KeyValues commonKeyValues,
											   ObservationRegistry registryCandidate,
											   boolean isMono) {
		this.commonKeyValues = commonKeyValues;
		this.isMono = isMono;
		this.sequenceName = sequenceName;
		this.registry = registryCandidate;
	}
}
