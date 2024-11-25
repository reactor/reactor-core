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

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.reactivestreams.Publisher;

import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * A companion configuration object for {@link MicrometerMeterListener} that serves as the state created by
 * {@link MicrometerMeterListenerFactory}.
 *
 * @author Simon Baslé
 */
final class MicrometerMeterListenerConfiguration {

	private static final Logger LOGGER = Loggers.getLogger(MicrometerMeterListenerConfiguration.class);

	static MicrometerMeterListenerConfiguration fromFlux(Flux<?> source, MeterRegistry meterRegistry) {
		Tags defaultTags = MicrometerMeterListener.DEFAULT_TAGS_FLUX;
		final String name = resolveName(source, LOGGER, Micrometer.DEFAULT_METER_PREFIX);
		final Tags tags = resolveTags(source, defaultTags);

		return new MicrometerMeterListenerConfiguration(name, tags, meterRegistry, false);
	}

	static MicrometerMeterListenerConfiguration fromMono(Mono<?> source, MeterRegistry meterRegistry) {
		Tags defaultTags = MicrometerMeterListener.DEFAULT_TAGS_MONO;
		final String name = resolveName(source, LOGGER, Micrometer.DEFAULT_METER_PREFIX);
		final Tags tags = resolveTags(source, defaultTags);

		return new MicrometerMeterListenerConfiguration(name, tags, meterRegistry, true);
	}

	/**
	 * Extract the name from the upstream, and detect if there was an actual name (ie. distinct from {@link
	 * Scannable#stepName()}) set by the user.
	 *
	 * @param source the upstream
	 *
	 * @return a name
	 */
	static String resolveName(Publisher<?> source, Logger logger, String defaultName) {
		Scannable scannable = Scannable.from(source);
		if (!scannable.isScanAvailable()) {
			logger.warn("Attempting to activate metrics but the upstream is not Scannable. You might want to use `name()` (and optionally `tags()`) right before this listener");
			return defaultName;
		}

		String nameOrDefault = scannable.name();
		if (scannable.stepName()
			.equals(nameOrDefault)) {
			return defaultName;
		}
		else {
			return nameOrDefault;
		}
	}

	/**
	 * Extract the tags from the upstream
	 *
	 * @param source the upstream
	 *
	 * @return a {@link Tags} of {@link io.micrometer.core.instrument.Tag}
	 */
	static Tags resolveTags(Publisher<?> source, Tags tags) {
		Scannable scannable = Scannable.from(source);

		if (scannable.isScanAvailable()) {
			// `Tags#and` deduplicates tags by key, retaining the last value as required.
			List<Tag> discoveredTags = scannable.tags()
				.map(t -> Tag.of(t.getT1(), t.getT2()))
				.collect(Collectors.toList());
			return tags.and(discoveredTags);
		}

		return tags;
	}

	final Tags    commonTags;
	final boolean isMono;
	final String  sequenceName;

	//Note: meters and tag names are normalized by micrometer on the basis that the word
	// separator is the dot, not camelCase...
	final MeterRegistry registry;

	MicrometerMeterListenerConfiguration(String sequenceName, Tags tags, MeterRegistry registryCandidate, boolean isMono) {
		this.commonTags = tags;
		this.isMono = isMono;
		this.sequenceName = sequenceName;
		this.registry = registryCandidate;
	}
}
