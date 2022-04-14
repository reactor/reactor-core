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

import java.util.LinkedList;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

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
import reactor.util.function.Tuple2;

/**
 * @author Simon Baslé
 */
final class MicrometerListenerConfiguration {

	private static final Logger LOGGER = Loggers.getLogger(MicrometerListenerConfiguration.class);

	static MicrometerListenerConfiguration fromFlux(Flux<?> source) {
		Clock clock = Clock.SYSTEM;
		Tags defaultTags = MicrometerListener.DEFAULT_TAGS_FLUX;
		final String name = resolveName(source, LOGGER);
		final Tags tags = resolveTags(source, defaultTags);

		return new MicrometerListenerConfiguration(name, tags, Micrometer.getRegistry(), clock, false);
	}

	static MicrometerListenerConfiguration fromMono(Mono<?> source) {
		Clock clock = Clock.SYSTEM;
		Tags defaultTags = MicrometerListener.DEFAULT_TAGS_MONO;
		final String name = resolveName(source, LOGGER);
		final Tags tags = resolveTags(source, defaultTags);

		return new MicrometerListenerConfiguration(name, tags, Micrometer.getRegistry(), clock, true);
	}

	/**
	 * Extract the name from the upstream, and detect if there was an actual name (ie. distinct from {@link
	 * Scannable#stepName()}) set by the user.
	 *
	 * @param source the upstream
	 *
	 * @return a name
	 */
	static String resolveName(Publisher<?> source, Logger logger) {
		Scannable scannable = Scannable.from(source);
		if (!scannable.isScanAvailable()) {
			logger.warn("Attempting to activate metrics but the upstream is not Scannable. You might want to use `name()` (and optionally `tags()`) right before this listener");
			return MicrometerListener.REACTOR_DEFAULT_NAME;
		}

		String nameOrDefault = scannable.name();
		if (scannable.stepName()
			.equals(nameOrDefault)) {
			return MicrometerListener.REACTOR_DEFAULT_NAME;
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
			LinkedList<Tuple2<String, String>> scannableTags = new LinkedList<>();
			scannable.tags().forEach(scannableTags::push);
			return scannableTags.stream()
				//Note the combiner below is for parallel streams, which won't be used
				//For the identity, `commonTags` should be ok (even if reduce uses it multiple times)
				//since it deduplicates
				.reduce(tags, TAG_ACCUMULATOR, TAG_COMBINER);
		}

		return tags;
	}

	final Clock   clock;
	final Tags    commonTags;
	final boolean isMono;
	final String  sequenceName;

	//Note: meters and tag names are normalized by micrometer on the basis that the word
	// separator is the dot, not camelCase...
	final MeterRegistry registry;

	MicrometerListenerConfiguration(String sequenceName, Tags tags, MeterRegistry registryCandidate, Clock clock,
									boolean isMono) {
		this.clock = clock;
		this.commonTags = tags;
		this.isMono = isMono;
		this.sequenceName = sequenceName;
		this.registry = registryCandidate;
	}

	static final BiFunction<Tags, Tuple2<String, String>, Tags> TAG_ACCUMULATOR =
		(prev, tuple) -> prev.and(Tag.of(tuple.getT1(), tuple.getT2()));

	static final BinaryOperator<Tags>                           TAG_COMBINER    = Tags::and;
}
