/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.List;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;

import static reactor.core.publisher.FluxMetrics.resolveNameAndTags;

/**
 * Activate metrics gathering on a {@link Flux} (Fuseable version), assumes Micrometer is
 * on the classpath.
 *
 * @implNote Metrics.isInstrumentationAvailable() test should be performed BEFORE instantiating
 * or referencing this class, otherwise a {@link NoClassDefFoundError} will be thrown if
 * Micrometer is not there.
 *
 * @author Simon Baslé
 */
final class FluxMetricsFuseable<T> extends FluxOperator<T, T> implements Fuseable {

	final String    name;
	final List<Tag> tags;
	@Nullable
	final MeterRegistry     registryCandidate;

	FluxMetricsFuseable(Flux<? extends T> flux) {
		this(flux, null);
	}

	/**
	 * For testing purposes.
	 *
	 * @param candidate the registry to use, as a plain {@link Object} to avoid leaking dependency
	 */
	FluxMetricsFuseable(Flux<? extends T> flux, @Nullable MeterRegistry candidate) {
		super(flux);

		Tuple2<String, List<Tag>> nameAndTags = resolveNameAndTags(flux);
		this.name = nameAndTags.getT1();
		this.tags = nameAndTags.getT2();

		this.registryCandidate = candidate;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		MeterRegistry registry = Metrics.globalRegistry;
		if (registryCandidate != null) {
			registry = registryCandidate;
		}
		source.subscribe(new FluxMetrics.MicrometerFluxMetricsFuseableSubscriber<>(actual, registry,
				Clock.SYSTEM, this.name, this.tags));
	}

}
