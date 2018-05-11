/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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
import reactor.core.publisher.FluxMetrics.MicrometerMetricsSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;

/**
 * Activate metrics gathering on a {@link Mono} if Micrometer is on the classpath.
 *
 * @author Simon Baslé
 */
public class MonoMetrics<T> extends MonoOperator<T, T> {

	final String    name;
	final List<Tag> tags;

	@Nullable
	final Object registryCandidate;

	MonoMetrics(Mono<? extends T> mono) {
		this(mono, null);
	}

	/**
	 * For testing purposes.
	 *
	 * @param registryCandidate the registry to use, as a plain {@link Object} to avoid leaking dependency
	 */
	MonoMetrics(Mono<? extends T> mono, @Nullable Object registryCandidate) {
		super(mono);

		Tuple2<String, List<Tag>> nameAndTags = FluxMetrics.resolveNameAndTags(mono);
		this.name = nameAndTags.getT1();
		this.tags = nameAndTags.getT2();

		this.registryCandidate = registryCandidate;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		CoreSubscriber<? super T> metricsOperator;
		if (reactor.util.Metrics.isMicrometerAvailable()) {
			MeterRegistry registry = Metrics.globalRegistry;
			if (registryCandidate instanceof MeterRegistry) {
				registry = (MeterRegistry) registryCandidate;
			}
			metricsOperator = new MicrometerMetricsSubscriber<>(actual, registry,
					Clock.SYSTEM, this.name, this.tags, true);
		}
		else {
			metricsOperator = actual;
		}
		source.subscribe(metricsOperator);
	}

}
