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
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.function.Tuple2;

import static reactor.core.publisher.FluxMetrics.resolveNameAndTags;

/**
 * Activate metrics gathering on a {@link Flux} if Micrometer is on the classpath
 * (Fuseable version).
 *
 * @author Simon Baslé
 */
final class FluxMetricsFuseable<T> extends FluxOperator<T, T> implements Fuseable {

	final String    name;
	final List<Tag> tags;

	FluxMetricsFuseable(Flux<? extends T> flux) {
		super(flux);

		Tuple2<String, List<Tag>> nameAndTags = resolveNameAndTags(flux);
		this.name = nameAndTags.getT1();
		this.tags = nameAndTags.getT2();
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		CoreSubscriber<? super T> metricsOperator;
		if (reactor.util.Metrics.isMicrometerAvailable()) {
			metricsOperator = new FluxMetrics.MicrometerMetricsFuseableSubscriber<>(actual, Metrics.globalRegistry,
					Clock.SYSTEM, this.name, this.tags, false);
		}
		else {
			metricsOperator = actual;
		}
		source.subscribe(metricsOperator);
	}

}
