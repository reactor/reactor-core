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
 * Activate metrics gathering on a {@link Mono}, assumes Micrometer is on the classpath.
 *
 * @implNote Metrics.isMicrometerAvailable() test should be performed BEFORE instantiating
 * or referencing this class, otherwise a {@link NoClassDefFoundError} will be thrown if
 * Micrometer is not there.
 *
 * @author Simon Baslé
 */
public class MonoMetrics<T> extends MonoOperator<T, T> {

	final String    name;
	final List<Tag> tags;

	@Nullable
	final MeterRegistry meterRegistry;

	MonoMetrics(Mono<? extends T> mono) {
		this(mono, null);
	}

	/**
	 * For testing purposes.
	 *
	 * @param meterRegistry the registry to use
	 */
	MonoMetrics(Mono<? extends T> mono, @Nullable MeterRegistry meterRegistry) {
		super(mono);

		Tuple2<String, List<Tag>> nameAndTags = FluxMetrics.resolveNameAndTags(mono);
		this.name = nameAndTags.getT1();
		this.tags = nameAndTags.getT2();

		this.meterRegistry = meterRegistry;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		MeterRegistry registry = Metrics.globalRegistry;
		if (meterRegistry != null) {
			registry = meterRegistry;
		}
		source.subscribe(new MicrometerMetricsSubscriber<>(actual, registry,
				Clock.SYSTEM, this.name, this.tags,true));
	}

}
