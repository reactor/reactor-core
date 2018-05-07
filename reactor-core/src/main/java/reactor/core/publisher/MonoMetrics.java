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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * Activate metrics gathering on a {@link Mono} if Micrometer is on the classpath.
 *
 * @author Simon Baslé
 */
public class MonoMetrics<T> extends MonoOperator<T, T> {

	private static final Logger
			LOGGER = Loggers.getLogger(reactor.core.publisher.FluxMetrics.class);

	final String    name;
	final List<Tag> tags;

	MonoMetrics(Mono<? extends T> mono) {
		super(mono);
		//resolve the tags and names at instantiation
		Scannable scannable = Scannable.from(mono);
		if (scannable.isScanAvailable()) {
			String nameOrDefault = scannable.name();
			if (scannable.stepName().equals(nameOrDefault)) {
				this.name = FluxMetrics.REACTOR_DEFAULT_NAME;
			}
			else {
				this.name = nameOrDefault;
			}
			this.tags = scannable.tags()
			                     .map(tuple -> Tag.of(tuple.getT1(), tuple.getT2()))
			                     .collect(Collectors.toList());
		}
		else {
			LOGGER.warn("Attempting to activate metrics but the upstream is not Scannable. " +
					"You might want to use `name()` (and optionally `tags()`) right before `metrics()`");
			this.name = FluxMetrics.REACTOR_DEFAULT_NAME;
			this.tags = Collections.emptyList();
		}
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		CoreSubscriber<? super T> metricsOperator;
		try {
			metricsOperator = new reactor.core.publisher.FluxMetrics.MicrometerMetricsSubscriber<>(actual, Metrics.globalRegistry,
					this.name, this.tags, true);
		}
		catch (Throwable e) {
			metricsOperator = actual;
		}
		source.subscribe(metricsOperator);
	}

}
