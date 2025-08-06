/*
 * Copyright (c) 2022-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

import reactor.core.observability.SignalListener;
import reactor.core.observability.SignalListenerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.function.Function;

/**
 * A {@link SignalListenerFactory} for {@link MicrometerObservationListener}.
 *
 * @author Simon Baslé
 */
class MicrometerObservationListenerFactory<T> implements SignalListenerFactory<T, MicrometerObservationListenerConfiguration> {

	final ObservationRegistry registry;
	@Nullable final Function<ObservationRegistry, Observation> observationSupplier;

	public MicrometerObservationListenerFactory(ObservationRegistry registry) {
		this(registry, null);
	}

	public MicrometerObservationListenerFactory(ObservationRegistry registry,
			@Nullable Function<ObservationRegistry, Observation> observationSupplier) {
		this.registry = registry;
		this.observationSupplier = observationSupplier;
	}

	@Override
	public MicrometerObservationListenerConfiguration initializePublisherState(Publisher<? extends T> source) {
		if (source instanceof Mono) {
			return MicrometerObservationListenerConfiguration.fromMono((Mono<?>) source, this.registry);
		}
		else if (source instanceof Flux) {
			return MicrometerObservationListenerConfiguration.fromFlux((Flux<?>) source, this.registry);
		}
		else {
			throw new IllegalArgumentException("MicrometerObservationListenerFactory must only be used via the tap operator / with a Flux or Mono");
		}
	}

	@Override
	public SignalListener<T> createListener(Publisher<? extends T> source, ContextView listenerContext,
											MicrometerObservationListenerConfiguration publisherContext) {
		return new MicrometerObservationListener<>(listenerContext, publisherContext, observationSupplier);
	}
}
