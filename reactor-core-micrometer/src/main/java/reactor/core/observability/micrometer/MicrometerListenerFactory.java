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

package reactor.core.observability.micrometer;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;
import reactor.core.observability.SignalListener;
import reactor.core.observability.SignalListenerFactory;

/**
 * A {@link SignalListenerFactory} for {@link MicrometerListener}.
 *
 * @author Simon Baslé
 */
class MicrometerListenerFactory<T> implements SignalListenerFactory<T, MicrometerListenerConfiguration> {

	protected Clock useClock() {
		return Clock.SYSTEM;
	}

	protected MeterRegistry useRegistry() {
		return Micrometer.getRegistry();
	}

	@Override
	public MicrometerListenerConfiguration initializePublisherState(Publisher<? extends T> source) {
		if (source instanceof Mono) {
			return MicrometerListenerConfiguration.fromMono((Mono<?>) source, useRegistry(), useClock());
		}
		else if (source instanceof Flux) {
			return MicrometerListenerConfiguration.fromFlux((Flux<?>) source, useRegistry(), useClock());
		}
		else {
			throw new IllegalArgumentException("MicrometerListenerFactory must only be used via the tap operator / with a Flux or Mono");
		}
	}

	@Override
	public SignalListener<T> createListener(Publisher<? extends T> source, ContextView listenerContext,
											MicrometerListenerConfiguration publisherContext) {
		return new MicrometerListener<>(publisherContext);
	}
}
