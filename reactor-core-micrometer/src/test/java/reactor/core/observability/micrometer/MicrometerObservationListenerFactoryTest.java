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

import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import reactor.core.observability.SignalListener;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * @author Simon Baslé
 */
class MicrometerObservationListenerFactoryTest {

	protected static final ObservationRegistry CUSTOM_REGISTRY = ObservationRegistry.create();

	protected static final MicrometerObservationListenerFactory<Object> CUSTOM_FACTORY =
		new MicrometerObservationListenerFactory<>(CUSTOM_REGISTRY);

	@Test
	void configurationFromMono() {
		MicrometerObservationListenerConfiguration configuration = CUSTOM_FACTORY.initializePublisherState(Mono.just(1));

		assertThat(configuration.registry).as("registry").isSameAs(CUSTOM_REGISTRY);
		assertThat(configuration.isMono).as("isMono").isTrue();
		assertThat(configuration.commonKeyValues).map(Object::toString).containsExactly("keyValue(" + "reactor.type" + "=Mono)");
	}

	@Test
	void configurationFromFlux() {
		MicrometerObservationListenerConfiguration configuration = CUSTOM_FACTORY.initializePublisherState(Flux.just(1, 2));

		assertThat(configuration.registry).as("registry").isSameAs(CUSTOM_REGISTRY);
		assertThat(configuration.isMono).as("isMono").isFalse();
		assertThat(configuration.commonKeyValues).map(Object::toString).containsExactly("keyValue(" + "reactor.type" + "=Flux)");
	}

	@Test
	void configurationFromGenericPublisherIsRejected() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> CUSTOM_FACTORY.initializePublisherState(Operators::complete))
			.withMessage("MicrometerObservationListenerFactory must only be used via the tap operator / with a Flux or Mono");
	}

	@Test
	void createListenerOfTypeMicrometer() {
		Publisher<Integer> source = Mono.just(1);
		ContextView expectedContext = Context.of(1, "A");
		MicrometerObservationListenerConfiguration conf = CUSTOM_FACTORY.initializePublisherState(source);
		SignalListener<?> signalListener = CUSTOM_FACTORY.createListener(source, expectedContext, conf);

		assertThat(signalListener).isInstanceOf(MicrometerObservationListener.class);
		MicrometerObservationListener<?> observationListener = (MicrometerObservationListener<?>) signalListener;

		assertThat(observationListener.configuration).as("configuration").isSameAs(conf);
		assertThat(observationListener.originalContext).as("context").isSameAs(expectedContext);
	}

}