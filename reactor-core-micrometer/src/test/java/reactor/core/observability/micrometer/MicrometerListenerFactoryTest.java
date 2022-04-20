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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;
import reactor.core.observability.SignalListener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * @author Simon Baslé
 */
class MicrometerListenerFactoryTest {

	@Test
	void useClockDefaultsToSystemClock() {
		MicrometerListenerFactory<?> factory = new MicrometerListenerFactory<>();

		assertThat(factory.useClock()).isSameAs(Clock.SYSTEM);
	}

	@Test
	void useRegistryDefaultsToCommonRegistry() {
		SimpleMeterRegistry commonRegistry = new SimpleMeterRegistry();
		MeterRegistry defaultCommon = Micrometer.useRegistry(commonRegistry);
		try {
			MicrometerListenerFactory<?> factory = new MicrometerListenerFactory<>();

			assertThat(factory.useRegistry()).isSameAs(Micrometer.getRegistry())
				.isSameAs(commonRegistry);
		}
		finally {
			Micrometer.useRegistry(defaultCommon);
		}
	}

	@Test
	void configurationFromMono() {
		MicrometerListenerConfiguration configuration = CUSTOM_FACTORY.initializePublisherState(Mono.just(1));

		assertThat(configuration.registry).as("registry").isSameAs(CUSTOM_REGISTRY);
		assertThat(configuration.clock).as("clock").isSameAs(CUSTOM_CLOCK);
		assertThat(configuration.isMono).as("isMono").isTrue();
		assertThat(configuration.commonTags).map(Object::toString).containsExactly("tag(type=Mono)");
	}

	@Test
	void configurationFromFlux() {
		MicrometerListenerConfiguration configuration = CUSTOM_FACTORY.initializePublisherState(Flux.just(1, 2));

		assertThat(configuration.registry).as("registry").isSameAs(CUSTOM_REGISTRY);
		assertThat(configuration.clock).as("clock").isSameAs(CUSTOM_CLOCK);
		assertThat(configuration.isMono).as("isMono").isFalse();
		assertThat(configuration.commonTags).map(Object::toString).containsExactly("tag(type=Flux)");
	}

	@Test
	void configurationFromGenericPublisherIsRejected() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> CUSTOM_FACTORY.initializePublisherState(Operators::complete))
			.withMessage("MicrometerListenerFactory must only be used via the tap operator / with a Flux or Mono");
	}

	@Test
	void createListenerOfTypeMicrometer() {
		Publisher<Integer> source = Mono.just(1);
		MicrometerListenerConfiguration conf = CUSTOM_FACTORY.initializePublisherState(source);
		SignalListener<?> signalListener = CUSTOM_FACTORY.createListener(source, Context.empty(), conf);

		assertThat(signalListener).isInstanceOf(MicrometerListener.class);
		assertThat(((MicrometerListener<?>) signalListener).configuration).as("configuration").isSameAs(conf);
	}

	protected static final Clock CUSTOM_CLOCK = new Clock() {
		@Override
		public long wallTime() {
			return 0;
		}

		@Override
		public long monotonicTime() {
			return 0;
		}
	};
	protected static final SimpleMeterRegistry CUSTOM_REGISTRY = new SimpleMeterRegistry();
	protected static final MicrometerListenerFactory<Object> CUSTOM_FACTORY = new MicrometerListenerFactory<Object>() {
		@Override
		protected Clock useClock() {
			return CUSTOM_CLOCK;
		}

		@Override
		protected MeterRegistry useRegistry() {
			return CUSTOM_REGISTRY;
		}
	};
}