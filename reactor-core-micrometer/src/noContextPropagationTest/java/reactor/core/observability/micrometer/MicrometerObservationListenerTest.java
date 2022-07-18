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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.Clock;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.ObservationContextAssert;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistryAssert;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
public class MicrometerObservationListenerTest {


	TestObservationRegistry registry;
	AtomicLong              virtualClockTime;
	Clock                                      virtualClock;
	MicrometerObservationListenerConfiguration configuration;
	ContextView                                subscriberContext;

	@BeforeEach
	void initRegistry() {
		registry = TestObservationRegistry.create();
		virtualClockTime = new AtomicLong();
		virtualClock = new Clock() {
			@Override
			public long wallTime() {
				return virtualClockTime.get();
			}

			@Override
			public long monotonicTime() {
				return virtualClockTime.get();
			}
		};
		configuration = new MicrometerObservationListenerConfiguration(
			"testName",
			KeyValues.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			false);
		subscriberContext = Context.of("contextKey", "contextValue");
	}

	@Test
	void whenNoPropagationGetsParentFromContext() {
		configuration = new MicrometerObservationListenerConfiguration(
			MicrometerObservationListener.ANONYMOUS_OBSERVATION,
			//note: "type" key is added by MicrometerObservationListenerConfiguration#fromFlux (which is tested separately)
			KeyValues.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			false);

		//detached
		TestObservationRegistry otherRegistry = TestObservationRegistry.create();
		Observation parent = Observation.start("testParent", otherRegistry);
		parent.openScope();

		//attached to test registry
		Observation inRegistry = Observation.start("inregistry", registry);
		inRegistry.openScope();
		Context contextWithAParent = Context.of(subscriberContext).put(Micrometer.OBSERVATION_CONTEXT_KEY, parent);

		//NB: registry.getCurrentObservation() always reflects the last Observation.openScope, even if Observation was started from a different registry
		assertThat(registry.getCurrentObservation()).as("inRegistry is reported as current / ThreadLocal")
			.isSameAs(otherRegistry.getCurrentObservation())
			.isSameAs(inRegistry);

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(contextWithAParent, configuration);

		listener.doFirst(); // forces observation start and discovery of the parent

		assertThat(listener.originalContext).as("originalContext").isSameAs(contextWithAParent);
		assertThat(listener.contextWithScope).as("contextWithScope")
				.matches(c -> c.hasKey(Micrometer.OBSERVATION_CONTEXT_KEY), "has OBSERVATION_CONTEXT_KEY");

		AtomicReference<Observation.Context> parentRef = new AtomicReference<>();
		AtomicReference<Observation.Context> inRegistryRef = new AtomicReference<>();

		assertThat(otherRegistry)
			.hasSingleObservationThat()
			.hasNameEqualTo("testParent")
			.hasBeenStarted()
			.isNotStopped()
			.hasNoKeyValues()
			.satisfies(parentRef::set);
		assertThat(registry)
			.hasObservationWithNameEqualTo("inregistry")
			.that()
			.satisfies(inRegistryRef::set);

		assertThat(registry)
			.as("subscribeToTerminalObservation")
			.hasObservationWithNameEqualTo("reactor.observation")
			.that()
			.hasBeenStarted()
			.isNotStopped()
			.hasLowCardinalityKeyValue("testTag1", "testTagValue1")
			.hasLowCardinalityKeyValue("testTag2", "testTagValue2")
			.satisfies(o -> {
				assertThat(o.getParentObservation())
					.as("parentObservation()")
					.isNotNull();
				assertThat(o.getParentObservation().getContext())
					.as("parent")
					.isSameAs(parentRef.get())
					.isNotSameAs(inRegistryRef.get());
			});
	}
}
