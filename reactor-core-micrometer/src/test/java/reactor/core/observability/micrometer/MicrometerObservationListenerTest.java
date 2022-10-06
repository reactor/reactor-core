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

import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.Clock;
import io.micrometer.observation.Observation;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class MicrometerObservationListenerTest {

	TestObservationRegistry                    registry;
	AtomicLong                                 virtualClockTime;
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
	void whenStartedFluxWithDefaultName() {
		configuration = new MicrometerObservationListenerConfiguration(
			MicrometerObservationListenerDocumentation.ANONYMOUS.getName(),
			//note: "type" key is added by MicrometerObservationListenerConfiguration#fromFlux (which is tested separately)
			KeyValues.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			false);

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(subscriberContext, configuration);

		assertThat(listener.valued).as("valued").isFalse();
		assertThat(listener.tapObservation)
			.as("subscribeToTerminalObservation field")
			.isNotNull();
		assertThat(registry).as("before start").doesNotHaveAnyObservation();

		listener.doFirst(); // forces observation start

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("reactor.observation")
			.as("subscribeToTerminalObservation")
			.hasBeenStarted()
			.isNotStopped()
			.hasLowCardinalityKeyValue("testTag1", "testTagValue1")
			.hasLowCardinalityKeyValue("testTag2", "testTagValue2");
	}

	@Test
	void whenStartedFluxWithCustomName() {
		configuration = new MicrometerObservationListenerConfiguration(
			"testName",
			//note: "type" key is added by MicrometerObservationListenerConfiguration#fromFlux (which is tested separately)
			KeyValues.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			false);

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(subscriberContext, configuration);

		assertThat(listener.valued).as("valued").isFalse();
		assertThat(listener.tapObservation)
			.as("subscribeToTerminalObservation field")
			.isNotNull();
		assertThat(registry).as("no observation started").doesNotHaveAnyObservation();

		listener.doFirst(); // forces observation start

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("testName")
			.hasContextualNameEqualTo("testName")
			.as("subscribeToTerminalObservation")
			.hasBeenStarted()
			.isNotStopped()
			.hasLowCardinalityKeyValue("testTag1", "testTagValue1")
			.hasLowCardinalityKeyValue("testTag2", "testTagValue2");
	}

	@Test
	void whenStartedMono() {
		configuration = new MicrometerObservationListenerConfiguration(
			MicrometerObservationListenerDocumentation.ANONYMOUS.getName(),
			//note: "type" key is added by MicrometerObservationListenerConfiguration#fromMono (which is tested separately)
			KeyValues.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			true);

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(subscriberContext, configuration);

		assertThat(listener.valued).as("valued").isFalse();
		assertThat(listener.tapObservation)
			.as("subscribeToTerminalObservation field")
			.isNotNull();
		assertThat(registry).as("no observation started").doesNotHaveAnyObservation();

		listener.doFirst(); // forces observation start

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("reactor.observation")
			.hasContextualNameEqualTo("reactor.observation")
			.as("subscribeToTerminalObservation")
			.hasBeenStarted()
			.isNotStopped()
			.hasLowCardinalityKeyValue("testTag1", "testTagValue1")
			.hasLowCardinalityKeyValue("testTag2", "testTagValue2");
	}

	@Test
	void tapFromFluxWithTags() {
		Flux<Integer> flux = Flux.just(1)
			.name("testFlux")
			.tag("testTag1", "testTagValue1")
			.tag("testTag2", "testTagValue2")
			.tap(new MicrometerObservationListenerFactory<>(registry));

		assertThat(registry).as("before subscription").doesNotHaveAnyObservation();

		flux.blockLast();

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("testFlux")
			.hasContextualNameEqualTo("testFlux")
			.as("subscribeToTerminalObservation")
			.hasBeenStarted()
			.hasBeenStopped()
			.hasLowCardinalityKeyValue("testTag1", "testTagValue1")
			.hasLowCardinalityKeyValue("testTag2", "testTagValue2")
			.hasLowCardinalityKeyValue("reactor.type", "Flux")
			.hasLowCardinalityKeyValue("reactor.status",  "completed")
			.hasKeyValuesCount(4);
	}

	@Test
	void tapFromMonoWithTags() {
		Mono<Integer> mono = Mono.just(1)
			.name("testMono")
			.tag("testTag1", "testTagValue1")
			.tag("testTag2", "testTagValue2")
			.tap(new MicrometerObservationListenerFactory<>(registry));

		assertThat(registry).as("before subscription").doesNotHaveAnyObservation();

		mono.block();

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("testMono")
			.hasContextualNameEqualTo("testMono")
			.as("subscribeToTerminalObservation")
			.hasBeenStarted()
			.hasBeenStopped()
			.hasLowCardinalityKeyValue("testTag1", "testTagValue1")
			.hasLowCardinalityKeyValue("testTag2", "testTagValue2")
			.hasLowCardinalityKeyValue("reactor.type", "Mono")
			.hasLowCardinalityKeyValue("reactor.status",  "completed")
			.hasKeyValuesCount(4);
	}

	@Test
	void observationStoppedByCancellation() {
		configuration = new MicrometerObservationListenerConfiguration(
			"flux",
			KeyValues.of("forcedType", "Flux"),
			registry,
			false);

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(subscriberContext, configuration);

		listener.doFirst(); // forces observation start
		listener.doOnCancel(); // stops via cancellation

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("flux")
			.hasContextualNameEqualTo("flux")
			.hasBeenStarted()
			.hasBeenStopped()
			.hasLowCardinalityKeyValue("forcedType", "Flux")
			.hasLowCardinalityKeyValue("reactor.status",  "cancelled")
			.hasKeyValuesCount(2)
			.doesNotHaveError();
	}

	@Test
	void observationStoppedByCompleteEmpty() {
		configuration = new MicrometerObservationListenerConfiguration(
			"emptyFlux",
			KeyValues.of("forcedType", "Flux"),
			registry,
			false);

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(subscriberContext, configuration);

		listener.doFirst(); // forces observation start
		listener.doOnComplete(); // stops via completion (empty)

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("emptyFlux")
			.hasContextualNameEqualTo("emptyFlux")
			.hasBeenStarted()
			.hasBeenStopped()
			.hasLowCardinalityKeyValue("forcedType", "Flux")
			.hasLowCardinalityKeyValue("reactor.status",  "completedEmpty")
			.hasKeyValuesCount(2)
			.doesNotHaveError();
	}

	@Test
	void observationStoppedByCompleteWithValues() {
		configuration = new MicrometerObservationListenerConfiguration(
			"flux",
			KeyValues.of("forcedType", "Flux"),
			registry,
			false);

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(subscriberContext, configuration);

		listener.doFirst(); // forces observation start
		listener.doOnNext(1);
		listener.doOnComplete(); // stops via completion

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("flux")
			.hasContextualNameEqualTo("flux")
			.hasBeenStarted()
			.hasBeenStopped()
			.hasLowCardinalityKeyValue("forcedType", "Flux")
			.hasLowCardinalityKeyValue("reactor.status",  "completed")
			.hasKeyValuesCount(2)
			.doesNotHaveError();
	}

	@Test
	void observationMonoStoppedByOnNext() {
		configuration = new MicrometerObservationListenerConfiguration(
			"valuedMono",
			KeyValues.of("forcedType", "Mono"),
			registry,
			true);

		final String expectedStatus = "completedOnNext";

		//we use a test-oriented constructor to force the onNext completion case to have a different tag value
		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(subscriberContext, configuration, expectedStatus);

		listener.doFirst(); // forces observation start
		listener.doOnNext(1); // emulates onNext, should stop observation

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("valuedMono")
			.hasContextualNameEqualTo("valuedMono")
			.hasBeenStarted()
			.hasBeenStopped()
			.hasLowCardinalityKeyValue("forcedType", "Mono")
			.hasLowCardinalityKeyValue("reactor.status", expectedStatus)
			.doesNotHaveError();

		listener.doOnComplete();

		//if the test in doOnComplete was bugged, the status key would be associated with "completed" now
		assertThat(registry)
			.hasSingleObservationThat()
			.as("post-doOnComplete")
			.hasLowCardinalityKeyValue("reactor.status",  expectedStatus);
	}

	@Test
	void observationEmptyMonoStoppedByOnComplete() {
		configuration = new MicrometerObservationListenerConfiguration(
			"emptyMono",
			KeyValues.of("forcedType", "Mono"),
			registry,
			true);

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(subscriberContext, configuration);

		listener.doFirst(); // forces observation start
		listener.doOnComplete(); // stops via completion (empty)

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("emptyMono")
			.hasContextualNameEqualTo("emptyMono")
			.hasBeenStarted()
			.hasBeenStopped()
			.hasLowCardinalityKeyValue("forcedType", "Mono")
			.hasLowCardinalityKeyValue("reactor.status",  "completedEmpty")
			.doesNotHaveError();
	}

	@Test
	void observationStoppedByError() {
		configuration = new MicrometerObservationListenerConfiguration(
			"errorFlux",
			KeyValues.of("forcedType", "Flux"),
			registry,
			false);
		IllegalStateException exception = new IllegalStateException("observationStoppedByError");

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(subscriberContext, configuration);

		listener.doFirst(); // forces observation start
		listener.doOnError(exception); // stops via onError

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("errorFlux")
			.hasContextualNameEqualTo("errorFlux")
			.hasBeenStarted()
			.hasBeenStopped()
			.hasOnlyKeys("forcedType", "reactor.status")
			.hasLowCardinalityKeyValue("forcedType", "Flux")
			.hasLowCardinalityKeyValue("reactor.status",  "error")
			.hasError(exception);
	}

	@Test
	void observationGetsParentFromContext() {
		configuration = new MicrometerObservationListenerConfiguration(
			MicrometerObservationListenerDocumentation.ANONYMOUS.getName(),
			//note: "type" key is added by MicrometerObservationListenerConfiguration#fromFlux (which is tested separately)
			KeyValues.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			false);

		Observation parent = Observation.start("testParent", registry);
		Context contextWithAParent = Context.of(subscriberContext).put(ObservationThreadLocalAccessor.KEY, parent);
		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(contextWithAParent, configuration);

		//we don't open scopes, but we did however start an observation
		assertThat(registry)
			.satisfies(r -> assertThat(r.getCurrentObservationScope()).as("current scope").isNull())
			.doesNotHaveAnyRemainingCurrentObservation();

		listener.doFirst(); // forces observation start and discovery of the parent from context
		listener.doOnComplete(); // forces observation stop

		assertThat(listener.originalContext).as("originalContext").isSameAs(contextWithAParent);
		assertThat(listener.contextWithObservation).as("contextWithObservation")
			.matches(c -> c.hasKey(ObservationThreadLocalAccessor.KEY), "has OBSERVATION_CONTEXT_KEY");

		assertThat(registry)
			//--
			.hasObservationWithNameEqualTo("testParent")
			.that()
			.isSameAs(parent.getContext())
			.hasBeenStarted()
			.isNotStopped()
			.hasNoKeyValues()
			.backToTestObservationRegistry()
			//--
			.hasObservationWithNameEqualTo("reactor.observation")
			.that()
			.hasBeenStarted()
			.hasBeenStopped()
			.hasLowCardinalityKeyValue("testTag1", "testTagValue1")
			.hasLowCardinalityKeyValue("testTag2", "testTagValue2")
			.hasLowCardinalityKeyValue("reactor.status", "completedEmpty")
			.hasParentObservationEqualTo(parent)
			.backToTestObservationRegistry()
			//--
			.doesNotHaveAnyRemainingCurrentObservation();

		parent.stop();
		assertThat(registry)
			.doesNotHaveAnyRemainingCurrentObservation()
			.satisfies(r -> assertThat(r.getCurrentObservationScope()).as("no leftover currentObservationScope()").isNull());
	}

	@Test
	void observationWithEmptyContextHasNoParent() {
		configuration = new MicrometerObservationListenerConfiguration(
			MicrometerObservationListenerDocumentation.ANONYMOUS.getName(),
			//note: "type" key is added by MicrometerObservationListenerConfiguration#fromFlux (which is tested separately)
			KeyValues.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			false);

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(Context.empty(), configuration);

		//we don't open scopes so it's never stored
		assertThat(registry)
			.satisfies(r -> assertThat(r.getCurrentObservationScope()).as("no current scope").isNull())
			.doesNotHaveAnyRemainingCurrentObservation();

		listener.doFirst(); // forces observation start and discovery of the parent from context
		listener.doOnComplete(); // forces observation stop

		assertThat(listener.contextWithObservation).as("contextWithObservation")
			.matches(c -> c.hasKey(ObservationThreadLocalAccessor.KEY), "has OBSERVATION_CONTEXT_KEY");

		assertThat(registry)
			//--
			.hasSingleObservationThat()
			.hasNameEqualTo("reactor.observation")
			.hasBeenStarted()
			.hasBeenStopped()
			.hasLowCardinalityKeyValue("testTag1", "testTagValue1")
			.hasLowCardinalityKeyValue("testTag2", "testTagValue2")
			.hasLowCardinalityKeyValue("reactor.status", "completedEmpty")
			.doesNotHaveParentObservation()
			.backToTestObservationRegistry()
			//--
			.doesNotHaveAnyRemainingCurrentObservation();

		assertThat(registry)
			.doesNotHaveAnyRemainingCurrentObservation()
			.satisfies(r -> assertThat(r.getCurrentObservationScope()).as("no leftover currentObservationScope()").isNull());
	}

	@Test
	void observationWithEmptyContextHasParentWhenExternalScopeOpened() {
		configuration = new MicrometerObservationListenerConfiguration(
			MicrometerObservationListenerDocumentation.ANONYMOUS.getName(),
			//note: "type" key is added by MicrometerObservationListenerConfiguration#fromFlux (which is tested separately)
			KeyValues.of("testTag1", "testTagValue1","testTag2", "testTagValue2"),
			registry,
			false);

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(Context.empty(), configuration);
		
		Observation parentFromThreadLocal = Observation.start("testParent", registry);
		Observation.Scope parentScope = parentFromThreadLocal.openScope();

		//operator doesn't open scopes but we have
		assertThat(registry)
			.satisfies(r -> assertThat(r.getCurrentObservationScope()).as("has parent scope").isEqualTo(parentScope))
			.hasRemainingCurrentObservationSameAs(parentFromThreadLocal);

		listener.doFirst(); // forces observation start and discovery of the parent from context
		listener.doOnComplete(); // forces observation stop

		assertThat(listener.contextWithObservation).as("contextWithObservation")
			.matches(c -> c.hasKey(ObservationThreadLocalAccessor.KEY), "has OBSERVATION_CONTEXT_KEY");

		assertThat(registry)
			//--
			.hasObservationWithNameEqualTo("testParent")
			.that()
			.isSameAs(parentFromThreadLocal.getContext())
			.hasBeenStarted()
			.isNotStopped()
			.hasNoKeyValues()
			.backToTestObservationRegistry()
			//--
			.hasObservationWithNameEqualTo("reactor.observation")
			.that()
			.hasBeenStarted()
			.hasBeenStopped()
			.hasLowCardinalityKeyValue("testTag1", "testTagValue1")
			.hasLowCardinalityKeyValue("testTag2", "testTagValue2")
			.hasLowCardinalityKeyValue("reactor.status", "completedEmpty")
			.hasParentObservationEqualTo(parentFromThreadLocal);

		parentFromThreadLocal.stop();
		parentScope.close();
		assertThat(registry)
			.doesNotHaveAnyRemainingCurrentObservation()
			.satisfies(r -> assertThat(r.getCurrentObservationScope()).as("no leftover currentObservationScope()").isNull());
	}
}