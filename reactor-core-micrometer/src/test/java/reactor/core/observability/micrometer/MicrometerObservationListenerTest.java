/*
 * Copyright (c) 2022-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.Clock;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.ValueSource;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.ParameterizedTestWithName;
import reactor.test.StepVerifier;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
			KeyValues.of("testTag1", "testTagValue1", "testTag2", "testTagValue2"),
			registry,
			false);
		subscriberContext = Context.of("contextKey", "contextValue");
	}

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void whenStartedFluxWithDefaultNameWithCustomObservationSupplier(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		configuration = new MicrometerObservationListenerConfiguration(
				MicrometerObservationListenerDocumentation.ANONYMOUS.getName(),
				//note: "type" key is added by MicrometerObservationListenerConfiguration#fromFlux (which is tested separately)
				KeyValues.of("testTag1", "testTagValue1", "testTag2", "testTagValue2"),
				registry,
				false);

		MicrometerObservationListener<Integer> listener =
				new MicrometerObservationListener<>(subscriberContext, configuration,
						or -> Observation.createNotStarted("user.name", or)
						                 .lowCardinalityKeyValue("userType", "admin")
						                 .highCardinalityKeyValue("userId", "testId")
						                 .contextualName("getting-user-name"));

		assertThat(listener.valued).as("valued").isFalse();
		assertThat(listener.tapObservation)
				.as("subscribeToTerminalObservation field")
				.isNotNull();
		assertThat(registry).as("before start").doesNotHaveAnyObservation();

		listener.doFirst(); // forces observation start

		assertThat(registry)
				.hasSingleObservationThat()
				.hasNameEqualTo("user.name")
				.hasContextualNameEqualTo("getting-user-name")
				.as("subscribeToTerminalObservation")
				.hasBeenStarted()
				.isNotStopped()
				.hasLowCardinalityKeyValue("testTag1", "testTagValue1")
				.hasLowCardinalityKeyValue("testTag2", "testTagValue2")
				.hasLowCardinalityKeyValue("userType", "admin")
				.hasHighCardinalityKeyValue("userId", "testId");
	}

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void whenStartedFluxWithDefaultName(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		configuration = new MicrometerObservationListenerConfiguration(
			MicrometerObservationListenerDocumentation.ANONYMOUS.getName(),
			//note: "type" key is added by MicrometerObservationListenerConfiguration#fromFlux (which is tested separately)
			KeyValues.of("testTag1", "testTagValue1", "testTag2", "testTagValue2"),
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

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void whenStartedFluxWithCustomName(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		configuration = new MicrometerObservationListenerConfiguration(
			"testName",
			//note: "type" key is added by MicrometerObservationListenerConfiguration#fromFlux (which is tested separately)
			KeyValues.of("testTag1", "testTagValue1", "testTag2", "testTagValue2"),
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

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void whenStartedMono(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		configuration = new MicrometerObservationListenerConfiguration(
			MicrometerObservationListenerDocumentation.ANONYMOUS.getName(),
			//note: "type" key is added by MicrometerObservationListenerConfiguration#fromMono (which is tested separately)
			KeyValues.of("testTag1", "testTagValue1", "testTag2", "testTagValue2"),
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

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void tapFromFluxWithTags(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
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

	// see https://github.com/reactor/reactor-core/issues/3972
	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void tapMonoCancelAfterNext(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		TestObservationRegistry observationRegistry = TestObservationRegistry.create();
		Mono<Integer> mono = Mono.just(1).tap(Micrometer.observation(observationRegistry));
		StepVerifier.create(Flux.from(mono).take(1).single())
		            .expectNext(1)
		            .expectComplete()
		            .verify();
	}

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void tapFromMonoWithTags(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
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

	private static class CustomConvention implements ObservationConvention<Observation.Context> {

		@Override
		public KeyValues getLowCardinalityKeyValues(Observation.Context context) {
			return KeyValues.of("testTag1", "testTagValue1");
		}

		@Override
		public boolean supportsContext(Observation.Context context) {
			return true;
		}

		@Override
		public String getName() {
			return "myName";
		}

		@Override
		public String getContextualName(Observation.Context context) {
			return "myContextualName";
		}
	}

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void tapFromMonoWithCustomConvention(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		Mono<Integer> mono = Mono.just(1)
		    .name("testMono")
		    .tap(Micrometer.observation(
					registry,
				    observationRegistry -> Observation.createNotStarted(new CustomConvention(), observationRegistry)));

		assertThat(registry).as("before subscription").doesNotHaveAnyObservation();

		mono.block();

		assertThat(registry)
				.hasSingleObservationThat()
				.hasNameEqualTo("myName")
				.hasContextualNameEqualTo("myContextualName")
				.as("subscribeToTerminalObservation")
				.hasBeenStarted()
				.hasBeenStopped()
				.hasLowCardinalityKeyValue("testTag1", "testTagValue1")
				.hasLowCardinalityKeyValue("reactor.type", "Mono")
				.hasLowCardinalityKeyValue("reactor.status",  "completed")
				.hasKeyValuesCount(3);
	}

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void tapFromMonoWithObservationSupplierReturningNull(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		Mono<Integer> mono = Mono.just(1)
		                         .name("testMono")
		                         .tap(Micrometer.observation(
				                         registry,
				                         observationRegistry -> null));

		assertThat(registry).as("before subscription").doesNotHaveAnyObservation();

		mono.block();

		assertThat(registry)
				.hasSingleObservationThat()
				.hasNameEqualTo("testMono")
				.hasContextualNameEqualTo("testMono")
				.as("subscribeToTerminalObservation")
				.hasBeenStarted()
				.hasBeenStopped()
				.hasLowCardinalityKeyValue("reactor.type", "Mono")
				.hasLowCardinalityKeyValue("reactor.status",  "completed")
				.hasKeyValuesCount(2);
	}

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void tapFromMonoWithObservationSupplierThrowingException(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		Mono<Integer> mono = Mono.just(1)
		                         .name("testMono")
		                         .tap(Micrometer.observation(
				                         registry,
				                         observationRegistry -> {
											 throw new IllegalStateException("Exception should be handled");
				                         }));

		assertThat(registry).as("before subscription").doesNotHaveAnyObservation();

		assertThatThrownBy(mono::block)
				.isInstanceOf(IllegalStateException.class);
	}

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void observationStoppedByCancellation(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
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

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void observationStoppedByCompleteEmpty(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
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

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void observationStoppedByCompleteWithValues(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
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

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void observationMonoStoppedByOnNext(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		configuration = new MicrometerObservationListenerConfiguration(
			"valuedMono",
			KeyValues.of("forcedType", "Mono"),
			registry,
			true);

		MicrometerObservationListener<Integer> listener = new MicrometerObservationListener<>(subscriberContext, configuration, null);

		listener.doFirst(); // forces observation start
		listener.doOnNext(1); // emulates onNext, should stop observation

		assertThat(registry)
			.hasSingleObservationThat()
			.hasNameEqualTo("valuedMono")
			.hasContextualNameEqualTo("valuedMono")
			.hasBeenStarted()
			.hasBeenStopped()
			.hasLowCardinalityKeyValue("forcedType", "Mono")
			.hasLowCardinalityKeyValue("reactor.status", "completed")
			.doesNotHaveError();

		// Should this fail, an io.micrometer.observation.tck.InvalidObservationException
		// would have been thrown with
		// "Invalid stop: Observation 'valuedMono' has already been stopped" message
		// (Starting from Micrometer 1.14.0)
		listener.doOnComplete();
	}

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void observationEmptyMonoStoppedByOnComplete(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
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

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void observationStoppedByError(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
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

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void observationGetsParentFromContext(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
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

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void observationHierarchyCreatedInMonoCase(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		Observation parent = Observation.start("testParent", registry);
		Context contextWithAParent = Context.of(subscriberContext).put(ObservationThreadLocalAccessor.KEY, parent);

		AtomicReference<Observation> inner = new AtomicReference<>();
		AtomicReference<Observation> outer = new AtomicReference<>();

		Mono.just("hello")
				.delayElement(Duration.ofMillis(1))
				.handle((v, s) -> {
					inner.set(registry.getCurrentObservation());
					s.next(v);
				})
				.tap(Micrometer.observation(registry))
				.handle((v, s) -> {
					outer.set(registry.getCurrentObservation());
					s.next(v);
				})
				.contextWrite(contextWithAParent)
				.block();

		assertThat(inner.get().getContext().getParentObservation()).isEqualTo(outer.get());
	}

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void observationHierarchyCreatedInFluxCase(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		Observation parent = Observation.start("testParent", registry);
		Context contextWithAParent = Context.of(subscriberContext).put(ObservationThreadLocalAccessor.KEY, parent);

		AtomicReference<Observation> inner = new AtomicReference<>();
		AtomicReference<Observation> outer = new AtomicReference<>();

		Flux.just("hello")
				.delayElements(Duration.ofMillis(1))
				.handle((v, s) -> {
					inner.set(registry.getCurrentObservation());
					s.next(v);
				})
				.tap(Micrometer.observation(registry))
				.handle((v, s) -> {
					outer.set(registry.getCurrentObservation());
					s.next(v);
				})
				.contextWrite(contextWithAParent)
				.blockLast();

		assertThat(inner.get().getContext().getParentObservation()).isEqualTo(outer.get());
	}

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void observationWithEmptyContextHasNoParent(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
		configuration = new MicrometerObservationListenerConfiguration(
			MicrometerObservationListenerDocumentation.ANONYMOUS.getName(),
			//note: "type" key is added by MicrometerObservationListenerConfiguration#fromFlux (which is tested separately)
			KeyValues.of("testTag1", "testTagValue1", "testTag2", "testTagValue2"),
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

	@ParameterizedTestWithName
	@ValueSource(booleans =  {true, false})
	void observationWithEmptyContextHasParentWhenExternalScopeOpened(boolean automatic) {
		if (automatic) {
			Hooks.enableAutomaticContextPropagation();
		}
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