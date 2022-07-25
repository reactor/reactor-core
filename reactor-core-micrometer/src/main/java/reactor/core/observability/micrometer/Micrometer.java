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

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import reactor.core.observability.SignalListener;
import reactor.core.observability.SignalListenerFactory;
import reactor.core.scheduler.Scheduler;

public final class Micrometer {

	private static MeterRegistry registry = Metrics.globalRegistry;

	/**
	 * The default "name" to use as a prefix for meter if the instrumented sequence doesn't define a {@link reactor.core.publisher.Flux#name(String) name}.
	 */
	public static final String DEFAULT_METER_PREFIX = "reactor";

	/**
	 * Set the registry to use in reactor-core-micrometer for metrics related purposes.
	 * @return the previously configured registry.
	 * @deprecated in M4, will be removed in M5 / RC1. prefer your own singleton and explicitly
	 * passing the registry to {@link #metrics(MeterRegistry, Clock)}
	 */
	@Deprecated
	public static MeterRegistry useRegistry(MeterRegistry newRegistry) {
		MeterRegistry previous = registry;
		registry = newRegistry;
		return previous;
	}

	/**
	 * Get the registry used in reactor-core-micrometer for metrics related purposes.
	 *
	 * @deprecated in M4, will be removed in M5 / RC1. prefer your own singleton and explicitly
	 * passing the registry to {@link #metrics(MeterRegistry, Clock)}
	 */
	@Deprecated
	public static MeterRegistry getRegistry() {
		return registry;
	}

	/**
	 * A {@link SignalListener} factory that will ultimately produce Micrometer metrics
	 * to the configured default {@link #getRegistry() registry}.
	 * To be used with either the {@link reactor.core.publisher.Flux#tap(SignalListenerFactory)} or
	 * {@link reactor.core.publisher.Mono#tap(SignalListenerFactory)} operator.
	 * <p>
	 * When used in a {@link reactor.core.publisher.Flux#tap(SignalListenerFactory)} operator, meter names use
	 * the {@link reactor.core.publisher.Flux#name(String)} set upstream of the tap as id prefix if applicable
	 * or default to {@link #DEFAULT_METER_PREFIX}. Similarly, upstream tags are gathered and added
	 * to the default set of tags for meters.
	 * <p>
	 * Note that some monitoring systems like Prometheus require to have the exact same set of
	 * tags for each meter bearing the same name.
	 *
	 * @param <T> the type of onNext in the target publisher
	 * @return a {@link SignalListenerFactory} to record metrics
	 * @deprecated in M4, will be removed in M5 / RC1. prefer explicitly passing a registry via {@link #metrics(MeterRegistry, Clock)}
	 */
	@Deprecated
	public static <T> SignalListenerFactory<T, ?> metrics() {
		return new MicrometerMeterListenerFactory<>();
	}

	/**
	 * A {@link SignalListener} factory that will ultimately produce Micrometer metrics
	 * to the provided {@link MeterRegistry} using the provided {@link Clock} for timings.
	 * To be used with either the {@link reactor.core.publisher.Flux#tap(SignalListenerFactory)} or
	 * {@link reactor.core.publisher.Mono#tap(SignalListenerFactory)} operator.
	 * <p>
	 * When used in a {@link reactor.core.publisher.Flux#tap(SignalListenerFactory)} operator, meter names use
	 * the {@link reactor.core.publisher.Flux#name(String)} set upstream of the tap as id prefix if applicable
	 * or default to {@link #DEFAULT_METER_PREFIX}. Similarly, upstream tags are gathered and added
	 * to the default set of tags for meters.
	 * <p>
	 * Note that some monitoring systems like Prometheus require to have the exact same set of
	 * tags for each meter bearing the same name.
	 *
	 * @param <T> the type of onNext in the target publisher
	 * @return a {@link SignalListenerFactory} to record metrics
	 */
	public static <T> SignalListenerFactory<T, ?> metrics(MeterRegistry registry, Clock clock) {
		return new MicrometerMeterListenerFactory<T>() {
			@Override
			protected Clock useClock() {
				return clock;
			}

			@Override
			protected MeterRegistry useRegistry() {
				return registry;
			}
		};
	}

	/**
	 * A {@link SignalListener} factory that will ultimately produce a Micrometer {@link Observation}
	 * representing the runtime of the publisher to the provided {@link ObservationRegistry}.
	 * To be used with either the {@link reactor.core.publisher.Flux#tap(SignalListenerFactory)} or
	 * {@link reactor.core.publisher.Mono#tap(SignalListenerFactory)} operator.
	 * <p>
	 * The {@link Observation} covers the entire length of the sequence, from subscription to termination.
	 * Said termination can be a cancellation, a completion with or without values or an error.
	 * This is denoted by the low cardinality {@code status} {@link KeyValue}.
	 * In case of an exception, a high cardinality {@code exception} KeyValue with the exception class name is also added.
	 * Finally, the low cardinality {@code type} KeyValue informs whether we're observing a {@code Flux}
	 * or a {@code Mono}.
	 * <p>
	 * Note that the Micrometer {@code context-propagation} is used to populate thread locals
	 * around the opening of the observation (upon {@code onSubscribe(Subscription)}).
	 * <p>
	 * The observation is named after the {@link reactor.core.publisher.Flux#name(String)} defined upstream
	 * of the tap if applicable or use {@code "reactor.observation"} otherwise (although it is strongly recommended
	 * to provide a meaningful name).
	 * Similarly, Reactor tags defined upstream via eg. {@link reactor.core.publisher.Flux#tag(String, String)})
	 * are gathered and added to the default set of {@link io.micrometer.common.KeyValues} used by the Observation
	 * as {@link Observation#lowCardinalityKeyValues(KeyValues) low cardinality keyValues}.
	 *
	 *  @param <T> the type of onNext in the target publisher
	 * @return a {@link SignalListenerFactory} to record observations
	 */
	public static <T> SignalListenerFactory<T, ?> observation(ObservationRegistry registry) {
		return new MicrometerObservationListenerFactory<>(registry);
	}

	//FIXME: remove these and replace with an option to decorate an arbitrary Scheduler

	/**
	 * Wrap a {@link Scheduler} in an instance that gather various task-related metrics using
	 * the provided {@link MeterRegistry} and naming meters using the provided {@code metricsPrefix}.
	 * Note that no tags are set up for these meters.
	 *
	 * @param original the original {@link Scheduler} to decorate with metrics
	 * @param meterRegistry the {@link MeterRegistry} in which to register the various meters
	 * @param metricsPrefix the prefix to use in meter names. if needed, a dot is added at the end
	 * @return a {@link Scheduler} that is instrumented with dedicated metrics
	 */
	public static Scheduler timedScheduler(Scheduler original, MeterRegistry meterRegistry, String metricsPrefix) {
		return new TimedScheduler(original, meterRegistry, metricsPrefix, Tags.empty());
	}

	/**
	 * Wrap a {@link Scheduler} in an instance that gather various task-related metrics using
	 * the provided {@link MeterRegistry} and naming meters using the provided {@code metricsPrefix}.
	 * User-provided collection of {@link Tag} (ie. {@link Tags}) can also be provided to be added to
	 * all the meters of that timed Scheduler..
	 *
	 * @param original the original {@link Scheduler} to decorate with metrics
	 * @param meterRegistry the {@link MeterRegistry} in which to register the various meters
	 * @param metricsPrefix the prefix to use in meter names. if needed, a dot is added at the end
	 * @param tags the tags to put on meters
	 * @return a {@link Scheduler} that is instrumented with dedicated metrics
	 */
	public static Scheduler timedScheduler(Scheduler original, MeterRegistry meterRegistry, String metricsPrefix, Iterable<Tag> tags) {
		return new TimedScheduler(original, meterRegistry, metricsPrefix, tags);
	}
}