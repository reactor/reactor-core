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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;

import reactor.core.observability.SignalListener;
import reactor.core.observability.SignalListenerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class Micrometer {

	private static final String SCHEDULERS_DECORATOR_KEY = "reactor.core.observability.micrometer.schedulerDecorator";
	private static MeterRegistry registry = Metrics.globalRegistry;

	/**
	 * The default "name" to use as a prefix for meter if the instrumented sequence doesn't define a {@link reactor.core.publisher.Flux#name(String) name}.
	 */
	public static final String DEFAULT_METER_PREFIX = "reactor";

	private static final boolean isTracingAvailable;
	private static final boolean isContextPropagationAvailable;

	static {
		boolean tracing;
		try {
			Class.forName("io.micrometer.tracing.Tracer");
			tracing = true;
		}
		catch (Throwable t) {
			tracing = false;
		}
		isTracingAvailable = tracing;

		boolean contextPropagation;
//		try {
//			io.micrometer.context.ContextRegistry.getInstance();
//			contextPropagation = true;
//		}
//		catch (Throwable t) {
			contextPropagation = isTracingAvailable; //FIXME
//		}
		isContextPropagationAvailable = contextPropagation;
	}

	/**
	 * Indicate if the current runtime supports Micrometer Tracing features.
	 * <p>
	 * This indirectly informs the behavior of {@link #observation(ObservationRegistry)},
	 * since Micrometer Observation will likely discover and load a tracing handler at
	 * runtime.
	 *
	 * @return true if Micrometer Tracing is available, false otherwise
	 */
	public static boolean isTracingAvailable() {
		return isTracingAvailable;
	}

	/**
	 * Indicate if the current runtime supports Micrometer Context Propagation features.
	 * <p>
	 * Context Propagation support impacts the behavior of {@link #observation(ObservationRegistry)},
	 * which uses the feature to propagate Scopes between an observed parent and an observed
	 * child Flux/Mono.
	 * <p>
	 * Please note that Micrometer Core, Metrics and Observation features are always available
	 * as these features are direct dependencies of the reactor-core-micrometer module.
	 *
	 * @return true if Micrometer Context Propagation is available, false otherwise
	 */
	public static boolean isContextPropagationAvailable() {
		return isContextPropagationAvailable;
	}

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
	 * Set-up a decorator that will instrument any {@link ExecutorService} that backs a reactor-core {@link Scheduler}
	 * (or scheduler implementations which use {@link Schedulers#decorateExecutorService(Scheduler, ScheduledExecutorService)}).
	 * <p>
	 * The {@link MeterRegistry} to use can be configured via {@link #useRegistry(MeterRegistry)}
	 * prior to using this method, the default being {@link io.micrometer.core.instrument.Metrics#globalRegistry}.
	 *
	 * @implNote Note that this is added as a decorator via Schedulers when enabling metrics for schedulers,
	 * which doesn't change the Factory.
	 */
	public static void enableSchedulersMetricsDecorator() {
		Schedulers.addExecutorServiceDecorator(SCHEDULERS_DECORATOR_KEY,
			new MicrometerSchedulerMetricsDecorator(getRegistry()));
	}

	/**
	 * If {@link #enableSchedulersMetricsDecorator()} has been previously called, removes the decorator.
	 * No-op if {@link #enableSchedulersMetricsDecorator()} hasn't been called.
	 */
	public static void disableSchedulersMetricsDecorator() {
		Schedulers.removeExecutorServiceDecorator(SCHEDULERS_DECORATOR_KEY);
	}
}