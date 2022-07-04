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

import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.observation.Observation;
import io.micrometer.observation.Observation.Scope;
import io.micrometer.observation.ObservationRegistry;

import reactor.core.observability.SignalListener;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.core.observability.SignalListenerFactory;

public final class Micrometer {

	private static final String SCHEDULERS_DECORATOR_KEY = "reactor.core.observability.micrometer.schedulerDecorator";
	private static MeterRegistry registry = Metrics.globalRegistry;

	/**
	 * The default "name" to use as a prefix for meter or observation IDs if the instrumented sequence doesn't
	 * define a {@link reactor.core.publisher.Flux#name(String) name}.
	 */
	public static final String DEFAULT_METER_PREFIX = "reactor";

	/**
	 * Set the registry to use in reactor for metrics related purposes.
	 * @return the previously configured registry.
	 */
	public static MeterRegistry useMeterRegistry(MeterRegistry newRegistry) {
		MeterRegistry previous = registry;
		registry = newRegistry;
		return previous;
	}

	/**
	 * Get the registry used in reactor for metrics related purposes.
	 */
	public static MeterRegistry getMeterRegistry() {
		return registry;
	}

	/**
	 * A {@link SignalListener} factory that will ultimately produce Micrometer metrics
	 * to the configured default {@link #getMeterRegistry() registry}.
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
	 * A {@link SignalListener} factory that will ultimately produce Micrometer {@link Observation}s
	 * to the provided {@link ObservationRegistry}.
	 * To be used with either the {@link reactor.core.publisher.Flux#tap(SignalListenerFactory)} or
	 * {@link reactor.core.publisher.Mono#tap(SignalListenerFactory)} operator.
	 * <p>
	 * Two Observations are made by this operator:
	 * <ul>
	 *     <li>
	 *         {@code NAME.observation.flow}: one {@link Observation} for the entire length of the sequence, from subscription to termination.
	 *         Termination can be a cancellation, a completion with or without values or an error, which is denoted by the
	 *         {@code status} tag.
	 *     </li>
	 *     <li>
	 *         {@code NAME.observation.values}: one {@link Scope} per each onNext event.
	 *         First scope is opened at subscription, last scope is closed by whichever event terminates the sequence
	 *         (which might skew the count off by one if the sequence is empty, as we need to close that initial subscription
	 *         scope).
	 *     </li>
	 * </ul>
	 * <p>
	 * Common {@link KeyValues} include the low-cardinality {@code type} (are we observing a "Flux" or a "Mono"?)
	 * and {@code status} (what type of even terminated the sequence). It also includes the high cardinality
	 * {@code exception} which contains the name of the exception that terminated the flow in case of an onError.
	 * <p>
	 * Observation names are prefixed by the {@link reactor.core.publisher.Flux#name(String)} defined upstream
	 * of the tap if applicable or by the default prefix {@link #DEFAULT_METER_PREFIX}.
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

	/**
	 * Set-up a decorator that will instrument any {@link ExecutorService} that backs a reactor-core {@link Scheduler}
	 * (or scheduler implementations which use {@link Schedulers#decorateExecutorService(Scheduler, ScheduledExecutorService)}).
	 * <p>
	 * The {@link MeterRegistry} to use can be configured via {@link #useMeterRegistry(MeterRegistry)}
	 * prior to using this method, the default being {@link io.micrometer.core.instrument.Metrics#globalRegistry}.
	 *
	 * @implNote Note that this is added as a decorator via Schedulers when enabling metrics for schedulers,
	 * which doesn't change the Factory.
	 */
	public static void enableSchedulersMetricsDecorator() {
		Schedulers.addExecutorServiceDecorator(SCHEDULERS_DECORATOR_KEY,
			new MicrometerSchedulerMetricsDecorator(getMeterRegistry()));
	}

	/**
	 * If {@link #enableSchedulersMetricsDecorator()} has been previously called, removes the decorator.
	 * No-op if {@link #enableSchedulersMetricsDecorator()} hasn't been called.
	 */
	public static void disableSchedulersMetricsDecorator() {
		Schedulers.removeExecutorServiceDecorator(SCHEDULERS_DECORATOR_KEY);
	}
}