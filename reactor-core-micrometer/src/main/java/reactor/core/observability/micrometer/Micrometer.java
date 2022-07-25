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

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import reactor.core.observability.SignalListener;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.core.observability.SignalListenerFactory;

public final class Micrometer {

	private static final String SCHEDULERS_DECORATOR_KEY = "reactor.core.observability.micrometer.schedulerDecorator";
	private static MeterRegistry registry = Metrics.globalRegistry;

	/**
	 * The default "name" to use as a prefix for meter IDs if the instrumented sequence doesn't
	 * define a {@link reactor.core.publisher.Flux#name(String) name}.
	 */
	public static final String DEFAULT_METER_PREFIX = "reactor";

	/**
	 * Set the registry to use in reactor for metrics related purposes.
	 * @return the previously configured registry.
	 */
	public static MeterRegistry useRegistry(MeterRegistry newRegistry) {
		MeterRegistry previous = registry;
		registry = newRegistry;
		return previous;
	}

	/**
	 * Get the registry used in reactor for metrics related purposes.
	 */
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
	 */
	public static <T> SignalListenerFactory<T, ?> metrics() {
		return new MicrometerListenerFactory<>();
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
		return new MicrometerListenerFactory<T>() {
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
	 * Set-up a decorator that will instrument any {@link ExecutorService} that backs a reactor-core {@link Scheduler}
	 * (or scheduler implementations which use {@link Schedulers#decorateExecutorService(Scheduler, ScheduledExecutorService)}).
	 * <p>
	 * The {@link MeterRegistry} to use can be configured via {@link #useRegistry(MeterRegistry)}
	 * prior to using this method, the default being {@link io.micrometer.core.instrument.Metrics#globalRegistry}.
	 *
	 * @implNote Note that this is added as a decorator via Schedulers when enabling metrics for schedulers,
	 * which doesn't change the Factory.
	 *
	 * @deprecated in M4, to be removed in M5. Replace with {@link TimedScheduler#of(Scheduler, String, MeterRegistry, String, Iterable)}
	 * for individual schedulers that you would like to instrument.
	 */
	@Deprecated
	public static void enableSchedulersMetricsDecorator() {
		Schedulers.addExecutorServiceDecorator(SCHEDULERS_DECORATOR_KEY,
			new MicrometerSchedulerMetricsDecorator(getRegistry()));
	}

	/**
	 * If {@link #enableSchedulersMetricsDecorator()} has been previously called, removes the decorator.
	 * No-op if {@link #enableSchedulersMetricsDecorator()} hasn't been called.
	 *
	 * @deprecated in M4, to be removed in M5. Replaced by {@link TimedScheduler}, which doesn't need global disabling.
	 */
	@Deprecated
	public static void disableSchedulersMetricsDecorator() {
		Schedulers.removeExecutorServiceDecorator(SCHEDULERS_DECORATOR_KEY);
	}

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