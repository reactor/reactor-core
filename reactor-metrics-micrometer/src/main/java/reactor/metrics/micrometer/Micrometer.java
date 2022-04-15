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

package reactor.metrics.micrometer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.observability.SignalListenerFactory;

public final class Micrometer {

	private static final String SCHEDULERS_DECORATOR_KEY = "reactor.metrics.micrometer.schedulerDecorator";

	private static MeterRegistry registry = Metrics.globalRegistry;

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
	 * A {@link reactor.util.observability.SignalListener} factory that will ultimately produce Micrometer metrics
	 * to the configured default {@link #getRegistry() registry}.
	 * To be used with either the {@link reactor.core.publisher.Flux#listen(SignalListenerFactory)} or
	 * {@link reactor.core.publisher.Mono#listen(SignalListenerFactory)} operator.
	 *
	 * @param <T> the type of onNext in the target publisher
	 * @return a {@link reactor.util.observability.SignalListenerFactory} to record metrics
	 */
	public static <T> SignalListenerFactory<T, ?> metrics() {
		return new MicrometerListenerFactory<>();
	}

	/**
	 * A {@link reactor.util.observability.SignalListener} factory that will ultimately produce Micrometer metrics
	 * to the provided {@link MeterRegistry} using the provided {@link Clock} for timings.
	 * To be used with either the {@link reactor.core.publisher.Flux#listen(SignalListenerFactory)} or
	 * {@link reactor.core.publisher.Mono#listen(SignalListenerFactory)} operator.
	 *
	 * @param <T> the type of onNext in the target publisher
	 * @return a {@link reactor.util.observability.SignalListenerFactory} to record metrics
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