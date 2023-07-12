/*
 * Copyright (c) 2022-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.function.Function;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import reactor.core.observability.SignalListener;
import reactor.core.observability.SignalListenerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

public final class Micrometer {

	/**
	 * The default "name" to use as a prefix for meter if the instrumented sequence doesn't define a {@link reactor.core.publisher.Flux#name(String) name}.
	 */
	public static final String DEFAULT_METER_PREFIX = "reactor";

	/**
	 * A {@link SignalListener} factory that will ultimately produce Micrometer metrics
	 * to the provided {@link MeterRegistry} (and using the registry's {@link MeterRegistry.Config#clock() configured}
	 * {@link Clock} in case additional timings are needed).
	 * To be used with either the {@link reactor.core.publisher.Flux#tap(SignalListenerFactory)} or
	 * {@link reactor.core.publisher.Mono#tap(SignalListenerFactory)} operator.
	 * <p>
	 * When used in a {@link reactor.core.publisher.Flux#tap(SignalListenerFactory)} operator, meter names use
	 * the {@link reactor.core.publisher.Flux#name(String)} set upstream of the tap as id prefix if applicable
	 * or default to {@link #DEFAULT_METER_PREFIX}. Similarly, upstream tags are gathered and added
	 * to the default set of tags for meters.
	 * See {@link MicrometerMeterListenerDocumentation} for a documentation of the default set of meters and tags.
	 * <p>
	 * Note that some monitoring systems like Prometheus require to have the exact same set of
	 * tags for each meter bearing the same name.
	 *
	 * @param <T> the type of onNext in the target publisher
	 * @param meterRegistry the {@link MeterRegistry} in which to register and publish metrics
	 * @return a {@link SignalListenerFactory} to record metrics
	 * @see MicrometerMeterListenerDocumentation
	 */
	public static <T> SignalListenerFactory<T, ?> metrics(MeterRegistry meterRegistry) {
		return new MicrometerMeterListenerFactory<T>(meterRegistry);
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
	 * See {@link MicrometerObservationListenerDocumentation} for a documentation of the default set of tags.
	 *
	 * @param <T> the type of onNext in the target publisher
	 * @return a {@link SignalListenerFactory} to record observations
	 * @see MicrometerObservationListenerDocumentation
	 */
	public static <T> SignalListenerFactory<T, ?> observation(ObservationRegistry registry) {
		return new MicrometerObservationListenerFactory<>(registry);
	}

	/**
	 * Similar to {@link #observation(ObservationRegistry)} but enables providing
	 * a function creating the Micrometer {@link Observation}
	 * representing the runtime of the publisher to the provided {@link ObservationRegistry}.
	 * If this function returns {@code null}, the behavior will be identical to
	 * {@link #observation(ObservationRegistry)} with a default {@link Observation}.
	 *
	 * @param <T> the type of onNext in the target publisher
	 * @return a {@link SignalListenerFactory} to record observations
	 * @see MicrometerObservationListenerDocumentation
	 */
	public static <T> SignalListenerFactory<T, ?> observation(ObservationRegistry registry,
			Function<ObservationRegistry, Observation> observationSupplier) {
		return new MicrometerObservationListenerFactory<>(registry, observationSupplier);
	}

	/**
	 * Wrap a {@link Scheduler} in an instance that gathers various task-related metrics using
	 * the provided {@link MeterRegistry} and naming meters using the provided {@code metricsPrefix}.
	 * Note that no common tags are set up for these meters.
	 * See {@link TimedSchedulerMeterDocumentation} for a documentation of the default set of meters and tags.
	 *
	 * @param original the original {@link Scheduler} to decorate with metrics
	 * @param meterRegistry the {@link MeterRegistry} in which to register the various meters
	 * @param metricsPrefix the prefix to use in meter names. Must not end with a dot, which is automatically added.
	 * @return a {@link Scheduler} that is instrumented with dedicated metrics
	 * @see TimedSchedulerMeterDocumentation
	 */
	public static Scheduler timedScheduler(Scheduler original, MeterRegistry meterRegistry, String metricsPrefix) {
		return new TimedScheduler(original, meterRegistry, metricsPrefix, Tags.empty());
	}

	/**
	 * Wrap a {@link Scheduler} in an instance that gathers various task-related metrics using
	 * the provided {@link MeterRegistry} and naming meters using the provided {@code metricsPrefix}.
	 * User-provided collection of common tags (ie. {@link Tags}) can also be provided to be added to
	 * all the meters of that timed Scheduler.
	 * See {@link TimedSchedulerMeterDocumentation} for a documentation of the default set of meters and tags.
	 *
	 * @param original the original {@link Scheduler} to decorate with metrics
	 * @param meterRegistry the {@link MeterRegistry} in which to register the various meters
	 * @param metricsPrefix the prefix to use in meter names. Must not end with a dot, which is automatically added.
	 * @param tags the tags to put on meters
	 * @return a {@link Scheduler} that is instrumented with dedicated metrics
	 * @see TimedSchedulerMeterDocumentation
	 */
	public static Scheduler timedScheduler(Scheduler original, MeterRegistry meterRegistry, String metricsPrefix, Iterable<Tag> tags) {
		return new TimedScheduler(original, meterRegistry, metricsPrefix, tags);
	}

    /**
     * Install a {@link Schedulers.Factory} with metrics support. All new schedulers will be instrumented with
	 * the provided {@link MeterRegistry} and naming meters using the provided {@code metricsPrefix}.
     *
     * @param metricsPrefix the prefix to use in meter names. Must not end with a dot, which is automatically added.
     * @param meterRegistry the {@link MeterRegistry} in which to register the various meters
     */
    public static void installSchedulerMetrics(final String metricsPrefix, final MeterRegistry meterRegistry) {
        Schedulers.Factory factory = new Schedulers.Factory() {
            @Override
            public Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
                final Scheduler original = Schedulers.Factory.super.newBoundedElastic(threadCap, queuedTaskCap, threadFactory, ttlSeconds);
                final String simplifiedName = inferSimpleSchedulerName(threadFactory, "boundedElastic");
                return Micrometer.timedScheduler(original, meterRegistry, metricsPrefix + simplifiedName);
            }

            @Override
            public Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
                final Scheduler original = Schedulers.Factory.super.newParallel(parallelism, threadFactory);
                final String simplifiedName = inferSimpleSchedulerName(threadFactory, "parallel");
                return Micrometer.timedScheduler(original, meterRegistry, metricsPrefix + simplifiedName);
            }

            @Override
            public Scheduler newSingle(ThreadFactory threadFactory) {
                final Scheduler original = Schedulers.Factory.super.newSingle(threadFactory);
                final String simplifiedName = inferSimpleSchedulerName(threadFactory, "single");
                return Micrometer.timedScheduler(original, meterRegistry, metricsPrefix + simplifiedName);
            }
        };

        Schedulers.setFactory(factory);
    }

    private static String inferSimpleSchedulerName(ThreadFactory threadFactory, String defaultName) {
        if (!(threadFactory instanceof Supplier)) {
            return defaultName;
        }
        Object supplied = ((Supplier<?>) threadFactory).get();
        if (!(supplied instanceof String)) {
            return defaultName;
        }
        return (String) supplied;
    }

    /**
     * Re-apply default factory to {@link Schedulers}
     */
    public static void uninstallSchedulerMetrics() {
        Schedulers.resetFactory();
    }
}
