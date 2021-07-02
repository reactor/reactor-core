/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.search.Search;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.Scannable.Attr;
import reactor.util.Metrics;

final class SchedulerMetricDecorator
			implements BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService>,
			           Disposable {

	static final String TAG_SCHEDULER_ID = "reactor.scheduler.id";
	static final String METRICS_DECORATOR_KEY = "reactor.metrics.decorator";

	final WeakHashMap<Scheduler, String>        seenSchedulers          = new WeakHashMap<>();
	final Map<String, AtomicInteger>            schedulerDifferentiator = new HashMap<>();
	final WeakHashMap<Scheduler, AtomicInteger> executorDifferentiator  = new WeakHashMap<>();
	final MeterRegistry 						registry;

	SchedulerMetricDecorator() {
		registry = Metrics.MicrometerConfiguration.getRegistry();
	}

	@Override
	public synchronized ScheduledExecutorService apply(Scheduler scheduler, ScheduledExecutorService service) {
		//this is equivalent to `toString`, a detailed name like `parallel("foo", 3)`
		String schedulerName = Scannable
				.from(scheduler)
				.scanOrDefault(Attr.NAME, scheduler.getClass().getName());

		//we hope that each NAME is unique enough, but we'll differentiate by Scheduler
		String schedulerId =
				seenSchedulers.computeIfAbsent(scheduler, s -> {
					int schedulerDifferentiator = this.schedulerDifferentiator
							.computeIfAbsent(schedulerName, k -> new AtomicInteger(0))
							.getAndIncrement();

					return (schedulerDifferentiator == 0) ? schedulerName
							: schedulerName + "#" + schedulerDifferentiator;
				});

		//we now want an executorId unique to a given scheduler
		String executorId = schedulerId + "-" +
				executorDifferentiator.computeIfAbsent(scheduler, key -> new AtomicInteger(0))
				                      .getAndIncrement();

		Tags tags = Tags.of(TAG_SCHEDULER_ID, schedulerId);

		/*
		Design note: we assume that a given Scheduler won't apply the decorator twice to the
		same ExecutorService. Even though, it would simply create an extraneous meter for
		that ExecutorService, which we think is not that bad (compared to paying the price
		upfront of also tracking executors instances to deduplicate). The main goal is to
		detect Scheduler instances that have already started decorating their executors,
		in order to avoid consider two calls in a row as duplicates (yet still being able
		to distinguish between two instances with the same name and configuration).
		 */

		class MetricsRemovingScheduledExecutorService extends DelegatingScheduledExecutorService {

			MetricsRemovingScheduledExecutorService() {
				super(ExecutorServiceMetrics.monitor(registry, service, executorId, tags));
			}

			@Override
			public List<Runnable> shutdownNow() {
				removeMetrics();
				return super.shutdownNow();
			}

			@Override
			public void shutdown() {
				removeMetrics();
				super.shutdown();
			}

			void removeMetrics() {
				Search.in(registry)
				      .tag("name", executorId)
				      .meters()
				      .forEach(registry::remove);
			}
		}
		return new MetricsRemovingScheduledExecutorService();
	}

	@Override
	public void dispose() {
		Search.in(registry)
		      .tagKeys(TAG_SCHEDULER_ID)
		      .meters()
		      .forEach(registry::remove);

		//note default isDisposed (returning false) is good enough, since the cleared
		//collections can always be reused even though they probably won't
		this.seenSchedulers.clear();
		this.schedulerDifferentiator.clear();
		this.executorDifferentiator.clear();
	}
}
