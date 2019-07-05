/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.search.Search;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.Scannable.Attr;

import static io.micrometer.core.instrument.Metrics.globalRegistry;

final class SchedulerMetricDecorator
		implements Schedulers.SchedulerExecutorDecorator,
		           Disposable {

	static final String TAG_SCHEDULER_TYPE = "reactor.scheduler.type";
	static final String TAG_SCHEDULER_ID = "reactor.scheduler.id";
	static final String METRICS_DECORATOR_KEY = "reactor.metrics.decorator";

	final Map<String, AtomicInteger> executorDifferentiator  = new HashMap<>();

	@Override
	public synchronized ScheduledExecutorService apply(String schedulerType, String schedulerName, ScheduledExecutorService service) {
		//with https://github.com/reactor/reactor-core/issues/1778 we no longer see the instance
		//(which was problematic due to reference escaping the constructor most of the time)
		//so we can only rely on the NAME, potentially aggregating executors from different instances if their Schedulers
		//are similarly named (eg. parallel using the same parallelism and thread prefix).
		//We can only hope that each NAME is unique enough. Applying the decorator twice will always create duplicate meters.

		String executorId = schedulerName + "-" +
				executorDifferentiator.computeIfAbsent(schedulerName, key -> new AtomicInteger(0))
				                      .getAndIncrement();

		// TODO return the result of ExecutorServiceMetrics#monitor
		//  once ScheduledExecutorService gets supported by Micrometer
		//  See https://github.com/micrometer-metrics/micrometer/issues/1021
		ExecutorServiceMetrics.monitor(globalRegistry, service, executorId,
				Tag.of(TAG_SCHEDULER_ID, schedulerName),
				Tag.of(TAG_SCHEDULER_TYPE, schedulerType));

		return service;
	}

	@Override
	public void dispose() {
		Search.in(globalRegistry)
		      .tagKeys(TAG_SCHEDULER_ID)
		      .meters()
		      .forEach(globalRegistry::remove);

		//note default isDisposed (returning false) is good enough, since the cleared
		//collections can always be reused even though they probably won't
		this.executorDifferentiator.clear();
	}
}
