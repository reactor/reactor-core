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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.search.Search;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;

/**
 * @author Simon Baslé
 */
@Deprecated
final class MicrometerSchedulerMetricsDecorator implements BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService>,
	Disposable {

	//TODO expose keys and tags publicly?
	static final String TAG_SCHEDULER_ID = "reactor.scheduler.id";

	final WeakHashMap<Scheduler, String>        seenSchedulers          = new WeakHashMap<>();
	final Map<String, AtomicInteger>            schedulerDifferentiator = new HashMap<>();
	final WeakHashMap<Scheduler, AtomicInteger> executorDifferentiator  = new WeakHashMap<>();
	final MeterRegistry                         registry;

	MicrometerSchedulerMetricsDecorator(MeterRegistry registry) {
		this.registry = registry;
	}

	@Override
	public synchronized ScheduledExecutorService apply(Scheduler scheduler, ScheduledExecutorService service) {
		//this is equivalent to `toString`, a detailed name like `parallel("foo", 3)`
		String schedulerName = Scannable
			.from(scheduler)
			.scanOrDefault(Scannable.Attr.NAME, scheduler.getClass().getName());

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

		Tag[] tags = new Tag[] { Tag.of(TAG_SCHEDULER_ID, schedulerId) };

		/*
		Design note: we assume that a given Scheduler won't apply the decorator twice to the
		same ExecutorService. Even though, it would simply create an extraneous meter for
		that ExecutorService, which we think is not that bad (compared to paying the price
		upfront of also tracking executors instances to deduplicate). The main goal is to
		detect Scheduler instances that have already started decorating their executors,
		in order to avoid consider two calls in a row as duplicates (yet still being able
		to distinguish between two instances with the same name and configuration).
		 */


		return new MetricsRemovingScheduledExecutorService(service, this.registry, executorId, tags);
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

	static class MetricsRemovingScheduledExecutorService implements ScheduledExecutorService {

		final ScheduledExecutorService scheduledExecutorService;
		final MeterRegistry registry;
		final String executorId;

		MetricsRemovingScheduledExecutorService(ScheduledExecutorService service, MeterRegistry registry, String executorId, Tag[] tags) {
			this.scheduledExecutorService = ExecutorServiceMetrics.monitor(registry, service, executorId, tags);
			this.registry = registry;
			this.executorId = executorId;
		}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
			return scheduledExecutorService.awaitTermination(timeout, unit);
		}

		@Override
		public void execute(Runnable command) {
			scheduledExecutorService.execute(command);
		}

		@Override
		public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
			return scheduledExecutorService.invokeAll(tasks);
		}

		@Override
		public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
			return scheduledExecutorService.invokeAll(tasks, timeout, unit);
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
																			   ExecutionException {
			return scheduledExecutorService.invokeAny(tasks);
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
			return scheduledExecutorService.invokeAny(tasks, timeout, unit);
		}

		@Override
		public boolean isShutdown() {
			return scheduledExecutorService.isShutdown();
		}

		@Override
		public boolean isTerminated() {
			return scheduledExecutorService.isTerminated();
		}

		@Override
		public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
			return scheduledExecutorService.schedule(command, delay, unit);
		}

		@Override
		public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
			return scheduledExecutorService.schedule(callable, delay, unit);
		}

		@Override
		public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
			return scheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, unit);
		}

		@Override
		public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
			return scheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
		}

		@Override
		public List<Runnable> shutdownNow() {
			removeMetrics();
			return scheduledExecutorService.shutdownNow();
		}

		@Override
		public void shutdown() {
			removeMetrics();
			scheduledExecutorService.shutdown();
		}

		@Override
		public <T> Future<T> submit(Callable<T> task) {
			return scheduledExecutorService.submit(task);
		}

		@Override
		public <T> Future<T> submit(Runnable task, T result) {
			return scheduledExecutorService.submit(task, result);
		}

		@Override
		public Future<?> submit(Runnable task) {
			return scheduledExecutorService.submit(task);
		}

		void removeMetrics() {
			Search.in(registry)
				.tag("name", executorId)
				.meters()
				.forEach(registry::remove);
		}
	}
}
