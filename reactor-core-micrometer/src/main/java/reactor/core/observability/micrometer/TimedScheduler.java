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

import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/**
 * An instrumented {@link Scheduler} wrapping an original {@link Scheduler}
 * and gathering metrics around submitted tasks.
 *
 * @author Simon Baslé
 */
final class TimedScheduler implements Scheduler {

	//FIXME document all the tags/meters/etc... somewhere public?

	/**
	 * {@link Timer} reflecting tasks that have finished execution. Note that this reflects all types of
	 * active tasks, including tasks scheduled {@link #schedule(Runnable, long, TimeUnit) with a delay}
	 * or {@link #schedulePeriodically(Runnable, long, long, TimeUnit) periodically} (each
	 * iteration being considered a separate completed task).
	 */
	static final String METER_TASKS_COMPLETED = "scheduler.tasks.completed";
	/**
	 * {@link LongTaskTimer} reflecting tasks currently running. Note that this reflects all types of
	 * active tasks, including tasks scheduled {@link #schedule(Runnable, long, TimeUnit) with a delay}
	 * or {@link #schedulePeriodically(Runnable, long, long, TimeUnit) periodically} (each
	 * iteration being considered an active task).
	 */
	static final String METER_TASKS_ACTIVE    = "scheduler.tasks.active";
	/**
	 * {@link LongTaskTimer} reflecting tasks that were submitted for immediate execution but
	 * couldn't be started immediately because the scheduler is already at max capacity.
	 * Note that only immediate submissions via {@link Scheduler#schedule(Runnable)} and
	 * {@link Worker#schedule(Runnable)} are considered.
	 */
	static final String METER_TASKS_PENDING   = "scheduler.tasks.pending";
	/**
	 * {@link Counter} that increments by one each time a task is initially submitted (via any of the
	 * schedule methods on both {@link Scheduler} and {@link Worker}).
	 * <p>
	 * Note that due to periodical tasks not being counted on each iteration, this counter
	 * cannot really be compared with the {@link #METER_TASKS_COMPLETED} counter.
	 */
	static final String METER_SUBMITTED = "scheduler.tasks.submitted";
	/**
	 * {@link Counter} that increments by one each time a task is submitted with a delay
	 * (ie. {@link Scheduler#schedule(Runnable, long, TimeUnit)}
	 * or {@link Worker#schedule(Runnable, long, TimeUnit)}).
	 */
	static final String METER_SUBMITTED_DELAYED      = "scheduler.tasks.submitted.delayed";
	/**
	 * {@link Counter} that increments by one each time a task is submitted with a period
	 * (ie. {@link Scheduler#schedulePeriodically(Runnable, long, long, TimeUnit)}
	 * or {@link Worker#schedulePeriodically(Runnable, long, long, TimeUnit)}). This isn't
	 * incremented on each re-run of the periodical task.
	 */
	static final String METER_SUBMITTED_PERIODICALLY = "scheduler.tasks.submitted.periodically";

	final Scheduler delegate;

	final MeterRegistry registry;

	final Counter       submittedTotal;
	final Counter       submittedDelayed;
	final Counter       submittedPeriodically;
	final LongTaskTimer pendingTasks;
	final LongTaskTimer activeTasks;
	final Timer         completedTasks;


	TimedScheduler(Scheduler delegate, MeterRegistry registry,  String metricPrefix, Iterable<Tag> tags) {
		this.registry = registry;
		this.delegate = delegate;
		if (!metricPrefix.endsWith(".")) {
			metricPrefix = metricPrefix + ".";
		}

		this.submittedTotal = registry.counter(metricPrefix + METER_SUBMITTED, tags);
		this.submittedDelayed = registry.counter(metricPrefix + METER_SUBMITTED_DELAYED, tags);
		this.submittedPeriodically = registry.counter(metricPrefix + METER_SUBMITTED_PERIODICALLY, tags);

		this.pendingTasks = LongTaskTimer.builder(metricPrefix + METER_TASKS_PENDING)
			.tags(tags).register(registry);
		this.activeTasks = LongTaskTimer.builder(metricPrefix + METER_TASKS_ACTIVE)
			.tags(tags).register(registry);
		this.completedTasks = registry.timer(metricPrefix + METER_TASKS_COMPLETED, tags);

	}

	Runnable wrap(Runnable task) {
		return new TimedRunnable(registry, this, task);
	}

	@Override
	public Disposable schedule(Runnable task) {
		this.submittedTotal.increment();
		return delegate.schedule(wrap(task));
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		this.submittedTotal.increment();
		this.submittedDelayed.increment();
		return delegate.schedule(wrap(task), delay, unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		this.submittedTotal.increment();
		this.submittedPeriodically.increment();
		return delegate.schedulePeriodically(wrap(task), initialDelay, period, unit);
	}

	@Override
	public Worker createWorker() {
		return new TimedWorker(this, delegate.createWorker());
	}

	@Override
	public boolean isDisposed() {
		return delegate.isDisposed();
	}

	@Override
	public long now(TimeUnit unit) {
		return delegate.now(unit);
	}

	@Override
	public void dispose() {
		delegate.dispose();
	}

	@Override
	public void start() {
		delegate.start();
	}

	static final class TimedWorker implements Worker {

		final TimedScheduler parent;
		final Worker      delegate;

		TimedWorker(TimedScheduler parent, Worker delegate) {
			this.parent = parent;
			this.delegate = delegate;
		}

		@Override
		public void dispose() {
			delegate.dispose();
		}

		@Override
		public boolean isDisposed() {
			return delegate.isDisposed();
		}

		@Override
		public Disposable schedule(Runnable task) {
			parent.submittedTotal.increment();
			return delegate.schedule(parent.wrap(task));
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			parent.submittedTotal.increment();
			parent.submittedDelayed.increment();
			return delegate.schedule(parent.wrap(task), delay, unit);
		}

		@Override
		public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
			parent.submittedTotal.increment();
			parent.submittedPeriodically.increment();
			return delegate.schedulePeriodically(parent.wrap(task), initialDelay, period, unit);
		}
	}

	static final class TimedRunnable implements Runnable {

		final MeterRegistry registry;
		final TimedScheduler parent;
		final Runnable task;

		final LongTaskTimer.Sample pendingSample;

		TimedRunnable(MeterRegistry registry, TimedScheduler parent, Runnable task) {
			this.registry = registry;
			this.parent = parent;
			this.task = task;

			this.pendingSample = parent.pendingTasks.start();
		}

		@Override
		public void run() {
			this.pendingSample.stop();
			Runnable completionTrackingTask = parent.completedTasks.wrap(this.task);
			this.parent.activeTasks.record(completionTrackingTask);
		}
	}
}
