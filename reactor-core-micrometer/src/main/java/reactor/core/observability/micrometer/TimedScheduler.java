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
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import reactor.core.Disposable;
import reactor.core.observability.micrometer.SchedulerMeters.SubmittedTags;
import reactor.core.scheduler.Scheduler;

import static reactor.core.observability.micrometer.SchedulerMeters.*;

/**
 * An instrumented {@link Scheduler} wrapping an original {@link Scheduler}
 * and gathering metrics around submitted tasks.
 * <p>
 * See {@link SchedulerMeters} for the various metrics and tags associated with this class.
 *
 * @author Simon Baslé
 */
final class TimedScheduler implements Scheduler {

	final Scheduler delegate;

	final MeterRegistry registry;

	final Counter       submittedDirect;
	final Counter       submittedDelayed;
	final Counter       submittedPeriodicInitial;
	final Counter       submittedPeriodicIteration;
	final LongTaskTimer pendingTasks;
	final LongTaskTimer activeTasks;
	final Timer         completedTasks;


	TimedScheduler(Scheduler delegate, MeterRegistry registry, String metricPrefix, Iterable<Tag> tagsList) {
		this.delegate = delegate;
		this.registry = registry;
		if (!metricPrefix.endsWith(".")) {
			metricPrefix = metricPrefix + ".";
		}
		Tags tags = Tags.of(tagsList);

		String submittedName = SUBMITTED.getName(metricPrefix);
		this.submittedDirect = registry.counter(submittedName, tags.and(SubmittedTags.SUBMISSION.asString(), SubmittedTags.SUBMISSION_DIRECT));
		this.submittedDelayed = registry.counter(submittedName, tags.and(SubmittedTags.SUBMISSION.asString(), SubmittedTags.SUBMISSION_DELAYED));
		this.submittedPeriodicInitial = registry.counter(submittedName, tags.and(SubmittedTags.SUBMISSION.asString(), SubmittedTags.SUBMISSION_PERIODIC_INITIAL));
		this.submittedPeriodicIteration = registry.counter(submittedName, tags.and(SubmittedTags.SUBMISSION.asString(), SubmittedTags.SUBMISSION_PERIODIC_ITERATION));

		this.pendingTasks = LongTaskTimer.builder(TASKS_PENDING.getName(metricPrefix))
			.tags(tags).register(registry);
		this.activeTasks = LongTaskTimer.builder(TASKS_ACTIVE.getName(metricPrefix))
			.tags(tags).register(registry);
		this.completedTasks = registry.timer(TASKS_COMPLETED.getName(metricPrefix), tags);

	}

	Runnable wrap(Runnable task) {
		return new TimedRunnable(registry, this, task);
	}

	Runnable wrapPeriodic(Runnable task) {
		return new TimedRunnable(registry, this, task, true);
	}

	@Override
	public Disposable schedule(Runnable task) {
		this.submittedDirect.increment();
		return delegate.schedule(wrap(task));
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		this.submittedDelayed.increment();
		return delegate.schedule(wrap(task), delay, unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		this.submittedPeriodicInitial.increment();
		return delegate.schedulePeriodically(wrapPeriodic(task), initialDelay, period, unit);
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
			parent.submittedDirect.increment();
			return delegate.schedule(parent.wrap(task));
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			parent.submittedDelayed.increment();
			return delegate.schedule(parent.wrap(task), delay, unit);
		}

		@Override
		public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
			parent.submittedPeriodicInitial.increment();
			return delegate.schedulePeriodically(parent.wrapPeriodic(task), initialDelay, period, unit);
		}
	}

	static final class TimedRunnable implements Runnable {

		final MeterRegistry registry;
		final TimedScheduler parent;
		final Runnable task;

		final LongTaskTimer.Sample pendingSample;

		boolean isRerun;

		TimedRunnable(MeterRegistry registry, TimedScheduler parent, Runnable task) {
			this(registry, parent, task, false);
		}

		TimedRunnable(MeterRegistry registry, TimedScheduler parent, Runnable task, boolean periodic) {
			this.registry = registry;
			this.parent = parent;
			this.task = task;

			if (periodic) {
				this.pendingSample = null;
			}
			else {
				this.pendingSample = parent.pendingTasks.start();
			}
			this.isRerun = false; //will be ignored if not periodic
		}

		@Override
		public void run() {
			if (this.pendingSample != null) {
				//NOT periodic
				this.pendingSample.stop();
			}
			else {
				if (!isRerun) {
					this.isRerun = true;
				}
				else {
					parent.submittedPeriodicIteration.increment();
				}
			}

			Runnable completionTrackingTask = parent.completedTasks.wrap(this.task);
			this.parent.activeTasks.record(completionTrackingTask);
		}
	}
}
