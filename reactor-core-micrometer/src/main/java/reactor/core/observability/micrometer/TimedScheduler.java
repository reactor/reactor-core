/*
 * Copyright (c) 2022-2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.observability.micrometer.TimedSchedulerMeterDocumentation.SubmittedTags;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

import static reactor.core.observability.micrometer.TimedSchedulerMeterDocumentation.*;

/**
 * An instrumented {@link Scheduler} wrapping an original {@link Scheduler}
 * and gathering metrics around submitted tasks.
 * <p>
 * See {@link TimedSchedulerMeterDocumentation} for the various metrics and tags associated with this class.
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
		if (metricPrefix.endsWith(".")) {
			metricPrefix = metricPrefix.substring(0, metricPrefix.length() - 1);
		}
		Tags tags = Tags.of(tagsList);

		String submittedName = TASKS_SUBMITTED.getName(metricPrefix);
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

	TimedRunnable wrap(Runnable task) {
		return new SchedulerBackedTimedRunnable(registry, this, delegate, task);
	}

	TimedRunnable wrapPeriodic(Runnable task) {
		return new SchedulerBackedTimedRunnable(registry, this, delegate, task, true);
	}

	@Override
	public Disposable schedule(Runnable task) {
		TimedRunnable timedTask = wrap(task);

		return timedTask.schedule();
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		TimedRunnable timedTask = wrap(task);

		return timedTask.schedule(delay, unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		TimedRunnable timedTask = wrapPeriodic(task);

		return timedTask.schedulePeriodically(initialDelay, period, unit);
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

	@Override
	public void init() {
		delegate.init();
	}

	static final class TimedWorker implements Worker {

		final TimedScheduler parent;
		final Worker      delegate;

		/**
		 * As this Worker creates {@link TimedRunnable} instances which are {@link Disposable}
		 * it needs to keep track of them to be able to dispose them when this instance
		 * is {@link #dispose() disposed}.
		 */
		final Composite disposables;

		TimedWorker(TimedScheduler parent, Worker delegate) {
			this.parent = parent;
			this.delegate = delegate;
			this.disposables = Disposables.composite();
		}

		TimedRunnable wrap(Runnable task) {
			return new WorkerBackedTimedRunnable(parent.registry, parent, delegate,
					task, disposables);
		}

		TimedRunnable wrapPeriodic(Runnable task) {
			return new WorkerBackedTimedRunnable(parent.registry, parent, delegate,
					task, disposables, true);
		}

		@Override
		public void dispose() {
			disposables.dispose();
			delegate.dispose();
		}

		@Override
		public boolean isDisposed() {
			return delegate.isDisposed();
		}

		@Override
		public Disposable schedule(Runnable task) {
			TimedRunnable timedTask = wrap(task);
			disposables.add(timedTask);

			return timedTask.schedule();
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			TimedRunnable timedTask = wrap(task);
			disposables.add(timedTask);

			return timedTask.schedule(delay, unit);
		}

		@Override
		public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
			TimedRunnable timedTask = wrapPeriodic(task);
			disposables.add(timedTask);

			return timedTask.schedulePeriodically(initialDelay, period, unit);
		}
	}

	private static abstract class TimedRunnable implements Runnable, Disposable {
		/** marker that the Worker was disposed and the parent got notified */
		static final Composite DISPOSED = new EmptyCompositeDisposable();
		/** marker that the Worker has completed, for the PARENT field */
		static final Composite DONE     = new EmptyCompositeDisposable();

		final MeterRegistry  registry;
		final TimedScheduler timedScheduler;
		final Runnable       task;

		final LongTaskTimer.Sample pendingSample;

		boolean isRerun;

		Disposable disposable;

		volatile Composite parent;
		static final AtomicReferenceFieldUpdater<TimedRunnable, Composite> PARENT =
				AtomicReferenceFieldUpdater.newUpdater(TimedRunnable.class, Composite.class, "parent");

		TimedRunnable(MeterRegistry registry, TimedScheduler timedScheduler, Runnable task,
				@Nullable Composite parent) {
			this(registry, timedScheduler, task, parent, false);
		}

		TimedRunnable(MeterRegistry registry, TimedScheduler timedScheduler, Runnable task,
				@Nullable Composite parent, boolean periodic) {
			this.registry = registry;
			this.timedScheduler = timedScheduler;
			this.task = task;

			if (periodic) {
				this.pendingSample = null;
			}
			else {
				this.pendingSample = timedScheduler.pendingTasks.start();
			}
			this.isRerun = false; //will be ignored if not periodic
			PARENT.lazySet(this, parent);
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
					timedScheduler.submittedPeriodicIteration.increment();
				}
			}

			try {
				Runnable completionTrackingTask = timedScheduler.completedTasks.wrap(this.task);
				this.timedScheduler.activeTasks.record(completionTrackingTask);
			} finally {
				Composite o = parent;
				if (o != DISPOSED && PARENT.compareAndSet(this, o, DONE) && o != null) {
					o.remove(this);
				}
			}
		}

		public Disposable schedule() {
			timedScheduler.submittedDirect.increment();

			try {
				disposable = this.internalSchedule();
				return this;
			} catch (RejectedExecutionException exception) {
				this.dispose();
				throw exception;
			}
		}

		public Disposable schedule(long delay, TimeUnit unit) {
			timedScheduler.submittedDelayed.increment();

			try {
				disposable = this.internalSchedule(delay, unit);
				return this;
			} catch (RejectedExecutionException exception) {
				this.dispose();
				throw exception;
			}
		}

		public Disposable schedulePeriodically(long initialDelay, long period, TimeUnit unit) {
			timedScheduler.submittedPeriodicInitial.increment();
			return this.internalSchedulePeriodically(initialDelay, period, unit);
		}

		@Override
		public void dispose() {
			if (disposable != null) {
				disposable.dispose();
			}

			if (pendingSample != null) {
				pendingSample.stop();
			}

			for (;;) {
				Composite o = parent;
				if (o == DONE || o == DISPOSED || o == null) {
					return;
				}
				if (PARENT.compareAndSet(this, o, DISPOSED)) {
					o.remove(this);
					return;
				}
			}
		}

		@Override
		public boolean isDisposed() {
			Composite o = PARENT.get(this);
			return o == DISPOSED || o == DONE;
		}

		abstract Disposable internalSchedule();

		abstract Disposable internalSchedule(long delay, TimeUnit unit);

		abstract Disposable internalSchedulePeriodically(long initialDelay, long period, TimeUnit unit);
	}

	static final class WorkerBackedTimedRunnable extends TimedRunnable {

		final Worker worker;

		WorkerBackedTimedRunnable(MeterRegistry registry, TimedScheduler timedScheduler,
				Worker worker, Runnable task, Composite parent) {
			super(registry, timedScheduler, task, parent);
			this.worker = worker;
		}

		WorkerBackedTimedRunnable(MeterRegistry registry, TimedScheduler timedScheduler,
				Worker worker, Runnable task, Composite parent, boolean periodic) {
			super(registry, timedScheduler, task, parent, periodic);
			this.worker = worker;
		}

		@Override
		Disposable internalSchedule() {
			return worker.schedule(this);
		}

		@Override
		Disposable internalSchedule(long delay, TimeUnit unit) {
			return worker.schedule(this, delay, unit);
		}

		@Override
		Disposable internalSchedulePeriodically(long initialDelay, long period, TimeUnit unit) {
			return worker.schedulePeriodically(this, initialDelay, period, unit);
		}
	}

	static final class SchedulerBackedTimedRunnable extends TimedRunnable {

		final Scheduler scheduler;

		SchedulerBackedTimedRunnable(MeterRegistry registry, TimedScheduler timedScheduler,
				Scheduler scheduler, Runnable task) {
			super(registry, timedScheduler, task, null);
			this.scheduler = scheduler;
		}

		SchedulerBackedTimedRunnable(MeterRegistry registry, TimedScheduler timedScheduler,
				Scheduler scheduler, Runnable task, boolean periodic) {
			super(registry, timedScheduler, task, null, periodic);
			this.scheduler = scheduler;
		}

		@Override
		Disposable internalSchedule() {
			return scheduler.schedule(this);
		}

		@Override
		Disposable internalSchedule(long delay, TimeUnit unit) {
			return scheduler.schedule(this, delay, unit);
		}

		@Override
		Disposable internalSchedulePeriodically(long initialDelay, long period, TimeUnit unit) {
			return scheduler.schedulePeriodically(this, initialDelay, period, unit);
		}
	}

	/**
	 * Copy of reactor.core.scheduler.EmptyCompositeDisposable for internal use.
	 */
	static final class EmptyCompositeDisposable implements Disposable.Composite {

		@Override
		public boolean add(Disposable d) {
			return false;
		}

		@Override
		public boolean addAll(Collection<? extends Disposable> ds) {
			return false;
		}

		@Override
		public boolean remove(Disposable d) {
			return false;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public void dispose() {
		}

		@Override
		public boolean isDisposed() {
			return false;
		}

	}
}
