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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

//FIXME javadoc
/**
 * @author Simon Baslé
 */
final class TimedScheduler implements Scheduler {

	//FIXME document somewhere public?
	public static final String METER_SCHEDULER              = "scheduler";
	public static final String METER_IDLE                   = "scheduler.idle";
	public static final String METER_SUBMITTED_ONCE         = "scheduler.submitted.once";
	public static final String METER_SUBMITTED_REPETITIVELY = "scheduler.submitted.repetitively";

	final Scheduler delegate;

	final MeterRegistry registry;
	final Timer         executionTimer;
	final Timer         idleTimer;
	final Counter       scheduledOnce;
	final Counter       scheduledRepetitively;

	TimedScheduler(Scheduler delegate, MeterRegistry registry,  String metricPrefix, Iterable<Tag> tags) {
		this.registry = registry;
		this.delegate = delegate;
		if (!metricPrefix.endsWith(".")) {
			metricPrefix = metricPrefix + ".";
		}
		this.executionTimer = registry.timer(metricPrefix + METER_SCHEDULER, tags);
		this.idleTimer = registry.timer(metricPrefix + METER_IDLE, tags);
		this.scheduledOnce = registry.counter(metricPrefix + METER_SUBMITTED_ONCE, tags);
		this.scheduledRepetitively = registry.counter(metricPrefix + METER_SUBMITTED_REPETITIVELY, tags);
	}

	/**
	 * Return the underlying {@link Scheduler} instance that this {@link TimedScheduler} is wrapping.
	 * Note that this could be a {@link TimedScheduler} itself, although it is discouraged to wrap
	 * these.
	 *
	 * @return the underlying wrapped {@link Scheduler}
	 */
	public Scheduler getDelegate() {
		return delegate;
	}

	private Runnable wrap(Runnable task) {
		return new TimedRunnable(registry, executionTimer, idleTimer, task);
	}

	@Override
	public Disposable schedule(Runnable task) {
		return delegate.schedule(wrap(task));
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		scheduledOnce.increment();
		return delegate.schedule(executionTimer.wrap(task), delay, unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		scheduledRepetitively.increment();
		return delegate.schedulePeriodically(executionTimer.wrap(task), initialDelay, period, unit);
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

	static final class TimedWorker implements Scheduler.Worker {

		final TimedScheduler parent;
		final Scheduler.Worker      delegate;

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
			return delegate.schedule(parent.wrap(task));
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			parent.scheduledOnce.increment();
			return delegate.schedule(parent.executionTimer.wrap(task), delay, unit);
		}

		@Override
		public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
			parent.scheduledRepetitively.increment();
			return delegate.schedulePeriodically(parent.executionTimer.wrap(task), initialDelay, period, unit);
		}
	}

	static final class TimedRunnable implements Runnable {

		final MeterRegistry registry;
		final Timer executionTimer;
		final Timer idleTimer;
		final Runnable task;
		final Timer.Sample idleSample;

		TimedRunnable(MeterRegistry registry, Timer executionTimer, Timer idleTimer, Runnable task) {
			this.registry = registry;
			this.executionTimer = executionTimer;
			this.idleTimer = idleTimer;
			this.task = task;
			this.idleSample = Timer.start(registry);
		}

		@Override
		public void run() {
			idleSample.stop(idleTimer);
			Timer.Sample executionSample = Timer.start(registry);
			try {
				task.run();
			}
			finally {
				executionSample.stop(executionTimer);
			}
		}
	}
}
