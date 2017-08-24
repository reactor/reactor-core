/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;

/**
 * Scheduler that works with a single-threaded ScheduledExecutorService and is suited for
 * same-thread work (like an event dispatch thread). This scheduler is time-capable (can
 * schedule with delay / periodically).
 */
final class SingleScheduler implements Scheduler, Supplier<ScheduledExecutorService> {

	static final AtomicLong COUNTER       = new AtomicLong();

	final ThreadFactory factory;

	volatile ScheduledExecutorService executor;
	static final AtomicReferenceFieldUpdater<SingleScheduler, ScheduledExecutorService> EXECUTORS =
			AtomicReferenceFieldUpdater.newUpdater(SingleScheduler.class,
					ScheduledExecutorService.class,
					"executor");

	static final ScheduledExecutorService TERMINATED;

	static {
		TERMINATED = Executors.newSingleThreadScheduledExecutor();
		TERMINATED.shutdownNow();
	}

	SingleScheduler(ThreadFactory factory) {
		this.factory = factory;
		init();
	}

	/**
	 * Instantiates the default {@link ScheduledExecutorService} for the SingleScheduler
	 * ({@code Executors.newSingleThreadExecutor}).
	 */
	@Override
	public ScheduledExecutorService get() {
		ScheduledExecutorService e = Executors.newScheduledThreadPool(1, this.factory);
		((ScheduledThreadPoolExecutor) e).setRemoveOnCancelPolicy(true);
		return e;
	}

	private void init() {
		EXECUTORS.lazySet(this,
				Schedulers.decorateScheduledExecutorService(Schedulers.SINGLE, this));
	}

	@Override
	public boolean isDisposed() {
		return executor == TERMINATED;
	}

	@Override
	public void start() {
		//TODO SingleTimedScheduler didn't implement start, check if any particular reason?
		ScheduledExecutorService b = null;
		for (; ; ) {
			ScheduledExecutorService a = executor;
			if (a != TERMINATED) {
				if (b != null) {
					b.shutdownNow();
				}
				return;
			}

			if (b == null) {
				b = Schedulers.decorateScheduledExecutorService(Schedulers.SINGLE, this);
			}

			if (EXECUTORS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	@Override
	public void dispose() {
		ScheduledExecutorService a = executor;
		if (a != TERMINATED) {
			a = EXECUTORS.getAndSet(this, TERMINATED);
			if (a != TERMINATED) {
				Schedulers.executorServiceShutdown(a, Schedulers.SINGLE);
			}
		}
	}

	@Override
	public Disposable schedule(Runnable task) {
		//RejectedExecutionException are propagated up
		return new ExecutorServiceScheduler.DisposableFuture(
				executor.submit(task),
				false);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		//RejectedExecutionException are propagated up
		return new ExecutorServiceScheduler.DisposableFuture(
				executor.schedule(task, delay, unit),
				false);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		//RejectedExecutionException are propagated up
		return new ExecutorServiceScheduler.DisposableFuture(
				executor.scheduleAtFixedRate(task, initialDelay, period, unit),
				false);
	}

	@Override
	public Worker createWorker() {
		return new SingleWorker(executor);
	}

	static final class SingleWorker implements Worker, Disposable {

		final ScheduledExecutorService exec;

		final Disposable.Composite tasks;

		SingleWorker(ScheduledExecutorService exec) {
			this.exec = exec;
			this.tasks = Disposables.composite();
		}

		@Override
		public Disposable schedule(Runnable task) {
			return schedule(task, 0L, TimeUnit.MILLISECONDS);
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			ScheduledRunnable sr = new ScheduledRunnable(task, tasks);
			if(!tasks.add(sr)){
				throw Exceptions.failWithRejected();
			}

			try {
				Future<?> f;
				if (delay <= 0L) {
					f = exec.submit(sr);
				}
				else {
					f = exec.schedule(sr, delay, unit);
				}
				sr.setFuture(f);
			}
			catch (RejectedExecutionException ex) {
				sr.dispose();
				//RejectedExecutionException are propagated up
				throw ex;
			}

			return sr;
		}

		@Override
		public Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {

			ScheduledRunnable sr = new ScheduledRunnable(task, tasks);
			if(!tasks.add(sr)) {
				throw Exceptions.failWithRejected();
			}

			try {
				Future<?> f = exec.scheduleAtFixedRate(sr, initialDelay, period, unit);
				sr.setFuture(f);
			}
			catch (RejectedExecutionException ex) {
				sr.dispose();
				//RejectedExecutionException are propagated up
				throw ex;
			}

			return sr;
		}

		@Override
		public void dispose() {
			tasks.dispose();
		}

		@Override
		public boolean isDisposed() {
			return tasks.isDisposed();
		}
	}
}
