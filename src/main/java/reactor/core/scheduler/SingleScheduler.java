/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Objects;
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
import reactor.util.concurrent.OpenHashSet;

/**
 * Scheduler that works with a single-threaded ScheduledExecutorService and is suited for
 * same-thread work (like an event dispatch thread). This scheduler is
 * {@link Scheduler#isTimeCapable() time-capable}
 */
final class SingleScheduler implements Scheduler, Supplier<ScheduledExecutorService>,
                                       TimedScheduler {

	static final AtomicLong COUNTER       = new AtomicLong();
	static final AtomicLong TIMER_COUNTER = new AtomicLong();

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
	public void shutdown() {
		dispose();
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
	public boolean isTimeCapable() {
		return true;
	}

	@Override
	public Disposable schedule(Runnable task) {
		try {
			return new ExecutorServiceScheduler.DisposableFuture(
					executor.submit(task),
					false);
		}
		catch (RejectedExecutionException ex) {
			return REJECTED;
		}
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		try {
			return new ExecutorServiceScheduler.DisposableFuture(
					executor.schedule(task, delay, unit),
					false);
		}
		catch (RejectedExecutionException ex) {
			return REJECTED;
		}
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		try {
			return new ExecutorServiceScheduler.DisposableFuture(
					executor.scheduleAtFixedRate(task, initialDelay, period, unit),
					false);
		}
		catch (RejectedExecutionException ex) {
			return REJECTED;
		}
	}

	@Override
	public TimedWorker createWorker() {
		return new SingleWorker(executor);
	}

	static final class SingleWorker implements Worker, DisposableContainer<ScheduledRunnable>, TimedWorker {

		final ScheduledExecutorService exec;

		OpenHashSet<ScheduledRunnable> tasks;

		volatile boolean shutdown;

		SingleWorker(ScheduledExecutorService exec) {
			this.exec = exec;
			this.tasks = new OpenHashSet<>();
		}

		@Override
		public boolean isTimeCapable() {
			return true;
		}

		@Override
		public Disposable schedule(Runnable task) {
			return schedule(task, 0L, TimeUnit.MILLISECONDS);
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			if (shutdown) {
				return REJECTED;
			}

			ScheduledRunnable sr = new ScheduledRunnable(task, this);
			add(sr);

			try {
				Future<?> f;
				if (delay <= 0L) {
					f = exec.submit(task);
				}
				else {
					f = exec.schedule(sr, delay, unit);
				}
				sr.setFuture(f);
			}
			catch (RejectedExecutionException ex) {
				sr.dispose();
				return REJECTED;
			}

			return sr;
		}

		@Override
		public Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			if (shutdown) {
				return REJECTED;
			}

			ScheduledRunnable sr = new ScheduledRunnable(task, this);
			add(sr);

			try {
				Future<?> f = exec.scheduleAtFixedRate(sr, initialDelay, period, unit);
				sr.setFuture(f);
			}
			catch (RejectedExecutionException ex) {
				sr.dispose();
				return REJECTED;
			}

			return sr;
		}

		@Override
		public void shutdown() {
			dispose();
		}

		@Override
		public void dispose() {
			if (shutdown) {
				return;
			}
			OpenHashSet<ScheduledRunnable> set;
			synchronized (this) {
				if (shutdown) {
					return;
				}
				shutdown = true;
				set = tasks;
				tasks = null;
			}

			if (set != null && !set.isEmpty()) {
				Object[] a = set.keys();
				for (Object o : a) {
					if (o != null) {
						((ScheduledRunnable) o).dispose();
					}
				}
			}
		}

		@Override
		public boolean isDisposed() {
			return shutdown;
		}

		@Override
		public boolean add(ScheduledRunnable disposable) {
			Objects.requireNonNull(disposable, "disposable is null");
			if (!shutdown) {
				synchronized (this) {
					if (!shutdown) {
						tasks.add(disposable);
						return true;
					}
				}
			}
			disposable.dispose();
			return false;
		}

		@Override
		public boolean remove(ScheduledRunnable task) {
			if (shutdown) {
				return false;
			}

			synchronized (this) {
				if (shutdown) {
					return false;
				}
				tasks.remove(task);
				return true;
			}
		}
	}
}
