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

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import reactor.core.Disposable;
import reactor.util.Logger;
import reactor.util.Loggers;

final class ForkJoinPoolScheduler implements Scheduler {

	private final ForkJoinPool             pool;
	private final ScheduledExecutorService scheduler;
	/**
	 * Construct a new instance
	 *
	 * @param parallelism Parallelism. Number of fork-join pool threads. See {@link
	 * ForkJoinPool}
	 * @param workerThreadFactory Thread factory for fork-join worker threads
	 * @param threadFactory Thread factory to use for constructing the single threaded
	 * {@link ScheduledExecutorService} used for time-based scheduling.
	 */
	ForkJoinPoolScheduler(int parallelism,
			ForkJoinPool.ForkJoinWorkerThreadFactory workerThreadFactory,
			ThreadFactory threadFactory) {
		pool = new ForkJoinPool(parallelism,
				workerThreadFactory,
				this::uncaughtException,
				true);
		scheduler = new ScheduledThreadPoolExecutor(1, threadFactory);
	}

	@Override
	public Scheduler.Worker createWorker() {
		return new Worker(pool, scheduler);
	}

	@Override
	public void dispose() {
		Schedulers.executorServiceShutdown(pool, FORK_JOIN_POOL);
		Schedulers.executorServiceShutdown(scheduler, FORK_JOIN_POOL_TIMER);
	}

	@Override
	public boolean isDisposed() {
		return pool.isShutdown();
	}

	@Override
	public Disposable schedule(Runnable runnable) {
		try {
			return new DisposableForkJoinTask(pool.submit(runnable));
		}
		catch (RejectedExecutionException ignored) {
			return REJECTED;
		}
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		if (delay == 0) {
			return schedule(task);
		}
		PendingTask pendingTask = new PendingTask(task, NO_PARENT);
		ScheduledFuture<?> future =
				scheduler.schedule(new ExecuteTask(pendingTask, pool), delay, unit);
		return new DisposableFuture(future, pendingTask);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		PendingTask pendingTask = new PendingTask(task, NO_PARENT);
		ScheduledFuture<?> future =
				scheduler.scheduleAtFixedRate(new ExecuteTask(pendingTask, pool),
						initialDelay,
						period,
						unit);
		return new DisposableFuture(future, pendingTask);
	}

	private void uncaughtException(Thread t, Throwable e) {
		log.error("Scheduler worker in group " + t.getThreadGroup()
		                                          .getName() + " failed with an uncaught exception",
				e);
	}

	private static class DisposableForkJoinTask implements Disposable {

		private final ForkJoinTask<?> task;

		DisposableForkJoinTask(ForkJoinTask<?> task) {
			this.task = task;
		}

		@Override
		public void dispose() {
			task.cancel(false);
		}

		@Override
		public boolean isDisposed() {
			return task.isDone();
		}
	}

	private static class ExecuteTask implements Runnable {

		private final Runnable task;
		private final Executor executor;

		ExecuteTask(Runnable task, Executor executor) {
			this.task = task;
			this.executor = executor;
		}

		@Override
		public void run() {
			executor.execute(task);
		}
	}

	private static class PendingTask implements Runnable, Disposable {

		private final    Runnable        task;
		private final    BooleanSupplier isParentDisposed;
		private volatile boolean         disposed;

		PendingTask(Runnable task, BooleanSupplier disposed) {
			this.task = task;
			isParentDisposed = disposed;
		}

		@Override
		public void dispose() {
			disposed = true;
		}

		@Override
		public boolean isDisposed() {
			return disposed || isParentDisposed.getAsBoolean();
		}

		@Override
		public void run() {
			if (!isDisposed()) {
				task.run();
			}
		}
	}

	static final class DisposableFuture implements Disposable {

		final Future<?>   f;
		final PendingTask task;

		DisposableFuture(Future<?> f, PendingTask task) {
			this.f = f;
			this.task = task;
		}

		@Override
		public void dispose() {
			task.dispose();
			f.cancel(false);
		}

		@Override
		public boolean isDisposed() {
			return (f.isCancelled() || f.isDone()) && task.isDisposed();
		}
	}

	private static class Worker implements Scheduler.Worker {

		private final Executor                 executor;
		private final ScheduledExecutorService scheduler;
		private final Object          lock  = new Object();
		private final Queue<Runnable> tasks = new ArrayDeque<>();
		private volatile boolean shutdown;
		private boolean executing = false;

		public Worker(Executor executor, ScheduledExecutorService scheduler) {
			this.executor = executor;
			this.scheduler = scheduler;
		}

		@Override
		public void dispose() {
			if (shutdown) {
				return;
			}

			shutdown = true;

			synchronized (lock) {
				tasks.clear();
			}
		}

		@Override
		public boolean isDisposed() {
			return shutdown;
		}

		@Override
		public Disposable schedule(Runnable task) {
			if (shutdown) {
				return REJECTED;
			}

			DisposableWorkerTask workerTask =
					new DisposableWorkerTask(task, this::isDisposed);

			try {
				execute(workerTask);
			}
			catch (RejectedExecutionException ignored) {
				// dispose the task since it made it into the queue
				workerTask.dispose();
				return REJECTED;
			}

			return workerTask;
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			if (delay == 0) {
				return schedule(task);
			}

			if (shutdown) {
				return REJECTED;
			}

			PendingTask pendingTask = new PendingTask(task, this::isDisposed);
			ScheduledFuture<?> future;
			try {
				future = scheduler.schedule(new ExecuteTask(pendingTask, this::execute),
						delay,
						unit);
			}
			catch (RejectedExecutionException ignored) {
				return REJECTED;
			}
			return new DisposableFuture(future, pendingTask);
		}

		@Override
		public Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			if (shutdown) {
				return REJECTED;
			}

			PendingTask pendingTask = new PendingTask(task, this::isDisposed);
			ScheduledFuture<?> future;
			try {
				future = scheduler.scheduleAtFixedRate(new ExecuteTask(pendingTask,
						this::execute), initialDelay, period, unit);
			}
			catch (RejectedExecutionException ignored) {
				return REJECTED;
			}
			return new DisposableFuture(future, pendingTask);
		}

		private void execute(Runnable command) {
			synchronized (lock) {
				tasks.add(command);

				if (!executing) {
					executor.execute(this::execute);
					executing = true;
				}
			}
		}

		private void execute() {
			while (true) {
				Runnable task;
				synchronized (lock) {
					task = tasks.poll();
					if (task == null) {
						executing = false;
						return;
					}
				}

				try {
					task.run();
				}
				catch (Throwable ex) {
					Schedulers.handleError(ex);
				}
			}
		}
	}

	private static class DisposableWorkerTask implements Disposable, Runnable {

		private final    Runnable        task;
		private final    BooleanSupplier isParentDisposed;
		private volatile boolean         disposed;

		private DisposableWorkerTask(Runnable task, BooleanSupplier disposed) {
			this.task = task;
			isParentDisposed = disposed;
		}

		@Override
		public void dispose() {
			disposed = true;
		}

		@Override
		public boolean isDisposed() {
			return disposed || isParentDisposed.getAsBoolean();
		}

		@Override
		public void run() {
			if (isDisposed()) {
				return;
			}

			task.run();
		}
	}

	static final         AtomicLong      COUNTER                     = new AtomicLong();
	static final         String          FORK_JOIN_POOL              = "forkJoinPool";
	static final         String          FORK_JOIN_POOL_TIMER_SUFFIX = "-timer";
	static final         String          FORK_JOIN_POOL_TIMER        =
			FORK_JOIN_POOL + FORK_JOIN_POOL_TIMER_SUFFIX;
	private static final Logger          log                         =
			Loggers.getLogger(Schedulers.class);
	private static final BooleanSupplier NO_PARENT                   = () -> false;
}
