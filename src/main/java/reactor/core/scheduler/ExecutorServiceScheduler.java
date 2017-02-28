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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import reactor.core.Disposable;
import reactor.util.concurrent.OpenHashSet;

/**
 * A simple {@link Scheduler} which uses a backing {@link ExecutorService} to schedule
 * Runnables for async operators. This scheduler is {@link Scheduler#isTimeCapable() time-capable}
 * if the backing executor is a {@link ScheduledExecutorService}.
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class ExecutorServiceScheduler implements Scheduler {

	static final Runnable  EMPTY     = () -> {
	};
	static final Future<?> CANCELLED = new FutureTask<>(EMPTY, null);
	static final Future<?> FINISHED  = new FutureTask<>(EMPTY, null);

	final ExecutorService executor;
	final boolean         interruptOnCancel;

	ExecutorServiceScheduler(ExecutorService executorService, boolean interruptOnCancel) {
		if (executorService instanceof ScheduledExecutorService) {
			this.executor = Schedulers.decorateScheduledExecutorService("ExecutorService",
					() -> (ScheduledExecutorService) executorService);
		}
		else {
			this.executor = Schedulers.decorateExecutorService("ExecutorService",
					() -> executorService);
		}
		this.interruptOnCancel = interruptOnCancel;
	}

	@Override
	public Worker createWorker() {
		return new ExecutorServiceWorker(executor, interruptOnCancel);
	}

	@Override
	public boolean isTimeCapable() {
		return executor instanceof ScheduledExecutorService;
	}

	@Override
	public Disposable schedule(Runnable task) {
		try {
			return new DisposableFuture(executor.submit(task), interruptOnCancel);
		}
		catch (RejectedExecutionException ree) {
			return REJECTED;
		}
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		if (!isTimeCapable()) {
			return NOT_TIMED;
		}

		ScheduledExecutorService scheduledExecutor = (ScheduledExecutorService) executor;

		try {
			return new DisposableFuture(scheduledExecutor.schedule(task, delay, unit), interruptOnCancel);
		}
		catch (RejectedExecutionException ree) {
			return REJECTED;
		}
	}

	@Override
	public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		if (!isTimeCapable()) {
			return NOT_TIMED;
		}

		ScheduledExecutorService scheduledExecutor = (ScheduledExecutorService) executor;

		try {
			return new DisposableFuture(scheduledExecutor.scheduleAtFixedRate(task, initialDelay, period, unit), interruptOnCancel);
		}
		catch (RejectedExecutionException ree) {
			return REJECTED;
		}
	}

	@Override
	public boolean isDisposed() {
		return executor.isShutdown();
	}

	@Override
	public void shutdown() {
		dispose();
	}

	@Override
	public void dispose() {
		Schedulers.executorServiceShutdown(executor, "ExecutorService");
	}

	static final class DisposableFuture implements Disposable {

		final Future<?> f;
		final boolean   interruptOnCancel;

		DisposableFuture(Future<?> f, boolean interruptOnCancel) {
			this.f = f;
			this.interruptOnCancel = interruptOnCancel;
		}

		@Override
		public void dispose() {
			f.cancel(interruptOnCancel);
		}

		@Override
		public boolean isDisposed() {
			return f.isCancelled() || f.isDone();
		}
	}

	static final class ExecutorServiceWorker implements Worker, DisposableContainer<ScheduledRunnable> {

		final ExecutorService executor;
		final boolean         interruptOnCancel;

		volatile boolean terminated;

		OpenHashSet<ScheduledRunnable> tasks;

		ExecutorServiceWorker(ExecutorService executor, boolean interruptOnCancel) {
			this.executor = executor;
			this.interruptOnCancel = interruptOnCancel;
			this.tasks = new OpenHashSet<>();
		}

		@Override
		public boolean isTimeCapable() {
			return executor instanceof ScheduledExecutorService;
		}

		@Override
		public Disposable schedule(Runnable t) {
			ScheduledRunnable sr = new ScheduledRunnable(t, this);
			try {
				if (add(sr)) {
					Future<?> f = executor.submit(sr);
					sr.setFuture(f);
					return sr;
				}
			}
			catch (RejectedExecutionException ree) {
				removeAndDispose(sr);
			}
			return REJECTED;
		}

		@Override
		public Disposable schedule(Runnable t, long delay, TimeUnit unit) {
			if (!isTimeCapable()) {
				return NOT_TIMED;
			}

			ScheduledExecutorService scheduledExecutor = (ScheduledExecutorService) executor;

			ScheduledRunnable sr = new ScheduledRunnable(t, this);
			try {
				if (add(sr)) {
					Future<?> f = scheduledExecutor.schedule(sr, delay, unit);
					sr.setFuture(f);
					return sr;
				}
			}
			catch (RejectedExecutionException ree) {
				removeAndDispose(sr);
			}
			return REJECTED;
		}

		@Override
		public Disposable schedulePeriodically(Runnable t, long initialDelay, long period, TimeUnit unit) {
			if (!isTimeCapable()) {
				return NOT_TIMED;
			}

			ScheduledExecutorService scheduledExecutor = (ScheduledExecutorService) executor;

			ScheduledRunnable sr = new ScheduledRunnable(t, this);
			try {
				if (add(sr)) {
					Future<?> f = scheduledExecutor.scheduleAtFixedRate(sr, initialDelay, period, unit);
					sr.setFuture(f);
					return sr;
				}
			}
			catch (RejectedExecutionException ree) {
				removeAndDispose(sr);
			}
			return REJECTED;
		}



		@Override
		public boolean add(ScheduledRunnable sr) {
			if (!terminated) {
				synchronized (this) {
					if (!terminated) {
						tasks.add(sr);
						return true;
					}
				}
			}
			return false;
		}

		@Override
		public boolean remove(ScheduledRunnable sr) {
			if (!terminated) {
				synchronized (this) {
					if (!terminated) {
						tasks.remove(sr);
						return true;
					}
				}
			}
			return false;
		}

		@Override
		public void shutdown() {
			dispose();
		}

		@Override
		public void dispose() {
			if (!terminated) {
				OpenHashSet<ScheduledRunnable> coll;
				synchronized (this) {
					if (terminated) {
						return;
					}
					coll = tasks;
					tasks = null;
					terminated = true;
				}

				if (!coll.isEmpty()) {
					Object[] a = coll.keys();
					for (Object o : a) {
						if (o != null) {
							((ScheduledRunnable) o).dispose();
						}
					}
				}
			}
		}

		@Override
		public boolean isDisposed() {
			return terminated;
		}
	}
}
