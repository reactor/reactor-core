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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import reactor.core.Disposable;
import reactor.util.concurrent.OpenHashSet;

/**
 * Scheduler that works with a single-threaded ExecutorService and is suited for
 * same-thread work (like an event dispatch thread).
 */
final class SingleScheduler implements Scheduler, Supplier<ExecutorService> {

	static final AtomicLong COUNTER = new AtomicLong();

	final ThreadFactory factory;

	volatile ExecutorService executor;
	static final AtomicReferenceFieldUpdater<SingleScheduler, ExecutorService> EXECUTORS =
			AtomicReferenceFieldUpdater.newUpdater(SingleScheduler.class,
					ExecutorService.class,
					"executor");

	static final ExecutorService TERMINATED;

	static {
		TERMINATED = Executors.newSingleThreadExecutor();
		TERMINATED.shutdownNow();
	}

	SingleScheduler(ThreadFactory factory) {
		this.factory = factory;
		init();
	}

	/**
	 * Instantiates the default {@link ExecutorService} for the SingleScheduler
	 * ({@code Executors.newSingleThreadExecutor}).
	 */
	@Override
	public ExecutorService get() {
		return Executors.newSingleThreadExecutor(factory);
	}

	private void init() {
		EXECUTORS.lazySet(this,
				Schedulers.decorateExecutorService(Schedulers.SINGLE, this));
	}

	@Override
	public boolean isDisposed() {
		return executor == TERMINATED;
	}

	@Override
	public void start() {
		ExecutorService b = null;
		for (; ; ) {
			ExecutorService a = executor;
			if (a != TERMINATED) {
				if (b != null) {
					b.shutdownNow();
				}
				return;
			}

			if (b == null) {
				b = Schedulers.decorateExecutorService(Schedulers.SINGLE, this);
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
		ExecutorService a = executor;
		if (a != TERMINATED) {
			a = EXECUTORS.getAndSet(this, TERMINATED);
			if (a != TERMINATED) {
				Schedulers.executorServiceShutdown(a, Schedulers.SINGLE);
			}
		}
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
	public Worker createWorker() {
		return new SingleWorker(executor);
	}

	static final class SingleWorker implements Worker {

		final ExecutorService exec;

		OpenHashSet<SingleWorkerTask> tasks;

		volatile boolean shutdown;

		SingleWorker(ExecutorService exec) {
			this.exec = exec;
			this.tasks = new OpenHashSet<>();
		}

		@Override
		public Disposable schedule(Runnable task) {
			if (shutdown) {
				return REJECTED;
			}

			SingleWorkerTask pw = new SingleWorkerTask(task, this);

			synchronized (this) {
				if (shutdown) {
					return REJECTED;
				}
				tasks.add(pw);
			}

			Future<?> f;
			try {
				f = exec.submit(pw);
			}
			catch (RejectedExecutionException ex) {
				return REJECTED;
			}

			if (shutdown) {
				f.cancel(true);
				return pw;
			}

			pw.setFuture(f);

			return pw;
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
			shutdown = true;
			OpenHashSet<SingleWorkerTask> set;
			synchronized (this) {
				set = tasks;
				tasks = null;
			}

			if (set != null && !set.isEmpty()) {
				Object[] a = set.keys();
				for (Object o : a) {
					if (o != null) {
						((SingleWorkerTask) o).cancelFuture();
					}
				}
			}
		}

		@Override
		public boolean isDisposed() {
			return shutdown;
		}

		void remove(SingleWorkerTask task) {
			if (shutdown) {
				return;
			}

			synchronized (this) {
				if (shutdown) {
					return;
				}
				tasks.remove(task);
			}
		}

		static final class SingleWorkerTask implements Runnable, Disposable {

			final Runnable run;

			final SingleWorker parent;

			volatile boolean cancelled;

			volatile Future<?> future;
			@SuppressWarnings("rawtypes")
			static final AtomicReferenceFieldUpdater<SingleWorkerTask, Future> FUTURE =
					AtomicReferenceFieldUpdater.newUpdater(SingleWorkerTask.class,
							Future.class,
							"future");

			static final Future<Object> FINISHED  =
					CompletableFuture.completedFuture(null);
			static final Future<Object> CANCELLED =
					CompletableFuture.completedFuture(null);

			SingleWorkerTask(Runnable run, SingleWorker parent) {
				this.run = run;
				this.parent = parent;
			}

			@Override
			public void run() {
				if (cancelled || parent.shutdown) {
					return;
				}
				try {
					try {
						run.run();
					}
					catch (Throwable ex) {
						Schedulers.handleError(ex);
					}
				}
				finally {
					for (; ; ) {
						Future<?> f = future;
						if (f == CANCELLED) {
							break;
						}
						if (FUTURE.compareAndSet(this, f, FINISHED)) {
							parent.remove(this);
							break;
						}
					}
				}
			}

			@Override
			public boolean isDisposed() {
				Future<?> a = future;
				return FINISHED == a || CANCELLED == a;
			}

			@Override
			public void dispose() {
				if (!cancelled) {
					cancelled = true;

					Future<?> f = future;
					if (f != CANCELLED && f != FINISHED) {
						f = FUTURE.getAndSet(this, CANCELLED);
						if (f != CANCELLED && f != FINISHED) {
							if (f != null) {
								f.cancel(parent.shutdown);
							}

							parent.remove(this);
						}
					}
				}
			}

			void setFuture(Future<?> f) {
				if (future != null || !FUTURE.compareAndSet(this, null, f)) {
					if (future != FINISHED) {
						f.cancel(parent.shutdown);
					}
				}
			}

			void cancelFuture() {
				Future<?> f = future;
				if (f != CANCELLED && f != FINISHED) {
					f = FUTURE.getAndSet(this, CANCELLED);
					if (f != null && f != CANCELLED && f != FINISHED) {
						f.cancel(true);
					}
				}
			}

		}
	}
}
