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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import reactor.core.Disposable;
import reactor.util.concurrent.OpenHashSet;

import static reactor.core.scheduler.ExecutorServiceScheduler.CANCELLED;
import static reactor.core.scheduler.ExecutorServiceScheduler.FINISHED;

/**
 * Dynamically creates ExecutorService-based Workers and caches the thread pools, reusing
 * them once the Workers have been shut down.
 * <p>
 * The maximum number of created thread pools is unbounded.
 * <p>
 * The default time-to-live for unused thread pools is 60 seconds, use the
 * appropriate constructor to set a different value.
 * <p>
 * This scheduler is not restartable (may be later).
 */
final class ElasticScheduler implements Scheduler, Supplier<ExecutorService> {

	static final AtomicLong COUNTER = new AtomicLong();

	static final ThreadFactory EVICTOR_FACTORY = r -> {
		Thread t = new Thread(r, "elastic-evictor-" + COUNTER.incrementAndGet());
		t.setDaemon(true);
		return t;
	};

	final ThreadFactory factory;

	final int ttlSeconds;

	static final int DEFAULT_TTL_SECONDS = 60;

	final Queue<ExecutorServiceExpiry> cache;

	final Queue<ExecutorService> all;

	final ScheduledExecutorService evictor;

	static final ExecutorService SHUTDOWN;

	static {
		SHUTDOWN = Executors.newSingleThreadExecutor();
		SHUTDOWN.shutdownNow();
	}

	volatile boolean shutdown;

	ElasticScheduler(ThreadFactory factory, int ttlSeconds) {
		if (ttlSeconds < 0) {
			throw new IllegalArgumentException("ttlSeconds must be positive, was: " + ttlSeconds);
		}
		this.ttlSeconds = ttlSeconds;
		this.factory = factory;
		this.cache = new ConcurrentLinkedQueue<>();
		this.all = new ConcurrentLinkedQueue<>();
		this.evictor = Executors.newScheduledThreadPool(1, EVICTOR_FACTORY);
		this.evictor.scheduleAtFixedRate(this::eviction,
				ttlSeconds,
				ttlSeconds,
				TimeUnit.SECONDS);
	}

	/**
	 * Instantiates the default {@link ExecutorService} for the ElasticScheduler
	 * ({@code Executors.newSingleThreadExecutor}).
	 */
	@Override
	public ExecutorService get() {
		return Executors.newSingleThreadExecutor(factory);
	}

	@Override
	public void start() {
		throw new UnsupportedOperationException("Restarting not supported yet");
	}

	@Override
	public boolean isDisposed() {
		return shutdown;
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

		evictor.shutdownNow();

		cache.clear();

		ExecutorService exec;

		while ((exec = all.poll()) != null) {
			exec.shutdownNow();
		}
	}

	ExecutorService pick() {
		if (shutdown) {
			return SHUTDOWN;
		}
		ExecutorService result;
		ExecutorServiceExpiry e = cache.poll();
		if (e != null) {
			return e.executor;
		}

		result = Schedulers.decorateExecutorService(Schedulers.ELASTIC, this);
		all.offer(result);
		if (shutdown) {
			all.remove(result);
			return SHUTDOWN;
		}
		return result;
	}

	@Override
	public Disposable schedule(Runnable task) {
		ExecutorService exec = pick();

		Runnable wrapper = () -> {
			try {
				task.run();
			}
			catch (Throwable ex) {
				Schedulers.handleError(ex);
			}
			finally {
				release(exec);
			}
		};
		Future<?> f;

		try {
			f = exec.submit(wrapper);
		}
		catch (RejectedExecutionException ex) {
			return REJECTED;
		}
		return () -> f.cancel(true);
	}

	@Override
	public Worker createWorker() {
		ExecutorService exec = pick();
		return new CachedWorker(exec, this);
	}

	void release(ExecutorService exec) {
		if (exec != SHUTDOWN && !shutdown) {
			ExecutorServiceExpiry e = new ExecutorServiceExpiry(exec,
					System.currentTimeMillis() + ttlSeconds * 1000L);
			cache.offer(e);
			if (shutdown) {
				if (cache.remove(e)) {
					exec.shutdownNow();
				}
			}
		}
	}

	void eviction() {
		long now = System.currentTimeMillis();

		List<ExecutorServiceExpiry> list = new ArrayList<>(cache);
		for (ExecutorServiceExpiry e : list) {
			if (e.expireMillis < now) {
				if (cache.remove(e)) {
					e.executor.shutdownNow();
				}
			}
		}
	}

	static final class ExecutorServiceExpiry {

		final ExecutorService executor;
		final long            expireMillis;

		ExecutorServiceExpiry(ExecutorService executor, long expireMillis) {
			this.executor = executor;
			this.expireMillis = expireMillis;
		}
	}

	static final class CachedWorker implements Worker {

		final ExecutorService executor;

		final ElasticScheduler parent;

		volatile boolean shutdown;

		OpenHashSet<CachedTask> tasks;

		CachedWorker(ExecutorService executor, ElasticScheduler parent) {
			this.executor = executor;
			this.parent = parent;
			this.tasks = new OpenHashSet<>();
		}

		@Override
		public Disposable schedule(Runnable task) {
			if (shutdown) {
				return REJECTED;
			}

			CachedTask ct = new CachedTask(task, this);

			synchronized (this) {
				if (shutdown) {
					return REJECTED;
				}
				tasks.add(ct);
			}

			Future<?> f;
			try {
				f = executor.submit(ct);
			}
			catch (RejectedExecutionException ex) {
				return REJECTED;
			}

			ct.setFuture(f);

			return ct;
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

			OpenHashSet<CachedTask> set;
			synchronized (this) {
				if (shutdown) {
					return;
				}
				shutdown = true;
				set = tasks;
				tasks = null;
			}

			if (!set.isEmpty()) {
				Object[] keys = set.keys();
				for (Object o : keys) {
					if (o != null) {
						((CachedTask) o).cancelFuture();
					}
				}
			}

			parent.release(executor);
		}

		@Override
		public boolean isDisposed() {
			return shutdown;
		}

		void remove(CachedTask task) {
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

		static final class CachedTask extends AtomicReference<Future<?>>
				implements Runnable, Disposable {

			/** */
			private static final long serialVersionUID = 6799295393954430738L;

			final Runnable run;

			final CachedWorker parent;

			volatile boolean cancelled;

			CachedTask(Runnable run, CachedWorker parent) {
				this.run = run;
				this.parent = parent;
			}

			@Override
			public void run() {
				try {
					if (!parent.shutdown && !cancelled) {
						run.run();
					}
				}
				catch (Throwable ex) {
					Schedulers.handleError(ex);
				}
				finally {
					lazySet(FINISHED);
					parent.remove(this);
				}
			}

			@Override
			public void dispose() {
				cancelled = true;
				cancelFuture();
			}

			@Override
			public boolean isDisposed() {
				Future<?> f = get();
				return f == CANCELLED || f == FINISHED;
			}

			void setFuture(Future<?> f) {
				if (!compareAndSet(null, f)) {
					if (get() != FINISHED) {
						f.cancel(true);
					}
				}
			}

			void cancelFuture() {
				Future<?> f = get();
				if (f != CANCELLED && f != FINISHED) {
					f = getAndSet(CANCELLED);
					if (f != null && f != CANCELLED && f != FINISHED) {
						f.cancel(true);
					}
				}
			}
		}
	}
}
