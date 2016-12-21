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
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.Cancellation;
import reactor.util.concurrent.OpenHashSet;

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
final class ElasticScheduler implements Scheduler {

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

	public ElasticScheduler(ThreadFactory factory, int ttlSeconds) {
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

	@Override
	public void start() {
		throw new UnsupportedOperationException("Restarting not supported yet");
	}

	@Override
	public void shutdown() {
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

		result = Executors.newSingleThreadExecutor(factory);
		all.offer(result);
		if (shutdown) {
			all.remove(result);
			return SHUTDOWN;
		}
		return result;
	}

	@Override
	public Cancellation schedule(Runnable task) {
		ExecutorService exec = pick();

		Runnable wrapper = () -> {
			try {
				try {
					task.run();
				}
				catch (Throwable ex) {
					Schedulers.handleError(ex);
				}
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

		public ExecutorServiceExpiry(ExecutorService executor, long expireMillis) {
			this.executor = executor;
			this.expireMillis = expireMillis;
		}
	}

	static final class CachedWorker implements Worker {

		final ExecutorService executor;

		final ElasticScheduler parent;

		volatile boolean shutdown;

		OpenHashSet<CachedTask> tasks;

		public CachedWorker(ExecutorService executor, ElasticScheduler parent) {
			this.executor = executor;
			this.parent = parent;
			this.tasks = new OpenHashSet<>();
		}

		@Override
		public Cancellation schedule(Runnable task) {
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
		public boolean isShutdown() {
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
				implements Runnable, Cancellation {

			/** */
			private static final long serialVersionUID = 6799295393954430738L;

			final Runnable run;

			final CachedWorker parent;

			volatile boolean cancelled;

			static final FutureTask<Object> CANCELLED = new FutureTask<>(() -> {
			}, null);

			static final FutureTask<Object> FINISHED = new FutureTask<>(() -> {
			}, null);

			public CachedTask(Runnable run, CachedWorker parent) {
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
