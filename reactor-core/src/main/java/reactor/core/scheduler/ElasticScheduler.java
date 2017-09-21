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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.util.annotation.Nullable;

/**
 * Dynamically creates ScheduledExecutorService-based Workers and caches the thread pools, reusing
 * them once the Workers have been shut down. This scheduler is time-capable (can schedule
 * with delay / periodically).
 * <p>
 * The maximum number of created thread pools is unbounded.
 * <p>
 * The default time-to-live for unused thread pools is 60 seconds, use the
 * appropriate constructor to push a different value.
 * <p>
 * This scheduler is not restartable (may be later).
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class ElasticScheduler implements Scheduler, Supplier<ScheduledExecutorService> {

	static final AtomicLong COUNTER = new AtomicLong();

	static final ThreadFactory EVICTOR_FACTORY = r -> {
		Thread t = new Thread(r, "elastic-evictor-" + COUNTER.incrementAndGet());
		t.setDaemon(true);
		return t;
	};

	static final CachedService SHUTDOWN = new CachedService(null);

	static final int DEFAULT_TTL_SECONDS = 60;

	final ThreadFactory factory;

	final int ttlSeconds;


	final Queue<ScheduledExecutorServiceExpiry> cache;

	final Queue<CachedService> all;

	final ScheduledExecutorService evictor;


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
	 * Instantiates the default {@link ScheduledExecutorService} for the ElasticScheduler
	 * ({@code Executors.newSingleThreadExecutor}).
	 */
	@Override
	public ScheduledExecutorService get() {
		return Executors.newSingleThreadScheduledExecutor(factory);
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
	public void dispose() {
		if (shutdown) {
			return;
		}
		shutdown = true;

		evictor.shutdownNow();

		cache.clear();

		CachedService cached;

		while ((cached = all.poll()) != null) {
			cached.exec.shutdownNow();
		}
	}

	CachedService pick() {
		if (shutdown) {
			return SHUTDOWN;
		}
		CachedService result;
		ScheduledExecutorServiceExpiry e = cache.poll();
		if (e != null) {
			return e.cached;
		}

		result = new CachedService(this);
		all.offer(result);
		if (shutdown) {
			all.remove(result);
			return SHUTDOWN;
		}
		return result;
	}

	@Override
	public Disposable schedule(Runnable task) {
		CachedService cached = pick();

		return Schedulers.directSchedule(cached.exec,
				new DirectScheduleTask(task, cached),
				0L,
				TimeUnit.MILLISECONDS);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		CachedService cached = pick();

		return Schedulers.directSchedule(cached.exec,
				new DirectScheduleTask(task, cached),
				delay,
				unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		CachedService cached = pick();

		return Disposables.composite(Schedulers.directSchedulePeriodically(cached.exec,
				task,
				initialDelay,
				period,
				unit), cached);
	}

	@Override
	public Worker createWorker() {
		return new ElasticWorker(pick());
	}

	void eviction() {
		long now = System.currentTimeMillis();

		List<ScheduledExecutorServiceExpiry> list = new ArrayList<>(cache);
		for (ScheduledExecutorServiceExpiry e : list) {
			if (e.expireMillis < now) {
				if (cache.remove(e)) {
					e.cached.exec.shutdownNow();
				}
			}
		}
	}

	static final class CachedService implements Disposable {

		final ElasticScheduler         parent;
		final ScheduledExecutorService exec;

		CachedService(@Nullable ElasticScheduler parent) {
			this.parent = parent;
			if (parent != null) {
				this.exec =
						Schedulers.decorateExecutorService(Schedulers.ELASTIC, parent);
			}
			else {
				this.exec = Executors.newSingleThreadScheduledExecutor();
				this.exec.shutdownNow();
			}
		}

		@Override
		public void dispose() {
			if (exec != null) {
				if (this != SHUTDOWN && !parent.shutdown) {
					ScheduledExecutorServiceExpiry e = new
							ScheduledExecutorServiceExpiry(this,
							System.currentTimeMillis() + parent.ttlSeconds * 1000L);
					parent.cache.offer(e);
					if (parent.shutdown) {
						if (parent.cache.remove(e)) {
							exec.shutdownNow();
						}
					}
				}
			}
		}
	}

	static final class DirectScheduleTask implements Runnable {

		final Runnable      delegate;
		final CachedService cached;

		DirectScheduleTask(Runnable delegate, CachedService cached) {
			this.delegate = delegate;
			this.cached = cached;
		}

		@Override
		public void run() {
			try {
				delegate.run();
			}
			catch (Throwable ex) {
				Schedulers.handleError(ex);
			}
			finally {
				cached.dispose();
			}
		}
	}

	static final class ScheduledExecutorServiceExpiry {

		final CachedService cached;
		final long          expireMillis;

		ScheduledExecutorServiceExpiry(CachedService cached, long expireMillis) {
			this.cached = cached;
			this.expireMillis = expireMillis;
		}
	}

	static final class ElasticWorker extends AtomicBoolean implements Worker {

		final CachedService cached;

		final Disposable.Composite tasks;

		ElasticWorker(CachedService cached) {
			this.cached = cached;
			this.tasks = Disposables.composite();
		}

		@Override
		public Disposable schedule(Runnable task) {
			return Schedulers.workerSchedule(cached.exec,
					tasks,
					task,
					0L,
					TimeUnit.MILLISECONDS);
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			return Schedulers.workerSchedule(cached.exec, tasks, task, delay, unit);
		}

		@Override
		public Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			return Schedulers.workerSchedulePeriodically(cached.exec,
					tasks,
					task,
					initialDelay,
					period,
					unit);
		}

		@Override
		public void dispose() {
			if (compareAndSet(false, true)) {
				tasks.dispose();
				cached.dispose();
			}
		}

		@Override
		public boolean isDisposed() {
			return tasks.isDisposed();
		}
	}
}
