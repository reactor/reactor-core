/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.scheduler;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Dynamically creates ScheduledExecutorService-based Workers and caches the thread pools, reusing
 * them once the Workers have been shut down. This scheduler is time-capable (can schedule
 * with delay / periodically).
 * <p>
 * The maximum number of created thread pools is unbounded.
 * <p>
 * The default time-to-live for unused thread pools is 60 seconds, use the
 * appropriate constructor to set a different value.
 * <p>
 * This scheduler is not restartable (may be later).
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
// To be removed in 3.5
final class ElasticScheduler implements Scheduler, Scannable {

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


	final Deque<ScheduledExecutorServiceExpiry> cache;

	final Queue<CachedService> all;

	ScheduledExecutorService evictor;


	volatile boolean shutdown;

	ElasticScheduler(ThreadFactory factory, int ttlSeconds) {
		if (ttlSeconds < 0) {
			throw new IllegalArgumentException("ttlSeconds must be positive, was: " + ttlSeconds);
		}
		this.ttlSeconds = ttlSeconds;
		this.factory = factory;
		this.cache = new ConcurrentLinkedDeque<>();
		this.all = new ConcurrentLinkedQueue<>();
		//evictor is now started in `start()`. make it look like it is constructed shutdown
		this.shutdown = true;
	}

	/**
	 * Instantiates the default {@link ScheduledExecutorService} for the ElasticScheduler
	 * ({@code Executors.newScheduledThreadPoolExecutor} with core and max pool size of 1).
	 */
	public ScheduledExecutorService createUndecoratedService() {
		ScheduledThreadPoolExecutor poolExecutor = new ScheduledThreadPoolExecutor(1, factory);
		poolExecutor.setMaximumPoolSize(1);
		poolExecutor.setRemoveOnCancelPolicy(true);
		return poolExecutor;
	}

	@Override
	public void start() {
		if (!shutdown) {
			return;
		}
		this.evictor = Executors.newScheduledThreadPool(1, EVICTOR_FACTORY);
		this.evictor.scheduleAtFixedRate(this::eviction,
				ttlSeconds,
				ttlSeconds,
				TimeUnit.SECONDS);
		this.shutdown = false;
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
		ScheduledExecutorServiceExpiry e = cache.pollLast();
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
				task,
				cached,
				0L,
				TimeUnit.MILLISECONDS);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		CachedService cached = pick();

		return Schedulers.directSchedule(cached.exec,
				task,
				cached,
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
	public String toString() {
		StringBuilder ts = new StringBuilder(Schedulers.ELASTIC)
				.append('(');
		if (factory instanceof ReactorThreadFactory) {
			ts.append('\"').append(((ReactorThreadFactory) factory).get()).append('\"');
		}
		ts.append(')');
		return ts.toString();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
		if (key == Attr.CAPACITY) return Integer.MAX_VALUE;
		if (key == Attr.BUFFERED) return cache.size(); //BUFFERED: number of workers alive
		if (key == Attr.NAME) return this.toString();

		return null;
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return cache.stream()
		            .map(cached -> cached.cached);
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
					all.remove(e.cached);
				}
			}
		}
	}

	static final class CachedService implements Disposable, Scannable {

		final ElasticScheduler         parent;
		final ScheduledExecutorService exec;

		CachedService(@Nullable ElasticScheduler parent) {
			this.parent = parent;
			if (parent != null) {
				this.exec = Schedulers.decorateExecutorService(parent, parent.createUndecoratedService());
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
					parent.cache.offerLast(e);
					if (parent.shutdown) {
						if (parent.cache.remove(e)) {
							exec.shutdownNow();
						}
					}
				}
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.NAME) return parent.scanUnsafe(key);
			if (key == Attr.PARENT) return parent;
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
			if (key == Attr.CAPACITY) {
				//assume 1 if unknown, otherwise use the one from underlying executor
				Integer capacity = (Integer) Schedulers.scanExecutor(exec, key);
				if (capacity == null || capacity == -1) return 1;
			}
			return Schedulers.scanExecutor(exec, key);
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

	static final class ElasticWorker extends AtomicBoolean implements Worker, Scannable {

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

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
			if (key == Attr.NAME) return cached.scanUnsafe(key) + ".worker";
			if (key == Attr.PARENT) return cached.parent;

			return cached.scanUnsafe(key);
		}
	}
}
