/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import java.util.stream.Stream;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Dynamically creates ScheduledExecutorService-based Workers and caches the thread pools, reusing
 * them once the Workers have been shut down. This scheduler is time-capable (can schedule
 * with delay / periodically).
 * <p>
 * The maximum number of created thread pools is capped.
 * <p>
 * The default time-to-live for unused thread pools is 60 seconds, use the
 * appropriate constructor to set a different value.
 * <p>
 * This scheduler is not restartable.
 *
 * @author Simon Baslé
 */
final class CappedScheduler implements Scheduler, Supplier<ScheduledExecutorService>, Scannable {

	static final AtomicLong COUNTER = new AtomicLong();

	static final ThreadFactory EVICTOR_FACTORY = r -> {
		Thread t = new Thread(r, "capped-evictor-" + COUNTER.incrementAndGet());
		t.setDaemon(true);
		return t;
	};

	static final CachedService SHUTDOWN            = new CachedService(null);
	static final int           DEFAULT_TTL_SECONDS = 60;

	final ThreadFactory              factory;
	final int                        ttlSeconds;
	final int                        cap;
	final Deque<CachedServiceExpiry> idleServicesWithExpiry;
	final Queue<DeferredFacade>      deferredFacades;
	final Queue<CachedService>       allServices;
	final ScheduledExecutorService   evictor;

	volatile boolean                                        shutdown;
	volatile int                                            remaining;
	static final AtomicIntegerFieldUpdater<CappedScheduler> REMAINING =
			AtomicIntegerFieldUpdater.newUpdater(CappedScheduler.class, "remaining");

	CappedScheduler(int cap, ThreadFactory factory, int ttlSeconds) {
		if (ttlSeconds < 0) {
			throw new IllegalArgumentException("ttlSeconds must be positive, was: " + ttlSeconds);
		}
		this.ttlSeconds = ttlSeconds;
		if (cap < 1) {
			throw new IllegalArgumentException("cap must be strictly positive, was: " + cap);
		}
		this.cap = cap;
		this.remaining = cap;
		this.factory = factory;
		this.idleServicesWithExpiry = new ConcurrentLinkedDeque<>();
		this.deferredFacades = new ConcurrentLinkedQueue<>();
		this.allServices = new ConcurrentLinkedQueue<>();
		this.evictor = Executors.newScheduledThreadPool(1, EVICTOR_FACTORY);
		this.evictor.scheduleAtFixedRate(this::eviction,
				ttlSeconds,
				ttlSeconds,
				TimeUnit.SECONDS);
	}

	/**
	 * Instantiates the default {@link ScheduledExecutorService} for the CappedScheduler
	 * ({@code Executors.newScheduledThreadPoolExecutor} with core and max pool size of 1).
	 */
	@Override
	public ScheduledExecutorService get() {
		ScheduledThreadPoolExecutor poolExecutor = new ScheduledThreadPoolExecutor(1, factory);
		poolExecutor.setMaximumPoolSize(1);
		poolExecutor.setRemoveOnCancelPolicy(true);
		return poolExecutor;
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
		idleServicesWithExpiry.clear();

		CachedService cached;
		while ((cached = allServices.poll()) != null) {
			cached.exec.shutdownNow();
		}
	}

	@Nullable
	CachedService tryPick() {
		if (shutdown) {
			return SHUTDOWN;
		}
		CachedService result;
		//try to see if there is an idle worker
		CachedServiceExpiry e = idleServicesWithExpiry.pollLast();
		if (e != null) {
			return e.cached;
		}

		if (REMAINING.decrementAndGet(this) < 0) {
			//cap reached
			REMAINING.incrementAndGet(this);
			if (shutdown) {
				return SHUTDOWN;
			}
			return null;
		}
		else {
			result = new CachedService(this);
			allServices.offer(result);
			if (shutdown) {
				allServices.remove(result);
				return SHUTDOWN;
			}
			return result;
		}
	}


	@Override
	public Worker createWorker() {
		if (shutdown) {
			return new ActiveWorker(SHUTDOWN);
		}
		//try to see if there is an idle worker
		CachedServiceExpiry e = idleServicesWithExpiry.pollLast();
		if (e != null) {
			return new ActiveWorker(e.cached);
		}

		if (REMAINING.decrementAndGet(this) < 0) {
			//cap reached
			REMAINING.incrementAndGet(this);
			if (shutdown) {
				return new ActiveWorker(SHUTDOWN);
			}
			DeferredWorker deferredWorker = new DeferredWorker(this.toString());
			this.deferredFacades.offer(deferredWorker);
			return deferredWorker;
		}
		else {
			CachedService availableService = new CachedService(this);

			allServices.offer(availableService);
			if (shutdown) {
				allServices.remove(availableService);
				return new ActiveWorker(SHUTDOWN);
			}
			return new ActiveWorker(availableService);
		}
	}

	@Override
	public Disposable schedule(Runnable task) {
		CachedService cached = tryPick();
		if (cached != null) {
			return Schedulers.directSchedule(cached.exec,
					task,
					cached,
					0L,
					TimeUnit.MILLISECONDS);
		}
		else {
			DeferredDirect deferredDirect = new DeferredDirect(task, 0L, 0L, TimeUnit.MILLISECONDS);
			deferredFacades.offer(deferredDirect);
			return deferredDirect;
		}
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		CachedService cached = tryPick();

		if (cached != null) {
			return Schedulers.directSchedule(cached.exec,
					task,
					cached,
					delay,
					unit);
		}
		else {
			DeferredDirect deferredDirect = new DeferredDirect(task, delay, 0L, TimeUnit.MILLISECONDS);
			deferredFacades.offer(deferredDirect);
			return deferredDirect;
		}
	}

	@Override
	public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		CachedService cached = tryPick();

		if (cached != null) {
			return Disposables.composite(Schedulers.directSchedulePeriodically(cached.exec,
					task,
					initialDelay,
					period,
					unit), cached);
		}
		else {
			DeferredDirect deferredDirect = new DeferredDirect(task, initialDelay, period, TimeUnit.MILLISECONDS);
			deferredFacades.offer(deferredDirect);
			return deferredDirect;
		}
	}

	@Override
	public String toString() {
		StringBuilder ts = new StringBuilder(Schedulers.CAPPED)
				.append('(');
		if (factory instanceof ReactorThreadFactory) {
			ts.append('\"').append(((ReactorThreadFactory) factory).get()).append("\",");
		}
		ts.append(cap).append(',').append(ttlSeconds).append("s)");
		return ts.toString();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
		if (key == Attr.CAPACITY) return cap;
		//TODO re-evaluate BUFFERED: should this include deferredWorkers?
		if (key == Attr.BUFFERED) return idleServicesWithExpiry.size(); //BUFFERED: number of workers alive
		if (key == Attr.NAME) return this.toString();

		return null;
	}

	@Override
		//TODO re-evaluate the inners? should these include deferredWorkers? allServices?
	public Stream<? extends Scannable> inners() {
		return idleServicesWithExpiry.stream()
		                             .map(cached -> cached.cached);
	}

	void eviction() {
		long now = System.currentTimeMillis();

		List<CachedServiceExpiry> list = new ArrayList<>(idleServicesWithExpiry);
		for (CachedServiceExpiry e : list) {
			if (e.expireMillis < now) {
				if (idleServicesWithExpiry.remove(e)) {
					e.cached.exec.shutdownNow();
					allServices.remove(e.cached);
				}
			}
		}
	}

	static final class CachedService implements Disposable, Scannable {

		final CappedScheduler          parent;
		final ScheduledExecutorService exec;

		CachedService(@Nullable CappedScheduler parent) {
			this.parent = parent;
			if (parent != null) {
				this.exec = Schedulers.decorateExecutorService(parent, parent.get());
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
					//in case of work, re-create an ActiveWorker
					DeferredFacade deferredFacade = parent.deferredFacades.poll();
					if (deferredFacade != null) {
						deferredFacade.setService(this);
					}
					else {
						//if no more work, the service is put back at end of the cached queue and new expiry is started
						CachedServiceExpiry e = new CachedServiceExpiry(this,
								System.currentTimeMillis() + parent.ttlSeconds * 1000L);
						parent.idleServicesWithExpiry.offerLast(e);
						if (parent.shutdown) {
							if (parent.idleServicesWithExpiry.remove(e)) {
								exec.shutdownNow();
							}
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

	static final class CachedServiceExpiry {

		final CachedService cached;
		final long          expireMillis;

		CachedServiceExpiry(CachedService cached, long expireMillis) {
			this.cached = cached;
			this.expireMillis = expireMillis;
		}
	}

	static final class ActiveWorker extends AtomicBoolean implements Worker, Scannable {

		final CachedService cached;
		final Composite tasks;

		ActiveWorker(CachedService cached) {
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

	/**
	 * Either a {@link reactor.core.scheduler.Scheduler.Worker} or a direct facade for tasks
	 * that cannot be immediately scheduled due to a lack of available services.
	 */
	@FunctionalInterface
	interface DeferredFacade {

		void setService(CachedService service);
	}

	/**
	 * Capture a submitted task, then its deferred execution when an ActiveWorker becomes available.
	 * Propagates task cancellation, as this would be the outer world {@link Disposable} interface
	 * even when the task is activated.
	 */
	static final class DeferredWorkerTask implements Disposable {

		final DeferredWorker parent;
		final Runnable       task;

		final long     delay;
		final long     period;
		final TimeUnit timeUnit;

		volatile Disposable                                                      activated;
		static final AtomicReferenceFieldUpdater<DeferredWorkerTask, Disposable> ACTIVATED =
				AtomicReferenceFieldUpdater.newUpdater(DeferredWorkerTask.class, Disposable.class, "activated");

		DeferredWorkerTask(DeferredWorker parent, Runnable task, long delay, long period, TimeUnit unit) {
			this.parent = parent;
			this.task = task;
			this.delay = delay;
			this.period = period;
			this.timeUnit = unit;
		}

		void activate(ActiveWorker delegate) {
			//pending task is implicitly removed because activate is called on a poll()
			if (this.period == 0 && this.delay == 0) {
				this.activated = delegate.schedule(this.task);
			}
			else if (this.period != 0) {
				this.activated = delegate.schedulePeriodically(this.task, this.delay, this.period, this.timeUnit);
			}
			else {
				this.activated = delegate.schedule(this.task, this.delay, this.timeUnit);
			}
		}

		@Override
		public void dispose() {
			parent.remove(this);
			disposeInner();
		}

		void disposeInner() {
			if (this.activated != null) {
				this.activated.dispose();
			}
		}
	}

	/**
	 * Represent a synthetic worker that doesn't actually submit tasks until a proper {@link ActiveWorker} has
	 * become available. Propagates cancellation of tasks and disposal of worker in early scenarios.
	 */
	static final class DeferredWorker extends ConcurrentLinkedQueue<DeferredWorkerTask> implements Worker, Scannable,
	                                                                                               DeferredFacade {

		volatile ActiveWorker                                                  delegate;
		static final AtomicReferenceFieldUpdater<DeferredWorker, ActiveWorker> DELEGATE =
				AtomicReferenceFieldUpdater.newUpdater(DeferredWorker.class, ActiveWorker.class, "delegate");

		volatile int                                           disposed;
		static final AtomicIntegerFieldUpdater<DeferredWorker> DISPOSED =
				AtomicIntegerFieldUpdater.newUpdater(DeferredWorker.class, "disposed");

		final String workerName;

		DeferredWorker(String parentName) {
			this.workerName = parentName + ".deferredWorker";
		}

		public void setService(CachedService service) {
			if (DISPOSED.get(this) == 1) {
				service.dispose();
				return;
			}
			ActiveWorker delegate = new ActiveWorker(service);
			if (DELEGATE.compareAndSet(this, null, delegate)) {
				DeferredWorkerTask pendingTask;
				while((pendingTask = this.poll()) != null) {
					pendingTask.activate(delegate);
				}
			}
			else {
				service.dispose();
			}
		}

		@Override
		public Disposable schedule(Runnable task) {
			if (DISPOSED.get(this) == 1) {
				throw Exceptions.failWithRejected("Worker has been disposed");
			}
			ActiveWorker aw = DELEGATE.get(this);
			if (aw == null) {
				DeferredWorkerTask
						pendingTask = new DeferredWorkerTask(this, task, 0L, 0L, TimeUnit.MILLISECONDS);
				offer(pendingTask);
				return pendingTask;

			}
			return aw.schedule(task);
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			if (DISPOSED.get(this) == 1) {
				throw Exceptions.failWithRejected("Worker has been disposed");
			}
			ActiveWorker aw = DELEGATE.get(this);
			if (aw == null) {
				DeferredWorkerTask
						pendingTask = new DeferredWorkerTask(this, task, delay, 0L, unit);
				offer(pendingTask);
				return pendingTask;

			}
			return aw.schedule(task, delay, unit);
		}

		@Override
		public Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			if (DISPOSED.get(this) == 1) {
				throw Exceptions.failWithRejected("Worker has been disposed");
			}
			ActiveWorker aw = DELEGATE.get(this);
			if (aw == null) {
				DeferredWorkerTask pendingTask = new DeferredWorkerTask(this, task, initialDelay, period, unit);
				offer(pendingTask);
				return pendingTask;
			}
			return aw.schedulePeriodically(task, initialDelay, period, unit);
		}

		@Override
		public void dispose() {
			if (DISPOSED.compareAndSet(this, 0, 1)) {
				DeferredWorkerTask pendingTask;
				while((pendingTask = this.poll()) != null) {
					pendingTask.disposeInner();
				}

				ActiveWorker aw = DELEGATE.getAndSet(this, null);
				if (aw != null) {
					aw.dispose();
				}
			}
		}

		@Override
		public boolean isDisposed() {
			return DISPOSED.get(this) == 1;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
			if (key == Attr.NAME) return workerName;
			if (key == Attr.CAPACITY) return Integer.MAX_VALUE;
			if (key == Attr.BUFFERED) return this.size();

			ActiveWorker d = delegate;
			if (d != null) {
				if (key == Attr.PARENT) return d.cached.parent;
				return d.cached.scanUnsafe(key);
			}
			return null;
		}
	}

	/**
	 * Capture a task submitted directly to the {@link Scheduler}, then its deferred execution when a {@link CachedService} becomes available.
	 * Propagates task cancellation, as this would be the outer world {@link Disposable} interface even when the task is activated.
	 * Propagates cancellation of tasks in early scenarios.
	 */
	static final class DeferredDirect extends AtomicReference<CachedService> implements Scannable, Disposable,
	                                                                                    DeferredFacade {

		volatile Disposable                                                  activeTask;
		static final AtomicReferenceFieldUpdater<DeferredDirect, Disposable> ACTIVE_TASK =
				AtomicReferenceFieldUpdater.newUpdater(DeferredDirect.class, Disposable.class, "activeTask");

		volatile int                                           disposed;
		static final AtomicIntegerFieldUpdater<DeferredDirect> DISPOSED =
				AtomicIntegerFieldUpdater.newUpdater(DeferredDirect.class, "disposed");

		final Runnable task;
		final long     delay;
		final long     period;
		final TimeUnit timeUnit;

		DeferredDirect(Runnable task, long delay, long period, TimeUnit unit) {
			this.task = task;
			this.delay = delay;
			this.period = period;
			this.timeUnit = unit;
		}

		@Override
		public void setService(CachedService service) {
			if (DISPOSED.get(this) == 1) {
				service.dispose();
				return;
			}
			if (this.compareAndSet(null, service)) {
				if (this.period == 0 && this.delay == 0) {
					ACTIVE_TASK.set(this, Schedulers.directSchedule(service.exec, this.task, this, 0L, TimeUnit.SECONDS));
				}
				else if (this.period != 0) {
					ACTIVE_TASK.set(this, Schedulers.directSchedulePeriodically(service.exec, this.task, this.delay, this.period, this.timeUnit));
				}
				else {
					ACTIVE_TASK.set(this, Schedulers.directSchedule(service.exec, this.task, this, this.delay, this.timeUnit));
				}
			}
			else {
				service.dispose();
			}
		}

		@Override
		public void dispose() {
			if (DISPOSED.compareAndSet(this, 0, 1)) {
				Disposable at = ACTIVE_TASK.getAndSet(this, null);
				if (at != null) {
					at.dispose();
				}
				CachedService c = this.getAndSet(null);
				if (c != null) {
					c.dispose();
				}
			}
		}

		@Override
		public boolean isDisposed() {
			return DISPOSED.get(this) == 1;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
//			if (key == Attr.NAME) return workerName; //FIXME
			if (key == Attr.CAPACITY) return Integer.MAX_VALUE;

			CachedService d = this.get();
			if (key == Attr.BUFFERED) return d == null ? 1 : 0;
			if (d != null) {
				if (key == Attr.PARENT) return d.parent;
				return d.scanUnsafe(key);
			}
			return null;
		}
	}
}
