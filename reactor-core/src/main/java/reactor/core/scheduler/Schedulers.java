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

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.Logger;
import reactor.util.Loggers;

import static reactor.core.Exceptions.unwrap;

/**
 * {@link Schedulers} provides various {@link Scheduler} factories useable by {@link
 * reactor.core.publisher.Flux#publishOn publishOn} or {@link reactor.core.publisher.Mono#subscribeOn
 * subscribeOn} :
 * <p>
 * <ul> <li>{@link #fromExecutorService(ExecutorService)}}. </li> <li>{@link #newParallel}
 * : Optimized for fast {@link Runnable} executions </li> <li>{@link #single} : Optimized
 * for low-latency {@link Runnable} executions </li> <li>{@link #immediate}. </li> </ul>
 * <p>
 * Factories prefixed with {@code new} return a new instance of their flavor of {@link Scheduler},
 * while other factories like {@link #elastic()} return a shared instance, that is the one
 * used by operators requiring that flavor as their default Scheduler.
 *
 * @author Stephane Maldini
 */
public abstract class Schedulers {

	/**
	 * Default pool size, initialized to the number of processors available to the runtime
	 * on init (but with a minimum value of 4).
	 *
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE = Math.max(Runtime.getRuntime().availableProcessors(), 4);

	static volatile BiConsumer<Thread, ? super Throwable> onHandleErrorHook;

	/**
	 * Create a {@link Scheduler} which uses a backing {@link Executor} to schedule
	 * Runnables for async operators.
	 *
	 * @param executor an {@link Executor}
	 *
	 * @return a new {@link Scheduler}
	 */
	public static Scheduler fromExecutor(Executor executor) {
		return fromExecutor(executor, false);
	}

	/**
	 * Create a {@link Scheduler} which uses a backing {@link Executor} to schedule
	 * Runnables for async operators.
	 *
	 * @param executor an {@link Executor}
	 * @param trampoline push to false if this {@link Scheduler} is used by "operators"
	 * that already conflate {@link Runnable} executions (publishOn, subscribeOn...)
	 *
	 * @return a new {@link Scheduler}
	 */
	public static Scheduler fromExecutor(Executor executor, boolean trampoline) {
		if(!trampoline && executor instanceof ExecutorService){
			return fromExecutorService((ExecutorService) executor);
		}
		return new ExecutorScheduler(executor, trampoline);
	}

	/**
	 * Create a {@link Scheduler} which uses a backing {@link ExecutorService} to schedule
	 * Runnables for async operators.
	 *
	 * @param executorService an {@link ExecutorService}
	 *
	 * @return a new {@link Scheduler}
	 */
	public static Scheduler fromExecutorService(ExecutorService executorService) {
		return new DelegateServiceScheduler(executorService);
	}

	/**
	 * {@link Scheduler} that dynamically creates ExecutorService-based Workers and caches
	 * the thread pools, reusing them once the Workers have been shut down.
	 * <p>
	 * The maximum number of created thread pools is unbounded.
	 * <p>
	 * The default time-to-live for unused thread pools is 60 seconds, use the appropriate
	 * factory to push a different value.
	 * <p>
	 * This scheduler is not restartable.
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler elastic() {
		return cache(CACHED_ELASTIC, ELASTIC, ELASTIC_SUPPLIER);
	}

	/**
	 * Executes tasks on the caller's thread immediately.
	 *
	 * @return a reusable {@link Scheduler}
	 */
	public static Scheduler immediate() {
		return ImmediateScheduler.instance();
	}

	/**
	 * {@link Scheduler} that dynamically creates ExecutorService-based Workers and caches
	 * the thread pools, reusing them once the Workers have been shut down.
	 * <p>
	 * The maximum number of created thread pools is unbounded.
	 * <p>
	 * The default time-to-live for unused thread pools is 60 seconds, use the appropriate
	 * factory to push a different value.
	 * <p>
	 * This scheduler is not restartable.
	 *
	 * @param name Thread prefix
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newElastic(String name) {
		return newElastic(name, ElasticScheduler.DEFAULT_TTL_SECONDS);
	}

	/**
	 * {@link Scheduler} that dynamically creates ExecutorService-based Workers and caches
	 * the thread pools, reusing them once the Workers have been shut down.
	 * <p>
	 * The maximum number of created thread pools is unbounded.
	 * <p>
	 * This scheduler is not restartable.
	 *
	 * @param name Thread prefix
	 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newElastic(String name, int ttlSeconds) {
		return newElastic(name, ttlSeconds, false);
	}

	/**
	 * {@link Scheduler} that dynamically creates ExecutorService-based Workers and caches
	 * the thread pools, reusing them once the Workers have been shut down.
	 * <p>
	 * The maximum number of created thread pools is unbounded.
	 * <p>
	 * This scheduler is not restartable.
	 *
	 * @param name Thread prefix
	 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
	 * @param daemon false if the {@link Scheduler} requires an explicit {@link
	 * Scheduler#dispose()} to exit the VM.
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newElastic(String name, int ttlSeconds, boolean daemon) {
		return newElastic(ttlSeconds,
				new SchedulerThreadFactory(name, daemon, ElasticScheduler.COUNTER));

	}

	/**
	 * {@link Scheduler} that dynamically creates ExecutorService-based Workers and caches
	 * the thread pools, reusing them once the Workers have been shut down.
	 * <p>
	 * The maximum number of created thread pools is unbounded.
	 * <p>
	 * This scheduler is not restartable.
	 *
	 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
	 * @param threadFactory a {@link ThreadFactory} to use each thread initialization
	 *
	 * @return a new {@link Scheduler} that dynamically creates ExecutorService-based
	 * Workers and caches the thread pools, reusing them once the Workers have been shut
	 * down.
	 */
	public static Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
		return factory.newElastic(ttlSeconds, threadFactory);
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work.
	 *
	 * @param name Thread prefix
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newParallel(String name) {
		return newParallel(name, Runtime.getRuntime()
				       .availableProcessors());
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work.
	 *
	 * @param name Thread prefix
	 * @param parallelism Number of pooled workers.
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newParallel(String name, int parallelism) {
		return newParallel(name, parallelism, false);
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work.
	 *
	 * @param name Thread prefix
	 * @param parallelism Number of pooled workers.
	 * @param daemon false if the {@link Scheduler} requires an explicit {@link
	 * Scheduler#dispose()} to exit the VM.
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newParallel(String name, int parallelism, boolean daemon) {
		return newParallel(parallelism,
				new SchedulerThreadFactory(name, daemon, ParallelScheduler.COUNTER));
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work.
	 *
	 * @param parallelism Number of pooled workers.
	 * @param threadFactory a {@link ThreadFactory} to use for the fixed initialized
	 * number of {@link Thread}
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
		return factory.newParallel(parallelism, threadFactory);
	}

	/**
	 * {@link Scheduler} that hosts a single-threaded ExecutorService-based worker and is
	 * suited for parallel work.
	 *
	 * @param name Component and thread name prefix
	 *
	 * @return a new {@link Scheduler} that hosts a single-threaded ExecutorService-based
	 * worker
	 */
	public static Scheduler newSingle(String name) {
		return newSingle(name, false);
	}

	/**
	 * {@link Scheduler} that hosts a single-threaded ExecutorService-based worker and is
	 * suited for parallel work.
	 *
	 * @param name Component and thread name prefix
	 * @param daemon false if the {@link Scheduler} requires an explicit {@link
	 * Scheduler#dispose()} to exit the VM.
	 *
	 * @return a new {@link Scheduler} that hosts a single-threaded ExecutorService-based
	 * worker
	 */
	public static Scheduler newSingle(String name, boolean daemon) {
		return newSingle(new SchedulerThreadFactory(name, daemon,
				SingleScheduler.COUNTER));
	}

	/**
	 * {@link Scheduler} that hosts a single-threaded ExecutorService-based worker and is
	 * suited for parallel work.
	 *
	 * @param threadFactory a {@link ThreadFactory} to use for the unique thread of the
	 * {@link Scheduler}
	 *
	 * @return a new {@link Scheduler} that hosts a single-threaded ExecutorService-based
	 * worker
	 */
	public static Scheduler newSingle(ThreadFactory threadFactory) {
		return factory.newSingle(threadFactory);
	}

	/**
	 * Define a hook that is executed when a {@link Scheduler} has
	 * {@link #handleError(Throwable) handled an error}. Note that it is executed after
	 * the error has been passed to the thread uncaughtErrorHandler, which is not the
	 * case when a fatal error occurs (see {@link Exceptions#throwIfJvmFatal(Throwable)}).
	 *
	 * @param c the new hook to push.
	 */
	public static void onHandleError(BiConsumer<Thread, ? super Throwable> c) {
		if (log.isDebugEnabled()) {
			log.debug("Hooking new default: onHandleError");
		}
		onHandleErrorHook = Objects.requireNonNull(c, "onHandleError");
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work.
	 *
	 * @return a reusable {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers
	 */
	public static Scheduler parallel() {
		return cache(CACHED_PARALLEL, PARALLEL, PARALLEL_SUPPLIER);
	}

	/**
	 * Re-apply default factory to {@link Schedulers}
	 */
	public static void resetFactory(){
		setFactory(DEFAULT);
	}

	/**
	 * Reset the {@link #onHandleError(BiConsumer)} hook to the default no-op behavior.
	 */
	public static void resetOnHandleError() {
		if (log.isDebugEnabled()) {
			log.debug("Reset to factory defaults: onHandleError");
		}
		onHandleErrorHook = null;
	}

	/**
	 * Replace {@link Schedulers} factories ({@link #newParallel(String) newParallel},
	 * {@link #newSingle(String) newSingle} and {@link #newElastic(String) newElastic}). Also
	 * shutdown Schedulers from the cached factories (like {@link #single()}) in order to
	 * also use these replacements, re-creating the shared schedulers from the new factory
	 * upon next use.
	 * <p>
	 * This method should be called safely and with caution, typically on app startup.
	 *
	 * @param factoryInstance an arbitrary {@link Factory} instance.
	 */
	public static void setFactory(Factory factoryInstance) {
		Objects.requireNonNull(factoryInstance, "factoryInstance");
		shutdownNow();
		factory = factoryInstance;
	}

	/**
	 * Clear any cached {@link Scheduler} and call dispose on them.
	 */
	public static void shutdownNow() {
		CachedScheduler oldElastic = CACHED_ELASTIC.getAndSet(null);
		CachedScheduler oldParallel = CACHED_PARALLEL.getAndSet(null);
		CachedScheduler oldSingle = CACHED_SINGLE.getAndSet(null);

		if (oldElastic != null) oldElastic._dispose();
		if (oldParallel != null) oldParallel._dispose();
		if (oldSingle != null) oldSingle._dispose();
	}

	/**
	 * {@link Scheduler} that hosts a single-threaded ExecutorService-based worker and is
	 * suited for parallel work. Will cache the returned schedulers for subsequent calls until dispose.
	 *
	 * @return a cached {@link Scheduler} that hosts a single-threaded
	 * ExecutorService-based worker
	 */
	public static Scheduler single() {
		return cache(CACHED_SINGLE, SINGLE, SINGLE_SUPPLIER);
	}

	/**
	 * Wraps a single {@link reactor.core.scheduler.Scheduler.Worker} from some other
	 * {@link Scheduler} and provides {@link reactor.core.scheduler.Scheduler.Worker}
	 * services on top of it.
	 * <p>
	 * Use the {@link Scheduler#dispose()} to release the wrapped worker.
	 *
	 * @param original a {@link Scheduler} to call upon to get the single {@link
	 * reactor.core.scheduler.Scheduler.Worker}
	 *
	 * @return a wrapping {@link Scheduler} consistently returning a same worker from a
	 * source {@link Scheduler}
	 */
	public static Scheduler single(Scheduler original) {
		return new SingleWorkerScheduler(original);
	}

	/**
	 * Public factory hook to override Schedulers behavior globally
	 */
	public interface Factory {

		/**
		 * Override this method to decorate {@link ScheduledExecutorService} internally used by
		 * Reactor's various {@link Scheduler} implementations, allowing to tune the
		 * {@link ScheduledExecutorService} backing implementation.
		 *
		 * @param schedulerType a name hinting at the flavor of Scheduler being tuned.
		 * @param actual the default backing implementation, provided lazily as a Supplier
		 * so that you can bypass instantiation completely if you want to replace it.
		 * @return the internal {@link ScheduledExecutorService} instance to use.
		 */
		default ScheduledExecutorService decorateExecutorService(String schedulerType,
				Supplier<? extends ScheduledExecutorService> actual) {
			return actual.get();
		}

		/**
		 * {@link Scheduler} that dynamically creates Workers resources and caches
		 * eventually, reusing them once the Workers have been shut down.
		 * <p>
		 * The maximum number of created workers is unbounded.
		 *
		 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
		 * @param threadFactory a {@link ThreadFactory} to use
		 *
		 * @return a new {@link Scheduler} that dynamically creates Workers resources and
		 * caches eventually, reusing them once the Workers have been shut down.
		 */
		default Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
			return new ElasticScheduler(threadFactory, ttlSeconds);
		}

		/**
		 * {@link Scheduler} that hosts a fixed pool of workers and is suited for parallel
		 * work.
		 *
		 * @param parallelism Number of pooled workers.
		 * @param threadFactory a {@link ThreadFactory} to use for the fixed initialized
		 * number of {@link Thread}
		 *
		 * @return a new {@link Scheduler} that hosts a fixed pool of workers and is
		 * suited for parallel work
		 */
		default Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
			return new ParallelScheduler(parallelism, threadFactory);
		}

		/**
		 * {@link Scheduler} that hosts a single worker and is suited for non-blocking
		 * work.
		 *
		 * @param threadFactory a {@link ThreadFactory} to use for the unique resource of
		 * the {@link Scheduler}
		 *
		 * @return a new {@link Scheduler} that hosts a single worker
		 */
		default Scheduler newSingle(ThreadFactory threadFactory) {
			return new SingleScheduler(threadFactory);
		}
	}

	// Internals
	static final String ELASTIC  = "elastic"; // IO stuff
	static final String PARALLEL = "parallel"; //scale up common tasks
	static final String SINGLE   = "single"; //non blocking tasks
	static final String TIMER    = "timer"; //timed tasks

	// Cached schedulers in atomic references:
	static AtomicReference<CachedScheduler> CACHED_ELASTIC  = new AtomicReference<>();
	static AtomicReference<CachedScheduler> CACHED_PARALLEL = new AtomicReference<>();
	static AtomicReference<CachedScheduler> CACHED_SINGLE   = new AtomicReference<>();

	static final Supplier<Scheduler> ELASTIC_SUPPLIER =
			() -> newElastic(ELASTIC, ElasticScheduler.DEFAULT_TTL_SECONDS, true);

	static final Supplier<Scheduler> PARALLEL_SUPPLIER = () -> newParallel(PARALLEL,
			Runtime.getRuntime()
			       .availableProcessors(),
			true);

	static final Supplier<Scheduler> SINGLE_SUPPLIER = () -> newSingle(SINGLE, true);

	static final Factory DEFAULT = new Factory() {
	};

	static volatile Factory factory = DEFAULT;

	/**
	 * Get a {@link CachedScheduler} out of the {@code reference} or create one using the
	 * {@link Supplier} if the reference is empty, effectively creating a single instance
	 * to be reused as a default scheduler for the given {@code key} category.
	 *
	 * @param reference the cache reference that holds the scheduler
	 * @param key the "name" for the Scheduler's category/type
	 * @param supplier the {@link Scheduler} generator to use and wrap into a {@link CachedScheduler}.
	 * Note that in case of a race, an extraneous Scheduler can be created, but it'll get
	 * immediately {@link Scheduler#dispose() disposed}.
	 * @return a {@link CachedScheduler} to be reused, either pre-existing or created
	 */
	static CachedScheduler cache(AtomicReference<CachedScheduler> reference, String key, Supplier<Scheduler> supplier) {
		CachedScheduler s = reference.get();
		if (s != null) {
			return s;
		}
		s = new CachedScheduler(key, supplier.get());
		if (reference.compareAndSet(null, s)) {
			return s;
		}
		//the reference was updated in the meantime with a cached scheduler
		//fallback to it and dispose the extraneous one
		s._dispose();
		return reference.get();
	}

	static final Logger log = Loggers.getLogger(Schedulers.class);

	static final class SchedulerThreadFactory
			implements ThreadFactory, Supplier<String>, Thread.UncaughtExceptionHandler {

		final String     name;
		final boolean    daemon;
		final AtomicLong COUNTER;

		SchedulerThreadFactory(String name, boolean daemon, AtomicLong counter) {
			this.name = name;
			this.daemon = daemon;
			this.COUNTER = counter;
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, name + "-" + COUNTER.incrementAndGet());
			t.setDaemon(daemon);
			t.setUncaughtExceptionHandler(this);
			return t;
		}

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			log.error("Scheduler worker in group " + t.getThreadGroup().getName() +
					" failed with an uncaught exception", e);
		}

		@Override
		public String get() {
			return name;
		}
	}

	static void handleError(Throwable ex) {
		Thread thread = Thread.currentThread();
		Throwable t = unwrap(ex);
		Exceptions.throwIfJvmFatal(t);
		Thread.UncaughtExceptionHandler x = thread.getUncaughtExceptionHandler();
		if (x != null) {
			x.uncaughtException(thread, t);
		}
		else {
			log.error("Scheduler worker failed with an uncaught exception", t);
		}
		if (onHandleErrorHook != null) {
			onHandleErrorHook.accept(thread, t);
		}
	}

	static class CachedScheduler implements Scheduler, Supplier<Scheduler> {

		final Scheduler cached;
		final String    key;

		CachedScheduler(String key, Scheduler cached) {
			this.cached = cached;
			this.key = key;
		}

		@Override
		public Disposable schedule(Runnable task) {
			return cached.schedule(task);
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			return cached.schedule(task, delay, unit);
		}

		@Override
		public Disposable schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			return cached.schedulePeriodically(task, initialDelay, period, unit);
		}

		@Override
		public Worker createWorker() {
			return cached.createWorker();
		}

		@Override
		public long now(TimeUnit unit) {
			return cached.now(unit);
		}

		@Override
		public void start() {
			cached.start();
		}

		@Override
		public void dispose() {
		}

		@Override
		public boolean isDisposed() {
			return cached.isDisposed();
		}

		/**
		 * Get the {@link Scheduler} that is cached and wrapped inside this
		 * {@link CachedScheduler}.
		 *
		 * @return the cached Scheduler
		 */
		@Override
		public Scheduler get() {
			return cached;
		}

		void _dispose() {
			cached.dispose();
		}
	}

	static Disposable directSchedule(ScheduledExecutorService exec,
			Runnable task,
			long delay,
			TimeUnit unit) {
		SchedulerTask sr = new SchedulerTask(task);
		Future<?> f;
		if (delay <= 0L) {
			f = exec.submit((Callable<?>) sr);
		}
		else {
			f = exec.schedule((Callable<?>) sr, delay, unit);
		}
		sr.setFuture(f);

		return sr;
	}

	static Disposable directSchedulePeriodically(ScheduledExecutorService exec,
			Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {

		SchedulerTask sr = new SchedulerTask(task);

		Future<?> f = exec.scheduleAtFixedRate(sr, initialDelay, period, unit);
		sr.setFuture(f);

		return sr;
	}

	static Disposable workerSchedule(ScheduledExecutorService exec,
			Disposable.Composite tasks,
			Runnable task,
			long delay,
			TimeUnit unit) {

		WorkerTask sr = new WorkerTask(task, tasks);
		if (!tasks.add(sr)) {
			throw Exceptions.failWithRejected();
		}

		try {
			Future<?> f;
			if (delay <= 0L) {
				f = exec.submit((Callable<?>) sr);
			}
			else {
				f = exec.schedule((Callable<?>) sr, delay, unit);
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

	static Disposable workerSchedulePeriodically(ScheduledExecutorService exec,
			Disposable.Composite tasks,
			Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {

		WorkerTask sr = new WorkerTask(task, tasks);
		if (!tasks.add(sr)) {
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


	static ScheduledExecutorService decorateExecutorService(String schedulerType,
			Supplier<? extends ScheduledExecutorService> actual) {
		return factory.decorateExecutorService(schedulerType, actual);
	}

}
