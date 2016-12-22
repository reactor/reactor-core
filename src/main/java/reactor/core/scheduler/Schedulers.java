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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import reactor.core.Cancellation;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.Logger;
import reactor.util.Loggers;

import static reactor.core.Exceptions.unwrap;

/**
 * {@link Schedulers} provide various {@link Scheduler} generator useable by {@link
 * reactor.core.publisher.Flux#publishOn publishOn} or {@link reactor.core.publisher.Mono#subscribeOn
 * subscribeOn} :
 * <p>
 * <ul> <li>{@link #fromExecutorService(ExecutorService)}}. </li> <li>{@link #newParallel}
 * : Optimized for fast {@link Runnable} executions </li> <li>{@link #single} : Optimized
 * for low-latency {@link Runnable} executions </li> <li>{@link #immediate}. </li> </ul>
 *
 * @author Stephane Maldini
 */
public class Schedulers {

	/**
	 * Default number of processors available to the runtime on init (min 4)
	 *
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE = Math.max(Runtime.getRuntime()
	                                                            .availableProcessors(),
			4);

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
		if(executor instanceof ExecutorService){
			return fromExecutorService((ExecutorService)executor);
		}
		return fromExecutor(executor, false);
	}

	/**
	 * Create a {@link Scheduler} which uses a backing {@link Executor} to schedule
	 * Runnables for async operators.
	 *
	 * @param executor an {@link Executor}
	 * @param trampoline set to false if this {@link Scheduler} is used by "operators"
	 * that already conflate {@link Runnable} executions (publishOn, subscribeOn...)
	 *
	 * @return a new {@link Scheduler}
	 */
	public static Scheduler fromExecutor(Executor executor, boolean trampoline) {
		if(executor instanceof ExecutorService){
			return fromExecutorService((ExecutorService)executor, trampoline);
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
		return fromExecutorService(executorService, false);
	}

	/**
	 * Create a {@link Scheduler} which uses a backing {@link ExecutorService} to schedule
	 * Runnables for async operators.
	 *
	 * @param executorService an {@link ExecutorService}
	 * @param interruptOnCancel delegate to {@link java.util.concurrent.Future#cancel(boolean)
	 * future.cancel(true)}  on {@link Disposable#dispose()}
	 *
	 * @return a new {@link Scheduler}
	 */
	public static Scheduler fromExecutorService(ExecutorService executorService,
			boolean interruptOnCancel) {
		return new ExecutorServiceScheduler(executorService, interruptOnCancel);
	}

	/**
	 * {@link Scheduler} that dynamically creates ExecutorService-based Workers and caches
	 * the thread pools, reusing them once the Workers have been shut down.
	 * <p>
	 * The maximum number of created thread pools is unbounded.
	 * <p>
	 * The default time-to-live for unused thread pools is 60 seconds, use the appropriate
	 * factory to set a different value.
	 * <p>
	 * This scheduler is not restartable.
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler elastic() {
		return cache(ELASTIC, ELASTIC_SUPPLIER);
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
	 * factory to set a different value.
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
	 * Create a new {@link TimedScheduler} backed by a single threaded {@link
	 * java.util.concurrent.ScheduledExecutorService}.
	 *
	 * @param name timer thread prefix
	 *
	 * @return a new {@link TimedScheduler}
	 */
	public static TimedScheduler newTimer(String name) {
		return newTimer(name, true);
	}

	/**
	 * Create a new {@link TimedScheduler} backed by a single threaded {@link
	 * java.util.concurrent.ScheduledExecutorService}.
	 *
	 * @param name Component and thread name prefix
	 * @param daemon false if the {@link Scheduler} requires an explicit {@link
	 * Scheduler#dispose()} to exit the VM.
	 *
	 * @return a new {@link TimedScheduler}
	 */
	public static TimedScheduler newTimer(String name, boolean daemon) {
		return newTimer(new SchedulerThreadFactory(name,
				daemon,
				SingleTimedScheduler.COUNTER));
	}

	/**
	 * Create a new {@link TimedScheduler} backed by a single threaded {@link
	 * java.util.concurrent.ScheduledExecutorService}.
	 *
	 * @param threadFactory a {@link ThreadFactory} to use for the unique thread of the
	 * {@link TimedScheduler}
	 *
	 * @return a new {@link TimedScheduler}
	 */
	public static TimedScheduler newTimer(ThreadFactory threadFactory) {
		return factory.newTimer(threadFactory);
	}

	/**
	 * Define a hook that is executed when a {@link Scheduler} has
	 * {@link #handleError(Throwable) handled an error}. Note that it is executed after
	 * the error has been passed to the thread uncaughtErrorHandler, which is not the
	 * case when a fatal error occurs (see {@link Exceptions#throwIfJvmFatal(Throwable)}).
	 *
	 * @param c the new hook to set, or null to ignore (default).
	 */
	public static void onHandleError(BiConsumer<Thread, ? super Throwable> c) {
		log.info("Hooking new default: onHandleError");
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
		return cache(PARALLEL, PARALLEL_SUPPLIER);
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
		log.info("Reset to factory defaults: onHandleError");
		onHandleErrorHook = null;
	}

	/**
	 * Override {@link Schedulers} finite signatures of {@link #newParallel}, {@link
	 * #newSingle}, {@link #newTimer} and {@link #newElastic} factory using the matching
	 * method signature in the target class. A finite signature corresponds to those
	 * including a {@link ThreadFactory} argument and should be instance methods.
	 * <p>
	 * This method should be called safely and with caution, typically on app startup.
	 * <p>
	 * Note that cached schedulers (like {@link #timer()}) are also shut down and will be
	 * re-created from the new factory upon next use.
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
		List<CachedScheduler> schedulers;
		Collection<CachedScheduler> view = cachedSchedulers.values();
		for (; ; ) {
			schedulers = new ArrayList<>(view);
			view.clear();
			schedulers.forEach(CachedScheduler::_dispose);
			if (view.isEmpty()) {
				return;
			}
		}
	}

	/**
	 * {@link Scheduler} that hosts a single-threaded ExecutorService-based worker and is
	 * suited for parallel work. Will cache the returned schedulers for subsequent calls until dispose.
	 *
	 * @return a cached {@link Scheduler} that hosts a single-threaded
	 * ExecutorService-based worker
	 */
	public static Scheduler single() {
		return cache(SINGLE, SINGLE_SUPPLIER);
	}

	/**
	 * Wraps a single worker of some other {@link Scheduler} and provides {@link
	 * reactor.core.scheduler.Scheduler.Worker} services on top of it.
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
	 * Create or reuse a hash-wheel based {@link TimedScheduler} with a resolution of 50MS
	 * All times will rounded up to the closest multiple of this resolution.
	 *
	 * @return a cached hash-wheel based {@link TimedScheduler}
	 */
	public static TimedScheduler timer() {
		return timedCache(TIMER, TIMER_SUPPLIER).asTimedScheduler();
	}

	/**
	 * Attempt to safely dispose a {@link Scheduler}'s {@link ExecutorService}. This
	 * method will call {@link ExecutorService#awaitTermination(long, TimeUnit)} with
	 * a fixed grace period of 30 seconds (logging a warn message if the awaitTermination
	 * couldn't finish).
	 */
	static void safeExecutorServiceShutdown(ExecutorService executorService, String type) {
		executorService.shutdownNow();
		try {
			executorService.awaitTermination(30, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			log.warn("{} scheduler in-flight tasks didn't terminate within 30 seconds", type);
		}
	}

	/**
	 * Public factory hook to override Schedulers behavior globally
	 */
	public interface Factory {

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

		/**
		 * Create a new {@link TimedScheduler} backed by a dedicated resource.
		 *
		 * @param threadFactory a {@link ThreadFactory} to use for the eventual thread
		 *
		 * @return a new {@link TimedScheduler}
		 */
		default TimedScheduler newTimer(ThreadFactory threadFactory) {
			return new SingleTimedScheduler(threadFactory);
		}
	}

	// Internals
	static final String ELASTIC  = "elastic"; // IO stuff
	static final String PARALLEL = "parallel"; //scale up common tasks
	static final String SINGLE   = "single"; //non blocking tasks
	static final String TIMER    = "timer"; //timed tasks

	static final ConcurrentMap<String, CachedScheduler> cachedSchedulers =
			new ConcurrentHashMap<>();

	static final Supplier<Scheduler> ELASTIC_SUPPLIER =
			() -> newElastic(ELASTIC, ElasticScheduler.DEFAULT_TTL_SECONDS, true);

	static final Supplier<Scheduler> PARALLEL_SUPPLIER = () -> newParallel(PARALLEL,
			Runtime.getRuntime()
			       .availableProcessors(),
			true);

	static final Supplier<Scheduler> SINGLE_SUPPLIER = () -> newSingle(SINGLE, true);

	static final Supplier<TimedScheduler> TIMER_SUPPLIER = () -> newTimer(TIMER);

	static final Factory DEFAULT = new Factory() {
	};

	static volatile Factory factory = DEFAULT;

	static CachedScheduler cache(String key, Supplier<Scheduler> schedulerSupplier) {
		for (; ; ) {
			CachedScheduler s = cachedSchedulers.get(key);
			if (s != null) {
				return s;
			}
			s = new CachedScheduler(key, schedulerSupplier.get());
			if (cachedSchedulers.putIfAbsent(key, s) == null) {
				return s;
			}
			s._dispose();
		}
	}

	static CachedScheduler timedCache(String key,
			Supplier<TimedScheduler> schedulerSupplier) {
		for (; ; ) {
			CachedScheduler s = cachedSchedulers.get(key);
			if (s != null) {
				return s;
			}
			s = new CachedTimedScheduler(key, schedulerSupplier.get());
			if (cachedSchedulers.putIfAbsent(key, s) == null) {
				return s;
			}
			s._dispose();
		}
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
		public Cancellation schedule(Runnable task) {
			return cached.schedule(task);
		}

		@Override
		public Worker createWorker() {
			return cached.createWorker();
		}

		@Override
		public void start() {
			cached.start();
		}

		@Override
		public void shutdown() {
			dispose();
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

		TimedScheduler asTimedScheduler() {
			throw new UnsupportedOperationException("Scheduler is not Timed");
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			CachedScheduler scheduler = (CachedScheduler) o;

			return cached.equals(scheduler.cached);

		}

		@Override
		public int hashCode() {
			return cached.hashCode();
		}
	}

	static final class CachedTimedScheduler extends CachedScheduler
			implements TimedScheduler {

		final TimedScheduler cachedTimed;

		CachedTimedScheduler(String key, TimedScheduler cachedTimed) {
			super(key, cachedTimed);
			this.cachedTimed = cachedTimed;
		}

		@Override
		public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
			return cachedTimed.schedule(task, delay, unit);
		}

		@Override
		public Cancellation schedulePeriodically(Runnable task,
				long initialDelay,
				long period,
				TimeUnit unit) {
			return cachedTimed.schedulePeriodically(task, initialDelay, period, unit);
		}

		@Override
		public TimedWorker createWorker() {
			return cachedTimed.createWorker();
		}

		@Override
		TimedScheduler asTimedScheduler() {
			return this;
		}

		@Override
		public TimedScheduler get() {
			return cachedTimed;
		}
	}
}
