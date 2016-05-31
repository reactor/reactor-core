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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.flow.Cancellation;
import reactor.core.state.Introspectable;
import reactor.core.util.Exceptions;
import reactor.core.util.PlatformDependent;
import reactor.core.util.WaitStrategy;

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
	 * {@link Scheduler} that hosts a fixed pool of single-threaded Event Loop based
	 * workers and is suited for non blocking work.
	 *
	 * @return a reusable {@link Scheduler} that hosts a fixed pool of single-threaded
	 * Event-Loop based workers
	 */
	public static Scheduler computation() {
		return cachedSchedulers.computeIfAbsent(COMPUTATION,
				k -> new CachedScheduler(k,
						newComputation(k,
								Runtime.getRuntime()
								       .availableProcessors(),
								true)));
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
		return new ExecutorServiceScheduler(executorService);
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
	 * {@link Scheduler} that hosts a fixed pool of single-threaded Event Loop based
	 * workers and is suited for non blocking work.
	 *
	 * @param name Group name derived for thread identification
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newComputation(String name) {
		return newComputation(name,
				Runtime.getRuntime()
				       .availableProcessors());
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded Event Loop based
	 * workers and is suited for non blocking work.
	 *
	 * @param name Group name derived for thread identification
	 * @param parallelism Number of pooled workers.
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newComputation(String name, int parallelism) {
		return newComputation(name, parallelism, false);
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded Event Loop based
	 * workers and is suited for non blocking work.
	 *
	 * @param name Group name derived for thread identification
	 * @param parallelism Number of pooled workers.
	 * @param bufferSize backlog size to be used by event loops.
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newComputation(String name, int parallelism, int bufferSize) {
		return newComputation(name, parallelism, bufferSize, false);
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded Event Loop based
	 * workers and is suited for non blocking work.
	 *
	 * @param name Group name derived for thread identification
	 * @param parallelism Number of pooled workers.
	 * @param daemon false if the {@link Scheduler} requires an explicit {@link
	 * Scheduler#shutdown()} to exit the VM.
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newComputation(String name, int parallelism, boolean daemon) {
		return newComputation(name,
				parallelism,
				PlatformDependent.MEDIUM_BUFFER_SIZE,
				daemon);
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded Event Loop based
	 * workers and is suited for non blocking work.
	 *
	 * @param name Group name derived for thread identification
	 * @param parallelism Number of pooled workers.
	 * @param bufferSize backlog size to be used by event loops.
	 * @param daemon false if the {@link Scheduler} requires an explicit {@link
	 * Scheduler#shutdown()} to exit the VM.
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newComputation(String name,
			int parallelism,
			int bufferSize,
			boolean daemon) {
		return newComputation(parallelism,
				bufferSize,
				new SchedulersFactory(name, daemon));
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded Event Loop based
	 * workers and is suited for non blocking work.
	 *
	 * @param parallelism Number of pooled workers.
	 * @param bufferSize backlog size to be used by event loops.
	 * @param threadFactory a {@link ThreadFactory} to use for the unique thread of the
	 * {@link Scheduler}
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newComputation(int parallelism,
			int bufferSize,
			ThreadFactory threadFactory) {
		try {
			return (Scheduler) COMPUTATION_FACTORY.get()
			                                      .invoke(null,
					                                      parallelism,
					                                      bufferSize,
					                                      threadFactory);
		}
		catch (Exception e) {
			throw Exceptions.bubble(e);
		}
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work.
	 *
	 * @param name Group name derived for thread identification
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newParallel(String name) {
		return newParallel(name,
				Runtime.getRuntime()
				       .availableProcessors());
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work.
	 *
	 * @param name Group name derived for thread identification
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
	 * @param name Group name derived for thread identification
	 * @param parallelism Number of pooled workers.
	 * @param daemon false if the {@link Scheduler} requires an explicit {@link
	 * Scheduler#shutdown()} to exit the VM.
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newParallel(String name, int parallelism, boolean daemon) {
		return new ParallelScheduler(parallelism, new SchedulersFactory(name, daemon));
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work.
	 *
	 * @param parallelism Number of pooled workers.
	 * @param threadFactory a {@link ThreadFactory} to use for the unique thread of the
	 * {@link Scheduler}
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
		return new ParallelScheduler(parallelism, threadFactory);
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
	 * Scheduler#shutdown()} to exit the VM.
	 *
	 * @return a new {@link Scheduler} that hosts a single-threaded ExecutorService-based
	 * worker
	 */
	public static Scheduler newSingle(String name, boolean daemon) {
		return new ParallelScheduler(1, new SchedulersFactory(name, daemon));
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
		return new ParallelScheduler(1, threadFactory);
	}

	/**
	 * Create a new {@link Timer} using the default resolution (50MS) and backlog size (64). All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 * @param name timer thread prefix
	 * <p>
	 * return a new {@link Timer}
	 */
	public static Timer newTimer(String name) {
		return newTimer(name, 50);
	}

	/**
	 * Create a new {@link Timer} using the the given timer {@code resolution} and backlog size (64). All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param name timer thread prefix
	 * @param resolution resolution of this timer in milliseconds
	 *                   <p>
	 *                   return a new {@link Timer}
	 */
	public static Timer newTimer(String name, int resolution) {
		return newTimer(name, resolution, PlatformDependent.XS_BUFFER_SIZE);
	}

	/**
	 * Create a new {@code Timer} using the given timer {@code resolution} and {@code bufferSize}. All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param name timer thread prefix
	 * @param resolution resolution of this timer in milliseconds
	 * @param bufferSize size of the wheel supporting the Timer, the larger the wheel, the less the lookup time is
	 *                   for sparse timeouts.
	 *                   <p>
	 *                   return a new {@link Timer}
	 */
	public static Timer newTimer(String name, int resolution, int bufferSize) {
		return new Timer(name, resolution, bufferSize, WaitStrategy.parking(), null);
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work.
	 *
	 * @return a reusable {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers
	 */
	public static Scheduler parallel() {
		return cachedSchedulers.computeIfAbsent(PARALLEL,
				k -> new CachedScheduler(k,
						newParallel(k,
								Runtime.getRuntime()
								       .availableProcessors(),
								true)));
	}

	/**
	 * Assign a {@link #newComputation} factory using the matching method signature in
	 * the target class.
	 *
	 * @param factoryClass an arbitrary type with static methods matching {@link
	 * #newComputation} signature(s).
	 */
	public static void setComputationsFactory(Class<?> factoryClass) {
		Objects.requireNonNull(factoryClass, "factoryClass");
		try {
			Method m = factoryClass.getDeclaredMethod(NEW_COMPUTATION,
					int.class,
					int.class,
					ThreadFactory.class);
			m.setAccessible(true);
			COMPUTATION_FACTORY.lazySet(m);
		}
		catch (NoSuchMethodException e) {
			throw Exceptions.bubble(e);
		}
	}

	/**
	 * Clear any cached {@link Scheduler} and call shutdown on them.
	 */
	public static void shutdownNow() {
		List<CachedScheduler> schedulers;
		Collection<CachedScheduler> view = cachedSchedulers.values();
		for (; ; ) {
			schedulers = new ArrayList<>(view);
			view.clear();
			schedulers.forEach(CachedScheduler::_shutdown);
			if (view.isEmpty()) {
				return;
			}
		}
	}

	/**
	 * {@link Scheduler} that hosts a single-threaded ExecutorService-based worker and is
	 * suited for parallel work. Will cache the returned schedulers for subsequent {@link
	 * #single} calls until shutdown.
	 *
	 * @return a cached {@link Scheduler} that hosts a single-threaded
	 * ExecutorService-based worker
	 */
	public static Scheduler single() {
		return cachedSchedulers.computeIfAbsent(SINGLE,
				k -> new CachedScheduler(k, newSingle(k, true)));
	}

	/**
	 * {@link Scheduler} that hosts a single-threaded ExecutorService-based worker and is
	 * suited for parallel work. Will cache the returned schedulers for subsequent {@link
	 * #single} calls until shutdown.
	 *
	 * @return a cached {@link Scheduler} that hosts a single-threaded
	 * ExecutorService-based worker
	 */
	public static TimedScheduler timer() {
		return cachedSchedulers.computeIfAbsent(TIMER,
				k -> new CachedTimedScheduler(k, newTimer(k)))
		                       .asTimedScheduler();
	}

	// Internals

	static final String COMPUTATION     = "computation";
	static final String NEW_COMPUTATION = "newComputation";
	static final String PARALLEL        = "parallel";
	static final String SINGLE          = "single";
	static final String TIMER           = "timer";

	static final AtomicLong                             COUNTER             =
			new AtomicLong();
	static final AtomicReference<Method>                COMPUTATION_FACTORY =
			new AtomicReference<>();
	static final ConcurrentMap<String, CachedScheduler> cachedSchedulers    =
			new ConcurrentHashMap<>();

	static {
		try {
			Class<?> factory = Schedulers.class.getClassLoader()
			                                   .loadClass(
					                                   "reactor.core.publisher.TopicProcessor");
			setComputationsFactory(factory);
		}
		catch (Exception e) {
			throw Exceptions.bubble(e);
		}
	}

	static final class SchedulersFactory implements ThreadFactory, Introspectable {

		final String  name;
		final boolean daemon;

		SchedulersFactory(String name, boolean daemon) {
			this.name = name;
			this.daemon = daemon;
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, name + "-" + COUNTER.incrementAndGet());
			t.setDaemon(daemon);
			return t;
		}

		@Override
		public String getName() {
			return name;
		}
	}

	static class CachedScheduler implements Scheduler {

		final Scheduler cached;
		final String    key;

		CachedScheduler(String key, Scheduler cached) {
			cached.start();
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
		}

		void _shutdown() {
			cached.shutdown();
		}

		TimedScheduler asTimedScheduler() {
			throw new UnsupportedOperationException("Scheduler is not Timed");
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
	}
}
