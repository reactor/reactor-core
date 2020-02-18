/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.Metrics;
import reactor.util.annotation.Nullable;

import static reactor.core.Exceptions.unwrap;

/**
 * {@link Schedulers} provides various {@link Scheduler} flavors usable by {@link
 * reactor.core.publisher.Flux#publishOn(Scheduler) publishOn} or {@link reactor.core.publisher.Mono#subscribeOn
 * subscribeOn} :
 * <p>
 * <ul>
 *     <li>{@link #parallel()}: Optimized for fast {@link Runnable} non-blocking executions </li>
 *     <li>{@link #single}: Optimized for low-latency {@link Runnable} one-off executions </li>
 *     <li>{@link #elastic()}: Optimized for longer executions, an alternative for blocking tasks where the number of active tasks (and threads) can grow indefinitely</li>
 *     <li>{@link #boundedElastic()}: Optimized for longer executions, an alternative for blocking tasks where the number of active tasks (and threads) is capped</li>
 *     <li>{@link #immediate}: to immediately run submitted {@link Runnable} instead of scheduling them (somewhat of a no-op or "null object" {@link Scheduler})</li>
 *     <li>{@link #fromExecutorService(ExecutorService)} to create new instances around {@link java.util.concurrent.Executors} </li>
 * </ul>
 * <p>
 * Factories prefixed with {@code new} (eg. {@link #newBoundedElastic(int, int, String)} return a new instance of their flavor of {@link Scheduler},
 * while other factories like {@link #boundedElastic()} return a shared instance - which is the one used by operators requiring that flavor as their default Scheduler.
 *
 * @author Stephane Maldini
 */
public abstract class Schedulers {

	/**
	 * Default pool size, initialized by system property {@code reactor.schedulers.defaultPoolSize}
	 * and falls back to the number of processors available to the runtime on init.
	 *
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE =
			Optional.ofNullable(System.getProperty("reactor.schedulers.defaultPoolSize"))
					.map(Integer::parseInt)
					.orElseGet(() -> Runtime.getRuntime().availableProcessors());

	/**
	 * Default maximum size for the global {@link #boundedElastic()} {@link Scheduler}, initialized
	 * by system property {@code reactor.schedulers.defaultBoundedElasticSize} and falls back to 10 x number
	 * of processors available to the runtime on init.
	 *
	 * @see Runtime#availableProcessors()
	 * @see #boundedElastic()
	 */
	public static final int DEFAULT_BOUNDED_ELASTIC_SIZE =
			Optional.ofNullable(System.getProperty("reactor.schedulers.defaultBoundedElasticSize"))
					.map(Integer::parseInt)
					.orElseGet(() -> 10 * Runtime.getRuntime().availableProcessors());

	/**
	 * Default maximum number of enqueued tasks PER THREAD for the global {@link #boundedElastic()} {@link Scheduler},
	 * initialized by system property {@code reactor.schedulers.defaultBoundedElasticQueueSize} and falls back to
	 * a bound of 100 000 tasks per backing thread.
	 *
	 * @see #boundedElastic()
	 */
	public static final int DEFAULT_BOUNDED_ELASTIC_QUEUESIZE =
			Optional.ofNullable(System.getProperty("reactor.schedulers.defaultBoundedElasticQueueSize"))
			        .map(Integer::parseInt)
			        .orElse(100000);

	static volatile BiConsumer<Thread, ? super Throwable> onHandleErrorHook;

	/**
	 * Create a {@link Scheduler} which uses a backing {@link Executor} to schedule
	 * Runnables for async operators.
	 *
	 * <p>Tasks scheduled with workers of this Scheduler are not guaranteed to run in FIFO
	 * order and strictly non-concurrently.
	 * If FIFO order is desired, use trampoline parameter of {@link Schedulers#fromExecutor(Executor, boolean)}
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
	 * Trampolining here means tasks submitted in a burst are queued by the Worker itself,
	 * which acts as a sole task from the perspective of the {@link ExecutorService},
	 * so no reordering (but also no threading).
	 *
	 * @param executor an {@link Executor}
	 * @param trampoline set to false if this {@link Scheduler} is used by "operators"
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
	 * <p>
	 * Prefer using {@link #fromExecutorService(ExecutorService, String)},
	 * especially if you plan on using metrics as this gives the executor a meaningful identifier.
	 *
	 * @param executorService an {@link ExecutorService}
	 *
	 * @return a new {@link Scheduler}
	 */
	public static Scheduler fromExecutorService(ExecutorService executorService) {
		String executorServiceHashcode = Integer.toHexString(System.identityHashCode(executorService));
		return fromExecutorService(executorService, "anonymousExecutor@" + executorServiceHashcode);
	}

	/**
	 * Create a {@link Scheduler} which uses a backing {@link ExecutorService} to schedule
	 * Runnables for async operators.
	 *
	 * @param executorService an {@link ExecutorService}
	 *
	 * @return a new {@link Scheduler}
	 */
	public static Scheduler fromExecutorService(ExecutorService executorService, String executorName) {
		return new DelegateServiceScheduler(executorName, executorService);
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
	 * @return default instance of a {@link Scheduler} that dynamically creates ExecutorService-based
	 * Workers and caches the threads, reusing them once the Workers have been shut
	 * down
	 */
	public static Scheduler elastic() {
		return cache(CACHED_ELASTIC, ELASTIC, ELASTIC_SUPPLIER);
	}

	/**
	 * {@link Scheduler} that dynamically creates a bounded number of ExecutorService-based
	 * Workers, reusing them once the Workers have been shut down. The underlying daemon
	 * threads can be evicted if idle for more than {@link BoundedElasticScheduler#DEFAULT_TTL_SECONDS 60} seconds.
	 * <p>
	 * The maximum number of created threads is bounded by a {@code cap} (by default
	 * ten times the number of available CPU cores, see {@link #DEFAULT_BOUNDED_ELASTIC_SIZE}).
	 * The maximum number of task submissions that can be enqueued and deferred on each of these
	 * backing threads is bounded (by default 100K additional tasks, see
	 * {@link #DEFAULT_BOUNDED_ELASTIC_QUEUESIZE}). Past that point, a {@link RejectedExecutionException}
	 * is thrown.
	 * <p>
	 * By order of preference, threads backing a new {@link reactor.core.scheduler.Scheduler.Worker} are
	 * picked from the idle pool, created anew or reused from the busy pool. In the later case, a best effort
	 * attempt at picking the thread backing the least amount of workers is made.
	 * <p>
	 * Note that if a thread is backing a low amount of workers, but these workers submit a lot of pending tasks,
	 * a second worker could end up being backed by the same thread and see tasks rejected.
	 * The picking of the backing thread is also done once and for all at worker creation, so
	 * tasks could be delayed due to two workers sharing the same backing thread and submitting long-running tasks,
	 * despite another backing thread becoming idle in the meantime.
	 *
	 * @return a new {@link Scheduler} that dynamically create workers with an upper bound to
	 * the number of backing threads and after that on the number of enqueued tasks,
	 * that reuses threads and evict idle ones
	 */
	public static Scheduler boundedElastic() {
		return cache(CACHED_BOUNDED_ELASTIC, BOUNDED_ELASTIC, BOUNDED_ELASTIC_SUPPLIER);
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work.
	 *
	 * @return default instance of a {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler parallel() {
		return cache(CACHED_PARALLEL, PARALLEL, PARALLEL_SUPPLIER);
	}

	/**
	 * Executes tasks immediately instead of scheduling them.
	 * <p>
	 * As a consequence tasks run on the thread that submitted them (eg. the
	 * thread on which an operator is currently processing its onNext/onComplete/onError signals).
	 * This {@link Scheduler} is typically used as a "null object" for APIs that require a
	 * Scheduler but one doesn't want to change threads.
	 *
	 * @return a reusable {@link Scheduler} that executes tasks immediately instead of scheduling them
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
	 * @return a new {@link Scheduler} that dynamically creates ExecutorService-based
	 * Workers and caches the thread pools, reusing them once the Workers have been shut
	 * down
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
	 * @return a new {@link Scheduler} that dynamically creates ExecutorService-based
	 * Workers and caches the thread pools, reusing them once the Workers have been shut
	 * down
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
	 * @return a new {@link Scheduler} that dynamically creates ExecutorService-based
	 * Workers and caches the thread pools, reusing them once the Workers have been shut
	 * down
	 */
	public static Scheduler newElastic(String name, int ttlSeconds, boolean daemon) {
		return newElastic(ttlSeconds,
				new ReactorThreadFactory(name, ElasticScheduler.COUNTER, daemon, false,
						Schedulers::defaultUncaughtException));
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
	 * down
	 */
	public static Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
		return factory.newElastic(ttlSeconds, threadFactory);
	}


	/**
	 * {@link Scheduler} that dynamically creates a bounded number of ExecutorService-based
	 * Workers, reusing them once the Workers have been shut down. The underlying (user)
	 * threads can be evicted if idle for more than {@link BoundedElasticScheduler#DEFAULT_TTL_SECONDS 60} seconds.
	 * <p>
	 * The maximum number of created threads is bounded by the provided {@code threadCap}.
	 * The maximum number of task submissions that can be enqueued and deferred on each of these
	 * backing threads is bounded by the provided {@code queuedTaskCap}. Past that point,
	 * a {@link RejectedExecutionException} is thrown.
	 * <p>
	 * By order of preference, threads backing a new {@link reactor.core.scheduler.Scheduler.Worker} are
	 * picked from the idle pool, created anew or reused from the busy pool. In the later case, a best effort
	 * attempt at picking the thread backing the least amount of workers is made.
	 * <p>
	 * Note that if a thread is backing a low amount of workers, but these workers submit a lot of pending tasks,
	 * a second worker could end up being backed by the same thread and see tasks rejected.
	 * The picking of the backing thread is also done once and for all at worker creation, so
	 * tasks could be delayed due to two workers sharing the same backing thread and submitting long-running tasks,
	 * despite another backing thread becoming idle in the meantime.
	 * <p>
	 * This scheduler is restartable. Backing threads are user threads, so they will prevent the JVM
	 * from exiting until their worker has been disposed AND they've been evicted by TTL, or the whole
	 * scheduler has been {@link Scheduler#dispose() disposed}.
	 *
	 * @param threadCap maximum number of underlying threads to create
	 * @param queuedTaskCap maximum number of tasks to enqueue when no more threads can be created. Can be {@link Integer#MAX_VALUE} for unbounded enqueueing.
	 * @param name Thread prefix
	 * @return a new {@link Scheduler} that dynamically create workers with an upper bound to
	 * the number of backing threads and after that on the number of enqueued tasks,
	 * that reuses threads and evict idle ones
	 */
	public static Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, String name) {
		return newBoundedElastic(threadCap, queuedTaskCap, name, BoundedElasticScheduler.DEFAULT_TTL_SECONDS, false);
	}

	/**
	 * {@link Scheduler} that dynamically creates a bounded number of ExecutorService-based
	 * Workers, reusing them once the Workers have been shut down. The underlying (user)
	 * threads can be evicted if idle for more than the provided {@code ttlSeconds}.
	 * <p>
	 * The maximum number of created threads is bounded by the provided {@code threadCap}.
	 * The maximum number of task submissions that can be enqueued and deferred on each of these
	 * backing threads is bounded by the provided {@code queuedTaskCap}. Past that point,
	 * a {@link RejectedExecutionException} is thrown.
	 * <p>
	 * By order of preference, threads backing a new {@link reactor.core.scheduler.Scheduler.Worker} are
	 * picked from the idle pool, created anew or reused from the busy pool. In the later case, a best effort
	 * attempt at picking the thread backing the least amount of workers is made.
	 * <p>
	 * Note that if a thread is backing a low amount of workers, but these workers submit a lot of pending tasks,
	 * a second worker could end up being backed by the same thread and see tasks rejected.
	 * The picking of the backing thread is also done once and for all at worker creation, so
	 * tasks could be delayed due to two workers sharing the same backing thread and submitting long-running tasks,
	 * despite another backing thread becoming idle in the meantime.
	 * <p>
	 * This scheduler is restartable. Backing threads are user threads, so they will prevent the JVM
	 * from exiting until their worker has been disposed AND they've been evicted by TTL, or the whole
	 * scheduler has been {@link Scheduler#dispose() disposed}.
	 *
	 * @param threadCap maximum number of underlying threads to create
	 * @param queuedTaskCap maximum number of tasks to enqueue when no more threads can be created. Can be {@link Integer#MAX_VALUE} for unbounded enqueueing.
	 * @param name Thread prefix
	 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
	 * @return a new {@link Scheduler} that dynamically create workers with an upper bound to
	 * the number of backing threads and after that on the number of enqueued tasks,
	 * that reuses threads and evict idle ones
	 */
	public static Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, String name, int ttlSeconds) {
		return newBoundedElastic(threadCap, queuedTaskCap, name, ttlSeconds, false);
	}

	/**
	 * {@link Scheduler} that dynamically creates a bounded number of ExecutorService-based
	 * Workers, reusing them once the Workers have been shut down. The underlying (user or daemon)
	 * threads can be evicted if idle for more than the provided {@code ttlSeconds}.
	 * <p>
	 * The maximum number of created threads is bounded by the provided {@code threadCap}.
	 * The maximum number of task submissions that can be enqueued and deferred on each of these
	 * backing threads is bounded by the provided {@code queuedTaskCap}. Past that point,
	 * a {@link RejectedExecutionException} is thrown.
	 * <p>
	 * By order of preference, threads backing a new {@link reactor.core.scheduler.Scheduler.Worker} are
	 * picked from the idle pool, created anew or reused from the busy pool. In the later case, a best effort
	 * attempt at picking the thread backing the least amount of workers is made.
	 * <p>
	 * Note that if a thread is backing a low amount of workers, but these workers submit a lot of pending tasks,
	 * a second worker could end up being backed by the same thread and see tasks rejected.
	 * The picking of the backing thread is also done once and for all at worker creation, so
	 * tasks could be delayed due to two workers sharing the same backing thread and submitting long-running tasks,
	 * despite another backing thread becoming idle in the meantime.
	 * <p>
	 * This scheduler is restartable. Depending on the {@code daemon} parameter, backing threads can be
	 * user threads or daemon threads. Note that user threads will prevent the JVM from exiting until their
	 * worker has been disposed AND they've been evicted by TTL, or the whole scheduler has been
	 * {@link Scheduler#dispose() disposed}.
	 *
	 * @param threadCap maximum number of underlying threads to create
	 * @param queuedTaskCap maximum number of tasks to enqueue when no more threads can be created. Can be {@link Integer#MAX_VALUE} for unbounded enqueueing.
	 * @param name Thread prefix
	 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
	 * @param daemon are backing threads {@link Thread#setDaemon(boolean) daemon threads}
	 * @return a new {@link Scheduler} that dynamically create workers with an upper bound to
	 * the number of backing threads and after that on the number of enqueued tasks,
	 * that reuses threads and evict idle ones
	 */
	public static Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, String name, int ttlSeconds, boolean daemon) {
		return newBoundedElastic(threadCap, queuedTaskCap,
				new ReactorThreadFactory(name, ElasticScheduler.COUNTER, daemon, false,
						Schedulers::defaultUncaughtException),
				ttlSeconds);
	}

	/**
	 * {@link Scheduler} that dynamically creates a bounded number of ExecutorService-based
	 * Workers, reusing them once the Workers have been shut down. The underlying (user)
	 * threads can be evicted if idle for more than the provided {@code ttlSeconds}.
	 * <p>
	 * The maximum number of created threads is bounded by the provided {@code threadCap}.
	 * The maximum number of task submissions that can be enqueued and deferred on each of these
	 * backing threads is bounded by the provided {@code queuedTaskCap}. Past that point,
	 * a {@link RejectedExecutionException} is thrown.
	 * <p>
	 * By order of preference, threads backing a new {@link reactor.core.scheduler.Scheduler.Worker} are
	 * picked from the idle pool, created anew or reused from the busy pool. In the later case, a best effort
	 * attempt at picking the thread backing the least amount of workers is made.
	 * <p>
	 * Note that if a thread is backing a low amount of workers, but these workers submit a lot of pending tasks,
	 * a second worker could end up being backed by the same thread and see tasks rejected.
	 * The picking of the backing thread is also done once and for all at worker creation, so
	 * tasks could be delayed due to two workers sharing the same backing thread and submitting long-running tasks,
	 * despite another backing thread becoming idle in the meantime.
	 * <p>
	 * This scheduler is restartable. Backing threads are created by the provided {@link ThreadFactory},
	 * which can decide whether to create user threads or daemon threads. Note that user threads
	 * will prevent the JVM from exiting until their worker has been disposed AND they've been evicted by TTL,
	 * or the whole scheduler has been {@link Scheduler#dispose() disposed}.
	 *
	 * @param threadCap maximum number of underlying threads to create
	 * @param queuedTaskCap maximum number of tasks to enqueue when no more threads can be created. Can be {@link Integer#MAX_VALUE} for unbounded enqueueing.
	 * @param threadFactory a {@link ThreadFactory} to use each thread initialization
	 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
	 * @return a new {@link Scheduler} that dynamically create workers with an upper bound to
	 * the number of backing threads and after that on the number of enqueued tasks,
	 * that reuses threads and evict idle ones
	 */
	public static Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
		return factory.newBoundedElastic(threadCap, queuedTaskCap, threadFactory, ttlSeconds);
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work. This type of {@link Scheduler} detects and
	 * rejects usage of blocking Reactor APIs.
	 *
	 * @param name Thread prefix
	 *
	 * @return a new {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work
	 */
	public static Scheduler newParallel(String name) {
		return newParallel(name, DEFAULT_POOL_SIZE);
	}

	/**
	 * {@link Scheduler} that hosts a fixed pool of single-threaded ExecutorService-based
	 * workers and is suited for parallel work. This type of {@link Scheduler} detects and
	 * rejects usage of blocking Reactor APIs.
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
	 * workers and is suited for parallel work. This type of {@link Scheduler} detects and
	 * rejects usage of blocking Reactor APIs.
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
				new ReactorThreadFactory(name, ParallelScheduler.COUNTER, daemon,
						true, Schedulers::defaultUncaughtException));
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
	 * suited for parallel work. This type of {@link Scheduler} detects and rejects usage
	 * 	 * of blocking Reactor APIs.
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
	 * suited for parallel work. This type of {@link Scheduler} detects and rejects usage
	 * of blocking Reactor APIs.
	 *
	 * @param name Component and thread name prefix
	 * @param daemon false if the {@link Scheduler} requires an explicit {@link
	 * Scheduler#dispose()} to exit the VM.
	 *
	 * @return a new {@link Scheduler} that hosts a single-threaded ExecutorService-based
	 * worker
	 */
	public static Scheduler newSingle(String name, boolean daemon) {
		return newSingle(new ReactorThreadFactory(name, SingleScheduler.COUNTER, daemon,
				true, Schedulers::defaultUncaughtException));
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
	 * @param c the new hook to set.
	 */
	public static void onHandleError(BiConsumer<Thread, ? super Throwable> c) {
		if (log.isDebugEnabled()) {
			log.debug("Hooking new default: onHandleError");
		}
		onHandleErrorHook = Objects.requireNonNull(c, "onHandleError");
	}

	/**
	 * Check if calling a Reactor blocking API in the current {@link Thread} is forbidden
	 * or not, by checking if the thread implements {@link NonBlocking} (in which case it is
	 * forbidden and this method returns {@code true}).
	 *
	 * @return {@code true} if blocking is forbidden in this thread, {@code false} otherwise
	 */
	public static boolean isInNonBlockingThread() {
		return Thread.currentThread() instanceof NonBlocking;
	}

	/**
	 * Check if calling a Reactor blocking API in the given {@link Thread} is forbidden
	 * or not, by checking if the thread implements {@link NonBlocking} (in which case it is
	 * forbidden and this method returns {@code true}).
	 *
	 * @return {@code true} if blocking is forbidden in that thread, {@code false} otherwise
	 */
	public static boolean isNonBlockingThread(Thread t) {
		return t instanceof NonBlocking;
	}

	/**
	 * If Micrometer is available, set-up a decorator that will instrument any
	 * {@link ExecutorService} that backs a {@link Scheduler}.
	 * No-op if Micrometer isn't available.
	 *
	 * This instrumentation sends data to the Micrometer Global Registry.
	 *
	 * @implNote Note that this is added as a decorator via Schedulers when enabling metrics for schedulers, which doesn't change the Factory.
	 */
	public static void enableMetrics() {
		if (Metrics.isInstrumentationAvailable()) {
			addExecutorServiceDecorator(SchedulerMetricDecorator.METRICS_DECORATOR_KEY, new SchedulerMetricDecorator());
		}
	}

	/**
	 * If {@link #enableMetrics()} has been previously called, removes the decorator.
	 * No-op if {@link #enableMetrics()} hasn't been called.
	 */
	public static void disableMetrics() {
		removeExecutorServiceDecorator(SchedulerMetricDecorator.METRICS_DECORATOR_KEY);
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
	 * Set up an additional {@link ScheduledExecutorService} decorator for a given key
	 * only if that key is not already present.
	 * <p>
	 * The decorator is a {@link BiFunction} taking the Scheduler and the backing
	 * {@link ScheduledExecutorService} as second argument. It returns the
	 * decorated {@link ScheduledExecutorService}.
	 *
	 * @param key the key under which to set up the decorator
	 * @param decorator the executor service decorator to add, if key not already present.
	 * @return true if the decorator was added, false if a decorator was already present
	 * for this key.
	 * @see #setExecutorServiceDecorator(String, BiFunction)
	 * @see #removeExecutorServiceDecorator(String)
	 * @see Schedulers#onScheduleHook(String, Function)
	 */
	public static boolean addExecutorServiceDecorator(String key, BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService> decorator) {
		synchronized (DECORATORS) {
			return DECORATORS.putIfAbsent(key, decorator) == null;
		}
	}

	/**
	 * Set up an additional {@link ScheduledExecutorService} decorator for a given key,
	 * even if that key is already present.
	 * <p>
	 * The decorator is a {@link BiFunction} taking the Scheduler and the backing
	 * {@link ScheduledExecutorService} as second argument. It returns the
	 * decorated {@link ScheduledExecutorService}.
	 *
	 * @param key the key under which to set up the decorator
	 * @param decorator the executor service decorator to add, if key not already present.
	 * @see #addExecutorServiceDecorator(String, BiFunction)
	 * @see #removeExecutorServiceDecorator(String)
	 * @see Schedulers#onScheduleHook(String, Function)
	 */
	public static void setExecutorServiceDecorator(String key, BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService> decorator) {
		synchronized (DECORATORS) {
			DECORATORS.put(key, decorator);
		}
	}

	/**
	 * Remove an existing {@link ScheduledExecutorService} decorator if it has been set up
	 * via {@link #addExecutorServiceDecorator(String, BiFunction)}.
	 * <p>
	 * In case the decorator implements {@link Disposable}, it is also
	 * {@link Disposable#dispose() disposed}.
	 *
	 * @param key the key for the executor service decorator to remove
	 * @return the removed decorator, or null if none was set for that key
	 * @see #addExecutorServiceDecorator(String, BiFunction)
	 * @see #setExecutorServiceDecorator(String, BiFunction)
	 */
	public static BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService> removeExecutorServiceDecorator(String key) {
		BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService> removed;
		synchronized (DECORATORS) {
			removed = DECORATORS.remove(key);
		}
		if (removed instanceof Disposable) {
			((Disposable) removed).dispose();
		}
		return removed;
	}

	/**
	 * This method is aimed at {@link Scheduler} implementors, enabling custom implementations
	 * that are backed by a {@link ScheduledExecutorService} to also have said executors
	 * decorated (ie. for instrumentation purposes).
	 * <p>
	 * It <strong>applies</strong> the decorators added via
	 * {@link #addExecutorServiceDecorator(String, BiFunction)}, so it shouldn't be added
	 * as a decorator. Note also that decorators are not guaranteed to be idempotent, so
	 * this method should be called only once per executor.
	 *
	 * @param owner a {@link Scheduler} that owns the {@link ScheduledExecutorService}
	 * @param original the {@link ScheduledExecutorService} that the {@link Scheduler}
	 * wants to use originally
	 * @return the decorated {@link ScheduledExecutorService}, or the original if no decorator is set up
	 * @see #addExecutorServiceDecorator(String, BiFunction)
	 * @see #removeExecutorServiceDecorator(String)
	 */
	public static ScheduledExecutorService decorateExecutorService(Scheduler owner, ScheduledExecutorService original) {
		synchronized (DECORATORS) {
			for (BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService> decorator : DECORATORS.values()) {
				original = decorator.apply(owner, original);
			}
		}

		return original;
	}

	/**
	 * Add or replace a named scheduling {@link Function decorator}. With subsequent calls
	 * to this method, the onScheduleHook hook can be a composite of several sub-hooks, each
	 * with a different key.
	 * <p>
	 * The sub-hook is a {@link Function} taking the scheduled {@link Runnable}.
	 * It returns the decorated {@link Runnable}.
	 *
	 * @param key the key under which to set up the onScheduleHook sub-hook
	 * @param decorator the {@link Runnable} decorator to add (or replace, if key is already present)
	 * @see #resetOnScheduleHook(String)
	 * @see #resetOnScheduleHooks()
	 */
	public static void onScheduleHook(String key, Function<Runnable, Runnable> decorator) {
		synchronized (onScheduleHooks) {
			onScheduleHooks.put(key, decorator);
			Function<Runnable, Runnable> newHook = null;
			for (Function<Runnable, Runnable> function : onScheduleHooks.values()) {
				if (newHook == null) {
					newHook = function;
				}
				else {
					newHook = newHook.andThen(function);
				}
			}
			onScheduleHook = newHook;
		}
	}

	/**
	 * Reset a specific onScheduleHook {@link Function sub-hook} if it has been set up
	 * via {@link #onScheduleHook(String, Function)}.
	 *
	 * @param key the key for onScheduleHook sub-hook to remove
	 * @see #onScheduleHook(String, Function)
	 * @see #resetOnScheduleHooks()
	 */
	public static void resetOnScheduleHook(String key) {
		synchronized (onScheduleHooks) {
			onScheduleHooks.remove(key);
			if (onScheduleHooks.isEmpty()) {
				onScheduleHook = Function.identity();
			}
			else {
				Function<Runnable, Runnable> newHook = null;
				for (Function<Runnable, Runnable> function : onScheduleHooks.values()) {
					if (newHook == null) {
						newHook = function;
					}
					else {
						newHook = newHook.andThen(function);
					}
				}
				onScheduleHook = newHook;
			}
		}
	}

	/**
	 * Remove all onScheduleHook {@link Function sub-hooks}.
	 *
	 * @see #onScheduleHook(String, Function)
	 * @see #resetOnScheduleHook(String)
	 */
	public static void resetOnScheduleHooks() {
		synchronized (onScheduleHooks) {
			onScheduleHooks.clear();
			onScheduleHook = null;
		}
	}

	/**
	 * Applies the hooks registered with {@link Schedulers#onScheduleHook(String, Function)}.
	 *
	 * @param runnable a {@link Runnable} submitted to a {@link Scheduler}
	 * @return decorated {@link Runnable} if any hook is registered, the original otherwise.
	 */
	public static Runnable onSchedule(Runnable runnable) {
		Function<Runnable, Runnable> hook = onScheduleHook;
		if (hook != null) {
			return hook.apply(runnable);
		}
		else {
			return runnable;
		}
	}

	/**
	 * Clear any cached {@link Scheduler} and call dispose on them.
	 */
	public static void shutdownNow() {
		CachedScheduler oldElastic = CACHED_ELASTIC.getAndSet(null);
		CachedScheduler oldBoundedElastic = CACHED_BOUNDED_ELASTIC.getAndSet(null);
		CachedScheduler oldParallel = CACHED_PARALLEL.getAndSet(null);
		CachedScheduler oldSingle = CACHED_SINGLE.getAndSet(null);

		if (oldElastic != null) oldElastic._dispose();
		if (oldBoundedElastic != null) oldBoundedElastic._dispose();
		if (oldParallel != null) oldParallel._dispose();
		if (oldSingle != null) oldSingle._dispose();
	}

	/**
	 * {@link Scheduler} that hosts a single-threaded ExecutorService-based worker and is
	 * suited for parallel work. Will cache the returned schedulers for subsequent calls until dispose.
	 *
	 * @return default instance of a {@link Scheduler} that hosts a single-threaded
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
		 * {@link Scheduler} that dynamically creates Workers resources and caches
		 * eventually, reusing them once the Workers have been shut down.
		 * <p>
		 * The maximum number of created workers is unbounded.
		 *
		 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
		 * @param threadFactory a {@link ThreadFactory} to use
		 *
		 * @return a new {@link Scheduler} that dynamically creates Workers resources and
		 * caches eventually, reusing them once the Workers have been shut down
		 */
		default Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
			return new ElasticScheduler(threadFactory, ttlSeconds);
		}

		/**
		 * {@link Scheduler} that dynamically creates a bounded number of ExecutorService-based
		 * Workers, reusing them once the Workers have been shut down. The underlying (user or daemon)
		 * threads can be evicted if idle for more than {@code ttlSeconds}.
		 * <p>
		 * The maximum number of created thread pools is bounded by the provided {@code cap}.
		 *
		 * @param threadCap maximum number of underlying threads to create
		 * @param queuedTaskCap maximum number of tasks to enqueue when no more threads can be created. Can be {@link Integer#MAX_VALUE} for unbounded enqueueing.
		 * @param threadFactory a {@link ThreadFactory} to use each thread initialization
		 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
		 *
		 * @return a new {@link Scheduler} that dynamically create workers with an upper bound to
		 * the number of backing threads, reuses threads and evict idle ones
		 */
		default Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
			return new BoundedElasticScheduler(threadCap, queuedTaskCap, threadFactory, ttlSeconds);
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
	static final String ELASTIC               = "elastic"; // IO stuff
	static final String BOUNDED_ELASTIC       = "boundedElastic"; // Blocking stuff with scale to zero
	static final String PARALLEL              = "parallel"; //scale up common tasks
	static final String SINGLE                = "single"; //non blocking tasks
	static final String IMMEDIATE             = "immediate";
	static final String FROM_EXECUTOR         = "fromExecutor";
	static final String FROM_EXECUTOR_SERVICE = "fromExecutorService";


	// Cached schedulers in atomic references:
	static AtomicReference<CachedScheduler> CACHED_ELASTIC         = new AtomicReference<>();
	static AtomicReference<CachedScheduler> CACHED_BOUNDED_ELASTIC = new AtomicReference<>();
	static AtomicReference<CachedScheduler> CACHED_PARALLEL        = new AtomicReference<>();
	static AtomicReference<CachedScheduler> CACHED_SINGLE          = new AtomicReference<>();

	static final Supplier<Scheduler> ELASTIC_SUPPLIER =
			() -> newElastic(ELASTIC, ElasticScheduler.DEFAULT_TTL_SECONDS, true);

	static final Supplier<Scheduler> BOUNDED_ELASTIC_SUPPLIER =
			() -> newBoundedElastic(DEFAULT_BOUNDED_ELASTIC_SIZE, DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
					BOUNDED_ELASTIC, BoundedElasticScheduler.DEFAULT_TTL_SECONDS, true);

	static final Supplier<Scheduler> PARALLEL_SUPPLIER =
			() -> newParallel(PARALLEL, DEFAULT_POOL_SIZE, true);

	static final Supplier<Scheduler> SINGLE_SUPPLIER = () -> newSingle(SINGLE, true);

	static final Factory DEFAULT = new Factory() { };

	static final Map<String, BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService>>
			DECORATORS = new LinkedHashMap<>();

	static volatile Factory factory = DEFAULT;

	private static final LinkedHashMap<String, Function<Runnable, Runnable>> onScheduleHooks = new LinkedHashMap<>(1);

	@Nullable
	private static Function<Runnable, Runnable> onScheduleHook;

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

	static final void defaultUncaughtException(Thread t, Throwable e) {
		Schedulers.log.error("Scheduler worker in group " + t.getThreadGroup().getName()
				+ " failed with an uncaught exception", e);
	}

	static void handleError(Throwable ex) {
		Thread thread = Thread.currentThread();
		Throwable t = unwrap(ex);
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

	static class CachedScheduler implements Scheduler, Supplier<Scheduler>, Scannable {

		final Scheduler cached;
		final String    stringRepresentation;

		CachedScheduler(String key, Scheduler cached) {
			this.cached = cached;
			this.stringRepresentation = "Schedulers." + key + "()";
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

		@Override
		public String toString() {
			return stringRepresentation;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (Attr.NAME == key) return stringRepresentation;
			return Scannable.from(cached).scanUnsafe(key);
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
			@Nullable Disposable parent,
			long delay,
			TimeUnit unit) {
		task = onSchedule(task);
		SchedulerTask sr = new SchedulerTask(task, parent);
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
		task = onSchedule(task);

		if (period <= 0L) {
			InstantPeriodicWorkerTask isr =
					new InstantPeriodicWorkerTask(task, exec);
			Future<?> f;
			if (initialDelay <= 0L) {
				f = exec.submit(isr);
			}
			else {
				f = exec.schedule(isr, initialDelay, unit);
			}
			isr.setFirst(f);

			return isr;
		}
		else {
			PeriodicSchedulerTask sr = new PeriodicSchedulerTask(task);
			Future<?> f = exec.scheduleAtFixedRate(sr, initialDelay, period, unit);
			sr.setFuture(f);

			return sr;
		}
	}

	static Disposable workerSchedule(ScheduledExecutorService exec,
			Disposable.Composite tasks,
			Runnable task,
			long delay,
			TimeUnit unit) {
		task = onSchedule(task);

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
		task = onSchedule(task);

		if (period <= 0L) {
			InstantPeriodicWorkerTask isr =
					new InstantPeriodicWorkerTask(task, exec, tasks);
			if (!tasks.add(isr)) {
			  throw Exceptions.failWithRejected();
			}
			try {
				Future<?> f;
				if (initialDelay <= 0L) {
					f = exec.submit(isr);
				}
				else {
					f = exec.schedule(isr, initialDelay, unit);
				}
				isr.setFirst(f);
			}
			catch (RejectedExecutionException ex) {
				isr.dispose();
				//RejectedExecutionException are propagated up
				throw ex;
			}
			catch (IllegalArgumentException | NullPointerException ex) {
				isr.dispose();
				//IllegalArgumentException are wrapped into RejectedExecutionException and propagated up
				throw new RejectedExecutionException(ex);
			}

			return isr;
		}

		PeriodicWorkerTask sr = new PeriodicWorkerTask(task, tasks);
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
		catch (IllegalArgumentException | NullPointerException ex) {
			sr.dispose();
			//IllegalArgumentException are wrapped into RejectedExecutionException and propagated up
			throw new RejectedExecutionException(ex);
		}

		return sr;
	}

	/**
	 * Scan an {@link Executor} or {@link ExecutorService}, recognizing several special
	 * implementations. Unwraps some decorating schedulers, recognizes {@link Scannable}
	 * schedulers and delegates to their {@link Scannable#scanUnsafe(Scannable.Attr)}
	 * method, introspects {@link ThreadPoolExecutor} instances.
	 * <p>
	 * If no data can be extracted, defaults to the provided {@code orElse}
	 * {@link Scannable#scanUnsafe(Scannable.Attr) scanUnsafe}.
	 *
	 * @param executor the executor to introspect in a best effort manner.
	 * @param key the key to scan for. CAPACITY and BUFFERED mainly.
	 * @return an equivalent of {@link Scannable#scanUnsafe(Scannable.Attr)} but that can
	 * also work on some implementations of {@link Executor}
	 */
	@Nullable
	static final Object scanExecutor(Executor executor, Scannable.Attr key) {
		if (executor instanceof DelegateServiceScheduler.UnsupportedScheduledExecutorService) {
			executor = ((DelegateServiceScheduler.UnsupportedScheduledExecutorService) executor).get();
		}
		if (executor instanceof Scannable) {
			return ((Scannable) executor).scanUnsafe(key);
		}

		if (executor instanceof ExecutorService) {
			ExecutorService service = (ExecutorService) executor;
			if (key == Scannable.Attr.TERMINATED) return service.isTerminated();
			if (key == Scannable.Attr.CANCELLED) return service.isShutdown();
		}

		if (executor instanceof ThreadPoolExecutor) {
				final ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) executor;
				if (key == Scannable.Attr.CAPACITY) return poolExecutor.getMaximumPoolSize();
				if (key == Scannable.Attr.BUFFERED) return ((Long) (poolExecutor.getTaskCount() - poolExecutor.getCompletedTaskCount())).intValue();
				if (key == Scannable.Attr.LARGE_BUFFERED) return poolExecutor.getTaskCount() - poolExecutor.getCompletedTaskCount();
		}

		return null;
	}

}
