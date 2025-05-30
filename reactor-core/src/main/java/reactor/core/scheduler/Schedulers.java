/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Collection;
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
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
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
 *     <li>{@link #boundedElastic()}: Optimized for longer executions, an alternative for blocking tasks where the number of active tasks (and threads) is capped</li>
 *     <li>{@link #immediate}: to immediately run submitted {@link Runnable} instead of scheduling them (somewhat of a no-op or "null object" {@link Scheduler})</li>
 *     <li>{@link #fromExecutorService(ExecutorService)} to create new instances around {@link java.util.concurrent.Executors} </li>
 * </ul>
 * <p>
 * Factories prefixed with {@code new} (eg. {@link #newBoundedElastic(int, int, String)} return a new instance of their flavor of {@link Scheduler},
 * while other factories like {@link #boundedElastic()} return a shared instance - which is the one used by operators requiring that flavor as their default Scheduler.
 * All instances are returned in a {@link Scheduler#init() initialized} state.
 * <p>
 * Since 3.6.0 {@link #boundedElastic()} can run tasks on {@link VirtualThread}s if the application
 * runs on a Java 21+ runtime and the {@link #DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS}
 * system property is set to {@code true}.
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

	/**
	 * Default execution of enqueued tasks on {@link Thread#ofVirtual} for the global
	 * {@link #boundedElastic()} {@link Scheduler},
	 * initialized by system property {@code reactor.schedulers.defaultBoundedElasticOnVirtualThreads}
	 * and falls back to false .
	 *
	 * @see #boundedElastic()
	 */
	public static final boolean DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS =
			Optional.ofNullable(System.getProperty("reactor.schedulers.defaultBoundedElasticOnVirtualThreads"))
			        .map(Boolean::parseBoolean)
			        .orElse(false);

	static final Predicate<Thread> DEFAULT_NON_BLOCKING_THREAD_PREDICATE = thread -> false;

	static Predicate<Thread> nonBlockingThreadPredicate = DEFAULT_NON_BLOCKING_THREAD_PREDICATE;

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
		final ExecutorScheduler scheduler = new ExecutorScheduler(executor, trampoline);
		scheduler.init();
		return scheduler;
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
		final DelegateServiceScheduler scheduler = new DelegateServiceScheduler(executorName, executorService);
		scheduler.init();
		return scheduler;
	}

	/**
	 * The common <em>boundedElastic</em> instance, a {@link Scheduler} that
	 * dynamically creates a bounded number of workers.
	 * <p>
	 * Depends on the available environment and specified configurations, there are two types
	 * of implementations for this shared scheduler:
	 * <ul>
	 *
	 * <li> ExecutorService-based implementation tailored to run on Platform {@link Thread}
	 * instances. Every Worker is {@link ExecutorService}-based. Reusing {@link Thread}s
	 * once the Workers have been shut down. The underlying daemon threads can be
	 * evicted if idle for more than
	 * {@link BoundedElasticScheduler#DEFAULT_TTL_SECONDS 60} seconds.
	 * </li>
	 *
	 * <li> As of 3.6.0 there is a thread-per-task implementation tailored for use
	 * with virtual threads. This implementation is enabled if the
	 * application runs on a JDK 21+ runtime and the system property
	 * {@link #DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS} is set to
	 * {@code true}. Every Worker is based on the custom implementation of the execution
	 * mechanism which ensures every submitted task runs on a new
	 * {@link VirtualThread} instance. This implementation has a shared instance of
	 * {@link ScheduledExecutorService} used to schedule delayed and periodic tasks
	 * such that when triggered they are offloaded to a dedicated new
	 * {@link VirtualThread} instance.
	 * </li>
	 *
	 * </ul>
	 *
	 * <p>
	 * Both implementations share the same configurations:
	 * <ul>
	 * <li>
	 * The maximum number of concurrent
	 * threads is bounded by a {@code cap} (by default
	 * ten times the number of available CPU cores, see {@link #DEFAULT_BOUNDED_ELASTIC_SIZE}).
	 * <p>
	 * <b> Note: </b> Consider increasing {@link #DEFAULT_BOUNDED_ELASTIC_SIZE} with the
	 * thread-per-task implementation to run more concurrent {@link VirtualThread}
	 * instances underneath.
	 * </li>
	 * <li>
	 * The maximum number of task submissions that can be enqueued and deferred on each of these
	 * backing threads is bounded (by default 100K additional tasks, see
	 * {@link #DEFAULT_BOUNDED_ELASTIC_QUEUESIZE}). Past that point, a {@link RejectedExecutionException}
	 * is thrown.
	 * </li>
	 * </ul>
	 *
	 * <p>
	 * Threads backing a new {@link reactor.core.scheduler.Scheduler.Worker} are
	 * picked from a pool or are created when needed. In the ExecutorService-based
	 * implementation, the pool is comprised either of idle or busy threads. When all
	 * threads are busy, a best effort attempt is made at picking the thread backing
	 * the least number of workers. In the case of the thread-per-task implementation, it
	 * always creates new threads up to the specified limit.
	 * <p>
	 * Note that if a scheduling mechanism is backing a low amount of workers, but these
	 * workers submit a lot of pending tasks, a second worker could end up being
	 * backed by the same mechanism and see tasks rejected.
	 * The picking of the backing mechanism is also done once and for all at worker
	 * creation, so tasks could be delayed due to two workers sharing the same backing
	 * mechanism and submitting long-running tasks, despite another backing mechanism
	 * becoming idle in the meantime.
	 * <p>
	 * Only one instance of this common scheduler will be created on the first call and is cached. The same instance
	 * is returned on subsequent calls until it is disposed.
	 * <p>
	 * One cannot directly {@link Scheduler#dispose() dispose} the common instances, as they are cached and shared
	 * between callers. They can however be all {@link #shutdownNow() shut down} together, or replaced by a
	 * {@link #setFactory(Factory) change in Factory}.
	 *
	 * <p>
	 *
	 * @return the ExecutorService/thread-per-task-based <em>boundedElastic</em>
	 * instance.
	 * A {@link Scheduler} that dynamically creates workers with an upper
	 * bound to the number of backing threads and after that on the number of enqueued tasks.
	 */
	public static Scheduler boundedElastic() {
		return cache(CACHED_BOUNDED_ELASTIC, BOUNDED_ELASTIC, BOUNDED_ELASTIC_SUPPLIER);
	}

	/**
	 * The common <em>parallel</em> instance, a {@link Scheduler} that hosts a fixed pool of single-threaded
	 * ExecutorService-based workers and is suited for parallel work.
	 * <p>
	 * Only one instance of this common scheduler will be created on the first call and is cached. The same instance
	 * is returned on subsequent calls until it is disposed.
	 * <p>
	 * One cannot directly {@link Scheduler#dispose() dispose} the common instances, as they are cached and shared
	 * between callers. They can however be all {@link #shutdownNow() shut down} together, or replaced by a
	 * {@link #setFactory(Factory) change in Factory}.
	 *
	 * @return the common <em>parallel</em> instance, a {@link Scheduler} that hosts a fixed pool of single-threaded
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
	 * Threads backing this scheduler are user threads, so they will prevent the JVM
	 * from exiting until their worker has been disposed AND they've been evicted by TTL, or the whole
	 * scheduler has been {@link Scheduler#dispose() disposed}.
	 *
	 * <p>
	 * Please note, this implementation is not designed to run tasks on
	 * {@link VirtualThread}. Please see
	 * {@link Factory#newThreadPerTaskBoundedElastic(int, int, ThreadFactory)} if you need
	 * {@link VirtualThread} compatible scheduler implementation
	 *
	 * @param threadCap maximum number of underlying threads to create
	 * @param queuedTaskCap maximum number of tasks to enqueue when no more threads can be created. Can be {@link Integer#MAX_VALUE} for unbounded enqueueing.
	 * @param name Thread prefix
	 * @return a new {@link Scheduler} that dynamically creates workers with an upper bound to
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
	 * Threads backing this scheduler are user threads, so they will prevent the JVM
	 * from exiting until their worker has been disposed AND they've been evicted by TTL, or the whole
	 * scheduler has been {@link Scheduler#dispose() disposed}.
	 *
	 * <p>
	 * Please note, this implementation is not designed to run tasks on
	 * {@link VirtualThread}. Please see
	 * {@link Factory#newThreadPerTaskBoundedElastic(int, int, ThreadFactory)} if you need
	 * {@link VirtualThread} compatible scheduler implementation
	 *
	 * @param threadCap maximum number of underlying threads to create
	 * @param queuedTaskCap maximum number of tasks to enqueue when no more threads can be created. Can be {@link Integer#MAX_VALUE} for unbounded enqueueing.
	 * @param name Thread prefix
	 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
	 * @return a new {@link Scheduler} that dynamically creates workers with an upper bound to
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
	 * Depending on the {@code daemon} parameter, threads backing this scheduler can be
	 * user threads or daemon threads. Note that user threads will prevent the JVM from exiting until their
	 * worker has been disposed AND they've been evicted by TTL, or the whole scheduler has been
	 * {@link Scheduler#dispose() disposed}.
	 *
	 * <p>
	 * Please note, this implementation is not designed to run tasks on
	 * {@link VirtualThread}. Please see
	 * {@link Factory#newThreadPerTaskBoundedElastic(int, int, ThreadFactory)} if you need
	 * {@link VirtualThread} compatible scheduler implementation
	 *
	 * @param threadCap maximum number of underlying threads to create
	 * @param queuedTaskCap maximum number of tasks to enqueue when no more threads can be created. Can be {@link Integer#MAX_VALUE} for unbounded enqueueing.
	 * @param name Thread prefix
	 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
	 * @param daemon are backing threads {@link Thread#setDaemon(boolean) daemon threads}
	 * @return a new {@link Scheduler} that dynamically creates workers with an upper bound to
	 * the number of backing threads and after that on the number of enqueued tasks,
	 * that reuses threads and evict idle ones
	 */
	public static Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, String name, int ttlSeconds, boolean daemon) {
		return newBoundedElastic(threadCap, queuedTaskCap,
				new ReactorThreadFactory(name, BoundedElasticScheduler.COUNTER, daemon, false,
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
	 * Threads backing this scheduler are created by the provided {@link ThreadFactory},
	 * which can decide whether to create user threads or daemon threads. Note that user threads
	 * will prevent the JVM from exiting until their worker has been disposed AND they've been evicted by TTL,
	 * or the whole scheduler has been {@link Scheduler#dispose() disposed}.
	 *
	 * <p>
	 * Please note, this implementation is not designed to run tasks on
	 * {@link VirtualThread}. Please see
	 * {@link Factory#newThreadPerTaskBoundedElastic(int, int, ThreadFactory)} if you need
	 * {@link VirtualThread} compatible scheduler implementation
	 *
	 * @param threadCap maximum number of underlying threads to create
	 * @param queuedTaskCap maximum number of tasks to enqueue when no more threads can be created. Can be {@link Integer#MAX_VALUE} for unbounded enqueueing.
	 * @param threadFactory a {@link ThreadFactory} to use each thread initialization
	 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
	 * @return a new {@link Scheduler} that dynamically creates workers with an upper bound to
	 * the number of backing threads and after that on the number of enqueued tasks,
	 * that reuses threads and evict idle ones
	 */
	public static Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
		Scheduler fromFactory = factory.newBoundedElastic(threadCap,
				queuedTaskCap,
				threadFactory,
				ttlSeconds);
		fromFactory.init();
		return fromFactory;
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
		final Scheduler fromFactory = factory.newParallel(parallelism, threadFactory);
		fromFactory.init();
		return fromFactory;
	}

	/**
	 * {@link Scheduler} that hosts a single-threaded ExecutorService-based worker. This type of {@link Scheduler}
	 * detects and rejects usage of blocking Reactor APIs.
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
	 * {@link Scheduler} that hosts a single-threaded ExecutorService-based worker. This type of {@link Scheduler}
	 * detects and rejects usage of blocking Reactor APIs.
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
	 * {@link Scheduler} that hosts a single-threaded ExecutorService-based worker.
	 *
	 * @param threadFactory a {@link ThreadFactory} to use for the unique thread of the
	 * {@link Scheduler}
	 *
	 * @return a new {@link Scheduler} that hosts a single-threaded ExecutorService-based
	 * worker
	 */
	public static Scheduler newSingle(ThreadFactory threadFactory) {
		final Scheduler fromFactory = factory.newSingle(threadFactory);
		fromFactory.init();
		return fromFactory;
	}

	/**
	 * Define a hook anonymous part that is executed alongside keyed parts when a {@link Scheduler} has
	 * {@link #handleError(Throwable) handled an error}. Note that it is executed after
	 * the error has been passed to the thread uncaughtErrorHandler, which is not the
	 * case when a fatal error occurs (see {@link Exceptions#throwIfJvmFatal(Throwable)}).
	 * <p>
	 * This variant uses an internal private key, which allows the method to be additive with
	 * {@link #onHandleError(String, BiConsumer)}. Prefer adding and removing handler parts
	 * for keys that you own via {@link #onHandleError(String, BiConsumer)} nonetheless.
	 *
	 * @param subHook the new {@link BiConsumer} to set as the hook's anonymous part.
	 * @see #onHandleError(String, BiConsumer)
	 */
	public static void onHandleError(BiConsumer<Thread, ? super Throwable> subHook) {
		Objects.requireNonNull(subHook, "onHandleError");
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Hooking onHandleError anonymous part");
		}
		synchronized (LOGGER) {
			@SuppressWarnings("unchecked")
			BiConsumer<Thread, Throwable> _subHook = (BiConsumer<Thread, Throwable>) subHook;
			onHandleErrorHooks.put(Schedulers.class.getName() + ".ON_HANDLE_ERROR_ANONYMOUS_PART", _subHook);
			onHandleErrorHook = createOrAppendHandleError(onHandleErrorHooks.values());
		}
	}

	/**
	 * Define a keyed hook part that is executed alongside other parts when a {@link Scheduler} has
	 * {@link #handleError(Throwable) handled an error}. Note that it is executed after
	 * the error has been passed to the thread uncaughtErrorHandler, which is not the
	 * case when a fatal error occurs (see {@link Exceptions#throwIfJvmFatal(Throwable)}).
	 * <p>
	 * Calling this method twice with the same key replaces the old hook part
	 * of the same key. Calling this method twice with two different keys is otherwise additive.
	 * Note that {@link #onHandleError(BiConsumer)} also defines an anonymous part which
	 * effectively uses a private internal key, making it also additive with this method.
	 *
	 * @param key the {@link String} key identifying the hook part to set/replace.
	 * @param subHook the new hook part to set for the given key.
	 */
	public static void onHandleError(String key, BiConsumer<Thread, ? super Throwable> subHook) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(subHook, "onHandleError");
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Hooking onHandleError part with key {}", key);
		}
		synchronized (LOGGER) {
			@SuppressWarnings("unchecked")
			BiConsumer<Thread, Throwable> _subHook = (BiConsumer<Thread, Throwable>) subHook;
			onHandleErrorHooks.put(key, _subHook);
			onHandleErrorHook = createOrAppendHandleError(onHandleErrorHooks.values());
		}
	}

	@Nullable
	private static BiConsumer<Thread, ? super Throwable> createOrAppendHandleError(Collection<BiConsumer<Thread, Throwable>> subHooks) {
		BiConsumer<Thread, Throwable> composite = null;
		for (BiConsumer<Thread, Throwable> value : subHooks) {
			if (composite != null) {
				composite = composite.andThen(value);
			}
			else {
				composite = value;
			}
		}
		return composite;
	}

	/**
	 * Check if calling a Reactor blocking API in the current {@link Thread} is forbidden
	 * or not. This method returns {@code true} and will forbid the Reactor blocking API if
	 * any of the following conditions meet:
	 * <ul>
	 *   <li>the thread implements {@link NonBlocking}; or</li>
	 *   <li>any of the {@link Predicate}s registered via {@link #registerNonBlockingThreadPredicate(Predicate)}
	 *       returns {@code true}.</li>
	 * </ul>
	 *
	 * @return {@code true} if blocking is forbidden in this thread, {@code false} otherwise
	 */
	public static boolean isInNonBlockingThread() {
		return isNonBlockingThread(Thread.currentThread());
	}

	/**
	 * Check if calling a Reactor blocking API in the given {@link Thread} is forbidden
	 * or not. This method returns {@code true} and will forbid the Reactor blocking API if
	 * any of the following conditions meet:
	 * <ul>
	 *   <li>the thread implements {@link NonBlocking}; or</li>
	 *   <li>any of the {@link Predicate}s registered via {@link #registerNonBlockingThreadPredicate(Predicate)}
	 *       returns {@code true}.</li>
	 * </ul>
	 *
	 * @return {@code true} if blocking is forbidden in that thread, {@code false} otherwise
	 */
	public static boolean isNonBlockingThread(Thread t) {
		return t instanceof NonBlocking || nonBlockingThreadPredicate.test(t);
	}

	/**
	 * Registers the specified {@link Predicate} that determines whether it is forbidden to call
	 * a Reactor blocking API in a given {@link Thread} or not.
	 */
	public static void registerNonBlockingThreadPredicate(Predicate<Thread> predicate) {
		nonBlockingThreadPredicate = nonBlockingThreadPredicate.or(predicate);
	}

	/**
	 * Unregisters all the {@link Predicate}s registered so far via
	 * {@link #registerNonBlockingThreadPredicate(Predicate)}.
	 */
	public static void resetNonBlockingThreadPredicate() {
		nonBlockingThreadPredicate = DEFAULT_NON_BLOCKING_THREAD_PREDICATE;
	}

	/**
	 * If Micrometer is available, set-up a decorator that will instrument any
	 * {@link ExecutorService} that backs a {@link Scheduler}.
	 * No-op if Micrometer isn't available.
	 *
	 * <p>
	 * The {@link MeterRegistry} used by reactor can be configured via
	 * {@link reactor.util.Metrics.MicrometerConfiguration#useRegistry(MeterRegistry)}
	 * prior to using this method, the default being
	 * {@link io.micrometer.core.instrument.Metrics#globalRegistry}.
	 * </p>
	 *
	 * @implNote Note that this is added as a decorator via Schedulers when enabling metrics for schedulers, which doesn't change the Factory.
	 * @deprecated prefer using Micrometer#timedScheduler from the reactor-core-micrometer module. To be removed at the earliest in 3.6.0.
	 */
	@Deprecated
	public static void enableMetrics() {
		if (Metrics.isInstrumentationAvailable()) {
			addExecutorServiceDecorator(SchedulerMetricDecorator.METRICS_DECORATOR_KEY, new SchedulerMetricDecorator());
		}
	}

	/**
	 * If {@link #enableMetrics()} has been previously called, removes the decorator.
	 * No-op if {@link #enableMetrics()} hasn't been called.
	 *
	 * @deprecated prefer using Micrometer#timedScheduler from the reactor-core-micrometer module. To be removed at the earliest in 3.6.0.
	 */
 	@Deprecated
	public static void disableMetrics() {
		removeExecutorServiceDecorator(SchedulerMetricDecorator.METRICS_DECORATOR_KEY);
	}

	/**
	 * Re-apply default factory to {@link Schedulers}
	 */
	public static void resetFactory() {
		setFactory(DEFAULT);
	}

	/**
	 * Replace {@link Schedulers} factories ({@link #newParallel(String) newParallel},
	 * {@link #newSingle(String) newSingle} and {@link #newBoundedElastic(int, int, String) newBoundedElastic}).
	 * Unlike {@link #setFactory(Factory)}, doesn't shutdown previous Schedulers but
	 * capture them in a {@link Snapshot} that can be later restored via {@link #resetFrom(Snapshot)}.
	 * <p>
	 * This method should be called safely and with caution, typically on app startup.
	 *
	 * @param newFactory an arbitrary {@link Factory} instance
	 * @return a {@link Snapshot} representing a restorable snapshot of {@link Schedulers}
	 */
	public static Snapshot setFactoryWithSnapshot(Factory newFactory) {
		//nulling out CACHED references ensures that the schedulers won't be disposed
		//when setting the newFactory via setFactory
		Snapshot snapshot = new Snapshot(
				CACHED_BOUNDED_ELASTIC.getAndSet(null),
				CACHED_PARALLEL.getAndSet(null),
				CACHED_SINGLE.getAndSet(null),
				factory);
		setFactory(newFactory);
		return snapshot;
	}

	/**
	 * Replace the current Factory and shared Schedulers with the ones saved in a
	 * previously {@link #setFactoryWithSnapshot(Factory) captured} snapshot.
	 * <p>
	 * Passing {@code null} re-applies the default factory.
	 */
	public static void resetFrom(@Nullable Snapshot snapshot) {
		if (snapshot == null) {
			resetFactory();
			return;
		}
		//Restore the atomic references first, so that concurrent calls to Schedulers either
		//get a soon-to-be-shutdown instance or the restored instance
		CachedScheduler oldBoundedElastic = CACHED_BOUNDED_ELASTIC.getAndSet(snapshot.oldBoundedElasticScheduler);
		CachedScheduler oldParallel = CACHED_PARALLEL.getAndSet(snapshot.oldParallelScheduler);
		CachedScheduler oldSingle = CACHED_SINGLE.getAndSet(snapshot.oldSingleScheduler);

		//From there on, we've restored all the snapshoted instances, the factory can be
		//restored too and will start backing Schedulers.newXxx().
		//We thus never create a CachedScheduler by accident.
		factory = snapshot.oldFactory;

		//Shutdown the old CachedSchedulers, if any
		if (oldBoundedElastic != null) oldBoundedElastic._dispose();
		if (oldParallel != null) oldParallel._dispose();
		if (oldSingle != null) oldSingle._dispose();
	}

	/**
	 * Reset the {@link #onHandleError(BiConsumer)} hook to the default no-op behavior, erasing
	 * all sub-hooks that might have individually added via {@link #onHandleError(String, BiConsumer)}
	 * or the whole hook set via {@link #onHandleError(BiConsumer)}.
	 *
	 * @see #resetOnHandleError(String)
	 */
	public static void resetOnHandleError() {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Reset to factory defaults: onHandleError");
		}
		synchronized (LOGGER) {
			onHandleErrorHooks.clear();
			onHandleErrorHook = null;
		}
	}

	/**
	 * Reset a specific onHandleError hook part keyed to the provided {@link String},
	 * removing that sub-hook if it has previously been defined via {@link #onHandleError(String, BiConsumer)}.
	 */
	public static void resetOnHandleError(String key) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Remove onHandleError sub-hook {}", key);
		}
		synchronized (LOGGER) {
			//avoid resetting monolithic hook if no keyed hook has been set
			//also avoid resetting anything if the key is unknown
			if (onHandleErrorHooks.remove(key) != null) {
				onHandleErrorHook = createOrAppendHandleError(onHandleErrorHooks.values());
			}
		}
	}

	/**
	 * Replace {@link Schedulers} factories ({@link #newParallel(String) newParallel},
	 * {@link #newSingle(String) newSingle} and {@link #newBoundedElastic(int, int, String) newBoundedElastic}).
	 * Also shutdown Schedulers from the cached factories (like {@link #single()}) in order to
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
		CachedScheduler oldBoundedElastic = CACHED_BOUNDED_ELASTIC.getAndSet(null);
		CachedScheduler oldParallel = CACHED_PARALLEL.getAndSet(null);
		CachedScheduler oldSingle = CACHED_SINGLE.getAndSet(null);

		if (oldBoundedElastic != null) oldBoundedElastic._dispose();
		if (oldParallel != null) oldParallel._dispose();
		if (oldSingle != null) oldSingle._dispose();
	}

	/**
	 * The common <em>single</em> instance, a {@link Scheduler} that hosts a single-threaded ExecutorService-based
	 * worker.
	 * <p>
	 * Only one instance of this common scheduler will be created on the first call and is cached. The same instance
	 * is returned on subsequent calls until it is disposed.
	 * <p>
	 * One cannot directly {@link Scheduler#dispose() dispose} the common instances, as they are cached and shared
	 * between callers. They can however be all {@link #shutdownNow() shut down} together, or replaced by a
	 * {@link #setFactory(Factory) change in Factory}.
	 *
	 * @return the common <em>single</em> instance, a {@link Scheduler} that hosts a single-threaded
	 * ExecutorService-based worker
	 */
	public static Scheduler single() {
		return cache(CACHED_SINGLE, SINGLE, SINGLE_SUPPLIER);
	}

	/**
	 * Wraps a single {@link reactor.core.scheduler.Scheduler.Worker} from some other
	 * {@link Scheduler} and provides {@link reactor.core.scheduler.Scheduler.Worker}
	 * services on top of it. Unlike with other factory methods in this class, the delegate
	 * is assumed to be {@link Scheduler#init() initialized} and won't be implicitly
	 * initialized by this method.
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
		 * {@link Scheduler} that dynamically creates a bounded number of ExecutorService-based
		 * Workers, reusing them once the Workers have been shut down. The underlying (user or daemon)
		 * threads can be evicted if idle for more than {@code ttlSeconds}.
		 * <p>
		 * The maximum number of created thread pools is bounded by the provided {@code threadCap}.
		 *
		 * @param threadCap maximum number of underlying threads to create
		 * @param queuedTaskCap maximum number of tasks to enqueue when no more threads can be created. Can be {@link Integer#MAX_VALUE} for unbounded enqueueing.
		 * @param threadFactory a {@link ThreadFactory} to use each thread initialization
		 * @param ttlSeconds Time-to-live for an idle {@link reactor.core.scheduler.Scheduler.Worker}
		 *
		 * @return a new {@link Scheduler} that dynamically creates workers with an upper bound to
		 * the number of backing threads, reuses threads and evict idle ones
		 */
		default Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
			return new BoundedElasticScheduler(threadCap, queuedTaskCap, threadFactory, ttlSeconds);
		}

		/**
		 * {@link Scheduler} that dynamically creates a bounded number of Workers.
		 * <p>
		 * The maximum number of created thread pools is bounded by the provided {@code threadCap}.
		 * <p>
		 * The main difference between {@link BoundedElasticScheduler} and
		 * {@link BoundedElasticThreadPerTaskScheduler} is that underlying machinery
		 * allocates a new thread for every new task which is one of the requirements
		 * for usage with {@link VirtualThread}s
		 * <p>
		 * <b>Note:</b> for now this scheduler is available only in Java 21+ runtime
		 *
		 * @param threadCap maximum number of underlying threads to create
		 * @param queuedTaskCap maximum number of tasks to enqueue when no more threads can be created. Can be {@link Integer#MAX_VALUE} for unbounded enqueueing.
		 * @param threadFactory a {@link ThreadFactory} to use each thread initialization
		 *
		 * @since 3.6.0
		 *
		 * @return a new {@link Scheduler} that dynamically creates workers with an upper bound to
		 * the number of backing threads
		 */
		default Scheduler newThreadPerTaskBoundedElastic(int threadCap, int queuedTaskCap,	ThreadFactory threadFactory) {
			return new BoundedElasticThreadPerTaskScheduler(threadCap, queuedTaskCap, threadFactory);
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

	/**
	 * It is also {@link Disposable} in case you don't want to restore the live {@link Schedulers}
	 */
	public static final class Snapshot implements Disposable {

		@Nullable
		final CachedScheduler oldBoundedElasticScheduler;

		@Nullable
		final CachedScheduler oldParallelScheduler;

		@Nullable
		final CachedScheduler oldSingleScheduler;

		final Factory oldFactory;

		private Snapshot(@Nullable CachedScheduler oldBoundedElasticScheduler,
				@Nullable CachedScheduler oldParallelScheduler,
				@Nullable CachedScheduler oldSingleScheduler,
				Factory factory) {
			this.oldBoundedElasticScheduler = oldBoundedElasticScheduler;
			this.oldParallelScheduler = oldParallelScheduler;
			this.oldSingleScheduler = oldSingleScheduler;
			oldFactory = factory;
		}

		@Override
		public boolean isDisposed() {
			return
					(oldBoundedElasticScheduler == null || oldBoundedElasticScheduler.isDisposed()) &&
					(oldParallelScheduler == null || oldParallelScheduler.isDisposed()) &&
					(oldSingleScheduler == null || oldSingleScheduler.isDisposed());
		}

		@Override
		public void dispose() {
			if (oldBoundedElasticScheduler != null) oldBoundedElasticScheduler._dispose();
			if (oldParallelScheduler != null) oldParallelScheduler._dispose();
			if (oldSingleScheduler != null) oldSingleScheduler._dispose();
		}
	}

	// Internals
	static final String BOUNDED_ELASTIC       = "boundedElastic"; // Blocking stuff with scale to zero
	static final String LOOM_BOUNDED_ELASTIC  = "loomBoundedElastic"; // Loom stuff
	static final String PARALLEL              = "parallel"; //scale up common tasks
	static final String SINGLE                = "single"; //non blocking tasks
	static final String IMMEDIATE             = "immediate";
	static final String FROM_EXECUTOR         = "fromExecutor";
	static final String FROM_EXECUTOR_SERVICE = "fromExecutorService";


	// Cached schedulers in atomic references:
	static AtomicReference<CachedScheduler> CACHED_BOUNDED_ELASTIC = new AtomicReference<>();
	static AtomicReference<CachedScheduler> CACHED_PARALLEL        = new AtomicReference<>();
	static AtomicReference<CachedScheduler> CACHED_SINGLE          = new AtomicReference<>();

	static final Supplier<Scheduler> BOUNDED_ELASTIC_SUPPLIER = new BoundedElasticSchedulerSupplier();

	static final Supplier<Scheduler> PARALLEL_SUPPLIER =
			() -> newParallel(PARALLEL, DEFAULT_POOL_SIZE, true);

	static final Supplier<Scheduler> SINGLE_SUPPLIER = () -> newSingle(SINGLE, true);

	static final Factory DEFAULT = new Factory() { };

	static final Map<String, BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService>>
			DECORATORS = new LinkedHashMap<>();

	static volatile Factory factory = DEFAULT;

	private static final LinkedHashMap<String, BiConsumer<Thread, Throwable>> onHandleErrorHooks = new LinkedHashMap<>(1);

	@Nullable
	static BiConsumer<Thread, ? super Throwable> onHandleErrorHook;

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

	static final Logger LOGGER = Loggers.getLogger(Schedulers.class);

	static final void defaultUncaughtException(Thread t, Throwable e) {
		Schedulers.LOGGER.error("Scheduler worker in group " + t.getThreadGroup().getName()
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
			LOGGER.error("Scheduler worker failed with an uncaught exception", t);
		}
		BiConsumer<Thread, ? super Throwable> hook = onHandleErrorHook;
		if (hook != null) {
			hook.accept(thread, t);
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
		@SuppressWarnings("deprecation")
		public void start() {
			cached.start();
		}

		@Override
		public void init() {
			cached.init();
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
