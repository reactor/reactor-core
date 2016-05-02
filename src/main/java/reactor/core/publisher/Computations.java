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

package reactor.core.publisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Cancellation;
import reactor.core.flow.Loopback;
import reactor.core.flow.MultiProducer;
import reactor.core.scheduler.Scheduler;
import reactor.core.state.Completable;
import reactor.core.state.Introspectable;
import reactor.core.util.Exceptions;
import reactor.core.util.Logger;
import reactor.core.util.PlatformDependent;
import reactor.core.util.WaitStrategy;

/**
 * {@link Computations} provide event-loop based {@link Scheduler} useable by
 * {@link Flux#publishOn publishOn} or {@link Mono#subscribeOn subscribeOn}.
 * <p>
 * Based on this scheduling contract, a
 * {@link Computations} offers a {@link reactor.core.scheduler.Scheduler.Worker} mutualized pool and will round-robin
 * assignation via {@link #createWorker()}.
 *  {@link Computations} also maintains a reference count on how many worker have been generated. Therefore it will
 * automatically shutdown the required resources after all references have been released, e.g. when all {@link Flux}
 * using
 * {@link Flux#publishOn publishOn} have been cancelled, completed or errored. The shutdown can also be {@link Computations#shutdown manual}
 * by setting the factories {@literal autoshutdown} to false.
 * <p>
 *   {@link Computations} offers ready-to-use pool configurations :
 *    <ul>
 *        <li>{@link #parallel} : Optimized for fast {@link Runnable} executions </li>
 *        <li>{@link #concurrent} : Optimized for slow {@link Runnable} executions </li>
 *        <li>{@link #single} : Optimized for low-latency {@link Runnable} executions </li>
 *        <li>{@link #create create} : Arbitrary group creation. </li>
 *    </ul>
 *
 * <p>
 * 
 * @author Stephane Maldini
 */
public final class Computations implements Scheduler, MultiProducer, Completable {

	static final Logger log = Logger.getLogger(Computations.class);

	/**
	 * Default number of processors available to the runtime on init (min 4)
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE = Math.max(Runtime.getRuntime()
	                                                            .availableProcessors(), 4);

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@link #DEFAULT_POOL_SIZE} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link #createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @return a new {@link Computations} tuned for fast tasks
	 */
	public static Scheduler parallel() {
		return parallel("parallel", PlatformDependent.MEDIUM_BUFFER_SIZE,
				DEFAULT_POOL_SIZE, true);
	}

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@link #DEFAULT_POOL_SIZE} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link #createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 *
	 * @return a new {@link Computations} tuned for fast tasks
	 */
	public static Scheduler parallel(String name) {
		return parallel(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@link #DEFAULT_POOL_SIZE} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link #createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 *
	 * @return a new {@link Computations} tuned for fast tasks
	 */
	public static Scheduler parallel(String name, int bufferSize) {
		return parallel(name, bufferSize, DEFAULT_POOL_SIZE);
	}

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@literal parallelSchedulers} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link #createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link TopicProcessor}
	 *
	 * @return a new {@link Computations} tuned for fast tasks
	 */
	public static Scheduler parallel(String name, int bufferSize, int parallelSchedulers) {
		return parallel(name, bufferSize, parallelSchedulers, null, null, false);
	}

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@literal parallelSchedulers} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link #createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link TopicProcessor}
	 * @param autoShutdown true if this {@link Computations} should automatically shutdown its resources
	 *
	 * @return a new {@link Computations} tuned for fast tasks
	 */
	public static Scheduler parallel(String name,
			int bufferSize,
			int parallelSchedulers,
			boolean autoShutdown) {
		return parallel(name, bufferSize, parallelSchedulers, null, null, autoShutdown);
	}

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@literal parallelSchedulers} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link #createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link TopicProcessor}
	 * @param uncaughtExceptionHandler Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownHandler Callback signalled when a {@link Subscriber} thread terminates
	 *
	 * @return a new {@link Computations} tuned for fast tasks
	 */
	public static Scheduler parallel(String name,
			int bufferSize,
			int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler) {
		return parallel(name, bufferSize, parallelSchedulers, uncaughtExceptionHandler, shutdownHandler, false);
	}

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@literal parallelSchedulers} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link #createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link TopicProcessor}
	 * @param uncaughtExceptionHandler Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownHandler Callback signalled when a {@link Subscriber} thread terminates
	 * @param autoShutdown true if this {@link Computations} should automatically shutdown its resources
	 *
	 * @return a new {@link Computations} tuned for fast tasks
	 */
	public static Scheduler parallel(final String name,
			final int bufferSize,
			int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {

		return parallel(name, bufferSize, parallelSchedulers, uncaughtExceptionHandler, shutdownHandler, autoShutdown, DEFAULT_WAIT_STRATEGY);
	}

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@literal parallelSchedulers} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link #createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link TopicProcessor}
	 * @param uncaughtExceptionHandler Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownHandler Callback signalled when a {@link Subscriber} thread terminates
	 * @param autoShutdown true if this {@link Computations} should automatically shutdown its resources
	 * @param waitStrategy a {@link WaitStrategy} {@link Supplier} to trade-off cpu use for task consumer latency
	 *
	 * @return a new {@link Computations} tuned for fast tasks
	 */
	public static Scheduler parallel(final String name,
			final int bufferSize,
			final int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown,
			final Supplier<? extends WaitStrategy> waitStrategy) {

		return create(new Supplier<Processor<Runnable, Runnable>>() {
			int i = 1;
			@Override
			public Processor<Runnable, Runnable> get() {
				return TopicProcessor.share(name+(parallelSchedulers > 1 ? "-"+(i++) : ""), bufferSize, waitStrategy
						.get(), false);
			}
		}, parallelSchedulers, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}


	/**
	 * Create a {@link Computations} pool of N {@literal parallelSchedulers} size calling the passed worker
	 * factory {@link Scheduler#createWorker()} once each.
	 * <p>
	 * It provides for reference counting when the containing {@link Computations} is used as a worker factory
	 * itself.
	 * If reference count returns to 0 it will automatically createWorker
	 * {@link Scheduler.Scheduler.Worker#shutdown()}.
	 * <p>
	 * Note: If the schedulerFactory generates a {@link Processor} it will be subscribed once.
	 *
	 * @param processor
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link Runnable} {@link Subscriber}
	 *
	 * @return a new {@link Computations}
	 */
	public static Scheduler create(Processor<Runnable, Runnable> processor, int
			parallelSchedulers) {
		return create(() -> processor, parallelSchedulers, false);
	}

	/**
	 *
	 * Create a {@link Computations} pool of N {@literal parallelSchedulers} size calling the {@link Processor}
	 * {@link Supplier}
	 * once each.
	 * <p>
	 * It provides for reference counting on {@link Computations#createWorker()} and {@link Scheduler.Worker#shutdown()}
	 * If autoShutdown is given true and reference count returns to 0 it will automatically call
	 * {@link Scheduler#shutdown()} which will invoke {@link Processor#onComplete()}.
	 * <p>
	 *
	 * @param processors
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link Runnable} {@link Subscriber}
	 * @param autoShutdown true if this {@link Computations} should automatically shutdown its resources

	 * @return a new {@link Computations}
	 */
	public static Scheduler create(Supplier<? extends Processor<Runnable, Runnable>> processors,
			int parallelSchedulers,
			boolean autoShutdown) {
		return create(processors, parallelSchedulers, null, null, autoShutdown);
	}

	/**
	 * Create a {@link Computations} pool of N {@literal parallelSchedulers} size calling the {@link Processor} {@link
	 * Supplier} once each.
	 * <p>
	 * It provides for reference counting on {@link Computations#createWorker()} and {@link Scheduler.Scheduler.Worker#shutdown()} If
	 * autoShutdown is given true and reference count returns to 0 it will automatically call {@link
	 * Scheduler#shutdown()} which will invoke {@link Processor#onComplete()}.
	 * <p>
	 *
	 * @param processors
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal {@link Runnable}
	 * {@link Subscriber}
	 * @param uncaughtExceptionHandler Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownHandler Callback signalled when a {@link Subscriber} thread terminates
	 * @param autoShutdown true if this {@link Computations} should automatically shutdown its resources
	 *
	 * @return a new {@link Computations}
	 */
	public static Scheduler create(Supplier<? extends Processor<Runnable, Runnable>> processors,
			int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		return new Computations(processors,
				parallelSchedulers,
				autoShutdown,
				uncaughtExceptionHandler,
				shutdownHandler);
	}

	/**
	 * An IO factory is  a worker factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http createWorker, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@link Computations#DEFAULT_POOL_SIZE} subscribers that will
	 * compete to execute the
	 * {@link Runnable} tasks. The task backlog will be relatively large {@link PlatformDependent#MEDIUM_BUFFER_SIZE}
	 * to mitigate consuming rate difference.
	 *
	 *
	 * @return a new {@link Computations} tuned for slow tasks
	 */
	public static Scheduler concurrent() {
		return concurrent("concurrent", PlatformDependent.MEDIUM_BUFFER_SIZE, DEFAULT_POOL_SIZE, true);
	}

	/**
	 * An IO factory is  a worker factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http createWorker, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@link Computations#DEFAULT_POOL_SIZE} subscribers that will
	 * compete to execute the
	 * {@link Runnable} tasks. The task backlog will be relatively large {@link PlatformDependent#MEDIUM_BUFFER_SIZE}
	 * to mitigate consuming rate difference.
	 *
	 * @param name Group name derived for thread identification
	 *
	 * @return a new {@link Computations} tuned for slow tasks
	 */
	public static Scheduler concurrent(String name) {
		return concurrent(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * An IO factory is  a worker factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http createWorker, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@link Computations#DEFAULT_POOL_SIZE} subscribers that will
	 * compete to execute the
	 * {@link Runnable} tasks. The task backlog will should be relatively large given {@literal bufferSize} to 
	 * mitigate consuming rate difference.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize Task backlog size, risk-off more memory for lower producer latency
	 *
	 * @return a new {@link Computations} tuned for slow tasks
	 */
	public static Scheduler concurrent(String name, int bufferSize) {
		return concurrent(name, bufferSize, DEFAULT_POOL_SIZE);
	}

	/**
	 * An IO factory is  a worker factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http createWorker, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@literal concurrency} number of subscribers that will
	 * compete to execute the  
	 * {@link Runnable} tasks. The task backlog will should be relatively large given {@literal bufferSize} to 
	 * mitigate consuming rate difference.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize Task backlog size, risk-off more memory for lower producer latency
	 * @param concurrency Parallel workers to subscribe to the internal {@link WorkQueueProcessor}
	 *
	 * @return a new {@link Computations} tuned for slow tasks
	 */
	public static Scheduler concurrent(String name, int bufferSize, int concurrency) {
		return concurrent(name, bufferSize, concurrency, null, null, false);
	}

	/**
	 * An IO factory is  a worker factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http createWorker, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@literal concurrency} number of subscribers that will
	 * compete to execute the
	 * {@link Runnable} tasks. The task backlog will should be relatively large given {@literal bufferSize} to
	 * mitigate consuming rate difference.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize Task backlog size, risk-off more memory for lower producer latency
	 * @param concurrency Parallel workers to subscribe to the internal {@link WorkQueueProcessor}
	 * @param autoShutdown true if this {@link Computations} should automatically shutdown its resources
	 *
	 * @return a new {@link Computations} tuned for slow tasks
	 */
	public static Scheduler concurrent(String name, int bufferSize, int concurrency, boolean autoShutdown) {
		return concurrent(name, bufferSize, concurrency, null, null, autoShutdown);
	}

	/**
	 * An IO factory is  a worker factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http createWorker, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@literal concurrency} number of subscribers that will
	 * compete to execute the
	 * {@link Runnable} tasks. The task backlog will should be relatively large given {@literal bufferSize} to
	 * mitigate consuming rate difference.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize Task backlog size, risk-off more memory for lower producer latency
	 * @param concurrency Parallel workers to subscribe to the internal {@link WorkQueueProcessor}
	 * @param uncaughtExceptionHandler Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownHandler Callback signalled when a {@link Subscriber} thread terminates
	 *
	 * @return a new {@link Computations} tuned for slow tasks
	 */
	public static Scheduler concurrent(String name, int bufferSize,
			int concurrency, Consumer<Throwable> uncaughtExceptionHandler, Runnable shutdownHandler) {
		return concurrent(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, false);
	}

	/**
	 * An IO factory is  a worker factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http createWorker, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@literal concurrency} number of subscribers that 
	 * will
	 * compete to execute the
	 * {@link Runnable} tasks. The task backlog will should be relatively large given {@literal bufferSize} to 
	 * mitigate consuming rate difference.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize Task backlog size, risk-off more memory for lower producer latency
	 * @param concurrency Parallel workers to subscribe to the internal {@link WorkQueueProcessor}
	 * @param uncaughtExceptionHandler Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownHandler Callback signalled when a {@link Subscriber} thread terminates
	 * @param autoShutdown true if this {@link Computations} should automatically shutdown its resources
	 *
	 * @return a new {@link Computations} tuned for slow tasks
	 */
	public static Scheduler concurrent(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler, Runnable shutdownHandler, boolean autoShutdown) {
		return concurrent(name,
				bufferSize,
				concurrency, uncaughtExceptionHandler, shutdownHandler,
				autoShutdown, DEFAULT_WAIT_STRATEGY.get());
	}

	/**
	 * An IO factory is  a worker factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http createWorker, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@link Computations#DEFAULT_POOL_SIZE} subscribers that will
	 * compete to execute the
	 * {@link Runnable} tasks. The task backlog will should be relatively large given {@literal bufferSize} to 
	 * mitigate consuming rate difference.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize Task backlog size, risk-off more memory for lower producer latency
	 * @param concurrency Parallel workers to subscribe to the internal {@link WorkQueueProcessor}
	 * @param uncaughtExceptionHandler Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownHandler Callback signalled when a {@link Subscriber} thread terminates
	 * @param autoShutdown true if this {@link Computations} should automatically shutdown its resources
	 * @param waitStrategy a {@link WaitStrategy} to trade-off cpu use for task consumer latency
	 *
	 * @return a new {@link Computations} tuned for slow tasks
	 */
	public static Scheduler concurrent(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown,
			WaitStrategy waitStrategy) {

		Processor<Runnable, Runnable> p = WorkQueueProcessor.share(name, bufferSize, waitStrategy, false);
		return new Computations(() -> p, concurrency, autoShutdown, uncaughtExceptionHandler, shutdownHandler);
	}

	/**
	 * A Single factory is  a worker factory with sensible defaults for for "ultra-fast" and low-latency consuming.
	 *
	 * <p>
	 * It uses a single
	 * {@link TopicProcessor} subscribed once by a subscriber executing its partition of {@link Runnable} tasks.
	 * Due to its single-backlog/single-thread design, sensitivity to task execution time difference will not be
	 * mitigated.
	 *
	 * @return a new {@link Computations} tuned for low latency tasks
	 */
	public static Scheduler single() {
		return single("single", PlatformDependent.MEDIUM_BUFFER_SIZE, true);
	}

	/**
	 * A Single factory is  a worker factory with sensible defaults for for "ultra-fast" and low-latency consuming.
	 *
	 * <p>
	 * It uses a single
	 * {@link TopicProcessor} subscribed once by a subscriber executing its partition of {@link Runnable} tasks.
	 * Due to its single-backlog/single-thread design, sensitivity to task execution time difference will not be
	 * mitigated.
	 *
	 * @param name Group name derived for thread identification
	 *
	 * @return a new {@link Computations} tuned for low latency tasks
	 */
	public static Scheduler single(String name) {
		return single(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * A Single factory is  a worker factory with sensible defaults for for "ultra-fast" and low-latency consuming.
	 *
	 * <p>
	 * It uses a single
	 * {@link TopicProcessor} subscribed once by a subscriber executing its partition of {@link Runnable} tasks.
	 * Due to its single-backlog/single-thread design, sensitivity to task execution time difference will not be
	 * mitigated.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 *
	 * @return a new {@link Computations} tuned for low latency tasks
	 */
	public static Scheduler single(String name, int bufferSize) {
		return single(name, bufferSize, null, null, false, SINGLE_WAIT_STRATEGY);
	}

	/**
	 * A Single factory is  a worker factory with sensible defaults for for "ultra-fast" and low-latency consuming.
	 *
	 * <p>
	 * It uses a single
	 * {@link TopicProcessor} subscribed once by a subscriber executing its partition of {@link Runnable} tasks.
	 * Due to its single-backlog/single-thread design, sensitivity to task execution time difference will not be
	 * mitigated.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param autoShutdown true if this {@link Computations} should automatically shutdown its resources
	 *
	 * @return a new {@link Computations} tuned for low latency tasks
	 */
	public static Scheduler single(String name, int bufferSize, boolean autoShutdown) {
		return single(name, bufferSize, null, null, autoShutdown, SINGLE_WAIT_STRATEGY);
	}

	/**
	 * A Single factory is  a worker factory with sensible defaults for for "ultra-fast" and low-latency consuming.
	 *
	 * <p>
	 * It uses a single
	 * {@link TopicProcessor} subscribed once by a subscriber executing its partition of {@link Runnable} tasks.
	 * Due to its single-backlog/single-thread design, sensitivity to task execution time difference will not be
	 * mitigated.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param waitStrategy a {@link WaitStrategy} to trade-off cpu use for task consumer latency
	 *
	 * @return a new {@link Computations} tuned for low latency tasks
	 */
	public static Scheduler single(String name, int bufferSize, WaitStrategy waitStrategy) {
		return single(name, bufferSize, null, null, false, () -> waitStrategy);
	}

	/**
	 * A Single factory is  a worker factory with sensible defaults for for "ultra-fast" and low-latency consuming.
	 *
	 * <p>
	 * It uses a single
	 * {@link TopicProcessor} subscribed once by a subscriber executing its partition of {@link Runnable} tasks.
	 * Due to its single-backlog/single-thread design, sensitivity to task execution time difference will not be
	 * mitigated.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param errorC Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownC Callback signalled when a {@link Subscriber} thread terminates
	 *
	 * @return a new {@link Computations} tuned for low latency tasks
	 */
	public static Scheduler single(String name, int bufferSize, Consumer<Throwable> errorC,
			Runnable shutdownC) {
		return single(name, bufferSize, errorC, shutdownC, false, SINGLE_WAIT_STRATEGY);
	}

	/**
	 * A Single factory is  a worker factory with sensible defaults for for "ultra-fast" and low-latency consuming.
	 *
	 * <p>
	 * It uses a single
	 * {@link TopicProcessor} subscribed once by a subscriber executing its partition of {@link Runnable} tasks.
	 * Due to its single-backlog/single-thread design, sensitivity to task execution time difference will not be
	 * mitigated.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param errorC Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownC Callback signalled when a {@link Subscriber} thread terminates
	 * @param autoShutdown true if this {@link Computations} should automatically shutdown its resources
	 * @param waitStrategy a {@link WaitStrategy} {@link Supplier} to trade-off cpu use for task consumer latency
	 *
	 * @return a new {@link Computations} tuned for low latency tasks
	 */
	public static Scheduler single(String name, int bufferSize,
			Consumer<Throwable> errorC,
			Runnable shutdownC,  boolean autoShutdown, Supplier<? extends WaitStrategy> waitStrategy) {
		return parallel(name, bufferSize, 1, errorC, shutdownC, autoShutdown, waitStrategy);
	}

	/**
	 * Creates an arbitrary single {@link Computations} wrapper around a given {@link Processor} of {@link Runnable}.
	 * Provides for reference counting when the containing {@link Computations} is supplied as a factory.
	 * When reference count returns to 0 it will automatically createWorker {@link Processor#onComplete()}.
	 * <p>
	 * It will be subscribed once.
	 *
	 * @param processor the {@link Processor} to decorate
	 * @return a new {@link Computations}
	 */
	public static Scheduler single(Processor<Runnable, Runnable> processor) {
		return single(processor, false);
	}

	/**
	 * Creates an arbitrary single {@link Computations} wrapper around a given {@link Processor} of {@link Runnable}.
	 * Provides for reference counting when the containing {@link Computations} is supplied as a factory.
	 * If autoShutdown is given true and reference count returns to 0 it will automatically createWorker {@link Processor#onComplete()}.
	 * <p>
	 * It will be subscribed once.
	 *
	 * @param processor the {@link Processor} to decorate
	 * @param autoShutdown true to automatically shutdown the inner worker
	 *
	 * @return a new {@link Computations}
	 */
	public static Scheduler single(final Processor<Runnable, Runnable> processor, boolean autoShutdown) {
		return create(() -> processor, 1, autoShutdown);
	}

	/**
	 * Blocking shutdown of the internal {@link EventLoopProcessor} with {@link Processor#onComplete()}. If the
	 * processor doesn't implement.
	 *
	 * The method will only return after shutdown has been confirmed, waiting undefinitely so.
	 *
	 * @return true if successfully shutdown
	 */
	public final boolean awaitAndShutdown() {
		return awaitAndShutdown(-1, TimeUnit.SECONDS);
	}

	/**
	 * Blocking shutdown of the internal {@link EventLoopProcessor} with {@link Processor#onComplete()}. If the
	 * processor doesn't implement
	 * {@link EventLoopProcessor} or if it is synchronous, throw an {@link UnsupportedOperationException}.
	 * @param timeout the time un given unit to wait for
	 * @param timeUnit the unit
	 *
	 * @return true if successfully shutdown
	 */
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		for (ProcessorWorker processorWorker : workerPool) {
			if (processorWorker.processor instanceof EventLoopProcessor && !((EventLoopProcessor) processorWorker.processor).awaitAndShutdown(timeout,
					timeUnit)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Return a worker reference to this {@link Computations}, incrementing use count by 1
	 *
	 * @return a new {@link reactor.core.scheduler.Scheduler.Worker} reference
	 */
	@Override
	public Worker createWorker() {
		references.incrementAndGet();
		return next();
	}

	@Override
	public Iterator<?> downstreams() {
		return Arrays.asList(workerPool)
		             .iterator();
	}

	@Override
	public long downstreamCount() {
		return workerPool.length;
	}

	/**
	 * Non-blocking forced shutdown of the internal {@link Processor} with {@link Processor#onComplete()}
	 * @return the current pending tasks if supported or throw an unchecked {@link UnsupportedOperationException}.
	 */
	@SuppressWarnings("unchecked")
	public Flux<Runnable> forceShutdown() {
		List<Flux<Runnable>> finish = new ArrayList<>(workerPool.length);
		for (ProcessorWorker worker : workerPool) {
			if (worker.processor instanceof EventLoopProcessor) {
				finish.add(((EventLoopProcessor<Runnable, Runnable>) worker.processor).forceShutdown());
			}
			else {
				throw new UnsupportedOperationException(
						"Underlying Processor is null or doesn't implement EventLoopProcessor");
			}
		}
		return Flux.merge(finish);

	}

	@Override
	public boolean isTerminated() {
		for (ProcessorWorker processorWorker : workerPool) {
			if (!(processorWorker.processor instanceof EventLoopProcessor && ((EventLoopProcessor) processorWorker.processor).isTerminated())) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean isStarted() {
		for (ProcessorWorker processorWorker : workerPool) {
			if (!(processorWorker.processor instanceof EventLoopProcessor && ((EventLoopProcessor) processorWorker.processor).isStarted())) {
				return false;
			}
		}
		return true;
	}

	@Override
	public Cancellation schedule(Runnable task) {
		next().processor.onNext(task);
		return NOOP_CANCEL;
	}

	@Override
	public void shutdown() {
		for (ProcessorWorker processorWorker : workerPool) {
			if (processorWorker.processor instanceof EventLoopProcessor) {
				((EventLoopProcessor) processorWorker.processor).shutdown();
			}
			else {
				processorWorker.processor.onComplete();
			}
		}
	}

	ProcessorWorker next() {
		int size = workerPool.length;
		if (size == 1) {
			return workerPool[0];
		}

		int index;
		for (; ; ) {
			index = this.index;
			if (index == Integer.MAX_VALUE) {
				if (INDEX.compareAndSet(this, Integer.MAX_VALUE, 0)) {
					index = 0;
					break;
				}
				continue;
			}

			if (INDEX.compareAndSet(this, index, index + 1)) {
				break;
			}
		}

		return workerPool[index % size];
	}

	/* INTERNAL */

	static final Supplier<? extends WaitStrategy> DEFAULT_WAIT_STRATEGY =
			() -> WaitStrategy.phasedOffLiteLock(200, 200, TimeUnit.MILLISECONDS);

	static final Supplier<? extends WaitStrategy> SINGLE_WAIT_STRATEGY =
			() -> WaitStrategy.phasedOffLiteLock(500, 50, TimeUnit.MILLISECONDS);

	static final AtomicIntegerFieldUpdater<Computations> INDEX =
			AtomicIntegerFieldUpdater.newUpdater(Computations.class, "index");

	final ProcessorWorker[] workerPool;
	final AtomicInteger references = new AtomicInteger(0);

	volatile int index = 0;

	@SuppressWarnings("unchecked")
	protected Computations(Supplier<? extends Processor<Runnable, Runnable>> processorSupplier,
			int parallelSchedulers,
			boolean autoShutdown,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler) {

		if (parallelSchedulers < 1) {
			throw new IllegalArgumentException("Cannot create group pools from null or negative parallel argument");
		}

		this.workerPool = new ProcessorWorker[parallelSchedulers];

		for (int i = 0; i < parallelSchedulers; i++) {
			workerPool[i] = new ProcessorWorker(processorSupplier.get(),
					autoShutdown,
					uncaughtExceptionHandler,
					shutdownHandler,
					references);
			workerPool[i].start();
		}
	}

	final static Cancellation NOOP_CANCEL = () -> {
	};

	final static class ProcessorWorker implements Subscriber<Runnable>, Loopback, Worker, Introspectable {

		final Consumer<Throwable>           uncaughtExceptionHandler;
		final Runnable                      shutdownHandler;
		final Processor<Runnable, Runnable> processor;
		final boolean                       autoShutdown;
		final AtomicInteger                 references;

		LinkedArrayNode head;
		LinkedArrayNode tail;
		boolean         running;

		Thread thread;

		ProcessorWorker(final Processor<Runnable, Runnable> processor,
				boolean autoShutdown,
				Consumer<Throwable> uncaughtExceptionHandler,
				Runnable shutdownHandler,
				AtomicInteger references) {
			this.processor = processor;
			this.autoShutdown = autoShutdown;
			this.uncaughtExceptionHandler = uncaughtExceptionHandler;
			this.shutdownHandler = shutdownHandler;
			this.references = references;
		}

		@Override
		public void onSubscribe(Subscription s) {
			thread = Thread.currentThread();
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Runnable task) {
			try {
				running = true;
				task.run();

				//tail recurse
				LinkedArrayNode n = head;

				while ( n != null ){
					for(int i = 0; i < n.count; i++){
						n.array[i].run();
					}
					n = n.next;
				}

				head = null;
				tail = null;

				running = false;
			}
			catch (Exceptions.CancelException ce) {
				//IGNORE
			}
			catch (Throwable t) {
				routeError(t);
			}
		}

		@Override
		public Cancellation schedule(Runnable task) {
			try {
				if (Thread.currentThread() == thread && running) {
					tail(task);
					return NOOP_CANCEL;
				}

				processor.onNext(task);
				return NOOP_CANCEL;
			}
			catch (Exceptions.CancelException ce) {
				//IGNORE
			}
			catch (Throwable t) {
				if (processor != null) {
					processor.onError(t);
				}
				else if (uncaughtExceptionHandler != null) {
					uncaughtExceptionHandler.accept(t);
				}
			} return REJECTED;
		}

		@Override
		public void shutdown() {
			if (references.decrementAndGet() <= 0 && autoShutdown) {
				processor.onComplete();
			}
		}

		void start() {
			processor.subscribe(this);
		}

		void tail(Runnable task) {
			LinkedArrayNode t = tail;

			if (t == null) {
				t = new LinkedArrayNode(task);

				head = t;
				tail = t;
			}
			else {
				if (t.count == LinkedArrayNode.DEFAULT_CAPACITY) {
					LinkedArrayNode n = new LinkedArrayNode(task);

					t.next = n;
					tail = n;
				}
				else {
					t.array[t.count++] = task;
				}
			}
		}

		void routeError(Throwable t) {
			if (uncaughtExceptionHandler != null) {
				uncaughtExceptionHandler.accept(t);
			}
			else {
				log.error("Unrouted exception", t);
			}
		}

		@Override
		public Object connectedInput() {
			return processor;
		}

		@Override
		public Object connectedOutput() {
			return processor;
		}

		@Override
		public int getMode() {
			return INNER;
		}

		@Override
		public void onError(Throwable t) {
			thread = null;
			if (uncaughtExceptionHandler != null) {
				uncaughtExceptionHandler.accept(t);
			}
			Exceptions.throwIfFatal(t);

			//TODO support resubscribe ?
			throw new UnsupportedOperationException("No error handler provided for this Computations", t);
		}

		@Override
		public void onComplete() {
			thread = null;
			if (shutdownHandler != null) {
				shutdownHandler.run();
			}
		}
	}

	/**
	 * Node in a linked array list that is only appended.
	 */
	static final class LinkedArrayNode {

		static final int DEFAULT_CAPACITY = 16;

		final Runnable[] array;
		int count;

		LinkedArrayNode next;

		@SuppressWarnings("unchecked")
		LinkedArrayNode(Runnable value) {
			array = new Runnable[DEFAULT_CAPACITY];
			array[0] = value;
			count = 1;
		}
	}
}