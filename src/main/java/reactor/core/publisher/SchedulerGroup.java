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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Loopback;
import reactor.core.flow.MultiProducer;
import reactor.core.state.Completable;
import reactor.core.state.Introspectable;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.Logger;
import reactor.core.util.PlatformDependent;
import reactor.core.util.WaitStrategy;
import reactor.fn.Consumer;
import reactor.fn.Supplier;

/**
 * "Scheduling" in Reactor via
 *
 * {@link Flux#dispatchOn dispatchOn}, {@link Mono#publishOn publishOn} or {@link FluxProcessor}.{@link FluxProcessor#async async} requires
 * {@link Consumer} of {@link Runnable}. Unlike {@link java.util.concurrent.Executor} which apparently has the same
 * signature, these {@link Consumer} allow {@literal null} argument and should treat them as terminal signal to dispose
 * any used resources.
 * <p>
 * Based on this scheduling contract, a
 * {@link SchedulerGroup} offers a scheduler generator pool mutualizing one or more internal scheduler {@link Callable}
 * generator.
 *  {@link SchedulerGroup} maintains a reference count on how many scheduler have been generated. Therefore it will
 * automatically shutdown the required resources after all references have been released, e.g. when all {@link Flux}
 * using
 * {@link Flux#dispatchOn dispatchOn} have been cancelled, completed or errored. The shutdown can also be {@link SchedulerGroup#shutdown manual}
 * by setting the factories {@literal autoshutdown} to false.
 * <p>
 *   {@link SchedulerGroup} offers ready-to-use pool configurations :
 *    <ul>
 *        <li>{@link #async} : Optimized for fast {@link Runnable} executions </li>
 *        <li>{@link #io} : Optimized for slow {@link Runnable} executions </li>
 *        <li>{@link #single} : Optimized for low-latency {@link Runnable} executions </li>
 *        <li>{@link #create create} : Arbitrary group creation. </li>
 *    </ul>
 *
 * <p>
 * By default the {@link SchedulerGroup} are not guaranteed reentrant and such support is obtained via
 * {@link SchedulerGroup#call(boolean)} or {@link FluxProcessor#async(SchedulerGroup)}.
 * 
 * @author Stephane Maldini
 */
public class SchedulerGroup implements Callable<Consumer<Runnable>>, Consumer<Runnable>, Loopback,
                                       Completable {

	static final Logger log = Logger.getLogger(SchedulerGroup.class);

	/**
	 * Default number of processors available to the runtime on init (min 4)
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE = Math.max(Runtime.getRuntime()
	                                                            .availableProcessors(), 4);

	/**
	 * An Async factory is  a scheduler factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@link #DEFAULT_POOL_SIZE} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each scheduler generation {@link #call} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing scheduler request from dynamic
	 * flows.
	 *
	 * @return a new {@link SchedulerGroup} tuned for fast tasks
	 */
	public static SchedulerGroup async() {
		return async("async", PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * An Async factory is  a scheduler factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@link #DEFAULT_POOL_SIZE} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each scheduler generation {@link #call} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing scheduler request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 *
	 * @return a new {@link SchedulerGroup} tuned for fast tasks
	 */
	public static SchedulerGroup async(String name) {
		return async(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * An Async factory is  a scheduler factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@link #DEFAULT_POOL_SIZE} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each scheduler generation {@link #call} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing scheduler request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 *
	 * @return a new {@link SchedulerGroup} tuned for fast tasks
	 */
	public static SchedulerGroup async(String name, int bufferSize) {
		return async(name, bufferSize, DEFAULT_POOL_SIZE);
	}

	/**
	 * An Async factory is  a scheduler factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@literal parallelSchedulers} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each scheduler generation {@link #call} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing scheduler request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link TopicProcessor}
	 *
	 * @return a new {@link SchedulerGroup} tuned for fast tasks
	 */
	public static SchedulerGroup async(String name, int bufferSize, int parallelSchedulers) {
		return async(name, bufferSize, parallelSchedulers, null, null, false);
	}

	/**
	 * An Async factory is  a scheduler factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@literal parallelSchedulers} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each scheduler generation {@link #call} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing scheduler request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link TopicProcessor}
	 * @param autoShutdown true if this {@link SchedulerGroup} should automatically shutdown its resources
	 *
	 * @return a new {@link SchedulerGroup} tuned for fast tasks
	 */
	public static SchedulerGroup async(String name,
			int bufferSize,
			int parallelSchedulers,
			boolean autoShutdown) {
		return async(name, bufferSize, parallelSchedulers, null, null, autoShutdown);
	}

	/**
	 * An Async factory is  a scheduler factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@literal parallelSchedulers} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each scheduler generation {@link #call} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing scheduler request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link TopicProcessor}
	 * @param uncaughtExceptionHandler Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownHandler Callback signalled when a {@link Subscriber} thread terminates
	 *
	 * @return a new {@link SchedulerGroup} tuned for fast tasks
	 */
	public static SchedulerGroup async(String name,
			int bufferSize,
			int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler) {
		return async(name, bufferSize, parallelSchedulers, uncaughtExceptionHandler, shutdownHandler, true);
	}

	/**
	 * An Async factory is  a scheduler factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@literal parallelSchedulers} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each scheduler generation {@link #call} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing scheduler request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link TopicProcessor}
	 * @param uncaughtExceptionHandler Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownHandler Callback signalled when a {@link Subscriber} thread terminates
	 * @param autoShutdown true if this {@link SchedulerGroup} should automatically shutdown its resources
	 *
	 * @return a new {@link SchedulerGroup} tuned for fast tasks
	 */
	public static SchedulerGroup async(final String name,
			final int bufferSize,
			int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {

		return async(name, bufferSize, parallelSchedulers, uncaughtExceptionHandler, shutdownHandler, autoShutdown, DEFAULT_WAIT_STRATEGY);
	}

	/**
	 * An Async factory is  a scheduler factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@literal parallelSchedulers} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each scheduler generation {@link #call} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing scheduler request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link TopicProcessor}
	 * @param uncaughtExceptionHandler Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownHandler Callback signalled when a {@link Subscriber} thread terminates
	 * @param autoShutdown true if this {@link SchedulerGroup} should automatically shutdown its resources
	 * @param waitStrategy a {@link WaitStrategy} {@link Supplier} to trade-off cpu use for task consumer latency
	 *
	 * @return a new {@link SchedulerGroup} tuned for fast tasks
	 */
	public static SchedulerGroup async(final String name,
			final int bufferSize,
			final int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown,
			final Supplier<? extends WaitStrategy> waitStrategy) {

		return fromProcessor(new Callable<Consumer<Runnable>>() {
			int i = 1;
			@Override
			public Consumer<Runnable> call() throws Exception {
				return TopicProcessor.share(name+(parallelSchedulers > 1 ? "-"+(i++) : ""), bufferSize, waitStrategy
						.get(), false);
			}
		}, parallelSchedulers, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}


	/**
	 * @param scheduler
	 * @param parallelSchedulers

	 * @return
	 */
	public static SchedulerGroup create(Callable<? extends Consumer<Runnable>> scheduler, int parallelSchedulers) {
		return create(scheduler, parallelSchedulers, true);
	}

	/**
	 * @param scheduler
	 * @param parallelSchedulers

	 * @return
	 */
	public static SchedulerGroup create(Callable<? extends Consumer<Runnable>> scheduler,
			int parallelSchedulers,
			boolean autoShutdown) {
		return new SchedulerGroup(scheduler, parallelSchedulers, null, null, autoShutdown);
	}


	/**
	 * An IO factory is  a scheduler factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http call, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@link SchedulerGroup#DEFAULT_POOL_SIZE} subscribers that will
	 * compete to execute the
	 * {@link Runnable} tasks. The task backlog will be relatively large {@link PlatformDependent#MEDIUM_BUFFER_SIZE}
	 * to mitigate consuming rate difference.
	 *
	 * @return a new {@link SchedulerGroup} tuned for slow tasks
	 */
	public static SchedulerGroup io() {
		return io("io", PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * An IO factory is  a scheduler factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http call, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@link SchedulerGroup#DEFAULT_POOL_SIZE} subscribers that will
	 * compete to execute the
	 * {@link Runnable} tasks. The task backlog will be relatively large {@link PlatformDependent#MEDIUM_BUFFER_SIZE}
	 * to mitigate consuming rate difference.
	 *
	 * @param name Group name derived for thread identification
	 *
	 * @return a new {@link SchedulerGroup} tuned for slow tasks
	 */
	public static SchedulerGroup io(String name) {
		return io(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * An IO factory is  a scheduler factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http call, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@link SchedulerGroup#DEFAULT_POOL_SIZE} subscribers that will
	 * compete to execute the
	 * {@link Runnable} tasks. The task backlog will should be relatively large given {@literal bufferSize} to 
	 * mitigate consuming rate difference.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize Task backlog size, risk-off more memory for lower producer latency
	 *
	 * @return a new {@link SchedulerGroup} tuned for slow tasks
	 */
	public static SchedulerGroup io(String name, int bufferSize) {
		return io(name, bufferSize, DEFAULT_POOL_SIZE);
	}

	/**
	 * An IO factory is  a scheduler factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http call, file write...).
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
	 * @return a new {@link SchedulerGroup} tuned for slow tasks
	 */
	public static SchedulerGroup io(String name, int bufferSize, int concurrency) {
		return io(name, bufferSize, concurrency, null, null, true);
	}

	/**
	 * An IO factory is  a scheduler factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http call, file write...).
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
	 * @param autoShutdown true if this {@link SchedulerGroup} should automatically shutdown its resources
	 *
	 * @return a new {@link SchedulerGroup} tuned for slow tasks
	 */
	public static SchedulerGroup io(String name, int bufferSize, int concurrency, boolean autoShutdown) {
		return io(name, bufferSize, concurrency, null, null, autoShutdown);
	}

	/**
	 * An IO factory is  a scheduler factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http call, file write...).
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
	 * @return a new {@link SchedulerGroup} tuned for slow tasks
	 */
	public static SchedulerGroup io(String name, int bufferSize,
			int concurrency, Consumer<Throwable> uncaughtExceptionHandler, Runnable shutdownHandler) {
		return io(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, true);
	}

	/**
	 * An IO factory is  a scheduler factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http call, file write...).
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
	 * @param autoShutdown true if this {@link SchedulerGroup} should automatically shutdown its resources
	 *
	 * @return a new {@link SchedulerGroup} tuned for slow tasks
	 */
	public static SchedulerGroup io(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler, Runnable shutdownHandler, boolean autoShutdown) {
		return io(name,
				bufferSize,
				concurrency,
				uncaughtExceptionHandler,
				shutdownHandler,
				autoShutdown,
				DEFAULT_WAIT_STRATEGY.get());
	}

	/**
	 * An IO factory is  a scheduler factory with sensible defaults for for "slow" tasks
	 * and "blocking" IO (e.g. blocking http call, file write...).
	 *
	 * <p>
	 * It uses a single {@link WorkQueueProcessor} with {@link SchedulerGroup#DEFAULT_POOL_SIZE} subscribers that will
	 * compete to execute the
	 * {@link Runnable} tasks. The task backlog will should be relatively large given {@literal bufferSize} to 
	 * mitigate consuming rate difference.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize Task backlog size, risk-off more memory for lower producer latency
	 * @param concurrency Parallel workers to subscribe to the internal {@link WorkQueueProcessor}
	 * @param uncaughtExceptionHandler Unsignalled exceptions consumer, extremely fatal situtions if invoked
	 * @param shutdownHandler Callback signalled when a {@link Subscriber} thread terminates
	 * @param autoShutdown true if this {@link SchedulerGroup} should automatically shutdown its resources
	 * @param waitStrategy a {@link WaitStrategy} to trade-off cpu use for task consumer latency
	 *
	 * @return a new {@link SchedulerGroup} tuned for slow tasks
	 */
	public static SchedulerGroup io(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown,
			WaitStrategy waitStrategy) {


		return fromProcessor(WorkQueueProcessor.<Runnable>share(name, bufferSize, waitStrategy, false),
				concurrency,
				uncaughtExceptionHandler,
				shutdownHandler,
				autoShutdown);
	}

	/**
	 * Signal terminal signal {@literal null} to the passed {@link Consumer} {@link Runnable} schedulers
	 *
	 * @param schedulers the schedulers to shutdown
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static void release(Consumer<Runnable>... schedulers) {
		if (schedulers == null) {
			return;
		}

		for (Consumer<Runnable> sharedProcessorReference : schedulers) {
			sharedProcessorReference.accept(null);
		}
	}

	/**
	 * A Single factory is  a scheduler factory with sensible defaults for for "ultra-fast" and low-latency consuming.
	 *
	 * <p>
	 * It uses a single
	 * {@link TopicProcessor} subscribed once by a subscriber executing its partition of {@link Runnable} tasks.
	 * Due to its single-backlog/single-thread design, sensitivity to task execution time difference will not be
	 * mitigated.
	 *
	 * @return a new {@link SchedulerGroup} tuned for low latency tasks
	 */
	public static SchedulerGroup single() {
		return single("single", PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * A Single factory is  a scheduler factory with sensible defaults for for "ultra-fast" and low-latency consuming.
	 *
	 * <p>
	 * It uses a single
	 * {@link TopicProcessor} subscribed once by a subscriber executing its partition of {@link Runnable} tasks.
	 * Due to its single-backlog/single-thread design, sensitivity to task execution time difference will not be
	 * mitigated.
	 *
	 * @param name Group name derived for thread identification
	 *
	 * @return a new {@link SchedulerGroup} tuned for low latency tasks
	 */
	public static SchedulerGroup single(String name) {
		return single(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * A Single factory is  a scheduler factory with sensible defaults for for "ultra-fast" and low-latency consuming.
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
	 * @return a new {@link SchedulerGroup} tuned for low latency tasks
	 */
	public static SchedulerGroup single(String name, int bufferSize) {
		return single(name, bufferSize,null, null, false, SINGLE_WAIT_STRATEGY);
	}

	/**
	 * A Single factory is  a scheduler factory with sensible defaults for for "ultra-fast" and low-latency consuming.
	 *
	 * <p>
	 * It uses a single
	 * {@link TopicProcessor} subscribed once by a subscriber executing its partition of {@link Runnable} tasks.
	 * Due to its single-backlog/single-thread design, sensitivity to task execution time difference will not be
	 * mitigated.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param autoShutdown true if this {@link SchedulerGroup} should automatically shutdown its resources
	 *
	 * @return a new {@link SchedulerGroup} tuned for low latency tasks
	 */
	public static SchedulerGroup single(String name, int bufferSize, boolean autoShutdown) {
		return single(name, bufferSize, null, null, autoShutdown, SINGLE_WAIT_STRATEGY);
	}

	/**
	 * A Single factory is  a scheduler factory with sensible defaults for for "ultra-fast" and low-latency consuming.
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
	 * @return a new {@link SchedulerGroup} tuned for low latency tasks
	 */
	public static SchedulerGroup single(String name, int bufferSize, Consumer<Throwable> errorC,
			Runnable shutdownC) {
		return single(name, bufferSize, errorC, shutdownC, false, SINGLE_WAIT_STRATEGY);
	}

	/**
	 * A Single factory is  a scheduler factory with sensible defaults for for "ultra-fast" and low-latency consuming.
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
	 * @param autoShutdown true if this {@link SchedulerGroup} should automatically shutdown its resources
	 * @param waitStrategy a {@link WaitStrategy} {@link Supplier} to trade-off cpu use for task consumer latency
	 *
	 * @return a new {@link SchedulerGroup} tuned for low latency tasks
	 */
	public static SchedulerGroup single(String name, int bufferSize,
			Consumer<Throwable> errorC,
			Runnable shutdownC,  boolean autoShutdown, Supplier<? extends WaitStrategy> waitStrategy) {
		return async(name, bufferSize, 1, errorC, shutdownC, autoShutdown, waitStrategy);
	}

	/**
	 * @param scheduler

	 * @return
	 */
	public static SchedulerGroup single(Consumer<Runnable> scheduler) {
		return single(scheduler, true);
	}

	/**
	 * @param scheduler
	 * @param autoShutdown

	 * @return
	 */
	public static SchedulerGroup single(final Consumer<Runnable> scheduler, boolean autoShutdown) {
		return create(new Callable<Consumer<Runnable>>() {
			@Override
			public Consumer<Runnable> call() throws Exception {
				return scheduler;
			}
		}, 1, autoShutdown);
	}

	/**
	 * @return A passthrough {@link SchedulerGroup} which uses no resources and runs immiately its tasks.
	 */
	@SuppressWarnings("unchecked")
	public static SchedulerGroup sync() {
		return SYNC_SERVICE;
	}

	/**
	 * Execute the given runnable or decrement scheduler reference if {@literal null} is accepted
	 *
	 * @param runnable the task to run or null signal
	 */
	@Override
	public void accept(Runnable runnable) {
		if (runnable == null) {
			decrementReference();
		}
		else if (scheduler == null) {
			runnable.run();
		}
		else {
			scheduler.accept(runnable);
		}
	}

	/**
	 * Return a scheduler reference to this {@link SchedulerGroup}, incrementing use count by 1
	 *
	 * @return a new scheduler reference
	 */
	@Override
	public Consumer<Runnable> call() throws Exception {
		if (scheduler == null) {
			return NOOP_TASK_SUBSCRIBER;
		}
		incrementReference();
		return this;
	}

	/**
	 * Return a scheduler reference to this {@link SchedulerGroup}, incrementing use count by 1
	 *
	 * @param tailRecurse true if the scheduler should be reentrant
	 *
	 * @return a new scheduler reference
	 * @throws Exception
	 */
	public Consumer<Runnable> call(boolean tailRecurse) throws Exception {
		if(tailRecurse){
			if (scheduler == null) {
				incrementReference();
			}
			FluxProcessor<Runnable, Runnable> processor = FluxProcessor.async(this);
			processor.subscribe(NOOP_TASK_SUBSCRIBER);
			return processor.start();
		}
		return call();
	}

	/**
	 * Blocking shutdown of the internal {@link ExecutorProcessor} with {@link Processor#onComplete()}. If the
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
	 * Blocking shutdown of the internal {@link ExecutorProcessor} with {@link Processor#onComplete()}. If the
	 * processor doesn't implement
	 * {@link ExecutorProcessor} or if it is synchronous, throw an {@link UnsupportedOperationException}.
	 * @param timeout the time un given unit to wait for
	 * @param timeUnit the unit
	 *
	 * @return true if successfully shutdown
	 */
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		if (scheduler == null) {
			return true;
		}
		else if (scheduler instanceof ExecutorProcessor) {
			return ((ExecutorProcessor) scheduler).awaitAndShutdown(timeout, timeUnit);
		}
		throw new UnsupportedOperationException("Underlying Processor is null or doesn't implement ExecutorProcessor");
	}

	/**
	 * Non-blocking forced shutdown of the internal {@link Processor} with {@link Processor#onComplete()}
	 */
	public void forceShutdown() {
		if (scheduler == null) {
			return;
		}
		else if (scheduler instanceof ExecutorProcessor) {
			((ExecutorProcessor) scheduler).forceShutdown();
			return;
		}
		throw new UnsupportedOperationException("Underlying Processor is null or doesn't implement ExecutorProcessor");
	}

	@Override
	public boolean isTerminated() {
		return scheduler != null && scheduler instanceof ExecutorProcessor && ((ExecutorProcessor) scheduler).isTerminated();
	}

	@Override
	public boolean isStarted() {
		return scheduler == null || !(scheduler instanceof ExecutorProcessor) || ((ExecutorProcessor) scheduler).isStarted();
	}

	/**
	 * Non-blocking shutdown of the internal {@link Processor} with {@link Processor#onComplete()}
	 */
	public void shutdown() {
		if (scheduler == null) {
			return;
		}
		if (scheduler instanceof ExecutorProcessor) {
			((ExecutorProcessor) scheduler).shutdown();
		}
		else {
			scheduler.accept(null);
		}
	}

	/* INTERNAL */

	@SuppressWarnings("unchecked")
	static SchedulerGroup fromProcessor(final Consumer<Runnable> scheduler,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		return new SchedulerGroup(new Callable<Consumer<Runnable>>() {
			@Override
			public Consumer<Runnable> call() throws Exception {
				return scheduler;
			}
		}, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	@SuppressWarnings("unchecked")
	static SchedulerGroup fromProcessor(Callable<? extends Consumer<Runnable>> schedulerFactory,
			int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		if (schedulerFactory != null && parallelSchedulers > 1) {
			return new PooledSchedulerGroup(schedulerFactory, parallelSchedulers, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
		}
		else {
			return new SchedulerGroup(schedulerFactory, 1, uncaughtExceptionHandler, shutdownHandler,
					autoShutdown);
		}
	}

	@SuppressWarnings("unchecked")
	static final SchedulerGroup SYNC_SERVICE = new SchedulerGroup(null, -1, null, null, false);

	static final Supplier<? extends WaitStrategy> DEFAULT_WAIT_STRATEGY = new Supplier<WaitStrategy>() {
		@Override
		public WaitStrategy get() {
			return WaitStrategy.phasedOffLiteLock(200, 200, TimeUnit.MILLISECONDS);
		}
	};

	static final Supplier<? extends WaitStrategy> SINGLE_WAIT_STRATEGY = new Supplier<WaitStrategy>() {
		@Override
		public WaitStrategy get() {
			return WaitStrategy.phasedOffLiteLock(500, 50, TimeUnit.MILLISECONDS);
		}
	};

	static final TaskSubscriber NOOP_TASK_SUBSCRIBER = new TaskSubscriber(null, null);

	final Consumer<Runnable>         scheduler;
	final boolean                    autoShutdown;
	final int                        parallelSchedulers;

	@SuppressWarnings("unused")
	private volatile int refCount = 0;

	static final AtomicIntegerFieldUpdater<SchedulerGroup> REF_COUNT =
			AtomicIntegerFieldUpdater.newUpdater(SchedulerGroup.class, "refCount");

	@SuppressWarnings("unchecked")
	protected SchedulerGroup(Callable<? extends Consumer<Runnable>> schedulerFactory,
			int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		this.autoShutdown = autoShutdown;
		this.parallelSchedulers = parallelSchedulers;

		if (schedulerFactory != null) {
			Consumer<Runnable> scheduler = null;
			try {
				scheduler = schedulerFactory.call();
			}
			catch (Throwable ex){
				Exceptions.throwIfFatal(ex);
				Exceptions.failUpstream(ex);
			}
			this.scheduler = Objects.requireNonNull(scheduler, "Provided schedulerFactory returned no scheduler");

			if(scheduler instanceof Processor){
				@SuppressWarnings("unchecked")
				Processor<Runnable, Runnable> p = (Processor<Runnable, Runnable>)scheduler;
				for (int i = 0; i < parallelSchedulers; i++) {
					p.onSubscribe(EmptySubscription.INSTANCE);
					p.subscribe(new TaskSubscriber(uncaughtExceptionHandler, shutdownHandler));
				}
			}


		}
		else {
			this.scheduler = null;
		}
	}

	@Override
	public Object connectedInput() {
		return scheduler;
	}

	@Override
	public Object connectedOutput() {
		return scheduler;
	}

	protected void decrementReference() {
		if ((scheduler != null || parallelSchedulers > 1) && REF_COUNT.decrementAndGet(this) <= 0 && autoShutdown) {
			shutdown();
		}
	}

	protected void incrementReference() {
		REF_COUNT.incrementAndGet(this);
	}

	final static class PooledSchedulerGroup extends SchedulerGroup implements MultiProducer {

		final SchedulerGroup[] schedulerGroups;

		volatile int index = 0;

		public PooledSchedulerGroup(Callable<? extends Consumer<Runnable>> schedulerFactory,
				int parallelSchedulers,
				Consumer<Throwable> uncaughtExceptionHandler,
				Runnable shutdownHandler,
				boolean autoShutdown) {
			super(null, parallelSchedulers, null, null, autoShutdown);

			schedulerGroups = new SchedulerGroup[parallelSchedulers];

			for (int i = 0; i < parallelSchedulers; i++) {
				schedulerGroups[i] =
						new InnerSchedulerGroup(schedulerFactory, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
			}
		}

		@Override
		public void shutdown() {
			for (SchedulerGroup schedulerGroup : schedulerGroups) {
				schedulerGroup.shutdown();
			}
		}

		@Override
		public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
			for (SchedulerGroup schedulerGroup : schedulerGroups) {
				if (!schedulerGroup.awaitAndShutdown(timeout, timeUnit)) {
					return false;
				}
			}
			return true;
		}

		@Override
		public Iterator<?> downstreams() {
			return Arrays.asList(schedulerGroups)
			             .iterator();
		}

		@Override
		public long downstreamCount() {
			return schedulerGroups.length;
		}

		@Override
		public void forceShutdown() {
			for (SchedulerGroup schedulerGroup : schedulerGroups) {
				schedulerGroup.forceShutdown();
			}
		}

		@Override
		public boolean isStarted() {
			for (SchedulerGroup schedulerGroup : schedulerGroups) {
				if (!schedulerGroup.isStarted()) {
					return false;
				}
			}
			return true;
		}

		private SchedulerGroup next() {
			int index = this.index++;
			if (index == Integer.MAX_VALUE) {
				this.index -= Integer.MAX_VALUE;
			}
			return schedulerGroups[index % parallelSchedulers];
		}

		@Override
		public void accept(Runnable runnable) {
			next().accept(runnable);
		}

		@Override
		public Consumer<Runnable> call() throws Exception {
			return next().call();
		}

		private class InnerSchedulerGroup extends SchedulerGroup implements Introspectable {

			public InnerSchedulerGroup(Callable<? extends Consumer<Runnable>> schedulerFactory,
					Consumer<Throwable> uncaughtExceptionHandler,
					Runnable shutdownHandler,
					boolean autoShutdown) {
				super(schedulerFactory, 1, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
			}

			@Override
			protected void decrementReference() {
				REF_COUNT.decrementAndGet(this);
				PooledSchedulerGroup.this.decrementReference();
			}

			@Override
			protected void incrementReference() {
				REF_COUNT.incrementAndGet(this);
				PooledSchedulerGroup.this.incrementReference();
			}

			@Override
			public int getMode() {
				return INNER;
			}

			@Override
			public String getName() {
				return InnerSchedulerGroup.class.getSimpleName();
			}
		}
	}

	final static class TaskSubscriber implements Subscriber<Runnable>, Consumer<Runnable>, Introspectable {

		private final Consumer<Throwable> uncaughtExceptionHandler;
		private final Runnable            shutdownHandler;

		public TaskSubscriber(Consumer<Throwable> uncaughtExceptionHandler, Runnable shutdownHandler) {
			this.uncaughtExceptionHandler = uncaughtExceptionHandler;
			this.shutdownHandler = shutdownHandler;
		}

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Runnable task) {
			try {
				task.run();
			}
			catch (Exceptions.CancelException ce) {
				//IGNORE
			}
			catch (Throwable t) {
				routeError(t);
			}
		}

		@Override
		public void accept(Runnable runnable) {
			try {
				if (runnable == null) {
					if (shutdownHandler != null) {
						shutdownHandler.run();
					}
				}
				else {
					runnable.run();
				}
			}
			catch (Exceptions.CancelException ce) {
				//IGNORE
			}
			catch (Throwable t){
				routeError(t);
			}
		}

		void routeError(Throwable t){
			if(uncaughtExceptionHandler != null){
				uncaughtExceptionHandler.accept(t);
			}
			else{
				log.error("Unrouted exception", t);
			}
		}

		@Override
		public int getMode() {
			return TRACE_ONLY;
		}

		@Override
		public String getName() {
			return TaskSubscriber.class.getSimpleName();
		}

		@Override
		public void onError(Throwable t) {
			if (uncaughtExceptionHandler != null) {
				uncaughtExceptionHandler.accept(t);
			}
			Exceptions.throwIfFatal(t);

			//TODO support resubscribe ?
			throw new UnsupportedOperationException("No error handler provided for this SchedulerGroup", t);
		}

		@Override
		public void onComplete() {
			if (shutdownHandler != null) {
				shutdownHandler.run();
			}
		}
	}
}