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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.core.util.PlatformDependent;
import reactor.core.util.WaitStrategy;

/**
 * @deprecated Use {@link Schedulers}
 * @author Stephane Maldini
 */
@Deprecated
public final class Computations {

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given current number of CPU (max 4) x {@link TopicProcessor} subscribed
	 * once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link Scheduler#createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @return a new {@link Scheduler} tuned for fast tasks
	 */
	public static Scheduler parallel() {
		return parallel("parallel", PlatformDependent.MEDIUM_BUFFER_SIZE,
				PlatformDependent.DEFAULT_POOL_SIZE, true);
	}

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given current number of CPU (max 4) x {@link TopicProcessor} subscribed
	 * once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link Scheduler#createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 *
	 * @return a new {@link Scheduler} tuned for fast tasks
	 */
	public static Scheduler parallel(String name) {
		return parallel(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given current number of CPU (max 4) x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link Scheduler#createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 *
	 * @return a new {@link Scheduler} tuned for fast tasks
	 */
	public static Scheduler parallel(String name, int bufferSize) {
		return parallel(name, bufferSize, PlatformDependent.DEFAULT_POOL_SIZE);
	}

	/**
	 * An Async factory is  a worker factory with sensible defaults for for "fast" or
	 *  "non-blocking" tasks.
	 *
	 * <p>
	 * It uses N given {@literal parallelSchedulers} x {@link TopicProcessor} subscribed once each by a
	 * subscriber executing its partition of {@link Runnable} tasks. Each worker generation {@link Scheduler#createWorker} will
	 * round robin over the pooled list of {@link TopicProcessor}. Due to its partitioned design, sensitivity to
	 * consuming rate difference is found mitigated which is suited for rapid firing worker request from dynamic
	 * flows.
	 *
	 * @param name Group name derived for thread identification
	 * @param bufferSize N x Task backlog size, risk-off more memory for lower producer latency
	 * @param parallelSchedulers Parallel schedulers subscribed once each to their respective internal
	 * {@link TopicProcessor}
	 *
	 * @return a new {@link Scheduler} tuned for fast tasks
	 */
	public static Scheduler parallel(String name, int bufferSize, int parallelSchedulers) {
		return parallel(name, bufferSize, parallelSchedulers, null, null, false);
	}

	/**
	 * @deprecated use {@link Schedulers#newComputation(int, int, ThreadFactory)}
	 */
	@Deprecated
	public static Scheduler parallel(String name,
			int bufferSize,
			int parallelSchedulers,
			boolean autoShutdown) {
		return parallel(name, bufferSize, parallelSchedulers, null, null, autoShutdown);
	}

	/**
	 * @deprecated use {@link Schedulers#newComputation(int, int, ThreadFactory)}
	 */
	@Deprecated
	public static Scheduler parallel(String name,
			int bufferSize,
			int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler) {
		return parallel(name, bufferSize, parallelSchedulers, uncaughtExceptionHandler, shutdownHandler, false);
	}

	/**
	 * @deprecated use {@link Schedulers#newComputation(int, int, ThreadFactory)}
	 */
	@Deprecated
	public static Scheduler parallel(final String name,
			final int bufferSize,
			int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {

		return parallel(name, bufferSize, parallelSchedulers, uncaughtExceptionHandler, shutdownHandler, autoShutdown, DEFAULT_WAIT_STRATEGY);
	}


	/**
	 * @deprecated use {@link Schedulers#newComputation(int, int, ThreadFactory)}
	 */
	@Deprecated
	public static Scheduler parallel(final String name,
			final int bufferSize,
			final int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown,
			final Supplier<? extends WaitStrategy> waitStrategy) {

		return Schedulers.newParallel(parallelSchedulers, new ThreadFactory() {
			AtomicInteger i = new AtomicInteger();

			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, name + i.incrementAndGet());
				t.setDaemon(autoShutdown);
				t.setUncaughtExceptionHandler((thread, e) -> uncaughtExceptionHandler.accept(
						e));
				return t;
			}
		});
	}



	/**
	 * @deprecated use {@link TopicProcessor#asScheduler(Supplier, int, boolean)} )}
	 */
	@Deprecated
	public static Scheduler create(EventLoopProcessor<Runnable> processor, int
			parallelSchedulers) {
		return create(() -> processor, parallelSchedulers, false);
	}

	/**
	 * @deprecated use {@link TopicProcessor#asScheduler(Supplier, int, boolean)} )}
	 */
	@Deprecated
	public static Scheduler create(Supplier<? extends EventLoopProcessor<Runnable>> processors,
			int parallelSchedulers,
			boolean autoShutdown) {
		return create(processors, parallelSchedulers, null, null, autoShutdown);
	}

	/**
	 * @deprecated use {@link TopicProcessor#asScheduler(Supplier, int, boolean)} )}
	 */
	@Deprecated
	public static Scheduler create(Supplier<? extends EventLoopProcessor<Runnable>> processors,
			int parallelSchedulers,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		return EventLoopProcessor.asScheduler(processors,
				parallelSchedulers,
				autoShutdown);
	}

	/**
	 * @deprecated use {@link Schedulers#parallel}
	 */
	@Deprecated
	public static Scheduler concurrent() {
		return concurrent("concurrent", PlatformDependent.MEDIUM_BUFFER_SIZE, PlatformDependent.DEFAULT_POOL_SIZE, true);
	}


	/**
	 * @deprecated use {@link Schedulers#parallel}
	 */
	@Deprecated
	public static Scheduler concurrent(String name) {
		return concurrent(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @deprecated use {@link Schedulers#parallel}
	 */
	@Deprecated
	public static Scheduler concurrent(String name, int bufferSize) {
		return concurrent(name, bufferSize, PlatformDependent.DEFAULT_POOL_SIZE);
	}

	/**
	 * @deprecated use {@link Schedulers#parallel}
	 */
	@Deprecated
	public static Scheduler concurrent(String name, int bufferSize, int concurrency) {
		return concurrent(name, bufferSize, concurrency, null, null, false);
	}


	/**
	 * @deprecated use {@link Schedulers#parallel}
	 */
	@Deprecated
	public static Scheduler concurrent(String name, int bufferSize, int concurrency, boolean autoShutdown) {
		return concurrent(name, bufferSize, concurrency, null, null, autoShutdown);
	}


	/**
	 * @deprecated use {@link Schedulers#parallel}
	 */
	@Deprecated
	public static Scheduler concurrent(String name, int bufferSize,
			int concurrency, Consumer<Throwable> uncaughtExceptionHandler, Runnable shutdownHandler) {
		return concurrent(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, false);
	}


	/**
	 * @deprecated use {@link Schedulers#parallel}
	 */
	@Deprecated
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
	 * @deprecated use {@link Schedulers#parallel}
	 */
	@Deprecated
	public static Scheduler concurrent(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown,
			WaitStrategy waitStrategy) {

		EventLoopProcessor<Runnable> p =
				WorkQueueProcessor.share(name, bufferSize, waitStrategy, false);
		return EventLoopProcessor.asScheduler(() -> p, concurrency, autoShutdown);
	}

	 /**
	 * @deprecated use {@link Schedulers#single}
	 */
	@Deprecated
	public static Scheduler single() {
		return single("single", PlatformDependent.MEDIUM_BUFFER_SIZE, true);
	}

	/**
	 * @deprecated use {@link Schedulers#single}
	 */
	@Deprecated
	public static Scheduler single(String name) {
		return single(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}


	/**
	 * @deprecated use {@link Schedulers#single}
	 */
	@Deprecated
	public static Scheduler single(String name, int bufferSize) {
		return single(name, bufferSize, null, null, false, SINGLE_WAIT_STRATEGY);
	}

	/**
	 * @deprecated use {@link Schedulers#single}
	 */
	@Deprecated
	public static Scheduler single(String name, int bufferSize, boolean autoShutdown) {
		return single(name, bufferSize, null, null, autoShutdown, SINGLE_WAIT_STRATEGY);
	}


	/**
	 * @deprecated use {@link Schedulers#single}
	 */
	@Deprecated
	public static Scheduler single(String name, int bufferSize, WaitStrategy waitStrategy) {
		return single(name, bufferSize, null, null, false, () -> waitStrategy);
	}


	/**
	 * @deprecated use {@link Schedulers#single}
	 */
	@Deprecated
	public static Scheduler single(String name, int bufferSize, Consumer<Throwable> errorC,
			Runnable shutdownC) {
		return single(name, bufferSize, errorC, shutdownC, false, SINGLE_WAIT_STRATEGY);
	}


	/**
	 * @deprecated use {@link Schedulers#single}
	 */
	@Deprecated
	public static Scheduler single(String name, int bufferSize,
			Consumer<Throwable> errorC,
			Runnable shutdownC,  boolean autoShutdown, Supplier<? extends WaitStrategy> waitStrategy) {
		return parallel(name, bufferSize, 1, errorC, shutdownC, autoShutdown, waitStrategy);
	}

	/**
	 * @deprecated use {@link Schedulers#single}
	 */
	@Deprecated
	public static Scheduler single(EventLoopProcessor<Runnable> processor) {
		return single(processor, false);
	}

	/**
	 * @deprecated use {@link Schedulers#single}
	 */
	@Deprecated
	public static Scheduler single(final EventLoopProcessor<Runnable> processor,
			boolean autoShutdown) {
		return create(() -> processor, 1, autoShutdown);
	}
	
	/* INTERNAL */

	static final Supplier<? extends WaitStrategy> DEFAULT_WAIT_STRATEGY =
			() -> WaitStrategy.phasedOffLiteLock(200, 200, TimeUnit.MILLISECONDS);

	static final Supplier<? extends WaitStrategy> SINGLE_WAIT_STRATEGY =
			() -> WaitStrategy.phasedOffLiteLock(500, 50, TimeUnit.MILLISECONDS);
}