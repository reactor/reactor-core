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

package reactor;

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.processor.EmitterProcessor;
import reactor.core.processor.ExecutorProcessor;
import reactor.core.processor.FluxProcessor;
import reactor.core.processor.ProcessorGroup;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.core.support.Assert;
import reactor.core.support.Exceptions;
import reactor.core.support.ReactiveState;
import reactor.core.support.WaitStrategy;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;

/**
 * Main gateway to build various asynchronous {@link Processor} or "pool" services that allow their reuse. Reactor
 * offers a few management API via the subclassed {@link FluxProcessor} for the underlying {@link
 * java.util.concurrent.Executor} in use.
 * @author Stephane Maldini
 * @since 2.5
 */
public final class Processors {

	/**
	 * Default number of processors available to the runtime on init (min 4)
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE = Math.max(Runtime.getRuntime()
	                                                            .availableProcessors(), 4);
	/**
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup() {
		return asyncGroup("async", ReactiveState.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name) {
		return asyncGroup(name, ReactiveState.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name, int bufferSize) {
		return asyncGroup(name, bufferSize, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name, int bufferSize, int concurrency) {
		return asyncGroup(name, bufferSize, concurrency, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name,
			int bufferSize,
			Consumer<Throwable> uncaughtExceptionHandler) {
		return asyncGroup(name, bufferSize, uncaughtExceptionHandler, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name,
			int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler) {
		return asyncGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name,
			int bufferSize,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler) {
		return asyncGroup(name, bufferSize, uncaughtExceptionHandler, shutdownHandler, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name,
			int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler) {
		return asyncGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(String name,
			int bufferSize,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		return asyncGroup(name, bufferSize, DEFAULT_POOL_SIZE, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**
	 *
	 * Non-Blocking "Asynchronous" Dedicated Pub-Sub (1 Thread by Sub)
	 *
	 *
	 */

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {

		return asyncGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown, DEFAULT_WAIT_STRATEGY);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param waitprovider
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> asyncGroup(final String name,
			final int bufferSize,
			final int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown,
			final Supplier<? extends WaitStrategy> waitprovider) {

		return ProcessorGroup.create(new Supplier<Processor<Runnable, Runnable>>() {
			int i = 1;
			@Override
			public Processor<Runnable, Runnable> get() {
				return RingBufferProcessor.share(name+(concurrency > 1 ? "-"+(i++) : ""), bufferSize, waitprovider
						.get(), false);
			}
		}, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT, E extends Subscriber<IN>> FluxProcessor<IN, OUT> blackbox(
			final E input,
			final Function<E, ? extends Publisher<OUT>> processor) {
		return new DelegateProcessor<>(processor.apply(input), input);
	}

	/**
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> FluxProcessor<IN, OUT> create(final Subscriber<IN> upstream, final Publisher<OUT> downstream) {
		return new DelegateProcessor<>(downstream, upstream);
	}

	/**
	 * Create a new {@link Processor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> FluxProcessor<E, E> emitter() {
		return emitter(true);
	}

	/**
	 *
	 * Non-Blocking "Asynchronous" Work Queue (akin to vanilla Java Executor)
	 *
	 *
	 */

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> FluxProcessor<E, E> emitter(boolean autoCancel) {
		return emitter(ReactiveState.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> FluxProcessor<E, E> emitter(int bufferSize) {
		return emitter(bufferSize, Integer.MAX_VALUE);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> FluxProcessor<E, E> emitter(int bufferSize, int concurrency) {
		return emitter(bufferSize, concurrency, true);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> FluxProcessor<E, E> emitter(int bufferSize, boolean autoCancel) {
		return emitter(bufferSize, Integer.MAX_VALUE, autoCancel);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> FluxProcessor<E, E> emitter(int bufferSize, int concurrency, boolean autoCancel) {
		return new EmitterProcessor<>(autoCancel, concurrency, bufferSize, -1);
	}

	/**
	 *
	 * Non-Blocking "Asynchronous" Pooled Processors or "ProcessorGroup" : reuse resources with virtual processor
	 * references delegating to a pool of asynchronous processors (e.g. Topic).
	 *
	 * Dispatching behavior will implicitly or explicitly adapt to the reference method used: dispatchOn()
	 * or publisherOn().
	 *
	 */

	/**
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup() {
		return ioGroup("io", ReactiveState.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(String name) {
		return ioGroup(name, ReactiveState.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(String name, int bufferSize) {
		return ioGroup(name, bufferSize, DEFAULT_POOL_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(String name, int bufferSize, int concurrency) {
		return ioGroup(name, bufferSize, concurrency, null, null, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(String name,
			int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler) {
		return ioGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, null, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(String name,
			int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler) {
		return ioGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		return ioGroup(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown,
				DEFAULT_WAIT_STRATEGY.get());
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param waitStrategy
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> ioGroup(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown,
			WaitStrategy waitStrategy) {

		return ProcessorGroup.create(RingBufferWorkProcessor.<Runnable>share(name, bufferSize,
				waitStrategy, false),
				concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls and is suited for
	 * multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> queue() {
		return queue("worker", ReactiveState.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls and is suited for
	 * multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> queue(String name) {
		return queue(name, ReactiveState.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> queue(boolean autoCancel) {
		return queue(Processors.class.getSimpleName(), ReactiveState.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> queue(String name, int bufferSize) {
		return queue(name, bufferSize, true);
	}

	/**
	 * Create a new {@link FluxProcessor} using the passed buffer size and auto-cancel settings. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> queue(String name, int bufferSize, boolean autoCancel) {
		return RingBufferWorkProcessor.create(name, bufferSize, autoCancel);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> FluxProcessor<E, E> replay() {
		return replay(ReactiveState.SMALL_BUFFER_SIZE);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> FluxProcessor<E, E> replay(int historySize) {
		return replay(historySize, Integer.MAX_VALUE);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> FluxProcessor<E, E> replay(int historySize, int concurrency) {
		return replay(historySize, concurrency, false);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p>
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> FluxProcessor<E, E> replay(int historySize, int concurrency, boolean autoCancel) {
		return new EmitterProcessor<>(autoCancel, concurrency, historySize, historySize);
	}

	/**
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup() {
		return singleGroup("single", ReactiveState.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup(String name) {
		return singleGroup(name, ReactiveState.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup(String name, int bufferSize) {
		return singleGroup(name, bufferSize, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup(String name, int bufferSize, Consumer<Throwable> errorC) {
		return singleGroup(name, bufferSize, errorC, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup(String name, int bufferSize, Consumer<Throwable> errorC,
			Runnable shutdownC) {
		return singleGroup(name, bufferSize, errorC, shutdownC, SINGLE_WAIT_STRATEGY);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> singleGroup(String name, int bufferSize, Consumer<Throwable> errorC,
			Runnable shutdownC, Supplier<? extends WaitStrategy> waitStrategy) {
		return asyncGroup(name, bufferSize, 1, errorC, shutdownC, true, waitStrategy);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> topic() {
		return topic("async", ReactiveState.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> topic(String name) {
		return topic(name, ReactiveState.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> topic(boolean autoCancel) {
		return topic(Processors.class.getSimpleName(), ReactiveState.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link FluxProcessor} using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> topic(String name, int bufferSize) {
		return topic(name, bufferSize, true);
	}

	/**
	 * Create a new {@link FluxProcessor} using the blockingWait Strategy, passed backlog size, and auto-cancel
	 * settings. <p> A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded publisher
	 * that will fan-in data. <p> The passed {@link java.util.concurrent.ExecutorService} will execute as many
	 * event-loop consuming the ringbuffer as subscribers.
	 * @param name Use a new Cached ExecutorService and assign this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ExecutorProcessor<E, E> topic(String name, int bufferSize, boolean autoCancel) {
		return RingBufferProcessor.create(name, bufferSize, autoCancel);
	}

	private Processors() {
	}


	private static final Supplier<? extends WaitStrategy> DEFAULT_WAIT_STRATEGY = new Supplier<WaitStrategy>() {
		@Override
		public WaitStrategy get() {
			return WaitStrategy.PhasedOff.withLiteLock(200, 200, TimeUnit.MILLISECONDS);
		}
	};

	/**
	 *
	 * Non-Blocking "Synchronous" Pub-Sub
	 *
	 *
	 */
	private static final Supplier<? extends WaitStrategy> SINGLE_WAIT_STRATEGY = new Supplier<WaitStrategy>() {
		@Override
		public WaitStrategy get() {
			return WaitStrategy.PhasedOff.withLiteLock(500, 50, TimeUnit.MILLISECONDS);
		}
	};

	private static class DelegateProcessor<IN, OUT>
			extends FluxProcessor<IN, OUT> implements ReactiveState.Downstream, ReactiveState.Bounded {

		private final Publisher<OUT> downstream;
		private final Subscriber<IN> upstream;

		public DelegateProcessor(Publisher<OUT> downstream, Subscriber<IN> upstream) {
			Assert.notNull(upstream, "Upstream must not be null");
			Assert.notNull(downstream, "Downstream must not be null");

			this.downstream = downstream;
			this.upstream = upstream;
		}

		@Override
		public Subscriber<? super IN> downstream() {
			return upstream;
		}

		@Override
		public long getCapacity() {
			return Bounded.class.isAssignableFrom(upstream.getClass()) ? ((Bounded) upstream).getCapacity() :
					Long.MAX_VALUE;
		}

		@Override
		public void onComplete() {
			upstream.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			upstream.onError(t);
		}

		@Override
		public void onNext(IN in) {
			upstream.onNext(in);
		}

		@Override
		public void onSubscribe(Subscription s) {
			upstream.onSubscribe(s);
		}

		@Override
		public void subscribe(Subscriber<? super OUT> s) {
			if(s == null)
				throw Exceptions.spec_2_13_exception();
			downstream.subscribe(s);
		}
	}

}
