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

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.concurrent.WaitStrategy;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 ** An implementation of a RingBuffer backed message-passing Processor implementing publish-subscribe with async event
 * loops.
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.0.M2/src/docs/marble/topic.png" alt="">
 * <p>
 *  Created from {@link #share}, the {@link TopicProcessor} will authorize concurrent publishing (multi-producer)
 *  from its receiving side {@link Subscriber#onNext(Object)}.
 *  Additionally, any of the {@link TopicProcessor} will stop the event loop thread if an error occurs.
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.0.M2/src/docs/marble/topics.png" alt="">
 * <p>
 * The processor
 * respects the Reactive Streams contract and must not be signalled concurrently on any
 * onXXXX method if {@link #share} has not been used. Each subscriber will be assigned a unique thread that will only
 * stop on
 * terminal event: Complete, Error or Cancel. If Auto-Cancel is enabled, when all
 * subscribers are unregistered, a cancel signal is sent to the upstream Publisher if any.
 * Executor can be customized and will define how many concurrent subscribers are allowed
 * (fixed thread). When a Subscriber requests Long.MAX, there won't be any backpressure
 * applied and the producer will run at risk of being throttled if the subscribers don't
 * catch up. With any other strictly positive demand, a subscriber will stop reading new
 * Next signals (Complete and Error will still be read) as soon as the demand has been
 * fully consumed by the publisher. <p> When more than 1 subscriber listens to that
 * processor, they will all receive the exact same events if their respective demand is
 * still strictly positive, very much like a Fan-Out scenario. <p> When the backlog has
 * been completely booked and no subscribers is draining the signals, the publisher will
 * start throttling. In effect the smaller the backlog size is defined, the smaller the
 * difference in processing rate between subscribers must remain. Since the sequence for
 * each subscriber will point to various ringBuffer locations, the processor knows when a
 * backlog can't override the previously occupied slot.
 * @param <E> Type of dispatched signal
 * @author Stephane Maldini
 * @author Anatoly Kadyshev
 */
public final class TopicProcessor<E> extends EventLoopProcessor<E>  {

	/**
	 * {@link TopicProcessor} builder that can be used to create new
	 * processors. Instantiate it through the {@link TopicProcessor#builder()} static
	 * method:
	 * <p>
	 * {@code TopicProcessor<String> processor = TopicProcessor.<String>builder().build()}
	 *
	 * @param <T> Type of dispatched signal
	 */
	public final static class Builder<T> {

		String name;
		ExecutorService executor;
		ExecutorService requestTaskExecutor;
		int bufferSize;
		WaitStrategy waitStrategy;
		boolean share;
		boolean autoCancel;
		Supplier<T> signalSupplier;

		Builder() {
			this.bufferSize = QueueSupplier.SMALL_BUFFER_SIZE;
			this.autoCancel = true;
			this.share = false;
		}

		/**
		 * Configures name for this builder. Default value is TopicProcessor.
		 * Name is set to default if the provided <code>name</code> is null.
		 * @param name Use a new cached ExecutorService and assign this name to the created threads
		 *             if {@link #executor(ExecutorService)} is not configured.
		 * @return builder with provided name
		 */
		public Builder<T> name(@Nullable String name) {
			if (executor != null)
				throw new IllegalArgumentException("Executor service is configured, name will not be used.");
			this.name = name;
			return this;
		}

		/**
		 * Configures buffer size for this builder. Default value is {@link QueueSupplier#SMALL_BUFFER_SIZE}.
		 * @param bufferSize the internal buffer size to hold signals, must be a power of 2.
		 * @return builder with provided buffer size
		 */
		public Builder<T> bufferSize(int bufferSize) {
			if (!QueueSupplier.isPowerOfTwo(bufferSize)) {
				throw new IllegalArgumentException("bufferSize must be a power of 2 : " + bufferSize);
			}

			if (bufferSize < 1){
				throw new IllegalArgumentException("bufferSize must be strictly positive, " +
						"was: "+bufferSize);
			}
			this.bufferSize = bufferSize;
			return this;
		}

		/**
		 * Configures wait strategy for this builder. Default value is {@link WaitStrategy#phasedOffLiteLock(long, long, TimeUnit)}.
		 * Wait strategy is set to default if the provided <code>waitStrategy</code> is null.
		 * @param waitStrategy A RingBuffer WaitStrategy to use instead of the default blocking wait strategy.
		 * @return builder with provided wait strategy
		 */
		public Builder<T> waitStrategy(@Nullable WaitStrategy waitStrategy) {
			this.waitStrategy = waitStrategy;
			return this;
		}

		/**
		 * Configures auto-cancel for this builder. Default value is true.
		 * @param autoCancel automatically cancel
		 * @return builder with provided auto-cancel
		 */
		public Builder<T> autoCancel(boolean autoCancel) {
			this.autoCancel = autoCancel;
			return this;
		}

		/**
		 * Configures an {@link ExecutorService} to execute as many event-loop consuming the
		 * ringbuffer as subscribers. Name configured using {@link #name(String)} will be ignored
		 * if executor is set.
		 * @param executor A provided ExecutorService to manage threading infrastructure
		 * @return builder with provided executor
		 */
		public Builder<T> executor(@Nullable ExecutorService executor) {
			this.executor = executor;
			return this;
		}


		/**
		 * Configures an additional {@link ExecutorService} that is used internally
		 * on each subscription.
		 * @param requestTaskExecutor internal request executor
		 * @return builder with provided internal request executor
		 */
		public Builder<T> requestTaskExecutor(@Nullable ExecutorService requestTaskExecutor) {
			this.requestTaskExecutor = requestTaskExecutor;
			return this;
		}

		/**
		 * Configures sharing state for this builder. A shared Processor authorizes
		 * concurrent onNext calls and is suited for multi-threaded publisher that
		 * will fan-in data.
		 * @param share true to support concurrent onNext calls
		 * @return builder with specified sharing
		 */
		public Builder<T> share(boolean share) {
			this.share = share;
			return this;
		}

		/**
		 * Configures a supplier of dispatched signals to preallocate in the ring buffer
		 * @param signalSupplier A supplier of dispatched signals to preallocate
		 * @return builder with provided signal supplier
		 */
		public Builder<T> signalSupplier(@Nullable Supplier<T> signalSupplier) {
			this.signalSupplier = signalSupplier;
			return this;
		}

		/**
		 * Creates a new {@link TopicProcessor} using the properties
		 * of this builder.
		 * @return a fresh processor
		 */
		public TopicProcessor<T>  build() {
			this.name = this.name != null ? this.name : TopicProcessor.class.getSimpleName();
			this.waitStrategy = this.waitStrategy != null ? this.waitStrategy : WaitStrategy.phasedOffLiteLock(200, 100, TimeUnit.MILLISECONDS);
			ThreadFactory threadFactory = this.executor != null ? null : new EventLoopFactory(name, autoCancel);
			ExecutorService requestTaskExecutor = this.requestTaskExecutor != null ? this.requestTaskExecutor : defaultRequestTaskExecutor(defaultName(threadFactory, TopicProcessor.class));
			return new TopicProcessor<>(
					threadFactory,
					executor,
					requestTaskExecutor,
					bufferSize,
					waitStrategy,
					share,
					autoCancel,
					signalSupplier);
		}
	}

	/**
	 * Create a new {@link TopicProcessor} {@link Builder} with default properties.
	 * @return new TopicProcessor builder
	 */
	public static <E> Builder<E> builder()  {
		return new Builder<>();
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be
	 * implicitly created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> create() {
		return TopicProcessor.<E>builder().build();
	}

	/**
	 * Create a new {@link TopicProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitly created.
	 * @param name processor thread logical name
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(String name) {
		return TopicProcessor.<E>builder().name(name).build();
	}


	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A new Cached
	 * ThreadExecutorPool will be implicitly created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(boolean autoCancel) {
		return TopicProcessor.<E>builder().autoCancel(autoCancel).build();
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(ExecutorService service) {
		return TopicProcessor.<E>builder().executor(service).build();
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(ExecutorService service,
	                                                boolean autoCancel) {
		return TopicProcessor.<E>builder().executor(service).autoCancel(autoCancel).build();
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A new Cached
	 * ThreadExecutorPool will be implicitly created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return the fresh TopicProcessor instance
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(String name, int bufferSize) {
		return TopicProcessor.<E>builder().name(name).bufferSize(bufferSize).build();
	}

	/**
	 * Create a new TopicProcessor using the blockingWait Strategy, passed backlog
	 * size, and auto-cancel settings. <p> The passed {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(String name, int bufferSize,
	                                                boolean autoCancel) {
		return TopicProcessor.<E>builder().name(name).bufferSize(bufferSize).autoCancel(autoCancel).build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, blockingWait Strategy
	 * and will auto-cancel. <p> The passed {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(ExecutorService service,
	                                                int bufferSize) {
		return TopicProcessor.<E>builder().executor(service).bufferSize(bufferSize).build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, blockingWait Strategy
	 * and the auto-cancel argument. <p> The passed {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(ExecutorService service,
	                                                int bufferSize, boolean autoCancel) {
		return TopicProcessor.<E>builder().executor(service).bufferSize(bufferSize).autoCancel(autoCancel).build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and will
	 * auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitly created and
	 * will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(String name, int bufferSize,
	                                                WaitStrategy strategy) {
		return TopicProcessor.<E>builder().name(name).bufferSize(bufferSize).waitStrategy(strategy).build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy, signal
	 * supplier. The created processor is not shared and will auto-cancel. <p> A new
	 * Cached ThreadExecutorPool will be implicitly created and will use the passed name
	 * to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param signalSupplier A supplier of dispatched signals to preallocate in the ring
	 * buffer
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(String name, int bufferSize,
	                                                WaitStrategy strategy,
	                                                Supplier<E> signalSupplier) {
		return TopicProcessor.<E>builder()
				.name(name)
				.bufferSize(bufferSize)
				.waitStrategy(strategy)
				.signalSupplier(signalSupplier)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> A new Cached ThreadExecutorPool will be implicitly
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(String name, int bufferSize,
	                                                WaitStrategy strategy,
	                                                boolean autoCancel) {
		return TopicProcessor.<E>builder()
				.name(name)
				.bufferSize(bufferSize)
				.waitStrategy(strategy)
				.autoCancel(autoCancel)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and will
	 * auto-cancel. <p> The passed {@link ExecutorService} will
	 * execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(ExecutorService service,
	                                                int bufferSize,
	                                                WaitStrategy strategy) {
		return TopicProcessor.<E>builder()
				.executor(service)
				.bufferSize(bufferSize)
				.waitStrategy(strategy)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> The passed {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(ExecutorService service,
	                                                int bufferSize, WaitStrategy strategy,
	                                                boolean autoCancel) {
		return TopicProcessor.<E>builder()
				.executor(service)
				.bufferSize(bufferSize)
				.waitStrategy(strategy)
				.autoCancel(autoCancel)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> The passed {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * <p> An additional {@code requestTaskExecutor} {@link ExecutorService} is also used
	 * internally on each subscription.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param requestTaskExecutor A provided ExecutorService to manage threading infrastructure.
	 * Should be capable of executing several runnables in parallel (eg. cached thread pool)
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> create(ExecutorService service, ExecutorService requestTaskExecutor,
	                                                int bufferSize, WaitStrategy strategy,
	                                                boolean autoCancel) {
		return TopicProcessor.<E>builder()
				.executor(service)
				.requestTaskExecutor(requestTaskExecutor)
				.bufferSize(bufferSize)
				.waitStrategy(strategy)
				.autoCancel(autoCancel)
				.build();
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitly created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(boolean autoCancel) {
		return TopicProcessor.<E>builder().share(true).autoCancel(autoCancel).build();
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent
	 * onNext calls and is suited for multi-threaded publisher that will fan-in data. <p>
	 * The passed {@link ExecutorService} will execute as many
	 * event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(ExecutorService service) {
		return TopicProcessor.<E>builder().share(true).executor(service).build();
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> The passed {@link ExecutorService} will
	 * execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(ExecutorService service,
	                                               boolean autoCancel) {
		return TopicProcessor.<E>builder().share(true).executor(service).autoCancel(autoCancel).build();
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitly created
	 * and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(String name, int bufferSize) {
		return TopicProcessor.<E>builder().share(true).name(name).bufferSize(bufferSize).build();
	}

	/**
	 * Create a new TopicProcessor using the blockingWait Strategy, passed backlog
	 * size, and auto-cancel settings. <p> A Shared Processor authorizes concurrent onNext
	 * calls and is suited for multi-threaded publisher that will fan-in data. <p> The
	 * passed {@link ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(String name, int bufferSize,
	                                               boolean autoCancel) {
		return TopicProcessor.<E>builder().share(true)
				.name(name)
				.bufferSize(bufferSize)
				.autoCancel(autoCancel)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, blockingWait Strategy
	 * and will auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls and
	 * is suited for multi-threaded publisher that will fan-in data. <p> The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(ExecutorService service,
	                                               int bufferSize) {
		return TopicProcessor.<E>builder().share(true)
				.executor(service)
				.bufferSize(bufferSize)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, blockingWait Strategy
	 * and the auto-cancel argument. <p> A Shared Processor authorizes concurrent onNext
	 * calls and is suited for multi-threaded publisher that will fan-in data. <p> The
	 * passed {@link ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(ExecutorService service,
	                                               int bufferSize, boolean autoCancel) {
		return TopicProcessor.<E>builder().share(true)
				.executor(service)
				.bufferSize(bufferSize)
				.autoCancel(autoCancel)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and will
	 * auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> A new Cached
	 * ThreadExecutorPool will be implicitly created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(String name, int bufferSize,
	                                               WaitStrategy strategy) {
		return TopicProcessor.<E>builder().share(true)
				.name(name)
				.bufferSize(bufferSize)
				.waitStrategy(strategy)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * signal supplier. The created processor will auto-cancel and is shared. <p> A Shared
	 * Processor authorizes concurrent onNext calls and is suited for multi-threaded
	 * publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitly created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param signalSupplier A supplier of dispatched signals to preallocate in the ring
	 * buffer
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(String name, int bufferSize,
	                                               Supplier<E> signalSupplier) {
		return TopicProcessor.<E>builder().share(true)
				.name(name)
				.bufferSize(bufferSize)
				.signalSupplier(signalSupplier)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * signal supplier. The created processor will auto-cancel and is shared. <p> A Shared
	 * Processor authorizes concurrent onNext calls and is suited for multi-threaded
	 * publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitly created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param waitStrategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * buffer
	 * @param <E> Type of processed signals
	 * @param signalSupplier the supplier of signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(String name, int bufferSize,
			WaitStrategy waitStrategy,
			Supplier<E> signalSupplier) {
		return TopicProcessor.<E>builder().share(true)
				.name(name)
				.bufferSize(bufferSize)
				.waitStrategy(waitStrategy)
				.signalSupplier(signalSupplier)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> A Shared Processor authorizes concurrent onNext calls and
	 * is suited for multi-threaded publisher that will fan-in data. <p> A new Cached
	 * ThreadExecutorPool will be implicitly created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(String name, int bufferSize,
	                                               WaitStrategy strategy,
	                                               boolean autoCancel) {
		return TopicProcessor.<E>builder().share(true)
				.name(name)
				.bufferSize(bufferSize)
				.waitStrategy(strategy)
				.autoCancel(autoCancel)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and will
	 * auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(ExecutorService service,
	                                               int bufferSize,
	                                               WaitStrategy strategy) {
		return TopicProcessor.<E>builder().share(true)
				.executor(service)
				.bufferSize(bufferSize)
				.waitStrategy(strategy)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> A Shared Processor authorizes concurrent onNext calls and
	 * is suited for multi-threaded publisher that will fan-in data. <p> The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(ExecutorService service,
	                                               int bufferSize, WaitStrategy strategy,
	                                               boolean autoCancel) {
		return TopicProcessor.<E>builder().share(true)
				.executor(service)
				.bufferSize(bufferSize)
				.waitStrategy(strategy)
				.autoCancel(autoCancel)
				.build();
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> A Shared Processor authorizes concurrent onNext calls and
	 * is suited for multi-threaded publisher that will fan-in data. <p> The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * <p> An additional {@code requestTaskExecutor} {@link ExecutorService} is also used
	 * internally on each subscription.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param requestTaskExecutor A provided ExecutorService to manage threading infrastructure.
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> TopicProcessor<E> share(ExecutorService service,
			ExecutorService requestTaskExecutor,
			int bufferSize, WaitStrategy strategy,
			boolean autoCancel) {
		return TopicProcessor.<E>builder().share(true)
				.executor(service)
				.requestTaskExecutor(requestTaskExecutor)
				.bufferSize(bufferSize)
				.waitStrategy(strategy)
				.autoCancel(autoCancel)
				.build();
	}

	final RingBuffer.Reader barrier;

	final RingBuffer.Sequence minimum;

	final ExecutorService requestTaskExecutor;

	TopicProcessor(String name,
			int bufferSize,
			WaitStrategy waitStrategy,
			boolean shared,
			boolean autoCancel,
			@Nullable final Supplier<E> signalSupplier) {
		this(new EventLoopFactory(name, autoCancel),
				null,
				bufferSize,
				waitStrategy,
				shared,
				autoCancel,
				signalSupplier);
	}

	TopicProcessor(
			@Nullable ThreadFactory threadFactory,
			@Nullable ExecutorService executor,
			int bufferSize,
			WaitStrategy waitStrategy,
			boolean shared,
			boolean autoCancel,
			@Nullable final Supplier<E> signalSupplier) {
		this(threadFactory, executor,
				defaultRequestTaskExecutor(defaultName(threadFactory, TopicProcessor.class)),
				bufferSize, waitStrategy, shared, autoCancel, signalSupplier);
	}

	TopicProcessor(
			@Nullable ThreadFactory threadFactory,
			@Nullable ExecutorService executor,
			ExecutorService requestTaskExecutor,
			int bufferSize,
			WaitStrategy waitStrategy,
			boolean shared,
			boolean autoCancel,
			@Nullable final Supplier<E> signalSupplier) {
		super(bufferSize, threadFactory, executor, autoCancel, shared, () -> {
			Slot<E> signal = new Slot<>();
			if (signalSupplier != null) {
				signal.value = signalSupplier.get();
			}
			return signal;
		}, waitStrategy);

		Objects.requireNonNull(requestTaskExecutor, "requestTaskExecutor");

		this.minimum = RingBuffer.newSequence(-1);
		this.barrier = ringBuffer.newReader();
		this.requestTaskExecutor = requestTaskExecutor;
	}

	@Override
	public void subscribe(final Subscriber<? super E> subscriber, Context ctx) {
		//noinspection ConstantConditions
		if (subscriber == null) {
			throw Exceptions.argumentIsNullException();
		}

		if (!alive()) {
			coldSource(ringBuffer, null, error, minimum).subscribe(subscriber);
			return;
		}

		//create a unique eventProcessor for this subscriber
		final RingBuffer.Sequence pendingRequest = RingBuffer.newSequence(0);
		final TopicInner<E> signalProcessor =
				new TopicInner<>(this, pendingRequest, subscriber);

		//bind eventProcessor sequence to observe the ringBuffer

		//if only active subscriber, replay missed data
		if (incrementSubscribers()) {

			signalProcessor.sequence.set(minimum.getAsLong());
			ringBuffer.addGatingSequence(signalProcessor.sequence);
			//set eventProcessor sequence to minimum index (replay)
		}
		else {
			//otherwise only listen to new data
			//set eventProcessor sequence to ringbuffer index
			signalProcessor.sequence.set(ringBuffer.getCursor());
			ringBuffer.addGatingSequence(signalProcessor.sequence);


		}

		try {
			//start the subscriber thread
			executor.execute(signalProcessor);

		}
		catch (Throwable t) {
			ringBuffer.removeGatingSequence(signalProcessor.sequence);
			decrementSubscribers();
			if (!alive() && RejectedExecutionException.class.isAssignableFrom(t.getClass())){
				coldSource(ringBuffer, t, error, minimum).subscribe(subscriber);
			}
			else{
				Operators.error(subscriber, t);
			}
		}
	}

	@Override
	public Flux<E> drain() {
		return coldSource(ringBuffer, null, error, minimum);
	}

	@Override
	protected void doError(Throwable t) {
		barrier.signal();
		//ringBuffer.markAsTerminated();

	}

	@Override
	protected void doComplete() {
		barrier.signal();
		//ringBuffer.markAsTerminated();
	}

	@Override
	public long getPending() {
		return ringBuffer.getPending();
	}

	@Override
	protected void requestTask(Subscription s) {
		minimum.set(ringBuffer.getCursor());
		ringBuffer.addGatingSequence(minimum);
		requestTaskExecutor.execute(
				EventLoopProcessor.createRequestTask(s, () -> {
					             if (!alive()) {
						             WaitStrategy.alert();
					             }
				             }, minimum::set, () -> SUBSCRIBER_COUNT.get(TopicProcessor.this) == 0 ?
								minimum.getAsLong() :
						ringBuffer.getMinimumGatingSequence(minimum),
				readWait,
				this,
				ringBuffer.bufferSize()));
	}

	@Override
	public void run() {
		if (!alive() && SUBSCRIBER_COUNT.get(TopicProcessor.this) == 0) {
			WaitStrategy.alert();
		}
	}

	@Override
	protected void specificShutdown() {
		requestTaskExecutor.shutdown();
	}

	/**
	 * Disruptor BatchEventProcessor port that deals with pending demand. <p> Convenience
	 * class for handling the batching semantics of consuming entries from a {@link
	 * reactor.core.publisher .rb.disruptor .RingBuffer}. <p>
	 * @param <T> event implementation storing the data for sharing during exchange or
	 * parallel coordination of an event.
	 */
	final static class TopicInner<T>
			implements Runnable, InnerProducer<T> {

		final AtomicBoolean running = new AtomicBoolean(true);

		final RingBuffer.Sequence sequence = RingBuffer.newSequence(RingBuffer.INITIAL_CURSOR_VALUE);

		final TopicProcessor<T> processor;

		final RingBuffer.Sequence pendingRequest;

		final Subscriber<? super T> subscriber;

		final Runnable waiter = new Runnable() {
			@Override
			public void run() {
				if (!running.get() || processor.isTerminated()) {
					WaitStrategy.alert();
				}
			}
		};

		/**
		 * Construct a ringbuffer consumer that will automatically track the progress by
		 * updating its sequence
		 *
		 * @param processor the target processor
		 * @param pendingRequest holder for the number of pending requests
		 * @param subscriber the output Subscriber instance
		 */
		TopicInner(TopicProcessor<T> processor,
		                            RingBuffer.Sequence pendingRequest,
		                            Subscriber<? super T> subscriber) {
			this.processor = processor;
			this.pendingRequest = pendingRequest;
			this.subscriber = subscriber;
		}

		void halt() {
			running.set(false);
			processor.barrier.alert();
		}

		/**
		 * It is ok to have another thread rerun this method after a halt().
		 */
		@Override
		public void run() {
			try {
				Thread.currentThread()
				      .setContextClassLoader(processor.contextClassLoader);
				subscriber.onSubscribe(this);

				if (!EventLoopProcessor
						.waitRequestOrTerminalEvent(pendingRequest, processor.barrier, running, sequence, waiter)) {
					if(!running.get()){
						return;
					}
					if(processor.terminated == SHUTDOWN) {
						if (processor.ringBuffer.getAsLong() == -1L) {
							if (processor.error != null) {
								subscriber.onError(processor.error);
								return;
							}
							subscriber.onComplete();
							return;
						}
					}
					else if (processor.terminated == FORCED_SHUTDOWN) {
						return;
					}
				}

				Slot<T> event;
				long nextSequence = sequence.getAsLong() + 1L;
				final boolean unbounded = pendingRequest.getAsLong() == Long.MAX_VALUE;

				while (true) {
					try {

						final long availableSequence = processor.barrier.waitFor(nextSequence, waiter);
						while (nextSequence <= availableSequence) {
							event = processor.ringBuffer.get(nextSequence);

								//if bounded and out of capacity
								while (!unbounded && getAndSub(pendingRequest, 1L) ==
												0) {
									//Todo Use WaitStrategy?
									if(!running.get() || processor.isTerminated()){
										WaitStrategy.alert();
									}
									LockSupport.parkNanos(1L);
								}

								//It's an unbounded subscriber or there is enough capacity to process the signal
								subscriber.onNext(event.value);
								nextSequence++;

						}
						sequence.set(availableSequence);

						if (Operators.emptySubscription() !=
								processor.upstreamSubscription) {
							processor.readWait.signalAllWhenBlocking();
						}
					}
					catch (Throwable ex) {
						if(WaitStrategy.isAlert(ex) || Exceptions.isCancel(ex)) {

							if (!running.get()) {
								break;
							}
							else {
								if (processor.terminated == SHUTDOWN) {
									if (processor.error != null) {
										subscriber.onError(processor.error);
										break;
									}
									if (nextSequence > processor.ringBuffer.getAsLong()) {
										subscriber.onComplete();
										break;
									}

									LockSupport.parkNanos(1L);
								}
								else if (processor.terminated == FORCED_SHUTDOWN) {
									break;
								}
								processor.barrier.clearAlert();
							}
						}
						else {
							throw Exceptions.propagate(ex);
						}
					}
				}
			}
			finally {
				processor.ringBuffer.removeGatingSequence(sequence);
				processor.decrementSubscribers();
				running.set(false);
				processor.readWait.signalAllWhenBlocking();
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return processor;
			if (key == IntAttr.PREFETCH) return Integer.MAX_VALUE;
			if (key == BooleanAttr.TERMINATED) return processor.isTerminated();
			if (key == BooleanAttr.CANCELLED) return !running.get();
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return pendingRequest.getAsLong();
			if (key == LongAttr.LARGE_BUFFERED) {
				return processor.ringBuffer.getCursor() - sequence.getAsLong();
			}
			if (key == IntAttr.BUFFERED) {
				long realBuffered = processor.ringBuffer.getCursor() - sequence.getAsLong();
				if (realBuffered <= Integer.MAX_VALUE) {
					return (int) realBuffered;
				}
				return Integer.MIN_VALUE;
			}

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public Subscriber<? super T> actual() {
			return subscriber;
		}

		@Override
		public void request(long n) {
			if (Operators.checkRequest(n, subscriber)) {
				if (!running.get()) {
					return;
				}

				addCap(pendingRequest, n);
			}
		}

		@Override
		public void cancel() {
			halt();
		}
	}
}
