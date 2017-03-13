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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.concurrent.WaitStrategy;

/**
 ** An implementation of a RingBuffer backed message-passing Processor implementing work-queue distribution with
 * async event loops.
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.5.RELEASE/src/docs/marble/workqueue.png" alt="">
 *     <p>
 *         Created from {@link #share}, the {@link WorkQueueProcessor} will authorize concurrent publishing
 *         (multi-producer) from its receiving side {@link Subscriber#onNext(Object)}.
 *         {@link WorkQueueProcessor} is able to replay up to its buffer size number of failed signals (either
 *         dropped or
 *         fatally throwing on child {@link Subscriber#onNext}).
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.5.RELEASE/src/docs/marble/workqueuef.png" alt="">
 * <p>
 *     The
 * processor is very similar to {@link TopicProcessor} but
 * only partially respects the Reactive Streams contract. <p> The purpose of this
 * processor is to distribute the signals to only one of the subscribed subscribers and to
 * share the demand amongst all subscribers. The scenario is akin to Executor or
 * Round-Robin distribution. However there is no guarantee the distribution will be
 * respecting a round-robin distribution all the time. <p> The core use for this component
 * is to scale up easily without suffering the overhead of an Executor and without using
 * dedicated queues by subscriber, which is less used memory, less GC, more win.
 * @param <E> Type of dispatched signal
 * @author Stephane Maldini
 */
public final class WorkQueueProcessor<E> extends EventLoopProcessor<E> {

	/**
	 * Create a new WorkQueueProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be
	 * implicitly created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create() {
		return create(WorkQueueProcessor.class.getSimpleName(),
				QueueSupplier.SMALL_BUFFER_SIZE,
				null, true);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A new Cached
	 * ThreadExecutorPool will be implicitly created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(boolean autoCancel) {
		return create(WorkQueueProcessor.class.getSimpleName(),
				QueueSupplier.SMALL_BUFFER_SIZE,
				null, autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(ExecutorService service) {
		return create(service, QueueSupplier.SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(ExecutorService service,
			boolean autoCancel) {
		return create(service, QueueSupplier.SMALL_BUFFER_SIZE, null,
				autoCancel);
	}

	/**
	 * Create a new TopicProcessor using the default buffer size 32, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitly
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(String name) {
		return create(name, QueueSupplier.SMALL_BUFFER_SIZE);
	}

	/**
	 * Create a new TopicProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitly
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(String name, int bufferSize) {
		return create(name, bufferSize, null, true);
	}

	/**
	 * Create a new TopicProcessor using the passed buffer size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A new Cached ThreadExecutorPool
	 * will be implicitly created and will use the passed name to qualify the created
	 * threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(String name, int bufferSize,
			boolean autoCancel) {
		return create(name, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new TopicProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> The passed {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(ExecutorService service,
			int bufferSize) {
		return create(service, bufferSize, null, true);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> The passed {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(ExecutorService service,
			int bufferSize, boolean autoCancel) {
		return create(service, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitly
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(String name, int bufferSize,
			WaitStrategy strategy) {
		return create(name, bufferSize, strategy, true);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel settings. <p> A new Cached ThreadExecutorPool will be
	 * implicitly created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(String name, int bufferSize,
			WaitStrategy strategy, boolean autoCancel) {
		return new WorkQueueProcessor<>(name,
				bufferSize,
				strategy == null ? WaitStrategy.liteBlocking() : strategy,
				false,
				autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size and blockingWait
	 * Strategy settings but will auto-cancel. <p> The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(ExecutorService executor,
			int bufferSize, WaitStrategy strategy) {
		return create(executor, bufferSize, strategy, true);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size, wait strategy
	 * and auto-cancel settings. <p> The passed {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(ExecutorService executor,
			int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new WorkQueueProcessor<>(null,
				executor,
				bufferSize,
				strategy == null ? WaitStrategy.liteBlocking() : strategy,
				false,
				autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size, wait strategy
	 * and auto-cancel settings. <p> The passed {@code executor} {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * <p> An additional {@code requestTaskExecutor} {@link ExecutorService} is also used
	 * internally on each subscription.
	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param requestTaskExecutor A provided ExecutorService to manage threading infrastructure.
	 * Should be capable of executing several runnables in parallel (eg. cached thread pool)
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(ExecutorService executor,
			ExecutorService requestTaskExecutor,
			int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new WorkQueueProcessor<>(null,
				executor, requestTaskExecutor,
				bufferSize,
				strategy == null ? WaitStrategy.liteBlocking() : strategy,
				false,
				autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitly created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(boolean autoCancel) {
		return share(WorkQueueProcessor.class.getSimpleName(),
				QueueSupplier.SMALL_BUFFER_SIZE,
				null, autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(ExecutorService service) {
		return share(service, QueueSupplier.SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> The passed {@link ExecutorService} will
	 * execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(ExecutorService service,
			boolean autoCancel) {
		return share(service, QueueSupplier.SMALL_BUFFER_SIZE, null,
				autoCancel);
	}

	/**
	 * Create a new TopicProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> A new Cached
	 * ThreadExecutorPool will be implicitly created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(String name, int bufferSize) {
		return share(name, bufferSize, null, true);
	}

	/**
	 * Create a new TopicProcessor using the passed buffer size, blockingWait
	 * Strategy and the passed auto-cancel setting. <p> A Shared Processor authorizes
	 * concurrent onNext calls and is suited for multi-threaded publisher that will fan-in
	 * data. <p> A new Cached ThreadExecutorPool will be implicitly created and will use
	 * the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(String name, int bufferSize,
			boolean autoCancel) {
		return share(name, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new TopicProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> The passed
	 * {@link ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(ExecutorService service,
			int bufferSize) {
		return share(service, bufferSize, null, true);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> The passed
	 * {@link ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(ExecutorService service,
			int bufferSize, boolean autoCancel) {
		return share(service, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> A new Cached
	 * ThreadExecutorPool will be implicitly created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(String name, int bufferSize,
			WaitStrategy strategy) {
		return share(name, bufferSize, strategy, true);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel settings. <p> A Shared Processor authorizes concurrent
	 * onNext calls and is suited for multi-threaded publisher that will fan-in data. <p>
	 * A new Cached ThreadExecutorPool will be implicitly created and will use the passed
	 * name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(String name, int bufferSize,
			WaitStrategy strategy, boolean autoCancel) {
		return new WorkQueueProcessor<>(name,
				bufferSize,
				strategy == null ? WaitStrategy.liteBlocking() : strategy,
				true,
				autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size and blockingWait
	 * Strategy settings but will auto-cancel. <p> A Shared Processor authorizes
	 * concurrent onNext calls and is suited for multi-threaded publisher that will fan-in
	 * data. <p> The passed {@link ExecutorService} will execute as
	 * many event-loop consuming the ringbuffer as subscribers.
	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(ExecutorService executor,
			int bufferSize, WaitStrategy strategy) {
		return share(executor, bufferSize, strategy, true);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size, wait strategy
	 * and auto-cancel settings. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> The passed
	 * {@link ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(ExecutorService executor,
			int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new WorkQueueProcessor<>(null,
				executor,
				bufferSize,
				strategy == null ? WaitStrategy.liteBlocking() : strategy,
				true,
				autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size, wait strategy
	 * and auto-cancel settings. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> The passed
	 * {@link ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * <p> An additional {@code requestTaskExecutor} {@link ExecutorService} is also used
	 * internally on each subscription.
	 * @param executor A provided ExecutorService to manage threading infrastructure
	 * @param requestTaskExecutor A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * smart blocking wait strategy.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(ExecutorService executor,
			ExecutorService requestTaskExecutor,
			int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new WorkQueueProcessor<>(null,
				executor,
				requestTaskExecutor,
				bufferSize,
				strategy == null ? WaitStrategy.liteBlocking() : strategy,
				true,
				autoCancel);
	}

	@SuppressWarnings("rawtypes")
    static final Supplier FACTORY = (Supplier<Slot>) Slot::new;

	/**
	 * Instance
	 */

	final RingBuffer.Sequence workSequence =
			RingBuffer.newSequence(RingBuffer.INITIAL_CURSOR_VALUE);

	final Queue<Object> claimedDisposed = new ConcurrentLinkedQueue<>();

	final ExecutorService requestTaskExecutor;

	final WaitStrategy writeWait;

	volatile int replaying;

	@SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<WorkQueueProcessor> REPLAYING =
			AtomicIntegerFieldUpdater
					.newUpdater(WorkQueueProcessor.class, "replaying");

	WorkQueueProcessor(String name,
			int bufferSize,
			WaitStrategy waitStrategy,
			boolean share,
			boolean autoCancel) {
		this(new EventLoopFactory(name, autoCancel),
				null,
				bufferSize,
				waitStrategy,
				share,
				autoCancel);
	}

	@SuppressWarnings("unchecked")
	WorkQueueProcessor(ThreadFactory threadFactory,
			ExecutorService executor,
			int bufferSize, WaitStrategy waitStrategy, boolean share,
	                                boolean autoCancel) {
		this(threadFactory, executor, defaultRequestTaskExecutor(defaultName(threadFactory, WorkQueueProcessor.class)),
				bufferSize, waitStrategy, share, autoCancel);
	}

	@SuppressWarnings("unchecked")
	WorkQueueProcessor(ThreadFactory threadFactory,
			ExecutorService executor, ExecutorService requestTaskExecutor,
			int bufferSize, WaitStrategy waitStrategy, boolean share,
	                                boolean autoCancel) {
		super(bufferSize, threadFactory,
				executor,
				autoCancel,
				share,
				FACTORY,
				waitStrategy);

		Objects.requireNonNull(requestTaskExecutor, "requestTaskExecutor");

		this.writeWait = waitStrategy;

		ringBuffer.addGatingSequence(workSequence);
		this.requestTaskExecutor = requestTaskExecutor;
	}

	@Override
	public void subscribe(final Subscriber<? super E> subscriber) {
		if (subscriber == null) {
			throw Exceptions.argumentIsNullException();
		}

		if (!alive()) {
			TopicProcessor.coldSource(ringBuffer, null, error, workSequence).subscribe(subscriber);
			return;
		}

		final WorkQueueInner<E> signalProcessor =
				new WorkQueueInner<>(subscriber, this);
		try {

			incrementSubscribers();

			//bind eventProcessor sequence to observe the ringBuffer
			signalProcessor.sequence.set(workSequence.getAsLong());
			ringBuffer.addGatingSequence(signalProcessor.sequence);

			//best effort to prevent starting the subscriber thread if we can detect the pool is too small
			int maxSubscribers = bestEffortMaxSubscribers(executor);
			if (maxSubscribers > Integer.MIN_VALUE && subscriberCount > maxSubscribers) {
				throw new IllegalStateException("The executor service could not accommodate" +
						" another subscriber, detected limit " + maxSubscribers);
			}

			executor.execute(signalProcessor);
		}
		catch (Throwable t) {
			decrementSubscribers();
			ringBuffer.removeGatingSequence(signalProcessor.sequence);
			if(RejectedExecutionException.class.isAssignableFrom(t.getClass())){
				TopicProcessor.coldSource(ringBuffer, t, error, workSequence).subscribe(subscriber);
			}
			else {
				Operators.error(subscriber, t);
			}
		}
	}

	/**
	 * This method will attempt to compute the maximum amount of subscribers a
	 * {@link WorkQueueProcessor} can accomodate based on a given {@link ExecutorService}.
	 * <p>
	 * It can only accurately detect this for {@link ThreadPoolExecutor} and
	 * {@link ForkJoinPool} instances, and will return {@link Integer#MIN_VALUE} for other
	 * executor implementations.
	 *
	 * @param executor the executor to attempt to introspect.
	 * @return the maximum number of subscribers the executor can accommodate if it can
	 * be computed, or {@link Integer#MIN_VALUE} if it cannot be determined.
	 */
	static int bestEffortMaxSubscribers(ExecutorService executor) {
		int maxSubscribers = Integer.MIN_VALUE;
		if (executor instanceof ThreadPoolExecutor) {
			maxSubscribers = ((ThreadPoolExecutor) executor).getMaximumPoolSize();
		}
		else if (executor instanceof ForkJoinPool) {
			maxSubscribers = ((ForkJoinPool) executor).getParallelism();
		}
		return maxSubscribers;
	}

	@Override
	public Flux<E> drain() {
		return TopicProcessor.coldSource(ringBuffer, null, error, workSequence);
	}

	@Override
	protected void doError(Throwable t) {
		writeWait.signalAllWhenBlocking();
		//ringBuffer.markAsTerminated();
	}

	@Override
	protected void doComplete() {
		writeWait.signalAllWhenBlocking();
		//ringBuffer.markAsTerminated();
	}

	@Override
	protected void requestTask(Subscription s) {
		requestTaskExecutor.execute(EventLoopProcessor.createRequestTask(s,
				() -> {
					if (!alive()) {
						WaitStrategy.alert();
					}
				}, null,
				ringBuffer::getMinimumGatingSequence,
				readWait, this, ringBuffer.bufferSize()));
	}

	@Override
	public long getPending() {
		return (getBufferSize() - ringBuffer.getPending()) + claimedDisposed.size();
	}

	@Override
	public void run() {
		if (!alive()) {
			WaitStrategy.alert();
		}
	}

	@Override
	protected void specificShutdown() {
		requestTaskExecutor.shutdown();
	}

	/**
	 * Disruptor WorkProcessor port that deals with pending demand. <p> Convenience class
	 * for handling the batching semantics of consuming entries from a {@link
	 * RingBuffer} <p>
	 * @param <T> event implementation storing the data for sharing during exchange or
	 * parallel coordination of an event.
	 */
	final static class WorkQueueInner<T>
			implements Runnable, InnerProducer<T> {

		final AtomicBoolean running = new AtomicBoolean(true);

		final RingBuffer.Sequence
				sequence = RingBuffer.newSequence(RingBuffer.INITIAL_CURSOR_VALUE);

		final RingBuffer.Sequence pendingRequest = RingBuffer.newSequence(0);

		final RingBuffer.Reader barrier;

		final WorkQueueProcessor<T> processor;

		final Subscriber<? super T> subscriber;

		final Runnable waiter = new Runnable() {
			@Override
			public void run() {
				if (barrier.isAlerted() || !isRunning() ||
						replay(pendingRequest.getAsLong() == Long.MAX_VALUE)) {
					WaitStrategy.alert();
				}
			}
		};

		/**
		 * Construct a ringbuffer consumer that will automatically track the progress by
		 * updating its sequence
		 * @param subscriber the output Subscriber instance
		 * @param processor the source processor
		 */
		WorkQueueInner(Subscriber<? super T> subscriber,
				WorkQueueProcessor<T> processor) {
			this.processor = processor;
			this.subscriber = subscriber;

			this.barrier = processor.ringBuffer.newReader();
		}

		void halt() {
			running.set(false);
			barrier.alert();
		}

		boolean isRunning() {
			return running.get() && (processor.terminated == 0 || processor.error == null &&
					processor.ringBuffer.getAsLong() > sequence.getAsLong());
		}


		/**
		 * It is ok to have another thread rerun this method after a halt().
		 */
		@Override
		public void run() {
			long nextSequence;
			boolean processedSequence = true;

			try {

				//while(processor.alive() && processor.upstreamSubscription == null);
				Thread.currentThread()
				      .setContextClassLoader(processor.contextClassLoader);
				subscriber.onSubscribe(this);

				long cachedAvailableSequence = Long.MIN_VALUE;
				nextSequence = sequence.getAsLong();
				Slot<T> event = null;

				if (!EventLoopProcessor.waitRequestOrTerminalEvent(pendingRequest, barrier, running, sequence,
						waiter)) {
					if(!running.get()){
						return;
					}
					if(processor.terminated == 1 && processor.ringBuffer.getAsLong() == -1L) {
						if (processor.error != null) {
							subscriber.onError(processor.error);
							return;
						}
						subscriber.onComplete();
						return;
					}
				}

				final boolean unbounded = pendingRequest.getAsLong() == Long.MAX_VALUE;

				if (replay(unbounded)) {
					running.set(false);
					return;
				}

				while (true) {
					try {
						// if previous sequence was processed - fetch the next sequence and set
						// that we have successfully processed the previous sequence
						// typically, this will be true
						// this prevents the sequence getting too far forward if an error
						// is thrown from the WorkHandler
						if (processedSequence) {
							if(!running.get()){
								break;
							}
							processedSequence = false;
							do {
								nextSequence = processor.workSequence.getAsLong() + 1L;
								while ((!unbounded && pendingRequest.getAsLong() == 0L)) {
									if (!isRunning()) {
										WaitStrategy.alert();
									}
									LockSupport.parkNanos(1L);
								}
								sequence.set(nextSequence - 1L);
							}
							while (!processor.workSequence.compareAndSet(nextSequence - 1L, nextSequence));
						}

						if (cachedAvailableSequence >= nextSequence) {
							event = processor.ringBuffer.get(nextSequence);

							try {
								readNextEvent(unbounded);
							}
							catch (Exception ce) {
								if (!running.get() || !WaitStrategy.isAlert(ce)) {
									throw ce;
								}
								barrier.clearAlert();
							}

							processedSequence = true;
							subscriber.onNext(event.value);


						}
						else {
							processor.readWait.signalAllWhenBlocking();
								cachedAvailableSequence =
										barrier.waitFor(nextSequence, waiter);

						}

					}
					catch (InterruptedException | RuntimeException ce) {
						if (Exceptions.isCancel(ce)){
							reschedule(event);
							break;
						}
						if (!WaitStrategy.isAlert(ce)) {
							throw Exceptions.propagate(ce);
						}

						barrier.clearAlert();
						if (!running.get()) {
							break;
						}
						if(processor.terminated == 1) {
							if (processor.error != null) {
								processedSequence = true;
								subscriber.onError(processor.error);
								break;
							}
							if (processor.ringBuffer.getPending() == 0) {
								processedSequence = true;
								subscriber.onComplete();
								break;
							}
						}
						//processedSequence = true;
						//continue event-loop

					}
				}
			}
			finally {
				processor.decrementSubscribers();
				running.set(false);

				if(!processedSequence) {
					processor.claimedDisposed.add(sequence);
				}
				else{
					processor.ringBuffer.removeGatingSequence(sequence);
				}


				processor.writeWait.signalAllWhenBlocking();
			}
		}

		@SuppressWarnings("unchecked")
		boolean replay(final boolean unbounded) {
			if (REPLAYING.compareAndSet(processor, 0, 1)) {
				try {
					RingBuffer.Sequence s = null;
					for (; ; ) {

						if (!running.get()) {
							processor.readWait.signalAllWhenBlocking();
							return true;
						}

						Object v = processor.claimedDisposed.peek();

						if (v == null) {
							processor.readWait.signalAllWhenBlocking();
							return !processor.alive() && processor.ringBuffer.getPending() == 0;
						}

						if (v instanceof RingBuffer.Sequence) {
							s = (RingBuffer.Sequence) v;
							long cursor = s.getAsLong() + 1L;
							if(cursor > processor.ringBuffer.getAsLong()){
								processor.readWait.signalAllWhenBlocking();
								return !processor.alive() && processor.ringBuffer.getPending() == 0;
							}

							barrier.waitFor(cursor, waiter);

							v = processor.ringBuffer.get(cursor).value;

							if (v == null) {
								processor.ringBuffer.removeGatingSequence(s);
								processor.claimedDisposed.poll();
								s = null;
								continue;
							}
						}

						readNextEvent(unbounded);
						subscriber.onNext((T) v);
						processor.claimedDisposed.poll();
						if(s != null){
							processor.ringBuffer.removeGatingSequence(s);
							s = null;
						}
					}
				}
				catch (RuntimeException ce) {
					if (!running.get() || Exceptions.isCancel(ce)) {
						running.set(false);
						return true;
					}
					throw ce;
				}
				catch (InterruptedException e) {
					running.set(false);
					return true;
				}
				finally {
					REPLAYING.compareAndSet(processor, 1, 0);
				}
			}
			else {
				return !processor.alive() && processor.ringBuffer.getPending() == 0;
			}
		}

		boolean reschedule(Slot<T> event) {
			if (event != null &&
					event.value != null) {
				processor.claimedDisposed.add(event.value);
				barrier.alert();
				processor.readWait.signalAllWhenBlocking();
				return true;
			}
			return false;
		}

		void readNextEvent(final boolean unbounded) {
				//pause until request
			while ((!unbounded && getAndSub(pendingRequest, 1L) == 0L)) {
				if (!isRunning()) {
					WaitStrategy.alert();
				}
				//Todo Use WaitStrategy?
				LockSupport.parkNanos(1L);
			}
		}

		@Override
		public Object scan(Attr key) {
			switch (key){
				case PARENT:
					return processor;
				case PREFETCH:
					return Integer.MAX_VALUE;
				case TERMINATED:
					return processor.isTerminated();
				case CANCELLED:
					return !running.get();
				case REQUESTED_FROM_DOWNSTREAM:
					return pendingRequest.getAsLong();
			}
			return InnerProducer.super.scan(key);
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

	static final Logger log = Loggers.getLogger(WorkQueueProcessor.class);

}
