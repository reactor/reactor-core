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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.util.Exceptions;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.concurrent.RingBuffer;
import reactor.util.concurrent.RingBufferReader;
import reactor.util.concurrent.Sequence;
import reactor.util.concurrent.WaitStrategy;

/**
 ** An implementation of a RingBuffer backed message-passing Processor implementing publish-subscribe with async event
 * loops.
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/topic.png" alt="">
 * <p>
 *  Created from {@link #share}, the {@link TopicProcessor} will authorize concurrent publishing (multi-producer)
 *  from its receiving side {@link Subscriber#onNext(Object)}.
 *  Additionally, any of the {@link TopicProcessor} will stop the event loop thread if an error occurs.
 * <img width="640" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/topics.png" alt="">
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
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> create() {
		return create(TopicProcessor.class.getSimpleName(),
				QueueSupplier.SMALL_BUFFER_SIZE,
				null, true);
	}

	/**
	 * Create a new {@link TopicProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param name processor thread logical name
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> create(String name) {
		return create(name, QueueSupplier.SMALL_BUFFER_SIZE, true);
	}


	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> create(boolean autoCancel) {
		return create(TopicProcessor.class.getSimpleName(),
				QueueSupplier.SMALL_BUFFER_SIZE,
				null, autoCancel);
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> create(ExecutorService service) {
		return create(service, QueueSupplier.SMALL_BUFFER_SIZE, null, true);
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
	 */
	public static <E> TopicProcessor<E> create(ExecutorService service,
	                                                boolean autoCancel) {
		return create(service, QueueSupplier.SMALL_BUFFER_SIZE, null, autoCancel);
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return the fresh TopicProcessor instance
	 */
	public static <E> TopicProcessor<E> create(String name, int bufferSize) {
		return create(name, bufferSize, null, true);
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
	 */
	public static <E> TopicProcessor<E> create(String name, int bufferSize,
	                                                boolean autoCancel) {
		return create(name, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, blockingWait Strategy
	 * and will auto-cancel. <p> The passed {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> create(ExecutorService service,
	                                                int bufferSize) {
		return create(service, bufferSize, WaitStrategy.liteBlocking(), true);
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
	 */
	public static <E> TopicProcessor<E> create(ExecutorService service,
	                                                int bufferSize, boolean autoCancel) {
		return create(service, bufferSize, WaitStrategy.liteBlocking(), autoCancel);
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and will
	 * auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely created and
	 * will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> create(String name, int bufferSize,
	                                                WaitStrategy strategy) {
		return create(name, bufferSize, strategy, null);
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy, signal
	 * supplier. The created processor is not shared and will auto-cancel. <p> A new
	 * Cached ThreadExecutorPool will be implicitely created and will use the passed name
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
	 */
	public static <E> TopicProcessor<E> create(String name, int bufferSize,
	                                                WaitStrategy strategy,
	                                                Supplier<E> signalSupplier) {
		return new TopicProcessor<>(name,
				bufferSize,
				strategy == null ?
						WaitStrategy.phasedOffLiteLock(200, 100, TimeUnit.MILLISECONDS) :
						strategy,
				false,
				true,
				signalSupplier);
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> A new Cached ThreadExecutorPool will be implicitely
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
	 */
	public static <E> TopicProcessor<E> create(String name, int bufferSize,
	                                                WaitStrategy strategy,
	                                                boolean autoCancel) {
		return new TopicProcessor<>(name,
				bufferSize,
				strategy == null ?
						WaitStrategy.phasedOffLiteLock(200, 100, TimeUnit.MILLISECONDS) :
						strategy,
				false,
				autoCancel, null);
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
	 */
	public static <E> TopicProcessor<E> create(ExecutorService service,
	                                                int bufferSize,
	                                                WaitStrategy strategy) {
		return create(service, bufferSize, strategy, true);
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
	 */
	public static <E> TopicProcessor<E> create(ExecutorService service,
	                                                int bufferSize, WaitStrategy strategy,
	                                                boolean autoCancel) {
		return new TopicProcessor<>(null,
				service,
				bufferSize,
				strategy == null ?
						WaitStrategy.phasedOffLiteLock(200, 100, TimeUnit.MILLISECONDS) :
						strategy,
				false,
				autoCancel, null);
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent
	 * onNext calls and is suited for multi-threaded publisher that will fan-in data. <p>
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> share() {
		return share(TopicProcessor.class.getSimpleName(),
				QueueSupplier.SMALL_BUFFER_SIZE,
				null, true);
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> share(boolean autoCancel) {
		return share(TopicProcessor.class.getSimpleName(),
				QueueSupplier.SMALL_BUFFER_SIZE,
				null, autoCancel);
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
	 */
	public static <E> TopicProcessor<E> share(ExecutorService service) {
		return share(service, QueueSupplier.SMALL_BUFFER_SIZE, null, true);
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
	 */
	public static <E> TopicProcessor<E> share(ExecutorService service,
	                                               boolean autoCancel) {
		return share(service, QueueSupplier.SMALL_BUFFER_SIZE, null,
				autoCancel);
	}

	/**
	 * Create a new TopicProcessor using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitely created
	 * and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> share(String name, int bufferSize) {
		return share(name, bufferSize, null, true);
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
	 */
	public static <E> TopicProcessor<E> share(String name, int bufferSize,
	                                               boolean autoCancel) {
		return share(name, bufferSize, null, autoCancel);
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
	 */
	public static <E> TopicProcessor<E> share(ExecutorService service,
	                                               int bufferSize) {
		return share(service, bufferSize, null, true);
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
	 */
	public static <E> TopicProcessor<E> share(ExecutorService service,
	                                               int bufferSize, boolean autoCancel) {
		return share(service, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and will
	 * auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls and is
	 * suited for multi-threaded publisher that will fan-in data. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created and will use the passed name to
	 * qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param strategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> share(String name, int bufferSize,
	                                               WaitStrategy strategy) {
		return share(name, bufferSize, strategy, null);
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * signal supplier. The created processor will auto-cancel and is shared. <p> A Shared
	 * Processor authorizes concurrent onNext calls and is suited for multi-threaded
	 * publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param signalSupplier A supplier of dispatched signals to preallocate in the ring
	 * buffer
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> share(String name, int bufferSize,
	                                               Supplier<E> signalSupplier) {
		return share(name, bufferSize, null, signalSupplier);
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * signal supplier. The created processor will auto-cancel and is shared. <p> A Shared
	 * Processor authorizes concurrent onNext calls and is suited for multi-threaded
	 * publisher that will fan-in data. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param waitStrategy A RingBuffer WaitStrategy to use instead of the default
	 * blocking wait strategy.
	 * buffer
	 * @param <E> Type of processed signals
	 * @param signalSupplier the supplier of signals
	 * @return a fresh processor
	 */
	public static <E> TopicProcessor<E> share(String name, int bufferSize,
			WaitStrategy waitStrategy,
			Supplier<E> signalSupplier) {
		return new TopicProcessor<>(name, bufferSize,
				waitStrategy == null ?
						WaitStrategy.phasedOffLiteLock(200, 100, TimeUnit.MILLISECONDS) :
						waitStrategy,
				true,
				true,
				signalSupplier);
	}

	/**
	 * Create a new TopicProcessor using passed backlog size, wait strategy and
	 * auto-cancel settings. <p> A Shared Processor authorizes concurrent onNext calls and
	 * is suited for multi-threaded publisher that will fan-in data. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created and will use the passed name to
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
	 */
	public static <E> TopicProcessor<E> share(String name, int bufferSize,
	                                               WaitStrategy strategy,
	                                               boolean autoCancel) {
		return new TopicProcessor<>(name,
				bufferSize,
				strategy == null ?
						WaitStrategy.phasedOffLiteLock(200, 100, TimeUnit.MILLISECONDS) :
						strategy,
				true,
				autoCancel, null);
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
	 */
	public static <E> TopicProcessor<E> share(ExecutorService service,
	                                               int bufferSize,
	                                               WaitStrategy strategy) {
		return share(service, bufferSize, strategy, true);
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
	 */
	public static <E> TopicProcessor<E> share(ExecutorService service,
	                                               int bufferSize, WaitStrategy strategy,
	                                               boolean autoCancel) {
		return new TopicProcessor<>(null,
				service,
				bufferSize,
				strategy == null ?
						WaitStrategy.phasedOffLiteLock(200, 100, TimeUnit.MILLISECONDS) :
						strategy,
				true,
				autoCancel, null);
	}

	final RingBufferReader barrier;

	final Sequence minimum;

	TopicProcessor(String name, int bufferSize,
	                            WaitStrategy waitStrategy, boolean shared,
	                            boolean autoCancel, final Supplier<E> signalSupplier) {
		this(new EventLoopFactory(name, autoCancel),
				null,
				bufferSize,
				waitStrategy,
				shared,
				autoCancel,
				signalSupplier);
	}

	TopicProcessor(ThreadFactory threadFactory,
			ExecutorService executor,
			int bufferSize,
			WaitStrategy waitStrategy,
			boolean shared,
			boolean autoCancel,
			final Supplier<E> signalSupplier) {
		super(bufferSize, threadFactory, executor, autoCancel, shared, () -> {
			Slot<E> signal = new Slot<>();
			if (signalSupplier != null) {
				signal.value = signalSupplier.get();
			}
			return signal;
		}, waitStrategy);

		this.minimum = RingBuffer.newSequence(-1);
		this.barrier = ringBuffer.newReader();
	}

	@Override
	public void subscribe(final Subscriber<? super E> subscriber) {
		super.subscribe(subscriber);

		if (!alive()) {
			coldSource(ringBuffer, null, error, minimum).subscribe(subscriber);
			return;
		}

		//create a unique eventProcessor for this subscriber
		final Sequence pendingRequest = RingBuffer.newSequence(0);
		final TopicSubscriberLoop<E> signalProcessor =
				new TopicSubscriberLoop<>(this, pendingRequest, subscriber);

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
			ringBuffer.removeGatingSequence(signalProcessor.getSequence());
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
		new Thread(EventLoopProcessor.createRequestTask(s, () -> {
					             if (!alive()) {
						             if(cancelled){
							             throw Exceptions.CancelException.INSTANCE;
						             }
						             else {
							             RingBuffer.throwAlert();
						             }
					             }
				             }, minimum::set, () -> SUBSCRIBER_COUNT.get(TopicProcessor.this) == 0 ?
								minimum.getAsLong() :
						ringBuffer.getMinimumGatingSequence(minimum),
				readWait,
				this,
				ringBuffer.bufferSize()),
				name+"[request-task]").start();
	}

	@Override
	public long downstreamCount() {
		return ringBuffer.getSequenceReceivers().length - (isStarted() ? 1 : 0);
	}

	@Override
	public void run() {
		if (!alive() && SUBSCRIBER_COUNT.get(TopicProcessor.this) == 0) {
			RingBuffer.throwAlert();
		}
	}

	/**
	 * Disruptor BatchEventProcessor port that deals with pending demand. <p> Convenience
	 * class for handling the batching semantics of consuming entries from a {@link
	 * reactor.core.publisher .rb.disruptor .RingBuffer}. <p>
	 * @param <T> event implementation storing the data for sharing during exchange or
	 * parallel coordination of an event.
	 */
	private final static class TopicSubscriberLoop<T>
			implements Runnable, Producer, Receiver, Trackable, Subscription {

		private final AtomicBoolean running = new AtomicBoolean(false);

		private final Sequence sequence =
				wrap(RingBuffer.INITIAL_CURSOR_VALUE, this);

		private final TopicProcessor<T> processor;

		private final Sequence pendingRequest;

		private final Subscriber<? super T> subscriber;

		private final Runnable waiter = new Runnable() {
			@Override
			public void run() {
				if (!running.get() || processor.isTerminated()) {
					RingBuffer.throwAlert();
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
		public TopicSubscriberLoop(TopicProcessor<T> processor,
		                            Sequence pendingRequest,
		                            Subscriber<? super T> subscriber) {
			this.processor = processor;
			this.pendingRequest = pendingRequest;
			this.subscriber = subscriber;
		}

		public Sequence getSequence() {
			return sequence;
		}

		public void halt() {
			running.set(false);
			processor.barrier.alert();
		}

		/**
		 * It is ok to have another thread rerun this method after a halt().
		 */
		@Override
		public void run() {
			try {
				if (!running.compareAndSet(false, true)) {
					Operators.error(subscriber, new IllegalStateException("Thread is already running"));
					return;
				}

				if(!processor.startSubscriber(subscriber, this)){
					return;
				}

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

				Slot<T> event = null;
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
										RingBuffer.throwAlert();
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

					catch (final InterruptedException ex) {
						Thread.currentThread().interrupt();
						break;
					}
					catch (Throwable ex) {
						if(RingBuffer.isAlert(ex) ||
								ex instanceof Exceptions.CancelException) {

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
							continue;
						}
						Exceptions.throwIfFatal(ex);
						subscriber.onError(ex);
						sequence.set(nextSequence);
						nextSequence++;
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
		public boolean isCancelled() {
			return !running.get();
		}

		@Override
		public boolean isStarted() {
			return sequence.getAsLong() != -1L;
		}

		@Override
		public boolean isTerminated() {
			return !running.get();
		}

		@Override
		public long requestedFromDownstream() {
			return pendingRequest.getAsLong();
		}

		@Override
		public long getPending() {
			return processor.ringBuffer.getCursor() - sequence.getAsLong();
		}

		@Override
		public long getCapacity() {
			return processor.getCapacity();
		}

		@Override
		public Object downstream() {
			return subscriber;
		}

		@Override
		public Object upstream() {
			return processor;
		}

		@Override
		public void request(long n) {
			if (Operators.checkRequest(n, subscriber)) {
				if (!running.get()) {
					return;
				}

				getAndAddCap(pendingRequest, n);
			}
		}

		@Override
		public void cancel() {
			halt();
		}
	}

}
