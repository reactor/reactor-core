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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.MultiProducer;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.queue.RingBuffer;
import reactor.core.queue.RingBufferReceiver;
import reactor.core.queue.Slot;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.state.Completable;
import reactor.core.state.Introspectable;
import reactor.core.state.Requestable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.ExecutorUtils;
import reactor.core.util.PlatformDependent;
import reactor.core.util.Sequence;
import reactor.core.util.WaitStrategy;

/**
 ** An implementation of a RingBuffer backed message-passing Processor implementing work-queue distribution with
 * async event loops.
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/workqueue.png" alt="">
 *     <p>
 *         Created from {@link #share}, the {@link WorkQueueProcessor} will authorize concurrent publishing
 *         (multi-producer) from its receiving side {@link Subscriber#onNext(Object)}.
 *         {@link WorkQueueProcessor} is able to replay up to its buffer size number of failed signals (either
 *         dropped or
 *         fatally throwing on child {@link Subscriber#onNext}).
 * <img width="640" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/workqueuef.png" alt="">
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
public final class WorkQueueProcessor<E> extends EventLoopProcessor<E, E> implements Backpressurable, MultiProducer {

	/**
	 * Create a new WorkQueueProcessor using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create() {
		return create(WorkQueueProcessor.class.getSimpleName(), PlatformDependent.SMALL_BUFFER_SIZE,
				null, true);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(boolean autoCancel) {
		return create(WorkQueueProcessor.class.getSimpleName(), PlatformDependent.SMALL_BUFFER_SIZE,
				null, autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(ExecutorService service) {
		return create(service, PlatformDependent.SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size,
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
		return create(service, PlatformDependent.SMALL_BUFFER_SIZE, null,
				autoCancel);
	}

	/**
	 * Create a new TopicProcessor using the default buffer size 32, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(String name) {
		return create(name, PlatformDependent.SMALL_BUFFER_SIZE);
	}

	/**
	 * Create a new TopicProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely
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
	 * will be implicitely created and will use the passed name to qualify the created
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
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely
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
	 * implicitely created and will use the passed name to qualify the created threads.
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
		return new WorkQueueProcessor<E>(name, null, bufferSize, strategy, false,
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
		return new WorkQueueProcessor<E>(null, executor, bufferSize, strategy, false,
				autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent
	 * onNext calls and is suited for multi-threaded publisher that will fan-in data. <p>
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share() {
		return share(WorkQueueProcessor.class.getSimpleName(), PlatformDependent.SMALL_BUFFER_SIZE,
				null, true);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(boolean autoCancel) {
		return share(WorkQueueProcessor.class.getSimpleName(), PlatformDependent.SMALL_BUFFER_SIZE,
				null, autoCancel);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> share(ExecutorService service) {
		return share(service, PlatformDependent.SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new WorkQueueProcessor using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size,
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
		return share(service, PlatformDependent.SMALL_BUFFER_SIZE, null,
				autoCancel);
	}

	/**
	 * Create a new TopicProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created and will use the passed name to
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
	 * data. <p> A new Cached ThreadExecutorPool will be implicitely created and will use
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
	 * ThreadExecutorPool will be implicitely created and will use the passed name to
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
	 * A new Cached ThreadExecutorPool will be implicitely created and will use the passed
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
		return new WorkQueueProcessor<E>(name, null, bufferSize, strategy, true,
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
		return new WorkQueueProcessor<E>(null, executor, bufferSize, strategy, true,
				autoCancel);
	}

	private static final Supplier FACTORY = new Supplier<Slot>() {
		@Override
		public Slot get() {
			return new Slot<>();
		}
	};

	/**
	 * Instance
	 */

	final Sequence workSequence =
			RingBuffer.newSequence(RingBuffer.INITIAL_CURSOR_VALUE);

	final Sequence retrySequence =
			RingBuffer.newSequence(RingBuffer.INITIAL_CURSOR_VALUE);

	final RingBuffer<Slot<E>> ringBuffer;

	volatile RingBuffer<Slot<E>> retryBuffer;

	final static AtomicReferenceFieldUpdater<WorkQueueProcessor, RingBuffer>
			RETRY_REF = PlatformDependent
			.newAtomicReferenceFieldUpdater(WorkQueueProcessor.class, "retryBuffer");

	final WaitStrategy readWait = WaitStrategy.liteBlocking();
	final WaitStrategy writeWait;

	volatile int replaying = 0;

	static final AtomicIntegerFieldUpdater<WorkQueueProcessor> REPLAYING =
			AtomicIntegerFieldUpdater
					.newUpdater(WorkQueueProcessor.class, "replaying");

	@SuppressWarnings("unchecked")
	private WorkQueueProcessor(String name, ExecutorService executor, int bufferSize,
	                                WaitStrategy waitStrategy, boolean share,
	                                boolean autoCancel) {
		super(name, executor, autoCancel);

		if (!RingBuffer.isPowerOfTwo(bufferSize) ){
			throw new IllegalArgumentException("bufferSize must be a power of 2 : "+bufferSize);
		}

		Supplier<Slot<E>> factory = (Supplier<Slot<E>>) FACTORY;

		Runnable spinObserver = new Runnable() {
			@Override
			public void run() {
				if (!alive()) {
					throw Exceptions.AlertException.INSTANCE;
				}
			}
		};

		WaitStrategy strategy = waitStrategy == null ?
				WaitStrategy.liteBlocking() :
				waitStrategy;

		this.writeWait = strategy;

		if (share) {
			this.ringBuffer = RingBuffer
					.createMultiProducer(factory, bufferSize, strategy, spinObserver);
		}
		else {
			this.ringBuffer = RingBuffer
					.createSingleProducer(factory, bufferSize, strategy, spinObserver);
		}
		ringBuffer.addGatingSequence(workSequence);

	}

	@Override
	public void subscribe(final Subscriber<? super E> subscriber) {
		super.subscribe(subscriber);

		if (!alive()) {
			TopicProcessor.coldSource(ringBuffer, null, error, workSequence).subscribe(subscriber);
			return;
		}

		final QueueSubscriberLoop<E> signalProcessor =
				new QueueSubscriberLoop<E>(subscriber, this);
		try {

			incrementSubscribers();

			//bind eventProcessor sequence to observe the ringBuffer
			signalProcessor.sequence.set(workSequence.getAsLong());
			ringBuffer.addGatingSequence(signalProcessor.sequence);

			//start the subscriber thread
			executor.execute(signalProcessor);

		}
		catch (Throwable t) {
			decrementSubscribers();
			ringBuffer.removeGatingSequence(signalProcessor.sequence);
			if(RejectedExecutionException.class.isAssignableFrom(t.getClass())){
				TopicProcessor.coldSource(ringBuffer, t, error, workSequence).subscribe(subscriber);
			}
			else {
				EmptySubscription.error(subscriber, t);
			}
		}
	}

	@Override
	public Flux<E> drain() {
		return TopicProcessor.coldSource(ringBuffer, null, error, workSequence);
	}

	@Override
	public void onNext(E o) {
		super.onNext(o);
		RingBuffer.onNext(o, ringBuffer);
	}

	@Override
	protected void doError(Throwable t) {
		readWait.signalAllWhenBlocking();
		writeWait.signalAllWhenBlocking();
		//ringBuffer.markAsTerminated();
	}

	@Override
	protected void doComplete() {
		readWait.signalAllWhenBlocking();
		writeWait.signalAllWhenBlocking();
		//ringBuffer.markAsTerminated();
	}

	@Override
	protected void requestTask(Subscription s) {
		ExecutorUtils.newNamedFactory(name+"[request-task]", null, null, false).newThread(RingBuffer.createRequestTask(s,
				() -> {
					if (!alive()) {
						if (cancelled) {
							throw Exceptions.CancelException.INSTANCE;
						}
						else {
							throw Exceptions.AlertException.INSTANCE;
						}
					}
				}, null,
				ringBuffer::getMinimumGatingSequence, readWait, this, (int)ringBuffer.getCapacity())).start();
	}

	@Override
	protected void cancel(Subscription subscription) {
		super.cancel(subscription);
		readWait.signalAllWhenBlocking();
	}

	@Override
	public boolean isStarted() {
		return super.isStarted() || ringBuffer.getAsLong() != -1;
	}

	@Override
	public long getAvailableCapacity() {
		return ringBuffer.remainingCapacity();
	}

	@Override
	public String toString() {
		return "WorkQueueProcessor{" +
				", ringBuffer=" + ringBuffer +
				", executor=" + executor +
				", workSequence=" + workSequence +
				", retrySequence=" + retrySequence +
				'}';
	}

	@Override
	public long getCapacity() {
		return ringBuffer.getCapacity();
	}

	@Override
	public boolean isWork() {
		return true;
	}

	@Override
	public long getPending() {
		return ringBuffer.getPending() + (retryBuffer != null ? retryBuffer.getPending() : 0L);
	}

	@SuppressWarnings("unchecked")
	RingBuffer<Slot<E>> retryBuffer() {
		RingBuffer<Slot<E>> retry = retryBuffer;
		if (retry == null) {
			retry =
					RingBuffer.createMultiProducer((Supplier<Slot<E>>) FACTORY, 32, WaitStrategy.busySpin());
			retry.addGatingSequence(retrySequence);
			if (!RETRY_REF.compareAndSet(this, null, retry)) {
				retry = retryBuffer;
			}
		}
		return retry;
	}


	@Override
	public Iterator<?> downstreams() {
		return Arrays.asList(ringBuffer.getSequenceReceivers()).iterator();
	}

	@Override
	public long downstreamCount() {
		return ringBuffer.getSequenceReceivers().length - 1;
	}

	/**
	 * Disruptor WorkProcessor port that deals with pending demand. <p> Convenience class
	 * for handling the batching semantics of consuming entries from a {@link
	 * reactor.core.publisher .rb.disruptor .RingBuffer} <p>
	 * @param <T> event implementation storing the data for sharing during exchange or
	 * parallel coordination of an event.
	 */
	final static class QueueSubscriberLoop<T>
			implements Runnable, Producer, Backpressurable, Completable, Cancellable, Introspectable,
			           Requestable, Subscription, Receiver {

		private final AtomicBoolean running = new AtomicBoolean(false);

		private final Sequence sequence =
				RingBuffer.wrap(RingBuffer.INITIAL_CURSOR_VALUE, this);

		private final Sequence pendingRequest = RingBuffer.newSequence(0);

		private final RingBufferReceiver barrier;

		private final WorkQueueProcessor<T> processor;

		private final Subscriber<? super T> subscriber;

		private final Runnable waiter = new Runnable() {
			@Override
			public void run() {
				if (barrier.isAlerted() || !isRunning() ||
						replay(pendingRequest.getAsLong() == Long.MAX_VALUE)) {
					throw Exceptions.AlertException.INSTANCE;
				}
			}
		};

		/**
		 * Construct a ringbuffer consumer that will automatically track the progress by
		 * updating its sequence
		 */
		public QueueSubscriberLoop(Subscriber<? super T> subscriber,
				WorkQueueProcessor<T> processor) {
			this.processor = processor;
			this.subscriber = subscriber;

			this.barrier = processor.ringBuffer.newBarrier();
		}

		public Sequence getSequence() {
			return sequence;
		}

		public void halt() {
			running.set(false);
			barrier.alert();
		}

		public boolean isRunning() {
			return running.get() && (processor.terminated == 0 || processor.error == null &&
					processor.ringBuffer.getAsLong() > sequence.getAsLong());
		}


		/**
		 * It is ok to have another thread rerun this method after a halt().
		 */
		@Override
		public void run() {

			try {
				if (!running.compareAndSet(false, true)) {
					EmptySubscription.error(subscriber, new IllegalStateException("Thread is already running"));
					return;
				}

				//while(processor.alive() && processor.upstreamSubscription == null);
				if(!processor.startSubscriber(subscriber, this)){
					return;
				}

				boolean processedSequence = true;
				long cachedAvailableSequence = Long.MIN_VALUE;
				long nextSequence = sequence.getAsLong();
				Slot<T> event = null;

				if (!RingBuffer.waitRequestOrTerminalEvent(pendingRequest, barrier, running, sequence,
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
							processedSequence = false;
							do {
								nextSequence = processor.workSequence.getAsLong() + 1L;
								while ((!unbounded && pendingRequest.getAsLong() == 0L)) {
									if (!isRunning()) {
										throw Exceptions.AlertException.INSTANCE;
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
							catch (Exceptions.AlertException ce) {
								barrier.clearAlert();
								throw Exceptions.CancelException.INSTANCE;
							}

							subscriber.onNext(event.value);

							processedSequence = true;

						}
						else {
							processor.readWait.signalAllWhenBlocking();
							try {
								cachedAvailableSequence =
										barrier.waitFor(nextSequence, waiter);
							}
							catch (Exceptions.AlertException ce) {
								barrier.clearAlert();
								if (!running.get()) {
									processor.decrementSubscribers();
								}
								else {
									throw ce;
								}
								try {
									for (; ; ) {
										try {
											cachedAvailableSequence =
													barrier.waitFor(nextSequence);
											event = processor.ringBuffer.get(nextSequence);
											break;
										}
										catch (Exceptions.AlertException cee) {
											barrier.clearAlert();
										}
									}
									reschedule(event);
								}
								catch (Exception c) {
									//IGNORE
								}
								processor.incrementSubscribers();
								throw ce;
							}
						}

					}
					catch (Exceptions.CancelException ce) {
						reschedule(event);
						break;
					}
					catch (Exceptions.AlertException ex) {
						barrier.clearAlert();
						if (!running.get()) {
							break;
						}
						if(processor.terminated == 1) {
							if (processor.error != null) {
								subscriber.onError(processor.error);
								break;
							}
							if (processor.ringBuffer.getPending() == 0L) {
								subscriber.onComplete();
								break;
							}
						}
						//processedSequence = true;
						//continue event-loop

					}
					catch (final Throwable ex) {
						reschedule(event);
						subscriber.onError(ex);
						sequence.set(nextSequence);
						processedSequence = true;
					}
				}
			}
			finally {
				processor.decrementSubscribers();
				processor.ringBuffer.removeGatingSequence(sequence);
				/*if(processor.decrementSubscribers() == 0){
					long r = processor.ringBuffer.getCursor();
					long w = processor.workSequence.getAsLong();
					if ( w > r ){
						processor.workSequence.compareAndSet(w, r);
					}
				}*/
				running.set(false);
				processor.writeWait.signalAllWhenBlocking();
			}
		}

		private boolean replay(final boolean unbounded) {

			if (REPLAYING.compareAndSet(processor, 0, 1)) {
				Slot<T> signal;

				try {
					RingBuffer<Slot<T>> q = processor.retryBuffer;
					if (q == null) {
						return false;
					}

					for (; ; ) {

						if (!running.get()) {
							return true;
						}

						long cursor = processor.retrySequence.getAsLong() + 1;

						if (q.getCursor() >= cursor) {
							signal = q.get(cursor);
						}
						else {
							processor.readWait.signalAllWhenBlocking();
							return !processor.alive();
						}
						if(signal.value != null) {
							readNextEvent(unbounded);
							subscriber.onNext(signal.value);
							processor.retrySequence.set(cursor);
						}
					}

				}
				catch (Exceptions.CancelException ce) {
					running.set(false);
					return true;
				}
				finally {
					REPLAYING.compareAndSet(processor, 1, 0);
				}
			}
			else {
				return !processor.alive();
			}
		}

		private void reschedule(Slot<T> event) {
			if (event != null &&
					event.value != null) {

				RingBuffer<Slot<T>> retry = processor.retryBuffer();
				long seq = retry.next();
				retry.get(seq).value = event.value;
				retry.publish(seq);
				barrier.alert();
				processor.readWait.signalAllWhenBlocking();
			}
		}

		private void readNextEvent(final boolean unbounded)
				throws Exceptions.AlertException {
				//pause until request
			while ((!unbounded && BackpressureUtils.getAndSub(pendingRequest, 1L) == 0L)) {
				if (!isRunning()) {
					throw Exceptions.AlertException.INSTANCE;
				}
				//Todo Use WaitStrategy?
				LockSupport.parkNanos(1L);
			}
		}

		@Override
		public long requestedFromDownstream() {
			return pendingRequest.getAsLong();
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
		public long getPending() {
			return processor.ringBuffer.getPending();
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
			if (BackpressureUtils.checkRequest(n, subscriber)) {
				if (!running.get()) {
					return;
				}

				BackpressureUtils.getAndAddCap(pendingRequest, n);
			}
		}

		@Override
		public void cancel() {
			halt();
		}

		@Override
		public int getMode() {
			return INNER;
		}

		@Override
		public String getName() {
			return processor.getName()+"#loop";
		}
	}


}
