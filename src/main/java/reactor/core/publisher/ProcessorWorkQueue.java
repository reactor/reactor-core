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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.queue.RingBuffer;
import reactor.core.queue.SequenceBarrier;
import reactor.core.queue.Sequencer;
import reactor.core.queue.Slot;
import reactor.core.subscription.BackpressureUtils;
import reactor.core.subscription.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.ExecutorUtils;
import reactor.core.util.ReactiveState;
import reactor.core.util.Sequence;
import reactor.core.util.WaitStrategy;
import reactor.core.util.internal.PlatformDependent;
import reactor.fn.LongSupplier;
import reactor.fn.Supplier;

/**
 * An implementation of a RingBuffer backed message-passing WorkProcessor. <p> The
 * processor is very similar to {@link ProcessorTopic} but
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
public final class ProcessorWorkQueue<E> extends ProcessorExecutor<E, E>
		implements ReactiveState.Buffering, ReactiveState.LinkedDownstreams {

	/**
	 * Create a new ProcessorWorkQueue using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be
	 * implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> create() {
		return create(ProcessorWorkQueue.class.getSimpleName(), SMALL_BUFFER_SIZE,
				null, true);
	}

	/**
	 * Create a new ProcessorWorkQueue using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A new Cached
	 * ThreadExecutorPool will be implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> create(boolean autoCancel) {
		return create(ProcessorWorkQueue.class.getSimpleName(), SMALL_BUFFER_SIZE,
				null, autoCancel);
	}

	/**
	 * Create a new ProcessorWorkQueue using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> create(ExecutorService service) {
		return create(service, SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new ProcessorWorkQueue using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> create(ExecutorService service,
			boolean autoCancel) {
		return create(service, SMALL_BUFFER_SIZE, null,
				autoCancel);
	}

	/**
	 * Create a new ProcessorTopic using the default buffer size 32, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> create(String name) {
		return create(name, SMALL_BUFFER_SIZE);
	}

	/**
	 * Create a new ProcessorTopic using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitely
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> create(String name, int bufferSize) {
		return create(name, bufferSize, null, true);
	}

	/**
	 * Create a new ProcessorTopic using the passed buffer size, blockingWait
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
	public static <E> ProcessorWorkQueue<E> create(String name, int bufferSize,
			boolean autoCancel) {
		return create(name, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new ProcessorTopic using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> The passed {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> create(ExecutorService service,
			int bufferSize) {
		return create(service, bufferSize, null, true);
	}

	/**
	 * Create a new ProcessorWorkQueue using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> The passed {@link ExecutorService}
	 * will execute as many event-loop consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> create(ExecutorService service,
			int bufferSize, boolean autoCancel) {
		return create(service, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new ProcessorWorkQueue using the passed buffer size, blockingWait
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
	public static <E> ProcessorWorkQueue<E> create(String name, int bufferSize,
			WaitStrategy strategy) {
		return create(name, bufferSize, strategy, true);
	}

	/**
	 * Create a new ProcessorWorkQueue using the passed buffer size, blockingWait
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
	public static <E> ProcessorWorkQueue<E> create(String name, int bufferSize,
			WaitStrategy strategy, boolean autoCancel) {
		return new ProcessorWorkQueue<E>(name, null, bufferSize, strategy, false,
				autoCancel);
	}

	/**
	 * Create a new ProcessorWorkQueue using the passed buffer size and blockingWait
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
	public static <E> ProcessorWorkQueue<E> create(ExecutorService executor,
			int bufferSize, WaitStrategy strategy) {
		return create(executor, bufferSize, strategy, true);
	}

	/**
	 * Create a new ProcessorWorkQueue using the passed buffer size, wait strategy
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
	public static <E> ProcessorWorkQueue<E> create(ExecutorService executor,
			int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new ProcessorWorkQueue<E>(null, executor, bufferSize, strategy, false,
				autoCancel);
	}

	/**
	 * Create a new ProcessorWorkQueue using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent
	 * onNext calls and is suited for multi-threaded publisher that will fan-in data. <p>
	 * A new Cached ThreadExecutorPool will be implicitely created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> share() {
		return share(ProcessorWorkQueue.class.getSimpleName(), SMALL_BUFFER_SIZE,
				null, true);
	}

	/**
	 * Create a new ProcessorWorkQueue using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and the passed auto-cancel setting. <p> A Shared Processor
	 * authorizes concurrent onNext calls and is suited for multi-threaded publisher that
	 * will fan-in data. <p> A new Cached ThreadExecutorPool will be implicitely created.
	 * @param autoCancel Should this propagate cancellation when unregistered by all
	 * subscribers ?
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> share(boolean autoCancel) {
		return share(ProcessorWorkQueue.class.getSimpleName(), SMALL_BUFFER_SIZE,
				null, autoCancel);
	}

	/**
	 * Create a new ProcessorWorkQueue using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. The passed {@link
	 * ExecutorService} will execute as many event-loop consuming the
	 * ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> share(ExecutorService service) {
		return share(service, SMALL_BUFFER_SIZE, null, true);
	}

	/**
	 * Create a new ProcessorWorkQueue using {@link ReactiveState#SMALL_BUFFER_SIZE} backlog size,
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
	public static <E> ProcessorWorkQueue<E> share(ExecutorService service,
			boolean autoCancel) {
		return share(service, SMALL_BUFFER_SIZE, null,
				autoCancel);
	}

	/**
	 * Create a new ProcessorTopic using the passed buffer size, blockingWait
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
	public static <E> ProcessorWorkQueue<E> share(String name, int bufferSize) {
		return share(name, bufferSize, null, true);
	}

	/**
	 * Create a new ProcessorTopic using the passed buffer size, blockingWait
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
	public static <E> ProcessorWorkQueue<E> share(String name, int bufferSize,
			boolean autoCancel) {
		return share(name, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new ProcessorTopic using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A Shared Processor authorizes concurrent onNext calls
	 * and is suited for multi-threaded publisher that will fan-in data. <p> The passed
	 * {@link ExecutorService} will execute as many event-loop
	 * consuming the ringbuffer as subscribers.
	 * @param service A provided ExecutorService to manage threading infrastructure
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ProcessorWorkQueue<E> share(ExecutorService service,
			int bufferSize) {
		return share(service, bufferSize, null, true);
	}

	/**
	 * Create a new ProcessorWorkQueue using the passed buffer size, blockingWait
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
	public static <E> ProcessorWorkQueue<E> share(ExecutorService service,
			int bufferSize, boolean autoCancel) {
		return share(service, bufferSize, null, autoCancel);
	}

	/**
	 * Create a new ProcessorWorkQueue using the passed buffer size, blockingWait
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
	public static <E> ProcessorWorkQueue<E> share(String name, int bufferSize,
			WaitStrategy strategy) {
		return share(name, bufferSize, strategy, true);
	}

	/**
	 * Create a new ProcessorWorkQueue using the passed buffer size, blockingWait
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
	public static <E> ProcessorWorkQueue<E> share(String name, int bufferSize,
			WaitStrategy strategy, boolean autoCancel) {
		return new ProcessorWorkQueue<E>(name, null, bufferSize, strategy, true,
				autoCancel);
	}

	/**
	 * Create a new ProcessorWorkQueue using the passed buffer size and blockingWait
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
	public static <E> ProcessorWorkQueue<E> share(ExecutorService executor,
			int bufferSize, WaitStrategy strategy) {
		return share(executor, bufferSize, strategy, true);
	}

	/**
	 * Create a new ProcessorWorkQueue using the passed buffer size, wait strategy
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
	public static <E> ProcessorWorkQueue<E> share(ExecutorService executor,
			int bufferSize, WaitStrategy strategy, boolean autoCancel) {
		return new ProcessorWorkQueue<E>(null, executor, bufferSize, strategy, true,
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
			Sequencer.newSequence(Sequencer.INITIAL_CURSOR_VALUE);

	final Sequence retrySequence =
			Sequencer.newSequence(Sequencer.INITIAL_CURSOR_VALUE);

	final RingBuffer<Slot<E>> ringBuffer;

	volatile RingBuffer<Slot<E>> retryBuffer;

	final static AtomicReferenceFieldUpdater<ProcessorWorkQueue, RingBuffer>
			RETRY_REF = PlatformDependent
			.newAtomicReferenceFieldUpdater(ProcessorWorkQueue.class, "retryBuffer");

	final WaitStrategy readWait = new WaitStrategy.LiteBlocking();
	final WaitStrategy writeWait;

	volatile int replaying = 0;

	static final AtomicIntegerFieldUpdater<ProcessorWorkQueue> REPLAYING =
			AtomicIntegerFieldUpdater
					.newUpdater(ProcessorWorkQueue.class, "replaying");

	@SuppressWarnings("unchecked")
	private ProcessorWorkQueue(String name, ExecutorService executor, int bufferSize,
	                                WaitStrategy waitStrategy, boolean share,
	                                boolean autoCancel) {
		super(name, executor, autoCancel);

		if (!Sequencer.isPowerOfTwo(bufferSize) ){
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
				new WaitStrategy.LiteBlocking() :
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
			ProcessorTopic.coldSource(ringBuffer, null, error, workSequence).subscribe(subscriber);
			return;
		}

		final QueueSubscriberLoop<E> signalProcessor =
				new QueueSubscriberLoop<E>(subscriber, this);
		try {

			incrementSubscribers();

			//bind eventProcessor sequence to observe the ringBuffer
			signalProcessor.sequence.set(workSequence.get());
			ringBuffer.addGatingSequence(signalProcessor.sequence);

			//start the subscriber thread
			executor.execute(signalProcessor);

		}
		catch (Throwable t) {
			decrementSubscribers();
			ringBuffer.removeGatingSequence(signalProcessor.sequence);
			if(RejectedExecutionException.class.isAssignableFrom(t.getClass())){
				ProcessorTopic.coldSource(ringBuffer, t, error, workSequence).subscribe(subscriber);
			}
			else {
				EmptySubscription.error(subscriber, t);
			}
		}
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
	}

	@Override
	protected void doComplete() {
		readWait.signalAllWhenBlocking();
		writeWait.signalAllWhenBlocking();
	}

	@Override
	protected void requestTask(Subscription s) {
		ExecutorUtils.newNamedFactory(name+"[request-task]", null, null, false).newThread(RingBuffer.createRequestTask(s, new
				Runnable() {
			@Override
			public void run() {
				if (!alive()) {
					if (cancelled) {
						throw Exceptions.CancelException.INSTANCE;
					}
					else {
						throw Exceptions.AlertException.INSTANCE;
					}
				}
			}
		}, null, new LongSupplier() {
			@Override
			public long get() {
				return ringBuffer.getMinimumGatingSequence();
			}
		}, readWait, this, ringBuffer)).start();
	}

	@Override
	protected void cancel(Subscription subscription) {
		super.cancel(subscription);
		readWait.signalAllWhenBlocking();
	}

	@Override
	public boolean isStarted() {
		return super.isStarted() || ringBuffer.get() != -1;
	}

	@Override
	public long getAvailableCapacity() {
		return ringBuffer.remainingCapacity();
	}

	@Override
	public String toString() {
		return "ProcessorWorkQueue{" +
				", ringBuffer=" + ringBuffer +
				", executor=" + executor +
				", workSequence=" + workSequence +
				", retrySequence=" + retrySequence +
				'}';
	}

	@Override
	public long getCapacity() {
		return ringBuffer.getBufferSize();
	}

	@Override
	public boolean isWork() {
		return true;
	}

	@Override
	public long pending() {
		return ringBuffer.pending() + (retryBuffer != null ? retryBuffer.pending() : 0L);
	}

	@SuppressWarnings("unchecked")
	RingBuffer<Slot<E>> retryBuffer() {
		RingBuffer<Slot<E>> retry = retryBuffer;
		if (retry == null) {
			retry =
					RingBuffer.createMultiProducer((Supplier<Slot<E>>) FACTORY, 32, RingBuffer.NO_WAIT);
			retry.addGatingSequence(retrySequence);
			if (!RETRY_REF.compareAndSet(this, null, retry)) {
				retry = retryBuffer;
			}
		}
		return retry;
	}


	@Override
	public Iterator<?> downstreams() {
		return Arrays.asList(ringBuffer.getSequencer().getGatingSequences()).iterator();
	}

	@Override
	public long downstreamsCount() {
		return ringBuffer.getSequencer().getGatingSequences().length - 1;
	}

	/**
	 * Disruptor WorkProcessor port that deals with pending demand. <p> Convenience class
	 * for handling the batching semantics of consuming entries from a {@link
	 * reactor.core.publisher .rb.disruptor .RingBuffer} <p>
	 * @param <T> event implementation storing the data for sharing during exchange or
	 * parallel coordination of an event.
	 */
	private final static class QueueSubscriberLoop<T>
			implements Runnable, Downstream, Buffering, ActiveUpstream,
			ActiveDownstream, Inner, DownstreamDemand, Subscription, Upstream {

		private final AtomicBoolean running = new AtomicBoolean(false);

		private final Sequence sequence =
				Sequencer.wrap(Sequencer.INITIAL_CURSOR_VALUE, this);

		private final Sequence pendingRequest = Sequencer.newSequence(0);

		private final SequenceBarrier barrier;

		private final ProcessorWorkQueue<T> processor;

		private final Subscriber<? super T> subscriber;

		private final Runnable waiter = new Runnable() {
			@Override
			public void run() {
				if (barrier.isAlerted() || !isRunning() ||
						replay(pendingRequest.get() == Long.MAX_VALUE)) {
					throw Exceptions.AlertException.INSTANCE;
				}
			}
		};

		/**
		 * Construct a ringbuffer consumer that will automatically track the progress by
		 * updating its sequence
		 */
		public QueueSubscriberLoop(Subscriber<? super T> subscriber,
				ProcessorWorkQueue<T> processor) {
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
			return running.get() && (processor.terminated == 0 || processor.error == null && processor.ringBuffer.get
					() >
					sequence.get());
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
				long nextSequence = sequence.get();
				Slot<T> event = null;

				if (!RingBuffer.waitRequestOrTerminalEvent(pendingRequest, barrier, running, sequence,
						waiter)) {
					if(!running.get()){
						return;
					}
					if(processor.terminated == 1 && processor.ringBuffer.get() == -1L) {
						if (processor.error != null) {
							subscriber.onError(processor.error);
							return;
						}
						subscriber.onComplete();
						return;
					}
				}

				final boolean unbounded = pendingRequest.get() == Long.MAX_VALUE;

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
								nextSequence = processor.workSequence.get() + 1L;
								while ((!unbounded && pendingRequest.get() == 0L)) {
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
							if(processor.ringBuffer.pending() == 0L) {
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
					long w = processor.workSequence.get();
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

						long cursor = processor.retrySequence.get() + 1;

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
			return pendingRequest.get();
		}

		@Override
		public boolean isCancelled() {
			return !running.get();
		}

		@Override
		public boolean isStarted() {
			return sequence.get() != -1L;
		}

		@Override
		public boolean isTerminated() {
			return !running.get();
		}

		@Override
		public long pending() {
			return processor.ringBuffer.pending();
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

				BackpressureUtils.getAndAdd(pendingRequest, n);
			}
		}

		@Override
		public void cancel() {
			halt();
		}
	}


}
