/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.concurrent.WaitStrategy;

/**
 ** An implementation of a RingBuffer backed message-passing Processor implementing work-queue distribution with
 * async event loops.
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/workqueue.png" alt="">
 * <p>
 * Created from {@link #share()}, the {@link WorkQueueProcessor} will authorize concurrent publishing
 * (multi-producer) from its receiving side {@link Subscriber#onNext(Object)}.
 * {@link WorkQueueProcessor} is able to replay up to its buffer size number of failed signals (either
 * dropped or fatally throwing on child {@link Subscriber#onNext}).
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/workqueuef.png" alt="">
 * <p>
 * The processor is very similar to {@link TopicProcessor} but
 * only partially respects the Reactive Streams contract. <p> The purpose of this
 * processor is to distribute the signals to only one of the subscribed subscribers and to
 * share the demand amongst all subscribers. The scenario is akin to Executor or
 * Round-Robin distribution. However there is no guarantee the distribution will be
 * respecting a round-robin distribution all the time. <p> The core use for this component
 * is to scale up easily without suffering the overhead of an Executor and without using
 * dedicated queues by subscriber, which is less used memory, less GC, more win.
 *
 * @param <E> Type of dispatched signal
 * @author Stephane Maldini
 * @deprecated Has been moved to io.projectreactor.addons:reactor-extra:3.3.0+ and will be removed in 3.4.0
 */
@Deprecated
@SuppressWarnings("deprecation")
public final class WorkQueueProcessor<E> extends EventLoopProcessor<E> {

	/**
	 * {@link WorkQueueProcessor} builder that can be used to create new
	 * processors. Instantiate it through the {@link WorkQueueProcessor#builder()} static
	 * method:
	 * <p>
	 * {@code WorkQueueProcessor<String> processor = WorkQueueProcessor.<String>builder().build()}
	 *
	 * @param <T> Type of dispatched signal
	 * @deprecated Has been moved to io.projectreactor.addons:reactor-extra:3.3.0+ and will be removed in 3.4.0
	 */
	@Deprecated
	public final static class Builder<T> {

		String          name;
		ExecutorService executor;
		ExecutorService requestTaskExecutor;
		int             bufferSize;
		WaitStrategy    waitStrategy;
		boolean         share;
		boolean         autoCancel;

		Builder() {
			this.bufferSize = Queues.SMALL_BUFFER_SIZE;
			this.autoCancel = true;
			this.share = false;
		}

		/**
		 * Configures name for this builder. Default value is WorkQueueProcessor.
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
		 * Configures buffer size for this builder. Default value is {@link Queues#SMALL_BUFFER_SIZE}.
		 * @param bufferSize the internal buffer size to hold signals, must be a power of 2
		 * @return builder with provided buffer size
		 */
		public Builder<T> bufferSize(int bufferSize) {
			if (!Queues.isPowerOfTwo(bufferSize)) {
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
		 * Configures wait strategy for this builder. Default value is {@link WaitStrategy#liteBlocking()}.
		 * Wait strategy is set to default if the provided <code>waitStrategy</code> is null.
		 * @param waitStrategy A RingBuffer WaitStrategy to use instead of the default smart blocking wait strategy.
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
		 * Creates a new {@link WorkQueueProcessor} using the properties
		 * of this builder.
		 * @return a fresh processor
		 */
		public WorkQueueProcessor<T> build() {
			String name = this.name != null ? this.name : WorkQueueProcessor.class.getSimpleName();
			WaitStrategy waitStrategy = this.waitStrategy != null ? this.waitStrategy : WaitStrategy.liteBlocking();
			ThreadFactory threadFactory = this.executor != null ? null : new EventLoopFactory(name, autoCancel);
			ExecutorService requestTaskExecutor = this.requestTaskExecutor != null ?
					this.requestTaskExecutor : defaultRequestTaskExecutor(defaultName(threadFactory, WorkQueueProcessor.class));
			return new WorkQueueProcessor<>(
					threadFactory,
					executor,
					requestTaskExecutor,
					bufferSize,
					waitStrategy,
					share,
					autoCancel);
		}
	}

	/**
	 * Create a new {@link WorkQueueProcessor} {@link Builder} with default properties.
	 * @return new WorkQueueProcessor builder
	 */
	public final static <T> Builder<T> builder() {
		return new Builder<>();
	}

	/**
	 * Create a new WorkQueueProcessor using {@link Queues#SMALL_BUFFER_SIZE} backlog size,
	 * blockingWait Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be
	 * implicitly created.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create() {
		return WorkQueueProcessor.<E>builder().build();
	}

	/**
	 * Create a new WorkQueueProcessor using the passed buffer size, blockingWait
	 * Strategy and auto-cancel. <p> A new Cached ThreadExecutorPool will be implicitly
	 * created and will use the passed name to qualify the created threads.
	 * @param name Use a new Cached ExecutorService and assign this name to the created
	 * threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> WorkQueueProcessor<E> create(String name, int bufferSize) {
		return WorkQueueProcessor.<E>builder().name(name).bufferSize(bufferSize).build();
	}

	/**
	 * Create a new shared WorkQueueProcessor using the passed buffer size, blockingWait
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
		return WorkQueueProcessor.<E>builder().share(true).name(name).bufferSize(bufferSize).build();
	}

	@SuppressWarnings("rawtypes")
    static final Supplier FACTORY = (Supplier<Slot>) Slot::new;

	/**
	 * Instance
	 */

	final RingBuffer.Sequence workSequence =
			RingBuffer.newSequence(RingBuffer.INITIAL_CURSOR_VALUE);

	final Queue<Object> claimedDisposed = new ConcurrentLinkedQueue<>();

	final WaitStrategy writeWait;

	volatile int replaying;

	@SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<WorkQueueProcessor> REPLAYING =
			AtomicIntegerFieldUpdater
					.newUpdater(WorkQueueProcessor.class, "replaying");

	@SuppressWarnings("unchecked")
	WorkQueueProcessor(
			@Nullable ThreadFactory threadFactory,
			@Nullable ExecutorService executor,
			ExecutorService requestTaskExecutor,
			int bufferSize, WaitStrategy waitStrategy, boolean share,
	                                boolean autoCancel) {
		super(bufferSize, threadFactory,
				executor, requestTaskExecutor,
				autoCancel,
				share,
				FACTORY,
				waitStrategy);

		this.writeWait = waitStrategy;

		ringBuffer.addGatingSequence(workSequence);
	}

	@Override
	public void subscribe(final CoreSubscriber<? super E> actual) {
		Objects.requireNonNull(actual, "subscribe");

		if (!alive()) {
			TopicProcessor.coldSource(ringBuffer, null, error, workSequence).subscribe(
					actual);
			return;
		}

		final WorkQueueInner<E> signalProcessor =
				new WorkQueueInner<>(actual, this);
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
				TopicProcessor.coldSource(ringBuffer, t, error, workSequence).subscribe(
						actual);
			}
			else {
				Operators.error(actual, t);
			}
		}
	}

	/**
	 * This method will attempt to compute the maximum amount of subscribers a
	 * {@link WorkQueueProcessor} can accommodate based on a given {@link ExecutorService}.
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
		requestTaskExecutor.execute(createRequestTask(s, this,
				null, ringBuffer::getMinimumGatingSequence));
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

	/**
	 * Disruptor WorkProcessor port that deals with pending demand. <p> Convenience class
	 * for handling the batching semantics of consuming entries from a {@link
	 * RingBuffer} <p>
	 * @param <T> event implementation storing the data for sharing during exchange or
	 * parallel coordination of an event.
	 */
	final static class WorkQueueInner<T>
			implements Runnable, Subscription, Scannable {

		final AtomicBoolean running = new AtomicBoolean(true);

		final RingBuffer.Sequence
				sequence = RingBuffer.newSequence(RingBuffer.INITIAL_CURSOR_VALUE);

		final RingBuffer.Sequence pendingRequest = RingBuffer.newSequence(0);

		final RingBuffer.Reader barrier;

		final WorkQueueProcessor<T> processor;

		final CoreSubscriber<? super T> subscriber;

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
		WorkQueueInner(CoreSubscriber<? super T> subscriber,
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
			return running.get() && (processor.terminated == 0 ||
					(processor.terminated != FORCED_SHUTDOWN &&
							processor.error == null &&
							processor.ringBuffer.getAsLong() > sequence.getAsLong())
			);
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

				final boolean unbounded = pendingRequest.getAsLong() == Long.MAX_VALUE;

				if (!EventLoopProcessor.waitRequestOrTerminalEvent(pendingRequest, barrier, running, sequence,
						waiter) && replay(unbounded)) {
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
						if (ce instanceof InterruptedException) {
							Thread.currentThread().interrupt();
						}
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
						if(processor.terminated == SHUTDOWN) {
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
						else if (processor.terminated == FORCED_SHUTDOWN) {
								break;
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
					Thread.currentThread().interrupt();
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

		boolean reschedule(@Nullable Slot<T> event) {
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT ) return processor;
			if (key == Attr.ACTUAL ) return subscriber;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.TERMINATED ) return processor.isTerminated();
			if (key == Attr.CANCELLED) return !running.get();
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return pendingRequest.getAsLong();

			return null;
		}

		@Override
		public void request(long n) {
			if (!Operators.validate(n) || !running.get()) {
				return;
			}

			addCap(pendingRequest, n);
		}

		@Override
		public void cancel() {
			halt();
		}

	}

	static final Logger log = Loggers.getLogger(WorkQueueProcessor.class);

}
