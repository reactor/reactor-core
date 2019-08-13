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

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.concurrent.WaitStrategy;
import reactor.util.context.Context;

/**
 * A base processor used by executor backed processors to take care of their ExecutorService
 *
 * @author Stephane Maldini
 */
@SuppressWarnings("deprecation")
abstract class EventLoopProcessor<IN> extends FluxProcessor<IN, IN>
		implements Runnable {

	static <E> Flux<E> coldSource(RingBuffer<Slot<E>> ringBuffer,
			@Nullable Throwable t,
			@Nullable Throwable error,
			RingBuffer.Sequence start){
		Flux<E> bufferIterable = generate(start::getAsLong, (seq, sink) -> {
			long s = seq + 1;
			if(s > ringBuffer.getCursor()){
				sink.complete();
			}
			else {
				E d = ringBuffer.get(s).value;
				if (d != null) {
					sink.next(d);
				}
			}
			return s;
		});
		if (error != null) {
			if (t != null) {
				t = Exceptions.addSuppressed(t, error);
				return concat(bufferIterable, Flux.error(t));
			}
			return concat(bufferIterable, Flux.error(error));
		}
		return bufferIterable;
	}

	/**
	 * Create a {@link Runnable} event loop that will keep monitoring a {@link
	 * LongSupplier} and compare it to a {@link RingBuffer}
	 *
	 * @param upstream the {@link Subscription} to request/cancel on
	 * @param p parent {@link EventLoopProcessor}
	 * @param postWaitCallback a {@link Consumer} notified with the latest sequence read
	 * @param readCount a {@link LongSupplier} a sequence cursor to wait on
	 *
	 * @return a {@link Runnable} loop to execute to start the requesting loop
	 */
	static Runnable createRequestTask(
			Subscription upstream,
			EventLoopProcessor<?> p,
			@Nullable Consumer<Long> postWaitCallback, LongSupplier readCount) {
		return new RequestTask(upstream, p, postWaitCallback, readCount);
	}

	/**
	 * Spin CPU until the request {@link LongSupplier} is populated at least once by a
	 * strict positive value. To relieve the spin loop, the read sequence itself will be
	 * used against so it will wake up only when a signal is emitted upstream or other
	 * stopping condition including terminal signals thrown by the {@link
	 * RingBuffer.Reader} waiting barrier.
	 *
	 * @param pendingRequest the {@link LongSupplier} request to observe
	 * @param barrier {@link RingBuffer.Reader} to wait on
	 * @param isRunning {@link AtomicBoolean} calling loop running state
	 * @param nextSequence {@link LongSupplier} ring buffer read cursor
	 * @param waiter an optional extra spin observer for the wait strategy in {@link
	 * RingBuffer.Reader}
	 *
	 * @return true if a request has been received, false in any other case.
	 */
	static boolean waitRequestOrTerminalEvent(LongSupplier pendingRequest,
			RingBuffer.Reader barrier,
			AtomicBoolean isRunning,
			LongSupplier nextSequence,
			Runnable waiter) {
		try {
			long waitedSequence;
			while (pendingRequest.getAsLong() <= 0L) {
				//pause until first request
				waitedSequence = nextSequence.getAsLong() + 1;
				waiter.run();
				barrier.waitFor(waitedSequence, waiter);

				if (!isRunning.get()) {
					WaitStrategy.alert();
				}
				LockSupport.parkNanos(1L);
			}
		}
		catch (InterruptedException ie) {
			Thread.currentThread()
			      .interrupt();
		}

		catch (Exception e) {
			if (!isRunning.get() || WaitStrategy.isAlert(e)) {
				return false;
			}
			throw e;
		}

		return true;
	}

	/**
	 * Concurrent addition bound to Long.MAX_VALUE. Any concurrent write will "happen"
	 * before this operation.
	 *
	 * @param sequence current sequence to update
	 * @param toAdd delta to add
	 */
	static void addCap(RingBuffer.Sequence sequence, long toAdd) {
		long u, r;
		do {
			r = sequence.getAsLong();
			if (r == Long.MAX_VALUE) {
				return;
			}
			u = Operators.addCap(r, toAdd);
		}
		while (!sequence.compareAndSet(r, u));
	}

	/**
	 * Concurrent subtraction bound to 0 and Long.MAX_VALUE. Any concurrent write will
	 * "happen" before this operation.
	 *
	 * @param sequence current sequence to update
	 * @param toSub delta to sub
	 *
	 * @return value before subscription, 0 or Long.MAX_VALUE
	 */
	static long getAndSub(RingBuffer.Sequence sequence, long toSub) {
		long r, u;
		do {
			r = sequence.getAsLong();
			if (r == 0 || r == Long.MAX_VALUE) {
				return r;
			}
			u = Operators.subOrZero(r, toSub);
		}
		while (!sequence.compareAndSet(r, u));

		return r;
	}

	final ExecutorService  executor;
	final ExecutorService requestTaskExecutor;
	final EventLoopContext contextClassLoader;
	final String           name;
	final boolean          autoCancel;

	final RingBuffer<Slot<IN>> ringBuffer;
	final WaitStrategy readWait = WaitStrategy.liteBlocking();

	Subscription upstreamSubscription;
	volatile        boolean         cancelled;
	volatile        int             terminated;
	volatile        Throwable       error;

	volatile       int                                                  subscriberCount;

	EventLoopProcessor(
			int bufferSize,
			@Nullable ThreadFactory threadFactory,
			@Nullable ExecutorService executor,
			ExecutorService requestExecutor,
			boolean autoCancel,
			boolean multiproducers,
			Supplier<Slot<IN>> factory,
			WaitStrategy strategy) {

		if (!Queues.isPowerOfTwo(bufferSize)) {
			throw new IllegalArgumentException("bufferSize must be a power of 2 : " + bufferSize);
		}

		if (bufferSize < 1){
			throw new IllegalArgumentException("bufferSize must be strictly positive, " +
					"was: "+bufferSize);
		}

		this.autoCancel = autoCancel;

		contextClassLoader = new EventLoopContext(multiproducers);

		this.name = defaultName(threadFactory, getClass());

		this.requestTaskExecutor = Objects.requireNonNull(requestExecutor, "requestTaskExecutor");

		if (executor == null) {
			this.executor = Executors.newCachedThreadPool(threadFactory);
		}
		else {
			this.executor = executor;
		}

		if (multiproducers) {
			this.ringBuffer = RingBuffer.createMultiProducer(factory,
					bufferSize,
					strategy,
					this);
		}
		else {
			this.ringBuffer = RingBuffer.createSingleProducer(factory,
					bufferSize,
					strategy,
					this);
		}
	}

	/**
	 * Return the number of parked elements in the emitter backlog.
	 *
	 * @return the number of parked elements in the emitter backlog.
	 */
	public abstract long getPending();

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return upstreamSubscription;

		return super.scanUnsafe(key);
	}

	/**
	 * A method to extract a name from the ThreadFactory if it turns out to be a Supplier
	 * (in which case the supplied value string representation is used). Otherwise return
	 * the current class's simpleName.
	 *
	 * @param threadFactory the factory to test for a supplied name
	 * @param clazz
	 * @return the name to use in thread pools
	 */
	protected static String defaultName(@Nullable ThreadFactory threadFactory,
			Class<? extends EventLoopProcessor> clazz) {
		String name = threadFactory instanceof Supplier ? ((Supplier)
				threadFactory).get().toString() : null;
		return null != name ? name : clazz.getSimpleName();
	}

	/**
	 * A method to create a suitable default {@link ExecutorService} for use in implementors
	 * {@link #requestTask(Subscription)} (a {@link Executors#newCachedThreadPool() cached
	 * thread pool}), reusing a main name and appending {@code [request-task]} suffix.
	 *
	 * @param name the main thread name used by the processor.
	 * @return a default {@link ExecutorService} for requestTask.
	 */
	protected static ExecutorService defaultRequestTaskExecutor(String name) {
		return Executors.newCachedThreadPool(r -> new Thread(r,name+"[request-task]"));
	}

	/**
	 * Determine whether this {@code Processor} can be used.
	 *
	 * @return {@literal true} if this {@code Resource} is alive and can be used, {@literal false} otherwise.
	 */
	final public boolean alive() {
		return 0 == terminated;
	}

	/**
	 * Block until all submitted tasks have completed, then do a normal {@code EventLoopProcessor.dispose()}.
	 * @return if the underlying executor terminated and false if the timeout elapsed before termination
	 */
	public final boolean awaitAndShutdown() {
		return awaitAndShutdown(Duration.ofSeconds(-1));
	}

	/**
	 * Block until all submitted tasks have completed, then do a normal {@code EventLoopProcessor#dispose()}.
	 * @param timeout the timeout value
	 * @param timeUnit the unit for timeout
     * @return if the underlying executor terminated and false if the timeout elapsed before termination
	 * @deprecated use {@link #awaitAndShutdown(Duration)} instead
	 */
	@Deprecated
	public final boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		try {
			shutdown();
			return executor.awaitTermination(timeout, timeUnit);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	/**
	 * Block until all submitted tasks have completed, then do a normal {@code EventLoopProcessor#dispose()}.
	 * @param timeout the timeout value as a {@link Duration}. Note this is converted to a {@link Long}
	 * of nanoseconds (which amounts to roughly 292 years maximum timeout).
     * @return if the underlying executor terminated and false if the timeout elapsed before termination
	 */
	public final boolean awaitAndShutdown(Duration timeout) {
		long nanos = -1;
		if (!timeout.isNegative()) {
			nanos = timeout.toNanos();
		}
		try {
			shutdown();
			return executor.awaitTermination(nanos, TimeUnit.NANOSECONDS);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	//FIXME store current subscribers
	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.empty();
	}

	/**
	 * Drain is a hot replication of the current buffer delivered if supported. Since it is hot there might be no
	 * guarantee to see an end if the buffer keeps replenishing due to concurrent
	 * producing.
	 *
	 * @return a {@link Flux} sequence possibly unbounded of incoming buffered values or empty if not supported.
	 */
	public Flux<IN> drain(){
		return Flux.empty();
	}

	/**
	 * Shutdown this {@code Processor}, forcibly halting any work currently executing and discarding any tasks that have
	 * not yet been executed.
	 * @return a Flux instance with the remaining undelivered values
	 */
	final public Flux<IN> forceShutdown() {
		int t = terminated;
		if (t != FORCED_SHUTDOWN && TERMINATED.compareAndSet(this, t, FORCED_SHUTDOWN)) {
			executor.shutdownNow();
			requestTaskExecutor.shutdownNow();
		}
		return drain();
	}

	/**
	 * @return a snapshot number of available onNext before starving the resource
	 */
	final public long getAvailableCapacity() {
		return ringBuffer.bufferSize() - ringBuffer.getPending();
	}

	@Override
	@Nullable
	final public Throwable getError() {
		return error;
	}

	@Override
	final public String toString() {
		return "/Processors/" + name + "/" + contextClassLoader.hashCode();
	}

	@Override
	final public int hashCode() {
		return contextClassLoader.hashCode();
	}

	@Override
	public boolean isSerialized() {
		return contextClassLoader.multiproducer;
	}

	@Override
	final public boolean isTerminated() {
		return terminated > 0;
	}

	@Override
	final public void onComplete() {
		if (TERMINATED.compareAndSet(this, 0, SHUTDOWN)) {
			upstreamSubscription = null;
			doComplete();
			executor.shutdown();
			readWait.signalAllWhenBlocking();
		}
	}

	@Override
	final public void onError(Throwable t) {
		Objects.requireNonNull(t, "onError");
		if (TERMINATED.compareAndSet(this, 0, SHUTDOWN)) {
			error = t;
			upstreamSubscription = null;
			doError(t);
			executor.shutdown();
			readWait.signalAllWhenBlocking();
		}
		else {
			Operators.onErrorDropped(t, Context.empty());
		}
	}

	@Override
	final public void onNext(IN o) {
		Objects.requireNonNull(o, "onNext");
		final long seqId = ringBuffer.next();
		final Slot<IN> signal = ringBuffer.get(seqId);
		signal.value = o;
		ringBuffer.publish(seqId);
	}

	@Override
	final public void onSubscribe(final Subscription s) {
		if (Operators.validate(upstreamSubscription, s)) {
			this.upstreamSubscription = s;
			try {
				if (s != Operators.emptySubscription()) {
					requestTask(s);
				}
			}
			catch (Throwable t) {
				onError(Operators.onOperatorError(s, t, currentContext()));
			}
		}
	}

	@Override
	protected boolean serializeAlways() {
		return !contextClassLoader.multiproducer;
	}

	/**
	 * Shutdown this active {@code Processor} such that it can no longer be used. If the resource carries any work, it
	 * will wait (but NOT blocking the caller) for all the remaining tasks to perform before closing the resource.
	 */
	public final void shutdown() {
		try {
			onComplete();
			executor.shutdown();
			requestTaskExecutor.shutdown();
		}
		catch (Throwable t) {
			onError(Operators.onOperatorError(t, currentContext()));
		}
	}

	@Override
	final public int getBufferSize() {
		return ringBuffer.bufferSize();
	}

	final void cancel() {
		cancelled = true;
		if (TERMINATED.compareAndSet(this, 0, SHUTDOWN)) {
			executor.shutdown();
		}
		readWait.signalAllWhenBlocking();
	}

	protected void doComplete() {

	}


	/**
	 * An async request client for ring buffer impls
	 *
	 * @author Stephane Maldini
	 */
	static final class RequestTask implements Runnable {

		final LongSupplier readCount;

		final Subscription upstream;

		final EventLoopProcessor<?> parent;

		final Consumer<Long> postWaitCallback;

		 RequestTask(Subscription upstream,
				 EventLoopProcessor<?> p,
				 @Nullable Consumer<Long> postWaitCallback,
				LongSupplier readCount) {
			this.parent = p;
			this.readCount = readCount;
			this.postWaitCallback = postWaitCallback;
			this.upstream = upstream;
		}

		@Override
		public void run() {
			final long bufferSize = parent.ringBuffer.bufferSize();
			final long limit = bufferSize == 1 ? bufferSize : bufferSize - Math.max(bufferSize >> 2, 1);
			long cursor = -1;
			try {
				parent.run();
				upstream.request(bufferSize);

				long c;
				//noinspection InfiniteLoopStatement
				for (; ; ) {
					c = cursor + limit;
					cursor = parent.readWait.waitFor(c, readCount, parent);
					if (postWaitCallback != null) {
						postWaitCallback.accept(cursor);
					}
					//spinObserver.accept(null);
					upstream.request(limit + (cursor - c));
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread()
				      .interrupt();
			}
			catch (Throwable t) {
				if(WaitStrategy.isAlert(t)){
					if(parent.cancelled){
						upstream.cancel();
					}
					return;
				}
				parent.onError(Operators.onOperatorError(t, parent.currentContext()));
			}
		}
	}

	protected void requestTask(final Subscription s) {
		//implementation might run a specific request task for the given subscription
	}

	final void decrementSubscribers() {
		Subscription subscription = upstreamSubscription;
		int subs = SUBSCRIBER_COUNT.decrementAndGet(this);
		if (subs == 0) {
			if (subscription != null && autoCancel) {
				upstreamSubscription = null;
				cancel();
			}
		}
	}

	@Override
	public long downstreamCount() {
		return subscriberCount;
	}

	abstract void doError(Throwable throwable);

	final boolean incrementSubscribers() {
		return SUBSCRIBER_COUNT.getAndIncrement(this) == 0;
	}

	final static class EventLoopContext extends ClassLoader {

		final boolean multiproducer;

		EventLoopContext(boolean multiproducer) {
			super(Thread.currentThread()
			            .getContextClassLoader());
			this.multiproducer = multiproducer;
		}
	}

	static final int                                                                  SHUTDOWN         = 1;
	static final int                                                                  FORCED_SHUTDOWN  = 2;
	@SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<EventLoopProcessor> SUBSCRIBER_COUNT =
			AtomicIntegerFieldUpdater.newUpdater(EventLoopProcessor.class, "subscriberCount");
	@SuppressWarnings("rawtypes")
	final static AtomicIntegerFieldUpdater<EventLoopProcessor> TERMINATED       =
			AtomicIntegerFieldUpdater.newUpdater(EventLoopProcessor.class, "terminated");

	/**
	 * A simple reusable data container.
	 *
	 * @param <T> the value type
	 */
	public static final class Slot<T> implements Serializable {

		private static final long serialVersionUID = 5172014386416785095L;

		public T value;
	}


	final static class EventLoopFactory
			implements ThreadFactory, Supplier<String> {
		/** */

		static final AtomicInteger COUNT = new AtomicInteger();

		final String  name;
		final boolean daemon;

		EventLoopFactory(String name, boolean daemon) {
			this.name = name;
			this.daemon = daemon;
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, name + "-" + COUNT.incrementAndGet());
			t.setDaemon(daemon);
			return t;
		}

		@Override
		public String get() {
			return name;
		}
	}
}
