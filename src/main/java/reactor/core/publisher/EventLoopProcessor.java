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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.MultiProducer;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Exceptions;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.concurrent.RingBuffer;
import reactor.util.concurrent.RingBufferReader;
import reactor.util.concurrent.Sequence;
import reactor.util.concurrent.WaitStrategy;

/**
 * A base processor used by executor backed processors to take care of their ExecutorService
 *
 * @author Stephane Maldini
 */
abstract class EventLoopProcessor<IN> extends FluxProcessor<IN, IN>
		implements Receiver, Runnable, MultiProducer {

	/**
	 * Whether the RingBuffer*Processor can be graphed by wrapping the individual Sequence with the target downstream
	 */
	public static final  boolean TRACEABLE_RING_BUFFER_PROCESSOR =
			Boolean.parseBoolean(System.getProperty("reactor.ringbuffer.trace", "true"));

	static <E> Flux<E> coldSource(RingBuffer<Slot<E>> ringBuffer, Throwable t, Throwable error,
			Sequence start){
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
				t.addSuppressed(error);
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
	 * @param stopCondition {@link Runnable} evaluated in the spin loop that may throw
	 * @param postWaitCallback a {@link Consumer} notified with the latest sequence read
	 * @param readCount a {@link LongSupplier} a sequence cursor to wait on
	 * @param waitStrategy a {@link WaitStrategy} to trade off cpu cycle for latency
	 * @param errorSubscriber an error subscriber if request/cancel fails
	 * @param prefetch the target prefetch size
	 *
	 * @return a {@link Runnable} loop to execute to start the requesting loop
	 */
	static Runnable createRequestTask(Subscription upstream,
			Runnable stopCondition,
			Consumer<Long> postWaitCallback,
			LongSupplier readCount,
			WaitStrategy waitStrategy,
			Subscriber<?> errorSubscriber,
			int prefetch) {
		return new RequestTask(upstream,
				stopCondition,
				postWaitCallback,
				readCount,
				waitStrategy,
				errorSubscriber,
				prefetch);
	}

	/**
	 * Create a new single producer RingBuffer using the default wait strategy  {@link
	 * WaitStrategy#busySpin()}. <p>See {@code MultiProducer}.
	 *
	 * @param <E> the element type
	 * @param bufferSize number of elements to create within the ring buffer.
	 *
	 * @return the new RingBuffer instance
	 */
	@SuppressWarnings("unchecked")
	static <E> RingBuffer<Slot<E>> createSingleProducer(int bufferSize) {
		return RingBuffer.createSingleProducer(EMITTED,
				bufferSize,
				WaitStrategy.busySpin());
	}

	/**
	 * Spin CPU until the request {@link LongSupplier} is populated at least once by a
	 * strict positive value. To relieve the spin loop, the read sequence itself will be
	 * used against so it will wake up only when a signal is emitted upstream or other
	 * stopping condition including terminal signals thrown by the {@link
	 * RingBufferReader} waiting barrier.
	 *
	 * @param pendingRequest the {@link LongSupplier} request to observe
	 * @param barrier {@link RingBufferReader} to wait on
	 * @param isRunning {@link AtomicBoolean} calling loop running state
	 * @param nextSequence {@link LongSupplier} ring buffer read cursor
	 * @param waiter an optional extra spin observer for the wait strategy in {@link
	 * RingBufferReader}
	 *
	 * @return true if a request has been received, false in any other case.
	 */
	static boolean waitRequestOrTerminalEvent(LongSupplier pendingRequest,
			RingBufferReader barrier,
			AtomicBoolean isRunning,
			LongSupplier nextSequence,
			Runnable waiter) {
		try {
			long waitedSequence;
			while (pendingRequest.getAsLong() <= 0L) {
				//pause until first request
				waitedSequence = nextSequence.getAsLong() + 1;
				if (waiter != null) {
					waiter.run();
					barrier.waitFor(waitedSequence, waiter);
				}
				else {
					barrier.waitFor(waitedSequence);
				}
				if (!isRunning.get()) {
					throw Exceptions.CancelException.INSTANCE;
				}
				LockSupport.parkNanos(1L);
			}
		}
		catch (InterruptedException ie) {
			Thread.currentThread()
			      .interrupt();
		}

		catch (Exception e) {
			if (RingBuffer.isAlert(e) || e instanceof Exceptions.CancelException) {
				return false;
			}
			throw e;
		}

		return true;
	}

	@SuppressWarnings("rawtypes")
	static final Supplier EMITTED = Slot::new;

	/**
	 * Concurrent addition bound to Long.MAX_VALUE. Any concurrent write will "happen"
	 * before this operation.
	 *
	 * @param sequence current sequence to update
	 * @param toAdd delta to add
	 *
	 * @return value before addition or Long.MAX_VALUE
	 */
	static long getAndAddCap(Sequence sequence, long toAdd) {
		long u, r;
		do {
			r = sequence.getAsLong();
			if (r == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			u = Operators.addCap(r, toAdd);
		}
		while (!sequence.compareAndSet(r, u));
		return r;
	}

	/**
	 * Concurrent substraction bound to 0 and Long.MAX_VALUE. Any concurrent write will
	 * "happen" before this operation.
	 *
	 * @param sequence current sequence to update
	 * @param toSub delta to sub
	 *
	 * @return value before subscription, 0 or Long.MAX_VALUE
	 */
	static long getAndSub(Sequence sequence, long toSub) {
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

	/**
	 * Wrap a new sequence into a traceable {@link Producer} thus keeping reference and adding an extra stack level
	 * when
	 * peeking. Mostly invisible cost but the option is left open. Keeping reference of the arbitrary consumer allows
	 * expanded operational navigation (graph) by finding all target subscribers of a given ring buffer.
	 *
	 * @param init the initial sequence index
	 * @param delegate the target to proxy
	 *
	 * @return a wrapped {@link Sequence}
	 */
	static Sequence wrap(long init, Object delegate) {
		if (TRACEABLE_RING_BUFFER_PROCESSOR) {
			return wrap(RingBuffer.newSequence(init), delegate);
		}
		else {
			return RingBuffer.newSequence(init);
		}
	}

	/**
	 * Wrap a sequence into a traceable {@link Producer} thus keeping reference and adding an extra stack level when
	 * peeking. Mostly invisible cost but the option is left open.
	 *
	 * @param init the sequence reference
	 * @param delegate the object to wrap
	 *
	 * @return a wrapped {@link Sequence}
	 */
	static Sequence wrap(Sequence init, Object delegate){
		return new Wrapped<>(delegate, init);
	}

	final ExecutorService executor;
	final ClassLoader     contextClassLoader;
	final String          name;
	final boolean         autoCancel;

	final RingBuffer<Slot<IN>> ringBuffer;
	final WaitStrategy readWait = WaitStrategy.liteBlocking();

	Subscription upstreamSubscription;
	volatile        boolean         cancelled;
	volatile        int             terminated;
	volatile        Throwable       error;

	volatile       int                                                  subscriberCount;

	EventLoopProcessor(
			int bufferSize,
			ThreadFactory threadFactory,
			ExecutorService executor,
			boolean autoCancel,
			boolean multiproducers,
			Supplier<Slot<IN>> factory,
			WaitStrategy strategy) {

		if (!QueueSupplier.isPowerOfTwo(bufferSize)) {
			throw new IllegalArgumentException("bufferSize must be a power of 2 : " + bufferSize);
		}

		this.autoCancel = autoCancel;

		contextClassLoader = new EventLoopContext();

		@SuppressWarnings("rawtypes")
        String name = threadFactory instanceof Supplier ? ((Supplier)
				threadFactory).get().toString() : null;
		this.name = null != name ? name : getClass().getSimpleName();

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
	 * Determine whether this {@code Processor} can be used.
	 *
	 * @return {@literal true} if this {@code Resource} is alive and can be used, {@literal false} otherwise.
	 */
	final public boolean alive() {
		return 0 == terminated;
	}

	/**
	 * Block until all submitted tasks have completed, then do a normal {@code EventLoopProcessor.shutdown()}.
	 * @return if the underlying executor terminated and false if the timeout elapsed before termination
	 */
	public final boolean awaitAndShutdown() {
		return awaitAndShutdown(-1, TimeUnit.SECONDS);
	}

	/**
	 * Block until all submitted tasks have completed, then do a normal {@code EventLoopProcessor#shutdown()}.
	 * @param timeout the timeout value
	 * @param timeUnit the unit for timeout
     * @return if the underlying executor terminated and false if the timeout elapsed before termination
	 */
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
	 * Drain is a hot replication of the current buffer delivered if supported. Since it is hot there might be no
	 * guarantee to see a end if the buffer keeps replenishing due to concurrent producing.
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
		}
		return drain();
	}

	/**
	 * @return a snapshot number of available onNext before starving the resource
	 */
	public long getAvailableCapacity() {
		return ringBuffer.remainingCapacity();
	}


	@Override
	public Iterator<?> downstreams() {
		return Arrays.asList(ringBuffer.getSequenceReceivers()).iterator();
	}

	@Override
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
	final public boolean isCancelled() {
		return cancelled;
	}

	@Override
	final public boolean isStarted() {
		return upstreamSubscription != null || ringBuffer.getAsLong() != -1;
	}

	@Override
	final public boolean isTerminated() {
		return terminated > 0;
	}

	@Override
	final public void onComplete() {
		if (TERMINATED.compareAndSet(this, 0, SHUTDOWN)) {
			upstreamSubscription = null;
			executor.shutdown();
			readWait.signalAllWhenBlocking();
			doComplete();
		}
	}

	@Override
	final public void onError(Throwable t) {
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}
		if (TERMINATED.compareAndSet(this, 0, SHUTDOWN)) {
			error = t;
			upstreamSubscription = null;
			executor.shutdown();
			readWait.signalAllWhenBlocking();
			doError(t);
		}
	}

	@Override
	final public void onNext(IN o) {
		if (o == null) {
			throw Exceptions.argumentIsNullException();
		}
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
				Exceptions.throwIfFatal(t);
				s.cancel();
				onError(t);
			}
		}
	}

	/**
	 * Shutdown this active {@code Processor} such that it can no longer be used. If the resource carries any work, it
	 * will wait (but NOT blocking the caller) for all the remaining tasks to perform before closing the resource.
	 */
	public final void shutdown() {
		try {
			onComplete();
			executor.shutdown();
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			onError(t);
		}
	}

	@Override
	final public Subscription upstream() {
		return upstreamSubscription;
	}

	@Override
	final public long getCapacity() {
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

		final WaitStrategy waitStrategy;

		final LongSupplier readCount;

		final Subscription upstream;

		final Runnable spinObserver;

		final Consumer<Long> postWaitCallback;

		final Subscriber<?> errorSubscriber;

		final int prefetch;

		public RequestTask(Subscription upstream,
				Runnable stopCondition,
				Consumer<Long> postWaitCallback,
				LongSupplier readCount,
				WaitStrategy waitStrategy,
				Subscriber<?> errorSubscriber,
				int prefetch) {
			this.waitStrategy = waitStrategy;
			this.readCount = readCount;
			this.postWaitCallback = postWaitCallback;
			this.errorSubscriber = errorSubscriber;
			this.upstream = upstream;
			this.spinObserver = stopCondition;
			this.prefetch = prefetch;
		}

		@Override
		public void run() {
			final long bufferSize = prefetch;
			final long limit = bufferSize - Math.max(bufferSize >> 2, 1);
			long cursor = -1;
			try {
				spinObserver.run();
				upstream.request(bufferSize - 1);

				for (; ; ) {
					cursor = waitStrategy.waitFor(cursor + limit, readCount, spinObserver);
					if (postWaitCallback != null) {
						postWaitCallback.accept(cursor);
					}
					//spinObserver.accept(null);
					upstream.request(limit);
				}
			}
			catch (Exceptions.CancelException ce) {
				upstream.cancel();
			}
			catch (InterruptedException e) {
				Thread.currentThread()
				      .interrupt();
			}
			catch (Throwable t) {
				if(RingBuffer.isAlert(t)){
					return;
				}
				Exceptions.throwIfFatal(t);
				errorSubscriber.onError(t);
			}
		}
	}


	protected void requestTask(final Subscription s) {
		//implementation might run a specific request task for the given subscription
	}

	final int decrementSubscribers() {
		Subscription subscription = upstreamSubscription;
		int subs = SUBSCRIBER_COUNT.decrementAndGet(this);
		if (subs == 0) {
			if (subscription != null && autoCancel) {
				upstreamSubscription = null;
				cancel();
			}
			return subs;
		}
		return subs;
	}

	abstract void doError(Throwable throwable);

	final boolean incrementSubscribers() {
		return SUBSCRIBER_COUNT.getAndIncrement(this) == 0;
	}

	final boolean startSubscriber(Subscriber<? super IN> subscriber,
			Subscription subscription) {
		try {
			Thread.currentThread()
			      .setContextClassLoader(contextClassLoader);
			subscriber.onSubscribe(subscription);
			return true;
		}
		catch (Throwable t) {
			Operators.error(subscriber, t);
			return false;
		}
	}

	final static class EventLoopContext extends ClassLoader {

		EventLoopContext() {
			super(Thread.currentThread()
			            .getContextClassLoader());
		}
	}

	static final int SHUTDOWN = 1;
	static final int                                           FORCED_SHUTDOWN  = 2;
	@SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<EventLoopProcessor> SUBSCRIBER_COUNT =
			AtomicIntegerFieldUpdater.newUpdater(EventLoopProcessor.class, "subscriberCount");
	@SuppressWarnings("rawtypes")
	final static AtomicIntegerFieldUpdater<EventLoopProcessor> TERMINATED =
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


final class Wrapped<E> implements Sequence, Producer {

	public final E        delegate;
	public final Sequence sequence;

	public Wrapped(E delegate, Sequence sequence) {
		this.delegate = delegate;
		this.sequence = sequence;
	}

	@Override
	public long getAsLong() {
		return sequence.getAsLong();
	}

	@Override
	public Object downstream() {
		return delegate;
	}

	@Override
	public void set(long value) {
		sequence.set(value);
	}

	@Override
	public void setVolatile(long value) {
		sequence.setVolatile(value);
	}

	@Override
	public boolean compareAndSet(long expectedValue, long newValue) {
		return sequence.compareAndSet(expectedValue, newValue);
	}

	@Override
	public long incrementAndGet() {
		return sequence.incrementAndGet();
	}

	@Override
	public long addAndGet(long increment) {
		return sequence.addAndGet(increment);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		Wrapped<?> wrapped = (Wrapped<?>) o;

		return sequence.equals(wrapped.sequence);

	}

	@Override
	public int hashCode() {
		return sequence.hashCode();
	}

}
