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
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.MultiProducer;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.util.Exceptions;
import reactor.core.Reactor;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.concurrent.RingBuffer;
import reactor.util.concurrent.Sequence;
import reactor.util.concurrent.Slot;
import reactor.util.concurrent.WaitStrategy;

import static reactor.core.Reactor.Logger;

/**
 * A base processor used by executor backed processors to take care of their ExecutorService
 *
 * @author Stephane Maldini
 */
abstract class EventLoopProcessor<IN> extends FluxProcessor<IN, IN>
		implements Receiver, Runnable, MultiProducer {

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
		if (Reactor.TRACEABLE_RING_BUFFER_PROCESSOR) {
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
		RingBuffer.onNext(o, ringBuffer);
	}

	@Override
	final public void onSubscribe(final Subscription s) {
		if (Operators.validate(upstreamSubscription, s)) {
			this.upstreamSubscription = s;
			try {
				if (s != Operators.empty()) {
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
    final static AtomicIntegerFieldUpdater<EventLoopProcessor> TERMINATED       =
			AtomicIntegerFieldUpdater.newUpdater(EventLoopProcessor.class, "terminated");

}



final class EventLoopFactory
		implements ThreadFactory, Supplier<String> {
	/** */

	static final AtomicInteger COUNT = new AtomicInteger();

	private static final long serialVersionUID = -3202326942393105842L;
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
