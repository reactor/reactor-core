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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.MultiProducer;
import reactor.core.flow.Receiver;
import reactor.core.queue.RingBuffer;
import reactor.core.queue.Slot;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.ExecutorUtils;
import reactor.core.util.WaitStrategy;

/**
 * A base processor used by executor backed processors to take care of their ExecutorService
 *
 * @author Stephane Maldini
 */
abstract class EventLoopProcessor<IN> extends FluxProcessor<IN, IN>
		implements Cancellable, Receiver, Runnable, MultiProducer, Backpressurable {

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
	@SuppressWarnings("unused")
	volatile       int                                                  subscriberCount  = 0;

	EventLoopProcessor(String name,
			int bufferSize,
			ExecutorService executor,
			boolean autoCancel,
			boolean multiproducers,
			Supplier<Slot<IN>> factory,
			WaitStrategy strategy) {

		if (!RingBuffer.isPowerOfTwo(bufferSize)) {
			throw new IllegalArgumentException("bufferSize must be a power of 2 : " + bufferSize);
		}
		
		this.autoCancel = autoCancel;

		contextClassLoader = new EventLoopContext();

		this.name = null != name ? name : getClass().getSimpleName();

		if (executor == null) {
			this.executor = ExecutorUtils.singleUse(name, contextClassLoader);
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
	 * Block until all submitted tasks have completed, then do a normal {@link #shutdown()}.
	 */
	final public boolean awaitAndShutdown() {
		return awaitAndShutdown(-1, TimeUnit.SECONDS);
	}

	/**
	 * Block until all submitted tasks have completed, then do a normal {@link #shutdown()}.
	 */
	final public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
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
	final public int getMode() {
		return UNIQUE;
	}

	@Override
	final public String getName() {
		return "/Processors/" + name;
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
	final public Object key() {
		return contextClassLoader.hashCode();
	}

	@Override
	final public void onComplete() {
		if (TERMINATED.compareAndSet(this, 0, SHUTDOWN)) {
			upstreamSubscription = null;
			ExecutorUtils.shutdownIfSingleUse(executor);
			readWait.signalAllWhenBlocking();
			doComplete();
		}
	}

	@Override
	final public void onError(Throwable t) {
		super.onError(t);
		if (TERMINATED.compareAndSet(this, 0, SHUTDOWN)) {
			error = t;
			upstreamSubscription = null;
			ExecutorUtils.shutdownIfSingleUse(executor);
			readWait.signalAllWhenBlocking();
			doError(t);
		}
	}


	@Override
	final public void onNext(IN o) {
		super.onNext(o);
		RingBuffer.onNext(o, ringBuffer);
	}

	@Override
	final public void onSubscribe(final Subscription s) {
		if (BackpressureUtils.validate(upstreamSubscription, s)) {
			this.upstreamSubscription = s;
			try {
				if (s != EmptySubscription.INSTANCE) {
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
	final public void shutdown() {
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
		return ringBuffer.getCapacity();
	}

	final void cancel() {
		cancelled = true;
		if (TERMINATED.compareAndSet(this, 0, SHUTDOWN)) {
			ExecutorUtils.shutdownIfSingleUse(executor);
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
			EmptySubscription.error(subscriber, t);
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
	static final AtomicIntegerFieldUpdater<EventLoopProcessor> SUBSCRIBER_COUNT =
			AtomicIntegerFieldUpdater.newUpdater(EventLoopProcessor.class, "subscriberCount");
	final static AtomicIntegerFieldUpdater<EventLoopProcessor> TERMINATED       =
			AtomicIntegerFieldUpdater.newUpdater(EventLoopProcessor.class, "terminated");
}
