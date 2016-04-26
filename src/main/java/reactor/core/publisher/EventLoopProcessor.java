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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.state.Cancellable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.ExecutorUtils;

/**
 * A base processor used by executor backed processors to take care of their ExecutorService
 *
 * @author Stephane Maldini
 */
public abstract class EventLoopProcessor<IN, OUT> extends FluxProcessor<IN, OUT>
		implements Cancellable, reactor.core.flow.Receiver {

	protected final ExecutorService executor;
	protected final ClassLoader     contextClassLoader;
	protected final String          name;
	protected final boolean         autoCancel;
	Subscription upstreamSubscription;
	volatile        boolean         cancelled;
	volatile        int             terminated;
	volatile        Throwable       error;
	@SuppressWarnings("unused")
	volatile       int                                                  subscriberCount  = 0;
	protected EventLoopProcessor(String name, ExecutorService executor,
			boolean autoCancel) {
		this.autoCancel = autoCancel;
		contextClassLoader = new EventLoopContext();
		this.name = null != name ? name : getClass().getSimpleName();
		if (executor == null) {
			this.executor = ExecutorUtils.singleUse(name, contextClassLoader);
		}
		else {
			this.executor = executor;
		}
	}

	/**
	 * Determine whether this {@code Processor} can be used.
	 *
	 * @return {@literal true} if this {@code Resource} is alive and can be used, {@literal false} otherwise.
	 */
	public boolean alive() {
		return 0 == terminated;
	}

	/**
	 * Block until all submitted tasks have completed, then do a normal {@link #shutdown()}.
	 */
	public boolean awaitAndShutdown() {
		return awaitAndShutdown(-1, TimeUnit.SECONDS);
	}

	/**
	 * Block until all submitted tasks have completed, then do a normal {@link #shutdown()}.
	 */
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
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
	public Flux<IN> forceShutdown() {
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
		return getCapacity();
	}

	@Override
	public final Throwable getError() {
		return error;
	}

	@Override
	public int getMode() {
		return UNIQUE;
	}

	@Override
	public String getName() {
		return "/Processors/" + name;
	}

	@Override
	public int hashCode() {
		return contextClassLoader.hashCode();
	}

	@Override
	public boolean isCancelled() {
		return cancelled;
	}

	/**
	 * @return true if the classLoader marker is detected in the current thread
	 */
	public final boolean isInContext() {
		return Thread.currentThread()
		             .getContextClassLoader() == contextClassLoader;
	}

	@Override
	public boolean isStarted() {
		return upstreamSubscription != null;
	}

	@Override
	public boolean isTerminated() {
		return terminated > 0;
	}

	/**
	 * @return true if the attached Subscribers will read exclusive sequences (akin to work-queue pattern)
	 */
	public abstract boolean isWork();

	@Override
	public Object key() {
		return contextClassLoader.hashCode();
	}

	@Override
	public final void onComplete() {
		if (TERMINATED.compareAndSet(this, 0, SHUTDOWN)) {
			upstreamSubscription = null;
			ExecutorUtils.shutdownIfSingleUse(executor);
			doComplete();
		}
	}

	@Override
	public final void onError(Throwable t) {
		super.onError(t);
		if (TERMINATED.compareAndSet(this, 0, SHUTDOWN)) {
			error = t;
			upstreamSubscription = null;
			ExecutorUtils.shutdownIfSingleUse(executor);
			doError(t);
		}
	}

	@Override
	public void onSubscribe(final Subscription s) {
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
	public void shutdown() {
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
	public Subscription upstream() {
		return upstreamSubscription;
	}

	protected void cancel(Subscription subscription) {
		cancelled = true;
		if (TERMINATED.compareAndSet(this, 0, SHUTDOWN)) {
			ExecutorUtils.shutdownIfSingleUse(executor);
		}
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
				cancel(subscription);
			}
			return subs;
		}
		return subs;
	}

	abstract void doError(Throwable throwable);

	final boolean incrementSubscribers() {
		return SUBSCRIBER_COUNT.getAndIncrement(this) == 0;
	}

	final boolean startSubscriber(Subscriber<? super OUT> subscriber, Subscription subscription){
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
	protected static final int SHUTDOWN = 1;
	protected static final int                                           FORCED_SHUTDOWN  = 2;
	protected static final AtomicIntegerFieldUpdater<EventLoopProcessor> SUBSCRIBER_COUNT =
			AtomicIntegerFieldUpdater.newUpdater(EventLoopProcessor.class, "subscriberCount");
	protected final static AtomicIntegerFieldUpdater<EventLoopProcessor> TERMINATED       =
			AtomicIntegerFieldUpdater.newUpdater(EventLoopProcessor.class, "terminated");
}
