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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.LongSupplier;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.concurrent.WaitStrategy;

/**
 * A {@code MonoProcessor} is a {@link Mono} extension that implements stateful semantics. Multi-subscribe is allowed.
 *
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.0.M1/src/docs/marble/monoprocessor.png" alt="">
 * <p>
 *
 * Once a {@link MonoProcessor} has been resolved, newer subscribers will benefit from the cached result.
 *
 * @param <O> the type of the value that will be made available
 *
 * @author Stephane Maldini
 */
public final class MonoProcessor<O> extends Mono<O>
		implements Processor<O, O>, Disposable, Subscription, Scannable, LongSupplier {

	/**
	 * Create a {@link MonoProcessor} that will eagerly request 1 on {@link #onSubscribe(Subscription)}, cache and emit
	 * the eventual result for 1 or N subscribers.
	 *
	 * @param <T> type of the expected value
	 *
	 * @return A {@link MonoProcessor}.
	 */
	public static <T> MonoProcessor<T> create() {
		return new MonoProcessor<>(null);
	}

	/**
	 * Create a {@link MonoProcessor} that will eagerly request 1 on {@link #onSubscribe(Subscription)}, cache and emit
	 * the eventual result for 1 or N subscribers.
	 *
	 * @param waitStrategy a {@link WaitStrategy} for blocking {@link #block} strategy
	 * @param <T> type of the expected value
	 *
	 * @return A {@link MonoProcessor}.
	 */
	public static <T> MonoProcessor<T> create(WaitStrategy waitStrategy) {
		return new MonoProcessor<>(null, waitStrategy);
	}

	final Publisher<? extends O> source;
	final WaitStrategy           waitStrategy;

	Subscription subscription;
	volatile Processor<O, O> processor;
	volatile O               value;
	volatile Throwable       error;
	volatile int             state;
	volatile int             wip;
	volatile int             connected;

	MonoProcessor(Publisher<? extends O> source) {
		this(source, WaitStrategy.sleeping());
	}

	MonoProcessor(Publisher<? extends O> source, WaitStrategy waitStrategy) {
		this.source = source;
		this.waitStrategy = Objects.requireNonNull(waitStrategy, "waitStrategy");
	}

	@Override
	public final void cancel() {
		int state = this.state;
		for (; ; ) {
			if (state != STATE_READY && state != STATE_SUBSCRIBED && state != STATE_POST_SUBSCRIBED) {
				return;
			}
			if (STATE.compareAndSet(this, state, STATE_CANCELLED)) {
				break;
			}
			state = this.state;
		}
		if (WIP.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	@Override
	public void dispose() {
		cancel();
	}

	@Override
	public O block() {
		return block(Duration.ofSeconds(300));
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code MonoProcessor}. If the
	 * {@link MonoProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @param timeout the timeout value as a {@link Duration}
	 *
	 * @return the value of this {@code MonoProcessor} or {@code null} if the timeout is reached and the {@code MonoProcessor} has
	 * not completed
	 */
	@Override
	public O block(Duration timeout) {
		try {
			if (!isPending()) {
				return peek();
			}
			else if(subscription == null) {
				getOrStart();
			}

			long delay = System.nanoTime() + timeout.toNanos();

			try {
				long endState = waitStrategy.waitFor(STATE_SUCCESS_VALUE, this,	() -> {
					if (delay < System.nanoTime()) {
						WaitStrategy.alert();
					}
				});

				switch ((int)endState) {
					case STATE_SUCCESS_VALUE:
						return value;
					case STATE_ERROR:
						RuntimeException re = Exceptions.propagate(error);
						re.addSuppressed(new Exception("Mono#block terminated with an error"));
						throw re;
					case STATE_COMPLETE_NO_VALUE:
						return null;
				}
				throw new IllegalStateException("Mono has been cancelled");
			}
			catch (RuntimeException ce) {
				if(WaitStrategy.isAlert(ce)) {
					cancel();
					throw new IllegalStateException("Timeout on Mono blocking read");
				}
				throw ce;
			}
		}
		catch (InterruptedException ie) {
			Thread.currentThread().interrupt();

			throw new IllegalStateException("Thread Interruption on Mono blocking read");
		}
	}

	/**
	 * Return the produced {@link Throwable} error if any or null
	 *
	 * @return the produced {@link Throwable} error if any or null
	 */
	public final Throwable getError() {
		return error;
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been interrupted via cancellation.
	 *
	 * @return {@code true} if this {@code MonoProcessor} is cancelled, {@code false}
	 * otherwise.
	 */
	public boolean isCancelled() {
		return state == STATE_CANCELLED;
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been completed with an error.
	 *
	 * @return {@code true} if this {@code MonoProcessor} was completed with an error, {@code false} otherwise.
	 */
	public final boolean isError() {
		return state == STATE_ERROR;
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been successfully completed a value.
	 *
	 * @return {@code true} if this {@code MonoProcessor} is successful, {@code false} otherwise.
	 */
	public final boolean isSuccess() {
		return state == STATE_COMPLETE_NO_VALUE || state == STATE_SUCCESS_VALUE;
	}

	/**
	 * Indicates whether this {@code MonoProcessor} has been terminated by the
	 * source producer with a success or an error.
	 *
	 * @return {@code true} if this {@code MonoProcessor} is successful, {@code false} otherwise.
	 */
	public final boolean isTerminated() {
		return state > STATE_POST_SUBSCRIBED;
	}

	@Override
	public boolean isDisposed() {
		return isTerminated() || isCancelled();
	}

	@Override
	public final void onComplete() {
		onNext(null);
	}

	@Override
	public final void onError(Throwable cause) {
		Subscription s = subscription;

		if ((source != null && s == null) || this.error != null) {
			Operators.onErrorDropped(cause);
			return;
		}

		this.error = cause;
		subscription = null;

		int state = this.state;
		for (; ; ) {
			if (state != STATE_READY && state != STATE_SUBSCRIBED && state != STATE_POST_SUBSCRIBED) {
				Operators.onErrorDropped(cause);
				return;
			}
			if (STATE.compareAndSet(this, state, STATE_ERROR)) {
				waitStrategy.signalAllWhenBlocking();
				break;
			}
			state = this.state;
		}
		if (WIP.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	@Override
	public final void onNext(O value) {
		Subscription s = subscription;

		if (value != null && ((source != null && s == null) || this.value != null)) {
			Operators.onNextDropped(value);
			return;
		}
		subscription = null;

		final int finalState;
		if(value != null) {
			finalState = STATE_SUCCESS_VALUE;
			this.value = value;
			if (s != null && !(source instanceof Mono)) {
				s.cancel();
			}
		}
		else {
			finalState = STATE_COMPLETE_NO_VALUE;
		}
		int state = this.state;
		for (; ; ) {
			if (state != STATE_READY && state != STATE_SUBSCRIBED && state != STATE_POST_SUBSCRIBED) {
				if(value != null) {
					Operators.onNextDropped(value);
				}
				return;
			}
			if (STATE.compareAndSet(this, state, finalState)) {
				waitStrategy.signalAllWhenBlocking();
				break;
			}
			state = this.state;
		}


		if (WIP.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	@Override
	public final void onSubscribe(Subscription subscription) {
		if (Operators.validate(this.subscription, subscription)) {
			this.subscription = subscription;
			if (STATE.compareAndSet(this, STATE_READY, STATE_SUBSCRIBED)){
				subscription.request(Long.MAX_VALUE);
			}

			if (WIP.getAndIncrement(this) == 0) {
				drainLoop();
			}
		}
	}

	/**
	 * Returns the internal state from -1 Cancelled to 5 errored, beyond 3 included is
	 * fulfilled.
	 *
	 * @return the internal state from -1 Cancelled to 5 errored, beyond 3 included is
	 * fulfilled.
	 */
	@Override
	public long getAsLong() {
		return state;
	}

	/**
	 * Returns the value that completed this {@link MonoProcessor}. Returns {@code null} if the {@link MonoProcessor} has not been completed. If the
	 * {@link MonoProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value that completed the {@link MonoProcessor}, or {@code null} if it has not been completed
	 *
	 * @throws RuntimeException if the {@link MonoProcessor} was completed with an error
	 */
	public O peek() {
		int endState = this.state;

		if (endState == STATE_SUCCESS_VALUE) {
			return value;
		}
		else if (endState == STATE_ERROR) {
			RuntimeException re = Exceptions.propagate(error);
			re.addSuppressed(new Exception("Mono#peek terminated with an error"));
			throw re;
		}
		else {
			return null;
		}
	}

	@Override
	public final void request(long n) {
		try {
			Operators.checkRequest(n);
		}
		catch (Throwable e) {
			Exceptions.throwIfFatal(e);
			onError(e);
		}
		if (WIP.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	@Override
	public void subscribe(final Subscriber<? super O> subscriber) {
		for (; ; ) {
			int endState = this.state;
			if (endState == STATE_COMPLETE_NO_VALUE) {
				Operators.complete(subscriber);
				return;
			}
			else if (endState == STATE_SUCCESS_VALUE) {
				subscriber.onSubscribe(Operators.scalarSubscription(subscriber, value));
				return;
			}
			else if (endState == STATE_ERROR) {
				Operators.error(subscriber, error);
				return;
			}
			else if (endState == STATE_CANCELLED) {
				Operators.error(subscriber, new CancellationException("Mono has previously been cancelled"));
				return;
			}
			Processor<O, O> out = getOrStart();
			if (out == NOOP_PROCESSOR) {
				continue;
			}
			out.subscribe(subscriber);
			break;
		}

		if (WIP.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == ScannableAttr.ACTUAL) return processor;
		if (key == ScannableAttr.PARENT) return subscription;
		if (key == ThrowableAttr.ERROR) return error;
		if (key == IntAttr.PREFETCH) return Integer.MAX_VALUE;
		if (key == BooleanAttr.CANCELLED) return isCancelled();
		if (key == BooleanAttr.TERMINATED) return isTerminated();
		return null;
	}

	final boolean isPending() {
		return !isTerminated() && !isCancelled();
	}

	final void connect() {
		if(CONNECTED.compareAndSet(this, 0, 1)){
			if(source == null){
				onSubscribe(Operators.emptySubscription());
			}
			else{
				source.subscribe(this);
			}
		}
	}

	/**
	 * Return the number of active {@link Subscriber} or {@literal -1} if untracked.
	 *
	 * @return the number of active {@link Subscriber} or {@literal -1} if untracked
	 */
	public final long downstreamCount() {
		return Scannable.from(processor).inners().count();
	}

	/**
	 * Return true if any {@link Subscriber} is actively subscribed
	 *
	 * @return true if any {@link Subscriber} is actively subscribed
	 */
	public final boolean hasDownstreams() {
		return downstreamCount() != 0;
	}

	@SuppressWarnings("unchecked")
	final void drainLoop() {
		int missed = 1;

		int state;
		for (; ; ) {
			state = this.state;
			if (state > STATE_POST_SUBSCRIBED) {
				Processor<O, O> p = PROCESSOR.getAndSet(this, NOOP_PROCESSOR);
				if (p != NOOP_PROCESSOR && p != null) {
					switch (state) {
						case STATE_COMPLETE_NO_VALUE:
							p.onComplete();
							break;
						case STATE_SUCCESS_VALUE:
							p.onNext(value);
							p.onComplete();
							break;
						case STATE_ERROR:
							p.onError(error);
							break;
					}
					return;
				}
			}
			Subscription subscription = this.subscription;

			if (subscription != null) {
				if (state == STATE_CANCELLED && PROCESSOR.getAndSet(this, NOOP_PROCESSOR) != NOOP_PROCESSOR) {
					this.subscription = null;
					subscription.cancel();
					return;
				}
			}

			if (state == STATE_SUBSCRIBED && STATE.compareAndSet(this, STATE_SUBSCRIBED, STATE_POST_SUBSCRIBED)) {
				Processor<O, O> p = PROCESSOR.get(this);
				if (p != null && p != NOOP_PROCESSOR) {
					p.onSubscribe(this);
				}
			}

			missed = WIP.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	@SuppressWarnings("unchecked")
	Processor<O, O> getOrStart(){
		Processor<O, O> out = processor;
		if (out == null) {
			out = ReplayProcessor.cacheLastOrDefault(value);
			if (PROCESSOR.compareAndSet(this, null, out)) {
				connect();
			}
			else {
				out = PROCESSOR.get(this);
			}
		}
		return out;
	}

	@SuppressWarnings("rawtypes")
    final static class NoopProcessor implements Processor {

		@Override
		public void onComplete() {

		}

		@Override
		public void onError(Throwable t) {

		}

		@Override
		public void onNext(Object o) {

		}

		@Override
		public void onSubscribe(Subscription s) {

		}

		@Override
		public void subscribe(Subscriber s) {

		}
	}
	final static NoopProcessor NOOP_PROCESSOR = new NoopProcessor();
	@SuppressWarnings("rawtypes")
    final static AtomicIntegerFieldUpdater<MonoProcessor>              STATE     =
			AtomicIntegerFieldUpdater.newUpdater(MonoProcessor.class, "state");
	@SuppressWarnings("rawtypes")
	final static AtomicIntegerFieldUpdater<MonoProcessor>              WIP       =
			AtomicIntegerFieldUpdater.newUpdater(MonoProcessor.class, "wip");
	@SuppressWarnings("rawtypes")
	final static AtomicIntegerFieldUpdater<MonoProcessor>              CONNECTED       =
			AtomicIntegerFieldUpdater.newUpdater(MonoProcessor.class, "connected");
	@SuppressWarnings("rawtypes")
	final static AtomicReferenceFieldUpdater<MonoProcessor, Processor> PROCESSOR =
		    AtomicReferenceFieldUpdater.newUpdater(MonoProcessor.class, Processor.class,
				    "processor");
	final static int       STATE_CANCELLED         = -1;
	final static int       STATE_READY             = 0;
	final static int       STATE_SUBSCRIBED        = 1;
	final static int       STATE_POST_SUBSCRIBED   = 2;
	final static int       STATE_SUCCESS_VALUE     = 3;
	final static int       STATE_COMPLETE_NO_VALUE = 4;
	final static int       STATE_ERROR             = 5;
}