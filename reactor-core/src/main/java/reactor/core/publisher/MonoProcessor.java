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
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.WaitStrategy;

/**
 * A {@code MonoProcessor} is a {@link Mono} extension that implements stateful semantics. Multi-subscribe is allowed.
 *
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/monoprocessor.png" alt="">
 * <p>
 *
 * Once a {@link MonoProcessor} has been resolved, newer subscribers will benefit from the cached result.
 *
 * @param <O> the type of the value that will be made available
 *
 * @author Stephane Maldini
 * @deprecated instantiate through {@link Processors#first} and use as a {@link BalancedMonoProcessor}
 */
@Deprecated
public final class MonoProcessor<O> extends Mono<O>
		implements CoreSubscriber<O>, Subscription,
		           Scannable,
		           LongSupplier,
		           BalancedMonoProcessor<O> {

	/**
	 * Create a {@link MonoProcessor} that will eagerly request 1 on {@link #onSubscribe(Subscription)}, cache and emit
	 * the eventual result for 1 or N subscribers.
	 *
	 * @param <T> type of the expected value
	 *
	 * @return A {@link MonoProcessor}.
	 */
	@Deprecated
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
	@Deprecated
	public static <T> MonoProcessor<T> create(WaitStrategy waitStrategy) {
		return new MonoProcessor<>(null, waitStrategy);
	}

	final WaitStrategy       waitStrategy;

	Publisher<? extends O>   source;
	Subscription             subscription;

	volatile FluxProcessor<O, O> processor; //deliberately initially null, don't use NOOP_PROCESSOR
	volatile O               value;
	volatile Throwable       error;
	volatile int             state;
	volatile int             wip;
	volatile int             connected;

	MonoProcessor(@Nullable Publisher<? extends O> source) {
		this(source, WaitStrategy.sleeping());
	}

	MonoProcessor(@Nullable Publisher<? extends O> source, WaitStrategy waitStrategy) {
		this.source = source;
		this.waitStrategy = Objects.requireNonNull(waitStrategy, "waitStrategy");
	}

	@Override
	public Mono<O> asMono() {
		return this;
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

	/**
	 * Block the calling thread indefinitely, waiting for the completion of this {@code MonoProcessor}. If the
	 * {@link MonoProcessor} is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value of this {@code MonoProcessor}
	 */
	@Override
	@Nullable
	public O block() {
		return block(WaitStrategy.NOOP_SPIN_OBSERVER);
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
	@Nullable
	public O block(Duration timeout) {
		long delay = System.nanoTime() + timeout.toNanos();
		Runnable spinObserver = () -> {
			if (delay < System.nanoTime()) {
				WaitStrategy.alert();
			}
		};
		return block(spinObserver);
	}

	@Nullable
	O block(Runnable spinObserver) {
		try {
			if (!isPending()) {
				return peek();
			}
			else if(subscription == null) {
				getOrStart();
			}

			try {
				long endState = waitStrategy.waitFor(STATE_SUCCESS_VALUE, this, spinObserver);

				switch ((int)endState) {
					case STATE_SUCCESS_VALUE:
						return value;
					case STATE_ERROR:
						RuntimeException re = Exceptions.propagate(error);
						re = Exceptions.addSuppressed(re, new Exception("Mono#block terminated with an error"));
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

	@Override
	@Nullable
	public final Throwable getError() {
		return error;
	}

	@Override
	public boolean isCancelled() {
		return state == STATE_CANCELLED;
	}

	@Override
	public final boolean isError() {
		return state == STATE_ERROR;
	}

	@Override
	public final boolean isSuccess() {
		return state == STATE_COMPLETE_NO_VALUE || state == STATE_SUCCESS_VALUE;
	}

	@Override
	public final boolean isTerminated() {
		return state > STATE_POST_SUBSCRIBED;
	}

	@Override
	public boolean isDisposed() {
		return isTerminated() || isCancelled();
	}

	@Override
	public final void onComplete() {
		Subscription s = subscription;
		int state = this.state;
		if ((source != null && s == null) || state >= STATE_SUCCESS_VALUE) {
			return;
		}
		subscription = null;
		source = null;

		final int finalState = STATE_COMPLETE_NO_VALUE;

		for (; ; ) {
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
	public final void onError(Throwable cause) {
		Subscription s = subscription;

		if ((source != null && s == null) || this.error != null) {
			Operators.onErrorDroppedMulticast(cause);
			return;
		}

		this.error = cause;
		subscription = null;
		source = null;

		int state = this.state;
		for (; ; ) {
			if (state != STATE_READY && state != STATE_SUBSCRIBED && state != STATE_POST_SUBSCRIBED) {
				Operators.onErrorDroppedMulticast(cause);
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
			Operators.onNextDroppedMulticast(value);
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
		else { //shouldn't happen
			finalState = STATE_COMPLETE_NO_VALUE;
		}

		source = null;

		int state = this.state;
		for (; ; ) {
			if (state != STATE_READY && state != STATE_SUBSCRIBED && state != STATE_POST_SUBSCRIBED) {
				if(value != null) {
					Operators.onNextDroppedMulticast(value);
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
	@Nullable
	public O peek() {
		int endState = this.state;

		if (endState == STATE_SUCCESS_VALUE) {
			return value;
		}
		else if (endState == STATE_ERROR) {
			RuntimeException re = Exceptions.propagate(error);
			re = Exceptions.addSuppressed(re, new Exception("Mono#peek terminated with an error"));
			throw re;
		}
		else {
			return null;
		}
	}

	@Override
	public final void request(long n) {
		if (Operators.validate(n) &&
				WIP.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	@Override
	public void subscribe(final CoreSubscriber<? super O> actual) {
		for (; ; ) {
			int endState = this.state;
			if (endState == STATE_COMPLETE_NO_VALUE) {
				Operators.complete(actual);
				return;
			}
			else if (endState == STATE_SUCCESS_VALUE) {
				actual.onSubscribe(Operators.scalarSubscription(actual, value));
				return;
			}
			else if (endState == STATE_ERROR) {
				Operators.error(actual, error);
				return;
			}
			else if (endState == STATE_CANCELLED) {
				Operators.error(actual, new CancellationException("Mono has previously been cancelled"));
				return;
			}
			Processor<O, O> out = getOrStart();
			if (out == NOOP_PROCESSOR) {
				continue;
			}
			out.subscribe(actual);
			break;
		}

		if (WIP.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.ACTUAL) return processor;
		if (key == Attr.PARENT) return subscription;
		if (key == Attr.ERROR) return error;
		if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
		if (key == Attr.CANCELLED) return isCancelled();
		if (key == Attr.TERMINATED) return isTerminated();
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

	@Override
	public final long downstreamCount() {
		//noinspection ConstantConditions
		return Scannable.from(processor).inners().count();
	}

	@Override
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

			if (subscription != null && state == STATE_CANCELLED) {
				FluxProcessor<O,O> p = PROCESSOR.getAndSet(this, NOOP_PROCESSOR);
				if (p != NOOP_PROCESSOR) {
					this.subscription = null;
					this.source = null;
					subscription.cancel();
					//we need p = null as a 3rd possible value to detect the case were we should null out the source/subscription
					if (p != null) {
						p.dispose();
					}
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
	FluxProcessor<O, O> getOrStart(){
		FluxProcessor<O, O> out = processor;
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

	@Override
	public boolean isSerialized() {
		return false;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
    final static class NoopProcessor extends FluxProcessor {

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
		public void subscribe(CoreSubscriber actual) {

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
	final static AtomicReferenceFieldUpdater<MonoProcessor, FluxProcessor> PROCESSOR =
		    AtomicReferenceFieldUpdater.newUpdater(MonoProcessor.class, FluxProcessor.class,
				    "processor");
	final static int       STATE_CANCELLED         = -1;
	final static int       STATE_READY             = 0;
	final static int       STATE_SUBSCRIBED        = 1;
	final static int       STATE_POST_SUBSCRIBED   = 2;
	final static int       STATE_SUCCESS_VALUE     = 3;
	final static int       STATE_COMPLETE_NO_VALUE = 4;
	final static int       STATE_ERROR             = 5;
}