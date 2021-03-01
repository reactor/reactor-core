/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * A Processor implementation that takes a custom queue and allows
 * only a single subscriber. UnicastProcessor allows multiplexing of the events which
 * means that it supports multiple producers and only one consumer.
 * However, it should be noticed that multi-producer case is only valid if appropriate
 * Queue
 * is provided. Otherwise, it could break
 * <a href="https://www.reactive-streams.org/">Reactive Streams Spec</a> if Publishers
 * publish on different threads.
 *
 * <p>
 *      <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.2.0.M2/src/docs/marble/unicastprocessornormal.png" alt="">
 * </p>
 *
 * </br>
 * </br>
 *
 * <p>
 *      <b>Note: </b> UnicastProcessor does not respect the actual subscriber's
 *      demand as it is described in
 *      <a href="https://www.reactive-streams.org/">Reactive Streams Spec</a>. However,
 *      UnicastProcessor embraces configurable Queue internally which allows enabling
 *      backpressure support and preventing of consumer's overwhelming.
 *
 *      Hence, interaction model between producers and UnicastProcessor will be PUSH
 *      only. In opposite, interaction model between UnicastProcessor and consumer will be
 *      PUSH-PULL as defined in
 *      <a href="https://www.reactive-streams.org/">Reactive Streams Spec</a>.
 *
 *      In the case when upstream's signals overflow the bound of internal Queue,
 *      UnicastProcessor will fail with signaling onError(
 *      {@literal reactor.core.Exceptions.OverflowException}).
 *
 *      <p>
 *         <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.2.0.M2/src/docs/marble/unicastprocessoroverflow.png" alt="">
 *      </p>
 * </p>
 *
 * </br>
 * </br>
 *
 * <p>
 *      <b>Note: </b> The implementation keeps the order of signals. That means that in
 *      case of terminal signal (completion or error signals) it will be postponed
 *      until all of the previous signals has been consumed.
 *      <p>
 *         <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.2.0.M2/src/docs/marble/unicastprocessorterminal.png" alt="">
 *      </p>
 * </p>
 *
 * @param <T> the input and output type
 * @deprecated to be removed in 3.5, prefer clear cut usage of {@link Sinks} through
 * variations under {@link reactor.core.publisher.Sinks.UnicastSpec Sinks.many().unicast()}.
 */
@Deprecated
public final class UnicastProcessor<T> extends FluxProcessor<T, T>
		implements Fuseable.QueueSubscription<T>, Fuseable, InnerOperator<T, T>,
		           InternalManySink<T> {

	/**
	 * Create a new {@link UnicastProcessor} that will buffer on an internal queue in an
	 * unbounded fashion.
	 *
	 * @param <E> the relayed type
	 * @return a unicast {@link FluxProcessor}
	 * @deprecated use {@link Sinks.UnicastSpec#onBackpressureBuffer() Sinks.many().unicast().onBackpressureBuffer()}
	 * (or the unsafe variant if you're sure about external synchronization). To be removed in 3.5.
	 */
	@Deprecated
	public static <E> UnicastProcessor<E> create() {
		return new UnicastProcessor<>(Queues.<E>unbounded().get());
	}

	/**
	 * Create a new {@link UnicastProcessor} that will buffer on a provided queue in an
	 * unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param <E> the relayed type
	 * @return a unicast {@link FluxProcessor}
	 * @deprecated use {@link Sinks.UnicastSpec#onBackpressureBuffer(Queue) Sinks.many().unicast().onBackpressureBuffer(queue)}
	 * (or the unsafe variant if you're sure about external synchronization). To be removed in 3.5.
	 */
	@Deprecated
	public static <E> UnicastProcessor<E> create(Queue<E> queue) {
		return new UnicastProcessor<>(Hooks.wrapQueue(queue));
	}

	/**
	 * Create a new {@link UnicastProcessor} that will buffer on a provided queue in an
	 * unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param endcallback called on any terminal signal
	 * @param <E> the relayed type
	 * @return a unicast {@link FluxProcessor}
	 * @deprecated use {@link Sinks.UnicastSpec#onBackpressureBuffer(Queue, Disposable)  Sinks.many().unicast().onBackpressureBuffer(queue, endCallback)}
	 * (or the unsafe variant if you're sure about external synchronization). To be removed in 3.5.
	 */
	@Deprecated
	public static <E> UnicastProcessor<E> create(Queue<E> queue, Disposable endcallback) {
		return new UnicastProcessor<>(Hooks.wrapQueue(queue), endcallback);
	}

	/**
	 * Create a new {@link UnicastProcessor} that will buffer on a provided queue in an
	 * unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param endcallback called on any terminal signal
	 * @param onOverflow called when queue.offer return false and unicastProcessor is
	 * about to emit onError.
	 * @param <E> the relayed type
	 *
	 * @return a unicast {@link FluxProcessor}
	 * @deprecated use {@link Sinks.UnicastSpec#onBackpressureBuffer(Queue, Disposable)  Sinks.many().unicast().onBackpressureBuffer(queue, endCallback)}
	 * (or the unsafe variant if you're sure about external synchronization). The {@code onOverflow} callback is not
	 * supported anymore. To be removed in 3.5.
	 */
	@Deprecated
	public static <E> UnicastProcessor<E> create(Queue<E> queue,
			Consumer<? super E> onOverflow,
			Disposable endcallback) {
		return new UnicastProcessor<>(Hooks.wrapQueue(queue), onOverflow, endcallback);
	}

	final Queue<T>            queue;
	final Consumer<? super T> onOverflow;

	volatile Disposable onTerminate;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<UnicastProcessor, Disposable> ON_TERMINATE =
			AtomicReferenceFieldUpdater.newUpdater(UnicastProcessor.class, Disposable.class, "onTerminate");

	boolean   cancelled;
	boolean   done;
	Throwable error;

	CoreSubscriber<? super T> actual;

	static final long FLAG_TERMINATED      =
			0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
	static final long FLAG_DISPOSED        =
			0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
	static final long FLAG_CANCELLED       =
			0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
	static final long FLAG_SUBSCRIBER_READY =
			0b0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
	static final long FLAG_SUBSCRIBED_ONCE =
			0b0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
	static final long MAX_WIP_VALUE        =
			0b0000_0111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111L;

	volatile long state;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<UnicastProcessor> STATE =
			AtomicLongFieldUpdater.newUpdater(UnicastProcessor.class, "state");

	volatile int discardGuard;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<UnicastProcessor> DISCARD_GUARD =
			AtomicIntegerFieldUpdater.newUpdater(UnicastProcessor.class, "discardGuard");

	volatile long requested;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<UnicastProcessor> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(UnicastProcessor.class, "requested");

	boolean outputFused;

	public UnicastProcessor(Queue<T> queue) {
		this.queue = Objects.requireNonNull(queue, "queue");
		this.onTerminate = null;
		this.onOverflow = null;
	}

	public UnicastProcessor(Queue<T> queue, Disposable onTerminate) {
		this.queue = Objects.requireNonNull(queue, "queue");
		this.onTerminate = Objects.requireNonNull(onTerminate, "onTerminate");
		this.onOverflow = null;
	}

	@Deprecated
	public UnicastProcessor(Queue<T> queue,
			Consumer<? super T> onOverflow,
			Disposable onTerminate) {
		this.queue = Objects.requireNonNull(queue, "queue");
		this.onOverflow = Objects.requireNonNull(onOverflow, "onOverflow");
		this.onTerminate = Objects.requireNonNull(onTerminate, "onTerminate");
	}

	@Override
	public int getBufferSize() {
		return Queues.capacity(this.queue);
	}

	@Override
	public Stream<Scannable> inners() {
		final long state = this.state;
		final CoreSubscriber<? super T> actual = this.actual;
		return isSubscriberReady(state) ? Stream.of(Scannable.from(actual)) :
				Stream.empty();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (Attr.ACTUAL == key) return actual();
		if (Attr.BUFFERED == key) return queue.size();
		if (Attr.PREFETCH == key) return Integer.MAX_VALUE;
		if (Attr.CANCELLED == key) {
			final long state = this.state;
			return isCancelled(state) || isDisposed(state);
		}

		//TERMINATED and ERROR covered in super
		return super.scanUnsafe(key);
	}

	@Override
	public void onComplete() {
		//no particular error condition handling for onComplete
		@SuppressWarnings("unused") EmitResult emitResult = tryEmitComplete();
	}

	@Override
	public EmitResult tryEmitComplete() {
		if (this.done) {
			return EmitResult.FAIL_TERMINATED;
		}
		if (this.cancelled) {
			return EmitResult.FAIL_CANCELLED;
		}

		this.done = true;

		doTerminate();

		drain();
		return Sinks.EmitResult.OK;
	}

	@Override
	public void onError(Throwable throwable) {
		emitError(throwable, Sinks.EmitFailureHandler.FAIL_FAST);
	}

	@Override
	public Sinks.EmitResult tryEmitError(Throwable t) {
		if (this.done) {
			return Sinks.EmitResult.FAIL_TERMINATED;
		}
		if (this.cancelled) {
			return EmitResult.FAIL_CANCELLED;
		}

		this.error = t;
		this.done = true;

		doTerminate();

		drain();
		return EmitResult.OK;
	}

	@Override
	public void onNext(T t) {
		emitNext(t, Sinks.EmitFailureHandler.FAIL_FAST);
	}

	@Override
	public void emitNext(T value, Sinks.EmitFailureHandler failureHandler) {
		if (this.onOverflow == null) {
			InternalManySink.super.emitNext(value, failureHandler);
			return;
		}

		// TODO consider deprecating onOverflow and suggesting using a strategy instead
		InternalManySink.super.emitNext(
				value, (signalType, emission) -> {
					boolean shouldRetry = failureHandler.onEmitFailure(SignalType.ON_NEXT, emission);
					if (!shouldRetry) {
						switch (emission) {
							case FAIL_ZERO_SUBSCRIBER:
							case FAIL_OVERFLOW:
								try {
									this.onOverflow.accept(value);
								}
								catch (Throwable e) {
									Exceptions.throwIfFatal(e);
									emitError(e, Sinks.EmitFailureHandler.FAIL_FAST);
								}
								break;
						}
					}
					return shouldRetry;
				}
		);
	}

	@Override
	public EmitResult tryEmitNext(T t) {
		if (this.done) {
			return EmitResult.FAIL_TERMINATED;
		}
		if (this.cancelled) {
			return EmitResult.FAIL_CANCELLED;
		}

		if (!this.queue.offer(t)) {
			return isSubscriberReady(this.state) ? EmitResult.FAIL_OVERFLOW :
					EmitResult.FAIL_ZERO_SUBSCRIBER;
		}
		drain();
		return EmitResult.OK;
	}

	@Override
	public int currentSubscriberCount() {
		final long state = this.state;
		return isSubscriberReady(state) ? 1 : 0;
	}

	@Override
	public Flux<T> asFlux() {
		return this;
	}

	@Override
	protected boolean isIdentityProcessor() {
		return true;
	}

	void doTerminate() {
		Disposable r = onTerminate;
		if (r != null && ON_TERMINATE.compareAndSet(this, r, null)) {
			r.dispose();
		}
	}

	void drain() {
		long previousState = wipIncrement(this);
		if (isTerminated(previousState)) {
			this.clearSafely();
			return;
		}

		if (isWorkInProgress(previousState)) {
			return;
		}

		long expectedState = previousState + 1;
		for (;;) {
			if (isSubscriberReady(expectedState)) {
				final boolean outputFused = this.outputFused;
				final CoreSubscriber<? super T> a = this.actual;

				if (outputFused) {
					drainFused(expectedState, a);
				}
				else {
					if (isCancelled(expectedState)) {
						clearAndTerminate(this);
						return;
					}

					if (isDisposed(expectedState)) {
						clearAndTerminate(this);
						a.onError(new CancellationException("Disposed"));
						return;
					}

					drainRegular(expectedState, a);
				}
				return;
			}
			else {
				if (isCancelled(expectedState) || isDisposed(expectedState)) {
					clearAndTerminate(this);
					return;
				}
			}

			expectedState = wipRemoveMissing(this, expectedState);
			if (!isWorkInProgress(expectedState)) {
				return;
			}
		}
	}

	void drainRegular(long expectedState, CoreSubscriber<? super T> a) {
		final Queue<T> q = this.queue;

		for (;;) {

			long r = this.requested;
			long e = 0L;

			while (r != e) {
				// done has to be read before queue.poll to ensure there was no racing:
				// Thread1: <#drain>: queue.poll(null) --------------------> this.done(true)
				// Thread2: ------------------> <#onNext(V)> --> <#onComplete()>
				boolean done = this.done;

				T t;
				boolean empty;

				t = q.poll();
				empty = t == null;

				if (checkTerminated(done, empty, a, t)) {
					return;
				}

				if (empty) {
					break;
				}

				a.onNext(t);

				e++;
			}

			if (r == e) {
				// done has to be read before queue.isEmpty to ensure there was no racing:
				// Thread1: <#drain>: queue.isEmpty(true) --------------------> this.done(true)
				// Thread2: --------------------> <#onNext(V)> ---> <#onComplete()>
				if (checkTerminated(this.done, q.isEmpty(), a, null)) {
					return;
				}
			}

			if (e != 0 && r != Long.MAX_VALUE) {
				REQUESTED.addAndGet(this, -e);
			}

			expectedState = wipRemoveMissing(this, expectedState);
			if (isCancelled(expectedState)) {
				clearAndTerminate(this);
				return;
			}

			if (isDisposed(expectedState)) {
				clearAndTerminate(this);
				a.onError(new CancellationException("Disposed"));
				return;
			}

			if (!isWorkInProgress(expectedState)) {
				break;
			}
		}
	}

	void drainFused(long expectedState, CoreSubscriber<? super T> a) {
		for (;;) {
			// done has to be read before queue.poll to ensure there was no racing:
			// Thread1: <#drain>: queue.poll(null) --------------------> this.done(true)
			boolean d = this.done;

			a.onNext(null);

			if (d) {
				Throwable ex = this.error;
				if (ex != null) {
					a.onError(ex);
				}
				else {
					a.onComplete();
				}
				return;
			}

			expectedState = wipRemoveMissing(this, expectedState);
			if (isCancelled(expectedState)) {
				return;
			}

			if (isDisposed(expectedState)) {
				a.onError(new CancellationException("Disposed"));
				return;
			}

			if (!isWorkInProgress(expectedState)) {
				break;
			}
		}
	}

	boolean checkTerminated(boolean done,
			boolean empty,
			CoreSubscriber<? super T> a,
			@Nullable T value) {
		final long state = this.state;
		if (isCancelled(state)) {
			if (!empty) {
				Operators.onDiscard(value, currentContext());
			}
			clearAndTerminate(this);
			return true;
		}

		if (isDisposed(state)) {
			if (!empty) {
				Operators.onDiscard(value, currentContext());
			}
			clearAndTerminate(this);
			a.onError(new CancellationException("Disposed"));
			return true;
		}

		if (done && empty) {
			clearAndTerminate(this);
			Throwable e = this.error;
			if (e != null) {
				a.onError(e);
			}
			else {
				a.onComplete();
			}
			return true;
		}

		return false;
	}

	@Override
	public void onSubscribe(Subscription s) {
		final long state = this.state;
		if (this.done || isTerminated(state) || isCancelled(state) || isDisposed(state)) {
			s.cancel();
		}
		else {
			s.request(Long.MAX_VALUE);
		}
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Context currentContext() {
		final long state = this.state;
		if (isSubscriberReady(state) || isTerminated(state)) {
			final CoreSubscriber<? super T> actual = this.actual;
			return actual != null ? actual.currentContext() : Context.empty();
		}

		return Context.empty();
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Objects.requireNonNull(actual, "subscribe");
		if (markSubscribedOnce(this)) {
			actual.onSubscribe(this);
			this.actual = actual;
			long previousState = markSubscriberReady(this);
			if (isCancelled(previousState)) {
				return;
			}
			if (isDisposed(previousState)) {
				actual.onError(new CancellationException("Disposed"));
				return;
			}
			if (isWorkInProgress(previousState)) {
				return;
			}
			drain();
		}
		else {
			Operators.error(actual,
					new IllegalStateException(
							"UnicastProcessor allows only a single Subscriber"));
		}
	}

	@Override
	public void request(long n) {
		if (Operators.validate(n)) {
			Operators.addCap(REQUESTED, this, n);
			drain();
		}
	}

	@Override
	public void cancel() {
		this.cancelled = true;

		final long previousState = markCancelled(this);
		if (isTerminated(previousState) || isCancelled(previousState) || isDisposed(
				previousState) || isWorkInProgress(previousState)) {
			return;
		}

		if (!isSubscriberReady(previousState) || !this.outputFused) {
			clearAndTerminate(this);
		}
	}

	@Override
	public void dispose() {
		this.cancelled = true;

		final long previousState = markDisposed(this);
		if (isTerminated(previousState) || isCancelled(previousState) || isDisposed(
				previousState) || isWorkInProgress(previousState)) {
			return;
		}

		if (!isSubscriberReady(previousState)) {
			clearAndTerminate(this);
			return;
		}

		if (!this.outputFused) {
			clearAndTerminate(this);
		}
		this.actual.onError(new CancellationException("Disposed"));
	}

	@Override
	@Nullable
	public T poll() {
		return this.queue.poll();
	}

	@Override
	public int size() {
		return this.queue.size();
	}

	@Override
	public boolean isEmpty() {
		return this.queue.isEmpty();
	}

	/**
	 * Clears all elements from queues and set state to terminate. This method MUST be
	 * called only by the downstream subscriber which has enabled {@link Fuseable#ASYNC}
	 * fusion with the given {@link UnicastProcessor} and is and indicator that the
	 * downstream is done with draining, it has observed any terminal signal (ON_COMPLETE
	 * or ON_ERROR or CANCEL) and will never be interacting with SingleConsumer queue
	 * anymore.
	 */
	@Override
	public void clear() {
		clearAndTerminate(this);
	}

	void clearSafely() {
		if (DISCARD_GUARD.getAndIncrement(this) != 0) {
			return;
		}

		int missed = 1;
		for (;;) {
			clearUnsafely();

			missed = DISCARD_GUARD.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	void clearUnsafely() {
		Operators.onDiscardQueueWithClear(this.queue, currentContext(), null);
	}

	@Override
	public int requestFusion(int requestedMode) {
		if ((requestedMode & Fuseable.ASYNC) != 0) {
			this.outputFused = true;
			return Fuseable.ASYNC;
		}
		return Fuseable.NONE;
	}

	@Override
	public boolean isDisposed() {
		final long state = this.state;
		return isTerminated(state) || isCancelled(state) || isDisposed(state) || this.done || this.cancelled;
	}

	@Override
	public boolean isTerminated() {
		//noinspection unused
		final long state = this.state;
		return this.done;
	}

	@Override
	@Nullable
	public Throwable getError() {
		//noinspection unused
		final long state = this.state;
		if (this.done) {
			return this.error;
		}
		else {
			return null;
		}
	}

	@Override
	public CoreSubscriber<? super T> actual() {
		return isSubscriberReady(this.state) ? this.actual : null;
	}

	@Override
	public long downstreamCount() {
		return isSubscriberReady(this.state) ? 1L : 0L;
	}

	@Override
	public boolean hasDownstreams() {
		return isSubscriberReady(this.state);
	}

	/**
	 * Sets {@link #FLAG_SUBSCRIBED_ONCE} flag if it was not set before and if
	 * flags {@link #FLAG_TERMINATED}, {@link #FLAG_CANCELLED} or
	 * {@link #FLAG_DISPOSED} are unset
	 *
	 * @return {@code true} if {@link #FLAG_SUBSCRIBED_ONCE} was successfully set
	 */
	static boolean markSubscribedOnce(UnicastProcessor<?> instance) {
		for (; ; ) {
			long state = instance.state;

			if ((state & FLAG_TERMINATED) == FLAG_TERMINATED || (state & FLAG_SUBSCRIBED_ONCE) == FLAG_SUBSCRIBED_ONCE || (state & FLAG_CANCELLED) == FLAG_CANCELLED || (state & FLAG_DISPOSED) == FLAG_DISPOSED) {
				return false;
			}

			if (STATE.compareAndSet(instance, state, state | FLAG_SUBSCRIBED_ONCE)) {
				return true;
			}
		}
	}

	/**
	 * Sets {@link #FLAG_SUBSCRIBER_READY} flag if
	 * flags {@link #FLAG_TERMINATED}, {@link #FLAG_CANCELLED} or
	 * {@link #FLAG_DISPOSED} are unset
	 *
	 * @return previous state
	 */
	static long markSubscriberReady(UnicastProcessor<?> instance) {
		for (; ; ) {
			long state = instance.state;

			if ((state & FLAG_TERMINATED) == FLAG_TERMINATED || (state & FLAG_CANCELLED) == FLAG_CANCELLED || (state & FLAG_DISPOSED) == FLAG_DISPOSED) {
				return state;
			}

			if (STATE.compareAndSet(instance, state, state | FLAG_SUBSCRIBER_READY)) {
				return state;
			}
		}
	}

	/**
	 * Sets {@link #FLAG_CANCELLED} flag if it was not set before and if flag
	 * {@link #FLAG_TERMINATED} is unset. Also, this method increments number of work in
	 * progress (WIP)
	 *
	 * @return previous state
	 */
	static long markCancelled(UnicastProcessor<?> instance) {
		for (; ; ) {
			long state = instance.state;

			if ((state & FLAG_TERMINATED) == FLAG_TERMINATED || (state & FLAG_CANCELLED) == FLAG_CANCELLED) {
				return state;
			}

			long nextState = state + 1;
			if ((nextState & MAX_WIP_VALUE) == 0) {
				nextState = state;
			}

			if (STATE.compareAndSet(instance, state, nextState | FLAG_CANCELLED)) {
				return state;
			}
		}
	}

	/**
	 * Sets {@link #FLAG_DISPOSED} flag if it was not set before and if flags
	 * {@link #FLAG_TERMINATED}, {@link #FLAG_CANCELLED} are unset. Also,
	 * this method increments number of work in progress (WIP)
	 *
	 * @return previous state
	 */
	static long markDisposed(UnicastProcessor<?> instance) {
		for (; ; ) {
			long state = instance.state;

			if ((state & FLAG_TERMINATED) == FLAG_TERMINATED || (state & FLAG_CANCELLED) == FLAG_CANCELLED || (state & FLAG_DISPOSED) == FLAG_DISPOSED) {
				return state;
			}

			long nextState = state + 1;
			if ((nextState & MAX_WIP_VALUE) == 0) {
				nextState = state;
			}

			if (STATE.compareAndSet(instance, state, nextState | FLAG_DISPOSED)) {
				return state;
			}
		}
	}

	/**
	 * Tries to increment the amount of work in progress (max value is {@link
	 * #MAX_WIP_VALUE} on the given state. Fails if flag {@link #FLAG_TERMINATED} is set.
	 *
	 * @return previous state
	 */
	static long wipIncrement(UnicastProcessor<?> instance) {
		for (; ; ) {
			long state = instance.state;

			if ((state & FLAG_TERMINATED) == FLAG_TERMINATED) {
				return state;
			}

			final long nextState = state + 1;
			if ((nextState & MAX_WIP_VALUE) == 0) {
				return state;
			}

			if (STATE.compareAndSet(instance, state, nextState)) {
				return state;
			}
		}
	}

	/**
	 * Tries to decrement the amount of work in progress by the given amount on the given
	 * state. Fails if flag is {@link #FLAG_TERMINATED} is set or if fusion disabled and
	 * flags {@link #FLAG_CANCELLED} or {@link #FLAG_DISPOSED} are set.
	 *
	 * <p>Note, if fusion is enabled, the decrement should work if flags
	 * {@link #FLAG_CANCELLED} or {@link #FLAG_DISPOSED} are set, since, while the
	 * operator was not terminate by the downstream, we still have to propagate
	 * notifications that new elements are enqueued
	 *
	 * @return state after changing WIP or current state if update failed
	 */
	static long wipRemoveMissing(UnicastProcessor<?> instance, long previousState) {
		long missed = previousState & MAX_WIP_VALUE;
		for (; ; ) {
			final long state = instance.state;

			if ((state & FLAG_TERMINATED) == FLAG_TERMINATED) {
				return state;
			}

			if ((!isSubscriberReady(state) || !instance.outputFused) && ((state & FLAG_CANCELLED) == FLAG_CANCELLED || (state & FLAG_DISPOSED) == FLAG_DISPOSED)) {
				return state;
			}

			final long nextState = state - missed;
			if (STATE.compareAndSet(instance, state, nextState)) {
				return nextState;
			}
		}
	}

	/**
	 * Set flag {@link #FLAG_TERMINATED} and discards all the elements from {@link
	 * #queue}.
	 *
	 * <p>This method may be called concurrently only if the given {@link
	 * UnicastProcessor} has no
	 * output fusion ({@link #outputFused} {@code == true}). Otherwise this method MUST be
	 * called once and only by the downstream calling method {@link #clear()}
	 */
	static void clearAndTerminate(UnicastProcessor<?> instance) {
		for (; ; ) {
			final long state = instance.state;

			if (!isSubscriberReady(state) || !instance.outputFused) {
				instance.clearSafely();
			}
			else {
				instance.clearUnsafely();
			}

			if ((state & FLAG_TERMINATED) == FLAG_TERMINATED) {
				return;
			}

			if (STATE.compareAndSet(instance,
					state,
					(state & ~MAX_WIP_VALUE) | FLAG_TERMINATED)) {
				instance.doTerminate();
				break;
			}
		}
	}

	static boolean isCancelled(long state) {
		return (state & FLAG_CANCELLED) == FLAG_CANCELLED;
	}

	static boolean isDisposed(long state) {
		return (state & FLAG_DISPOSED) == FLAG_DISPOSED;
	}

	static boolean isWorkInProgress(long state) {
		return (state & MAX_WIP_VALUE) != 0;
	}

	static boolean isTerminated(long state) {
		return (state & FLAG_TERMINATED) == FLAG_TERMINATED;
	}

	static boolean isSubscriberReady(long state) {
		return !isTerminated(state) && (state & FLAG_SUBSCRIBER_READY) == FLAG_SUBSCRIBER_READY;
	}

}
