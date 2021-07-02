/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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

	volatile boolean done;
	Throwable error;

	boolean hasDownstream; //important to not loose the downstream too early and miss discard hook, while having relevant hasDownstreams()
	volatile CoreSubscriber<? super T> actual;

	volatile boolean cancelled;

	volatile int once;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<UnicastProcessor> ONCE =
			AtomicIntegerFieldUpdater.newUpdater(UnicastProcessor.class, "once");

	volatile int wip;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<UnicastProcessor> WIP =
			AtomicIntegerFieldUpdater.newUpdater(UnicastProcessor.class, "wip");

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
		return hasDownstream ? Stream.of(Scannable.from(actual)) : Stream.empty();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (Attr.ACTUAL == key) return actual();
		if (Attr.BUFFERED == key) return queue.size();
		if (Attr.PREFETCH == key) return Integer.MAX_VALUE;
		if (Attr.CANCELLED == key) return cancelled;

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
		if (done) {
			return EmitResult.FAIL_TERMINATED;
		}
		if (cancelled) {
			return EmitResult.FAIL_CANCELLED;
		}

		done = true;

		doTerminate();

		drain(null);
		return Sinks.EmitResult.OK;
	}

	@Override
	public void onError(Throwable throwable) {
		emitError(throwable, Sinks.EmitFailureHandler.FAIL_FAST);
	}

	@Override
	public Sinks.EmitResult tryEmitError(Throwable t) {
		if (done) {
			return Sinks.EmitResult.FAIL_TERMINATED;
		}
		if (cancelled) {
			return EmitResult.FAIL_CANCELLED;
		}

		error = t;
		done = true;

		doTerminate();

		drain(null);
		return EmitResult.OK;
	}

	@Override
	public void onNext(T t) {
		emitNext(t, Sinks.EmitFailureHandler.FAIL_FAST);
	}

	@Override
	public void emitNext(T value, Sinks.EmitFailureHandler failureHandler) {
		if (onOverflow == null) {
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
									onOverflow.accept(value);
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
		if (done) {
			return EmitResult.FAIL_TERMINATED;
		}
		if (cancelled) {
			return EmitResult.FAIL_CANCELLED;
		}

		if (!queue.offer(t)) {
			return (once > 0) ? EmitResult.FAIL_OVERFLOW : EmitResult.FAIL_ZERO_SUBSCRIBER;
		}
		drain(t);
		return EmitResult.OK;
	}

	@Override
	public int currentSubscriberCount() {
		return hasDownstream ? 1 : 0;
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

	void drainRegular(CoreSubscriber<? super T> a) {
		int missed = 1;

		final Queue<T> q = queue;

		for (;;) {

			long r = requested;
			long e = 0L;

			while (r != e) {
				boolean d = done;

				T t = q.poll();
				boolean empty = t == null;

				if (checkTerminated(d, empty, a, q, t)) {
					return;
				}

				if (empty) {
					break;
				}

				a.onNext(t);

				e++;
			}

			if (r == e) {
				if (checkTerminated(done, q.isEmpty(), a, q, null)) {
					return;
				}
			}

			if (e != 0 && r != Long.MAX_VALUE) {
				REQUESTED.addAndGet(this, -e);
			}

			missed = WIP.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	void drainFused(CoreSubscriber<? super T> a) {
		int missed = 1;

		for (;;) {

			if (cancelled) {
				// We are the holder of the queue, but we still have to perform discarding under the guarded block
				// to prevent any racing done by downstream
				this.clear();
				hasDownstream = false;
				return;
			}

			boolean d = done;

			a.onNext(null);

			if (d) {
				hasDownstream = false;

				Throwable ex = error;
				if (ex != null) {
					a.onError(ex);
				} else {
					a.onComplete();
				}
				return;
			}

			missed = WIP.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	void drain(@Nullable T dataSignalOfferedBeforeDrain) {
		if (WIP.getAndIncrement(this) != 0) {
			if (dataSignalOfferedBeforeDrain != null) {
				if (cancelled) {
					Operators.onDiscard(dataSignalOfferedBeforeDrain,
							actual.currentContext());
				}
				else if (done) {
					Operators.onNextDropped(dataSignalOfferedBeforeDrain,
							currentContext());
				}
			}
			return;
		}

		int missed = 1;

		for (;;) {
			CoreSubscriber<? super T> a = actual;
			if (a != null) {

				if (outputFused) {
					drainFused(a);
				} else {
					drainRegular(a);
				}
				return;
			}

			missed = WIP.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	boolean checkTerminated(boolean d, boolean empty, CoreSubscriber<? super T> a, Queue<T> q, @Nullable T t) {
		if (cancelled) {
			Operators.onDiscard(t, a.currentContext());
			Operators.onDiscardQueueWithClear(q, a.currentContext(), null);
			hasDownstream = false;
			return true;
		}
		if (d && empty) {
			Throwable e = error;
			hasDownstream = false;
			if (e != null) {
				a.onError(e);
			} else {
				a.onComplete();
			}
			return true;
		}

		return false;
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (done || cancelled) {
			s.cancel();
		} else {
			s.request(Long.MAX_VALUE);
		}
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Context currentContext() {
		CoreSubscriber<? super T> actual = this.actual;
		return actual != null ? actual.currentContext() : Context.empty();
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Objects.requireNonNull(actual, "subscribe");
		if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {

			this.hasDownstream = true;
			actual.onSubscribe(this);
			this.actual = actual;
			if (cancelled) {
				this.hasDownstream = false;
			} else {
				drain(null);
			}
		} else {
			Operators.error(actual, new IllegalStateException("UnicastProcessor " +
					"allows only a single Subscriber"));
		}
	}

	@Override
	public void request(long n) {
		if (Operators.validate(n)) {
			Operators.addCap(REQUESTED, this, n);
			drain(null);
		}
	}

	@Override
	public void cancel() {
		if (cancelled) {
			return;
		}
		cancelled = true;

		doTerminate();

		if (WIP.getAndIncrement(this) == 0) {
			if (!outputFused) {
				// discard MUST be happening only and only if there is no racing on elements consumption
				// which is guaranteed by the WIP guard here in case non-fused output
				Operators.onDiscardQueueWithClear(queue, currentContext(), null);
			}
			hasDownstream = false;
		}
	}

	@Override
	@Nullable
	public T poll() {
		return queue.poll();
	}

	@Override
	public int size() {
		return queue.size();
	}

	@Override
	public boolean isEmpty() {
		return queue.isEmpty();
	}

	@Override
	public void clear() {
		// use guard on the queue instance as the best way to ensure there is no racing on draining
		// the call to this method must be done only during the ASYNC fusion so all the callers will be waiting
		// this should not be performance costly with the assumption the cancel is rare operation
		if (DISCARD_GUARD.getAndIncrement(this) != 0) {
			return;
		}

		int missed = 1;

		for (;;) {
			Operators.onDiscardQueueWithClear(queue, currentContext(), null);

			int dg = discardGuard;
			if (missed == dg) {
				missed = DISCARD_GUARD.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
			else {
				missed = dg;
			}
		}
	}

	@Override
	public int requestFusion(int requestedMode) {
		if ((requestedMode & Fuseable.ASYNC) != 0) {
			outputFused = true;
			return Fuseable.ASYNC;
		}
		return Fuseable.NONE;
	}

	@Override
	public boolean isDisposed() {
		return cancelled || done;
	}

	@Override
	public boolean isTerminated() {
		return done;
	}

	@Override
	@Nullable
	public Throwable getError() {
		return error;
	}

	@Override
	public CoreSubscriber<? super T> actual() {
		return actual;
	}

	@Override
	public long downstreamCount() {
		return hasDownstreams() ? 1L : 0L;
	}

	@Override
	public boolean hasDownstreams() {
		return hasDownstream;
	}

}
