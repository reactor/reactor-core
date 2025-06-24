/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * A {@link Sinks.Many} implementation that takes a custom queue and allows
 * only a single subscriber. {@link SinkManyUnicast} allows multiplexing of the events which
 * means that it supports multiple producers and only one consumer.
 * However, it should be noticed that multi-producer case is only valid if appropriate
 * Queue is provided. Otherwise, it could break
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
 *      <b>Note: </b> SinkManyUnicast does not respect the actual subscriber's
 *      demand as it is described in
 *      <a href="https://www.reactive-streams.org/">Reactive Streams Spec</a>. However,
 *      SinkManyUnicast embraces configurable Queue internally which allows enabling
 *      backpressure support and preventing of consumer's overwhelming.
 *
 *      Hence, interaction model between producers and SinkManyUnicast will be PUSH
 *      only. In opposite, interaction model between SinkManyUnicast and consumer will be
 *      PUSH-PULL as defined in
 *      <a href="https://www.reactive-streams.org/">Reactive Streams Spec</a>.
 *
 *      In the case when upstream's signals overflow the bound of internal Queue,
 *      SinkManyUnicast will fail with signaling onError(
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
 */
final class SinkManyUnicast<T> extends Flux<T> implements InternalManySink<T>, Disposable, Fuseable.QueueSubscription<T>, Fuseable, SourceProducer<T> {

	/**
	 * Create a new {@link SinkManyUnicast} that will buffer on an internal queue in an
	 * unbounded fashion.
	 *
	 * @param <E> the relayed type
	 * @return a unicast {@link Sinks.Many}
	 */
	static <E> SinkManyUnicast<E> create() {
		return new SinkManyUnicast<>(Queues.<E>unbounded().get());
	}

	/**
	 * Create a new {@link SinkManyUnicast} that will buffer on a provided queue in an
	 * unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param <E> the relayed type
	 * @return a unicast {@link Sinks.Many}
	 */
	static <E> SinkManyUnicast<E> create(Queue<E> queue) {
		return new SinkManyUnicast<>(Hooks.wrapQueue(queue));
	}

	/**
	 * Create a new {@link SinkManyUnicast} that will buffer on a provided queue in an
	 * unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param endcallback called on any terminal signal
	 * @param <E> the relayed type
	 * @return a unicast {@link Sinks.Many}
	 */
	static <E> SinkManyUnicast<E> create(Queue<E> queue, Disposable endcallback) {
		return new SinkManyUnicast<>(Hooks.wrapQueue(queue), endcallback);
	}

	final Queue<T>            queue;

	volatile Disposable                                                   onTerminate;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SinkManyUnicast, Disposable> ON_TERMINATE =
			AtomicReferenceFieldUpdater.newUpdater(SinkManyUnicast.class, Disposable.class, "onTerminate");

	volatile boolean done;
	volatile boolean subscriptionDelivered;
	Throwable error;

	boolean hasDownstream; //important to not loose the downstream too early and miss discard hook, while having relevant hasDownstreams()
	volatile CoreSubscriber<? super T> actual;

	volatile boolean cancelled;

	volatile int                                            once;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<SinkManyUnicast> ONCE =
			AtomicIntegerFieldUpdater.newUpdater(SinkManyUnicast.class, "once");

	volatile int                                            wip;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<SinkManyUnicast> WIP =
			AtomicIntegerFieldUpdater.newUpdater(SinkManyUnicast.class, "wip");

	volatile int                                            discardGuard;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<SinkManyUnicast> DISCARD_GUARD =
			AtomicIntegerFieldUpdater.newUpdater(SinkManyUnicast.class, "discardGuard");

	volatile long                                        requested;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<SinkManyUnicast> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(SinkManyUnicast.class, "requested");

	boolean outputFused;

	SinkManyUnicast(Queue<T> queue) {
		this.queue = Objects.requireNonNull(queue, "queue");
		this.onTerminate = null;
	}

	SinkManyUnicast(Queue<T> queue, Disposable onTerminate) {
		this.queue = Objects.requireNonNull(queue, "queue");
		this.onTerminate = Objects.requireNonNull(onTerminate, "onTerminate");
	}

	@Override
	public Stream<Scannable> inners() {
		return hasDownstream ? Stream.of(Scannable.from(actual)) : Stream.empty();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (Attr.ACTUAL == key) return actual;
		if (Attr.BUFFERED == key) return queue.size();
		if (Attr.CAPACITY == key) return Queues.capacity(this.queue);
		if (Attr.PREFETCH == key) return Integer.MAX_VALUE;
		if (Attr.CANCELLED == key) return cancelled;
		if (Attr.TERMINATED == key) return done;
		if (Attr.ERROR == key) return error;
		if (InternalProducerAttr.INSTANCE == key) return true;

		return null;
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
							currentContext());
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
			if (subscriptionDelivered) {
			    CoreSubscriber<? super T> a = actual;
				if (outputFused) {
					drainFused(a);
				} else {
					drainRegular(a);
				}
				return;
			}

			// This handles a race condition where `cancel()` is
			// called before a subscriber arrives (e.g., via `take(0)`).
			if (cancelled) {
				if (dataSignalOfferedBeforeDrain != null) {
					Operators.onDiscard(dataSignalOfferedBeforeDrain, currentContext());
				}
				if (!outputFused) {
					Operators.onDiscardQueueWithClear(queue, currentContext(), null);
				}
			}

			missed = WIP.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}

			dataSignalOfferedBeforeDrain = null;
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
		CoreSubscriber<? super T> wrapped =
				Operators.restoreContextOnSubscriberIfAutoCPEnabled(this, actual);
		if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {

			this.hasDownstream = true;
			this.actual = wrapped;
			wrapped.onSubscribe(this);
			subscriptionDelivered = true;
			if (cancelled) {
				this.hasDownstream = false;
			} else {
				drain(null);
			}
		} else {
			Operators.error(wrapped, new IllegalStateException("Sinks.many().unicast() sinks only allow a single Subscriber"));
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

		drain(null);
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
	public void dispose() {
		emitError(new CancellationException("Disposed"), Sinks.EmitFailureHandler.FAIL_FAST);
	}

	@Override
	public boolean isDisposed() {
		return cancelled || done;
	}

	@Override
	public void terminateAndCleanup() {
		cancel();
	}
}
