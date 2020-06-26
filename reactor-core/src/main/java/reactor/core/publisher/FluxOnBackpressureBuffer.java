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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
final class FluxOnBackpressureBuffer<O> extends InternalFluxOperator<O, O> implements Fuseable {

	final Consumer<? super O> onOverflow;
	final int                 bufferSize;
	final boolean             unbounded;

	FluxOnBackpressureBuffer(Flux<? extends O> source,
			int bufferSize,
			boolean unbounded,
			@Nullable Consumer<? super O> onOverflow) {
		super(source);
		if (bufferSize < 1) {
			throw new IllegalArgumentException("Buffer Size must be strictly positive");
		}
		this.bufferSize = bufferSize;
		this.unbounded = unbounded;
		this.onOverflow = onOverflow;
	}

	@Override
	public CoreSubscriber<? super O> subscribeOrReturn(CoreSubscriber<? super O> actual) {
		return new BackpressureBufferSubscriber<>(actual,
				bufferSize,
				unbounded,
				onOverflow);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	static final class BackpressureBufferSubscriber<T>
			implements QueueSubscription<T>, InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;
		final Context                   ctx;
		final Queue<T>                  queue;
		final int                       capacityOrSkip;
		final Consumer<? super T>       onOverflow;

		Subscription s;

		volatile boolean cancelled;

		volatile boolean enabledFusion;

		volatile boolean done;
		Throwable error;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<BackpressureBufferSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BackpressureBufferSubscriber.class,
						"wip");

		volatile int discardGuard;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<BackpressureBufferSubscriber> DISCARD_GUARD =
				AtomicIntegerFieldUpdater.newUpdater(BackpressureBufferSubscriber.class,
						"discardGuard");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BackpressureBufferSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BackpressureBufferSubscriber.class,
						"requested");

		BackpressureBufferSubscriber(CoreSubscriber<? super T> actual,
				int bufferSize,
				boolean unbounded,
				@Nullable Consumer<? super T> onOverflow) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.onOverflow = onOverflow;

			Queue<T> q;

			if (unbounded) {
				q = Queues.<T>unbounded(bufferSize).get();
			}
			else {
				q = Queues.<T>get(bufferSize).get();
			}

			if (!unbounded && Queues.capacity(q) > bufferSize) {
				this.capacityOrSkip = bufferSize;
			}
			else {
				//for unbounded, the bufferSize is not terribly relevant
				//for bounded, if the queue has exact capacity then when checking q.size() == capacityOrSkip, this will skip the check
				this.capacityOrSkip = Integer.MAX_VALUE;
			}

			this.queue = q;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.TERMINATED) return done && queue.isEmpty();
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.BUFFERED) return queue.size();
			if (key == Attr.ERROR) return error;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.DELAY_ERROR) return true;
			if (key == Attr.CAPACITY) return capacityOrSkip == Integer.MAX_VALUE ? Queues.capacity(queue) : capacityOrSkip;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, ctx);
				return;
			}
			if (cancelled) {
				Operators.onDiscard(t, ctx);
			}

			if ((capacityOrSkip != Integer.MAX_VALUE && queue.size() >= capacityOrSkip) || !queue.offer(t)) {
				Throwable ex = Operators.onOperatorError(s, Exceptions.failWithOverflow(), t, ctx);
				if (onOverflow != null) {
					try {
						onOverflow.accept(t);
					}
					catch (Throwable e) {
						Exceptions.throwIfFatal(e);
						ex.initCause(e);
					}
				}
				Operators.onDiscard(t, ctx);
				onError(ex);
				return;
			}
			drain(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, ctx);
				return;
			}
			error = t;
			done = true;
			drain(null);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			drain(null);
		}

		void drain(@Nullable T dataSignal) {
			if (WIP.getAndIncrement(this) != 0) {
				if (dataSignal != null && cancelled) {
					Operators.onDiscard(dataSignal, actual.currentContext());
				}
				return;
			}

			int missed = 1;

			for (; ; ) {
				Subscriber<? super T> a = actual;
				if (a != null) {

					if (enabledFusion) {
						drainFused(a);
					}
					else {
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

		void drainRegular(Subscriber<? super T> a) {
			int missed = 1;

			final Queue<T> q = queue;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (r != e) {
					boolean d = done;

					T t = q.poll();
					boolean empty = t == null;

					if (checkTerminated(d, empty, a, t)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(t);

					e++;
				}

				if (r == e) {
					if (checkTerminated(done, q.isEmpty(), a, null)) {
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

		void drainFused(Subscriber<? super T> a) {
			int missed = 1;

			final Queue<T> q = queue;

			for (; ; ) {

				if (cancelled) {
					// We are the holder of the queue, but we still have to perform discarding under the guarded block
					// to prevent any racing done by downstream
					this.clear();
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
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

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				drain(null);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				s.cancel();

				if (WIP.getAndIncrement(this) == 0) {
					if (!enabledFusion) {
						// discard MUST be happening only and only if there is no racing on elements consumption
						// which is guaranteed by the WIP guard here in case non-fused output
						Operators.onDiscardQueueWithClear(queue, ctx, null);
					}
				}
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
				Operators.onDiscardQueueWithClear(queue, ctx, null);

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
				enabledFusion = true;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a, @Nullable T v) {
			if (cancelled) {
				s.cancel();
				Operators.onDiscard(v, ctx);
				Operators.onDiscardQueueWithClear(queue, ctx, null);
				return true;
			}
			if (d) {
				//the operator always delays the errors, particularly overflow one
				if (empty) {
					Throwable e = error;
					if (e != null) {
						a.onError(e);
					}
					else {
						a.onComplete();
					}
					return true;
				}
			}
			return false;
		}
	}

}
