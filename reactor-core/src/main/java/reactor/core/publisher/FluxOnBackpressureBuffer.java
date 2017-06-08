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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * @author Stephane Maldini
 */
final class FluxOnBackpressureBuffer<O> extends FluxOperator<O, O> implements Fuseable {

	final Consumer<? super O> onOverflow;
	final int                 bufferSize;
	final boolean             unbounded;
	final boolean             delayError;

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
		this.delayError = unbounded || onOverflow != null;
	}

	@Override
	public void subscribe(Subscriber<? super O> s, Context ctx) {
		source.subscribe(new BackpressureBufferSubscriber<>(s,
				bufferSize,
				unbounded,
				delayError,
				onOverflow), ctx);
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	static final class BackpressureBufferSubscriber<T>
			implements QueueSubscription<T>, InnerOperator<T, T> {

		final Subscriber<? super T> actual;
		final Queue<T>              queue;
		final Consumer<? super T>   onOverflow;
		final boolean             delayError;

		Subscription s;

		volatile boolean cancelled;

		volatile boolean enabledFusion;

		volatile boolean done;
		Throwable error;

		volatile int wip;
		static final AtomicIntegerFieldUpdater<BackpressureBufferSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BackpressureBufferSubscriber.class,
						"wip");

		volatile long requested;
		static final AtomicLongFieldUpdater<BackpressureBufferSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BackpressureBufferSubscriber.class,
						"requested");

		BackpressureBufferSubscriber(Subscriber<? super T> actual,
				int bufferSize,
				boolean unbounded,
				boolean delayError,
				@Nullable Consumer<? super T> onOverflow) {
			this.actual = actual;
			this.delayError = delayError;
			this.onOverflow = onOverflow;

			Queue<T> q;

			if (unbounded) {
				q = QueueSupplier.<T>unbounded(bufferSize).get();
			}
			else {
				q = QueueSupplier.<T>get(bufferSize).get();
			}

			this.queue = q;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == BooleanAttr.TERMINATED) return done && queue.isEmpty();
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == IntAttr.BUFFERED) return queue.size();
			if (key == ThrowableAttr.ERROR) return error;
			if (key == IntAttr.PREFETCH) return Integer.MAX_VALUE;
			if (key == BooleanAttr.DELAY_ERROR) return delayError;

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
				Operators.onNextDropped(t);
				return;
			}
			if (!queue.offer(t)) {
				Throwable ex =
						Operators.onOperatorError(s, Exceptions.failWithOverflow(), t);
				if (onOverflow != null) {
					try {
						onOverflow.accept(t);
					}
					catch (Throwable e) {
						Exceptions.throwIfFatal(e);
						ex.initCause(e);
					}
				}
				onError(ex);
				return;
			}
			drain();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			error = t;
			done = true;
			drain();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			drain();
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
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

					if (checkTerminated(d, empty, a)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(t);

					e++;
				}

				if (r == e) {
					if (checkTerminated(done, q.isEmpty(), a)) {
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
					s.cancel();
					q.clear();
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
				Operators.getAndAddCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				s.cancel();

				if (!enabledFusion) {
					if (WIP.getAndIncrement(this) == 0) {
						queue.clear();
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
			queue.clear();
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
		public Subscriber<? super T> actual() {
			return actual;
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a) {
			if (cancelled) {
				s.cancel();
				queue.clear();
				return true;
			}
			if (d) {
				if (delayError) {
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
				else {
					Throwable e = error;
					if (e != null) {
						queue.clear();
						a.onError(e);
						return true;
					}
					else if (empty) {
						a.onComplete();
						return true;
					}
				}
			}
			return false;
		}
	}

}
