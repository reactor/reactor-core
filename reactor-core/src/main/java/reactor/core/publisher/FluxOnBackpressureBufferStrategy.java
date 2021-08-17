/*
 * Copyright (c) 2015-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Buffers values if the subscriber doesn't request fast enough, bounding the
 * buffer to a chosen size. If the buffer overflows, apply a pre-determined
 * overflow strategy.

 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class FluxOnBackpressureBufferStrategy<O> extends InternalFluxOperator<O, O> {

	final Consumer<? super O>    onBufferOverflow;
	final int                    bufferSize;
	final boolean                delayError;
	final BufferOverflowStrategy bufferOverflowStrategy;

	FluxOnBackpressureBufferStrategy(Flux<? extends O> source,
			int bufferSize,
			@Nullable Consumer<? super O> onBufferOverflow,
			BufferOverflowStrategy bufferOverflowStrategy) {
		super(source);
		if (bufferSize < 1) {
			throw new IllegalArgumentException("Buffer Size must be strictly positive");
		}

		this.bufferSize = bufferSize;
		this.onBufferOverflow = onBufferOverflow;
		this.bufferOverflowStrategy = bufferOverflowStrategy;
		this.delayError = onBufferOverflow != null || bufferOverflowStrategy == BufferOverflowStrategy.ERROR;
	}

	@Override
	public CoreSubscriber<? super O> subscribeOrReturn(CoreSubscriber<? super O> actual) {
		return new BackpressureBufferDropOldestSubscriber<>(actual,
				bufferSize,
				delayError, onBufferOverflow, bufferOverflowStrategy);
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class BackpressureBufferDropOldestSubscriber<T>
			extends ArrayDeque<T>
			implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;
		final Context                   ctx;
		final int                       bufferSize;
		final Consumer<? super T>       onOverflow;
		final boolean                   delayError;
		final BufferOverflowStrategy    overflowStrategy;

		Subscription s;

		volatile boolean cancelled;

		volatile boolean done;
		Throwable error;

		volatile int wip;
		static final AtomicIntegerFieldUpdater<BackpressureBufferDropOldestSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BackpressureBufferDropOldestSubscriber.class,
						"wip");

		volatile long requested;
		static final AtomicLongFieldUpdater<BackpressureBufferDropOldestSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BackpressureBufferDropOldestSubscriber.class,
						"requested");

		BackpressureBufferDropOldestSubscriber(
				CoreSubscriber<? super T> actual,
				int bufferSize,
				boolean delayError,
				@Nullable Consumer<? super T> onOverflow,
				BufferOverflowStrategy overflowStrategy) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.delayError = delayError;
			this.onOverflow = onOverflow;
			this.overflowStrategy = overflowStrategy;
			this.bufferSize = bufferSize;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.TERMINATED) return done && isEmpty();
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.BUFFERED) return size();
			if (key == Attr.ERROR) return error;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.DELAY_ERROR) return delayError;
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

			boolean callOnOverflow = false;
			boolean callOnError = false;
			T overflowElement = t;
			synchronized(this) {
				if (size() == bufferSize) {
					callOnOverflow = true;
					switch (overflowStrategy) {
						case DROP_OLDEST:
							overflowElement = pollFirst();
							offer(t);
							break;
						case DROP_LATEST:
							//do nothing
							break;
						case ERROR:
						default:
							callOnError = true;
							break;
					}
				}
				else {
					offer(t);
				}
			}

			if (callOnOverflow) {
				if (onOverflow != null) {
					try {
						onOverflow.accept(overflowElement);
					}
					catch (Throwable e) {
						Throwable ex = Operators.onOperatorError(s, e, overflowElement, ctx);
						onError(ex);
						return;
					}
					finally {
						Operators.onDiscard(overflowElement, ctx);
					}
				}
				else {
					Operators.onDiscard(overflowElement, ctx);
				}
			}

			if (callOnError) {
				Throwable ex = Operators.onOperatorError(s, Exceptions.failWithOverflow(), overflowElement, ctx);
				onError(ex);
			}

			if (!callOnError && !callOnOverflow) {
				drain();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, ctx);
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
				//noinspection ConstantConditions
				if (a != null) {
					innerDrain(a);
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void innerDrain(Subscriber<? super T> a) {
			int missed = 1;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (r != e) {
					boolean d = done;

					T t;
					synchronized (this) {
						t = poll();
					}
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
					boolean empty;
					synchronized (this) {
						empty = isEmpty();
					}
					if (checkTerminated(done, empty, a)) {
						return;
					}
				}

				if (e != 0L && r != Long.MAX_VALUE) {
					Operators.produced(REQUESTED, this, e);
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
				drain();
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				s.cancel();

				if (WIP.getAndIncrement(this) == 0) {
					synchronized (this) {
						clear();
					}
				}
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a) {
			if (cancelled) {
				s.cancel();
				synchronized (this) {
					clear();
				}
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
						synchronized (this) {
							clear();
						}
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

		@Override
		public void clear() {
			Operators.onDiscardMultiple(this, ctx);
			super.clear();
		}
	}
}
