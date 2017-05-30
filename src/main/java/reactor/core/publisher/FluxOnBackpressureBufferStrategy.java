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

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;

/**
 * Buffers values if the subscriber doesn't request fast enough, bounding the
 * buffer to a chosen size. If the buffer overflows, apply a pre-determined
 * overflow strategy.

 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class FluxOnBackpressureBufferStrategy<O> extends FluxSource<O, O> {

	final Consumer<? super O>    onBufferOverflow;
	final int                    bufferSize;
	final boolean                delayError;
	final BufferOverflowStrategy bufferOverflowStrategy;

	FluxOnBackpressureBufferStrategy(Flux<? extends O> source,
			int bufferSize,
			Consumer<? super O> onBufferOverflow,
			BufferOverflowStrategy bufferOverflowStrategy) {
		super(source);
		this.bufferSize = bufferSize;
		this.onBufferOverflow = onBufferOverflow;
		this.bufferOverflowStrategy = bufferOverflowStrategy;
		this.delayError = onBufferOverflow != null;
	}

	@Override
	public void subscribe(Subscriber<? super O> s) {
		source.subscribe(new BackpressureBufferDropOldestSubscriber<>(s,
				bufferSize,
				delayError, onBufferOverflow, bufferOverflowStrategy));
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	static final class BackpressureBufferDropOldestSubscriber<T>
			extends ArrayDeque<T>
			implements InnerOperator<T, T> {

		final Subscriber<? super T>  actual;
		final int                    bufferSize;
		final Consumer<? super T>    onOverflow;
		final boolean                delayError;
		final BufferOverflowStrategy overflowStrategy;

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

		BackpressureBufferDropOldestSubscriber(Subscriber<? super T> actual,
				int bufferSize,
				boolean delayError,
				Consumer<? super T> onOverflow,
				BufferOverflowStrategy overflowStrategy) {
			this.actual = actual;
			this.delayError = delayError;
			this.onOverflow = onOverflow;
			this.overflowStrategy = overflowStrategy;
			this.bufferSize = bufferSize;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == BooleanAttr.TERMINATED) return done && isEmpty();
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == IntAttr.BUFFERED) return size();
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

			if (callOnOverflow && onOverflow != null) {
				try {
					onOverflow.accept(overflowElement);
				}
				catch (Throwable e) {
					Throwable ex = Operators.onOperatorError(s, e, overflowElement);
					onError(ex);
					return;
				}
			}

			if (callOnError) {
				Throwable ex = Operators.onOperatorError(s, Exceptions.failWithOverflow(), overflowElement);
				onError(ex);
			}

			if (!callOnError && !callOnOverflow) {
				drain();
			}
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

				if (e != 0 && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
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

				if (WIP.getAndIncrement(this) == 0) {
					synchronized (this) {
						clear();
					}
				}
			}
		}

		@Override
		public Subscriber<? super T> actual() {
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

	}
}
