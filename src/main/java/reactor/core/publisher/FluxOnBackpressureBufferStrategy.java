/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.publisher.FluxSink.OverflowStrategy;

/**
 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class FluxOnBackpressureBufferStrategy<O> extends FluxSource<O, O> {

//	public enum OverflowStrategy {
//		/**
//		 * Propagate an {@link IllegalStateException} when the buffer is full.
//		 */
//		ERROR,
//		/**
//		 * Drop the new element without propagating an error when the buffer is full.
//		 */
//		DROP_ELEMENT,
//		/**
//		 * When the buffer is full, remove the oldest element from it and offer the
//		 * new element at the end instead. Do not propagate an error.
//		 */
//		DROP_OLDEST
//	}

	final Consumer<? super O> onBufferOverflow;
	final int                 bufferSize;
	final boolean             delayError;
	final OverflowStrategy    bufferOverflowStrategy;

	public FluxOnBackpressureBufferStrategy(Publisher<? extends O> source,
			int bufferSize,
			Consumer<? super O> onBufferOverflow,
			OverflowStrategy bufferOverflowStrategy) {
		super(source);
		if (bufferOverflowStrategy == OverflowStrategy.BUFFER) {
			throw new IllegalArgumentException("Unexpected BUFFER strategy, this should be delegated to FluxOnBackpressureBuffer");
		}
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
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}

	static final class BackpressureBufferDropOldestSubscriber<T>
			implements Subscriber<T>, Subscription, Trackable, Producer,
			           Receiver {

		final Subscriber<? super T> actual;
		final int                   bufferSize;
		final Deque<T>              queue;
		final Consumer<? super T>   onOverflow;
		final boolean               delayError;
		final OverflowStrategy      overflowStrategy;

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

		public BackpressureBufferDropOldestSubscriber(Subscriber<? super T> actual,
				int bufferSize,
				boolean delayError,
				Consumer<? super T> onOverflow,
				OverflowStrategy overflowStrategy) {
			this.actual = actual;
			this.delayError = delayError;
			this.onOverflow = onOverflow;
			this.overflowStrategy = overflowStrategy;
			this.bufferSize = bufferSize;
			this.queue = new ArrayDeque<>(bufferSize);
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
			Deque<T> dq = queue;
			synchronized (dq) {
				if (dq.size() == bufferSize) {
					callOnOverflow = true;
					switch (overflowStrategy) {
						case LATEST:
							overflowElement = dq.pollFirst();
							dq.offer(t);
							break;
						case DROP:
							//do nothing
							break;
						case IGNORE:
						case ERROR:
						default:
							callOnError = true;
							break;
					}
				}
				else {
					dq.offer(t);
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

			final Queue<T> q = queue;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (r != e) {
					boolean d = done;

					T t;
					synchronized (q) {
						t = q.poll();
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
					synchronized (q) {
						empty = q.isEmpty();
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
					clear(queue);
				}
			}
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return s != null;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public long getCapacity() {
			return Long.MAX_VALUE;
		}

		@Override
		public long getPending() {
			return queue.size();
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a) {
			if (cancelled) {
				s.cancel();
				clear(queue);
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
						clear(queue);
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

		void clear(Deque<T> dq) {
			synchronized (dq) {
				dq.clear();
			}
		}

	}
}
