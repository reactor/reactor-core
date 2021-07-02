/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static reactor.core.Scannable.Attr.RUN_STYLE;
import static reactor.core.Scannable.Attr.RunStyle.ASYNC;

/**
 * Buffers values if the subscriber doesn't request fast enough, bounding the buffer to a
 * chosen size and applying a TTL (time-to-live) to the elements. If the buffer overflows,
 * drop the oldest element.
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 * @author David Karnok
 */
//see https://github.com/akarnokd/RxJava2Extensions/blob/master/src/main/java/hu/akarnokd/rxjava2/operators/FlowableOnBackpressureTimeout.java
final class FluxOnBackpressureBufferTimeout<O> extends InternalFluxOperator<O, O> {

	private static final Logger LOGGER =
			Loggers.getLogger(FluxOnBackpressureBufferTimeout.class);

	final Duration            ttl;
	final Scheduler           ttlScheduler;
	final int                 bufferSize;
	final Consumer<? super O> onBufferEviction;

	FluxOnBackpressureBufferTimeout(Flux<? extends O> source,
			Duration ttl,
			Scheduler ttlScheduler,
			int bufferSize,
			Consumer<? super O> onBufferEviction) {
		super(source);
		this.ttl = ttl;
		this.ttlScheduler = ttlScheduler;
		this.bufferSize = bufferSize;
		this.onBufferEviction = onBufferEviction;
	}

	@Override
	public CoreSubscriber<? super O> subscribeOrReturn(CoreSubscriber<? super O> actual) {
		return new BackpressureBufferTimeoutSubscriber<>(actual,
				ttl,
				ttlScheduler,
				bufferSize,
				onBufferEviction);
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return ttlScheduler;
		if (key == RUN_STYLE) return ASYNC;

		return super.scanUnsafe(key);
	}

	static final class BackpressureBufferTimeoutSubscriber<T> extends ArrayDeque<Object>
			implements InnerOperator<T, T>, Runnable {

		final CoreSubscriber<? super T> actual;
		final Context                   ctx;
		final Duration                  ttl;
		final Scheduler                 ttlScheduler;
		final Scheduler.Worker          worker;
		final int                       bufferSizeDouble;
		final Consumer<? super T>       onBufferEviction;

		Subscription s;

		volatile boolean cancelled;

		volatile boolean done;
		Throwable error;

		volatile int wip;
		static final AtomicIntegerFieldUpdater<BackpressureBufferTimeoutSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BackpressureBufferTimeoutSubscriber.class,
						"wip");

		volatile long requested;
		static final AtomicLongFieldUpdater<BackpressureBufferTimeoutSubscriber>
				REQUESTED = AtomicLongFieldUpdater.newUpdater(
				BackpressureBufferTimeoutSubscriber.class,
				"requested");

		BackpressureBufferTimeoutSubscriber(CoreSubscriber<? super T> actual,
				Duration ttl,
				Scheduler ttlScheduler,
				int bufferSize,
				Consumer<? super T> onBufferEviction) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.onBufferEviction = Objects.requireNonNull(onBufferEviction,
					"buffer eviction callback must not be null");
			this.bufferSizeDouble = bufferSize << 1;
			this.ttl = ttl;
			this.ttlScheduler = Objects.requireNonNull(ttlScheduler,
					"ttl Scheduler must not be null");
			this.worker = ttlScheduler.createWorker();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) {
				return requested;
			}
			if (key == Attr.TERMINATED) {
				return done && isEmpty();
			}
			if (key == Attr.CANCELLED) {
				return cancelled;
			}
			if (key == Attr.BUFFERED) {
				return size();
			}
			if (key == Attr.ERROR) {
				return error;
			}
			if (key == Attr.PREFETCH) {
				return Integer.MAX_VALUE;
			}
			if (key == Attr.DELAY_ERROR) {
				return false;
			}
			if (key == Attr.RUN_ON) {
				return ttlScheduler;
			}
			if (key == RUN_STYLE) {
			    return ASYNC;
			}

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
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
			cancelled = true;
			s.cancel();
			worker.dispose();

			if (WIP.getAndIncrement(this) == 0) {
				clearQueue();
			}
		}

		@SuppressWarnings("unchecked")
		void clearQueue() {
			for (; ; ) {
				T evicted;
				synchronized (this) {
					if (this.isEmpty()) {
						break;
					}

					this.poll();
					evicted = (T) this.poll();
				}

				evict(evicted);
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onNext(T t) {
			T evicted = null;
			synchronized (this) {
				if (this.size() == bufferSizeDouble) {
					this.poll();
					evicted = (T) this.poll();
				}
				this.offer(ttlScheduler.now(TimeUnit.NANOSECONDS));
				this.offer(t);
			}
			evict(evicted);
			try {
				worker.schedule(this, ttl.toNanos(), TimeUnit.NANOSECONDS);
			}
			catch (RejectedExecutionException re) {
				done = true;
				error = Operators.onRejectedExecution(re, this, null, t,
						actual.currentContext());
			}
			drain();
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;
			drain();
		}

		@Override
		public void onComplete() {
			done = true;
			drain();
		}

		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			for (; ; ) {
				if (cancelled) {
					break;
				}

				boolean d = done;
				boolean empty;
				T evicted = null;

				synchronized (this) {
					Long ts = (Long) this.peek();
					empty = ts == null;
					if (!empty) {
						if (ts <= ttlScheduler.now(TimeUnit.NANOSECONDS) - ttl.toNanos()) {
							this.poll();
							evicted = (T) this.poll();
						}
						else {
							break;
						}
					}
				}

				evict(evicted);

				if (empty) {
					if (d) {
						drain();
					}
					break;
				}
			}
		}

		void evict(@Nullable T evicted) {
			if (evicted != null) {
				try {
					onBufferEviction.accept(evicted);
				}
				catch (Throwable ex) {
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug(
								"value [{}] couldn't be evicted due to a callback error. This error will be dropped: {}",
								evicted,
								ex);
					}
					Operators.onErrorDropped(ex, actual.currentContext());
				}
				Operators.onDiscard(evicted, actual.currentContext());
			}
		}

		@SuppressWarnings("unchecked")
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;

			for (; ; ) {
				long r = requested;
				long e = 0;

				while (e != r) {
					if (cancelled) {
						clearQueue();
						return;
					}

					boolean d = done;
					T v;

					synchronized (this) {
						if (this.poll() != null) {
							v = (T) this.poll();
						}
						else {
							v = null;
						}
					}

					boolean empty = v == null;

					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							actual.onError(ex);
						}
						else {
							actual.onComplete();
						}

						worker.dispose();
						return;
					}

					if (empty) {
						break;
					}

					actual.onNext(v);

					e++;
				}

				if (e == r) {
					if (cancelled) {
						clearQueue();
						return;
					}

					boolean d = done;
					boolean empty;
					synchronized (this) {
						empty = this.isEmpty();
					}

					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							actual.onError(ex);
						}
						else {
							actual.onComplete();
						}

						worker.dispose();
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
	}
}
