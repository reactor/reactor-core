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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Runs the source in unbounded mode and emits only the latest value
 * if the subscriber can't keep up properly.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxOnBackpressureLatest<T> extends InternalFluxOperator<T, T> {

	FluxOnBackpressureLatest(Flux<? extends T> source) {
		super(source);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new LatestSubscriber<>(actual);
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

	static final class LatestSubscriber<T>
			implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;
		final Context ctx;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<LatestSubscriber> REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(LatestSubscriber.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<LatestSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(LatestSubscriber.class, "wip");

		Subscription s;

		Throwable error;
		volatile boolean done;

		volatile boolean cancelled;

		volatile T value;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<LatestSubscriber, Object> VALUE =
		  AtomicReferenceFieldUpdater.newUpdater(LatestSubscriber.class, Object.class, "value");

		LatestSubscriber(CoreSubscriber<? super T> actual) {
			this.actual = actual;
			this.ctx = actual.currentContext();
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
					Object toDiscard = VALUE.getAndSet(this, null);
					if (toDiscard != null) {
						Operators.onDiscard(toDiscard, ctx);
					}
				}
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

		@Override
		public void onNext(T t) {
			Object toDiscard = VALUE.getAndSet(this, t);
			if (toDiscard != null) {
				Operators.onDiscard(toDiscard, ctx);
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

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			final Subscriber<? super T> a = actual;

			int missed = 1;

			for (; ; ) {

				if (checkTerminated(done, value == null, a)) {
					return;
				}

				long r = requested;
				long e = 0L;

				while (r != e) {
					boolean d = done;

					@SuppressWarnings("unchecked")
					T v = (T) VALUE.getAndSet(this, null);

					boolean empty = v == null;

					if (checkTerminated(d, empty, a)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(v);

					e++;
				}

				if (r == e && checkTerminated(done, value == null, a)) {
					return;
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

		boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a) {
			if (cancelled) {
				Object toDiscard = VALUE.getAndSet(this, null);
				if (toDiscard != null) {
					Operators.onDiscard(toDiscard, ctx);
				}return true;
			}

			if (d) {
				Throwable e = error;
				if (e != null) {
					Object toDiscard = VALUE.getAndSet(this, null);
					if (toDiscard != null) {
						Operators.onDiscard(toDiscard, ctx);
					}

					a.onError(e);
					return true;
				} else if (empty) {
					a.onComplete();
					return true;
				}
			}

			return false;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.BUFFERED) return value != null ? 1 : 0;
			if (key == Attr.ERROR) return error;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

	}
}
