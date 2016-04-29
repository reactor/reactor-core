/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.state.Cancellable;
import reactor.core.state.Completable;
import reactor.core.state.Introspectable;
import reactor.core.state.Requestable;
import reactor.core.util.BackpressureUtils;

/**
 * Runs the source in unbounded mode and emits only the latest value
 * if the subscriber can't keep up properly.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxLatest<T> extends FluxSource<T, T> {

	public FluxLatest(Publisher<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new LatestSubscriber<>(s));
	}

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}

	static final class LatestSubscriber<T>
			implements Subscriber<T>, Subscription, Cancellable, Introspectable, Completable, Producer,
			           Requestable, Receiver {

		final Subscriber<? super T> actual;

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

		public LatestSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.getAndAddCap(REQUESTED, this, n);

				drain();
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {

				cancelled = true;

				s.cancel();

				if (WIP.getAndIncrement(this) == 0) {
					VALUE.lazySet(this, null);
				}
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			VALUE.lazySet(this, t);
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
					REQUESTED.addAndGet(this, -e);
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a) {
			if (cancelled) {
				VALUE.lazySet(this, null);
				return true;
			}

			if (d) {
				Throwable e = error;
				if (e != null) {
					VALUE.lazySet(this, null);

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
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return s != null && !cancelled && !done;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public Object upstream() {
			return s;
		}
	}
}
