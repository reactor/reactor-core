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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.core.flow.Fuseable;
import reactor.core.flow.Producer;
import reactor.core.state.Cancellable;
import reactor.core.state.Completable;
import reactor.core.state.Requestable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.ScalarSubscription;

/**
 * Emits a range of integer values.
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxRange
		extends Flux<Integer>
		implements Fuseable {

	final long start;

	final long end;

	public FluxRange(int start, int count) {
		if (count < 0) {
			throw new IllegalArgumentException("count >= required but it was " + count);
		}
		long e = (long) start + count;
		if (e - 1 > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("start + count must be less than Integer.MAX_VALUE + 1");
		}

		this.start = start;
		this.end = e;
	}

	@Override
	public void subscribe(Subscriber<? super Integer> s) {
		long st = start;
		long en = end;
		if (st == en) {
			EmptySubscription.complete(s);
			return;
		} else
		if (st + 1 == en) {
			s.onSubscribe(new ScalarSubscription<>(s, (int)st));
			return;
		}
		
		if (s instanceof ConditionalSubscriber) {
			s.onSubscribe(new RangeSubscriptionConditional((ConditionalSubscriber<? super Integer>)s, st, en));
			return;
		}
		s.onSubscribe(new RangeSubscription(s, st, en));
	}

	static final class RangeSubscription
			extends SynchronousSubscription<Integer>
	  implements Cancellable, Requestable, Completable, Producer {

		final Subscriber<? super Integer> actual;

		final long end;

		volatile boolean cancelled;

		long index;

		volatile long requested;
		static final AtomicLongFieldUpdater<RangeSubscription> REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(RangeSubscription.class, "requested");

		public RangeSubscription(Subscriber<? super Integer> actual, long start, long end) {
			this.actual = actual;
			this.index = start;
			this.end = end;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				if (BackpressureUtils.addAndGet(REQUESTED, this, n) == 0) {
					if (n == Long.MAX_VALUE) {
						fastPath();
					} else {
						slowPath(n);
					}
				}
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		void fastPath() {
			final long e = end;
			final Subscriber<? super Integer> a = actual;

			for (long i = index; i != e; i++) {
				if (cancelled) {
					return;
				}

				a.onNext((int) i);
			}

			if (cancelled) {
				return;
			}

			a.onComplete();
		}

		void slowPath(long n) {
			final Subscriber<? super Integer> a = actual;

			long f = end;
			long e = 0;
			long i = index;

			for (; ; ) {

				if (cancelled) {
					return;
				}

				while (e != n && i != f) {

					a.onNext((int) i);

					if (cancelled) {
						return;
					}

					e++;
					i++;
				}

				if (cancelled) {
					return;
				}

				if (i == f) {
					a.onComplete();
					return;
				}

				n = requested;
				if (n == e) {
					index = i;
					n = REQUESTED.addAndGet(this, -e);
					if (n == 0) {
						return;
					}
					e = 0;
				}
			}
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return end != index;
		}

		@Override
		public boolean isTerminated() {
			return end == index;
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
		public Integer poll() {
			long i = index;
			if (i == end) {
				return null;
			}
			index = i + 1;
			return (int)i;
		}

		@Override
		public Integer peek() {
			long i = index;
			if (i == end) {
				return null;
			}
			return (int)i;
		}

		@Override
		public boolean isEmpty() {
			return index == end;
		}

		@Override
		public void clear() {
			index = end;
		}
		
		@Override
		public void drop() {
			index++;
		}
		
		@Override
		public int size() {
			return (int)(end - index);
		}
	}
	
	static final class RangeSubscriptionConditional
	extends SynchronousSubscription<Integer>
	implements Cancellable, Requestable, Completable, Producer {

		final ConditionalSubscriber<? super Integer> actual;

		final long end;

		volatile boolean cancelled;

		long index;

		volatile long requested;
		static final AtomicLongFieldUpdater<RangeSubscriptionConditional> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(RangeSubscriptionConditional.class, "requested");

		public RangeSubscriptionConditional(ConditionalSubscriber<? super Integer> actual, long start, long end) {
			this.actual = actual;
			this.index = start;
			this.end = end;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				if (BackpressureUtils.addAndGet(REQUESTED, this, n) == 0) {
					if (n == Long.MAX_VALUE) {
						fastPath();
					} else {
						slowPath(n);
					}
				}
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		void fastPath() {
			final long e = end;
			final ConditionalSubscriber<? super Integer> a = actual;

			for (long i = index; i != e; i++) {
				if (cancelled) {
					return;
				}

				a.tryOnNext((int) i);
			}

			if (cancelled) {
				return;
			}

			a.onComplete();
		}

		void slowPath(long n) {
			final ConditionalSubscriber<? super Integer> a = actual;

			long f = end;
			long e = 0;
			long i = index;

			for (; ; ) {

				if (cancelled) {
					return;
				}

				while (e != n && i != f) {

					boolean b = a.tryOnNext((int) i);

					if (cancelled) {
						return;
					}

					if (b) {
						e++;
					}
					i++;
				}

				if (cancelled) {
					return;
				}

				if (i == f) {
					a.onComplete();
					return;
				}

				n = requested;
				if (n == e) {
					index = i;
					n = REQUESTED.addAndGet(this, -e);
					if (n == 0) {
						return;
					}
					e = 0;
				}
			}
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return end != index;
		}

		@Override
		public boolean isTerminated() {
			return end == index;
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
		public Integer poll() {
			long i = index;
			if (i == end) {
				return null;
			}
			index = i + 1;
			return (int)i;
		}

		@Override
		public Integer peek() {
			long i = index;
			if (i == end) {
				return null;
			}
			return (int)i;
		}

		@Override
		public boolean isEmpty() {
			return index == end;
		}

		@Override
		public void clear() {
			index = end;
		}

		@Override
		public void drop() {
			index++;
		}

		@Override
		public int size() {
			return (int)(end - index);
		}
	}
}
