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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.flow.Mergeable;
import reactor.core.flow.MultiReceiver;
import reactor.core.flow.Producer;
import reactor.core.state.Cancellable;
import reactor.core.state.Requestable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;

/**
 * Emits the contents of a wrapped (shared) array.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxArray<T> 
extends Flux<T>
		implements Mergeable {
	final T[] array;

	@SafeVarargs
	public FluxArray(T... array) {
		this.array = Objects.requireNonNull(array, "array");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (array.length == 0) {
			EmptySubscription.complete(s);
			return;
		}
		s.onSubscribe(new ArraySubscription<>(s, array));
	}

	static final class ArraySubscription<T>
			extends SynchronousSubscription<T>
	  implements Producer, Requestable, Cancellable, MultiReceiver {
		final Subscriber<? super T> actual;

		final T[] array;

		int index;

		volatile boolean cancelled;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ArraySubscription> REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(ArraySubscription.class, "requested");

		public ArraySubscription(Subscriber<? super T> actual, T[] array) {
			this.actual = actual;
			this.array = array;
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

		void slowPath(long n) {
			final T[] a = array;
			final int len = a.length;
			final Subscriber<? super T> s = actual;

			int i = index;
			int e = 0;

			for (; ; ) {
				if (cancelled) {
					return;
				}

				while (i != len && e != n) {
					T t = a[i];

					if (t == null) {
						s.onError(new NullPointerException("The " + i + "th array element was null"));
						return;
					}

					s.onNext(t);

					if (cancelled) {
						return;
					}

					i++;
					e++;
				}

				if (i == len) {
					s.onComplete();
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

		void fastPath() {
			final T[] a = array;
			final int len = a.length;
			final Subscriber<? super T> s = actual;

			for (int i = index; i != len; i++) {
				if (cancelled) {
					return;
				}

				T t = a[i];

				if (t == null) {
					s.onError(new NullPointerException("The " + i + "th array element was null"));
					return;
				}

				s.onNext(t);
			}
			if (cancelled) {
				return;
			}
			s.onComplete();
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
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
		public Iterator<?> upstreams() {
			return array instanceof Publisher[] ? Arrays.asList(array).iterator() : null;
		}

		@Override
		public long upstreamCount() {
			return array instanceof Publisher[] ? array.length : -1;
		}

		@Override
		public T poll() {
			int i = index++;
			T[] a = array;
			if (i < a.length) {
				T t = a[i];
				if (t == null) {
					throw new NullPointerException();
				}
				return t;
			}
			return null;
		}

		@Override
		public T peek() {
			int i = index;
			T[] a = array;
			if (i < a.length) {
				T t = a[i];
				if (t == null) {
					throw new NullPointerException();
				}
				return t;
			}
			return null;
		}

		@Override
		public boolean isEmpty() {
			return index == array.length;
		}

		@Override
		public void clear() {
			index = array.length;
		}
		
		@Override
		public void drop() {
			index++;
		}
	}

	static final class ArraySubscriptionConditional<T>
	extends SynchronousSubscription<T>
	implements Producer, Requestable, Cancellable, MultiReceiver {
		final ConditionalSubscriber<? super T> actual;

		final T[] array;

		int index;

		volatile boolean cancelled;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ArraySubscriptionConditional> REQUESTED =
		AtomicLongFieldUpdater.newUpdater(ArraySubscriptionConditional.class, "requested");

		public ArraySubscriptionConditional(ConditionalSubscriber<? super T> actual, T[] array) {
			this.actual = actual;
			this.array = array;
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

		void slowPath(long n) {
			final T[] a = array;
			final int len = a.length;
			final ConditionalSubscriber<? super T> s = actual;

			int i = index;
			int e = 0;

			for (; ; ) {
				if (cancelled) {
					return;
				}

				while (i != len && e != n) {
					T t = a[i];

					if (t == null) {
						s.onError(new NullPointerException("The " + i + "th array element was null"));
						return;
					}

					boolean b = s.tryOnNext(t);

					if (cancelled) {
						return;
					}

					if (b) {
						i++;
					}
					e++;
				}

				if (i == len) {
					s.onComplete();
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

		void fastPath() {
			final T[] a = array;
			final int len = a.length;
			final ConditionalSubscriber<? super T> s = actual;

			for (int i = index; i != len; i++) {
				if (cancelled) {
					return;
				}

				T t = a[i];

				if (t == null) {
					s.onError(new NullPointerException("The " + i + "th array element was null"));
					return;
				}

				s.tryOnNext(t);
			}
			if (cancelled) {
				return;
			}
			s.onComplete();
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
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
		public Iterator<?> upstreams() {
			return array instanceof Publisher[] ? Arrays.asList(array).iterator() : null;
		}

		@Override
		public long upstreamCount() {
			return array instanceof Publisher[] ? array.length : -1;
		}

		@Override
		public T poll() {
			int i = index++;
			T[] a = array;
			if (i < a.length) {
				T t = a[i];
				if (t == null) {
					throw new NullPointerException();
				}
				return t;
			}
			return null;
		}

		@Override
		public T peek() {
			int i = index;
			T[] a = array;
			if (i < a.length) {
				T t = a[i];
				if (t == null) {
					throw new NullPointerException();
				}
				return t;
			}
			return null;
		}

		@Override
		public boolean isEmpty() {
			return index == array.length;
		}

		@Override
		public void clear() {
			index = array.length;
		}

		@Override
		public void drop() {
			index++;
		}
	}

}
