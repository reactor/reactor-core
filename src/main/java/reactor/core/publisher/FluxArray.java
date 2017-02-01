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
import reactor.core.Fuseable;
import reactor.core.MultiReceiver;
import reactor.core.Producer;
import reactor.core.Trackable;

/**
 * Emits the contents of a wrapped (shared) array.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxArray<T> 
extends Flux<T>
		implements Fuseable {
	final T[] array;

	@SafeVarargs
	public FluxArray(T... array) {
		this.array = Objects.requireNonNull(array, "array");
	}

	@SuppressWarnings("unchecked")
	public static <T> void subscribe(Subscriber<? super T> s, T[] array) {
		if (array.length == 0) {
			Operators.complete(s);
			return;
		}
		if (s instanceof ConditionalSubscriber) {
			s.onSubscribe(new ArrayConditionalSubscription<>((ConditionalSubscriber<? super T>)s, array));
		} else {
			s.onSubscribe(new ArraySubscription<>(s, array));
		}
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		subscribe(s, array);
	}

	static final class ArraySubscription<T>
			implements Producer, Trackable, MultiReceiver,
			           SynchronousSubscription<T> {
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
			if (Operators.validate(n)) {
				if (Operators.getAndAddCap(REQUESTED, this, n) == 0) {
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
			return array instanceof Publisher[] ? Arrays.asList(array)
			                                            .iterator() : null;
		}

		@Override
		public long upstreamCount() {
			return array instanceof Publisher[] ? array.length : -1;
		}

		@Override
		public T poll() {
			int i = index;
			T[] a = array;
			if (i != a.length) {
				T t = a[i];
				if (t == null) {
					throw new NullPointerException();
				}
				index = i + 1;
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
		public int size() {
			return array.length - index;
		}
	}

	static final class ArrayConditionalSubscription<T>
			implements Producer, Trackable, MultiReceiver,
			           SynchronousSubscription<T> {
		final ConditionalSubscriber<? super T> actual;

		final T[] array;

		int index;

		volatile boolean cancelled;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ArrayConditionalSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ArrayConditionalSubscription.class,
						"requested");

		public ArrayConditionalSubscription(ConditionalSubscriber<? super T> actual,
				T[] array) {
			this.actual = actual;
			this.array = array;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (Operators.getAndAddCap(REQUESTED, this, n) == 0) {
					if (n == Long.MAX_VALUE) {
						fastPath();
					}
					else {
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

					i++;
					if (b) {
						e++;
					}
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
			int i = index;
			T[] a = array;
			if (i != a.length) {
				T t = Objects.requireNonNull(a[i], "Array returned null value");
				index = i + 1;
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
		public int size() {
			return array.length - index;
		}
	}

}
