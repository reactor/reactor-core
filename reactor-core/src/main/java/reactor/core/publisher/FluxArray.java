/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Emits the contents of a wrapped (shared) array.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxArray<T> extends Flux<T> implements Fuseable, SourceProducer<T> {

	final T[] array;

	@SafeVarargs
	public FluxArray(T... array) {
		this.array = Objects.requireNonNull(array, "array");
	}

	@SuppressWarnings("unchecked")
	public static <T> void subscribe(CoreSubscriber<? super T> s, T[] array) {
		if (array.length == 0) {
			Operators.complete(s);
			return;
		}
		if (s instanceof ConditionalSubscriber) {
			s.onSubscribe(new ArrayConditionalSubscription<>((ConditionalSubscriber<? super T>) s, array));
		}
		else {
			s.onSubscribe(new ArraySubscription<>(s, array));
		}
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		subscribe(actual, array);
	}


	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.BUFFERED) return array.length;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	static final class ArraySubscription<T>
			implements InnerProducer<T>, SynchronousSubscription<T> {

		final CoreSubscriber<? super T> actual;

		final T[] array;

		int index;

		volatile boolean cancelled;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ArraySubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ArraySubscription.class, "requested");

		ArraySubscription(CoreSubscriber<? super T> actual, T[] array) {
			this.actual = actual;
			this.array = array;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (Operators.addCap(REQUESTED, this, n) == 0) {
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
		@Nullable
		public T poll() {
			int i = index;
			T[] a = array;
			if (i != a.length) {
				T t = a[i];
				Objects.requireNonNull(t);
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
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void clear() {
			index = array.length;
		}

		@Override
		public int size() {
			return array.length - index;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return isEmpty();
			if (key == Attr.BUFFERED) return size();
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerProducer.super.scanUnsafe(key);
		}
	}

	static final class ArrayConditionalSubscription<T>
			implements InnerProducer<T>, SynchronousSubscription<T> {

		final ConditionalSubscriber<? super T> actual;

		final T[] array;

		int index;

		volatile boolean cancelled;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ArrayConditionalSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ArrayConditionalSubscription.class,
						"requested");

		ArrayConditionalSubscription(ConditionalSubscriber<? super T> actual, T[] array) {
			this.actual = actual;
			this.array = array;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (Operators.addCap(REQUESTED, this, n) == 0) {
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return isEmpty();
			if (key == Attr.BUFFERED) return size();
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		@Nullable
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
