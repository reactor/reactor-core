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

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import javax.annotation.Nullable;

import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

/**
 * Emits the contents of an Iterable source.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxIterable<T> extends Flux<T> implements Fuseable {

	final Iterable<? extends T> iterable;

	FluxIterable(Iterable<? extends T> iterable) {
		this.iterable = Objects.requireNonNull(iterable, "iterable");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Iterator<? extends T> it;

		try {
			it = iterable.iterator();
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		subscribe(actual, it);
	}

	/**
	 * Common method to take an Iterator as a source of values.
	 *
	 * @param s the subscriber to feed this iterator to
	 * @param it the iterator to use as a source of values
	 */
	@SuppressWarnings("unchecked")
	static <T> void subscribe(CoreSubscriber<? super T> s, Iterator<? extends T> it) {
		//noinspection ConstantConditions
		if (it == null) {
			Operators.error(s, new NullPointerException("The iterator is null"));
			return;
		}

		boolean b;

		try {
			b = it.hasNext();
		}
		catch (Throwable e) {
			Operators.error(s, Operators.onOperatorError(e, s.currentContext()));
			return;
		}
		if (!b) {
			Operators.complete(s);
			return;
		}

		if (s instanceof ConditionalSubscriber) {
			s.onSubscribe(new IterableSubscriptionConditional<>((ConditionalSubscriber<? super T>) s,
					it));
		}
		else {
			s.onSubscribe(new IterableSubscription<>(s, it));
		}
	}

	static final class IterableSubscription<T>
			implements InnerProducer<T>, SynchronousSubscription<T> {

		final CoreSubscriber<? super T> actual;

		final Iterator<? extends T> iterator;

		volatile boolean cancelled;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<IterableSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(IterableSubscription.class,
						"requested");

		int state;

		/**
		 * Indicates that the iterator's hasNext returned true before but the value is not
		 * yet retrieved.
		 */
		static final int STATE_HAS_NEXT_NO_VALUE  = 0;
		/**
		 * Indicates that there is a value available in current.
		 */
		static final int STATE_HAS_NEXT_HAS_VALUE = 1;
		/**
		 * Indicates that there are no more values available.
		 */
		static final int STATE_NO_NEXT            = 2;
		/**
		 * Indicates that the value has been consumed and a new value should be retrieved.
		 */
		static final int STATE_CALL_HAS_NEXT      = 3;

		T current;

		IterableSubscription(CoreSubscriber<? super T> actual,
				Iterator<? extends T> iterator) {
			this.actual = actual;
			this.iterator = iterator;
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
			final Iterator<? extends T> a = iterator;
			final Subscriber<? super T> s = actual;

			long e = 0L;

			for (; ; ) {

				while (e != n) {
					T t;

					try {
						t = Objects.requireNonNull(a.next(),
								"The iterator returned a null value");
					}
					catch (Throwable ex) {
						s.onError(ex);
						return;
					}

					if (cancelled) {
						return;
					}

					s.onNext(t);

					if (cancelled) {
						return;
					}

					boolean b;

					try {
						b = a.hasNext();
					}
					catch (Throwable ex) {
						s.onError(ex);
						return;
					}

					if (cancelled) {
						return;
					}

					if (!b) {
						s.onComplete();
						return;
					}

					e++;
				}

				n = requested;

				if (n == e) {
					n = REQUESTED.addAndGet(this, -e);
					if (n == 0L) {
						return;
					}
					e = 0L;
				}
			}
		}

		void fastPath() {
			final Iterator<? extends T> a = iterator;
			final Subscriber<? super T> s = actual;

			for (; ; ) {

				if (cancelled) {
					return;
				}

				T t;

				try {
					t = Objects.requireNonNull(a.next(),
							"The iterator returned a null value");
				}
				catch (Exception ex) {
					s.onError(ex);
					return;
				}

				if (cancelled) {
					return;
				}

				s.onNext(t);

				if (cancelled) {
					return;
				}

				boolean b;

				try {
					b = a.hasNext();
				}
				catch (Exception ex) {
					s.onError(ex);
					return;
				}

				if (cancelled) {
					return;
				}

				if (!b) {
					s.onComplete();
					return;
				}
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.TERMINATED) return state == STATE_NO_NEXT;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public void clear() {
			state = STATE_NO_NEXT;
		}

		@Override
		public boolean isEmpty() {
			int s = state;
			if (s == STATE_NO_NEXT) {
				return true;
			}
			else if (s == STATE_HAS_NEXT_HAS_VALUE || s == STATE_HAS_NEXT_NO_VALUE) {
				return false;
			}
			else if (iterator.hasNext()) {
				state = STATE_HAS_NEXT_NO_VALUE;
				return false;
			}
			state = STATE_NO_NEXT;
			return true;
		}

		@Override
		@Nullable
		public T poll() {
			if (!isEmpty()) {
				T c;
				if (state == STATE_HAS_NEXT_NO_VALUE) {
					c = iterator.next();
				}
				else {
					c = current;
					current = null;
				}
				state = STATE_CALL_HAS_NEXT;
				if (c == null) {
					throw new NullPointerException("The iterator returned a null value");
				}
				return c;
			}
			return null;
		}

		@Override
		public int size() {
			if (state == STATE_NO_NEXT) {
				return 0;
			}
			return 1;
		}
	}

	static final class IterableSubscriptionConditional<T>
			implements InnerProducer<T>, SynchronousSubscription<T> {

		final ConditionalSubscriber<? super T> actual;

		final Iterator<? extends T> iterator;

		volatile boolean cancelled;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<IterableSubscriptionConditional> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(IterableSubscriptionConditional.class,
						"requested");

		int state;

		/**
		 * Indicates that the iterator's hasNext returned true before but the value is not
		 * yet retrieved.
		 */
		static final int STATE_HAS_NEXT_NO_VALUE  = 0;
		/**
		 * Indicates that there is a value available in current.
		 */
		static final int STATE_HAS_NEXT_HAS_VALUE = 1;
		/**
		 * Indicates that there are no more values available.
		 */
		static final int STATE_NO_NEXT            = 2;
		/**
		 * Indicates that the value has been consumed and a new value should be retrieved.
		 */
		static final int STATE_CALL_HAS_NEXT      = 3;

		T current;

		IterableSubscriptionConditional(ConditionalSubscriber<? super T> actual,
				Iterator<? extends T> iterator) {
			this.actual = actual;
			this.iterator = iterator;
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
			final Iterator<? extends T> a = iterator;
			final ConditionalSubscriber<? super T> s = actual;

			long e = 0L;

			for (; ; ) {

				while (e != n) {
					T t;

					try {
						t = Objects.requireNonNull(a.next(),
								"The iterator returned a null value");
					}
					catch (Throwable ex) {
						s.onError(ex);
						return;
					}

					if (cancelled) {
						return;
					}

					boolean consumed = s.tryOnNext(t);

					if (cancelled) {
						return;
					}

					boolean b;

					try {
						b = a.hasNext();
					}
					catch (Throwable ex) {
						s.onError(ex);
						return;
					}

					if (cancelled) {
						return;
					}

					if (!b) {
						s.onComplete();
						return;
					}

					if (consumed) {
						e++;
					}
				}

				n = requested;

				if (n == e) {
					n = REQUESTED.addAndGet(this, -e);
					if (n == 0L) {
						return;
					}
					e = 0L;
				}
			}
		}

		void fastPath() {
			final Iterator<? extends T> a = iterator;
			final ConditionalSubscriber<? super T> s = actual;

			for (; ; ) {

				if (cancelled) {
					return;
				}

				T t;

				try {
					t = Objects.requireNonNull(a.next(),
							"The iterator returned a null value");
				}
				catch (Exception ex) {
					s.onError(ex);
					return;
				}

				if (cancelled) {
					return;
				}

				s.tryOnNext(t);

				if (cancelled) {
					return;
				}

				boolean b;

				try {
					b = a.hasNext();
				}
				catch (Exception ex) {
					s.onError(ex);
					return;
				}

				if (cancelled) {
					return;
				}

				if (!b) {
					s.onComplete();
					return;
				}
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.TERMINATED) return state == STATE_NO_NEXT;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public void clear() {
			state = STATE_NO_NEXT;
		}

		@Override
		public boolean isEmpty() {
			int s = state;
			if (s == STATE_NO_NEXT) {
				return true;
			}
			else if (s == STATE_HAS_NEXT_HAS_VALUE || s == STATE_HAS_NEXT_NO_VALUE) {
				return false;
			}
			else if (iterator.hasNext()) {
				state = STATE_HAS_NEXT_NO_VALUE;
				return false;
			}
			state = STATE_NO_NEXT;
			return true;
		}

		@Override
		@Nullable
		public T poll() {
			if (!isEmpty()) {
				T c;
				if (state == STATE_HAS_NEXT_NO_VALUE) {
					c = iterator.next();
				}
				else {
					c = current;
					current = null;
				}
				state = STATE_CALL_HAS_NEXT;
				return c;
			}
			return null;
		}

		@Override
		public int size() {
			if (state == STATE_NO_NEXT) {
				return 0;
			}
			return 1; // no way of knowing without enumerating first
		}
	}
}
