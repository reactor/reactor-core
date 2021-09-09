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

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;

/**
 * Emits the contents of an Iterable source. Attempt to discard remainder of a source
 * in case of error / cancellation, but uses the {@link Spliterator} API to try and detect
 * infinite sources (so that said discarding doesn't loop infinitely).
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxIterable<T> extends Flux<T> implements Fuseable, SourceProducer<T> {

	/**
	 * Utility method to check that a given {@link Iterable} can be positively identified as
	 * finite, which implies forEachRemaining type of iteration can be done to discard unemitted
	 * values (in case of cancellation or error).
	 * <p>
	 * A {@link Collection} is assumed to be finite, and for other iterables the {@link Spliterator}
	 * {@link Spliterator#SIZED} characteristic is looked for.
	 *
	 * @param iterable the {@link Iterable} to check.
	 * @param <T>
	 * @return true if the {@link Iterable} can confidently classified as finite, false if not finite/unsure
	 */
	static <T> boolean checkFinite(Iterable<T> iterable) {
		return iterable instanceof Collection || iterable.spliterator().hasCharacteristics(Spliterator.SIZED);
	}

	final Iterable<? extends T> iterable;
	@Nullable
	private final Runnable      onClose;

	FluxIterable(Iterable<? extends T> iterable, @Nullable Runnable onClose) {
		this.iterable = Objects.requireNonNull(iterable, "iterable");
		this.onClose = onClose;
	}

	FluxIterable(Iterable<? extends T> iterable) {
		this(iterable, null);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		boolean knownToBeFinite;
		Iterator<? extends T> it;

		try {
			knownToBeFinite = FluxIterable.checkFinite(iterable);
			it = iterable.iterator();
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		subscribe(actual, it, knownToBeFinite, onClose);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.BUFFERED) {
			if (iterable instanceof Collection) return ((Collection) iterable).size();
			if (iterable instanceof Tuple2) return ((Tuple2) iterable).size();
		}
		if (key == Attr.RUN_STYLE) {
		    return Attr.RunStyle.SYNC;
		}
		return null;
	}

	/**
	 * Common method to take an {@link Iterator} as a source of values.
	 *
	 * @param s the subscriber to feed this iterator to
	 * @param it the {@link Iterator} to use as a predictable source of values
	 */
	static <T> void subscribe(CoreSubscriber<? super T> s, Iterator<? extends T> it, boolean knownToBeFinite) {
		subscribe(s, it, knownToBeFinite, null);
	}

	/**
	 * Common method to take an {@link Iterator} as a source of values.
	 *
	 * @param s the subscriber to feed this iterator to
	 * @param it the {@link Iterator} to use as a source of values
	 * @param onClose close handler to call once we're done with the iterator (provided it
	 * is not null, this includes when the iteration errors or complete or the subscriber
	 * is cancelled). Null to ignore.
	 */
	@SuppressWarnings("unchecked")
	static <T> void subscribe(CoreSubscriber<? super T> s, Iterator<? extends T> it,
			boolean knownToBeFinite, @Nullable Runnable onClose) {
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
			if (onClose != null) {
				try {
					onClose.run();
				}
				catch (Throwable t) {
					Operators.onErrorDropped(t, s.currentContext());
				}
			}
			return;
		}
		if (!b) {
			Operators.complete(s);
			if (onClose != null) {
				try {
					onClose.run();
				}
				catch (Throwable t) {
					Operators.onErrorDropped(t, s.currentContext());
				}
			}
			return;
		}

		if (s instanceof ConditionalSubscriber) {
			s.onSubscribe(new IterableSubscriptionConditional<>((ConditionalSubscriber<? super T>) s,
					it, knownToBeFinite, onClose));
		}
		else {
			s.onSubscribe(new IterableSubscription<>(s, it, knownToBeFinite, onClose));
		}
	}

	static final class IterableSubscription<T>
			implements InnerProducer<T>, SynchronousSubscription<T> {

		final CoreSubscriber<? super T> actual;

		final Iterator<? extends T> iterator;
		final boolean               knownToBeFinite;
		final Runnable              onClose;

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
		Throwable hasNextFailure;

		IterableSubscription(CoreSubscriber<? super T> actual,
				Iterator<? extends T> iterator, boolean knownToBeFinite, @Nullable Runnable onClose) {
			this.actual = actual;
			this.iterator = iterator;
			this.knownToBeFinite = knownToBeFinite;
			this.onClose = onClose;
		}

		IterableSubscription(CoreSubscriber<? super T> actual,
				Iterator<? extends T> iterator, boolean knownToBeFinite) {
			this(actual, iterator, knownToBeFinite, null);
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

		private void onCloseWithDropError() {
			if (onClose != null) {
				try {
					onClose.run();
				}
				catch (Throwable t) {
					Operators.onErrorDropped(t, actual.currentContext());
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
						onCloseWithDropError();
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
						onCloseWithDropError();
						return;
					}

					if (cancelled) {
						return;
					}

					if (!b) {
						s.onComplete();
						onCloseWithDropError();
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
					onCloseWithDropError();
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
					onCloseWithDropError();
					return;
				}

				if (cancelled) {
					return;
				}

				if (!b) {
					s.onComplete();
					onCloseWithDropError();
					return;
				}
			}
		}

		@Override
		public void cancel() {
			onCloseWithDropError();
			cancelled = true;
			Operators.onDiscardMultiple(this.iterator, this.knownToBeFinite, actual.currentContext());
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
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public void clear() {
			Operators.onDiscardMultiple(this.iterator, this.knownToBeFinite, actual.currentContext());
			state = STATE_NO_NEXT;
		}

		@Override
		public boolean isEmpty() {
			int s = state;
			if (s == STATE_NO_NEXT) {
				return true;
			}
			else if (cancelled && !knownToBeFinite) {
				return true; //interrupts poll in discard loops due to cancellation
			}
			else if (s == STATE_HAS_NEXT_HAS_VALUE || s == STATE_HAS_NEXT_NO_VALUE) {
				return false;
			}
			else {
				boolean hasNext;
				try {
					hasNext = iterator.hasNext();
				}
				catch (Throwable t) {
					//this is a corner case, most Iterators are not expected to throw in hasNext.
					//since most calls to isEmpty are in preparation for poll() in fusion, we "defer"
					//the exception by pretending queueSub isn't empty, but keeping track of exception
					//to be re-thrown by a subsequent call to poll()
					state = STATE_HAS_NEXT_NO_VALUE;
					hasNextFailure = t;
					return false;
				}

				if (hasNext) {
					state = STATE_HAS_NEXT_NO_VALUE;
					return false;
				}
				state = STATE_NO_NEXT;
				return true;
			}
		}

		@Override
		@Nullable
		public T poll() {
			if (hasNextFailure != null) {
				state = STATE_NO_NEXT;
				throw Exceptions.propagate(hasNextFailure);
			}
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
					onCloseWithDropError();
					throw new NullPointerException("iterator returned a null value");
				}
				return c;
			}
			onCloseWithDropError();
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
		final boolean               knownToBeFinite;
		final Runnable              onClose;

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

		Throwable hasNextFailure;

		IterableSubscriptionConditional(ConditionalSubscriber<? super T> actual,
				Iterator<? extends T> iterator, boolean knownToBeFinite, @Nullable Runnable onClose) {
			this.actual = actual;
			this.iterator = iterator;
			this.knownToBeFinite = knownToBeFinite;
			this.onClose = onClose;
		}

		IterableSubscriptionConditional(ConditionalSubscriber<? super T> actual,
				Iterator<? extends T> iterator, boolean knownToBeFinite) {
			this(actual, iterator, knownToBeFinite, null);
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

		private void onCloseWithDropError() {
			if (onClose != null) {
				try {
					onClose.run();
				}
				catch (Throwable t) {
					Operators.onErrorDropped(t, actual.currentContext());
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
						onCloseWithDropError();
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
						onCloseWithDropError();
						return;
					}

					if (cancelled) {
						return;
					}

					if (!b) {
						s.onComplete();
						onCloseWithDropError();
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
					onCloseWithDropError();
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
					onCloseWithDropError();
					return;
				}

				if (cancelled) {
					return;
				}

				if (!b) {
					s.onComplete();
					onCloseWithDropError();
					return;
				}
			}
		}

		@Override
		public void cancel() {
			onCloseWithDropError();
			cancelled = true;
			Operators.onDiscardMultiple(this.iterator, this.knownToBeFinite, actual.currentContext());
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
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public void clear() {
			Operators.onDiscardMultiple(this.iterator, this.knownToBeFinite, actual.currentContext());
			state = STATE_NO_NEXT;
		}

		@Override
		public boolean isEmpty() {
			int s = state;
			if (s == STATE_NO_NEXT) {
				return true;
			}
			else if (cancelled && !knownToBeFinite) {
				return true; //interrupts poll() during discard loop if cancelled
			}
			else if (s == STATE_HAS_NEXT_HAS_VALUE || s == STATE_HAS_NEXT_NO_VALUE) {
				return false;
			}
			else {
				boolean hasNext;
				try {
					hasNext = iterator.hasNext();
				}
				catch (Throwable t) {
					//this is a corner case, most Iterators are not expected to throw in hasNext.
					//since most calls to isEmpty are in preparation for poll() in fusion, we "defer"
					//the exception by pretending queueSub isn't empty, but keeping track of exception
					//to be re-thrown by a subsequent call to poll()
					state = STATE_HAS_NEXT_NO_VALUE;
					hasNextFailure = t;
					return false;
				}

				if (hasNext) {
					state = STATE_HAS_NEXT_NO_VALUE;
					return false;
				}
				state = STATE_NO_NEXT;
				return true;
			}
		}

		@Override
		@Nullable
		public T poll() {
			if (hasNextFailure != null) {
				state = STATE_NO_NEXT;
				throw Exceptions.propagate(hasNextFailure);
			}
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
			onCloseWithDropError();
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
