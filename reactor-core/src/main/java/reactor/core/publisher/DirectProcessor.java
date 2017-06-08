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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.context.Context;

/**
 * Dispatches onNext, onError and onComplete signals to zero-to-many Subscribers.
 * <p>
 * <p>
 * This implementation signals an IllegalStateException if a Subscriber is not ready to
 * receive a value due to not requesting enough.
 * <p>
 * <p>
 * The implementation ignores Subscriptions set via onSubscribe.
 * <p>
 * <p>
 * A terminated DirectProcessor will emit the terminal signal to late subscribers.
 *
 * @param <T> the input and output value type
 */
public final class DirectProcessor<T> extends FluxProcessor<T, T> {

	/**
	 * Create a new {@link DirectProcessor}
	 *
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> DirectProcessor<E> create() {
		return new DirectProcessor<>();
	}

	@SuppressWarnings("rawtypes")
	private static final DirectInner[] EMPTY = new DirectInner[0];

	@SuppressWarnings("rawtypes")
	private static final DirectInner[] TERMINATED = new DirectInner[0];

	@SuppressWarnings("unchecked")
	private volatile     DirectInner<T>[] subscribers = EMPTY;
	@SuppressWarnings("rawtypes")
	private static final AtomicReferenceFieldUpdater<DirectProcessor, DirectInner[]>
	                                      SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(DirectProcessor.class,
					DirectInner[].class,
					"subscribers");

	Throwable error;

	DirectProcessor() {
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void onSubscribe(Subscription s) {
		Objects.requireNonNull(s, "s");
		if (subscribers != TERMINATED) {
			s.request(Long.MAX_VALUE);
		}
		else {
			s.cancel();
		}
	}

	@Override
	public void onNext(T t) {
		Objects.requireNonNull(t, "t");

		for (DirectInner<T> s : subscribers) {
			s.onNext(t);
		}
	}

	@Override
	public void onError(Throwable t) {
		Objects.requireNonNull(t, "t");

		error = t;
		for (DirectInner<?> s : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			s.onError(t);
		}
	}

	@Override
	public void onComplete() {
		for (DirectInner<?> s : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			s.onComplete();
		}
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		//noinspection ConstantConditions
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}

		DirectInner<T> p = new DirectInner<>(s, this);
		s.onSubscribe(p);

		if (add(p)) {
			if (p.cancelled) {
				remove(p);
			}
		}
		else {
			Throwable e = error;
			if (e != null) {
				s.onError(e);
			}
			else {
				s.onComplete();
			}
		}
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	@Override
	public boolean isTerminated() {
		return TERMINATED == subscribers;
	}

	@Override
	public long downstreamCount() {
		return subscribers.length;
	}

	boolean add(DirectInner<T> s) {
		DirectInner<T>[] a = subscribers;
		if (a == TERMINATED) {
			return false;
		}

		synchronized (this) {
			a = subscribers;
			if (a == TERMINATED) {
				return false;
			}
			int len = a.length;

			@SuppressWarnings("unchecked") DirectInner<T>[] b = new DirectInner[len + 1];
			System.arraycopy(a, 0, b, 0, len);
			b[len] = s;

			subscribers = b;

			return true;
		}
	}

	@SuppressWarnings("unchecked")
	void remove(DirectInner<T> s) {
		DirectInner<T>[] a = subscribers;
		if (a == TERMINATED || a == EMPTY) {
			return;
		}

		synchronized (this) {
			a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			int len = a.length;

			int j = -1;

			for (int i = 0; i < len; i++) {
				if (a[i] == s) {
					j = i;
					break;
				}
			}
			if (j < 0) {
				return;
			}
			if (len == 1) {
				subscribers = EMPTY;
				return;
			}

			DirectInner<T>[] b = new DirectInner[len - 1];
			System.arraycopy(a, 0, b, 0, j);
			System.arraycopy(a, j + 1, b, j, len - j - 1);

			subscribers = b;
		}
	}

	@Override
	public boolean hasDownstreams() {
		DirectInner<T>[] s = subscribers;
		return s != EMPTY && s != TERMINATED;
	}

	/**
	 * Return true if terminated with onComplete
	 *
	 * @return true if terminated with onComplete
	 */
	public boolean hasCompleted() {
		return subscribers == TERMINATED && error == null;
	}

	/**
	 * Return true if terminated with onError
	 *
	 * @return true if terminated with onError
	 */
	public boolean hasError() {
		return subscribers == TERMINATED && error != null;
	}

	@Override
	@Nullable
	public Throwable getError() {
		if (subscribers == TERMINATED) {
			return error;
		}
		return null;
	}

	static final class DirectInner<T> implements InnerProducer<T> {

		final Subscriber<? super T> actual;

		final DirectProcessor<T> parent;

		volatile boolean cancelled;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<DirectInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(DirectInner.class, "requested");

		DirectInner(Subscriber<? super T> actual, DirectProcessor<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				parent.remove(this);
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) {
				return parent;
			}
			if (key == BooleanAttr.CANCELLED) {
				return cancelled;
			}

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		void onNext(T value) {
			if (requested != 0L) {
				actual.onNext(value);
				if (requested != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return;
			}
			parent.remove(this);
			actual.onError(Exceptions.failWithOverflow(
					"Can't deliver value due to lack of requests"));
		}

		void onError(Throwable e) {
			actual.onError(e);
		}

		void onComplete() {
			actual.onComplete();
		}

	}
}
