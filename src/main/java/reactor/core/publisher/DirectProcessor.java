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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.MultiProducer;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;

/**
 * Dispatches onNext, onError and onComplete signals to zero-to-many Subscribers.
 * <p>
 * <p>
 * This implementation signals an IllegalStateException if a Subscriber is not ready to receive a value due to not
 * requesting enough.
 * <p>
 * <p>
 * The implementation ignores Subscriptions set via onSubscribe.
 * <p>
 * <p>
 * A terminated DirectProcessor will emit the terminal signal to late subscribers.
 *
 * @param <T> the input and output value type
 */
public final class DirectProcessor<T>
		extends FluxProcessor<T, T>
	implements Receiver, MultiProducer {


	/**
	 * Create a new {@link DirectProcessor}
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> DirectProcessor<E> create() {
		return new DirectProcessor<>();
	}

	@SuppressWarnings("rawtypes")
	private static final DirectProcessorSubscription[] EMPTY = new DirectProcessorSubscription[0];

	@SuppressWarnings("rawtypes")
	private static final DirectProcessorSubscription[] TERMINATED = new DirectProcessorSubscription[0];

	@SuppressWarnings("unchecked")
	private volatile	 DirectProcessorSubscription<T>[]										   subscribers = EMPTY;
	@SuppressWarnings("rawtypes")
	private static final AtomicReferenceFieldUpdater<DirectProcessor, DirectProcessorSubscription[]>SUBSCRIBERS =
	  AtomicReferenceFieldUpdater.newUpdater(DirectProcessor.class,
		DirectProcessorSubscription[].class,
		"subscribers");

	Throwable error;

	DirectProcessor() {
	}

	@Override
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}

	@Override
	public void onSubscribe(Subscription s) {
		Objects.requireNonNull(s, "s");
		if (subscribers != TERMINATED) {
			s.request(Long.MAX_VALUE);
		} else {
			s.cancel();
		}
	}

	@Override
	public void onNext(T t) {
		Objects.requireNonNull(t, "t");

		for (DirectProcessorSubscription<T> s : subscribers) {
			s.onNext(t);
		}
	}

	@Override
	public void onError(Throwable t) {
		Objects.requireNonNull(t, "t");

		error = t;
		for (DirectProcessorSubscription<?> s : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			s.onError(t);
		}
	}

	@Override
	public void onComplete() {
		for (DirectProcessorSubscription<?> s : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			s.onComplete();
		}
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Objects.requireNonNull(s, "s");

		DirectProcessorSubscription<T> p = new DirectProcessorSubscription<>(s, this);
		s.onSubscribe(p);

		if (add(p)) {
			if (p.cancelled) {
				remove(p);
			}
		} else {
			Throwable e = error;
			if (e != null) {
				s.onError(e);
			} else {
				s.onComplete();
			}
		}
	}

	@Override
	public boolean isStarted() {
		return true;
	}

	@Override
	public boolean isTerminated() {
		return TERMINATED == subscribers;
	}

	@Override
	public Iterator<?> downstreams() {
		return Arrays.asList(subscribers).iterator();
	}

	@Override
	public long downstreamCount() {
		return subscribers.length;
	}

	boolean add(DirectProcessorSubscription<T> s) {
		DirectProcessorSubscription<T>[] a = subscribers;
		if (a == TERMINATED) {
			return false;
		}

		synchronized (this) {
			a = subscribers;
			if (a == TERMINATED) {
				return false;
			}
			int len = a.length;

			@SuppressWarnings("unchecked") DirectProcessorSubscription<T>[] b = new DirectProcessorSubscription[len + 1];
			System.arraycopy(a, 0, b, 0, len);
			b[len] = s;

			subscribers = b;

			return true;
		}
	}

	@SuppressWarnings("unchecked")
	void remove(DirectProcessorSubscription<T> s) {
		DirectProcessorSubscription<T>[] a = subscribers;
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

			DirectProcessorSubscription<T>[] b = new DirectProcessorSubscription[len - 1];
			System.arraycopy(a, 0, b, 0, j);
			System.arraycopy(a, j + 1, b, j, len - j - 1);

			subscribers = b;
		}
	}

	@Override
	public boolean hasDownstreams() {
		DirectProcessorSubscription<T>[] s = subscribers;
		return s != EMPTY && s != TERMINATED;
	}

	public boolean hasCompleted() {
		return subscribers == TERMINATED && error == null;
	}

	public boolean hasError() {
		return subscribers == TERMINATED && error != null;
	}

	@Override
	public Throwable getError() {
		if (subscribers == TERMINATED) {
			return error;
		}
		return null;
	}

	static final class DirectProcessorSubscription<T> implements Subscription,
																 Receiver, Producer,
																 Trackable {

		final Subscriber<? super T> actual;

		final DirectProcessor<T> parent;

		volatile boolean cancelled;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<DirectProcessorSubscription> REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(DirectProcessorSubscription.class, "requested");

		public DirectProcessorSubscription(Subscriber<? super T> actual, DirectProcessor<T> parent) {
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
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public Subscriber<? super T> downstream() {
			return actual;
		}

		@Override
		public long requestedFromDownstream() {
			return 0;
		}

		@Override
		public Processor<T, T> upstream() {
			return parent;
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
			actual.onError(new IllegalStateException("Can't deliver value due to lack of requests"));
		}

		void onError(Throwable e) {
			actual.onError(e);
		}

		void onComplete() {
			actual.onComplete();
		}

		@Override
		public boolean isStarted() {
			return parent.isStarted();
		}

		@Override
		public boolean isTerminated() {
			return parent.isTerminated();
		}
	}

	@Override
	public Object upstream() {
		return null;
	}
}
