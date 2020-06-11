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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Dispatches onNext, onError and onComplete signals to zero-to-many Subscribers.
 * Please note, that along with multiple consumers, current implementation of
 * DirectProcessor supports multiple producers. However, all producers must produce
 * messages on the same Thread, otherwise
 * <a href="https://www.reactive-streams.org/">Reactive Streams Spec</a> contract is
 * violated.
 * <p>
 *      <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.2.0.M2/src/docs/marble/directprocessornormal.png" alt="">
 * </p>
 *
 * </br>
 * </br>
 *
 * <p>
 *     <b>Note: </b> DirectProcessor does not coordinate backpressure between its
 *     Subscribers and the upstream, but consumes its upstream in an
 *     unbounded manner.
 *     In the case where a downstream Subscriber is not ready to receive items (hasn't
 *     requested yet or enough), it will be terminated with an
 *     <i>{@link IllegalStateException}</i>.
 *     Hence in terms of interaction model, DirectProcessor only supports PUSH from the
 *     source through the processor to the Subscribers.
 *
 *     <p>
 *        <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.2.0.M2/src/docs/marble/directprocessorerror.png" alt="">
 *     </p>
 * </p>
 *
 * </br>
 * </br>
 *
 * <p>
 *      <b>Note: </b> If there are no Subscribers, upstream items are dropped and only
 *      the terminal events are retained. A terminated DirectProcessor will emit the
 *      terminal signal to late subscribers.
 *
 *      <p>
 *         <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.2.0.M2/src/docs/marble/directprocessorterminal.png" alt="">
 *      </p>
 * </p>
 *
 * </br>
 * </br>
 *
 * <p>
 *      <b>Note: </b> The implementation ignores Subscriptions set via onSubscribe.
 * </p>
 *
 * @param <T> the input and output value type
 * @deprecated Prefer clear cut usage of either {@link Processors} or {@link Sinks}, to be removed in 3.5
 */
@Deprecated
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

		DirectInner<T>[] inners = subscribers;

		if (inners == TERMINATED) {
			Operators.onNextDropped(t, currentContext());
			return;
		}

		for (DirectInner<T> s : inners) {
			s.onNext(t);
		}
	}

	@Override
	public void onError(Throwable t) {
		Objects.requireNonNull(t, "t");

		DirectInner<T>[] inners = subscribers;

		if (inners == TERMINATED) {
			Operators.onErrorDropped(t, currentContext());
			return;
		}

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
	public void subscribe(CoreSubscriber<? super T> actual) {
		Objects.requireNonNull(actual, "subscribe");

		DirectInner<T> p = new DirectInner<>(actual, this);
		actual.onSubscribe(p);

		if (add(p)) {
			if (p.cancelled) {
				remove(p);
			}
		}
		else {
			Throwable e = error;
			if (e != null) {
				actual.onError(e);
			}
			else {
				actual.onComplete();
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

	@Override
	@Nullable
	public Throwable getError() {
		if (subscribers == TERMINATED) {
			return error;
		}
		return null;
	}

	static final class DirectInner<T> implements InnerProducer<T> {

		final CoreSubscriber<? super T> actual;

		final DirectProcessor<T> parent;

		volatile boolean cancelled;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<DirectInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(DirectInner.class, "requested");

		DirectInner(CoreSubscriber<? super T> actual, DirectProcessor<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
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
			if (key == Attr.PARENT) return parent;
			if (key == Attr.CANCELLED) return cancelled;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
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
