/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Given a set of source Publishers the values of that Publisher is forwarded to the
 * subscriber which responds first with any signal.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxFirstWithSignal<T> extends Flux<T> implements SourceProducer<T> {

	final Publisher<? extends T>[] array;

	final Iterable<? extends Publisher<? extends T>> iterable;

	@SafeVarargs
	FluxFirstWithSignal(Publisher<? extends T>... array) {
		this.array = Objects.requireNonNull(array, "array");
		this.iterable = null;
	}

	FluxFirstWithSignal(Iterable<? extends Publisher<? extends T>> iterable) {
		this.array = null;
		this.iterable = Objects.requireNonNull(iterable);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Publisher<? extends T>[] a = array;
		int n;
		if (a == null) {
			n = 0;
			a = new Publisher[8];

			Iterator<? extends Publisher<? extends T>> it;

			try {
				it = Objects.requireNonNull(iterable.iterator(),
						"The iterator returned is null");
			}
			catch (Throwable e) {
				Operators.error(actual, Operators.onOperatorError(e,
						actual.currentContext()));
				return;
			}

			for (; ; ) {

				boolean b;

				try {
					b = it.hasNext();
				}
				catch (Throwable e) {
					Operators.error(actual, Operators.onOperatorError(e,
							actual.currentContext()));
					return;
				}

				if (!b) {
					break;
				}

				Publisher<? extends T> p;

				try {
					p = Objects.requireNonNull(it.next(),
							"The Publisher returned by the iterator is null");
				}
				catch (Throwable e) {
					Operators.error(actual, Operators.onOperatorError(e,
							actual.currentContext()));
					return;
				}

				if (n == a.length) {
					Publisher<? extends T>[] c = new Publisher[n + (n >> 2)];
					System.arraycopy(a, 0, c, 0, n);
					a = c;
				}
				a[n++] = p;
			}

		}
		else {
			n = a.length;
		}

		if (n == 0) {
			Operators.complete(actual);
			return;
		}
		if (n == 1) {
			Publisher<? extends T> p = a[0];

			if (p == null) {
				Operators.error(actual,
						new NullPointerException("The single source Publisher is null"));
			}
			else {
				p.subscribe(actual);
			}
			return;
		}

		RaceCoordinator<T> coordinator = new RaceCoordinator<>(n);

		coordinator.subscribe(a, n, actual);
	}

	/**
	 * Returns a new instance which has the additional source to be amb'd together with
	 * the current array of sources.
	 * <p>
	 * This operation doesn't change the current {@link FluxFirstWithSignal} instance.
	 *
	 * @param source the new source to merge with the others
	 *
	 * @return the new {@link FluxFirstWithSignal} instance or null if the Amb runs with an Iterable
	 */
	@Nullable
	FluxFirstWithSignal<T> orAdditionalSource(Publisher<? extends T> source) {
		if (array != null) {
			int n = array.length;
			@SuppressWarnings("unchecked") Publisher<? extends T>[] newArray =
					new Publisher[n + 1];
			System.arraycopy(array, 0, newArray, 0, n);
			newArray[n] = source;

			return new FluxFirstWithSignal<>(newArray);
		}
		return null;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	static final class RaceCoordinator<T>
			implements Subscription, Scannable {

		final FirstEmittingSubscriber<T>[] subscribers;

		volatile boolean cancelled;

		volatile int winner;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RaceCoordinator> WINNER =
				AtomicIntegerFieldUpdater.newUpdater(RaceCoordinator.class, "winner");

		@SuppressWarnings("unchecked")
		RaceCoordinator(int n) {
			subscribers = new FirstEmittingSubscriber[n];
			winner = Integer.MIN_VALUE;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;

			return null;
		}

		void subscribe(Publisher<? extends T>[] sources,
				int n,
				CoreSubscriber<? super T> actual) {
			FirstEmittingSubscriber<T>[] a = subscribers;

			for (int i = 0; i < n; i++) {
				a[i] = new FirstEmittingSubscriber<>(actual, this, i);
			}

			actual.onSubscribe(this);

			for (int i = 0; i < n; i++) {
				if (cancelled || winner != Integer.MIN_VALUE) {
					return;
				}

				Publisher<? extends T> p = sources[i];

				if (p == null) {
					if (WINNER.compareAndSet(this, Integer.MIN_VALUE, -1)) {
						actual.onError(new NullPointerException("The " + i + " th Publisher source is null"));
					}
					return;
				}

				p.subscribe(a[i]);
			}

		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				int w = winner;
				if (w >= 0) {
					subscribers[w].request(n);
				}
				else {
					for (FirstEmittingSubscriber<T> s : subscribers) {
						s.request(n);
					}
				}
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			cancelled = true;

			int w = winner;
			if (w >= 0) {
				subscribers[w].cancel();
			}
			else {
				for (FirstEmittingSubscriber<T> s : subscribers) {
					s.cancel();
				}
			}
		}

		boolean tryWin(int index) {
			if (winner == Integer.MIN_VALUE) {
				if (WINNER.compareAndSet(this, Integer.MIN_VALUE, index)) {

					FirstEmittingSubscriber<T>[] a = subscribers;
					int n = a.length;

					for (int i = 0; i < n; i++) {
						if (i != index) {
							a[i].cancel();
						}
					}

					return true;
				}
			}
			return false;
		}
	}

	static final class FirstEmittingSubscriber<T> extends Operators.DeferredSubscription
			implements InnerOperator<T, T> {

		final RaceCoordinator<T> parent;

		final CoreSubscriber<? super T> actual;

		final int index;

		boolean won;

		FirstEmittingSubscriber(CoreSubscriber<? super T> actual,
				RaceCoordinator<T> parent,
				int index) {
			this.actual = actual;
			this.parent = parent;
			this.index = index;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return parent.cancelled;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			set(s);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void onNext(T t) {
			if (won) {
				actual.onNext(t);
			}
			else if (parent.tryWin(index)) {
				won = true;
				actual.onNext(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (won) {
				actual.onError(t);
			}
			else if (parent.tryWin(index)) {
				won = true;
				actual.onError(t);
			}
		}

		@Override
		public void onComplete() {
			if (won) {
				actual.onComplete();
			}
			else if (parent.tryWin(index)) {
				won = true;
				actual.onComplete();
			}
		}
	}
}