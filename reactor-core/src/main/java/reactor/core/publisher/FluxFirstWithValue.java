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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Stream;

/**
 * Given a set of source Publishers the values of that Publisher is forwarded to the
 * subscriber which responds first with a value.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxFirstWithValue<T> extends Flux<T> implements SourceProducer<T> {

	final Publisher<? extends T>[] array;

	final Iterable<? extends Publisher<? extends T>> iterable;

	private FluxFirstWithValue(Publisher<? extends T>[] array) {
		this.array = Objects.requireNonNull(array, "array");
		this.iterable = null;
	}

	@SafeVarargs
	FluxFirstWithValue(Publisher<? extends T> first, Publisher<? extends T>... others) {
		Objects.requireNonNull(first, "first");
		Objects.requireNonNull(others, "others");
		@SuppressWarnings("unchecked")
		Publisher<? extends T>[] newArray = new Publisher[others.length + 1];
		newArray[0] = first;
		System.arraycopy(others, 0, newArray, 1, others.length);
		this.array = newArray;
		this.iterable = null;
	}

	FluxFirstWithValue(Iterable<? extends Publisher<? extends T>> iterable) {
		this.array = null;
		this.iterable = Objects.requireNonNull(iterable);
	}

	/**
	 * Returns a new instance which has the additional sources to be flattened together with
	 * the current array of sources.
	 * <p>
	 * This operation doesn't change the current {@link FluxFirstWithValue} instance.
	 *
	 * @param others the new sources to merge with the current sources
	 *
	 * @return the new {@link FluxFirstWithValue} instance or null if new sources cannot be added (backed by an Iterable)
	 */
	@SafeVarargs
	@Nullable
	final FluxFirstWithValue<T> firstValuedAdditionalSources(Publisher<? extends T>... others) {
		Objects.requireNonNull(others, "others");
		if (others.length == 0) {
			return this;
		}
		if (array == null) {
			//iterable mode, returning null to convey 2 nested operators are needed here
			return null;
		}
		int currentSize = array.length;
		int otherSize = others.length;
		@SuppressWarnings("unchecked")
		Publisher<? extends T>[] newArray = new Publisher[currentSize + otherSize];
		System.arraycopy(array, 0, newArray, 0, currentSize);
		System.arraycopy(others, 0, newArray, currentSize, otherSize);

		return new FluxFirstWithValue<>(newArray);
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
			} catch (Throwable e) {
				Operators.error(actual, Operators.onOperatorError(e,
						actual.currentContext()));
				return;
			}

			for (; ; ) {

				boolean b;

				try {
					b = it.hasNext();
				} catch (Throwable e) {
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
				} catch (Throwable e) {
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

		} else {
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
			} else {
				p.subscribe(actual);
			}
			return;
		}

		RaceValuesCoordinator<T> coordinator = new RaceValuesCoordinator<>(n);

		coordinator.subscribe(a, n, actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	static final class RaceValuesCoordinator<T>
			implements Subscription, Scannable {

		final FirstValuesEmittingSubscriber<T>[] subscribers;
		final Throwable[] errorsOrCompleteEmpty;

		volatile boolean cancelled;

		@SuppressWarnings("rawtypes")
		volatile int winner;
		static final AtomicIntegerFieldUpdater<RaceValuesCoordinator> WINNER =
				AtomicIntegerFieldUpdater.newUpdater(RaceValuesCoordinator.class, "winner");

		@SuppressWarnings("rawtypes")
		volatile int nbErrorsOrCompletedEmpty;
		static final AtomicIntegerFieldUpdater<RaceValuesCoordinator> ERRORS_OR_COMPLETED_EMPTY =
				AtomicIntegerFieldUpdater.newUpdater(RaceValuesCoordinator.class, "nbErrorsOrCompletedEmpty");

		@SuppressWarnings("unchecked")
		public RaceValuesCoordinator(int n) {
			subscribers = new FirstValuesEmittingSubscriber[n];
			errorsOrCompleteEmpty = new Throwable[n];
			winner = Integer.MIN_VALUE;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;

			return null;
		}

		void subscribe(Publisher<? extends T>[] sources,
				int n, CoreSubscriber<? super T> actual) {

			for (int i = 0; i < n; i++) {
				subscribers[i] = new FirstValuesEmittingSubscriber<T>(actual, this, i);
			}

			actual.onSubscribe(this);

			for (int i = 0; i < n; i++) {
				if (cancelled || winner != Integer.MIN_VALUE) {
					return;
				}

				if (sources[i] == null) {
					actual.onError(new NullPointerException("The " + i + " th Publisher source is null"));
					return;
				}

				sources[i].subscribe(subscribers[i]);
			}
		}


		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				int w = winner;
				if (w >= 0) {
					subscribers[w].request(n);
				} else {
					for (FirstValuesEmittingSubscriber<T> s : subscribers) {
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
			} else {
				for (FirstValuesEmittingSubscriber<T> s : subscribers) {
					s.cancel();
				}
			}
		}

		boolean tryWin(int index) {
			if (winner == Integer.MIN_VALUE) {
				if (WINNER.compareAndSet(this, Integer.MIN_VALUE, index)) {
					for (int i = 0; i < subscribers.length; i++) {
						if (i != index) {
							subscribers[i].cancel();
							errorsOrCompleteEmpty[i] = null;
						}
					}
					return true;
				}
			}
			return false;
		}

	}

	static final class FirstValuesEmittingSubscriber<T> extends Operators.DeferredSubscription
			implements InnerOperator<T, T> {

		final RaceValuesCoordinator<T> parent;

		final CoreSubscriber<? super T> actual;

		final int index;

		boolean won;

		FirstValuesEmittingSubscriber(CoreSubscriber<? super T> actual, RaceValuesCoordinator<T> parent, int index) {
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
			} else if (parent.tryWin(index)) {
				won = true;
				actual.onNext(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (won) {
				actual.onError(t);
			} else {
				recordTerminalSignals(t);
			}
		}

		@Override
		public void onComplete() {
			if (won) {
				actual.onComplete();
			} else {
				recordTerminalSignals(new NoSuchElementException("source at index " + index + " completed empty"));
			}
		}

		void recordTerminalSignals(Throwable t) {
			parent.errorsOrCompleteEmpty[index] = t;
			int nb = RaceValuesCoordinator.ERRORS_OR_COMPLETED_EMPTY.incrementAndGet(parent);

			if (nb == parent.subscribers.length) {
				NoSuchElementException e = new NoSuchElementException("All sources completed with error or without values");
				e.initCause(Exceptions.multiple(parent.errorsOrCompleteEmpty));
				actual.onError(e);
			}
		}
	}
}
