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
final class FluxFirstValues<T> extends Flux<T> implements SourceProducer<T> {

	final Publisher<? extends T>[] array;

	final Iterable<? extends Publisher<? extends T>> iterable;

	@SafeVarargs
	FluxFirstValues(Publisher<? extends T>... array) {
		this.array = Objects.requireNonNull(array, "array");
		this.iterable = null;
	}

	FluxFirstValues(Iterable<? extends Publisher<? extends T>> iterable) {
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

		coordinator.subscribe(a, actual);
	}

	/**
	 * Returns a new instance which has the additional source to be amb'd together with
	 * the current array of sources.
	 * <p>
	 * This operation doesn't change the current FluxFirstValueEmitting instance.
	 *
	 * @param source the new source to merge with the others
	 *
	 * @return the new FluxFirstValueEmitting instance or null if the Amb runs with an Iterable
	 */
	@Nullable
	FluxFirstValues<T> orAdditionalSource(Publisher<? extends T> source) {
		if (array != null) {
			int n = array.length;
			@SuppressWarnings("unchecked") Publisher<? extends T>[] newArray =
					new Publisher[n + 1];
			System.arraycopy(array, 0, newArray, 0, n);
			newArray[n] = source;

			return new FluxFirstValues<>(newArray);
		}
		return null;
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
					   CoreSubscriber<? super T> actual) {

			for (int i = 0; i < sources.length; i++) {
				subscribers[i] = new FirstValuesEmittingSubscriber<T>(actual, this, i);
			}

			actual.onSubscribe(this);

			for (int i = 0; i < sources.length; i++) {
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
				e.addSuppressed(Exceptions.multiple(parent.errorsOrCompleteEmpty));
				actual.onError(e);
			}
		}
	}
}
