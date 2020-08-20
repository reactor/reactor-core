package reactor.core.publisher;


import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

import java.util.Iterator;
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
final class FluxFirstValueEmitting<T> extends Flux<T> implements SourceProducer<T> {

	final Publisher<? extends T>[] array;

	final Iterable<? extends Publisher<? extends T>> iterable;

	private final static Throwable completedEmpty = new Throwable();

	@SafeVarargs
	FluxFirstValueEmitting(Publisher<? extends T>... array) {
		this.array = Objects.requireNonNull(array, "array");
		this.iterable = null;
	}

	FluxFirstValueEmitting(Iterable<? extends Publisher<? extends T>> iterable) {
		this.array = null;
		this.iterable = Objects.requireNonNull(iterable);
	}

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

		RaceValueCoordinator<T> coordinator = new RaceValueCoordinator<>(n);

		coordinator.subscribe(a, actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	static final class RaceValueCoordinator<T>
			implements Subscription, Scannable {

		final FirstValueEmittingSubscriber<T>[] subscribers;
		final Throwable[] errorsOrCompleteEmpty;

		volatile boolean cancelled;

		@SuppressWarnings("rawtypes")
		volatile int wip;
		static final AtomicIntegerFieldUpdater<RaceValueCoordinator> WIP =
				AtomicIntegerFieldUpdater.newUpdater(RaceValueCoordinator.class, "wip");

		@SuppressWarnings("rawtypes")
		volatile int nbErrorsOrCompletedEmpty;
		static final AtomicIntegerFieldUpdater<RaceValueCoordinator> ERRORS_OR_COMPLETED_EMPTY =
				AtomicIntegerFieldUpdater.newUpdater(RaceValueCoordinator.class, "nbErrorsOrCompletedEmpty");

		@SuppressWarnings("unchecked")
		public RaceValueCoordinator(int n) {
			subscribers = new FirstValueEmittingSubscriber[n];
			errorsOrCompleteEmpty = new Throwable[n];
			wip = Integer.MIN_VALUE;
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
				subscribers[i] = new FirstValueEmittingSubscriber<T>(actual, this, i);
			}

			actual.onSubscribe(this);

			for (int i = 0; i < sources.length; i++) {
				if (cancelled || wip != Integer.MIN_VALUE) {
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
				int w = wip;
				if (w >= 0) {
					subscribers[w].request(n);
				} else {
					for (FirstValueEmittingSubscriber<T> s : subscribers) {
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

			int w = wip;
			if (w >= 0) {
				subscribers[w].cancel();
			} else {
				for (FirstValueEmittingSubscriber<T> s : subscribers) {
					s.cancel();
				}
			}
		}

		boolean tryWin(int index) {
			if (wip == Integer.MIN_VALUE) {
				if (WIP.compareAndSet(this, Integer.MIN_VALUE, index)) {
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

	static final class FirstValueEmittingSubscriber<T> extends Operators.DeferredSubscription
			implements InnerOperator<T, T> {

		final RaceValueCoordinator<T> parent;

		final CoreSubscriber<? super T> actual;

		final int index;

		boolean won;

		FirstValueEmittingSubscriber(CoreSubscriber<? super T> actual, RaceValueCoordinator<T> parent, int index) {
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
				recordTerminalSignals(completedEmpty);
			}
		}

		void recordTerminalSignals(Throwable t) {
			parent.errorsOrCompleteEmpty[index] = t;
			int nb = RaceValueCoordinator.ERRORS_OR_COMPLETED_EMPTY.incrementAndGet(parent);

			if (nb == parent.subscribers.length) {
				actual.onError(Exceptions.multiple(parent.errorsOrCompleteEmpty));
			}
		}
	}
}
