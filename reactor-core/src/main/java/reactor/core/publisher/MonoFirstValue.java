package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

import java.util.Iterator;
import java.util.Objects;

final class MonoFirstValue<T> extends Mono<T> implements SourceProducer<T> {

	final Mono<? extends T>[] array;

	final Iterable<? extends Mono<? extends T>> iterable;

	@SafeVarargs
	MonoFirstValue(Mono<? extends T>... array) {
		this.array = Objects.requireNonNull(array, "array");
		this.iterable = null;
	}

	MonoFirstValue(Iterable<? extends Mono<? extends T>> iterable) {
		this.array = null;
		this.iterable = Objects.requireNonNull(iterable);
	}

	@Nullable
	Mono<T> orAdditionalSource(Mono<? extends T> other) {
		if (array != null) {
			int n = array.length;
			@SuppressWarnings("unchecked") Mono<? extends T>[] newArray = new Mono[n + 1];
			System.arraycopy(array, 0, newArray, 0, n);
			newArray[n] = other;

			return new MonoFirstValue<>(newArray);
		}
		return null;
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
				it = Objects.requireNonNull(iterable.iterator(), "The iterator returned is null");
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
						Operators.onOperatorError(new NullPointerException("The single source Publisher is null"),
								actual.currentContext()));
			}
			else {
				p.subscribe(actual);
			}
			return;
		}

		FluxFirstValues.RaceValuesCoordinator<T> coordinator =
				new FluxFirstValues.RaceValuesCoordinator<>(n);

		coordinator.subscribe(a, actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}
}
