package reactor.core.scrabble;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

/**
 * Streams the characters of a string.
 */
final class FluxCharSequence extends Flux<Integer> implements Fuseable {

	final CharSequence string;

	FluxCharSequence(CharSequence string) {
		this.string = string;
	}

	@Override
	public void subscribe(CoreSubscriber<? super Integer> actual) {
		actual.onSubscribe(new CharSequenceSubscription(actual, string));
	}

	static final class CharSequenceSubscription implements Fuseable.QueueSubscription<Integer> {

		final CoreSubscriber<? super Integer> actual;

		final CharSequence string;

		final int end;

		int index;

		volatile boolean cancelled;

		volatile     long                                             requested;
		CharSequenceSubscription(CoreSubscriber<? super Integer> actual, CharSequence string) {
			this.actual = actual;
			this.string = string;
			this.end = string.length();
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		@Override
		public void clear() {
			index = end;
		}

		@Override
		public boolean isEmpty() {
			return index != end;
		}

		@Override
		public Integer poll() {
			int i = index;
			if (i != end) {
				index = i + 1;
				return (int) string.charAt(i);
			}
			return null;
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

		@Override
		public int requestFusion(int requestedMode) {
			return requestedMode & Fuseable.SYNC;
		}

		@Override
		public int size() {
			return end - index;
		}

		void fastPath() {
			int e = end;
			CharSequence s = string;
			Subscriber<? super Integer> a = actual;

			for (int i = index; i != e; i++) {
				if (cancelled) {
					return;
				}

				a.onNext((int) s.charAt(i));
			}

			if (!cancelled) {
				a.onComplete();
			}
		}

		void slowPath(long r) {
			long e = 0L;
			int i = index;
			int f = end;
			CharSequence s = string;
			Subscriber<? super Integer> a = actual;

			for (; ; ) {

				while (e != r && i != f) {
					if (cancelled) {
						return;
					}

					a.onNext((int) s.charAt(i));

					i++;
					e++;
				}

				if (i == f) {
					if (!cancelled) {
						a.onComplete();
					}
					return;
				}

				r = requested;
				if (e == r) {
					index = i;
					r = REQUESTED.addAndGet(this, -e);
					if (r == 0L) {
						break;
					}
				}
			}
		}
		static final AtomicLongFieldUpdater<CharSequenceSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(CharSequenceSubscription.class, "requested");
	}

}