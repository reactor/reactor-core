package reactor.core.publisher;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.flow.Loopback;
import reactor.core.subscriber.MultiSubscriptionSubscriber;

/**
 * Switches to another source if the first source turns out to be empty.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxSwitchIfEmpty<T> extends FluxSource<T, T> {

	final Publisher<? extends T> other;

	public FluxSwitchIfEmpty(Publisher<? extends T> source, Publisher<? extends T> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		SwitchIfEmptySubscriber<T> parent = new SwitchIfEmptySubscriber<>(s, other);

		s.onSubscribe(parent);

		source.subscribe(parent);
	}

	static final class SwitchIfEmptySubscriber<T> extends MultiSubscriptionSubscriber<T, T>
			implements Loopback {

		final Publisher<? extends T> other;

		boolean once;

		public SwitchIfEmptySubscriber(Subscriber<? super T> actual, Publisher<? extends T> other) {
			super(actual);
			this.other = other;
		}

		@Override
		public void onNext(T t) {
			if (!once) {
				once = true;
			}

			subscriber.onNext(t);
		}

		@Override
		public void onComplete() {
			if (!once) {
				once = true;

				other.subscribe(this);
			} else {
				subscriber.onComplete();
			}
		}

		@Override
		public Object connectedInput() {
			return null;
		}

		@Override
		public Object connectedOutput() {
			return other;
		}
	}
}
