package reactor.core.publisher;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberMultiSubscription;

/**
 * Switches to another source if the first source turns out to be empty.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class FluxSwitchIfEmpty<T> extends reactor.Flux.FluxBarrier<T, T> {

	final Publisher<? extends T> other;

	public FluxSwitchIfEmpty(Publisher<? extends T> source, Publisher<? extends T> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		FluxSwitchIfEmptySubscriber<T> parent = new FluxSwitchIfEmptySubscriber<>(s, other);

		s.onSubscribe(parent);

		source.subscribe(parent);
	}

	static final class FluxSwitchIfEmptySubscriber<T> extends SubscriberMultiSubscription<T, T>
	implements FeedbackLoop {

		final Publisher<? extends T> other;

		boolean once;

		public FluxSwitchIfEmptySubscriber(Subscriber<? super T> actual, Publisher<? extends T> other) {
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
		public Object delegateInput() {
			return null;
		}

		@Override
		public Object delegateOutput() {
			return other;
		}
	}
}
