package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.subscriber.Subscribers;
import reactor.core.subscriber.SubscriptionHelper;

/**
 * Relays values from the main Publisher until another Publisher signals an event.
 *
 * @param <T> the value type of the main Publisher
 * @param <U> the value type of the other Publisher
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @since 2.5
 */
final class FluxTakeUntil<T, U> extends FluxSource<T, T> {

	final Publisher<U> other;

	public FluxTakeUntil(Publisher<? extends T> source, Publisher<U> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		TakeUntilMainSubscriber<T> mainSubscriber = new TakeUntilMainSubscriber<>(s);

		TakeUntilOtherSubscriber<U> otherSubscriber = new TakeUntilOtherSubscriber<>(mainSubscriber);

		other.subscribe(otherSubscriber);

		source.subscribe(mainSubscriber);
	}

	static final class TakeUntilOtherSubscriber<U> implements Subscriber<U> {
		final TakeUntilMainSubscriber<?> main;

		boolean once;

		public TakeUntilOtherSubscriber(TakeUntilMainSubscriber<?> main) {
			this.main = main;
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.setOther(s);

			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(U t) {
			onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (once) {
				return;
			}
			once = true;
			main.onError(t);
		}

		@Override
		public void onComplete() {
			if (once) {
				return;
			}
			once = true;
			main.onComplete();
		}


	}

	static final class TakeUntilMainSubscriber<T> implements Subscriber<T>, Subscription {
		final Subscriber<T> actual;

		volatile Subscription main;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<TakeUntilMainSubscriber, Subscription> MAIN =
		  AtomicReferenceFieldUpdater.newUpdater(TakeUntilMainSubscriber.class, Subscription.class, "main");

		volatile Subscription other;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<TakeUntilMainSubscriber, Subscription> OTHER =
		  AtomicReferenceFieldUpdater.newUpdater(TakeUntilMainSubscriber.class, Subscription.class, "other");

		public TakeUntilMainSubscriber(Subscriber<? super T> actual) {
			this.actual = Subscribers.serialize(actual);
		}

		void setOther(Subscription s) {
			if (!OTHER.compareAndSet(this, null, s)) {
				s.cancel();
				if (other != SubscriptionHelper.cancelled()) {
					SubscriptionHelper.reportSubscriptionSet();
				}
			}
		}

		@Override
		public void request(long n) {
			main.request(n);
		}

		void cancelMain() {
			Subscription s = main;
			if (s != SubscriptionHelper.cancelled()) {
				s = MAIN.getAndSet(this, SubscriptionHelper.cancelled());
				if (s != null && s != SubscriptionHelper.cancelled()) {
					s.cancel();
				}
			}
		}

		void cancelOther() {
			Subscription s = other;
			if (s != SubscriptionHelper.cancelled()) {
				s = OTHER.getAndSet(this, SubscriptionHelper.cancelled());
				if (s != null && s != SubscriptionHelper.cancelled()) {
					s.cancel();
				}
			}
		}

		@Override
		public void cancel() {
			cancelMain();
			cancelOther();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!MAIN.compareAndSet(this, null, s)) {
				s.cancel();
				if (main != SubscriptionHelper.cancelled()) {
					SubscriptionHelper.reportSubscriptionSet();
				}
			} else {
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {

			if (main == null) {
				if (MAIN.compareAndSet(this, null, SubscriptionHelper.cancelled())) {
					SubscriptionHelper.error(actual, t);
					return;
				}
			}
			cancel();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (main == null) {
				if (MAIN.compareAndSet(this, null, SubscriptionHelper.cancelled())) {
					cancelOther();
					SubscriptionHelper.complete(actual);
					return;
				}
			}
			cancel();

			actual.onComplete();
		}
	}
}
