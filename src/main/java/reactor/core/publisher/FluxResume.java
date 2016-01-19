package reactor.core.publisher;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.subscriber.SubscriberMultiSubscription;
import reactor.fn.Function;

/**
 * Resumes the failed main sequence with another sequence returned by
 * a function for the particular failure exception.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class FluxResume<T> extends reactor.Flux.FluxBarrier<T, T> {

	final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

// FIXME this causes ambiguity error because javac can't distinguish between different lambda arities:
//
//	new FluxResume(source, e -> other) tries to match the (Publisher, Publisher) constructor and fails
//
//	public FluxResume(Publisher<? extends T> source,
//			Publisher<? extends T> next) {
//		this(source, create(next));
//	}
//
	static <T> Function<Throwable, Publisher<? extends T>> create(Publisher<? extends T> next) {
		Objects.requireNonNull(next, "next");
		return new Function<Throwable, Publisher<? extends T>>() {
			@Override
			public Publisher<? extends T> apply(Throwable e) {
				return next;
			}
		};
	}
	
	public static <T> FluxResume<T> create(Publisher<? extends T> source, Publisher<? extends T> other) {
		return new FluxResume<>(source, create(other));
	}
	
	public FluxResume(Publisher<? extends T> source,
						   Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory) {
		super(source);
		this.nextFactory = Objects.requireNonNull(nextFactory, "nextFactory");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new FluxResumeSubscriber<>(s, nextFactory));
	}

	static final class FluxResumeSubscriber<T> extends SubscriberMultiSubscription<T, T>
	implements FeedbackLoop {

		final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

		boolean second;

		public FluxResumeSubscriber(Subscriber<? super T> actual,
										 Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory) {
			super(actual);
			this.nextFactory = nextFactory;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!second) {
				subscriber.onSubscribe(this);
			}
			set(s);
		}

		@Override
		public void onNext(T t) {
			subscriber.onNext(t);

			if (!second) {
				producedOne();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (!second) {
				second = true;

				Publisher<? extends T> p;

				try {
					p = nextFactory.apply(t);
				} catch (Throwable e) {
					Throwable _e = Exceptions.unwrap(e);
					_e.addSuppressed(t);
					Exceptions.throwIfFatal(e);
					subscriber.onError(_e);
					return;
				}
				if (p == null) {
					NullPointerException t2 = new NullPointerException("The nextFactory returned a null Publisher");
					t2.addSuppressed(t);
					subscriber.onError(t2);
				} else {
					p.subscribe(this);
				}
			} else {
				subscriber.onError(t);
			}
		}

		@Override
		public Object delegateInput() {
			return nextFactory;
		}

		@Override
		public Object delegateOutput() {
			return null;
		}
	}
}
