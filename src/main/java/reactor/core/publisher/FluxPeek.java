package reactor.core.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.fn.Consumer;
import reactor.fn.LongConsumer;

/**
 * Peek into the lifecycle events and signals of a sequence.
 * <p>
 * <p>
 * The callbacks are all optional.
 * <p>
 * <p>
 * Crashes by the lambdas are ignored.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxPeek<T> extends FluxSource<T, T> {

	final Consumer<? super Subscription> onSubscribeCall;

	final Consumer<? super T> onNextCall;

	final Consumer<? super Throwable> onErrorCall;

	final Runnable onCompleteCall;

	final Runnable onAfterTerminateCall;

	final LongConsumer onRequestCall;

	final Runnable onCancelCall;

	public FluxPeek(Publisher<? extends T> source, Consumer<? super Subscription> onSubscribeCall,
						 Consumer<? super T> onNextCall, Consumer<? super Throwable> onErrorCall, Runnable
						   onCompleteCall,
						 Runnable onAfterTerminateCall, LongConsumer onRequestCall, Runnable onCancelCall) {
		super(source);
		this.onSubscribeCall = onSubscribeCall;
		this.onNextCall = onNextCall;
		this.onErrorCall = onErrorCall;
		this.onCompleteCall = onCompleteCall;
		this.onAfterTerminateCall = onAfterTerminateCall;
		this.onRequestCall = onRequestCall;
		this.onCancelCall = onCancelCall;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new PeekSubscriber<>(s, this));
	}

	static final class PeekSubscriber<T> implements Subscriber<T>, Subscription, Receiver, Producer {

		final Subscriber<? super T> actual;

		final FluxPeek<T> parent;

		Subscription s;

		public PeekSubscriber(Subscriber<? super T> actual, FluxPeek<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if(parent.onRequestCall != null) {
				try {
					parent.onRequestCall.accept(n);
				}
				catch (Throwable e) {
					cancel();
					onError(Exceptions.unwrap(e));
					return;
				}
			}
			s.request(n);
		}

		@Override
		public void cancel() {
			if(parent.onCancelCall != null) {
				try {
					parent.onCancelCall.run();
				}
				catch (Throwable e) {
					Exceptions.throwIfFatal(e);
					s.cancel();
					onError(Exceptions.unwrap(e));
					return;
				}
			}
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if(parent.onSubscribeCall != null) {
				try {
					parent.onSubscribeCall.accept(s);
				}
				catch (Throwable e) {
					onError(e);
					EmptySubscription.error(actual, Exceptions.unwrap(e));
					return;
				}
			}
			this.s = s;
			actual.onSubscribe(this);
		}

		@Override
		public void onNext(T t) {
			if(parent.onNextCall != null) {
				try {
					parent.onNextCall.accept(t);
				}
				catch (Throwable e) {
					cancel();
					Exceptions.throwIfFatal(e);
					onError(Exceptions.unwrap(e));
					return;
				}
			}
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if(parent.onErrorCall != null) {
				Exceptions.throwIfFatal(t);
				parent.onErrorCall.accept(t);
			}

			actual.onError(t);

			if(parent.onAfterTerminateCall != null) {
				try {
					parent.onAfterTerminateCall.run();
				}
				catch (Throwable e) {
					Exceptions.throwIfFatal(e);
					Throwable _e = Exceptions.unwrap(e);
					e.addSuppressed(Exceptions.unwrap(t));
					if(parent.onErrorCall != null) {
						parent.onErrorCall.accept(_e);
					}
					actual.onError(_e);
				}
			}
		}

		@Override
		public void onComplete() {
			if(parent.onCompleteCall != null) {
				try {
					parent.onCompleteCall.run();
				}
				catch (Throwable e) {
					Exceptions.throwIfFatal(e);
					onError(Exceptions.unwrap(e));
					return;
				}
			}

			actual.onComplete();

			if(parent.onAfterTerminateCall != null) {
				try {
					parent.onAfterTerminateCall.run();
				}
				catch (Throwable e) {
					Exceptions.throwIfFatal(e);
					Throwable _e = Exceptions.unwrap(e);
					if(parent.onErrorCall != null) {
						parent.onErrorCall.accept(_e);
					}
					actual.onError(_e);
				}
			}
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return s;
		}
	}
}
