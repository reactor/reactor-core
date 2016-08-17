package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;

/**
 * Intercepts the onSubscribe call and makes sure calls to Subscription methods
 * only happen after the child Subscriber has returned from its onSubscribe method.
 * 
 * <p>This helps with child Subscribers that don't expect a recursive call from
 * onSubscribe into their onNext because, for example, they request immediately from
 * their onSubscribe but don't finish their preparation before that and onNext
 * runs into a half-prepared state. This can happen with non Rx mentality based Subscribers.
 *
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 * @since 3.0
 */
final class FluxAwaitOnSubscribe<T> extends FluxSource<T, T> {

	public FluxAwaitOnSubscribe(Publisher<? extends T> source) {
		super(source);
	}
	
	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new PostOnSubscribeSubscriber<>(s));
	}
	
	static final class PostOnSubscribeSubscriber<T> implements Receiver,
	                                                           Producer,
	                                                           Trackable, Subscriber<T>,
	                                                           Subscription {
		final Subscriber<? super T> actual;
		
		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<PostOnSubscribeSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(PostOnSubscribeSubscriber.class, Subscription.class, "s");
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PostOnSubscribeSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PostOnSubscribeSubscriber.class, "requested");

		public PostOnSubscribeSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				
				actual.onSubscribe(this);
				
				if (Operators.setOnce(S, this, s)) {
					long r = REQUESTED.getAndSet(this, 0L);
					if (r != 0L) {
						s.request(r);
					}
				}
			}
		}
		
		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}
		
		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}
		
		@Override
		public void onComplete() {
			actual.onComplete();
		}
		
		@Override
		public void request(long n) {
			Subscription a = s;
			if (a != null) {
				a.request(n);
			} else {
				if (Operators.validate(n)) {
					Operators.getAndAddCap(REQUESTED, this, n);
					a = s;
					if (a != null) {
						long r = REQUESTED.getAndSet(this, 0L);
						if (r != 0L) {
							a.request(n);
						}
					}
				}
			}
		}
		
		@Override
		public void cancel() {
			Operators.terminate(S, this);
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}
	}
}
