package reactor.core.publisher;

import org.reactivestreams.*;

import reactor.core.util.BackpressureUtils;

/**
 * Detaches the both the child Subscriber and the Subscription on
 * termination or cancellation.
 * <p>This should help with odd retention scenarios when running
 * wit non Rx mentality based Publishers.
 * 
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxDetach<T> extends FluxSource<T, T> {

	public FluxDetach(Publisher<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new DetachSubscriber<>(s));
	}
	
	static final class DetachSubscriber<T> implements Subscriber<T>, Subscription {
		
		Subscriber<? super T> actual;
		
		Subscription s;

		public DetachSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				
				actual.onSubscribe(this);
			}
		}
		
		@Override
		public void onNext(T t) {
			Subscriber<? super T> a = actual;
			if (a != null) {
				a.onNext(t);
			}
		}
		
		@Override
		public void onError(Throwable t) {
			Subscriber<? super T> a = actual;
			if (a != null) {
				actual = null;
				s = null;
				
				a.onError(t);
			}
		}
		
		@Override
		public void onComplete() {
			Subscriber<? super T> a = actual;
			if (a != null) {
				actual = null;
				s = null;
				
				a.onComplete();
			}
		}
		
		@Override
		public void request(long n) {
			Subscription a = s;
			if (a != null) {
				a.request(n);
			}
		}
		
		@Override
		public void cancel() {
			Subscription a = s;
			if (a != null) {
				actual = null;
				s = null;
				
				a.cancel();
			}
		}
	}
}
