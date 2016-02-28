package reactor.core.subscriber;

import java.util.Objects;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Producer;
import reactor.core.util.DeferredSubscription;

/**
 * Arbitrates the requests and cancellation for a Subscription that may be set onSubscribe once only.
 * <p>
 * Note that {@link #request(long)} doesn't validate the amount.
 * 
 * @param <I> the input value type
 * @param <O> the output value type
 */
public class DeferredSubscriptionSubscriber<I, O>
		extends DeferredSubscription
implements Subscriber<I>, Producer {

	protected final Subscriber<? super O> subscriber;

	/**
	 * Constructs a SingleSubscriptionArbiter with zero initial request.
	 * 
	 * @param subscriber the actual subscriber
	 */
	public DeferredSubscriptionSubscriber(Subscriber<? super O> subscriber) {
		this.subscriber = Objects.requireNonNull(subscriber, "subscriber");
	}

	@Override
	public final Subscriber<? super O> downstream() {
		return subscriber;
	}

	@Override
	public void onSubscribe(Subscription s) {
		set(s);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onNext(I t) {
		subscriber.onNext((O) t);
	}

	@Override
	public void onError(Throwable t) {
		subscriber.onError(t);
	}

	@Override
	public void onComplete() {
		subscriber.onComplete();
	}
}
