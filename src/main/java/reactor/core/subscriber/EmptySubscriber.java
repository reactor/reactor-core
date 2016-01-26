package reactor.core.subscriber;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public enum EmptySubscriber implements Subscriber<Object> {
	INSTANCE;

	@SuppressWarnings("unchecked")
	public static <T> Subscriber<T> instance() {
		return (Subscriber<T>) INSTANCE;
	}

	@Override
	public void onSubscribe(Subscription s) {
		// deliberately no op
	}

	@Override
	public void onNext(Object t) {
		// deliberately no op
	}

	@Override
	public void onError(Throwable t) {
		// deliberately no op
	}

	@Override
	public void onComplete() {
		// deliberately no op
	}

}
