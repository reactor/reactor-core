package reactor.core.util;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.trait.Introspectable;

/**
 * A singleton enumeration that represents a no-op Subscription instance that can be freely given out to clients.
 */
public enum EmptySubscription implements Subscription, Introspectable {
	INSTANCE;

	@Override
	public void request(long n) {
		// deliberately no op
	}

	@Override
	public void cancel() {
		// deliberately no op
	}

	/**
	 * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onError with the
	 * supplied error.
	 *
	 * @param s
	 * @param e
	 */
	public static void error(Subscriber<?> s, Throwable e) {
		s.onSubscribe(INSTANCE);
		s.onError(e);
	}

	/**
	 * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onComplete.
	 *
	 * @param s
	 */
	public static void complete(Subscriber<?> s) {
		s.onSubscribe(INSTANCE);
		s.onComplete();
	}

	@Override
	public int getMode() {
		return TRACE_ONLY;
	}

	@Override
	public String getName() {
		return INSTANCE.name();
	}

}
