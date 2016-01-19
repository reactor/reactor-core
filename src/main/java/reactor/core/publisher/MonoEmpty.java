package reactor.core.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.ReactiveState;
import reactor.fn.Supplier;

/**
 * Represents an empty publisher which only calls onSubscribe and onComplete.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
public final class MonoEmpty 
extends reactor.Mono<Object>
implements Supplier<Object>,
											 ReactiveState.Factory,
											 ReactiveState.ActiveUpstream {

	private static final Publisher<Object> INSTANCE = new MonoEmpty();

	private MonoEmpty() {
		// deliberately no op
	}

	@Override
	public boolean isStarted() {
		return false;
	}

	@Override
	public boolean isTerminated() {
		return true;
	}

	@Override
	public void subscribe(Subscriber<? super Object> s) {
		s.onSubscribe(EmptySubscription.INSTANCE);
		s.onComplete();
	}

	/**
	 * Returns a properly parametrized instance of this empty Publisher.
	 *
	 * @param <T> the output type
	 * @return a properly parametrized instance of this empty Publisher
	 */
	@SuppressWarnings("unchecked")
	public static <T> reactor.Mono<T> instance() {
		return (reactor.Mono<T>) INSTANCE;
	}

	@Override
	public Object get() {
		return null; /* Scalar optimizations on empty */
	}
}
