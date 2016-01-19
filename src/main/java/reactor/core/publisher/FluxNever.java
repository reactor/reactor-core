package reactor.core.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.ReactiveState;

/**
 * Represents an never publisher which only calls onSubscribe.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class FluxNever 
extends reactor.Flux<Object>
implements 
											 ReactiveState.Factory,
											 ReactiveState.ActiveUpstream {

	private static final Publisher<Object> INSTANCE = new FluxNever();

	private FluxNever() {
		// deliberately no op
	}

	@Override
	public boolean isStarted() {
		return true;
	}

	@Override
	public boolean isTerminated() {
		return false;
	}

	@Override
	public void subscribe(Subscriber<? super Object> s) {
		s.onSubscribe(EmptySubscription.INSTANCE);
	}

	/**
	 * Returns a properly parametrized instance of this never Publisher.
	 *
	 * @param <T> the value type
	 * @return a properly parametrized instance of this never Publisher
	 */
	@SuppressWarnings("unchecked")
	public static <T> reactor.Flux<T> instance() {
		return (reactor.Flux<T>) INSTANCE;
	}
}
