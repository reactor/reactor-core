package reactor.core.publisher;

import java.util.Objects;

import org.reactivestreams.Subscriber;
import reactor.core.subscription.ScalarSubscription;
import reactor.core.support.ReactiveState;
import reactor.fn.Supplier;


/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
public final class MonoJust<T> 
extends reactor.Mono<T>
implements Supplier<T>,
											   ReactiveState.Factory,
											   ReactiveState.Upstream {

	final T value;

	public MonoJust(T value) {
		this.value = Objects.requireNonNull(value, "value");
	}

	@Override
	public T get() {
		return value;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		s.onSubscribe(new ScalarSubscription<>(s, value));
	}

	@Override
	public Object upstream() {
		return value;
	}
}
