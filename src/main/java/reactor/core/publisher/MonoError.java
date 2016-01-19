package reactor.core.publisher;

import java.util.Objects;

import org.reactivestreams.Subscriber;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.ReactiveState;
import reactor.fn.Supplier;

/**
 * Emits a constant or generated Throwable instance to Subscribers.
 *
 * @param <T> the value type
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
public final class MonoError<T> 
extends reactor.Mono<T>
implements 
												ReactiveState.Factory,
												ReactiveState.FailState {

	final Supplier<? extends Throwable> supplier;

	public MonoError(Throwable error) {
		this(create(error));
	}

	static Supplier<Throwable> create(final Throwable error) {
		Objects.requireNonNull(error);
		return new Supplier<Throwable>() {
			@Override
			public Throwable get() {
				return error;
			}
		};
	}

	public MonoError(Supplier<? extends Throwable> supplier) {
		this.supplier = Objects.requireNonNull(supplier);
	}

	@Override
	public Throwable getError() {
		return supplier.get();
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Throwable e;

		try {
			e = supplier.get();
		} catch (Throwable ex) {
			e = ex;
		}

		if (e == null) {
			e = new NullPointerException("The Throwable returned by the supplier is null");
		}

		EmptySubscription.error(s, e);
	}
}
