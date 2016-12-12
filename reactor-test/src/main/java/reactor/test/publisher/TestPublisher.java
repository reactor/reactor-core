package reactor.test.publisher;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A {@link Publisher} that you can directly manipulate, triggering
 * {@link #next(Object) onNext}, {@link #complete() onComplete} and
 * {@link #error(Throwable) onError} events, for testing purposes.
 * You can assert the state of the publisher using its {@code assertXXX} methods,
 * usually inside a {@link reactor.test.StepVerifier}'s
 * {@link reactor.test.StepVerifier.Step#then(Runnable) then} callback.
 * <p>
 * The TestPublisher can also be made more lenient towards the RS spec
 * and allow "spec violations", as enumerated in {@link Violation}. Use the
 * {@link #createNoncompliant(Violation, Violation...)} factory method to create such
 * a misbehaving publisher.
 *
 * @author Simon Baslé
 */
public abstract class TestPublisher<T> implements Publisher<T> {

	/**
	 * Create a standard {@link TestPublisher}.
	 *
	 * @param <T> the type of the publisher
	 * @return the new {@link TestPublisher}
	 */
	public static <T> TestPublisher<T> create() {
		return new DefaultTestPublisher<>();
	}

	/**
	 * Create a {@link Violation noncompliant} {@link TestPublisher}
	 * with a given set of reactive streams spec violations that will be overlooked.
	 *
	 * @param first the first allowed {@link Violation}
	 * @param rest additional optional violations
	 * @param <T> the type of the publisher
	 * @return the new noncompliant {@link TestPublisher}
	 */
	public static <T> TestPublisher<T> createNoncompliant(Violation first, Violation... rest) {
		return new DefaultTestPublisher<>(first, rest);
	}

	/**
	 * Convenience method to wrap this {@link TestPublisher} to a {@link Flux}.
	 */
	public abstract Flux<T> flux();

	/**
	 * Convenience method to wrap this {@link TestPublisher} to a {@link Mono}.
	 */
	public abstract Mono<T> mono();

	/**
	 * Assert that the current minimum request of all this publisher's subscribers
	 * is &gt;= {@code n}.
	 *
	 * @param n the expected minimum request
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertMinRequested(long n);

	/**
	 * Asserts that this publisher has subscribers.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertSubscribers();

	/**
	 * Asserts that this publisher has exactly n subscribers.
	 *
	 * @param n the expected number of subscribers
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertSubscribers(int n);

	/**
	 * Asserts that this publisher has no subscribers.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertNoSubscribers();

	/**
	 * Asserts that this publisher has had at least one subscriber that has been cancelled.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertCancelled();

	/**
	 * Asserts that this publisher has had at least n subscribers that have been cancelled.
	 *
	 * @param n the expected number of subscribers to have been cancelled.
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertCancelled(int n);

	/**
	 * Asserts that this publisher has had no cancelled subscribers.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertNotCancelled();

	/**
	 * Asserts that this publisher has had subscriber that saw request overflow,
	 * that is received an onNext event despite having a requested amount of 0 at
	 * the time.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertRequestOverflow();

	/**
	 * Asserts that this publisher has had no subscriber with request overflow.
	 * Request overflow is receiving an onNext event despite having a requested amount
	 * of 0 at that time.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> assertNoRequestOverflow();

	/**
	 * Send 1 {@link Subscriber#onNext(Object) onNext} signal to the subscribers.
	 *
	 * @param value the item to emit
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> next(T value);

	/**
	 * Triggers an {@link Subscriber#onError(Throwable) error} signal to the subscribers.
	 *
	 * @param t the {@link Throwable} to trigger
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> error(Throwable t);

	/**
	 * Triggers {@link Subscriber#onComplete() completion} of this publisher.
	 *
	 * @return this {@link TestPublisher} for chaining.
	 */
	public abstract TestPublisher<T> complete();

	/**
	 * Send 1-n {@link Subscriber#onNext(Object) onNext} signals to the subscribers.
	 *
	 * @param first the first item to emit
	 * @param rest the optional remaining items to emit
	 * @return this {@link TestPublisher} for chaining.
	 * @see #next(T) next
	 */
	@SafeVarargs
	public final TestPublisher<T> next(T first, T... rest) {
		Objects.requireNonNull(rest, "rest array is null, please cast to T if null T required");
		next(first);
		for (T t : rest) {
			next(t);
		}
		return this;
	}

	/**
	 * Combine emitting items and completing this publisher.
	 *
	 * @param values the values to emit to subscribers
	 * @return this {@link TestPublisher} for chaining.
	 * @see #next(T) next
	 * @see #complete() complete
	 */
	@SafeVarargs
	public final TestPublisher<T> emit(T... values) {
		Objects.requireNonNull(values, "values array is null, please cast to T if null T required");
		for (T t : values) {
			next(t);
		}
		return complete();
	}

	/**
	 * Possible misbehavior for a {@link TestPublisher}.
	 */
	public enum Violation {
		/**
		 * Allow {@link TestPublisher#next(Object, Object[]) next} calls to be made
		 * despite insufficient request, without triggering an {@link IllegalStateException}.
		 */
		REQUEST_OVERFLOW,
		/**
		 * Allow {@link TestPublisher#next(Object, Object[]) next}  calls to be made
		 * with a {@code null} value without triggering a {@link NullPointerException}
		 */
		ALLOW_NULL
	}
}
