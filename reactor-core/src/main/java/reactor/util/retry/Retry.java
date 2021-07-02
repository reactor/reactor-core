/*
 * Copyright (c) 2020-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.retry;

import java.time.Duration;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static reactor.util.retry.RetrySpec.*;

/**
 * Base abstract class for a strategy to decide when to retry given a companion {@link Flux} of {@link RetrySignal},
 * for use with {@link Flux#retryWhen(Retry)} and {@link reactor.core.publisher.Mono#retryWhen(Retry)}.
 * Also provides access to configurable built-in strategies via static factory methods:
 * <ul>
 *     <li>{@link #indefinitely()}</li>
 *     <li>{@link #max(long)}</li>
 *     <li>{@link #maxInARow(long)}</li>
 *     <li>{@link #fixedDelay(long, Duration)}</li>
 *     <li>{@link #backoff(long, Duration)}</li>
 * </ul>
 * <p>
 * Users are encouraged to provide either concrete custom {@link Retry} strategies or builders that produce
 * such concrete {@link Retry}. The {@link RetrySpec} returned by e.g. {@link #max(long)} is a good inspiration
 * for a fluent approach that generates a {@link Retry} at each step and uses immutability/copy-on-write to enable
 * sharing of intermediate steps (that can thus be considered templates).
 *
 * @author Simon Baslé
 */
public abstract class Retry {

	public final ContextView retryContext;

	public Retry() {
		this(Context.empty());
	}

	protected Retry(ContextView retryContext) {
		this.retryContext = retryContext;
	}

	/**
	 * Generates the companion publisher responsible for reacting to incoming {@link RetrySignal} emissions, effectively
	 * deciding when to retry.
	 * <p>
	 * When the source signals an error, that {@link org.reactivestreams.Subscriber#onError(Throwable) onError} signal
	 * will be suppressed. Its {@link Throwable} will instead be attached to a {@link RetrySignal}, immediately emitted
	 * on the {@code retrySignals} publisher. Right after that emission,
	 * {@link org.reactivestreams.Subscription#request(long) request(1)} is called on the companion publisher.
	 * <p>
	 * The response to that request decides if a retry should be made. Thus, the outer publisher will wait until a signal
	 * is emitted by the companion publisher, making it possible to delay retry attempts.
	 * <p>
	 * Any
	 * {@link org.reactivestreams.Subscriber#onNext(Object) onNext} emitted by the companion publisher triggers a retry,
	 * {@link org.reactivestreams.Subscriber#onError(Throwable) onError} will fail the outer publisher and
	 * {@link org.reactivestreams.Subscriber#onComplete() onComplete} will complete the outer publisher (effectively
	 * suppressing the original error/{@link Throwable}).
	 * <p>
	 * As an example, the simplest form of retry companion would be to return the incoming {@link Flux} of {@link RetrySignal}
	 * without modification. This would render a retry strategy that immediately retries, forever.
	 *
	 * @param retrySignals the errors from the outer publisher as {@link RetrySignal} objects,
	 * containing the {@link Throwable} causing the error as well as retry counter metadata.
	 * @return the companion publisher responsible for reacting to incoming {@link RetrySignal} emissions,
	 * effectively deciding when to retry.
	 */
	public abstract Publisher<?> generateCompanion(Flux<RetrySignal> retrySignals);

	/**
	 * Return the user provided context that was set at construction time.
	 *
	 * @return the user provided context that will be accessible via {@link RetrySignal#retryContextView()}.
	 */
	public ContextView retryContext() {
		return retryContext;
	}


	/**
	 * State used in {@link Flux#retryWhen(Retry)} and {@link reactor.core.publisher.Mono#retryWhen(Retry)},
	 * providing the {@link Throwable} that caused the source to fail as well as counters keeping track of retries.
	 */
	public interface RetrySignal {

		/**
		 * The total number of retries since the source first was subscribed to (in other words the number of errors -1
		 * since the source was first subscribed to).
		 *
		 * @return the total number of retries since the source first was subscribed to.
		 */
		long totalRetries();

		/**
		 * Retry counter resetting after each {@link org.reactivestreams.Subscriber#onNext(Object) onNext} (in other
		 * words the number of errors -1 since the latest {@link org.reactivestreams.Subscriber#onNext(Object) onNext}).
		 *
		 * @return the number of retries since the latest {@link org.reactivestreams.Subscriber#onNext(Object) onNext},
		 * or the number of retries since the source first was subscribed to if there hasn't been any
		 * {@link org.reactivestreams.Subscriber#onNext(Object) onNext} signals (in which case
		 * {@link RetrySignal#totalRetries()} and {@link RetrySignal#totalRetriesInARow()} are equivalent).
		 */
		long totalRetriesInARow();

		/**
		 * The {@link Throwable} that caused the current {@link org.reactivestreams.Subscriber#onError(Throwable) onError} signal.
		 *
		 * @return the current failure.
		 */
		Throwable failure();

		/**
		 * Return a read-only view of the user provided context, which may be used to store
		 * objects to be reset/rolled-back or otherwise mutated before or after a retry.
		 *
		 * @return a read-only view of a user provided context.
		 */
		default ContextView retryContextView() {
			return Context.empty();
		}

		/**
		 * An immutable copy of this {@link RetrySignal} which is guaranteed to give a consistent view
		 * of the state at the time at which this method is invoked.
		 * This is especially useful if this {@link RetrySignal} is a transient view of the state of the underlying
		 * retry subscriber.
		 *
		 * @return an immutable copy of the current {@link RetrySignal}, always safe to use
		 */
		default RetrySignal copy() {
			return new ImmutableRetrySignal(totalRetries(), totalRetriesInARow(), failure(), retryContextView());
		}
	}

	/**
	 * A {@link RetryBackoffSpec} preconfigured for exponential backoff strategy with jitter, given a maximum number of retry attempts
	 * and a minimum {@link Duration} for the backoff.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/retrySpecBackoff.svg" alt="">
	 *
	 * @param maxAttempts the maximum number of retry attempts to allow
	 * @param minBackoff the minimum {@link Duration} for the first backoff
	 * @return the exponential backoff spec for further configuration
	 * @see RetryBackoffSpec#maxAttempts(long)
	 * @see RetryBackoffSpec#minBackoff(Duration)
	 */
	public static RetryBackoffSpec backoff(long maxAttempts, Duration minBackoff) {
		return new RetryBackoffSpec(Context.empty(), maxAttempts, t -> true, false, minBackoff, MAX_BACKOFF, 0.5d, Schedulers::parallel,
				NO_OP_CONSUMER, NO_OP_CONSUMER, NO_OP_BIFUNCTION, NO_OP_BIFUNCTION,
				RetryBackoffSpec.BACKOFF_EXCEPTION_GENERATOR);
	}

	/**
	 * A {@link RetryBackoffSpec} preconfigured for fixed delays (min backoff equals max backoff, no jitter), given a maximum number of retry attempts
	 * and the fixed {@link Duration} for the backoff.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/retrySpecFixed.svg" alt="">
	 * <p>
	 * Note that calling {@link RetryBackoffSpec#minBackoff(Duration)} or {@link RetryBackoffSpec#maxBackoff(Duration)} would switch
	 * back to an exponential backoff strategy.
	 *
	 * @param maxAttempts the maximum number of retry attempts to allow
	 * @param fixedDelay the {@link Duration} of the fixed delays
	 * @return the fixed delays spec for further configuration
	 * @see RetryBackoffSpec#maxAttempts(long)
	 * @see RetryBackoffSpec#minBackoff(Duration)
	 * @see RetryBackoffSpec#maxBackoff(Duration)
	 */
	public static RetryBackoffSpec fixedDelay(long maxAttempts, Duration fixedDelay) {
		return new RetryBackoffSpec(Context.empty(), maxAttempts, t -> true, false, fixedDelay, fixedDelay, 0d, Schedulers::parallel,
				NO_OP_CONSUMER, NO_OP_CONSUMER, NO_OP_BIFUNCTION, NO_OP_BIFUNCTION,
				RetryBackoffSpec.BACKOFF_EXCEPTION_GENERATOR);
	}

	/**
	 * A {@link RetrySpec} preconfigured for a simple strategy with maximum number of retry attempts.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/retrySpecAttempts.svg" alt="">
	 *
	 * @param max the maximum number of retry attempts to allow
	 * @return the max attempt spec for further configuration
	 * @see RetrySpec#maxAttempts(long)
	 */
	public static RetrySpec max(long max) {
		return new RetrySpec(Context.empty(), max, t -> true, false, NO_OP_CONSUMER, NO_OP_CONSUMER, NO_OP_BIFUNCTION, NO_OP_BIFUNCTION,
				RetrySpec.RETRY_EXCEPTION_GENERATOR);
	}

	/**
	 * A {@link RetrySpec} preconfigured for a simple strategy with maximum number of retry attempts over
	 * subsequent transient errors. An {@link org.reactivestreams.Subscriber#onNext(Object)} between
	 * errors resets the counter (see {@link RetrySpec#transientErrors(boolean)}).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/retrySpecInARow.svg" alt="">
	 *
	 * @param maxInARow the maximum number of retry attempts to allow in a row, reset by successful onNext
	 * @return the max in a row spec for further configuration
	 * @see RetrySpec#maxAttempts(long)
	 * @see RetrySpec#transientErrors(boolean)
	 */
	public static RetrySpec maxInARow(long maxInARow) {
		return new RetrySpec(Context.empty(), maxInARow, t -> true, true, NO_OP_CONSUMER, NO_OP_CONSUMER, NO_OP_BIFUNCTION, NO_OP_BIFUNCTION,
				RETRY_EXCEPTION_GENERATOR);
	}

	/**
	 * A {@link RetrySpec} preconfigured for the most simplistic retry strategy: retry immediately and indefinitely
	 * (similar to {@link Flux#retry()}).
	 *
	 * @return the retry indefinitely spec for further configuration
	 */
	public static RetrySpec indefinitely() {
		return new RetrySpec(Context.empty(), Long.MAX_VALUE, t -> true, false, NO_OP_CONSUMER, NO_OP_CONSUMER, NO_OP_BIFUNCTION, NO_OP_BIFUNCTION,
				RetrySpec.RETRY_EXCEPTION_GENERATOR);
	}

	/**
	 * A wrapper around {@link Function} to provide {@link Retry} by using lambda expressions.
	 *
	 * @param function the {@link Function} representing the desired {@link Retry} strategy as a lambda
	 * @return the {@link Retry} strategy adapted from the {@link Function}
	 */
	public static final Retry from(Function<Flux<RetrySignal>, ? extends Publisher<?>> function) {
		return new Retry(Context.empty()) {
			@Override
			public Publisher<?> generateCompanion(Flux<RetrySignal> retrySignalCompanion) {
				return function.apply(retrySignalCompanion);
			}
		};
	}

	/**
	 * An adapter for {@link Flux} of {@link Throwable}-based {@link Function} to provide {@link Retry}
	 * from a legacy retryWhen {@link Function}.
	 *
	 * @param function the {@link Function} representing the desired {@link Retry} strategy as a lambda
	 * @return the {@link Retry} strategy adapted from the {@link Function}
	 */
	public static final Retry withThrowable(Function<Flux<Throwable>, ? extends Publisher<?>> function) {
		return new Retry(Context.empty()) {
			@Override
			public Publisher<?> generateCompanion(Flux<RetrySignal> retrySignals) {
				return function.apply(retrySignals.map(RetrySignal::failure));
			}
		};
	}

}
