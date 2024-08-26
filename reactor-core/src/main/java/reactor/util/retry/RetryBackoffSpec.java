/*
 * Copyright (c) 2020-2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * A {@link Retry} strategy based on exponential backoffs, with configurable features. Use {@link Retry#backoff(long, Duration)}
 * to obtain a preconfigured instance to start with.
 * <p>
 * Retry delays are randomized with a user-provided {@link #jitter(double)} factor between {@code 0.d} (no jitter)
 * and {@code 1.0} (default is {@code 0.5}).
 * Even with the jitter, the effective backoff delay cannot be less than {@link #minBackoff(Duration)}
 * nor more than {@link #maxBackoff(Duration)}. The delays and subsequent attempts are executed on the
 * provided backoff {@link #scheduler(Scheduler)}. Alternatively, {@link Retry#fixedDelay(long, Duration)} provides
 * a strategy where the min and max backoffs are the same and jitters are deactivated.
 * <p>
 * Only errors that match the {@link #filter(Predicate)} are retried (by default all),
 * and the number of attempts can also limited with {@link #maxAttempts(long)}.
 * When the maximum attempt of retries is reached, a runtime exception is propagated downstream which
 * can be pinpointed with {@link reactor.core.Exceptions#isRetryExhausted(Throwable)}. The cause of
 * the last attempt's failure is attached as said {@link reactor.core.Exceptions#retryExhausted(String, Throwable) retryExhausted}
 * exception's cause. This can be customized with {@link #onRetryExhaustedThrow(BiFunction)}.
 * <p>
 * Additionally, to help dealing with bursts of transient errors in a long-lived Flux as if each burst
 * had its own backoff, one can choose to set {@link #transientErrors(boolean)} to {@code true}.
 * The comparison to {@link #maxAttempts(long)} will then be done with the number of subsequent attempts
 * that failed without an {@link org.reactivestreams.Subscriber#onNext(Object) onNext} in between.
 * <p>
 * The {@link RetryBackoffSpec} is copy-on-write and as such can be stored as a "template" and further configured
 * by different components without a risk of modifying the original configuration.
 *
 * @author Simon Baslé
 */
public final class RetryBackoffSpec extends Retry {

	static final BiFunction<RetryBackoffSpec, RetrySignal, Throwable> BACKOFF_EXCEPTION_GENERATOR = (builder, rs) ->
			Exceptions.retryExhausted("Retries exhausted: " + (
					builder.isTransientErrors
					? rs.totalRetriesInARow() + "/" + builder.maxAttempts + " in a row (" + rs.totalRetries() + " total)"
					: rs.totalRetries() + "/" + builder.maxAttempts
					), rs.failure());

	/**
	 * The configured minimum backoff {@link Duration}.
	 * @see #minBackoff(Duration)
	 */
	public final Duration  minBackoff;

	/**
	 * The configured maximum backoff {@link Duration}.
	 * @see #maxBackoff(Duration)
	 */
	public final Duration  maxBackoff;

	/**
	 * The configured multiplier, as a {@code double}.
	 * @see #multiplier(double)
	 */
	public final double    multiplier;

	/**
	 * The configured jitter factor, as a {@code double}.
	 * @see #jitter(double)
	 */
	public final double    jitterFactor;

	/**
	 * The configured {@link Supplier} of {@link Scheduler} on which to execute backoffs.
	 * @see #scheduler(Scheduler)
	 */
	public final Supplier<Scheduler> backoffSchedulerSupplier;

	/**
	 * The configured maximum for retry attempts.
	 *
	 * @see #maxAttempts(long)
	 */
	public final long maxAttempts;

	/**
	 * The configured {@link Predicate} to filter which exceptions to retry.
	 * @see #filter(Predicate)
	 * @see #modifyErrorFilter(Function)
	 */
	public final Predicate<Throwable> errorFilter;

	/**
	 * The configured transient error handling flag.
	 * @see #transientErrors(boolean)
	 */
	public final boolean isTransientErrors;

	final Consumer<RetrySignal>                           syncPreRetry;
	final Consumer<RetrySignal>                           syncPostRetry;
	final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry;
	final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry;

	final BiFunction<RetryBackoffSpec, RetrySignal, Throwable> retryExhaustedGenerator;

	/**
	 * Copy constructor.
	 */
	RetryBackoffSpec(
			ContextView retryContext,
			long max,
			Predicate<? super Throwable> aThrowablePredicate,
			boolean isTransientErrors,
			Duration minBackoff, Duration maxBackoff,
			double multiplier, double jitterFactor,
			Supplier<Scheduler> backoffSchedulerSupplier,
			Consumer<RetrySignal> doPreRetry,
			Consumer<RetrySignal> doPostRetry,
			BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry,
			BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry,
			BiFunction<RetryBackoffSpec, RetrySignal, Throwable> retryExhaustedGenerator) {
		super(retryContext);
		this.maxAttempts = max;
		this.errorFilter = aThrowablePredicate::test; //massaging type
		this.isTransientErrors = isTransientErrors;
		this.minBackoff = minBackoff;
		this.maxBackoff = maxBackoff;
		this.multiplier = multiplier > 1.0 ? multiplier : 1;
		this.jitterFactor = jitterFactor;
		this.backoffSchedulerSupplier = backoffSchedulerSupplier;
		this.syncPreRetry = doPreRetry;
		this.syncPostRetry = doPostRetry;
		this.asyncPreRetry = asyncPreRetry;
		this.asyncPostRetry = asyncPostRetry;
		this.retryExhaustedGenerator = retryExhaustedGenerator;
	}

	/**
	 * Set the user provided {@link Retry#retryContext() context} that can be used to manipulate state on retries.
	 *
	 * @param retryContext a new snapshot of user provided data
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec withRetryContext(ContextView retryContext) {
		return new RetryBackoffSpec(
				retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Set the maximum number of retry attempts allowed. 1 meaning "1 retry attempt":
	 * the original subscription plus an extra re-subscription in case of an error, but
	 * no more.
	 *
	 * @param maxAttempts the new retry attempt limit
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec maxAttempts(long maxAttempts) {
		return new RetryBackoffSpec(
				this.retryContext,
				maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Set the {@link Predicate} that will filter which errors can be retried. Exceptions
	 * that don't pass the predicate will be propagated downstream and terminate the retry
	 * sequence. Defaults to allowing retries for all exceptions.
	 *
	 * @param errorFilter the predicate to filter which exceptions can be retried
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec filter(Predicate<? super Throwable> errorFilter) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				Objects.requireNonNull(errorFilter, "errorFilter"),
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Allows to augment a previously {@link #filter(Predicate) set} {@link Predicate} with
	 * a new condition to allow retries of some exception or not. This can typically be used with
	 * {@link Predicate#and(Predicate)} to combine existing predicate(s) with a new one.
	 * <p>
	 * For example:
	 * <pre><code>
	 * //given
	 * RetrySpec retryTwiceIllegalArgument = Retry.max(2)
	 *     .filter(e -> e instanceof IllegalArgumentException);
	 *
	 * RetrySpec retryTwiceIllegalArgWithCause = retryTwiceIllegalArgument.modifyErrorFilter(old ->
	 *     old.and(e -> e.getCause() != null));
	 * </code></pre>
	 *
	 * @param predicateAdjuster a {@link Function} that returns a new {@link Predicate} given the
	 * currently in place {@link Predicate} (usually deriving from the old predicate).
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec modifyErrorFilter(
			Function<Predicate<Throwable>, Predicate<? super Throwable>> predicateAdjuster) {
		Objects.requireNonNull(predicateAdjuster, "predicateAdjuster");
		Predicate<? super Throwable> newPredicate = Objects.requireNonNull(predicateAdjuster.apply(this.errorFilter),
				"predicateAdjuster must return a new predicate");
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				newPredicate,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Add synchronous behavior to be executed <strong>before</strong> the retry trigger is emitted in
	 * the companion publisher. This should not be blocking, as the companion publisher
	 * might be executing in a shared thread.
	 *
	 * @param doBeforeRetry the synchronous hook to execute before retry trigger is emitted
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 * @see #doBeforeRetryAsync(Function) andDelayRetryWith for an asynchronous version
	 */
	public RetryBackoffSpec doBeforeRetry(
			Consumer<RetrySignal> doBeforeRetry) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry.andThen(doBeforeRetry),
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Add synchronous behavior to be executed <strong>after</strong> the retry trigger is emitted in
	 * the companion publisher. This should not be blocking, as the companion publisher
	 * might be publishing events in a shared thread.
	 *
	 * @param doAfterRetry the synchronous hook to execute after retry trigger is started
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 * @see #doAfterRetryAsync(Function) andRetryThen for an asynchronous version
	 */
	public RetryBackoffSpec doAfterRetry(Consumer<RetrySignal> doAfterRetry) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry.andThen(doAfterRetry),
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Add asynchronous behavior to be executed <strong>before</strong> the current retry trigger in the companion publisher,
	 * thus <strong>delaying</strong> the resulting retry trigger with the additional {@link Mono}.
	 *
	 * @param doAsyncBeforeRetry the asynchronous hook to execute before original retry trigger is emitted
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec doBeforeRetryAsync(
			Function<RetrySignal, Mono<Void>> doAsyncBeforeRetry) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				(rs, m) -> asyncPreRetry.apply(rs, m).then(doAsyncBeforeRetry.apply(rs)),
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Add asynchronous behavior to be executed <strong>after</strong> the current retry trigger in the companion publisher,
	 * thus <strong>delaying</strong> the resulting retry trigger with the additional {@link Mono}.
	 *
	 * @param doAsyncAfterRetry the asynchronous hook to execute after original retry trigger is emitted
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec doAfterRetryAsync(
			Function<RetrySignal, Mono<Void>> doAsyncAfterRetry) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				(rs, m) -> asyncPostRetry.apply(rs, m).then(doAsyncAfterRetry.apply(rs)),
				this.retryExhaustedGenerator);
	}

	/**
	 * Set the generator for the {@link Exception} to be propagated when the maximum amount of retries
	 * is exhausted. By default, throws an {@link Exceptions#retryExhausted(String, Throwable)} with the
	 * message reflecting the total attempt index, transient attempt index and maximum retry count.
	 * The cause of the last {@link reactor.util.retry.Retry.RetrySignal} is also added
	 * as the exception's cause.
	 *
	 *
	 * @param retryExhaustedGenerator the {@link Function} that generates the {@link Throwable} for the last
	 * {@link reactor.util.retry.Retry.RetrySignal}
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further
	 * configured or used as {@link reactor.util.retry.Retry}
	 */
	public RetryBackoffSpec onRetryExhaustedThrow(BiFunction<RetryBackoffSpec, RetrySignal, Throwable> retryExhaustedGenerator) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				Objects.requireNonNull(retryExhaustedGenerator, "retryExhaustedGenerator"));
	}

	/**
	 * Set the transient error mode, indicating that the strategy being built should use
	 * {@link reactor.util.retry.Retry.RetrySignal#totalRetriesInARow()} rather than
	 * {@link reactor.util.retry.Retry.RetrySignal#totalRetries()}.
	 * Transient errors are errors that could occur in bursts but are then recovered from by
	 * a retry (with one or more onNext signals) before another error occurs.
	 * <p>
	 * For a backoff based retry, the backoff is also computed based on the index within
	 * the burst, meaning the next error after a recovery will be retried with a {@link #minBackoff(Duration)} delay.
	 *
	 * @param isTransientErrors {@code true} to activate transient mode
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec transientErrors(boolean isTransientErrors) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Set the minimum {@link Duration} for the first backoff. This method switches to an
	 * exponential backoff strategy if not already done so. Defaults to {@link Duration#ZERO}
	 * when the strategy was initially not a backoff one.
	 *
	 * @param minBackoff the minimum backoff {@link Duration}
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec minBackoff(Duration minBackoff) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Set a hard maximum {@link Duration} for exponential backoffs. This method switches
	 * to an exponential backoff strategy with a zero minimum backoff if not already a backoff
	 * strategy. Defaults to {@code Duration.ofMillis(Long.MAX_VALUE)}.
	 *
	 * @param maxBackoff the maximum backoff {@link Duration}
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec maxBackoff(Duration maxBackoff) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.minBackoff,
				maxBackoff,
				this.multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Set a jitter factor for exponential backoffs that adds randomness to each backoff. This can
	 * be helpful in reducing cascading failure due to retry-storms. This method switches to an
	 * exponential backoff strategy with a zero minimum backoff if not already a backoff strategy.
	 * Defaults to {@code 0.5} (a jitter of at most 50% of the computed delay).
	 *
	 * @param jitterFactor the new jitter factor as a {@code double} between {@code 0d} and {@code 1d}
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec jitter(double jitterFactor) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Set a multiplier for exponential backoffs that is used as the base for each backoff. This method switches to an
	 * exponential backoff strategy with a zero minimum backoff if not already a backoff strategy.
	 * Defaults to {@code 2}.
	 *
	 * @param multiplier the new multiplier as a {@code double}
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec multiplier(double multiplier) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				multiplier,
				this.jitterFactor,
				this.backoffSchedulerSupplier,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Set a {@link Scheduler} on which to execute the delays computed by the exponential backoff
	 * strategy. Defaults to a deferred resolution of the current {@link Schedulers#parallel()} (use
	 * {@code null} to reset to this default).
	 *
	 * @param backoffScheduler the {@link Scheduler} to use, or {@code null} to revert to the default
	 * @return a new copy of the {@link RetryBackoffSpec} which can either be further configured or used as {@link Retry}
	 */
	public RetryBackoffSpec scheduler(@Nullable Scheduler backoffScheduler) {
		return new RetryBackoffSpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.minBackoff,
				this.maxBackoff,
				this.multiplier,
				this.jitterFactor,
				backoffScheduler == null ? Schedulers::parallel : () -> backoffScheduler,
				this.syncPreRetry,
				this.syncPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	//==========
	// strategy
	//==========

	protected void validateArguments() {
		if (jitterFactor < 0 || jitterFactor > 1) throw new IllegalArgumentException("jitterFactor must be between 0 and 1 (default 0.5)");
	}

	@Override
	public Flux<Long> generateCompanion(Flux<RetrySignal> t) {
		validateArguments();
		return Flux.deferContextual(cv ->
		   t.contextWrite(cv)
			.concatMap(retryWhenState -> {
				//capture the state immediately
				RetrySignal copy = retryWhenState.copy();
				Throwable currentFailure = copy.failure();
				long iteration = isTransientErrors ? copy.totalRetriesInARow() : copy.totalRetries();

				if (currentFailure == null) {
					return Mono.error(new IllegalStateException("Retry.RetrySignal#failure() not expected to be null"));
				}

				if (!errorFilter.test(currentFailure)) {
					return Mono.error(currentFailure);
				}

				if (iteration >= maxAttempts) {
					return Mono.error(retryExhaustedGenerator.apply(this, copy));
				}

				Duration nextBackoff;
				try {
					nextBackoff = minBackoff.multipliedBy((long) Math.pow(multiplier, iteration));
					if (nextBackoff.compareTo(maxBackoff) > 0) {
						nextBackoff = maxBackoff;
					}
				}
				catch (ArithmeticException overflow) {
					nextBackoff = maxBackoff;
				}

				//short-circuit delay == 0 case
				if (nextBackoff.isZero()) {
					return RetrySpec.applyHooks(copy, Mono.just(iteration),
							syncPreRetry, syncPostRetry, asyncPreRetry, asyncPostRetry, cv);
				}

				ThreadLocalRandom random = ThreadLocalRandom.current();

				long jitterOffset;
				try {
					jitterOffset = nextBackoff.multipliedBy((long) (100 * jitterFactor))
							.dividedBy(100)
							.toMillis();
				}
				catch (ArithmeticException ae) {
					jitterOffset = Math.round(Long.MAX_VALUE * jitterFactor);
				}
				long lowBound = Math.max(minBackoff.minus(nextBackoff)
						.toMillis(), -jitterOffset);
				long highBound = Math.min(maxBackoff.minus(nextBackoff)
						.toMillis(), jitterOffset);

				long jitter;
				if (highBound == lowBound) {
					if (highBound == 0) jitter = 0;
					else jitter = random.nextLong(highBound);
				}
				else {
					jitter = random.nextLong(lowBound, highBound);
				}
				Duration effectiveBackoff = nextBackoff.plusMillis(jitter);
				return RetrySpec.applyHooks(copy, Mono.delay(effectiveBackoff, backoffSchedulerSupplier.get()),
						syncPreRetry, syncPostRetry, asyncPreRetry, asyncPostRetry, cv);
			})
		    .onErrorStop()
		);
	}
}
