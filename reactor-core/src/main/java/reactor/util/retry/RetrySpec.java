/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

/**
 * A simple count-based {@link Retry} strategy with configurable features. Use {@link Retry#max(long)},
 * {@link Retry#maxInARow(long)} or {@link Retry#indefinitely()} to obtain a preconfigured instance to start with.
 * <p>
 * Only errors that match the {@link #filter(Predicate)} are retried (by default all), up to {@link #maxAttempts(long)} times.
 * <p>
 * When the maximum attempt of retries is reached, a runtime exception is propagated downstream which
 * can be pinpointed with {@link reactor.core.Exceptions#isRetryExhausted(Throwable)}. The cause of
 * the last attempt's failure is attached as said {@link reactor.core.Exceptions#retryExhausted(String, Throwable) retryExhausted}
 * exception's cause. This can be customized with {@link #onRetryExhaustedThrow(BiFunction)}.
 * <p>
 * Additionally, to help dealing with bursts of transient errors in a long-lived Flux as if each burst
 * had its own attempt counter, one can choose to set {@link #transientErrors(boolean)} to {@code true}.
 * The comparison to {@link #maxAttempts(long)} will then be done with the number of subsequent attempts
 * that failed without an {@link org.reactivestreams.Subscriber#onNext(Object) onNext} in between.
 * <p>
 * The {@link RetrySpec} is copy-on-write and as such can be stored as a "template" and further configured
 * by different components without a risk of modifying the original configuration.
 *
 * @author Simon Baslé
 */
public final class RetrySpec extends Retry {

	static final Duration                                        MAX_BACKOFF                 = Duration.ofMillis(Long.MAX_VALUE);
	static final Consumer<RetrySignal>                           NO_OP_CONSUMER              = rs -> {};
	static final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> NO_OP_BIFUNCTION            = (rs, m) -> m;


	static final BiFunction<RetrySpec, RetrySignal, Throwable>
			RETRY_EXCEPTION_GENERATOR = (builder, rs) ->
			Exceptions.retryExhausted("Retries exhausted: " + (
					builder.isTransientErrors
							? rs.totalRetriesInARow() + "/" + builder.maxAttempts + " in a row (" + rs.totalRetries() + " total)"
							: rs.totalRetries() + "/" + builder.maxAttempts
			), rs.failure());

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

	final Consumer<RetrySignal>                           doPreRetry;
	final Consumer<RetrySignal>                           doPostRetry;
	final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry;
	final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry;

	final BiFunction<RetrySpec, RetrySignal, Throwable> retryExhaustedGenerator;

	/**
	 * Copy constructor.
	 */
	RetrySpec(ContextView retryContext,
			long max,
			Predicate<? super Throwable> aThrowablePredicate,
			boolean isTransientErrors,
			Consumer<RetrySignal> doPreRetry,
			Consumer<RetrySignal> doPostRetry,
			BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry,
			BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry,
			BiFunction<RetrySpec, RetrySignal, Throwable> retryExhaustedGenerator) {
		super(retryContext);
		this.maxAttempts = max;
		this.errorFilter = aThrowablePredicate::test; //massaging type
		this.isTransientErrors = isTransientErrors;
		this.doPreRetry = doPreRetry;
		this.doPostRetry = doPostRetry;
		this.asyncPreRetry = asyncPreRetry;
		this.asyncPostRetry = asyncPostRetry;
		this.retryExhaustedGenerator = retryExhaustedGenerator;
	}

	/**
	 * Set the user provided {@link Retry#retryContext() context} that can be used to manipulate state on retries.
	 *
	 * @param retryContext a new snapshot of user provided data
	 * @return a new copy of the {@link RetrySpec} which can either be further configured or used as {@link Retry}
	 */
	public RetrySpec withRetryContext(ContextView retryContext) {
		return new RetrySpec(
				retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.doPreRetry,
				this.doPostRetry,
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
	 * @return a new copy of the {@link RetrySpec} which can either be further configured or used as a {@link Retry}
	 */
	public RetrySpec maxAttempts(long maxAttempts) {
		return new RetrySpec(
				this.retryContext,
				maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.doPreRetry,
				this.doPostRetry,
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
	 * @return a new copy of the {@link RetrySpec} which can either be further configured or used as {@link Retry}
	 */
	public RetrySpec filter(Predicate<? super Throwable> errorFilter) {
		return new RetrySpec(
				this.retryContext,
				this.maxAttempts,
				Objects.requireNonNull(errorFilter, "errorFilter"),
				this.isTransientErrors,
				this.doPreRetry,
				this.doPostRetry,
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
	 * @return a new copy of the {@link RetrySpec} which can either be further configured or used as {@link Retry}
	 */
	public RetrySpec modifyErrorFilter(
			Function<Predicate<Throwable>, Predicate<? super Throwable>> predicateAdjuster) {
		Objects.requireNonNull(predicateAdjuster, "predicateAdjuster");
		Predicate<? super Throwable> newPredicate = Objects.requireNonNull(predicateAdjuster.apply(this.errorFilter),
				"predicateAdjuster must return a new predicate");
		return new RetrySpec(
				this.retryContext,
				this.maxAttempts,
				newPredicate,
				this.isTransientErrors,
				this.doPreRetry,
				this.doPostRetry,
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
	 * @return a new copy of the {@link RetrySpec} which can either be further configured or used as {@link Retry}
	 * @see #doBeforeRetryAsync(Function) andDelayRetryWith for an asynchronous version
	 */
	public RetrySpec doBeforeRetry(
			Consumer<RetrySignal> doBeforeRetry) {
		return new RetrySpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.doPreRetry.andThen(doBeforeRetry),
				this.doPostRetry,
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
	 * @return a new copy of the {@link RetrySpec} which can either be further configured or used as {@link Retry}
	 * @see #doAfterRetryAsync(Function) andRetryThen for an asynchronous version
	 */
	public RetrySpec doAfterRetry(Consumer<RetrySignal> doAfterRetry) {
		return new RetrySpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.doPreRetry,
				this.doPostRetry.andThen(doAfterRetry),
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Add asynchronous behavior to be executed <strong>before</strong> the current retry trigger in the companion publisher,
	 * thus <strong>delaying</strong> the resulting retry trigger with the additional {@link Mono}.
	 *
	 * @param doAsyncBeforeRetry the asynchronous hook to execute before original retry trigger is emitted
	 * @return a new copy of the {@link RetrySpec} which can either be further configured or used as {@link Retry}
	 */
	public RetrySpec doBeforeRetryAsync(
			Function<RetrySignal, Mono<Void>> doAsyncBeforeRetry) {
		return new RetrySpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.doPreRetry,
				this.doPostRetry,
				(rs, m) -> asyncPreRetry.apply(rs, m).then(doAsyncBeforeRetry.apply(rs)),
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	/**
	 * Add asynchronous behavior to be executed <strong>after</strong> the current retry trigger in the companion publisher,
	 * thus <strong>delaying</strong> the resulting retry trigger with the additional {@link Mono}.
	 *
	 * @param doAsyncAfterRetry the asynchronous hook to execute after original retry trigger is emitted
	 * @return a new copy of the {@link RetrySpec} which can either be further configured or used as {@link Retry}
	 */
	public RetrySpec doAfterRetryAsync(
			Function<RetrySignal, Mono<Void>> doAsyncAfterRetry) {
		return new RetrySpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				(rs, m) -> asyncPostRetry.apply(rs, m).then(doAsyncAfterRetry.apply(rs)),
				this.retryExhaustedGenerator);
	}

	/**
	 * Set the generator for the {@link Exception} to be propagated when the maximum amount of retries
	 * is exhausted. By default, throws an {@link Exceptions#retryExhausted(String, Throwable)} with the
	 * message reflecting the total attempt index, transient attempt index and maximum retry count.
	 * The cause of the last {@link reactor.util.retry.Retry.RetrySignal} is also added as the exception's cause.
	 *
	 * @param retryExhaustedGenerator the {@link Function} that generates the {@link Throwable} for the last
	 * {@link reactor.util.retry.Retry.RetrySignal}
	 * @return a new copy of the {@link RetrySpec} which can either be further configured or used as {@link Retry}
	 */
	public RetrySpec onRetryExhaustedThrow(BiFunction<RetrySpec, RetrySignal, Throwable> retryExhaustedGenerator) {
		return new RetrySpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				this.isTransientErrors,
				this.doPreRetry,
				this.doPostRetry,
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
	 * In the case of a simple count-based retry, this means that the {@link #maxAttempts(long)}
	 * is applied to each burst individually.
	 *
	 * @param isTransientErrors {@code true} to activate transient mode
	 * @return a new copy of the {@link RetrySpec} which can either be further configured or used as {@link Retry}
	 */
	public RetrySpec transientErrors(boolean isTransientErrors) {
		return new RetrySpec(
				this.retryContext,
				this.maxAttempts,
				this.errorFilter,
				isTransientErrors,
				this.doPreRetry,
				this.doPostRetry,
				this.asyncPreRetry,
				this.asyncPostRetry,
				this.retryExhaustedGenerator);
	}

	//==========
	// strategy
	//==========

	@Override
	public Flux<Long> generateCompanion(Flux<RetrySignal> flux) {
		return flux.concatMap(retryWhenState -> {
			//capture the state immediately
			RetrySignal copy = retryWhenState.copy();
			Throwable currentFailure = copy.failure();
			long iteration = isTransientErrors ? copy.totalRetriesInARow() : copy.totalRetries();

			if (currentFailure == null) {
				return Mono.error(new IllegalStateException("RetryWhenState#failure() not expected to be null"));
			}
			else if (!errorFilter.test(currentFailure)) {
				return Mono.error(currentFailure);
			}
			else if (iteration >= maxAttempts) {
				return Mono.error(retryExhaustedGenerator.apply(this, copy));
			}
			else {
				return applyHooks(copy, Mono.just(iteration), doPreRetry, doPostRetry, asyncPreRetry, asyncPostRetry);
			}
		});
	}

	//===================
	// utility functions
	//===================

	static <T> Mono<T> applyHooks(RetrySignal copyOfSignal,
			Mono<T> originalCompanion,
			final Consumer<RetrySignal> doPreRetry,
			final Consumer<RetrySignal> doPostRetry,
			final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPreRetry,
			final BiFunction<RetrySignal, Mono<Void>, Mono<Void>> asyncPostRetry) {
		if (doPreRetry != NO_OP_CONSUMER) {
			try {
				doPreRetry.accept(copyOfSignal);
			}
			catch (Throwable e) {
				return Mono.error(e);
			}
		}

		Mono<Void> postRetrySyncMono;
		if (doPostRetry != NO_OP_CONSUMER) {
			postRetrySyncMono = Mono.fromRunnable(() -> doPostRetry.accept(copyOfSignal));
		}
		else {
			postRetrySyncMono = Mono.empty();
		}

		Mono<Void> preRetryMono = asyncPreRetry == NO_OP_BIFUNCTION ? Mono.empty() : asyncPreRetry.apply(copyOfSignal, Mono.empty());
		Mono<Void> postRetryMono = asyncPostRetry != NO_OP_BIFUNCTION ? asyncPostRetry.apply(copyOfSignal, postRetrySyncMono) : postRetrySyncMono;

		return preRetryMono.then(originalCompanion).flatMap(postRetryMono::thenReturn);
	}
}
