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

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import static reactor.util.retry.RetrySpec.*;

/**
 * Functional interface to configure retries depending on a companion {@link Flux} of {@link RetrySignal},
 * as well as builders for such {@link Flux#retryWhen(Retry)} retries} companions.
 *
 * @author Simon Baslé
 */
@FunctionalInterface
public interface Retry {

	/**
	 * The intent of the functional {@link Retry} class is to let users configure how to react to {@link RetrySignal}
	 * by providing the operator with a companion publisher. Any {@link org.reactivestreams.Subscriber#onNext(Object) onNext}
	 * emitted by this publisher will trigger a retry, but if that emission is delayed compared to the original signal then
	 * the attempt is delayed as well. This method generates the companion, out of a {@link Flux} of {@link RetrySignal},
	 * which itself can serve as the simplest form of retry companion (indefinitely and immediately retry on any error).
	 *
	 * @param retrySignalCompanion the original {@link Flux} of {@link RetrySignal}, notifying of each source error that
	 * _might_ result in a retry attempt, with context around the error and current retry cycle.
	 * @return the actual companion to use, which might delay or limit retry attempts
	 */
	Publisher<?> generateCompanion(Flux<RetrySignal> retrySignalCompanion);

	/**
	 * State for a {@link Flux#retryWhen(Retry)} Flux retry} or {@link reactor.core.publisher.Mono#retryWhen(Retry) Mono retry}.
	 * A flux of states is passed to the user, which gives information about the {@link #failure()} that potentially triggers
	 * a retry as well as two indexes: the number of errors that happened so far (and were retried) and the same number,
	 * but only taking into account <strong>subsequent</strong> errors (see {@link #totalRetriesInARow()}).
	 */
	interface RetrySignal {

		/**
		 * The ZERO BASED index number of this error (can also be read as how many retries have occurred
		 * so far), since the source was first subscribed to.
		 *
		 * @return a 0-index for the error, since original subscription
		 */
		long totalRetries();

		/**
		 * The ZERO BASED index number of this error since the beginning of the current burst of errors.
		 * This is reset to zero whenever a retry is made that is followed by at least one
		 * {@link org.reactivestreams.Subscriber#onNext(Object) onNext}.
		 *
		 * @return a 0-index for the error in the current burst of subsequent errors
		 */
		long totalRetriesInARow();

		/**
		 * The current {@link Throwable} that needs to be evaluated for retry.
		 *
		 * @return the current failure {@link Throwable}
		 */
		Throwable failure();

		/**
		 * Return an immutable copy of this {@link RetrySignal} which is guaranteed to give a consistent view
		 * of the state at the time at which this method is invoked.
		 * This is especially useful if this {@link RetrySignal} is a transient view of the state of the underlying
		 * retry subscriber,
		 *
		 * @return an immutable copy of the current {@link RetrySignal}, always safe to use
		 */
		default RetrySignal copy() {
			return new ImmutableRetrySignal(totalRetries(), totalRetriesInARow(), failure());
		}
	}

	/**
	 * A {@link RetryBackoffSpec} preconfigured for exponential backoff strategy with jitter, given a maximum number of retry attempts
	 * and a minimum {@link Duration} for the backoff.
	 *
	 * @param maxAttempts the maximum number of retry attempts to allow
	 * @param minBackoff the minimum {@link Duration} for the first backoff
	 * @return the builder for further configuration
	 * @see RetryBackoffSpec#maxAttempts(long)
	 * @see RetryBackoffSpec#minBackoff(Duration)
	 */
	static RetryBackoffSpec backoff(long maxAttempts, Duration minBackoff) {
		return new RetryBackoffSpec(maxAttempts, t -> true, false, minBackoff, MAX_BACKOFF, 0.5d, Schedulers.parallel(),
				NO_OP_CONSUMER, NO_OP_CONSUMER, NO_OP_BIFUNCTION, NO_OP_BIFUNCTION,
				RetryBackoffSpec.BACKOFF_EXCEPTION_GENERATOR);
	}

	/**
	 * A {@link RetrySpec} preconfigured for a simple strategy with maximum number of retry attempts.
	 *
	 * @param max the maximum number of retry attempts to allow
	 * @return the builder for further configuration
	 * @see RetrySpec#maxAttempts(long)
	 */
	static RetrySpec max(long max) {
		return new RetrySpec(max, t -> true, false, NO_OP_CONSUMER, NO_OP_CONSUMER, NO_OP_BIFUNCTION, NO_OP_BIFUNCTION,
				RetrySpec.RETRY_EXCEPTION_GENERATOR);
	}

	/**
	 * A {@link RetrySpec} preconfigured for a simple strategy with maximum number of retry attempts over
	 * subsequent transient errors. An {@link org.reactivestreams.Subscriber#onNext(Object)} between
	 * errors resets the counter (see {@link RetrySpec#transientErrors(boolean)}).
	 *
	 * @param maxInARow the maximum number of retry attempts to allow in a row, reset by successful onNext
	 * @return the builder for further configuration
	 * @see RetrySpec#maxAttempts(long)
	 * @see RetrySpec#transientErrors(boolean)
	 */
	static RetrySpec maxInARow(long maxInARow) {
		return new RetrySpec(maxInARow, t -> true, true, NO_OP_CONSUMER, NO_OP_CONSUMER, NO_OP_BIFUNCTION, NO_OP_BIFUNCTION,
				RETRY_EXCEPTION_GENERATOR);
	}

}
