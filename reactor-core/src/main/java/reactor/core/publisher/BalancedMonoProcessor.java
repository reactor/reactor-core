/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.util.annotation.Nullable;

/**
 * A simple {@link Disposable} symmetric {@link Processor} (taking the same type as what
 * it outputs), with {@link Mono} like semantics (0-1 elements).
 *
 * @author Simon Baslé
 */
public interface BalancedMonoProcessor<T> extends Processor<T, T>, Disposable {

	/**
	 * Return the produced {@link Throwable} error if any or null
	 *
	 * @return the produced {@link Throwable} error if any or null
	 */
	@Nullable
	Throwable getError();

	/**
	 * Indicates whether this {@link BalancedMonoProcessor} has been interrupted via cancellation.
	 *
	 * @return {@code true} if this {@link BalancedMonoProcessor} is cancelled, {@code false}
	 * otherwise.
	 */
	boolean isCancelled();

	/**
	 * Indicates whether this {@link BalancedMonoProcessor} has been completed with an error.
	 *
	 * @return {@code true} if this {@link BalancedMonoProcessor} was completed with an error, {@code false} otherwise.
	 */
	boolean isError();

	/**
	 * Indicates whether this {@link BalancedMonoProcessor} has been successfully completed a value.
	 *
	 * @return {@code true} if this {@link BalancedMonoProcessor} is successful, {@code false} otherwise.
	 */
	boolean isSuccess();

	/**
	 * Indicates whether this {@link BalancedMonoProcessor} has been terminated by the
	 * source producer with a success or an error.
	 *
	 * @return {@code true} if this {@link BalancedMonoProcessor} is successful, {@code false} otherwise.
	 */
	boolean isTerminated();

	/**
	 * Return the number of active {@link Subscriber} or {@literal -1} if untracked.
	 *
	 * @return the number of active {@link Subscriber} or {@literal -1} if untracked
	 */
	long downstreamCount();

	/**
	 * Return true if any {@link Subscriber} is actively subscribed
	 *
	 * @return true if any {@link Subscriber} is actively subscribed
	 */
	boolean hasDownstreams();

	/**
	 * Return true if this {@link BalancedMonoProcessor} supports multithread producing
	 *
	 * @return true if this {@link BalancedMonoProcessor} supports multithread producing
	 */
	boolean isSerialized();


	/**
	 * Expose a {@link Mono} API on top of the {@link BalancedMonoProcessor}'s output,
	 * allowing composition of operators on it.
	 *
	 * @implNote most implementations will already implement {@link Mono} and thus can
	 * return themselves.
	 *
	 * @return a {@link Mono} API on top of the {@link Processor}'s output
	 */
	Mono<T> asMono();
}
