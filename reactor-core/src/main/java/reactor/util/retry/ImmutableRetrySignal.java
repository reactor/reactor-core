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

import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * An immutable {@link reactor.util.retry.Retry.RetrySignal} that can be used for retained
 * copies of mutable implementations.
 *
 * @author Simon Baslé
 */
final class ImmutableRetrySignal implements Retry.RetrySignal {

	final long      failureTotalIndex;
	final long      failureSubsequentIndex;
	final Throwable failure;
	final ContextView retryContext;

	ImmutableRetrySignal(long failureTotalIndex, long failureSubsequentIndex,
			Throwable failure) {
		this(failureTotalIndex, failureSubsequentIndex, failure, Context.empty());
	}

	ImmutableRetrySignal(long failureTotalIndex, long failureSubsequentIndex,
			Throwable failure, ContextView retryContext) {
		this.failureTotalIndex = failureTotalIndex;
		this.failureSubsequentIndex = failureSubsequentIndex;
		this.failure = failure;
		this.retryContext = retryContext;
	}

	@Override
	public long totalRetries() {
		return this.failureTotalIndex;
	}

	@Override
	public long totalRetriesInARow() {
		return this.failureSubsequentIndex;
	}

	@Override
	public Throwable failure() {
		return this.failure;
	}

	@Override
	public ContextView retryContextView() {
		return retryContext;
	}

	@Override
	public Retry.RetrySignal copy() {
		return this;
	}

	@Override
	public String toString() {
		return "attempt #" + (failureTotalIndex + 1) + " (" + (failureSubsequentIndex + 1) + " in a row), last failure={" + failure + '}';
	}
}
