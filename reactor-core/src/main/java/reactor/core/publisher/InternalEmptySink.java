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

package reactor.core.publisher;

import reactor.core.publisher.Sinks.EmissionException;

interface InternalEmptySink<T> extends Sinks.Empty<T>, ContextHolder {

	@Override
	default void emitEmpty(Sinks.EmitFailureHandler failureHandler) {
		for (;;) {
			Sinks.EmitResult emitResult = tryEmitEmpty();
			if (emitResult.isSuccess()) {
				return;
			}

			boolean shouldRetry = failureHandler.onEmitFailure(SignalType.ON_COMPLETE,
					emitResult);
			if (shouldRetry) {
				continue;
			}

			switch (emitResult) {
				case FAIL_ZERO_SUBSCRIBER:
				case FAIL_OVERFLOW:
				case FAIL_CANCELLED:
				case FAIL_TERMINATED:
					return;
				case FAIL_NON_SERIALIZED:
					throw new EmissionException(emitResult,
							"Spec. Rule 1.3 - onSubscribe, onNext, onError and onComplete signaled to a Subscriber MUST be signaled serially."
					);
				default:
					throw new EmissionException(emitResult, "Unknown emitResult value");
			}
		}
	}

	@Override
	default void emitError(Throwable error, Sinks.EmitFailureHandler failureHandler) {
		for (;;) {
			Sinks.EmitResult emitResult = tryEmitError(error);
			if (emitResult.isSuccess()) {
				return;
			}

			boolean shouldRetry = failureHandler.onEmitFailure(SignalType.ON_ERROR,
					emitResult);
			if (shouldRetry) {
				continue;
			}

			switch (emitResult) {
				case FAIL_ZERO_SUBSCRIBER:
				case FAIL_OVERFLOW:
				case FAIL_CANCELLED:
					return;
				case FAIL_TERMINATED:
					Operators.onErrorDropped(error, currentContext());
					return;
				case FAIL_NON_SERIALIZED:
					throw new EmissionException(emitResult,
							"Spec. Rule 1.3 - onSubscribe, onNext, onError and onComplete signaled to a Subscriber MUST be signaled serially."
					);
				default:
					throw new EmissionException(emitResult, "Unknown emitResult value");
			}
		}
	}
}
