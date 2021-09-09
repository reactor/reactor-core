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

import reactor.core.Exceptions;
import reactor.core.publisher.Sinks.EmissionException;
import reactor.util.annotation.Nullable;

interface InternalOneSink<T> extends Sinks.One<T>, InternalEmptySink<T> {

	@Override
	default void emitValue(@Nullable T value, Sinks.EmitFailureHandler failureHandler) {
		if (value == null) {
			emitEmpty(failureHandler);
			return;
		}

		for (;;) {
			Sinks.EmitResult emitResult = tryEmitValue(value);
			if (emitResult.isSuccess()) {
				return;
			}

			boolean shouldRetry = failureHandler.onEmitFailure(SignalType.ON_NEXT,
					emitResult);
			if (shouldRetry) {
				continue;
			}

			switch (emitResult) {
				case FAIL_ZERO_SUBSCRIBER:
					//we want to "discard" without rendering the sink terminated.
					// effectively NO-OP cause there's no subscriber, so no context :(
					return;
				case FAIL_OVERFLOW:
					Operators.onDiscard(value, currentContext());
					//the emitError will onErrorDropped if already terminated
					emitError(Exceptions.failWithOverflow("Backpressure overflow during Sinks.One#emitValue"),
							failureHandler);
					return;
				case FAIL_CANCELLED:
					Operators.onDiscard(value, currentContext());
					return;
				case FAIL_TERMINATED:
					Operators.onNextDropped(value, currentContext());
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
