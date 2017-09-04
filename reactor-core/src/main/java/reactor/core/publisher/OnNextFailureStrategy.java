/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.function.BiConsumer;
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.util.context.Context;

/**
 * A strategy to deal with errors that happens during an operator's onNext call, by either
 * propagating the error or allowing the sequence to continue. This opt-in strategy is
 * applied by compatible operators through the {@link Operators#onNextFailure(Object, Throwable, Context, Subscription)}
 * and {@link Operators#onNextPollFailure(Object, Throwable, Context)} methods.
 * See {@link #STOP}, {@link #RESUME_DROP} and {@link #resume(BiConsumer)} for the
 * possible strategies.
 *
 * @author Simon Baslé
 */
public interface OnNextFailureStrategy {

	/**
	 * The key that can be used to store an {@link OnNextFailureStrategy} in a {@link Context}.
	 */
	String KEY_ON_NEXT_ERROR_STRATEGY = "reactor.onNextError.localStrategy";

	/**
	 * Apply the {@link OnNextFailureStrategy} by returning a {@link Throwable} when the
	 * error strategy doesn't allow sequences to continue, or null otherwise.
	 *
	 * @param value The onNext value that caused an error.
	 * @param error The error.
	 * @param context The most significant {@link Context} in case the strategy needs it.
	 * @param forCancel The {@link Subscription} that should be cancelled if the
	 * strategy is terminal. Null to ignore (typically from a poll()).
	 * @param <T> The type of the value causing the error.
	 * @return a {@link Throwable} to propagate through onError if the strategy is
	 * terminal and cancelled the subscription, null if not.
	 */
	@Nullable
	<T> Throwable apply(T value, Throwable error, Context context,
			@Nullable Subscription forCancel);

	/**
	 * A terminal strategy where the error is passed through the {@link Operators#onOperatorError(Subscription, Throwable, Object, Context)}
	 * hook (which therefore cancels the upstream subscription and propagates an error,
	 * potentially different from the original one).
	 */
	OnNextFailureStrategy STOP = new OnNextFailureStrategy() {

		@Override
		public <T> Throwable apply(T value, Throwable error, Context context, Subscription forCancel) {
			return Operators.onOperatorError(forCancel, error, value, context);
		}

	};

	/**
	 * A non-terminal strategy where the error is passed to the {@link Operators#onErrorDropped(Throwable, Context)}
	 * hook and the incriminating source value is passed to the {@link Operators#onNextDropped(Object, Context)}
	 * hook, allowing the sequence to continue with further values.
	 */
	OnNextFailureStrategy RESUME_DROP = new OnNextFailureStrategy() {

		@Override
		@Nullable
		public <T> Throwable apply(T value, Throwable error, Context context,
				@Nullable Subscription forCancel) {
			Operators.onNextDropped(value, context);
			Operators.onErrorDropped(error, context);
			return null;
		}

	};

	/**
	 * Create a non-terminal strategy where the error and incriminating value is passed to
	 * a custom {@link BiConsumer}, allowing the sequence to continue with further values.
	 * <p>
	 * Note that any {@link Exception} thrown by the consumer is wrapped by {@link Exceptions#propagate(Throwable)}
	 * and is returned to the operator, in effect falling back to a terminal strategy
	 * comparable to {@link #STOP}.
	 *
	 * @param causeConsumer the {@link BiConsumer} to process the recovered errors (and
	 * cause values) with.
	 * @return a new {@link OnNextFailureStrategy} strategy that allows resuming the sequence.
	 */
	static OnNextFailureStrategy resume(BiConsumer<Throwable, Object> causeConsumer) {
		return new ResumeStrategy(causeConsumer);
	}

	final class ResumeStrategy implements OnNextFailureStrategy {

		final BiConsumer<Throwable, Object> errorValueConsumer;

		ResumeStrategy(BiConsumer<Throwable, Object> errorValueConsumer) {
			this.errorValueConsumer = errorValueConsumer;
		}

		@Override
		@Nullable
		public final <T> Throwable apply(T value, Throwable error, Context context,
				@Nullable Subscription forCancel) {
			try {
				errorValueConsumer.accept(error, value);
				return null;
			}
			catch (Throwable t) {
				return Exceptions.propagate(t);
			}
		}

	}

}
