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
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
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
	 * @param value The onNext value that caused an error. null for cases like peek that
	 * are not tied to a particular value but still allow for resume.
	 * @param error The error.
	 * @param context The most significant {@link Context} in case the strategy needs it.
	 * @param forCancel The {@link Subscription} that should be cancelled if the
	 * strategy is terminal. Null to ignore (typically from a poll()).
	 * @param <T> The type of the value causing the error.
	 * @return a {@link Throwable} to propagate through onError if the strategy is
	 * terminal and cancelled the subscription, null if not.
	 */
	@Nullable
	<T> Throwable apply(@Nullable T value, Throwable error, Context context,
			@Nullable Subscription forCancel);

	/**
	 * A terminal strategy where the error is passed through the {@link Operators#onOperatorError(Subscription, Throwable, Object, Context)}
	 * hook (which therefore cancels the upstream subscription and propagates an error,
	 * potentially different from the original one).
	 */
	static OnNextFailureStrategy stop() {
		return STOP;
	}

	/**
	 * A non-terminal strategy where the error is passed to the {@link Operators#onErrorDropped(Throwable, Context)}
	 * hook and the incriminating source value is passed to the {@link Operators#onNextDropped(Object, Context)}
	 * hook, allowing the sequence to continue with further values.
	 */
	static OnNextFailureStrategy resumeDrop() {
		return RESUME_DROP;
	}

	/**
	 * A conditionally non-terminal strategy where the error is passed to the {@link Operators#onErrorDropped(Throwable, Context)}
	 * hook and the incriminating source value is passed to the {@link Operators#onNextDropped(Object, Context)}
	 * hook IF the error matches a predicate, allowing the sequence to continue with further values.
	 * <p>
	 * In case the error doesn't match the predicate, falls back to the {@link #stop()}
	 * strategy.
	 *
	 * @param causePredicate the predicate to match in order to resume from an error.
	 */
	static OnNextFailureStrategy resumeDropIf(Predicate<Throwable> causePredicate) {
		return new ConditionalDropStrategy(causePredicate);
	}

	/**
	 * Create a non-terminal strategy where the error and incriminating value are passed to
	 * custom {@link Consumer Consumers}, allowing the sequence to continue with further values.
	 * <p>
	 * Note that any {@link Exception} thrown by the consumer will suppress the original
	 * error and will be returned to the operator after cancelling upstream (behaving a
	 * bit like {@link #stop()} in this case).
	 *
	 * @param errorConsumer the {@link Consumer} to process the recovered errors with.
	 * @param valueConsumer the {@link Consumer} to process the error-causing values with.
	 * @return a new {@link OnNextFailureStrategy} that allows resuming the sequence.
	 */
	static OnNextFailureStrategy resume(Consumer<Throwable> errorConsumer,
			Consumer<Object> valueConsumer) {
		return new ResumeStrategy(errorConsumer, valueConsumer);
	}

	/**
	 * Create a partially non-terminal strategy where the sequence is allowed to continue
	 * with further values when a {@link Predicate} returns true. In that case, the error
	 * and incriminating value are passed to custom {@link Consumer Consumers}. Otherwise,
	 * falls back to the {@link #stop()} terminal strategy.
	 * <p>
	 * Note that any {@link Exception} thrown by the predicate or consumers will suppress
	 * the original error and will be processed by the {@link #stop()} strategy.
	 *
	 * @param causePredicate the {@link Predicate} to use to determine if a failure
	 * should be recovered from.
	 * @param errorConsumer the {@link Consumer} to process the recovered errors with.
	 * @param valueConsumer the {@link Consumer} to process the error-causing values with.
	 * @return a new {@link OnNextFailureStrategy} that allows resuming the sequence.
	 */
	static OnNextFailureStrategy resumeIf(
			Predicate<Throwable> causePredicate,
			Consumer<Throwable> errorConsumer,
			Consumer<Object>  valueConsumer) {
		return new ConditionalResumeStrategy(causePredicate, errorConsumer, valueConsumer, STOP);
	}

	//==== IMPLEMENTATIONS ====
	OnNextFailureStrategy STOP = new OnNextFailureStrategy() {

		@Override
		public <T> Throwable apply(@Nullable T value, Throwable error, Context context,
				@Nullable Subscription forCancel) {
			return Operators.onOperatorError(forCancel, error, value, context);
		}

	};

	OnNextFailureStrategy RESUME_DROP = new OnNextFailureStrategy() {

		@Override
		@Nullable
		public <T> Throwable apply(@Nullable T value, Throwable error, Context context,
				@Nullable Subscription forCancel) {
			if (value != null) Operators.onNextDropped(value, context);
			Operators.onErrorDropped(error, context);
			return null;
		}

	};

	final class ResumeStrategy implements OnNextFailureStrategy {

		final Consumer<Throwable> errorConsumer;
		final Consumer<Object>    valueConsumer;

		ResumeStrategy(Consumer<Throwable> errorConsumer, Consumer<Object> valueConsumer) {
			this.errorConsumer = errorConsumer;
			this.valueConsumer = valueConsumer;
		}

		@Override
		@Nullable
		public final <T> Throwable apply(@Nullable T value, Throwable error, Context context,
				@Nullable Subscription forCancel) {
			try {
				if (value != null) valueConsumer.accept(value);
				errorConsumer.accept(error);
				return null;
			}
			catch (Throwable t) {
				if (forCancel != null) {
					forCancel.cancel();
				}
				if (t != error) {
					t.addSuppressed(error);
				}
				return Exceptions.propagate(t);
			}
		}

	}

	final class ConditionalResumeStrategy implements OnNextFailureStrategy {

		final Predicate<Throwable>  errorPredicate;
		final Consumer<Throwable>   errorConsumer;
		final Consumer<Object>      valueConsumer;
		final OnNextFailureStrategy fallback;

		ConditionalResumeStrategy(Predicate<Throwable> errorPredicate,
				Consumer<Throwable> errorConsumer,
				Consumer<Object> valueConsumer,
				OnNextFailureStrategy fallback) {
			this.errorPredicate = errorPredicate;
			this.errorConsumer = errorConsumer;
			this.valueConsumer = valueConsumer;
			this.fallback = fallback;
		}

		@Override
		@Nullable
		public final <T> Throwable apply(@Nullable T value, Throwable error, Context context,
				@Nullable Subscription forCancel) {
			try {
				if (errorPredicate.test(error)) {
					if (value != null) valueConsumer.accept(value);
					errorConsumer.accept(error);
					return null;
				}
				else {
					return fallback.apply(value, error, context, forCancel);
				}
			}
			catch (Throwable t) {
				if (t != error) t.addSuppressed(error);
				return fallback.apply(value, t, context, forCancel);
			}
		}

	}

	/**
	 * A strategy that conditionally drops-and-resumes from errors. For errors that
	 * don't match the predicate, falls back to the STOP strategy.
	 */
	final class ConditionalDropStrategy implements OnNextFailureStrategy {

		final Predicate<Throwable> errorPredicate;

		ConditionalDropStrategy(Predicate<Throwable> errorPredicate) {
			this.errorPredicate = errorPredicate;
		}

		@Override
		@Nullable
		public final <T> Throwable apply(@Nullable T value, Throwable error, Context context,
				@Nullable Subscription forCancel) {
			try {
				if (errorPredicate.test(error)) {
					if (value != null) Operators.onNextDropped(value, context);
					Operators.onErrorDropped(error, context);
					return null;
				}
				else {
					return STOP.apply(value, error, context, forCancel);
				}
			}
			catch (Throwable t) {
				if (t != error) t.addSuppressed(error);
				return STOP.apply(value, t, context, forCancel);
			}
		}
	}

}
