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

import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.util.context.Context;

/**
 * A strategy to evaluate if errors that happens during an operator's onNext call can
 * be recovered from, allowing the sequence to continue. This opt-in strategy is
 * applied by compatible operators through the {@link Operators#onNextFailure(Object, Throwable, Context, Subscription)}
 * and {@link Operators#onNextPollFailure(Object, Throwable, Context)} methods.
 * See {@link #stop()}, {@link #resumeDrop()}, {@link #resumeDropIf(Predicate)},
 * {@link #resume(Consumer, Consumer)} and {@link #resumeIf(Predicate, Consumer, Consumer)}
 * for the possible strategies.
 *
 * @author Simon Baslé
 */
public interface OnNextFailureStrategy {

	/**
	 * The key that can be used to store an {@link OnNextFailureStrategy} in a {@link Context}.
	 */
	String KEY_ON_NEXT_ERROR_STRATEGY = "reactor.onNextError.localStrategy";

//	/**
//	 * Apply the {@link OnNextFailureStrategy} by returning a {@link Throwable} when the
//	 * error strategy doesn't allow sequences to continue, or null otherwise.
//	 *
//	 * @param value The onNext value that caused an error. null for cases like peek that
//	 * are not tied to a particular value but still allow for resume.
//	 * @param error The error.
//	 * @param context The most significant {@link Context} in case the strategy needs it.
//	 * @param forCancel The {@link Subscription} that should be cancelled if the
//	 * strategy is terminal. Null to ignore (typically from a poll()).
//	 * @param <T> The type of the value causing the error.
//	 * @return a {@link Throwable} to propagate through onError if the strategy is
//	 * terminal and cancelled the subscription, null if not.
//	 */
//	@Nullable
//	<T> Throwable apply(@Nullable T value, Throwable error, Context context,
//			@Nullable Subscription forCancel);

	/**
	 * Returns whether or not this strategy allows resume of a particular error (and
	 * optionally the value that caused it).
	 *
	 * @param error the error being potentially recovered from.
	 * @param value the value causing the error, null if not applicable.
	 * @return true if this strategy would allow resuming the sequence, in which case
	 * {@link #process(Throwable, Object, Context)} should be invoked.
	 * @see #process(Throwable, Object, Context)
	 */
	boolean canResume(Throwable error, @Nullable Object value);

	/**
	 * Process an error and the optional value that caused it (when applicable) in
	 * preparation for sequence resume, so that the error is not completely swallowed.
	 * Note that this method should only be called if the {@link #canResume(Throwable, Object)}
	 * method returns {@code true}.
	 *
	 * @param error the error being recovered from.
	 * @param value the value causing the error, null if not applicable.
	 * @param context the {@link Context} associated with the recovering sequence.
	 * @see #canResume(Throwable, Object)
	 */
	void process(Throwable error, @Nullable Object value, Context context);

	/**
	 * A strategy that never let any error resume.
	 */
	static OnNextFailureStrategy stop() {
		return STOP;
	}

	/**
	 * A strategy that let all error resume. When processing, the error is passed to the
	 * {@link Operators#onErrorDropped(Throwable, Context)} hook and the incriminating
	 * source value is passed to the {@link Operators#onNextDropped(Object, Context)}
	 * hook, allowing the sequence to continue with further values.
	 */
	static OnNextFailureStrategy resumeDrop() {
		return RESUME_DROP;
	}

	/**
	 * A strategy that let some error resume. When processing, the error is passed to the
	 * {@link Operators#onErrorDropped(Throwable, Context)} hook and the incriminating
	 * source value is passed to the {@link Operators#onNextDropped(Object, Context)}
	 * hook, allowing the sequence to continue with further values.
	 * <p>
	 * The predicate is not tested again by {@link #process(Throwable, Object, Context)}.
	 * Any exception thrown by the predicate is thrown as is.
	 *
	 * @param causePredicate the predicate to match in order to resume from an error.
	 */
	static OnNextFailureStrategy resumeDropIf(Predicate<Throwable> causePredicate) {
		return new ResumeDropStrategy(causePredicate);
	}

	/**
	 * A strategy that let all errors resume. When processing, the error and the
	 * incriminating source value are passed to custom {@link Consumer Consumers}.
	 * <p>
	 * Any exception thrown by the consumers is thrown as is.
	 *
	 * @param errorConsumer the {@link Consumer} to process the recovered errors with.
	 * @param valueConsumer the {@link Consumer} to process the error-causing values with.
	 * @return a new {@link OnNextFailureStrategy} that allows resuming the sequence.
	 */
	static OnNextFailureStrategy resume(Consumer<Throwable> errorConsumer,
			Consumer<Object> valueConsumer) {
		return new ResumeStrategy(e -> true, errorConsumer, valueConsumer);
	}

	/**
	 * A strategy that let some errors resume. When processing, the error and the
	 * incriminating source value are passed to custom {@link Consumer Consumers}.
	 * <p>
	 * The predicate is not tested again by {@link #process(Throwable, Object, Context)}.
	 * Any exception thrown by the predicate or consumers is thrown as is.
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
		return new ResumeStrategy(causePredicate, errorConsumer, valueConsumer);
	}

	//==== IMPLEMENTATIONS ====
	OnNextFailureStrategy STOP = new OnNextFailureStrategy() {

		@Override
		public boolean canResume(Throwable error, @Nullable Object value) {
			return false;
		}

		@Override
		public void process(Throwable error, @Nullable Object value, Context context) {
			throw new IllegalStateException("STOP strategy cannot process errors");
		}
	};

	OnNextFailureStrategy RESUME_DROP = new ResumeDropStrategy(e -> true);

	final class ResumeStrategy implements OnNextFailureStrategy {

		final Predicate<Throwable>  errorPredicate;
		final Consumer<Throwable>   errorConsumer;
		final Consumer<Object>      valueConsumer;

		ResumeStrategy(Predicate<Throwable> errorPredicate,
				Consumer<Throwable> errorConsumer,
				Consumer<Object> valueConsumer) {
			this.errorPredicate = errorPredicate;
			this.errorConsumer = errorConsumer;
			this.valueConsumer = valueConsumer;
		}

		@Override
		public boolean canResume(Throwable error, @Nullable Object value) {
			return errorPredicate.test(error);
		}

		@Override
		public void process(Throwable error, @Nullable Object value, Context context) {
			if (value != null) {
				valueConsumer.accept(value);
			}
			errorConsumer.accept(error);
		}
	}

	final class ResumeDropStrategy implements OnNextFailureStrategy {

		final Predicate<Throwable> errorPredicate;

		ResumeDropStrategy(Predicate<Throwable> errorPredicate) {
			this.errorPredicate = errorPredicate;
		}

		@Override
		public boolean canResume(Throwable error, @Nullable Object value) {
			return errorPredicate.test(error);
		}

		@Override
		public void process(Throwable error, @Nullable Object value, Context context) {
			if (value != null) Operators.onNextDropped(value, context);
			Operators.onErrorDropped(error, context);
		}
	}

}
