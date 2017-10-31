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

import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A strategy to evaluate if errors that happens during an operator's onNext call can
 * be recovered from, allowing the sequence to continue. This opt-in strategy is
 * applied by compatible operators through the {@link Operators#onNextError(Object, Throwable, Context, Subscription)}
 * and {@link Operators#onNextPollError(Object, Throwable, Context)} methods.
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

	/**
	 * Returns whether or not this strategy allows resume of a particular error (and
	 * optionally the value that caused it).
	 *
	 * @param error the error being potentially recovered from.
	 * @param value the value causing the error, null if not applicable.
	 * @return true if this strategy would allow resuming the sequence.
	 * @see #process(Throwable, Object, Context)
	 */
	boolean canResume(Throwable error, @Nullable Object value);

	/**
	 * Process an error and the optional value that caused it (when applicable) in
	 * preparation for sequence resume, so that the error is not completely swallowed.
	 * <p>
	 * If the strategy cannot resume this kind of error (ie. {@link #canResume(Throwable, Object)}
	 * returns false), return the original error. Any exception in the processing will be
	 * caught and returned. If the strategy was able to process the error correctly,
	 * returns null.
	 *
	 * @param error the error being recovered from.
	 * @param value the value causing the error, null if not applicable.
	 * @param context the {@link Context} associated with the recovering sequence.
	 * @return null if the error was processed for resume, a {@link Throwable} to propagate
	 * otherwise.
	 * @see #canResume(Throwable, Object)
	 */
	@Nullable
	Throwable process(Throwable error, @Nullable Object value, Context context);

	/**
	 * A strategy that never let any error resume.
	 */
	static OnNextFailureStrategy stop() {
		return STOP;
	}

	/**
	 * A strategy that let all error resume. When processing, the error is passed to the
	 * {@link Operators#onErrorDropped(Throwable, Context)} hook and the incriminating
	 * source value (if available) is passed to the {@link Operators#onNextDropped(Object, Context)}
	 * hook, allowing the sequence to continue with further values.
	 */
	static OnNextFailureStrategy resumeDrop() {
		return RESUME_DROP;
	}

	/**
	 * A strategy that let some error resume. When processing, the error is passed to the
	 * {@link Operators#onErrorDropped(Throwable, Context)} hook and the incriminating
	 * source value (if available) is passed to the {@link Operators#onNextDropped(Object, Context)}
	 * hook, allowing the sequence to continue with further values.
	 * <p>
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
	 * Any exception thrown by the consumers will suppress the original error and be
	 * returned for propagation. If the original error is fatal then it is thrown
	 * upon processing it (see {@link Exceptions#throwIfFatal(Throwable)}).
	 *
	 *
	 * @param errorConsumer the {@link Consumer} to process the recovered errors with.
	 * @param valueConsumer the {@link Consumer} to process the error-causing values with.
	 * It must deal with potential {@code null}s.
	 * @return a new {@link OnNextFailureStrategy} that allows resuming the sequence.
	 */
	static OnNextFailureStrategy resume(Consumer<Throwable> errorConsumer,
			Consumer<Object> valueConsumer) {
		return new ResumeStrategy(null, errorConsumer, valueConsumer);
	}

	/**
	 * A strategy that let some errors resume. When processing, the error and the
	 * incriminating source value are passed to custom {@link Consumer Consumers}.
	 * <p>
	 * Any exception thrown by the predicate is thrown as is. Any exception thrown by
	 * the consumers will suppress the original error and be returned for propagation.
	 * Even if the original error is fatal, if it passes the predicate then it can
	 * be processed and recovered from (see {@link Exceptions#throwIfFatal(Throwable)}).
	 *
	 * @param causePredicate the {@link Predicate} to use to determine if a failure
	 * should be recovered from.
	 * @param errorConsumer the {@link Consumer} to process the recovered errors with.
	 * @param valueConsumer the {@link Consumer} to process the error-causing values with.
	 * It must deal with potential {@code null}s.
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
		public Throwable process(Throwable error, @Nullable Object value, Context context) {
			Exceptions.throwIfFatal(error);
			Throwable iee = new IllegalStateException("STOP strategy cannot process errors");
			iee.addSuppressed(error);
			return iee;
		}
	};

	OnNextFailureStrategy RESUME_DROP = new ResumeDropStrategy(null);

	final class ResumeStrategy implements OnNextFailureStrategy {

		final Predicate<Throwable>  errorPredicate;
		final Consumer<Throwable>   errorConsumer;
		final Consumer<Object>      valueConsumer;

		ResumeStrategy(@Nullable Predicate<Throwable> errorPredicate,
				Consumer<Throwable> errorConsumer,
				Consumer<Object> valueConsumer) {
			this.errorPredicate = errorPredicate;
			this.errorConsumer = errorConsumer;
			this.valueConsumer = valueConsumer;
		}

		@Override
		public boolean canResume(Throwable error, @Nullable Object value) {
			return errorPredicate == null || errorPredicate.test(error);
		}

		@Override
		@Nullable
		public Throwable process(Throwable error, @Nullable Object value, Context context) {
			if (errorPredicate == null) {
				Exceptions.throwIfFatal(error);
			}
			else if (!errorPredicate.test(error)) {
				Exceptions.throwIfFatal(error);
				return error;
			}
			try {
				valueConsumer.accept(value);
				errorConsumer.accept(error);
				return null;
			}
			catch (Throwable e) {
				e.addSuppressed(error);
				return e;
			}
		}
	}

	final class ResumeDropStrategy implements OnNextFailureStrategy {

		final Predicate<Throwable> errorPredicate;

		ResumeDropStrategy(@Nullable Predicate<Throwable> errorPredicate) {
			this.errorPredicate = errorPredicate;
		}

		@Override
		public boolean canResume(Throwable error, @Nullable Object value) {
			return errorPredicate == null || errorPredicate.test(error);
		}

		@Override
		@Nullable
		public Throwable process(Throwable error, @Nullable Object value, Context context) {
			if (errorPredicate == null) {
				Exceptions.throwIfFatal(error);
			}
			else if (!errorPredicate.test(error)) {
				Exceptions.throwIfFatal(error);
				return error;
			}
			try {
				if (value != null) {
					Operators.onNextDropped(value, context);
				}
				Operators.onErrorDropped(error, context);
				return null;
			}
			catch (Throwable e) {
				e.addSuppressed(error);
				return e;
			}
		}
	}

}
