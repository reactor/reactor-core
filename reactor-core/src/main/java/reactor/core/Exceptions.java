/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.util.annotation.Nullable;

/**
 * Global Reactor Core Exception handling and utils to operate on.
 *
 * @author Stephane Maldini
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
public abstract class Exceptions {

	/**
	 * A common error message used when a reactive streams source doesn't seem to respect
	 * backpressure signals, resulting in an operator's internal queue to be full.
	 */
	public static final String BACKPRESSURE_ERROR_QUEUE_FULL = "Queue is full: Reactive Streams source doesn't respect backpressure";

	/**
	 * A singleton instance of a Throwable indicating a terminal state for exceptions,
	 * don't leak this!
	 */
	@SuppressWarnings("ThrowableInstanceNeverThrown")
	public static final Throwable TERMINATED = new StaticThrowable("Operator has been terminated");

	/**
	 * Update an empty atomic reference with the given exception, or combine further added
	 * exceptions together as suppressed exceptions under a root Throwable with
	 * the {@code "Multiple exceptions"} message, if the atomic reference already holds
	 * one. This is short-circuited if the reference contains {@link #TERMINATED}.
	 * <p>
	 * Since composite exceptions and traceback exceptions share the same underlying mechanism
	 * of suppressed exceptions, a traceback could be made part of a composite exception.
	 * Use {@link #unwrapMultipleExcludingTracebacks(Throwable)} to filter out such elements in
	 * a composite if needed.
	 *
	 * @param <T> the parent instance type
	 * @param field the target field updater
	 * @param instance the parent instance for the field
	 * @param exception the Throwable to add.
	 *
	 * @return true if added, false if the field contained the {@link #TERMINATED}
	 * instance.
	 * @see #unwrapMultiple(Throwable)
	 */
	public static <T> boolean addThrowable(AtomicReferenceFieldUpdater<T, Throwable> field,
			T instance,
			Throwable exception) {
		for (; ; ) {
			Throwable current = field.get(instance);

			if (current == TERMINATED) {
				return false;
			}

			if (current instanceof CompositeException) {
				//this is ok, composite exceptions are never singletons
				current.addSuppressed(exception);
				return true;
			}

			Throwable update;
			if (current == null) {
				update = exception;
			} else {
				update = multiple(current, exception);
			}

			if (field.compareAndSet(instance, current, update)) {
				return true;
			}
		}
	}

	/**
	 * Create a composite exception that wraps the given {@link Throwable Throwable(s)},
	 * as suppressed exceptions. Instances create by this method can be detected using the
	 * {@link #isMultiple(Throwable)} check. The {@link #unwrapMultiple(Throwable)} method
	 * will correctly unwrap these to a {@link List} of the suppressed exceptions. Note
	 * that is will also be consistent in producing a List for other types of exceptions
	 * by putting the input inside a single-element List.
	 * <p>
	 * Since composite exceptions and traceback exceptions share the same underlying mechanism
	 * of suppressed exceptions, a traceback could be made part of a composite exception.
	 * Use {@link #unwrapMultipleExcludingTracebacks(Throwable)} to filter out such elements in
	 * a composite if needed.
	 *
	 * @param throwables the exceptions to wrap into a composite
	 * @return a composite exception with a standard message, and the given throwables as
	 * suppressed exceptions
	 * @see #addThrowable(AtomicReferenceFieldUpdater, Object, Throwable)
	 */
	public static RuntimeException multiple(Throwable... throwables) {
		CompositeException multiple = new CompositeException();
		//noinspection ConstantConditions
		if (throwables != null) {
			for (Throwable t : throwables) {
				//this is ok, multiple is always a new non-singleton instance
				multiple.addSuppressed(t);
			}
		}
		return multiple;
	}

	/**
	 * Create a composite exception that wraps the given {@link Throwable Throwable(s)},
	 * as suppressed exceptions. Instances created by this method can be detected using the
	 * {@link #isMultiple(Throwable)} check. The {@link #unwrapMultiple(Throwable)} method
	 * will correctly unwrap these to a {@link List} of the suppressed exceptions. Note
	 * that is will also be consistent in producing a List for other types of exceptions
	 * by putting the input inside a single-element List.
	 * <p>
	 * Since composite exceptions and traceback exceptions share the same underlying mechanism
	 * of suppressed exceptions, a traceback could be made part of a composite exception.
	 * Use {@link #unwrapMultipleExcludingTracebacks(Throwable)} to filter out such elements in
	 * a composite if needed.
	 *
	 * @param throwables the exceptions to wrap into a composite
	 * @return a composite exception with a standard message, and the given throwables as
	 * suppressed exceptions
	 * @see #addThrowable(AtomicReferenceFieldUpdater, Object, Throwable)
	 */
	public static RuntimeException multiple(Iterable<Throwable> throwables) {
		CompositeException multiple = new CompositeException();
		//noinspection ConstantConditions
		if (throwables != null) {
			for (Throwable t : throwables) {
				//this is ok, multiple is always a new non-singleton instance
				multiple.addSuppressed(t);
			}
		}
		return multiple;
	}

	/**
	 * Prepare an unchecked {@link RuntimeException} that will bubble upstream if thrown
	 * by an operator. <p>This method invokes {@link #throwIfFatal(Throwable)}.
	 *
	 * @param t the root cause
	 *
	 * @return an unchecked exception that should choose bubbling up over error callback
	 * path
	 */
	public static RuntimeException bubble(Throwable t) {
		throwIfFatal(t);
		return new BubblingException(t);
	}

	/**
	 * @return a new {@link IllegalStateException} with a cause message abiding to
	 * reactive stream specification rule 2.12.
	 */
	public static IllegalStateException duplicateOnSubscribeException() {
		return new IllegalStateException(
				"Spec. Rule 2.12 - Subscriber.onSubscribe MUST NOT be called more than once (based on object equality)");
	}

	/**
	 * Return an {@link UnsupportedOperationException} indicating that the error callback
	 * on a subscriber was not implemented, yet an error was propagated.
	 *
	 * @param cause original error not processed by a receiver.
	 * @return an {@link UnsupportedOperationException} indicating the error callback was
	 * not implemented and holding the original propagated error.
	 * @see #isErrorCallbackNotImplemented(Throwable)
	 */
	public static UnsupportedOperationException errorCallbackNotImplemented(Throwable cause) {
		Objects.requireNonNull(cause, "cause");
		return new ErrorCallbackNotImplemented(cause);
	}

	/**
	 * An exception that is propagated upward and considered as "fatal" as per Reactive
	 * Stream limited list of exceptions allowed to bubble. It is not meant to be common
	 * error resolution but might assist implementors in dealing with boundaries (queues,
	 * combinations and async).
	 *
	 * @return a {@link RuntimeException} that can be identified via {@link #isCancel}
	 * @see #isCancel(Throwable)
	 */
	public static RuntimeException failWithCancel() {
		return new CancelException();
	}

	/**
	 * Return an {@link IllegalStateException} indicating the receiver is overrun by
	 * more signals than expected in case of a bounded queue, or more generally that data
	 * couldn't be emitted due to a lack of request
	 *
	 * @return an {@link IllegalStateException}
	 * @see #isOverflow(Throwable)
	 */
	public static IllegalStateException failWithOverflow() {
		return new OverflowException("The receiver is overrun by more signals than expected (bounded queue...)");
	}

	/**
	 * Return an {@link IllegalStateException} indicating the receiver is overrun by
	 * more signals than expected in case of a bounded queue or more generally that data
	 * couldn't be emitted due to a lack of request
	 *
	 * @param message the exception's message
	 * @return an {@link IllegalStateException}
	 * @see #isOverflow(Throwable)
	 */
	public static IllegalStateException failWithOverflow(String message) {
		return new OverflowException(message);
	}

	/**
	 * Return a singleton {@link RejectedExecutionException}
	 *
	 * @return a singleton {@link RejectedExecutionException}
	 */
	public static RejectedExecutionException failWithRejected() {
		return REJECTED_EXECUTION;
	}

	/**
	 * Return a singleton {@link RejectedExecutionException} with a message indicating
	 * the reason is due to the scheduler not being time-capable
	 *
	 * @return a singleton {@link RejectedExecutionException}
	 */
	public static RejectedExecutionException failWithRejectedNotTimeCapable() {
		return NOT_TIME_CAPABLE_REJECTED_EXECUTION;
	}

	/**
	 * Return a new {@link RejectedExecutionException} with standard message and cause,
	 * unless the {@code cause} is already a {@link RejectedExecutionException} created
	 * via {@link #failWithRejected(Throwable)} (not the singleton-producing variants).
	 *
	 * @param cause the original exception that caused the rejection
	 * @return a new {@link RejectedExecutionException} with standard message and cause
	 */
	public static RejectedExecutionException failWithRejected(Throwable cause) {
		if (cause instanceof ReactorRejectedExecutionException) {
			return (RejectedExecutionException) cause;
		}
		return new ReactorRejectedExecutionException("Scheduler unavailable", cause);
	}

	/**
	 * Return a new {@link RejectedExecutionException} with given message.
	 *
	 * @param message the rejection message
	 * @return a new {@link RejectedExecutionException} with custom message
	 */
	public static RejectedExecutionException failWithRejected(String message) {
		return new ReactorRejectedExecutionException(message);
	}

	/**
	 * Check if the given exception represents an {@link #failWithOverflow() overflow}.
	 * @param t the {@link Throwable} error to check
	 * @return true if the given {@link Throwable} represents an overflow.
	 */
	public static boolean isOverflow(@Nullable Throwable t) {
		return t instanceof OverflowException;
	}

	/**
	 * Check if the given exception is a {@link #bubble(Throwable) bubbled} wrapped exception.
	 * @param t the {@link Throwable} error to check
	 * @return true if given {@link Throwable} is a bubbled wrapped exception.
	 */
	public static boolean isBubbling(@Nullable Throwable t) {
		return t instanceof BubblingException;
	}

	/**
	 * Check if the given error is a {@link #failWithCancel() cancel signal}.
	 * @param t the {@link Throwable} error to check
	 * @return true if given {@link Throwable} is a cancellation token.
	 */
	public static boolean isCancel(@Nullable Throwable t) {
		return t instanceof CancelException;
	}

	/**
	 * Check if the given error is a {@link #errorCallbackNotImplemented(Throwable) callback not implemented}
	 * exception, in which case its {@link Throwable#getCause() cause} will be the propagated
	 * error that couldn't be processed.
	 *
	 * @param t the {@link Throwable} error to check
	 * @return true if given {@link Throwable} is a callback not implemented exception.
	 */
	public static boolean isErrorCallbackNotImplemented(@Nullable Throwable t) {
		return t instanceof ErrorCallbackNotImplemented;
	}

	/**
	 * Check a {@link Throwable} to see if it is a composite, as created by {@link #multiple(Throwable...)}.
	 *
	 * @param t the {@link Throwable} to check, {@literal null} always yields {@literal false}
	 * @return true if the Throwable is an instance created by {@link #multiple(Throwable...)}, false otherwise
	 */
	public static boolean isMultiple(@Nullable Throwable t) {
		return t instanceof CompositeException;
	}

	/**
	 * Check a {@link Throwable} to see if it is a traceback, as created by the checkpoint operator or debug utilities.
	 *
	 * @param t the {@link Throwable} to check, {@literal null} always yields {@literal false}
	 * @return true if the Throwable is a traceback, false otherwise
	 */
	public static boolean isTraceback(@Nullable Throwable t) {
		if (t == null) {
			return false;
		}
		//FIXME maybe add an interface here for detection purposes
		return "reactor.core.publisher.FluxOnAssembly.OnAssemblyException".equals(t.getClass().getCanonicalName());
	}

	/**
	 * @param elements the invalid requested demand
	 *
	 * @return a new {@link IllegalArgumentException} with a cause message abiding to
	 * reactive stream specification rule 3.9.
	 */
	public static IllegalArgumentException nullOrNegativeRequestException(long elements) {
		return new IllegalArgumentException(
				"Spec. Rule 3.9 - Cannot request a non strictly positive number: " + elements);
	}

	/**
	 * Prepare an unchecked {@link RuntimeException} that should be propagated
	 * downstream through {@link org.reactivestreams.Subscriber#onError(Throwable)}.
	 * <p>This method invokes {@link #throwIfFatal(Throwable)}.
	 *
	 * @param t the root cause
	 *
	 * @return an unchecked exception to propagate through onError signals.
	 */
	public static RuntimeException propagate(Throwable t) {
		throwIfFatal(t);
		if (t instanceof RuntimeException) {
			return (RuntimeException) t;
		}
		return new ReactiveException(t);
	}

	/**
	 * Atomic utility to safely mark a volatile throwable reference with a terminal
	 * marker.
	 *
	 * @param field the atomic container
	 * @param instance the reference instance
	 * @param <T> the instance type
	 *
	 * @return the previously masked throwable
	 */
	@Nullable
	public static <T> Throwable terminate(AtomicReferenceFieldUpdater<T, Throwable> field,
			T instance) {
		Throwable current = field.get(instance);
		if (current != TERMINATED) {
			current = field.getAndSet(instance, TERMINATED);
		}
		return current;
	}

	/**
	 * Throws a particular {@code Throwable} only if it belongs to a set of "fatal" error
	 * varieties. These varieties are as follows: <ul>
	 *     <li>{@code BubblingException} (as detectable by {@link #isBubbling(Throwable)})</li>
	 *     <li>{@code ErrorCallbackNotImplemented} (as detectable by {@link #isErrorCallbackNotImplemented(Throwable)})</li>
	 *     <li>{@link VirtualMachineError}</li> <li>{@link ThreadDeath}</li> <li>{@link LinkageError}</li> </ul>
	 *
	 * @param t the exception to evaluate
	 */
	public static void throwIfFatal(@Nullable Throwable t) {
		if (t instanceof BubblingException) {
			throw (BubblingException) t;
		}
		if (t instanceof ErrorCallbackNotImplemented) {
			throw (ErrorCallbackNotImplemented) t;
		}
		throwIfJvmFatal(t);
	}

	/**
	 * Throws a particular {@code Throwable} only if it belongs to a set of "fatal" error
	 * varieties native to the JVM. These varieties are as follows:
	 * <ul> <li>{@link VirtualMachineError}</li> <li>{@link ThreadDeath}</li>
	 * <li>{@link LinkageError}</li> </ul>
	 *
	 * @param t the exception to evaluate
	 */
	public static void throwIfJvmFatal(@Nullable Throwable t) {
		if (t instanceof VirtualMachineError) {
			throw (VirtualMachineError) t;
		}
		if (t instanceof ThreadDeath) {
			throw (ThreadDeath) t;
		}
		if (t instanceof LinkageError) {
			throw (LinkageError) t;
		}
	}

	/**
	 * Unwrap a particular {@code Throwable} only if it is was wrapped via
	 * {@link #bubble(Throwable) bubble} or {@link #propagate(Throwable) propagate}.
	 *
	 * @param t the exception to unwrap
	 *
	 * @return the unwrapped exception or current one if null
	 */
	public static Throwable unwrap(Throwable t) {
		Throwable _t = t;
		while (_t instanceof ReactiveException) {
			_t = _t.getCause();
		}
		return _t == null ? t : _t;
	}

	/**
	 * Attempt to unwrap a {@link Throwable} into a {@link List} of Throwables. This is
	 * only done on the condition that said Throwable is a composite exception built by
	 * {@link #multiple(Throwable...)}, in which case the list contains the exceptions
	 * wrapped as suppressed exceptions in the composite. In any other case, the list
	 * only contains the input Throwable (or is empty in case of null input).
	 * <p>
	 * Since composite exceptions and traceback exceptions share the same underlying mechanism
	 * of suppressed exceptions, a traceback could be made part of a composite exception.
	 * Use {@link #unwrapMultipleExcludingTracebacks(Throwable)} to filter out such elements in
	 * a composite if needed.
	 *
	 * @param potentialMultiple the {@link Throwable} to unwrap if multiple
	 * @return a {@link List} of the exceptions suppressed by the {@link Throwable} if
	 * multiple, or a List containing the Throwable otherwise. Null input results in an
	 * empty List.
	 * @see #unwrapMultipleExcludingTracebacks(Throwable)
	 */
	public static List<Throwable> unwrapMultiple(@Nullable Throwable potentialMultiple) {
		if (potentialMultiple == null) {
			return Collections.emptyList();
		}

		if (isMultiple(potentialMultiple)) {
			return Arrays.asList(potentialMultiple.getSuppressed());
		}

		return Collections.singletonList(potentialMultiple);
	}

	/**
	 * Attempt to unwrap a {@link Throwable} into a {@link List} of Throwables, excluding instances that
	 * are {@link #isTraceback(Throwable) tracebacks}.
	 * This is only done on the condition that said Throwable is a composite exception built by
	 * {@link #multiple(Throwable...)}, in which case the returned list contains its suppressed exceptions
	 * minus the tracebacks. In any other case, the list only contains the input Throwable (or is empty in
	 * case of null input).
	 * <p>
	 * This is useful because tracebacks are added as suppressed exceptions and thus can appear as components
	 * of a composite.
	 *
	 * @param potentialMultiple the {@link Throwable} to unwrap if multiple
	 * @return a {@link List} of the exceptions suppressed by the {@link Throwable} minus the
	 * tracebacks if multiple, or a List containing the Throwable otherwise. Null input results in an
	 * empty List.
	 */
	public static List<Throwable> unwrapMultipleExcludingTracebacks(@Nullable Throwable potentialMultiple) {
		if (potentialMultiple == null) {
			return Collections.emptyList();
		}

		if (isMultiple(potentialMultiple)) {
			final Throwable[] suppressed = potentialMultiple.getSuppressed();
			List<Throwable> filtered = new ArrayList<>(suppressed.length);
			for (Throwable t : suppressed) {
				if (isTraceback(t)) {
					continue;
				}
				filtered.add(t);
			}
			return filtered;
		}

		return Collections.singletonList(potentialMultiple);
	}

	/**
	 * Safely suppress a {@link Throwable} on a {@link RuntimeException}. The returned
	 * {@link RuntimeException}, bearing the suppressed exception, is most often the same
	 * as the original exception unless:
	 * <ul>
	 *     <li>original and tentatively suppressed exceptions are one and the same: do
	 *     nothing but return the original.</li>
	 *     <li>original is one of the singleton {@link RejectedExecutionException} created
	 *     by Reactor: make a copy the {@link RejectedExecutionException}, add the
	 *     suppressed exception to it and return that copy.</li>
	 *
	 * </ul>
	 * @param original the original {@link RuntimeException} to bear a suppressed exception
	 * @param suppressed the {@link Throwable} to suppress
	 * @return the (most of the time original) {@link RuntimeException} bearing the
	 * suppressed {@link Throwable}
	 */
	public static final RuntimeException addSuppressed(RuntimeException original, Throwable suppressed) {
		if (original == suppressed) {
			return original;
		}
		if (original == REJECTED_EXECUTION || original == NOT_TIME_CAPABLE_REJECTED_EXECUTION) {
			RejectedExecutionException ree = new RejectedExecutionException(original.getMessage());
			ree.addSuppressed(suppressed);
			return ree;
		}
		else {
			original.addSuppressed(suppressed);
			return original;
		}
	}

	/**
	 * Safely suppress a {@link Throwable} on an original {@link Throwable}. The returned
	 * {@link Throwable}, bearing the suppressed exception, is most often the same
	 * as the original one unless:
	 * <ul>
	 *     <li>original and tentatively suppressed exceptions are one and the same: do
	 *     nothing but return the original.</li>
	 *     <li>original is one of the singleton {@link RejectedExecutionException} created
	 *     by Reactor: make a copy the {@link RejectedExecutionException}, add the
	 *     suppressed exception to it and return that copy.</li>
	 *     <li>original is a special internal TERMINATED singleton instance: return it
	 *     without suppressing the exception.</li>
	 *
	 * </ul>
	 * @param original the original {@link Throwable} to bear a suppressed exception
	 * @param suppressed the {@link Throwable} to suppress
	 * @return the (most of the time original) {@link Throwable} bearing the
	 * suppressed {@link Throwable}
	 */
	public static final Throwable addSuppressed(Throwable original, final Throwable suppressed) {
		if (original == suppressed) {
			return original;
		}

		if (original == TERMINATED) {
			return original;
		}

		if (original == REJECTED_EXECUTION || original == NOT_TIME_CAPABLE_REJECTED_EXECUTION) {
			RejectedExecutionException ree = new RejectedExecutionException(original.getMessage());
			ree.addSuppressed(suppressed);
			return ree;
		}
		else {
			original.addSuppressed(suppressed);
			return original;
		}
	}

	Exceptions() {
	}

	static final RejectedExecutionException REJECTED_EXECUTION = new StaticRejectedExecutionException("Scheduler unavailable");

	static final RejectedExecutionException NOT_TIME_CAPABLE_REJECTED_EXECUTION =
			new StaticRejectedExecutionException("Scheduler is not capable of time-based scheduling");

	static class CompositeException extends ReactiveException {

		CompositeException() {
			super("Multiple exceptions");
		}

		private static final long serialVersionUID = 8070744939537687606L;
	}

	static class BubblingException extends ReactiveException {

		BubblingException(String message) {
			super(message);
		}

		BubblingException(Throwable cause) {
			super(cause);
		}

		private static final long serialVersionUID = 2491425277432776142L;
	}

	/**
	 * An exception that is propagated downward through {@link org.reactivestreams.Subscriber#onError(Throwable)}
	 */
	static class ReactiveException extends RuntimeException {

		ReactiveException(Throwable cause) {
			super(cause);
		}

		ReactiveException(String message) {
			super(message);
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return getCause() != null ? getCause().fillInStackTrace() :
					super.fillInStackTrace();
		}

		private static final long serialVersionUID = 2491425227432776143L;
	}

	static final class ErrorCallbackNotImplemented extends UnsupportedOperationException {

		ErrorCallbackNotImplemented(Throwable cause) {
			super(cause);
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return this;
		}

		private static final long serialVersionUID = 2491425227432776143L;
	}

	/**
	 * An error signal from downstream subscribers consuming data when their state is
	 * denying any additional event.
	 *
	 * @author Stephane Maldini
	 */
	static final class CancelException extends BubblingException {

		CancelException() {
			super("The subscriber has denied dispatching");
		}

		private static final long serialVersionUID = 2491425227432776144L;

	}

	static final class OverflowException extends IllegalStateException {

		OverflowException(String s) {
			super(s);
		}
	}

	static class ReactorRejectedExecutionException extends RejectedExecutionException {

		ReactorRejectedExecutionException(String message, Throwable cause) {
			super(message, cause);
		}

		ReactorRejectedExecutionException(String message) {
			super(message);
		}
	}

	/**
	 * A {@link RejectedExecutionException} that is tailored for usage as a static final
	 * field. It avoids {@link ClassLoader}-related leaks by bypassing stacktrace filling.
	 */
	static final class StaticRejectedExecutionException extends RejectedExecutionException {

		StaticRejectedExecutionException(String message, Throwable cause) {
			super(message, cause);
		}

		StaticRejectedExecutionException(String message) {
			super(message);
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return this;
		}
	}

	/**
	 * A general-purpose {@link Throwable} that is suitable for usage as a static final
	 * field. It avoids {@link ClassLoader}-related leaks by bypassing stacktrace filling.
	 * Exception {{@link Exception#addSuppressed(Throwable)} suppression} is also disabled.
	 */
	//see https://github.com/reactor/reactor-core/pull/1872
	static final class StaticThrowable extends Error {

		StaticThrowable(String message) {
			super(message, null, false, false);
		}

		StaticThrowable(String message, Throwable cause) {
			super(message, cause, false, false);
		}

		StaticThrowable(Throwable cause) {
			super(cause.toString(), cause, false, false);
		}
	}

}
