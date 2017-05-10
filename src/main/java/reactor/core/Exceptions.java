/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

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
	public static final Throwable TERMINATED = new Throwable("No further exceptions");

	/**
	 * Update an empty atomic reference with the given exception, or combine further added
	 * exceptions together as suppressed exceptions under a root Throwable with
	 * the {@code "Multiple exceptions"} message, if the atomic reference already holds
	 * one. This is short-circuited if the reference contains {@link #TERMINATED}.
	 *
	 * @param <T> the parent instance type
	 * @param field the target field updater
	 * @param instance the parent instance for the field
	 * @param exception the Throwable to add.
	 *
	 * @return true if added, false if the field contained the {@link #TERMINATED}
	 * instance.
	 */
	public static <T> boolean addThrowable(AtomicReferenceFieldUpdater<T, Throwable> field,
			T instance,
			Throwable exception) {
		for (; ; ) {
			Throwable current = field.get(instance);

			if (current == TERMINATED) {
				return false;
			}

			Throwable update;
			if (current == null) {
				update = exception;
			}
			else {
				update = multiple(current, exception);
			}

			if (field.compareAndSet(instance, current, update)) {
				return true;
			}
		}
	}

	public static RuntimeException multiple(Throwable... throwables) {
		RuntimeException multiple = new RuntimeException("Multiple exceptions");
		if (throwables != null) {
			for (Throwable t : throwables) {
				multiple.addSuppressed(t);
			}
		}
		return multiple;
	}

	/**
	 * @return a new {@link NullPointerException} with a cause message abiding to reactive
	 * stream specification rule 2.13.
	 */
	public static NullPointerException argumentIsNullException() {
		return new NullPointerException("Spec 2.13: Signal/argument cannot be null");
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
	 * Check if the given exception represents an {@link #failWithOverflow() overflow}.
	 * @param t the {@link Throwable} error to check
	 * @return true if the given {@link Throwable} represents an overflow.
	 */
	public static boolean isOverflow(Throwable t) {
		return t instanceof OverflowException;
	}

	/**
	 * Check if the given exception is a {@link #bubble(Throwable) bubbled} wrapped exception.
	 * @param t the {@link Throwable} error to check
	 * @return true if given {@link Throwable} is a bubbled wrapped exception.
	 */
	public static boolean isBubbling(Throwable t) {
		return t instanceof BubblingException;
	}

	/**
	 * Check if the given error is a {@link #failWithCancel() cancel signal}.
	 * @param t the {@link Throwable} error to check
	 * @return true if given {@link Throwable} is a cancellation token.
	 */
	public static boolean isCancel(Throwable t) {
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
	public static boolean isErrorCallbackNotImplemented(Throwable t) {
		return t != null && t.getClass().equals(ErrorCallbackNotImplemented
				.class);
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
	public static void throwIfFatal(Throwable t) {
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
	public static void throwIfJvmFatal(Throwable t) {
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
	 * @return the unwrapped exception
	 */
	public static Throwable unwrap(Throwable t) {
		Throwable _t = t;
		while (_t instanceof ReactiveException) {
			_t = _t.getCause();
		}
		return _t;
	}

	Exceptions() {
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

}
