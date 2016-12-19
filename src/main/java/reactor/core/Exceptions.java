/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
	 *
	 */
	public static final boolean CANCEL_STACKTRACE =
			Boolean.parseBoolean(System.getProperty("reactor.trace.cancel", "false"));

	/**
	 * A singleton instance of a Throwable indicating a terminal state for exceptions,
	 * don't leak this!
	 */
	@SuppressWarnings("ThrowableInstanceNeverThrown")
	public static final Throwable TERMINATED = new Throwable("No further exceptions");

	/**
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
				update = new Throwable("Multiple exceptions");
				update.addSuppressed(current);
				update.addSuppressed(exception);
			}

			if (field.compareAndSet(instance, current, update)) {
				return true;
			}
		}
	}

	/**
	 * @return a new {@link NullPointerException} with a cause message abiding to reactive
	 * stream specification.
	 */
	public static NullPointerException argumentIsNullException() {
		return new NullPointerException("Spec 2.13: Signal/argument cannot be null");
	}

	/**
	 * Return an unchecked {@link RuntimeException} to be thrown that will bubble
	 * upstream. <p>This method invokes {@link #throwIfFatal(Throwable)}.
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
	 * reactive stream specification.
	 */
	public static IllegalStateException duplicateOnSubscribeException() {
		return new IllegalStateException(
				"Spec. Rule 2.12 - Subscriber.onSubscribe MUST NOT be called more than once (based on object equality)");
	}

	/**
	 * Return an {@link UnsupportedOperationException}
	 * @param cause original error not processed by a receiver.
	 * @return an {@link UnsupportedOperationException}
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
	 * @return a {@link RuntimeException} that can be checked via {@link #isCancel}
	 */
	public static RuntimeException failWithCancel() {
		return CANCEL_STACKTRACE ? new CancelException() : CancelException.INSTANCE;
	}

	/**
	 * Return an {@link IllegalStateException} indicating the receiver is overrun by
	 * more signals than expected in case of a bounded queue.
	 *
	 * @return an {@link IllegalStateException}
	 */
	public static IllegalStateException failWithOverflow() {
		return new OverflowException("The receiver is overrun by more signals than expected (bounded queue...)");
	}

	/**
	 * Return an {@link IllegalStateException} indicating the receiver is overrun by
	 * more signals than expected in case of a bounded queue.
	 *
	 * @param message the exception's message
	 * @return an {@link IllegalStateException}
	 */
	public static IllegalStateException failWithOverflow(String message) {
		return new OverflowException(message);
	}

	/**
	 * @return true if the given {@link Throwable} represents an {@link #failWithOverflow() overflow}.
	 */
	public static boolean isOverflow(Throwable t) {
		return t instanceof OverflowException;
	}

	/**
	 * Check if the given error is a bubbled wrapped exception.
	 *
	 * @param t the {@link Throwable} error to check
	 *
	 * @return true if given error is a a bubbled wrapped exception.
	 */
	public static boolean isBubbling(Throwable t) {
		return t instanceof BubblingException;
	}

	/**
	 * Check if the given error is a cancel signal.
	 *
	 * @param t the {@link Throwable} error to check
	 *
	 * @return true if given error is a cancellation token.
	 */
	public static boolean isCancel(Throwable t) {
		return t == CancelException.INSTANCE || t instanceof CancelException;
	}

	/**
	 * Return an {@link UnsupportedOperationException}
	 * @param cause original error not processed by a receiver.
	 * @return an {@link UnsupportedOperationException}
	 */
	public static boolean isErrorCallbackNotImplemented(Throwable cause) {
		return cause != null && cause.getClass().equals(ErrorCallbackNotImplemented
				.class);
	}

	/**
	 * @param elements the invalid requested demand
	 *
	 * @return a new {@link IllegalArgumentException} with a cause message abiding to
	 * reactive stream specification.
	 */
	public static IllegalArgumentException nullOrNegativeRequestException(long elements) {
		return new IllegalArgumentException(
				"Spec. Rule 3.9 - Cannot request a non strictly positive number: " + elements);
	}

	/**
	 * Return an unchecked {@link RuntimeException} to be thrown that will be propagated
	 * downstream through {@link org.reactivestreams.Subscriber#onError(Throwable)}.
	 * <p>This method invokes {@link #throwIfFatal(Throwable)}.
	 *
	 * @param t the root cause
	 *
	 * @return an unchecked exception
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
	 * varieties. These varieties are as follows: <ul> <li>{@code BubblingException}</li>
	 * <li>{@code ErrorCallbackNotImplemented}</li> <li>{@code VirtualMachineError}</li>
	 * <li>{@code ThreadDeath}</li> <li>{@code LinkageError}</li> </ul>
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
	 * <ul> <li>{@code VirtualMachineError}</li> <li>{@code ThreadDeath}</li>
	 * <li>{@code LinkageError}</li> </ul>
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
	 * Unwrap a particular {@code Throwable} only if it is a wrapped exception via
	 * {@link #bubble} or {@link #propagate}.
	 *
	 * @param t the exception to wrap
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

		public BubblingException(String message) {
			super(message);
		}

		public BubblingException(Throwable cause) {
			super(cause);
		}

		private static final long serialVersionUID = 2491425277432776142L;
	}

	/**
	 * An exception that is propagated downward through {@link org.reactivestreams.Subscriber#onError(Throwable)}
	 */
	static class ReactiveException extends RuntimeException {

		public ReactiveException(Throwable cause) {
			super(cause);
		}

		public ReactiveException(String message) {
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

		@SuppressWarnings("ThrowableInstanceNeverThrown")
		public static final CancelException INSTANCE = new CancelException();

		private CancelException() {
			super("The subscriber has denied dispatching");
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return CANCEL_STACKTRACE ? super.fillInStackTrace() : this;
		}

		private static final long serialVersionUID = 2491425227432776144L;

	}

	static final class OverflowException extends IllegalStateException {

		OverflowException(String s) {
			super(s);
		}
	}
}
