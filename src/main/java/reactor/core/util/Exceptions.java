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
package reactor.core.util;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Static Helpers to decorate an error with an associated data
 * <p>
 *
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public enum Exceptions {
	;

	/**
	 * A singleton instance of a Throwable indicating a terminal state for exceptions, don't leak this!
	 */
	public static final Throwable TERMINATED = new Throwable("No further exceptions");
	private static final int MAX_DEPTH = 25;

	/**
	 * Adds a {@code Throwable} to a causality-chain of Throwables, as an additional cause (if it does not
	 * already appear in the chain among the causes).
	 *
	 * @param e     the {@code Throwable} at the head of the causality chain
	 * @param cause the {@code Throwable} you want to add as a cause of the chain
	 */
	public static void addCause(Throwable e, Throwable cause) {
		Set<Throwable> seenCauses = new HashSet<Throwable>();

		int i = 0;
		while (e.getCause() != null) {
			if (i++ >= MAX_DEPTH) {
				// stack too deep to associate cause
				return;
			}
			e = e.getCause();
			if (seenCauses.contains(e.getCause())) {
				break;
			} else {
				seenCauses.add(e.getCause());
			}
		}
		// we now have 'e' as the last in the chain
		try {
			e.initCause(cause);
		} catch (Throwable t) {
			// ignore
			// the javadocs say that some Throwables (depending on how they're made) will never
			// let me call initCause without blowing up even if it returns null
		}
	}
	/**
	 * Signal a desynchronization of demand and timer
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
	 * Adds the given item as the final cause of the given {@code Throwable}, wrapped in {@code OnNextValue}
	 * (which extends {@code RuntimeException}).
	 *
	 * @param e     the {@link Throwable} to which you want to add a cause
	 * @param value the item you want to add to {@code e} as the cause of the {@code Throwable}
	 * @return the same {@code Throwable} ({@code e}) that was passed in, with {@code value} added to it as a
	 * cause
	 */
	public static Throwable addValueAsLastCause(Throwable e, Object value) {
		Throwable lastCause = Exceptions.getFinalCause(e);
		if (lastCause != null && lastCause instanceof ValueCauseException) {
			// purposefully using == for object reference check
			if (((ValueCauseException) lastCause).getValue() == value) {
				// don't add another
				return e;
			}
		}
		Exceptions.addCause(e, new ValueCauseException(value));
		return e;
	}

	/**
	 *
	 * @return
	 */
	public static ArgumentIsNullException argumentIsNullException() {
		return new ArgumentIsNullException();
	}

	/**
	 *
	 * @return
	 */
	public static DuplicateOnSubscribeException duplicateOnSubscribeException() {
		return new DuplicateOnSubscribeException();
	}

	/**
	 * Throw an unchecked
	 * {@link RuntimeException} that will be propagated downstream through {@link org.reactivestreams.Subscriber#onError(Throwable)}
	 *
	 * @param t the root cause
	 */
	public static void fail(Throwable t) {
		throwIfFatal(t);
		throw wrapDownstream(t);
	}

	/**
	 * Throw an unchecked {@link RuntimeException} that will be propagated upstream
	 *
	 * @param t the root cause
	 */
	public static void failUpstream(Throwable t) {
		throwIfFatal(t);
		throw wrapUpstream(t);
	}

	/**
	 * Throw a {@link CancelException}
	 */
	public static void failWithCancel() {
		throw PlatformDependent.TRACE_CANCEL ? new CancelException() : CancelException.INSTANCE;
	}

	/**
	 * Throw a {@link InsufficientCapacityException}
	 */
	public static void failWithOverflow() {
		throw PlatformDependent.TRACE_NOCAPACITY ? new InsufficientCapacityException() :
				InsufficientCapacityException.INSTANCE;
	}

	/**
	 * Get the {@code Throwable} at the end of the causality-chain for a particular {@code Throwable}
	 *
	 * @param e the {@code Throwable} whose final cause you are curious about
	 * @return the last {@code Throwable} in the causality-chain of {@code e} (or a "Stack too deep to get
	 * final cause" {@code RuntimeException} if the chain is too long to traverse)
	 */
	public static Throwable getFinalCause(Throwable e) {
		int i = 0;
		while (e.getCause() != null) {
			if (i++ >= MAX_DEPTH) {
				// stack too deep to get final cause
				return new RuntimeException("Stack too deep to get final cause");
			}
			e = e.getCause();
		}
		return e;
	}

	/**
	 * Try to find the last value at the end of the causality-chain for a particular {@code Throwable}
	 * If the final cause wasn't of type {@link ValueCauseException},
	 * return null;
	 *
	 * @param e the {@code Throwable} whose final cause you are curious about
	 * @return the last {@code Throwable} in the causality-chain of {@code e} (or a "Stack too deep to get
	 * final cause" {@code RuntimeException} if the chain is too long to traverse)
	 */
	@SuppressWarnings("unchecked")
	public static Object getFinalValueCause(Throwable e) {
		Throwable t = getFinalCause(e);
		if (ValueCauseException.class.isAssignableFrom(t.getClass())) {
			return ((ValueCauseException) t).getValue();
		}
		return null;
	}

	/**
	 *
	 * @param elements
	 * @return
	 */
	public static NullOrNegativeRequestException nullOrNegativeRequestException(long elements) {
		return new NullOrNegativeRequestException(elements);
	}

	/**
	 * Take an unsignalled exception that is masking anowher one due to callback failure.
	 *
	 * @param e the exception to handle
	 */
	public static void onErrorDropped(Throwable e, Throwable root) {
		if(root != null) {
			e.addSuppressed(root);
		}
		onErrorDropped(e);
	}

	/**
	 * Take an unsignalled exception that is masking anowher one due to callback failure.
	 *
	 * @param e the exception to handle
	 */
	public static void onErrorDropped(Throwable e) {
		failUpstream(e);
	}

	/**
	 * An unexpected event is about to be dropped
	 *
	 * @param t
	 */
	public static <T> void onNextDropped(T t) {
		failWithCancel();
	}

	/**
	 * Throws the exception if it is a regular runtimeException or wraps
	 * it into a ReactiveException.
	 * <p>
	 * Use unwrap to get back the original cause.
	 * <p>
	 * The method calls throwIfFatal().
	 *
	 * @param e the exception to propagate
	 * @return dummy return type to allow using throw with the function call
	 */
	public static RuntimeException propagate(Throwable e) {
		throwIfFatal(e);
		if (e instanceof RuntimeException) {
			throw (RuntimeException)e;
		}
		throw new UpstreamException(e);
	}

	/**
	 *
	 * @param field
	 * @param instance
	 * @param <T>
	 * @return
	 */
	public static <T> Throwable terminate(AtomicReferenceFieldUpdater<T, Throwable> field, T instance) {
		Throwable current = field.get(instance);
		if (current != TERMINATED) {
			current = field.getAndSet(instance, TERMINATED);
		}
		return current;
	}

	/**
	 * Throws a particular {@code Throwable} only if it belongs to a set of "fatal" error varieties. These
	 * varieties are as follows:
	 * <ul>
	 * <li>{@link UpstreamException}</li>
	 * <li>{@code StackOverflowError}</li>
	 * <li>{@code VirtualMachineError}</li>
	 * <li>{@code ThreadDeath}</li>
	 * <li>{@code LinkageError}</li>
	 * </ul>
	 *
	 * @param t
	 */
	public static void throwIfFatal(Throwable t) {
		if (t instanceof UpstreamException) {
			throw (UpstreamException) t;
		} else if (t instanceof StackOverflowError) {
			throw (StackOverflowError) t;
		} else if (t instanceof VirtualMachineError) {
			throw (VirtualMachineError) t;
		} else if (t instanceof ThreadDeath) {
			throw (ThreadDeath) t;
		} else if (t instanceof LinkageError) {
			throw (LinkageError) t;
		}
	}

	/**
	 * Unwrap a particular {@code Throwable} only if it is a wrapped UpstreamException or DownstreamException
	 *
	 * @param t
	 */
	public static Throwable unwrap(Throwable t) {
		if (t instanceof ReactiveException) {
			return t.getCause();
		}
		return t;
	}

	/**
	 * Return an unchecked {@link RuntimeException} that will be propagated upstream
	 *
	 * @param t the root cause
	 */
	public static RuntimeException wrapDownstream(Throwable t) {
		if(t instanceof DownstreamException){
			return (DownstreamException)t;
		}
		return new DownstreamException(t);
	}

	/**
	 * Return an unchecked {@link RuntimeException} that will be propagated upstream
	 *
	 * @param t the root cause
	 */
	public static RuntimeException wrapUpstream(Throwable t) {
		if(t instanceof UpstreamException){
			return (UpstreamException) t;
		}
		return new UpstreamException(t);
	}

	public static final class TimerOverflowException extends java.util.concurrent.TimeoutException {

		public static final TimerOverflowException INSTANCE = new TimerOverflowException();

		public static TimerOverflowException get() {
			return PlatformDependent.TRACE_TIMEROVERLOW ? new TimerOverflowException() : INSTANCE;
		}

		private TimerOverflowException() {
			super("The subscriber has not requested for the timer signals, consider Stream#onBackpressureDrop or any " +
					"unbounded subscriber");
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return PlatformDependent.TRACE_TIMEROVERLOW ? super.fillInStackTrace() : this;
		}
	}

	/**
	 *
	 */
	public static final class NullOrNegativeRequestException extends IllegalArgumentException {

		NullOrNegativeRequestException(long elements) {
			super("Spec. Rule 3.9 - Cannot request a non strictly positive number: " +
			  elements);
		}
	}

	/**
	 *
	 */
	public static final class ArgumentIsNullException extends NullPointerException {

		ArgumentIsNullException() {
			super("Spec 2.13: Signal/argument cannot be null");
		}
	}

	/**
	 *
	 */
	public static final class DuplicateOnSubscribeException extends IllegalStateException {

		DuplicateOnSubscribeException() {
			super("Spec. Rule 2.12 - Subscriber.onSubscribe MUST NOT be called more than once" +
			" " +
			  "(based on object equality)");
		}
	}

	/**
	 * An exception helper for lambda and other checked-to-unchecked exception wrapping
	 */
	public static class ReactiveException extends RuntimeException {

		public ReactiveException(Throwable cause) {
			super(cause);
		}
		public ReactiveException(String message) {
			super(message);
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return getCause() != null ? getCause().fillInStackTrace() : super.fillInStackTrace();
		}

	}
	/**
	 * An exception that is propagated upward and considered as "fatal" as per Reactive Stream limited list of
	 * exceptions allowed to bubble. It is not meant to be common error resolution but might assist implementors in
	 * dealing with boundaries (queues, combinations and async).
	 */
	public static class UpstreamException extends ReactiveException {

		public static final UpstreamException INSTANCE = new UpstreamException("Uncaught exception");
		private static final long serialVersionUID = 2491425277432776142L;

		public static UpstreamException instance() {
			return INSTANCE;
		}

		public UpstreamException(String message) {
			super(message);
		}

		public UpstreamException(Throwable cause) {
			super(cause);
		}

	}
	/**
	 * An exception that is propagated downward through {@link org.reactivestreams.Subscriber#onError(Throwable)}
	 */
	public static class DownstreamException extends ReactiveException {

		private static final long serialVersionUID = 2491425227432776143L;

		public DownstreamException(Throwable cause) {
			super(cause);
		}
	}
	/**
	 * Used to alert consumers waiting with a {@link WaitStrategy} for status changes.
	 * <p>
	 * It does not fill in a stack trace for performance reasons.
	 */
	@SuppressWarnings("serial")
	public static final class AlertException extends RuntimeException {

		/** Pre-allocated exception to avoid garbage generation */
		public static final AlertException INSTANCE = new AlertException();

		/**
		 * Private constructor so only a single instance exists.
		 */
		private AlertException() {
		}

		/**
		 * Overridden so the stack trace is not filled in for this exception for performance reasons.
		 *
		 * @return this instance.
		 */
		@Override
		public Throwable fillInStackTrace() {
			return this;
		}

	}
	/**
	 * An error signal from downstream subscribers consuming data when their state is denying any additional event.
	 *
	 * @author Stephane Maldini
	 */
	public static final class CancelException extends UpstreamException {

		public static final CancelException INSTANCE = new CancelException();
		private static final long serialVersionUID = 2491425227432776144L;

		private CancelException() {
			super("The subscriber has denied dispatching");
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return PlatformDependent.TRACE_CANCEL ? super.fillInStackTrace() : this;
		}

	}
	/**
	 * <p>Exception thrown when the it is not possible to dispatch a signal due to insufficient capacity.
	 *
	 * @author Stephane Maldini
	 */
	@SuppressWarnings("serial")
	public static final class InsufficientCapacityException extends RuntimeException {

		private static final long serialVersionUID = 2491425227432776145L;

		private static final InsufficientCapacityException INSTANCE = new InsufficientCapacityException();

		private InsufficientCapacityException() {
			super("The subscriber is overrun by more signals than expected (bounded queue...)");
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return PlatformDependent.TRACE_NOCAPACITY ? super.fillInStackTrace() : this;
		}

	}

	/**
	 * Represents an error that was encountered while trying to emit an item from an Observable, and
	 * tries to preserve that item for future use and/or reporting.
	 */

	public static class ValueCauseException extends RuntimeException {

		private static final long serialVersionUID = -3454462756050397899L;

		/**
		 * Render the object if it is a basic type. This avoids the library making potentially expensive
		 * or calls to toString() which may throw exceptions.
		 *
		 * @param value the item that the Observable was trying to emit at the time of the error
		 * @return a string version of the object if primitive, otherwise the classname of the object
		 */
		private static String renderValue(Object value) {
			if (value == null) {
				return "null";
			}
			if (value.getClass().isPrimitive()) {
				return value.toString();
			}
			if (value instanceof String) {
				return (String) value;
			}
			if (value instanceof Enum) {
				return ((Enum) value).name();
			}
			return value.getClass().getName() + ".class : " + value;
		}
		private final Object value;

		/**
		 * Create a {@code CauseValue} error and include in its error message a string representation of
		 * the item that was intended to be emitted at the time the error was handled.
		 *
		 * @param value the item that the component was trying to emit at the time of the error
		 */
		public ValueCauseException(Object value) {
			super("Exception while signaling value: " + renderValue(value));
			this.value = value;
		}

		/**
		 * Retrieve the item that the component was trying to emit at the time this error occurred.
		 *
		 * @return the item that the component was trying to emit at the time of the error
		 */
		public Object getValue() {
			return value;
		}

	}
}
