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

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Static Helpers to decorate an error with an associated data
 * <p>
 *
 * @see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>
 * @author Stephane Maldini
 * @since 2.0
 */
public enum Exceptions {
	;

	/**
	 * A singleton instance of a Throwable indicating a terminal state for exceptions, don't leak this!
	 */
	public static final Throwable TERMINATED = new Throwable("No further exceptions");

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
	 * @return a new {@link NullPointerException} with a cause message abiding to reactive stream specification.
	 */
	public static NullPointerException argumentIsNullException() {
		return new NullPointerException("Spec 2.13: Signal/argument cannot be null");
	}

	/**
	 * @return a new {@link IllegalStateException} with a cause message abiding to reactive stream specification.
	 */
	public static IllegalStateException duplicateOnSubscribeException() {
		return new IllegalStateException("Spec. Rule 2.12 - Subscriber.onSubscribe MUST NOT be called more than once (based on object equality)");
	}

	/**
	 * Return an unchecked
	 * {@link RuntimeException} to be thrown that will be propagated downstream through {@link org.reactivestreams.Subscriber#onError(Throwable)}
	 *
	 * @param t the root cause
	 * @return an unchecked exception
	 */
	public static RuntimeException wrap(Throwable t) {
		throwIfFatal(t);
		if(t instanceof DownstreamException){
			return (DownstreamException)t;
		}
		return new DownstreamException(t);
	}

	/**
	 * Return an unchecked {@link RuntimeException} to be thrown that will be propagated upstream
	 *
	 * @param t the root cause
	 * @return an unchecked exception that should choose bubbling up over error callback path
	 */
	public static RuntimeException wrapUpstream(Throwable t) {
		throwIfFatal(t);
		if(t instanceof UpstreamException){
			return (UpstreamException) t;
		}
		return new UpstreamException(t);
	}

	/**
	 * Return a {@link CancelException}
	 * @return a {@link CancelException}
	 */
	public static CancelException cancelException() {
		throw PlatformDependent.TRACE_CANCEL ? new CancelException() : CancelException.INSTANCE;
	}

	/**
	 * Return an {@link InsufficientCapacityException}
	 * @return an {@link InsufficientCapacityException}
	 */
	public static InsufficientCapacityException overflow() {
		return PlatformDependent.TRACE_NOCAPACITY ? new InsufficientCapacityException() :
				InsufficientCapacityException.INSTANCE;
	}

	/**
	 * @param elements the invalid requested demand
	 * @return a new {@link IllegalArgumentException} with a cause message abiding to reactive stream specification.
	 */
	public static IllegalArgumentException nullOrNegativeRequestException(long elements) {
		return new IllegalArgumentException("Spec. Rule 3.9 - Cannot request a non strictly positive number: " +
				elements);
	}

	/**
	 * An unexpected event is about to be dropped
	 *
	 * @param t the dropping data
	 */
	public static <T> void onNextDropped(T t) {
		if(t != null) {
			throw cancelException();
		}
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
	 * Atomic utility to safely mark a volatile throwable reference with a terminal marker.
	 *
	 * @param field the atomic container
	 * @param instance the reference instance
	 * @param <T> the instance type
	 * @return the previously masked throwable
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
	 * @param t the exception to evaluate
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
	 * @return a cached or new
	 * {@link TimeoutException} depending on whether {@link PlatformDependent#TRACE_TIMEROVERLOW} has been set.
	 */
	public static TimeoutException timeOverflow() {
		return TimerOverflowException.get();
	}

	/**
	 * Unwrap a particular {@code Throwable} only if it is a wrapped UpstreamException or DownstreamException
	 *
	 * @param t the exception to wrap
	 */
	public static Throwable unwrap(Throwable t) {
		if (t instanceof ReactiveException) {
			return t.getCause();
		}
		return t;
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


	static final class TimerOverflowException extends TimeoutException {

		public static final TimerOverflowException INSTANCE = new TimerOverflowException();

		public static TimerOverflowException get() {
			return PlatformDependent.TRACE_TIMEROVERLOW ? new TimerOverflowException() : INSTANCE;
		}

		private TimerOverflowException() {
			super("The subscriber has not requested for the timer signals, consider Flux#onBackpressureDrop or any" +
					"unbounded subscriber");
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return PlatformDependent.TRACE_TIMEROVERLOW ? super.fillInStackTrace() : this;
		}
	}
}
