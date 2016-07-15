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
package reactor.util;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.Reactor;

/**
 * Static Helpers to decorate an error with an associated data
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @author Stephane Maldini
 */
public abstract class Exceptions {

	/**
	 * A singleton instance of a Throwable indicating a terminal state for exceptions, don't leak this!
	 */
	public static final Throwable TERMINATED = new Throwable("No further exceptions");

	/**
	 * Signal a desynchronization of demand and timer.
	 * @param <T> the parent instance type
	 * @param field the target field updater
	 * @param instance the parent instance for the field
	 * @param exception the Throwable to add.
	 * @return true if added, false if the field contained the {@link #TERMINATED} instance.
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
	 * Return an unchecked {@link RuntimeException} to be thrown that will be propagated
	 * downstream through {@link org.reactivestreams.Subscriber#onError(Throwable)}.
	 * <p>This method invokes {@link #throwIfFatal(Throwable)}.
	 *
	 * @param t the root cause
	 * @return an unchecked exception
	 */
	public static RuntimeException propagate(Throwable t) {
		throwIfFatal(t);
		if(t instanceof RuntimeException){
			return (RuntimeException)t;
		}
		return new ReactiveException(t);
	}

	/**
	 * Return an unchecked {@link RuntimeException} to be thrown that will bubble upstream.
	 * <p>This method invokes {@link #throwIfFatal(Throwable)}.
	 *
	 * @param t the root cause
	 * @return an unchecked exception that should choose bubbling up over error callback path
	 */
	public static RuntimeException bubble(Throwable t) {
		throwIfFatal(t);
		return new BubblingException(t);
	}

	/**
	 * Return a {@link CancelException}
	 * @return a {@link CancelException}
	 */
	public static CancelException failWithCancel() {
		return Reactor.TRACE_CANCEL ? new CancelException() : CancelException.INSTANCE;
	}

	/**
	 * Return an {@link InsufficientCapacityException}
	 * @return an {@link InsufficientCapacityException}
	 */
	public static InsufficientCapacityException failWithOverflow() {
		return Reactor.TRACE_NOCAPACITY ? new InsufficientCapacityException() :
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
	 * Take an unsignalled exception that is masking anowher one due to callback failure.
	 *
	 * @param e the exception to handle
	 * @param root the optional root cause to suppress
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
		throw bubble(e);
	}

	/**
	 * An unexpected event is about to be dropped.
	 *
	 * @param <T> the dropped value type
	 * @param t the dropping data
	 */
	public static <T> void onNextDropped(T t) {
		if(t != null) {
			throw failWithCancel();
		}
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
	 * <li>{@link BubblingException}</li>
	 * <li>{@code StackOverflowError}</li>
	 * <li>{@code VirtualMachineError}</li>
	 * <li>{@code ThreadDeath}</li>
	 * <li>{@code LinkageError}</li>
	 * </ul>
	 *
	 * @param t the exception to evaluate
	 */
	public static void throwIfFatal(Throwable t) {
		if (t instanceof BubblingException) {
			throw (BubblingException) t;
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
	 * Unwrap a particular {@code Throwable} only if it is a wrapped BubblingException or DownstreamException
	 *
	 * @param t the exception to wrap
	 * @return the unwrapped exception
	 */
	public static Throwable unwrap(Throwable t) {
		Throwable _t = t;
		while(_t instanceof ReactiveException) {
			_t = _t.getCause();
		}
		return _t;
	}

	/**
	 * An exception that is propagated upward and considered as "fatal" as per Reactive Stream limited list of
	 * exceptions allowed to bubble. It is not meant to be common error resolution but might assist implementors in
	 * dealing with boundaries (queues, combinations and async).
	 */
	public static class BubblingException extends ReactiveException {
		private static final long serialVersionUID = 2491425277432776142L;

		public BubblingException(String message) {
			super(message);
		}

		public BubblingException(Throwable cause) {
			super(cause);
		}
	}

	/**
	 * An exception that is propagated downward through {@link org.reactivestreams.Subscriber#onError(Throwable)}
	 */
	static class ReactiveException extends RuntimeException {
		private static final long serialVersionUID = 2491425227432776143L;

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
	 * An error signal from downstream subscribers consuming data when their state is denying any additional event.
	 *
	 * @author Stephane Maldini
	 */
	public static final class CancelException extends BubblingException {

		public static final CancelException INSTANCE = new CancelException();

		private static final long serialVersionUID = 2491425227432776144L;
		private CancelException() {
			super("The subscriber has denied dispatching");
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return Reactor.TRACE_CANCEL ? super.fillInStackTrace() : this;
		}

	}

	/**
	 * <p>Exception thrown when the it is not possible to dispatch a signal due to insufficient capacity.
	 *
	 * @author Stephane Maldini
	 */
	public static final class InsufficientCapacityException extends RuntimeException {

		private static final long serialVersionUID = 2491425227432776145L;

		private static final InsufficientCapacityException INSTANCE = new InsufficientCapacityException();

		private InsufficientCapacityException() {
			super("The subscriber is overrun by more signals than expected (bounded queue...)");
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return Reactor.TRACE_NOCAPACITY ? super.fillInStackTrace() : this;
		}

	}

	Exceptions(){}
}
