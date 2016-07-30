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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Subscription;

/**
 * Global Reactor Core Exception handling and utils to operate on.
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @author Stephane Maldini
 */
public abstract class Exceptions {

	/**
	 *
	 */
	public static final boolean CANCEL_STACKTRACE =
			Boolean.parseBoolean(System.getProperty("reactor.trace.cancel", "false"));


	/**
	 * A singleton instance of a Throwable indicating a terminal state for exceptions, don't leak this!
	 */
	public static final Throwable TERMINATED = new Throwable("No further exceptions");

	volatile static boolean TRACE_OPERATOR_STACKTRACE =
			Boolean.parseBoolean(System.getProperty("reactor.trace.operatorStacktrace",
					"false"));

	/**
	 *
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
	 * Disable operator stack recorder.
	 */
	public static void disableOperatorStacktrace() {
		TRACE_OPERATOR_STACKTRACE = false;
	}

	/**
	 * @return a new {@link IllegalStateException} with a cause message abiding to reactive stream specification.
	 */
	public static IllegalStateException duplicateOnSubscribeException() {
		return new IllegalStateException("Spec. Rule 2.12 - Subscriber.onSubscribe MUST NOT be called more than once (based on object equality)");
	}

	/**
	 * Enable operator stack recorder. When a producer is declared, an "assembly tracker"
	 * operator is automatically added to capture declaration stack. Errors are observed
	 * and enriched with a Suppressed Exception detailing the original stack. Must be
	 * called before producers (e.g. Flux.map, Mono.fromCallable) are actually called to
	 * intercept the right stack information.
	 */
	public static void enableOperatorStacktrace() {
		TRACE_OPERATOR_STACKTRACE = true;
	}

	/**
	 * An exception that is propagated upward and considered as "fatal" as per Reactive Stream limited list of
	 * exceptions allowed to bubble. It is not meant to be common error resolution but might assist implementors in
	 * dealing with boundaries (queues, combinations and async).
	 * @return a {@link RuntimeException} that can be checked via {@link #isCancel}
	 */
	public static RuntimeException failWithCancel() {
		return CANCEL_STACKTRACE ? new CancelException() : CancelException.INSTANCE;
	}

	/**
	 * Return an {@link IllegalStateException}
	 * @return an {@link IllegalStateException}
	 */
	public static IllegalStateException failWithOverflow() {
		return new IllegalStateException("The receiver is overrun by more signals than " +
				"expected (bounded queue...)");
	}

	/**
	 * Check if the given error is a cancel signal.
	 * @param t the {@link Throwable} error to check
	 * @return true if given error is a cancellation token.
	 */
	public static boolean isCancel(Throwable t){
		return t == CancelException.INSTANCE || t instanceof CancelException;
	}

	/**
	 * Check if the given error is a bubbled wrapped exception.
	 * @param t the {@link Throwable} error to check
	 * @return true if given error is a a bubbled wrapped exception.
	 */
	public static boolean isBubbling(Throwable t){
		return t instanceof BubblingException;
	}

	/**
	 * When enabled, producer declaration stacks are recorded via an intercepting
	 * "assembly tracker" operator and added as Suppressed Exception if the source
	 * producer fails.
	 *
	 * @return a true if assembly tracking is enabled
	 */
	public static boolean isOperatorStacktraceEnabled() {
		return TRACE_OPERATOR_STACKTRACE;
	}

	/**
	 * Map an "operator" error. The
	 * result error will be passed via onError to the operator downstream after
	 * checking for fatal error via
	 * {@link #throwIfFatal(Throwable)}.
	 *
	 * @param error the callback or operator error
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable mapOperatorError(Throwable error) {
		return mapOperatorError(null, error);
	}
	/**
	 * Map an "operator" error given an operator parent {@link Subscription}. The
	 * result error will be passed via onError to the operator downstream.
	 * {@link Subscription} will be cancelled after checking for fatal error via
	 * {@link #throwIfFatal(Throwable)}.
	 *
	 * @param subscription the linked operator parent {@link Subscription}
	 * @param error the callback or operator error
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable mapOperatorError(Subscription subscription, Throwable error) {
		Exceptions.throwIfFatal(error);
		if(subscription != null) {
			subscription.cancel();
		}
		return unwrap(error);
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
	 * <li>{@code BubblingException}</li>
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
	 * Unwrap a particular {@code Throwable} only if it is a wrapped exception via
	 * {@link #bubble} or {@link #propagate}.
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
	 * Wrap a {@link ThrowingFunction} with a {@link Function} that will wrap and {@link #propagate(Throwable)}
	 * any exceptions thrown by the {@link ThrowingFunction}.  Useful when using existing methods which throw
	 * checked exceptions to avoid try/catch blocks and improve readability.
	 *
	 * {@code flux.map(Exceptions.wrap(v -> legacyMethod()}.subscribe(Subscribers.unbounded())}
	 *
	 * @param function the function that throws a checked exception
	 * @param <T> the type of the input to the function
	 * @param <V> the type of the result of the function
	 * @return function that wraps and {@link #propagate(Throwable)}'s checked exceptions
	 */
	public static <T, V> Function<T, V> wrap(ThrowingFunction<T, V> function) {
		return t -> function.apply(t);
	}

	public static <T> Consumer<T> wrap(ThrowingConsumer<T> consumer) {
		return t -> consumer.accept(t);
	}

	public static <T> Predicate<T> wrap(ThrowingPredicate<T> predicate) {
		return t -> predicate.test(t);
	}

	public static <T> Supplier<T> wrap(ThrowingSupplier<T> supplier) {
		return () -> supplier.get();
	}

	Exceptions(){}

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
			return getCause() != null ? getCause().fillInStackTrace() : super.fillInStackTrace();
		}
		private static final long serialVersionUID = 2491425227432776143L;
	}

	/**
	 * An error signal from downstream subscribers consuming data when their state is denying any additional event.
	 *
	 * @author Stephane Maldini
	 */
	static final class CancelException extends BubblingException {

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

	@FunctionalInterface
	public interface ThrowingFunction<T, V> extends Function<T, V> {

		@Override
		default V apply(T event) {
			try {
				return applyThrows(event);
			} catch (Throwable t) {
				throw propagate(t);
			}
		}

		V applyThrows(T t) throws Throwable;
	}

	@FunctionalInterface
	public interface ThrowingConsumer<T> extends Consumer<T> {

		@Override
		default void accept(T event) {
			try {
				acceptThrows(event);
			} catch (Throwable t) {
				throw propagate(t);
			}
		}

		void acceptThrows(T t) throws Throwable;
	}

	@FunctionalInterface
	public interface ThrowingPredicate<T> extends Predicate<T> {

		@Override
		default boolean test(T event) {
			try {
				return testThrows(event);
			} catch (Throwable t) {
				throw propagate(t);
			}
		}

		boolean testThrows(T t) throws Throwable;
	}

	@FunctionalInterface
	public interface ThrowingSupplier<T> extends Supplier<T> {

		@Override
		default T get() {
			try {
				return getThrows();
			} catch (Throwable t) {
				throw propagate(t);
			}
		}

		T getThrows() throws Throwable;
	}

}
