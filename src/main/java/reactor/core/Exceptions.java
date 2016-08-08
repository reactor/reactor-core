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
import java.util.function.BiFunction;
import java.util.function.Consumer;

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
	 * When default global {@link #onOperatorError} transforms operator errors driven
	 * by a data signal, it wraps the value signal into a suppressed exception that can
	 * be resolved back via this method. Any override of this strategy via
	 * {@link #setOnOperatorErrorHook(BiFunction)} will defeat this util.
	 *
	 * @param error the error that might contain a value of the given type
	 * @param typeExpectation the expected value type
	 * @param <T> the refied value type
	 *
	 * @return a value found associated with error matching the given type
	 * or {@literal null}
	 */
	public static <T> T findValueCause(Throwable error, Class<T> typeExpectation){
		Objects.requireNonNull(error, "error");
		Objects.requireNonNull(typeExpectation, "typeExpectation");
		for(Throwable s : error.getSuppressed()){
			if(s instanceof ValueCause && typeExpectation.isAssignableFrom(((ValueCause)
					s).value.getClass())){

				@SuppressWarnings("unchecked")
				T v = (T) ((ValueCause)s).value;
				return v;
			}
		}
		return null;
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
		Consumer<? super Throwable> hook = onErrorDroppedHook;
		if (hook == null) {
			throw bubble(e);
		}
		hook.accept(e);
	}

	/**
	 * An unexpected event is about to be dropped.
	 *
	 * @param <T> the dropped value type
	 * @param t the dropping data
	 */
	public static <T> void onNextDropped(T t) {
		if(t != null) {
			Consumer<Object> hook = onNextDroppedHook;
			if (hook == null) {
				throw failWithCancel();
			}
			hook.accept(t);
		}
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
	public static Throwable onOperatorError(Throwable error) {
		return onOperatorError(null, error, null);
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
	public static Throwable onOperatorError(Subscription subscription, Throwable error) {
		return onOperatorError(subscription, error, null);
	}


	/**
	 * Map an "operator" error given an operator parent {@link Subscription}. The
	 * result error will be passed via onError to the operator downstream.
	 * {@link Subscription} will be cancelled after checking for fatal error via
	 * {@link #throwIfFatal(Throwable)}.
	 *
	 * @param subscription the linked operator parent {@link Subscription}
	 * @param error the callback or operator error
	 * @param dataSignal the value (onNext or onError) signal processed during failure
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable onOperatorError(Subscription subscription, Throwable
			error, Object dataSignal) {

		Exceptions.throwIfFatal(error);
		if(subscription != null) {
			subscription.cancel();
		}

		Throwable t = unwrap(error);
		BiFunction<? super Throwable, Object, ? extends Throwable> hook =
				onOperatorErrorHook;
		if (hook == null) {
			if (dataSignal != null) {
				if (dataSignal instanceof Throwable) {
					t.addSuppressed((Throwable) dataSignal);
				}
				else {
					t.addSuppressed(new ValueCause(dataSignal));
				}
			}
			return t;
		}
		return hook.apply(error, dataSignal);
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
	 * Reset global error dropped strategy to bubbling back the error.
	 */
	public static void resetOnErrorDroppedHook() {
		onErrorDroppedHook = null;
	}

	/**
	 * Reset global data dropped strategy to throwing via {@link #failWithCancel()}
	 */
	public static void resetOnNextDroppedHook() {
		onNextDroppedHook = null;
	}

	/**
	 * Reset global operator error mapping to adding as suppressed
	 * exception either data driven exception to fetch via {@link #findValueCause} or
	 * error driven exception.
	 */
	public static void resetOnOperatorErrorHook() {
		onOperatorErrorHook = null;
	}

	/**
	 * Override global error dropped strategy which by default bubble back the error.
	 *
	 * @param c the dropped error {@link Consumer} hook
	 */
	public static void setOnErrorDroppedHook(Consumer<? super Throwable> c) {
		onErrorDroppedHook = Objects.requireNonNull(c, "onErrorDroppedHook");
	}

	/**
	 * Override global data dropped strategy which by default throw {@link #failWithCancel()}
	 *
	 * @param c the dropped next {@link Consumer} hook
	 */
	public static void setOnNextDroppedHook(Consumer<Object> c) {
		onNextDroppedHook = Objects.requireNonNull(c, "onENextDroppedHook");
	}

	/**
	 * Override global operator error mapping which by default add as suppressed
	 * exception either data driven exception to fetch via {@link #findValueCause} or
	 * error driven exception.
	 *
	 * @param f the operator error {@link BiFunction} mapper, given the failure and an
	 * eventual original context (data or error) and returning an arbitrary exception.
	 */
	public static void setOnOperatorErrorHook(BiFunction<? super Throwable, Object, ?
			extends Throwable> f) {
		onOperatorErrorHook = Objects.requireNonNull(f, "onOperatorErrorHook");
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

	volatile static boolean TRACE_OPERATOR_STACKTRACE =
			Boolean.parseBoolean(System.getProperty("reactor.trace.operatorStacktrace",
					"false"));

	volatile static Consumer<? super Throwable> onErrorDroppedHook;
	volatile static Consumer<Object>            onNextDroppedHook;
	volatile static BiFunction<? super Throwable, Object, ? extends Throwable>
	                                            onOperatorErrorHook;

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

	static final class ValueCause extends Exception {

		final Object value;

		public ValueCause(Object value) {
			this.value = value;
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return this;
		}

		@Override
		public String getMessage() {
			return "Associated value: " + value.toString();
		}

		private static final long serialVersionUID = 2491425227432776145L;
	}
}
