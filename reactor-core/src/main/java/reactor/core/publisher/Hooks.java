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

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.reactivestreams.Publisher;
import reactor.util.Logger;
import reactor.util.Loggers;


/**
 * A push of overridable lifecycle hooks that can be used for cross-cutting
 * added behavior on {@link Flux}/{@link Mono} operators.
 */
public abstract class Hooks {

	/**
	 * Configure a {@link Publisher} operator interceptor for each operator created
	 * ({@link Flux} or {@link Mono}). The passed function applies to the original
	 * operator {@link Publisher} and return an eventually intercepted {@link Publisher}
	 * <p>
	 * Can be reset via {@link #resetOnEachOperator()}
	 *
	 * @param onOperator a function to intercept each operation call e.g. {@code map
	 * (fn)} and {@code map(fn2)} in {@code flux.map(fn).map(fn2).subscribe()}
	 *
	 * @see #onLastOperator(Function)
	 */
	public static void onEachOperator(Function<? super Publisher<Object>, ? extends Publisher<Object>> onOperator) {
		Objects.requireNonNull(onOperator, "onEachOperator");
		if (log.isDebugEnabled()) {
			log.debug("Hooking new default : onEachOperator");
		}

		synchronized (log) {
			onEachOperatorHook = createOrUpdateOpHook(onEachOperatorHook, onOperator);
		}
	}

	/**
	 * Override global error dropped strategy which by default bubble back the error.
	 *
	 * @param c the {@link Consumer} to apply to dropped errors
	 */
	public static void onErrorDropped(Consumer<? super Throwable> c) {
		Objects.requireNonNull(c, "onErrorDroppedHook");
		if(log.isDebugEnabled()) {
			log.debug("Hooking new default : onErrorDropped");
		}

		synchronized(log) {
			if (onErrorDroppedHook != null) {
				@SuppressWarnings("unchecked") Consumer<Throwable> _c =
						((Consumer<Throwable>)onErrorDroppedHook).andThen(c);
				onErrorDroppedHook = _c;
			}
			else {
				onErrorDroppedHook = c;
			}
		}
	}

	/**
	 * Configure a {@link Publisher} operator interceptor for the last operator created
	 * in every flow ({@link Flux} or {@link Mono}). The passed function applies
	 * to the original
	 * operator {@link Publisher} and return an eventually intercepted {@link Publisher}
	 * <p>
	 * Can be reset via {@link #resetOnLastOperator()}}
	 *
	 * @param onOperator a function to intercept last operation call e.g. {@code map(fn2)}
	 * in {@code flux.map(fn).map(fn2).subscribe()}
	 *
	 * @see #onEachOperator(Function)
	 */
	public static void onLastOperator(Function<? super Publisher<Object>, ? extends Publisher<Object>> onOperator) {
		Objects.requireNonNull(onOperator, "onLastOperator");
		if (log.isDebugEnabled()) {
			log.debug("Hooking new default : onLastOperator");
		}

		synchronized (log) {
			onLastOperatorHook = createOrUpdateOpHook(onLastOperatorHook, onOperator);
		}
	}

	/**
	 * Override global data dropped strategy which by default throw {@link
	 * reactor.core.Exceptions#failWithCancel()}
	 *
	 * @param c the {@link Consumer} to apply to data (onNext) that is dropped
	 */
	public static void onNextDropped(Consumer<Object> c) {
		Objects.requireNonNull(c, "onNextDroppedHook");
		if(log.isDebugEnabled()) {
			log.debug("Hooking new default : onNextDropped");
		}

		synchronized(log) {
			if (onNextDroppedHook != null) {
				onNextDroppedHook = onNextDroppedHook.andThen(c);
			}
			else {
				onNextDroppedHook = c;
			}
		}
	}

	/**
	 * Enable operator stack recorder that captures a declaration stack whenever an
	 * operator is instantiated. When errors are observed later on, they will be
	 * enriched with a Suppressed Exception detailing the original assembly line stack.
	 * Must be called before producers (e.g. Flux.map, Mono.fromCallable) are actually
	 * called to intercept the right stack information.
	 *
	 * @see #onEachOperator(Function)
	 */
	public static void onOperatorDebug() {
		if (log.isDebugEnabled()) {
			log.debug("Enabling stacktrace debugging via onOperatorDebug");
		}
		onEachOperator(OnOperatorDebug.instance());
	}

	/**
	 * Override global operator error mapping which by default add as suppressed exception
	 * either data driven exception or error driven exception.
	 *
	 * @param f an operator error {@link BiFunction} mapper, returning an arbitrary exception
	 * given the failure and optionally some original context (data or error).
	 */
	public static void onOperatorError(BiFunction<? super Throwable, Object, ? extends Throwable> f) {
		Objects.requireNonNull(f, "onOperatorErrorHook");
		if (log.isDebugEnabled()) {
			log.debug("Hooking new default : onOperatorError");
		}
		synchronized (log) {
			if (onOperatorErrorHook != null) {
				BiFunction<? super Throwable, Object, ? extends Throwable> ff =
						onOperatorErrorHook;
				onOperatorErrorHook = (e, data) -> f.apply(ff.apply(e, data), data);
			}
			else {
				onOperatorErrorHook = f;
			}
		}
	}

	/**
	 * Reset global "assembly" hook tracking
	 */
	public static void resetOnEachOperator() {
		if (log.isDebugEnabled()) {
			log.debug("Reset to factory defaults : onEachOperator");
		}
		synchronized (log) {
			onEachOperatorHook = null;
		}
	}

	/**
	 * Reset global error dropped strategy to bubbling back the error.
	 */
	public static void resetOnErrorDropped() {
		if(log.isDebugEnabled()) {
			log.debug("Reset to factory defaults : onErrorDropped");
		}
		synchronized (log) {
			onErrorDroppedHook = null;
		}
	}

	/**
	 * Reset global data dropped strategy to throwing via {@link
	 * reactor.core.Exceptions#failWithCancel()}
	 */
	public static void resetOnNextDropped() {
		if(log.isDebugEnabled()) {
			log.debug("Reset to factory defaults : onNextDropped");
		}
		synchronized (log) {
			onNextDroppedHook = null;
		}
	}

	/**
	 * Reset global "subscriber" hook tracking
	 */
	public static void resetOnLastOperator() {
		if (log.isDebugEnabled()) {
			log.debug("Reset to factory defaults : onLastOperator");
		}
		synchronized (log) {
			onLastOperatorHook = null;
		}
	}

	/**
	 * Reset global operator debug. Will have no effect if more than one
	 * {@link #onEachOperator(Function)} has been invoked, including {@link #onOperatorDebug()}
	 */
	public static void resetOnOperatorDebug() {
		if(log.isDebugEnabled()) {
			log.debug("Reset to factory defaults : onOperatorError");
		}
		synchronized (log) {
			if(onEachOperatorHook == OnOperatorDebug.INSTANCE) {
				onEachOperatorHook = null;
			}
		}
	}

	/**
	 * Reset global operator error mapping to adding as suppressed exception.
	 */
	public static void resetOnOperatorError() {
		if(log.isDebugEnabled()) {
			log.debug("Reset to factory defaults : onOperatorError");
		}
		synchronized (log) {
			onOperatorErrorHook = null;
		}
	}


	@SuppressWarnings("unchecked")
	static <T> Function<Publisher, Publisher> createOrUpdateOpHook(
		@Nullable Function<? super Publisher, ? extends Publisher> current,
			Function<? super Publisher<T>, ? extends Publisher<T>> onOperator
	) {
		Function<? super Publisher, ? extends Publisher> op = (Function<? super Publisher,
		? extends Publisher>)
				onOperator;
		if (current == op) {
			return (Function<Publisher, Publisher>)current;
		}
		if (current != null) {
			return (Function<Publisher, Publisher>)current.andThen(op);
		}
		else {
			return (Function<Publisher, Publisher>)op;
		}
	}

	static volatile Function<Publisher, Publisher> onEachOperatorHook;
	static volatile Function<Publisher, Publisher> onLastOperatorHook;

	static volatile Consumer<? super Throwable>                      onErrorDroppedHook;
	static volatile Consumer<Object>                                 onNextDroppedHook;

	static volatile BiFunction<? super Throwable, Object, ? extends Throwable>
	                                                                 onOperatorErrorHook;

	static {
		boolean globalTrace =
				Boolean.parseBoolean(System.getProperty("reactor.trace.operatorStacktrace",
						"false"));

		if (globalTrace) {
			onEachOperator(OnOperatorDebug.instance());
		}
	}

	Hooks() {
	}

	final static class OnOperatorDebug<T>
			implements Function<Publisher<T>, Publisher<T>> {

		static final OnOperatorDebug INSTANCE = new OnOperatorDebug<>();


		@SuppressWarnings("unchecked")
		static <T> OnOperatorDebug<T> instance(){
			return (OnOperatorDebug<T>)INSTANCE;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Publisher<T> apply(Publisher<T> publisher) {
			if (publisher instanceof Callable) {
				if (publisher instanceof Mono) {
					return new MonoCallableOnAssembly<>((Mono<T>) publisher);
				}
				return new FluxCallableOnAssembly<>((Flux<T>) publisher);
			}
			if (publisher instanceof Mono) {
				return new MonoOnAssembly<>((Mono<T>) publisher);
			}
			if (publisher instanceof ParallelFlux) {
				return new ParallelFluxOnAssembly<>((ParallelFlux<T>) publisher);
			}
			if (publisher instanceof ConnectableFlux) {
				return new ConnectableFluxOnAssembly<>((ConnectableFlux<T>) publisher);
			}
			return new FluxOnAssembly<>((Flux<T>) publisher);
		}

	}

	static final Logger log = Loggers.getLogger(Hooks.class);
}
