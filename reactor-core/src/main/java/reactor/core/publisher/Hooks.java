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

package reactor.core.publisher;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import reactor.core.Exceptions;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshot;
import reactor.core.publisher.FluxOnAssembly.MethodReturnSnapshot;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A set of overridable lifecycle hooks that can be used for cross-cutting
 * added behavior on {@link Flux}/{@link Mono} operators.
 */
public abstract class Hooks {

    /**
	 * Utility method to convert a {@link Publisher} to a {@link Flux} without applying {@link Hooks}.
	 *
	 * @param publisher the {@link Publisher} to convert to a {@link Flux}
	 * @param <T> the type of data emitted by the {@link Publisher}
	 * @return the {@link Publisher} wrapped as a {@link Flux}, or the original if it was a {@link Flux}
	 */
	public static <T> Flux<T> convertToFluxBypassingHooks(Publisher<T> publisher) {
		return Flux.wrap(publisher);
	}

	/**
	 * Utility method to convert a {@link Publisher} to a {@link Mono} without applying {@link Hooks}.
	 * Can optionally perform a "direct" (or unsafe) conversion when the caller is certain the {@link Publisher}
	 * has {@link Mono} semantics.
	 *
	 * @param publisher the {@link Publisher} to convert to a {@link Mono}
	 * @param enforceMonoContract {@code true} to ensure {@link Mono} semantics (by cancelling on first onNext if source isn't already a {@link Mono}),
	 * {@code false} to perform a direct conversion (see {@link Mono#fromDirect(Publisher)}).
	 * @param <T> the type of data emitted by the {@link Publisher}
	 * @return the {@link Publisher} wrapped as a {@link Mono}, or the original if it was a {@link Mono}
	 */
	public static <T> Mono<T> convertToMonoBypassingHooks(Publisher<T> publisher, boolean enforceMonoContract) {
		return Mono.wrap(publisher, enforceMonoContract);
	}


	/**
	 * Add a {@link Publisher} operator interceptor for each operator created
	 * ({@link Flux} or {@link Mono}). The passed function is applied to the original
	 * operator {@link Publisher} and can return a different {@link Publisher},
	 * on the condition that it generically maintains the same data type as the original.
	 * Use of the {@link Flux}/{@link Mono} APIs is discouraged as it will recursively
	 * call this hook, leading to {@link StackOverflowError}.
	 * <p>
	 * Note that sub-hooks are cumulative, but invoking this method twice with the same instance
	 * (or any instance that has the same `toString`) will result in only a single instance
	 * being applied. See {@link #onEachOperator(String, Function)} for a variant that
	 * allows you to name the sub-hooks (and thus replace them or remove them individually
	 * later on). Can be fully reset via {@link #resetOnEachOperator()}.
	 * <p>
	 * This pointcut function cannot make use of {@link Flux}, {@link Mono} or
	 * {@link ParallelFlux} APIs as it would lead to a recursive call to the hook: the
	 * operator calls would effectively invoke onEachOperator from onEachOperator.
	 * See {@link #convertToFluxBypassingHooks(Publisher)} and {@link #convertToMonoBypassingHooks(Publisher, boolean)}.
	 *
	 * @param onEachOperator the sub-hook: a function to intercept each operation call
	 * (e.g. {@code map (fn)} and {@code map(fn2)} in {@code flux.map(fn).map(fn2).subscribe()})
	 *
	 * @see #onEachOperator(String, Function)
	 * @see #resetOnEachOperator(String)
	 * @see #resetOnEachOperator()
	 * @see #onLastOperator(Function)
	 */
	public static void onEachOperator(Function<? super Publisher<Object>, ? extends Publisher<Object>> onEachOperator) {
		onEachOperator(onEachOperator.toString(), onEachOperator);
	}

	/**
	 * Add or replace a named {@link Publisher} operator interceptor for each operator created
	 * ({@link Flux} or {@link Mono}). The passed function is applied to the original
	 * operator {@link Publisher} and can return a different {@link Publisher},
	 * on the condition that it generically maintains the same data type as the original.
	 * Use of the {@link Flux}/{@link Mono} APIs is discouraged as it will recursively
	 * call this hook, leading to {@link StackOverflowError}.
	 * <p>
	 * Note that sub-hooks are cumulative. Invoking this method twice with the same key will
	 * replace the old sub-hook with that name, but keep the execution order (eg. A-h1, B-h2,
	 * A-h3 will keep A-B execution order, leading to hooks h3 then h2 being executed).
	 * Removing a particular key using {@link #resetOnEachOperator(String)} then adding it
	 * back will result in the execution order changing (the later sub-hook being executed
	 * last). Can be fully reset via {@link #resetOnEachOperator()}.
	 * <p>
	 * This pointcut function cannot make use of {@link Flux}, {@link Mono} or
	 * {@link ParallelFlux} APIs as it would lead to a recursive call to the hook: the
	 * operator calls would effectively invoke onEachOperator from onEachOperator.
	 * See {@link #convertToFluxBypassingHooks(Publisher)} and {@link #convertToMonoBypassingHooks(Publisher, boolean)}.
	 *
	 * @param key the key for the sub-hook to add/replace
	 * @param onEachOperator the sub-hook: a function to intercept each operation call
	 * (e.g. {@code map (fn)} and {@code map(fn2)} in {@code flux.map(fn).map(fn2).subscribe()})
	 *
	 * @see #onEachOperator(Function)
	 * @see #resetOnEachOperator(String)
	 * @see #resetOnEachOperator()
	 * @see #onLastOperator(String, Function)
	 */
	public static void onEachOperator(String key, Function<? super Publisher<Object>, ? extends Publisher<Object>> onEachOperator) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(onEachOperator, "onEachOperator");
		log.debug("Hooking onEachOperator: {}", key);

		synchronized (log) {
			onEachOperatorHooks.put(key, onEachOperator);
			onEachOperatorHook = createOrUpdateOpHook(onEachOperatorHooks.values());
		}
	}

	/**
	 * Remove the sub-hook with key {@code key} from the onEachOperator hook. No-op if
	 * no such key has been registered, and equivalent to calling {@link #resetOnEachOperator()}
	 * if it was the last sub-hook.
	 *
	 * @param key the key of the sub-hook to remove
	 */
	public static void resetOnEachOperator(String key) {
		Objects.requireNonNull(key, "key");
		log.debug("Reset onEachOperator: {}", key);

		synchronized (log) {
			onEachOperatorHooks.remove(key);
			onEachOperatorHook = createOrUpdateOpHook(onEachOperatorHooks.values());
		}
	}

	/**
	 * Reset global "assembly" hook tracking
	 */
	public static void resetOnEachOperator() {
		log.debug("Reset to factory defaults : onEachOperator");
		synchronized (log) {
			onEachOperatorHooks.clear();
			onEachOperatorHook = null;
		}
	}

	/**
	 * Override global error dropped strategy which by default bubble back the error.
	 * <p>
	 * The hook is cumulative, so calling this method several times will set up the hook
	 * for as many consumer invocations (even if called with the same consumer instance).
	 *
	 * @param c the {@link Consumer} to apply to dropped errors
	 */
	public static void onErrorDropped(Consumer<? super Throwable> c) {
		Objects.requireNonNull(c, "onErrorDroppedHook");
		log.debug("Hooking new default : onErrorDropped");

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
	 * Add a {@link Publisher} operator interceptor for the last operator created
	 * in every flow ({@link Flux} or {@link Mono}). The passed function is applied
	 * to the original operator {@link Publisher} and can return a different {@link Publisher},
	 * on the condition that it generically maintains the same data type as the original.
	 * <p>
	 * Note that sub-hooks are cumulative, but invoking this method twice with the same
	 * instance (or any instance that has the same `toString`) will result in only a single
	 * instance being applied. See {@link #onLastOperator(String, Function)} for a variant
	 * that allows you to name the sub-hooks (and thus replace them or remove them individually
	 * later on). Can be fully reset via {@link #resetOnLastOperator()}.
	 * <p>
	 * This pointcut function cannot make use of {@link Flux}, {@link Mono} or
	 * {@link ParallelFlux} APIs as it would lead to a recursive call to the hook: the
	 * operator calls would effectively invoke onEachOperator from onEachOperator.
	 * See {@link #convertToFluxBypassingHooks(Publisher)} and {@link #convertToMonoBypassingHooks(Publisher, boolean)}.
	 *
	 * @param onLastOperator the sub-hook: a function to intercept last operation call
	 * (e.g. {@code map(fn2)} in {@code flux.map(fn).map(fn2).subscribe()})
	 *
	 * @see #onLastOperator(String, Function)
	 * @see #resetOnLastOperator(String)
	 * @see #resetOnLastOperator()
	 * @see #onEachOperator(Function)
	 */
	public static void onLastOperator(Function<? super Publisher<Object>, ? extends Publisher<Object>> onLastOperator) {
		onLastOperator(onLastOperator.toString(), onLastOperator);
	}

	/**
	 * Add or replace a named {@link Publisher} operator interceptor for the last operator created
	 * in every flow ({@link Flux} or {@link Mono}). The passed function is applied
	 * to the original operator {@link Publisher} and can return a different {@link Publisher},
	 * on the condition that it generically maintains the same data type as the original.
	 * Use of the {@link Flux}/{@link Mono} APIs is discouraged as it will recursively
	 * call this hook, leading to {@link StackOverflowError}.
	 * <p>
	 * Note that sub-hooks are cumulative. Invoking this method twice with the same key will
	 * replace the old sub-hook with that name, but keep the execution order (eg. A-h1, B-h2,
	 * A-h3 will keep A-B execution order, leading to hooks h3 then h2 being executed).
	 * Removing a particular key using {@link #resetOnLastOperator(String)} then adding it
	 * back will result in the execution order changing (the later sub-hook being executed
	 * last). Can be fully reset via {@link #resetOnLastOperator()}.
	 * <p>
	 * This pointcut function cannot make use of {@link Flux}, {@link Mono} or
	 * {@link ParallelFlux} APIs as it would lead to a recursive call to the hook: the
	 * operator calls would effectively invoke onEachOperator from onEachOperator.
	 * See {@link #convertToFluxBypassingHooks(Publisher)} and {@link #convertToMonoBypassingHooks(Publisher, boolean)}.
	 *
	 * @param key the key for the sub-hook to add/replace
	 * @param onLastOperator the sub-hook: a function to intercept last operation call
	 * (e.g. {@code map(fn2)} in {@code flux.map(fn).map(fn2).subscribe()})
	 *
	 * @see #onLastOperator(Function)
	 * @see #resetOnLastOperator(String)
	 * @see #resetOnLastOperator()
	 * @see #onEachOperator(String, Function)
	 */
	public static void onLastOperator(String key, Function<? super Publisher<Object>, ? extends Publisher<Object>> onLastOperator) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(onLastOperator, "onLastOperator");
		log.debug("Hooking onLastOperator: {}", key);

		synchronized (log) {
			onLastOperatorHooks.put(key, onLastOperator);
			onLastOperatorHook = createOrUpdateOpHook(onLastOperatorHooks.values());
		}
	}

	/**
	 * Remove the sub-hook with key {@code key} from the onLastOperator hook. No-op if
	 * no such key has been registered, and equivalent to calling {@link #resetOnLastOperator()}
	 * if it was the last sub-hook.
	 *
	 * @param key the key of the sub-hook to remove
	 */
	public static void resetOnLastOperator(String key) {
		Objects.requireNonNull(key, "key");
		log.debug("Reset onLastOperator: {}", key);

		synchronized (log) {
			onLastOperatorHooks.remove(key);
			onLastOperatorHook = createOrUpdateOpHook(onLastOperatorHooks.values());
		}
	}

	/**
	 * Reset global "subscriber" hook tracking
	 */
	public static void resetOnLastOperator() {
		log.debug("Reset to factory defaults : onLastOperator");
		synchronized (log) {
			onLastOperatorHooks.clear();
			onLastOperatorHook = null;
		}
	}

	/**
	 * Override global data dropped strategy which by default logs at DEBUG level.
	 * <p>
	 * The hook is cumulative, so calling this method several times will set up the hook
	 * for as many consumer invocations (even if called with the same consumer instance).
	 *
	 * @param c the {@link Consumer} to apply to data (onNext) that is dropped
	 * @see #onNextDroppedFail()
	 */
	public static void onNextDropped(Consumer<Object> c) {
		Objects.requireNonNull(c, "onNextDroppedHook");
		log.debug("Hooking new default : onNextDropped");

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
	 * Resets {@link #resetOnNextDropped() onNextDropped hook(s)} and
	 * apply a strategy of throwing {@link Exceptions#failWithCancel()} instead.
	 * <p>
	 * Use {@link #resetOnNextDropped()} to reset to the default strategy of logging.
	 */
	public static void onNextDroppedFail() {
		log.debug("Enabling failure mode for onNextDropped");

		synchronized(log) {
			onNextDroppedHook = n -> {throw Exceptions.failWithCancel();};
		}
	}

	/**
	 * Enable operator stack recorder that captures a declaration stack whenever an
	 * operator is instantiated. When errors are observed later on, they will be
	 * enriched with a Suppressed Exception detailing the original assembly line stack.
	 * Must be called before producers (e.g. Flux.map, Mono.fromCallable) are actually
	 * called to intercept the right stack information.
	 * <p>
	 * This is added as a specifically-keyed sub-hook in {@link #onEachOperator(String, Function)}.
	 */
	public static void onOperatorDebug() {
		log.debug("Enabling stacktrace debugging via onOperatorDebug");
		GLOBAL_TRACE = true;
	}

	/**
	 * Reset global operator debug.
	 */
	public static void resetOnOperatorDebug() {
		GLOBAL_TRACE = false;
	}

	/**
	 * Set the custom global error mode hook for operators that support resuming
	 * during an error in their {@link org.reactivestreams.Subscriber#onNext(Object)}.
	 * <p>
	 * The hook is a {@link BiFunction} of {@link Throwable} and potentially null {@link Object}.
	 * If it is also a {@link java.util.function.BiPredicate}, its
	 * {@link java.util.function.BiPredicate#test(Object, Object) test} method should be
	 * used to determine if an error should be processed (matching predicate) or completely
	 * skipped (non-matching predicate). Typical usage, as in {@link Operators}, is to
	 * check if the predicate matches and fallback to {@link Operators#onOperatorError(Throwable, Context)}
	 * if it doesn't.
	 *
	 * @param onNextError the new {@link BiFunction} to use.
	 */
	public static void onNextError(BiFunction<? super Throwable, Object, ? extends Throwable> onNextError) {
		Objects.requireNonNull(onNextError, "onNextError");
		log.debug("Hooking new default : onNextError");

		if (onNextError instanceof OnNextFailureStrategy) {
			synchronized(log) {
				onNextErrorHook = (OnNextFailureStrategy) onNextError;
			}
		}
		else {
			synchronized(log) {
				onNextErrorHook = new OnNextFailureStrategy.LambdaOnNextErrorStrategy(onNextError);
			}
		}
	}

	/**
	 * Add a custom error mapping, overriding the default one. Custom mapping can be an
	 * accumulation of several sub-hooks each subsequently added via this method.
	 * <p>
	 * Note that sub-hooks are cumulative, but invoking this method twice with the same
	 * instance (or any instance that has the same `toString`) will result in only a single
	 * instance being applied. See {@link #onOperatorError(String, BiFunction)} for a variant
	 * that allows you to name the sub-hooks (and thus replace them or remove them individually
	 * later on). Can be fully reset via {@link #resetOnOperatorError()}.
	 * <p>
	 * For reference, the default mapping is to unwrap the exception and, if the second
	 * parameter is another exception, to add it to the first as suppressed.
	 *
	 * @param onOperatorError an operator error {@link BiFunction} mapper, returning an arbitrary exception
	 * given the failure and optionally some original context (data or error).
	 *
	 * @see #onOperatorError(String, BiFunction)
	 * @see #resetOnOperatorError(String)
	 * @see #resetOnOperatorError()
	 */
	public static void onOperatorError(BiFunction<? super Throwable, Object, ? extends Throwable> onOperatorError) {
		onOperatorError(onOperatorError.toString(), onOperatorError);
	}

	/**
	 * Add or replace a named custom error mapping, overriding the default one. Custom
	 * mapping can be an accumulation of several sub-hooks each subsequently added via this
	 * method.
	 * <p>
	 * Note that invoking this method twice with the same key will replace the old sub-hook
	 * with that name, but keep the execution order (eg. A-h1, B-h2, A-h3 will keep A-B
	 * execution order, leading to hooks h3 then h2 being executed). Removing a particular
	 * key using {@link #resetOnOperatorError(String)} then adding it back will result in
	 * the execution order changing (the later sub-hook being executed last).
	 * Can be fully reset via {@link #resetOnOperatorError()}.
	 * <p>
	 * For reference, the default mapping is to unwrap the exception and, if the second
	 * parameter is another exception, to add it to the first as a suppressed.
	 *
	 * @param key the key for the sub-hook to add/replace
	 * @param onOperatorError an operator error {@link BiFunction} mapper, returning an arbitrary exception
	 * given the failure and optionally some original context (data or error).
	 *
	 * @see #onOperatorError(String, BiFunction)
	 * @see #resetOnOperatorError(String)
	 * @see #resetOnOperatorError()
	 */
	public static void onOperatorError(String key, BiFunction<? super Throwable, Object, ? extends Throwable> onOperatorError) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(onOperatorError, "onOperatorError");
		log.debug("Hooking onOperatorError: {}", key);
		synchronized (log) {
			onOperatorErrorHooks.put(key, onOperatorError);
			onOperatorErrorHook = createOrUpdateOpErrorHook(onOperatorErrorHooks.values());
		}
	}

	/**
	 * Remove the sub-hook with key {@code key} from the onOperatorError hook. No-op if
	 * no such key has been registered, and equivalent to calling {@link #resetOnOperatorError()}
	 * if it was the last sub-hook.
	 *
	 * @param key the key of the sub-hook to remove
	 */
	public static void resetOnOperatorError(String key) {
		Objects.requireNonNull(key, "key");
		log.debug("Reset onOperatorError: {}", key);
		synchronized (log) {
			onOperatorErrorHooks.remove(key);
			onOperatorErrorHook = createOrUpdateOpErrorHook(onOperatorErrorHooks.values());
		}
	}

	/**
	 * Reset global operator error mapping to the default behavior.
	 * <p>
	 * For reference, the default mapping is to unwrap the exception and, if the second
	 * parameter is another exception, to add it to the first as a suppressed.
	 */
	public static void resetOnOperatorError() {
		log.debug("Reset to factory defaults : onOperatorError");
		synchronized (log) {
			onOperatorErrorHooks.clear();
			onOperatorErrorHook = null;
		}
	}

	/**
	 * Reset global error dropped strategy to bubbling back the error.
	 */
	public static void resetOnErrorDropped() {
		log.debug("Reset to factory defaults : onErrorDropped");
		synchronized (log) {
			onErrorDroppedHook = null;
		}
	}

	/**
	 * Reset global data dropped strategy to throwing via {@link
	 * reactor.core.Exceptions#failWithCancel()}
	 */
	public static void resetOnNextDropped() {
		log.debug("Reset to factory defaults : onNextDropped");
		synchronized (log) {
			onNextDroppedHook = null;
		}
	}

	/**
	 * Reset global onNext error handling strategy to terminating the sequence with
	 * an onError and cancelling upstream ({@link OnNextFailureStrategy#STOP}).
	 */
	public static void resetOnNextError() {
		log.debug("Reset to factory defaults : onNextError");
		synchronized (log) {
			onNextErrorHook = null;
		}
	}

	/**
	 * Globally enables the {@link Context} loss detection in operators like
	 * {@link Flux#transform} or {@link Mono#transformDeferred} when non-Reactor types are used.
	 *
	 * An exception will be thrown upon applying the transformation if the original {@link Context} isn't reachable
	 * (ie. it has been replaced by a totally different {@link Context}, or no {@link Context} at all)
	 */
	public static void enableContextLossTracking() {
		DETECT_CONTEXT_LOSS = true;
	}

	/**
	 * Globally disables the {@link Context} loss detection that was previously
	 * enabled by {@link #enableContextLossTracking()}.
	 *
	 */
	public static void disableContextLossTracking() {
		DETECT_CONTEXT_LOSS = false;
	}

	@Nullable
	@SuppressWarnings("unchecked")
	static Function<Publisher, Publisher> createOrUpdateOpHook(Collection<Function<? super Publisher<Object>, ? extends Publisher<Object>>> hooks) {
		Function<Publisher, Publisher> composite = null;
		for (Function<? super Publisher<Object>, ? extends Publisher<Object>> function : hooks) {
			Function<? super Publisher, ? extends Publisher> op = (Function<? super Publisher, ? extends Publisher>) function;
			if (composite != null) {
				composite = composite.andThen(op);
			}
			else {
				composite = (Function<Publisher, Publisher>) op;
			}
		}
		return composite;
	}

	@Nullable
	static BiFunction<? super Throwable, Object, ? extends Throwable> createOrUpdateOpErrorHook(Collection<BiFunction<? super Throwable, Object, ? extends Throwable>> hooks) {
		BiFunction<? super Throwable, Object, ? extends Throwable> composite = null;
		for (BiFunction<? super Throwable, Object, ? extends Throwable> function : hooks) {
			if (composite != null) {
				BiFunction<? super Throwable, Object, ? extends Throwable> ff = composite;
				composite = (e, data) -> function.apply(ff.apply(e, data), data);
			}
			else {
				composite = function;
			}
		}
		return composite;
	}

	//Hooks that are transformative
	static Function<Publisher, Publisher> onEachOperatorHook;
	static volatile Function<Publisher, Publisher> onLastOperatorHook;
	static volatile BiFunction<? super Throwable, Object, ? extends Throwable> onOperatorErrorHook;

	//Hooks that are just callbacks
	static volatile Consumer<? super Throwable> onErrorDroppedHook;
	static volatile Consumer<Object>            onNextDroppedHook;

	//Special hook that is between the two (strategy can be transformative, but not named)
	static volatile OnNextFailureStrategy onNextErrorHook;


	//For transformative hooks, allow to name them, keep track in an internal Map that retains insertion order
	//internal use only as it relies on external synchronization
	private static final LinkedHashMap<String, Function<? super Publisher<Object>, ? extends Publisher<Object>>> onEachOperatorHooks;
	private static final LinkedHashMap<String, Function<? super Publisher<Object>, ? extends Publisher<Object>>> onLastOperatorHooks;
	private static final LinkedHashMap<String, BiFunction<? super Throwable, Object, ? extends Throwable>> onOperatorErrorHooks;

	private static final LinkedHashMap<String, Function<Queue<?>, Queue<?>>> QUEUE_WRAPPERS = new LinkedHashMap<>(1);

	private static Function<Queue<?>, Queue<?>> QUEUE_WRAPPER = Function.identity();

	//Immutable views on hook trackers, for testing purpose
	static final Map<String, Function<? super Publisher<Object>, ? extends Publisher<Object>>> getOnEachOperatorHooks() {
		return Collections.unmodifiableMap(onEachOperatorHooks);
	}
	static final Map<String, Function<? super Publisher<Object>, ? extends Publisher<Object>>> getOnLastOperatorHooks() {
		return Collections.unmodifiableMap(onLastOperatorHooks);
	}
	static final Map<String, BiFunction<? super Throwable, Object, ? extends Throwable>> getOnOperatorErrorHooks() {
		return Collections.unmodifiableMap(onOperatorErrorHooks);
	}

	static final Logger log = Loggers.getLogger(Hooks.class);

	/**
	 * A key that can be used to store a sequence-specific {@link Hooks#onErrorDropped(Consumer)}
	 * hook in a {@link Context}, as a {@link Consumer Consumer&lt;Throwable&gt;}.
	 */
	static final String KEY_ON_ERROR_DROPPED = "reactor.onErrorDropped.local";
	/**
	 * A key that can be used to store a sequence-specific {@link Hooks#onNextDropped(Consumer)}
	 * hook in a {@link Context}, as a {@link Consumer Consumer&lt;Object&gt;}.
	 */
	static final String KEY_ON_NEXT_DROPPED = "reactor.onNextDropped.local";
	/**
	 * A key that can be used to store a sequence-specific {@link Hooks#onOperatorError(BiFunction)}
	 * hook in a {@link Context}, as a {@link BiFunction BiFunction&lt;Throwable, Object, Throwable&gt;}.
	 */
	static final String KEY_ON_OPERATOR_ERROR = "reactor.onOperatorError.local";

	/**
	 * A key that can be used to store a sequence-specific onDiscard(Consumer)
	 * hook in a {@link Context}, as a {@link Consumer Consumer&lt;Object&gt;}.
	 */
	static final String KEY_ON_DISCARD = "reactor.onDiscard.local";

	/**
	 * A key that can be used to store a sequence-specific {@link Hooks#onOperatorError(BiFunction)}
	 * hook THAT IS ONLY APPLIED TO Operators{@link Operators#onRejectedExecution(Throwable, Context) onRejectedExecution}
	 * in a {@link Context}, as a {@link BiFunction BiFunction&lt;Throwable, Object, Throwable&gt;}.
	 */
	static final String KEY_ON_REJECTED_EXECUTION = "reactor.onRejectedExecution.local";

	static boolean GLOBAL_TRACE = initStaticGlobalTrace();


	static boolean DETECT_CONTEXT_LOSS = false;

	static {
		onEachOperatorHooks = new LinkedHashMap<>(1);
		onLastOperatorHooks = new LinkedHashMap<>(1);
		onOperatorErrorHooks = new LinkedHashMap<>(1);
	}

	//isolated on static method for testing purpose
	static boolean initStaticGlobalTrace() {
		return Boolean.parseBoolean(System.getProperty("reactor.trace.operatorStacktrace",
				"false"));
	}

	Hooks() {
	}

	/**
	 *
	 * @deprecated Should only be used by the instrumentation, DOES NOT guarantee any compatibility
	 */
	@Nullable
	@Deprecated
	public static <T, P extends Publisher<T>> Publisher<T> addReturnInfo(@Nullable P publisher, String method) {
		if (publisher == null) {
			return null;
		}
		return addAssemblyInfo(publisher, new MethodReturnSnapshot(method));
	}

	/**
	 *
	 * @deprecated Should only be used by the instrumentation, DOES NOT guarantee any compatibility
	 */
	@Nullable
	@Deprecated
	public static <T, P extends Publisher<T>> Publisher<T> addCallSiteInfo(@Nullable P publisher, String callSite) {
		if (publisher == null) {
			return null;
		}
		return addAssemblyInfo(publisher, new AssemblySnapshot(callSite));
	}

	static <T, P extends Publisher<T>> Publisher<T> addAssemblyInfo(P publisher, AssemblySnapshot stacktrace) {
		if (publisher instanceof Callable) {
			if (publisher instanceof Mono) {
				return new MonoCallableOnAssembly<>((Mono<T>) publisher, stacktrace);
			}
			return new FluxCallableOnAssembly<>((Flux<T>) publisher, stacktrace);
		}
		if (publisher instanceof Mono) {
			return new MonoOnAssembly<>((Mono<T>) publisher, stacktrace);
		}
		if (publisher instanceof ParallelFlux) {
			return new ParallelFluxOnAssembly<>((ParallelFlux<T>) publisher, stacktrace);
		}
		if (publisher instanceof ConnectableFlux) {
			return new ConnectableFluxOnAssembly<>((ConnectableFlux<T>) publisher, stacktrace);
		}
		return new FluxOnAssembly<>((Flux<T>) publisher, stacktrace);
	}

	/**
	 * Adds a wrapper for every {@link Queue} used in Reactor.
	 * Note that it won't affect existing instances of {@link Queue}.
	 *
	 * Hint: one may use {@link AbstractQueue} to reduce the number of methods to implement.
	 *
	 * @implNote the resulting {@link Queue} MUST NOT change {@link Queue}'s behavior. Only side effects are allowed.
	 *
	 * @see #removeQueueWrapper(String)
	 */
	public static void addQueueWrapper(String key, Function<Queue<?>, Queue<?>> decorator) {
		synchronized (QUEUE_WRAPPERS) {
			QUEUE_WRAPPERS.put(key, decorator);
			Function<Queue<?>, Queue<?>> newHook = null;
			for (Function<Queue<?>, Queue<?>> function : QUEUE_WRAPPERS.values()) {
				if (newHook == null) {
					newHook = function;
				}
				else {
					newHook = newHook.andThen(function);
				}
			}
			QUEUE_WRAPPER = newHook;
		}
	}

	/**
	 * Removes existing {@link Queue} wrapper by key.
	 *
	 * @see #addQueueWrapper(String, Function)
	 */
	public static void removeQueueWrapper(String key) {
		synchronized (QUEUE_WRAPPERS) {
			QUEUE_WRAPPERS.remove(key);
			if (QUEUE_WRAPPERS.isEmpty()) {
				QUEUE_WRAPPER = Function.identity();
			}
			else {
				Function<Queue<?>, Queue<?>> newHook = null;
				for (Function<Queue<?>, Queue<?>> function : QUEUE_WRAPPERS.values()) {
					if (newHook == null) {
						newHook = function;
					}
					else {
						newHook = newHook.andThen(function);
					}
				}
				QUEUE_WRAPPER = newHook;
			}
		}
	}

	/**
	 * Remove all queue wrappers.
	 *
	 * @see #addQueueWrapper(String, Function)
	 * @see #removeQueueWrapper(String)
	 */
	public static void removeQueueWrappers() {
		synchronized (QUEUE_WRAPPERS) {
			QUEUE_WRAPPERS.clear();
			QUEUE_WRAPPER = Function.identity();
		}
	}

	/**
	 * Applies the {@link Queue} wrappers that were previously registered.
	 * SHOULD NOT change the behavior of the provided {@link Queue}.
	 *
	 * @param queue the {@link Queue} to wrap.
	 * @return the result of applying {@link Queue} wrappers registered with {@link #addQueueWrapper(String, Function)}.
	 *
	 * @see #addQueueWrapper(String, Function)
	 * @see #removeQueueWrapper(String)
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static <T> Queue<T> wrapQueue(Queue<T> queue) {
		return (Queue) QUEUE_WRAPPER.apply(queue);
	}
}
