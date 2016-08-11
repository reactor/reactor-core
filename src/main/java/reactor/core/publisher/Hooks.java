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

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.util.Logger;

/**
 * Allows for various lifecycle override
 *
 */
public abstract class Hooks {

	/**
	 * Reset global error dropped strategy to bubbling back the error.
	 */
	public static void resetOnErrorDropped() {
		onErrorDroppedHook = null;
	}

	/**
	 * Reset global data dropped strategy to throwing via {@link
	 * reactor.core.Exceptions#failWithCancel()}
	 */
	public static void resetOnNextDropped() {
		onNextDroppedHook = null;
	}

	/**
	 * Reset global "assembly" hook tracking
	 */
	public static void resetOnOperator() {
		onOperatorCreate = null;
	}

	/**
	 * Reset global operator error mapping to adding as suppressed exception either data
	 * driven exception or error driven exception.
	 */
	public static void resetOnOperatorError() {
		onOperatorErrorHook = null;
	}

	/**
	 * Override global error dropped strategy which by default bubble back the error.
	 *
	 * @param c the dropped error {@link Consumer} hook
	 */
	public static void onErrorDropped(Consumer<? super Throwable> c) {
		onErrorDroppedHook = Objects.requireNonNull(c, "onErrorDroppedHook");
	}

	/**
	 * Override global data dropped strategy which by default throw {@link
	 * reactor.core.Exceptions#failWithCancel()}
	 *
	 * @param c the dropped next {@link Consumer} hook
	 */
	public static void onNextDropped(Consumer<Object> c) {
		onNextDroppedHook = Objects.requireNonNull(c, "onNextDroppedHook");
	}

	/**
	 * Set a global "assembly" hook to intercept signals produced by the passed {@link
	 * Publisher} ({@link Flux} or {@link Mono}). The passed function must result in a
	 * value different from null, and {@link HookOptions#ignore()} can be used to discard
	 * a specific {@link Publisher} from transformations.
	 * <p>
	 * Can be reset via {@link #resetOnOperator()}
	 *
	 * @param newHook a callback for each assembly that must return a {@link SignalPeek}
	 * @param <T> the arbitrary assembled sequence type
	 */
	public static <T> void onOperator(Function<? super HookOptions<T>, ? extends HookOptions<T>>
			newHook) {
		onOperatorCreate = new OnOperatorCreate<>(newHook);
	}

	/**
	 * Override global operator error mapping which by default add as suppressed exception
	 * either data driven exception or error driven exception.
	 *
	 * @param f the operator error {@link BiFunction} mapper, given the failure and an
	 * eventual original context (data or error) and returning an arbitrary exception.
	 */
	public static void onOperatorError(BiFunction<? super Throwable, Object, ?
			extends Throwable> f) {
		onOperatorErrorHook = Objects.requireNonNull(f, "onOperatorErrorHook");
	}

	/**
	 * Filtering and Handling options to apply on a given {@link Publisher}
	 * @param <T> arbitrary sequence type
	 */
	@SuppressWarnings("unchecked")
	public static final class HookOptions<T> {

		/**
		 * Peek into sequence signals.
		 * <p>
		 * The callbacks are all optional.
		 *
		 * @param onNextCall A consumer that will observe {@link Subscriber#onNext(Object)}
		 * @param onErrorCall A consumer that will observe {@link Subscriber#onError(Throwable)}}
		 * @param onCompleteCall A task that will run on {@link Subscriber#onComplete()}
		 * @param onAfterTerminateCall A task will run after termination via {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}
		 * @return an observing {@link HookOptions}
		 */
		public final HookOptions<T> doOnEach(
				Consumer<? super T> onNextCall,
				Consumer<? super Throwable> onErrorCall,
				Runnable onCompleteCall,
				Runnable onAfterTerminateCall
		){
			return doOnSignal(null, onNextCall, onErrorCall, onCompleteCall,
					onAfterTerminateCall, null, null);
		}

		/**
		 * Peek into lifecycle signals.
		 * <p>
		 * The callbacks are all optional.
		 *
		 * @param onSubscribeCall A consumer that will observe {@link Subscriber#onSubscribe(Subscription)}
		 * @param onRequestCall A consumer of long that will observe {@link Subscription#request(long)}}
		 * @param onCancelCall A task that will run on {@link Subscription#cancel()}
		 * @return an observing {@link HookOptions}
		 */
		public final HookOptions<T> doOnLifecycle(
				Consumer<? super Subscription> onSubscribeCall,
				LongConsumer onRequestCall,
				Runnable onCancelCall
		){
			return doOnSignal(onSubscribeCall, null, null, null,
					null, onRequestCall, onCancelCall);
		}

		/**
		 * Peek into the lifecycle and sequence signals.
		 * <p>
		 * The callbacks are all optional.
		 *
		 * @param onSubscribeCall A consumer that will observe {@link Subscriber#onSubscribe(Subscription)}
		 * @param onNextCall A consumer that will observe {@link Subscriber#onNext(Object)}
		 * @param onErrorCall A consumer that will observe {@link Subscriber#onError(Throwable)}}
		 * @param onCompleteCall A task that will run on {@link Subscriber#onComplete()}
		 * @param onAfterTerminateCall A task will run after termination via {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}
		 * @param onRequestCall A consumer of long that will observe {@link Subscription#request(long)}}
		 * @param onCancelCall A task that will run on {@link Subscription#cancel()}
		 * @return an observing {@link HookOptions}
		 */
		public final HookOptions<T> doOnSignal(
				Consumer<? super Subscription> onSubscribeCall,
				Consumer<? super T> onNextCall,
				Consumer<? super Throwable> onErrorCall,
				Runnable onCompleteCall,
				Runnable onAfterTerminateCall,
				LongConsumer onRequestCall,
				Runnable onCancelCall
		){
			if(this == IGNORE || publisher instanceof ConnectableFlux){
				return this;
			}
			if (publisher instanceof Mono) {
				if (publisher instanceof Fuseable) {
					return new HookOptions<>(new MonoPeekFuseable<T>(publisher,
							onSubscribeCall, onNextCall, onErrorCall, onCompleteCall,
							onAfterTerminateCall, onRequestCall, onCancelCall),
							traced);
				}
				else {
					return new HookOptions<>(new MonoPeek<T>(publisher,
							onSubscribeCall, onNextCall, onErrorCall, onCompleteCall,
							onAfterTerminateCall, onRequestCall, onCancelCall), traced);
				}
			}
			else if (publisher instanceof Fuseable) {
				return new HookOptions<>(new FluxPeekFuseable<T>(publisher,
						onSubscribeCall, onNextCall, onErrorCall, onCompleteCall,
						onAfterTerminateCall, onRequestCall, onCancelCall), traced);
			}
			else {
				return new HookOptions<>(new FluxPeek<T>(publisher,
						onSubscribeCall, onNextCall, onErrorCall, onCompleteCall,
						onAfterTerminateCall, onRequestCall, onCancelCall), traced);
			}
		}

		/**
		 * Discard all {@link HookOptions} applied to the current {@link #publisher()}
		 *
		 * @return an ignoring {@link HookOptions}
		 */
		@SuppressWarnings("unchecked")
		public final HookOptions<T> ignore(){
			return HookOptions.IGNORE;
		}

		/**
		 * Apply hook only if {@link #publisher()} is {@link Flux}
		 * @return a possibly ignoring {@link HookOptions}
		 */
		public final HookOptions<T> ifFlux(){
			return publisher() instanceof Flux ? this : HookOptions.IGNORE;
		}

		/**
		 * Apply hook only if {@link #publisher()} is {@link Mono}
		 * @return a possibly ignoring {@link HookOptions}
		 */
		public final HookOptions<T> ifMono(){
			return publisher() instanceof Mono ? this : HookOptions.IGNORE;
		}

		/**
		 * Apply hook only if {@link #publisher()} if operator name match the type name
		 * (case insensitive, without Mono/Flux prefix or Fuseable suffix.
		 *
		 * @return a possibly ignoring {@link HookOptions}
		 */
		public final HookOptions<T> ifOperatorName(String name){
			return publisher().getClass().getSimpleName().replaceAll("Flux|Mono|Fuseable",
					"").equalsIgnoreCase(name) ? this : HookOptions.IGNORE;
		}

		/**
		 * Apply hook only if {@link #publisher()} if operator name match the type name
		 * (case insensitive, without Mono/Flux prefix or Fuseable suffix.
		 *
		 * @return a possibly ignoring {@link HookOptions}
		 */
		public final HookOptions<T> ifOperatorNameContains(String name){
			return publisher().getClass().getSimpleName().replaceAll("Flux|Mono|Fuseable",
					"").contains(name) ? this : HookOptions.IGNORE;
		}

		/**
		 * Observe Reactive Streams signals matching the passed filter {@code options} and use
		 * {@link Logger} support to handle trace implementation. Default will use the passed
		 * {@link Level} and java.util.logging. If SLF4J is available, it will be used
		 * instead.
		 * <p>
		 * Options allow fine grained filtering of the traced signal, for instance to only
		 * capture onNext and onError:
		 * <pre>
		 *     Operators.signalLogger(source, "category", Level.INFO, SignalType.ON_NEXT,
		 * SignalType.ON_ERROR)
		 *
		 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png"
		 * alt="">
		 *
		 * @param category to be mapped into logger configuration (e.g.
		 * org.springframework.reactor).
		 * @param level the level to enforce for this tracing Flux
		 * @param options a vararg {@link SignalType} option to filter log messages
		 *
		 * @return a logging {@link HookOptions}
		 */
		public HookOptions<T> log(String category, Level level, SignalType... options){
			SignalLogger peek = new SignalLogger<>(publisher, category, level, options);
			return doOnSignal(peek.onSubscribeCall(), peek.onNextCall(), peek
					.onErrorCall(), peek.onCompleteCall(), peek.onAfterTerminateCall(),
					peek.onRequestCall(), peek.onCancelCall());
		}

		/**
		 * Enable operator stack recorder and capture declaration stack. Errors are observed
		 * and enriched with a Suppressed Exception detailing the original stack. Must be
		 * called before producers (e.g. Flux.map, Mono.fromCallable) are actually called to
		 * intercept the right stack information.
		 *
		 * @return a operator stack capture {@link HookOptions}
		 */
		public HookOptions<T> operatorStacktrace(){
			traced = true;
			return this;
		}

		/**
		 * The current publisher to decorate
		 *
		 * @return The current publisher to decorate
		 */
		public Publisher<T> publisher() {
			return publisher;
		}

		static final HookOptions IGNORE = new HookOptions(null);

		final Publisher<T> publisher;

		boolean traced;

		HookOptions(Publisher<T> p) {
			this(p, false);
		}

		HookOptions(Publisher<T> p, boolean traced) {
			this.traced = traced;
			this.publisher = p;
		}
	}

	static volatile OnOperatorCreate<?>         onOperatorCreate;
	static volatile Consumer<? super Throwable> onErrorDroppedHook;
	static volatile Consumer<Object>            onNextDroppedHook;
	static volatile BiFunction<? super Throwable, Object, ? extends Throwable>
	                                            onOperatorErrorHook;

	static {
		boolean globalTrace =
				Boolean.parseBoolean(System.getProperty("reactor.trace" + ".operatorStacktrace",
						"false"));

		if (globalTrace) {
			onOperatorCreate = new OnOperatorCreate<>(HookOptions::operatorStacktrace);
		}
	}

	Hooks() {
	}

	final static class OnOperatorCreate<T>
			implements Function<Publisher<T>, Publisher<T>> {

		final Function<? super HookOptions<T>, ? extends HookOptions<T>> hook;

		OnOperatorCreate(Function<? super HookOptions<T>, ? extends HookOptions<T>> hook) {
			this.hook = hook;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Publisher<T> apply(Publisher<T> publisher) {
			if (hook != null && !(publisher instanceof ConnectableFlux)) {
				HookOptions<T> hooks =
						Objects.requireNonNull(hook.apply(new HookOptions<>(publisher)), "hook");

				if (hooks != HookOptions.IGNORE) {
					publisher = hooks.publisher();
					boolean trace = hooks.traced;

					if (trace){
						if (publisher instanceof Callable) {
							if (publisher instanceof Mono) {
								return new MonoCallableOnAssembly<>(publisher,
										null,
										trace);
							}
							return new FluxCallableOnAssembly<>(publisher, null,
									trace);
						}
						if (publisher instanceof Mono) {
							return new MonoOnAssembly<>(publisher, null, trace);
						}
						if (publisher instanceof ConnectableFlux) {
							return new ConnectableFluxOnAssembly<>((ConnectableFlux<T>) publisher,
									null,
									trace);
						}
						return new FluxOnAssembly<>(publisher, null, trace);
					}
					return publisher;
				}
			}
			return publisher;
		}
	}
}
