/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;

import io.micrometer.core.instrument.MeterRegistry;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.FluxOnAssembly.CheckpointHeavySnapshot;
import reactor.core.publisher.FluxOnAssembly.CheckpointLightSnapshot;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshot;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Metrics;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;
import reactor.util.context.ContextView;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuple5;
import reactor.util.function.Tuple6;
import reactor.util.function.Tuple7;
import reactor.util.function.Tuple8;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

/**
 * A Reactive Streams {@link Publisher} with basic rx operators that emits at most one item <em>via</em> the
 * {@code onNext} signal then terminates with an {@code onComplete} signal (successful Mono,
 * with or without value), or only emits a single {@code onError} signal (failed Mono).
 *
 * <p>Most Mono implementations are expected to immediately call {@link Subscriber#onComplete()}
 * after having called {@link Subscriber#onNext(T)}. {@link Mono#never() Mono.never()} is an outlier: it doesn't
 * emit any signal, which is not technically forbidden although not terribly useful outside
 * of tests. On the other hand, a combination of {@code onNext} and {@code onError} is explicitly forbidden.
 *
 * <p>
 * The recommended way to learn about the {@link Mono} API and discover new operators is
 * through the reference documentation, rather than through this javadoc (as opposed to
 * learning more about individual operators). See the <a href="https://projectreactor.io/docs/core/release/reference/docs/index.html#which-operator">
 * "which operator do I need?" appendix</a>.
 *
 * <p><img class="marble" src="doc-files/marbles/mono.svg" alt="">
 *
 * <p>
 *
 * <p>The rx operators will offer aliases for input {@link Mono} type to preserve the "at most one"
 * property of the resulting {@link Mono}. For instance {@link Mono#flatMap flatMap} returns a
 * {@link Mono}, while there is a {@link Mono#flatMapMany flatMapMany} alias with possibly more than
 * 1 emission.
 *
 * <p>{@code Mono<Void>} should be used for {@link Publisher} that just completes without any value.
 *
 * <p>It is intended to be used in implementations and return types, input parameters should keep
 * using raw {@link Publisher} as much as possible.
 *
 * <p>Note that using state in the {@code java.util.function} / lambdas used within Mono operators
 * should be avoided, as these may be shared between several {@link Subscriber Subscribers}.
 *
 * @param <T> the type of the single value of this class
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @author David Karnok
 * @author Simon Baslé
 * @see Flux
 */
public abstract class Mono<T> implements CorePublisher<T> {

//	 ==============================================================================================================
//	 Static Generators
//	 ==============================================================================================================

	/**
	 * Creates a deferred emitter that can be used with callback-based
	 * APIs to signal at most one value, a complete or an error signal.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/createForMono.svg" alt="">
	 * <p>
	 * Bridging legacy API involves mostly boilerplate code due to the lack
	 * of standard types and methods. There are two kinds of API surfaces:
	 * 1) addListener/removeListener and 2) callback-handler.
	 * <p>
	 * <b>1) addListener/removeListener pairs</b><br>
	 * To work with such API one has to instantiate the listener,
	 * call the sink from the listener then register it with the source:
	 * <pre><code>
	 * Mono.&lt;String&gt;create(sink -&gt; {
	 *     HttpListener listener = event -&gt; {
	 *         if (event.getResponseCode() >= 400) {
	 *             sink.error(new RuntimeException("Failed"));
	 *         } else {
	 *             String body = event.getBody();
	 *             if (body.isEmpty()) {
	 *                 sink.success();
	 *             } else {
	 *                 sink.success(body.toLowerCase());
	 *             }
	 *         }
	 *     };
	 *
	 *     client.addListener(listener);
	 *
	 *     sink.onDispose(() -&gt; client.removeListener(listener));
	 * });
	 * </code></pre>
	 * Note that this works only with single-value emitting listeners. Otherwise,
	 * all subsequent signals are dropped. You may have to add {@code client.removeListener(this);}
	 * to the listener's body.
	 * <p>
	 * <b>2) callback handler</b><br>
	 * This requires a similar instantiation pattern such as above, but usually the
	 * successful completion and error are separated into different methods.
	 * In addition, the legacy API may or may not support some cancellation mechanism.
	 * <pre><code>
	 * Mono.&lt;String&gt;create(sink -&gt; {
	 *     Callback&lt;String&gt; callback = new Callback&lt;String&gt;() {
	 *         &#64;Override
	 *         public void onResult(String data) {
	 *             sink.success(data.toLowerCase());
	 *         }
	 *
	 *         &#64;Override
	 *         public void onError(Exception e) {
	 *             sink.error(e);
	 *         }
	 *     }
	 *
	 *     // without cancellation support:
	 *
	 *     client.call("query", callback);
	 *
	 *     // with cancellation support:
	 *
	 *     AutoCloseable cancel = client.call("query", callback);
	 *     sink.onDispose(() -> {
	 *         try {
	 *             cancel.close();
	 *         } catch (Exception ex) {
	 *             Exceptions.onErrorDropped(ex);
	 *         }
	 *     });
	 * });
	 * </code></pre>
	 * @param callback Consume the {@link MonoSink} provided per-subscriber by Reactor to generate signals.
	 * @param <T> The type of the value emitted
	 * @return a {@link Mono}
	 */
	public static <T> Mono<T> create(Consumer<MonoSink<T>> callback) {
	    return onAssembly(new MonoCreate<>(callback));
	}

	/**
	 * Create a {@link Mono} provider that will {@link Supplier#get supply} a target {@link Mono} to subscribe to for
	 * each {@link Subscriber} downstream.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/deferForMono.svg" alt="">
	 * <p>
	 * @param supplier a {@link Mono} factory
	 * @param <T> the element type of the returned Mono instance
	 * @return a deferred {@link Mono}
	 * @see #deferContextual(Function)
	 */
	public static <T> Mono<T> defer(Supplier<? extends Mono<? extends T>> supplier) {
		return onAssembly(new MonoDefer<>(supplier));
	}

	/**
	 * Create a {@link Mono} provider that will {@link Function#apply supply} a target {@link Mono}
	 * to subscribe to for each {@link Subscriber} downstream.
	 * This operator behaves the same way as {@link #defer(Supplier)},
	 * but accepts a {@link Function} that will receive the current {@link Context} as an argument.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/deferForMono.svg" alt="">
	 * <p>
	 * @param contextualMonoFactory a {@link Mono} factory
	 * @param <T> the element type of the returned Mono instance
	 * @return a deferred {@link Mono} deriving actual {@link Mono} from context values for each subscription
	 * @deprecated use {@link #deferContextual(Function)} instead. to be removed in 3.5.0.
	 */
	@Deprecated
	public static <T> Mono<T> deferWithContext(Function<Context, ? extends Mono<? extends T>> contextualMonoFactory) {
		return deferContextual(view -> contextualMonoFactory.apply(Context.of(view)));
	}

	/**
	 * Create a {@link Mono} provider that will {@link Function#apply supply} a target {@link Mono}
	 * to subscribe to for each {@link Subscriber} downstream.
	 * This operator behaves the same way as {@link #defer(Supplier)},
	 * but accepts a {@link Function} that will receive the current {@link ContextView} as an argument.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/deferForMono.svg" alt="">
	 * <p>
	 * @param contextualMonoFactory a {@link Mono} factory
	 * @param <T> the element type of the returned Mono instance
	 * @return a deferred {@link Mono} deriving actual {@link Mono} from context values for each subscription
	 */
	public static <T> Mono<T> deferContextual(Function<ContextView, ? extends Mono<? extends T>> contextualMonoFactory) {
		return onAssembly(new MonoDeferContextual<>(contextualMonoFactory));
	}

	/**
	 * Create a Mono which delays an onNext signal by a given {@link Duration duration}
	 * on a default Scheduler and completes.
	 * If the demand cannot be produced in time, an onError will be signalled instead.
	 * The delay is introduced through the {@link Schedulers#parallel() parallel} default Scheduler.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delay.svg" alt="">
	 * <p>
	 * @param duration the duration of the delay
	 *
	 * @return a new {@link Mono}
	 */
	public static Mono<Long> delay(Duration duration) {
		return delay(duration, Schedulers.parallel());
	}

	/**
	 * Create a Mono which delays an onNext signal by a given {@link Duration duration}
	 * on a provided {@link Scheduler} and completes.
	 * If the demand cannot be produced in time, an onError will be signalled instead.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delay.svg" alt="">
	 * <p>
	 * @param duration the {@link Duration} of the delay
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a new {@link Mono}
	 */
	public static Mono<Long> delay(Duration duration, Scheduler timer) {
		return onAssembly(new MonoDelay(duration.toNanos(), TimeUnit.NANOSECONDS, timer));
	}

	/**
	 * Create a {@link Mono} that completes without emitting any item.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/empty.svg" alt="">
	 * <p>
	 * @param <T> the reified {@link Subscriber} type
	 *
	 * @return a completed {@link Mono}
	 */
	public static <T> Mono<T> empty() {
		return MonoEmpty.instance();
	}

	/**
	 * Create a {@link Mono} that terminates with the specified error immediately after
	 * being subscribed to.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/error.svg" alt="">
	 * <p>
	 * @param error the onError signal
	 * @param <T> the reified {@link Subscriber} type
	 *
	 * @return a failing {@link Mono}
	 */
	public static <T> Mono<T> error(Throwable error) {
		return onAssembly(new MonoError<>(error));
	}

	/**
	 * Create a {@link Mono} that terminates with an error immediately after being
	 * subscribed to. The {@link Throwable} is generated by a {@link Supplier}, invoked
	 * each time there is a subscription and allowing for lazy instantiation.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/errorWithSupplier.svg" alt="">
	 * <p>
	 * @param errorSupplier the error signal {@link Supplier} to invoke for each {@link Subscriber}
	 * @param <T> the reified {@link Subscriber} type
	 *
	 * @return a failing {@link Mono}
	 */
	public static <T> Mono<T> error(Supplier<? extends Throwable> errorSupplier) {
		return onAssembly(new MonoErrorSupplied<>(errorSupplier));
	}

	/**
	 * Pick the first {@link Mono} to emit any signal (value, empty completion or error)
	 * and replay that signal, effectively behaving like the fastest of these competing
	 * sources.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/firstWithSignalForMono.svg" alt="">
	 * <p>
	 * @param monos The deferred monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a new {@link Mono} behaving like the fastest of its sources.
	 * @deprecated use {@link #firstWithSignal(Mono[])}. To be removed in reactor 3.5.
	 */
	@SafeVarargs
	@Deprecated
	public static <T> Mono<T> first(Mono<? extends T>... monos) {
		return firstWithSignal(monos);
	}

	/**
	 * Pick the first {@link Mono} to emit any signal (value, empty completion or error)
	 * and replay that signal, effectively behaving like the fastest of these competing
	 * sources.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/firstWithSignalForMono.svg" alt="">
	 * <p>
	 * @param monos The deferred monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a new {@link Mono} behaving like the fastest of its sources.
	 * @deprecated use {@link #firstWithSignal(Iterable)}. To be removed in reactor 3.5.
	 */
	@Deprecated
	public static <T> Mono<T> first(Iterable<? extends Mono<? extends T>> monos) {
		return firstWithSignal(monos);
	}

	/**
	 * Pick the first {@link Mono} to emit any signal (value, empty completion or error)
	 * and replay that signal, effectively behaving like the fastest of these competing
	 * sources.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/firstWithSignalForMono.svg" alt="">
	 * <p>
	 * @param monos The deferred monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a new {@link Mono} behaving like the fastest of its sources.
	 */
	@SafeVarargs
	public static <T> Mono<T> firstWithSignal(Mono<? extends T>... monos) {
		return onAssembly(new MonoFirstWithSignal<>(monos));
	}

	/**
	 * Pick the first {@link Mono} to emit any signal (value, empty completion or error)
	 * and replay that signal, effectively behaving like the fastest of these competing
	 * sources.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/firstWithSignalForMono.svg" alt="">
	 * <p>
	 * @param monos The deferred monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a new {@link Mono} behaving like the fastest of its sources.
	 */
	public static <T> Mono<T> firstWithSignal(Iterable<? extends Mono<? extends T>> monos) {
		return onAssembly(new MonoFirstWithSignal<>(monos));
	}

	/**
	 * Pick the first {@link Mono} source to emit any value and replay that signal,
	 * effectively behaving like the source that first emits an
	 * {@link Subscriber#onNext(Object) onNext}.
	 *
	 * <p>
	 * Valued sources always "win" over an empty source (one that only emits onComplete)
	 * or a failing source (one that only emits onError).
	 * <p>
	 * When no source can provide a value, this operator fails with a {@link NoSuchElementException}
	 * (provided there are at least two sources). This exception has a {@link Exceptions#multiple(Throwable...) composite}
	 * as its {@link Throwable#getCause() cause} that can be used to inspect what went wrong with each source
	 * (so the composite has as many elements as there are sources).
	 * <p>
	 * Exceptions from failing sources are directly reflected in the composite at the index of the failing source.
	 * For empty sources, a {@link NoSuchElementException} is added at their respective index.
	 * One can use {@link Exceptions#unwrapMultiple(Throwable) Exceptions.unwrapMultiple(topLevel.getCause())}
	 * to easily inspect these errors as a {@link List}.
	 * <p>
	 * Note that like in {@link #firstWithSignal(Iterable)}, an infinite source can be problematic
	 * if no other source emits onNext.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/firstWithValueForMono.svg" alt="">
	 *
	 * @param monos An {@link Iterable} of the competing source monos
	 * @param <T> The type of the element in the sources and the resulting mono
	 *
	 * @return a new {@link Mono} behaving like the fastest of its sources
	 */
	public static <T> Mono<T> firstWithValue(Iterable<? extends Mono<? extends T>> monos) {
		return onAssembly(new MonoFirstWithValue<>(monos));
	}

	/**
	 * Pick the first {@link Mono} source to emit any value and replay that signal,
	 * effectively behaving like the source that first emits an
	 * {@link Subscriber#onNext(Object) onNext}.
	 * <p>
	 * Valued sources always "win" over an empty source (one that only emits onComplete)
	 * or a failing source (one that only emits onError).
	 * <p>
	 * When no source can provide a value, this operator fails with a {@link NoSuchElementException}
	 * (provided there are at least two sources). This exception has a {@link Exceptions#multiple(Throwable...) composite}
	 * as its {@link Throwable#getCause() cause} that can be used to inspect what went wrong with each source
	 * (so the composite has as many elements as there are sources).
	 * <p>
	 * Exceptions from failing sources are directly reflected in the composite at the index of the failing source.
	 * For empty sources, a {@link NoSuchElementException} is added at their respective index.
	 * One can use {@link Exceptions#unwrapMultiple(Throwable) Exceptions.unwrapMultiple(topLevel.getCause())}
	 * to easily inspect these errors as a {@link List}.
	 * <p>
	 * Note that like in {@link #firstWithSignal(Mono[])}, an infinite source can be problematic
	 * if no other source emits onNext.
	 * In case the {@code first} source is already an array-based {@link #firstWithValue(Mono, Mono[])}
	 * instance, nesting is avoided: a single new array-based instance is created with all the
	 * sources from {@code first} plus all the {@code others} sources at the same level.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/firstWithValueForMono.svg" alt="">
	 *
	 * @param first the first competing source {@link Mono}
	 * @param others the other competing sources {@link Mono}
	 * @param <T> The type of the element in the sources and the resulting mono
	 *
	 * @return a new {@link Mono} behaving like the fastest of its sources
	 */
	@SafeVarargs
	public static <T> Mono<T> firstWithValue(Mono<? extends T> first, Mono<? extends T>... others) {
		if (first instanceof MonoFirstWithValue) {
			@SuppressWarnings("unchecked")
			MonoFirstWithValue<T> a = (MonoFirstWithValue<T>) first;
			Mono<T> result =  a.firstValuedAdditionalSources(others);
			if (result != null) {
				return result;
			}
		}
		return onAssembly(new MonoFirstWithValue<>(first, others));
	}

	/**
	 * Expose the specified {@link Publisher} with the {@link Mono} API, and ensure it will emit 0 or 1 item.
	 * The source emitter will be cancelled on the first `onNext`.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromForMono.svg" alt="">
	 * <p>
	 * {@link Hooks#onEachOperator(String, Function)} and similar assembly hooks are applied
	 * unless the source is already a {@link Mono} (including {@link Mono} that was decorated as a {@link Flux},
	 * see {@link Flux#from(Publisher)}).
	 *
	 * @param source the {@link Publisher} source
	 * @param <T> the source type
	 *
	 * @return the next item emitted as a {@link Mono}
	 */
	public static <T> Mono<T> from(Publisher<? extends T> source) {
		//some sources can be considered already assembled monos
		//all conversion methods (from, fromDirect, wrap) must accommodate for this
		if (source instanceof Mono) {
			@SuppressWarnings("unchecked")
			Mono<T> casted = (Mono<T>) source;
			return casted;
		}
		if (source instanceof FluxSourceMono
				|| source instanceof FluxSourceMonoFuseable) {
			@SuppressWarnings("unchecked")
			FluxFromMonoOperator<T, T> wrapper = (FluxFromMonoOperator<T,T>) source;
			@SuppressWarnings("unchecked")
			Mono<T> extracted = (Mono<T>) wrapper.source;
			return extracted;
		}

		//we delegate to `wrap` and apply assembly hooks
		@SuppressWarnings("unchecked") Publisher<T> downcasted = (Publisher<T>) source;
		return onAssembly(wrap(downcasted, true));
	}

	/**
	 * Create a {@link Mono} producing its value using the provided {@link Callable}. If
	 * the Callable resolves to {@code null}, the resulting Mono completes empty.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromCallable.svg" alt="">
	 * <p>
	 * @param supplier {@link Callable} that will produce the value
	 * @param <T> type of the expected value
	 *
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> fromCallable(Callable<? extends T> supplier) {
		return onAssembly(new MonoCallable<>(supplier));
	}

	/**
	 * Create a {@link Mono}, producing its value using the provided {@link CompletionStage}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromFuture.svg" alt="">
	 * <p>
	 * Note that the completion stage is not cancelled when that Mono is cancelled, but
	 * that behavior can be obtained by using {@link #doFinally(Consumer)} that checks
	 * for a {@link SignalType#CANCEL} and calls eg.
	 * {@link CompletionStage#toCompletableFuture() .toCompletableFuture().cancel(false)}.
	 *
	 * @param completionStage {@link CompletionStage} that will produce a value (or a null to
	 * complete immediately)
	 * @param <T> type of the expected value
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> fromCompletionStage(CompletionStage<? extends T> completionStage) {
		return onAssembly(new MonoCompletionStage<>(completionStage));
	}

	/**
	 * Create a {@link Mono} that wraps a {@link CompletionStage} on subscription,
	 * emitting the value produced by the {@link CompletionStage}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromFutureSupplier.svg" alt="">
	 * <p>
	 * Note that the completion stage is not cancelled when that Mono is cancelled, but
	 * that behavior can be obtained by using {@link #doFinally(Consumer)} that checks
	 * for a {@link SignalType#CANCEL} and calls eg.
	 * {@link CompletionStage#toCompletableFuture() .toCompletableFuture().cancel(false)}.
	 *
	 * @param stageSupplier The {@link Supplier} of a {@link CompletionStage} that will produce a value (or a null to
	 * complete immediately). This allows lazy triggering of CompletionStage-based APIs.
	 * @param <T> type of the expected value
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> fromCompletionStage(Supplier<? extends CompletionStage<? extends T>> stageSupplier) {
		return defer(() -> onAssembly(new MonoCompletionStage<>(stageSupplier.get())));
	}

	/**
	 * Convert a {@link Publisher} to a {@link Mono} without any cardinality check
	 * (ie this method doesn't cancel the source past the first element).
	 * Conversion transparently returns {@link Mono} sources without wrapping and otherwise
	 * supports {@link Fuseable} sources.
	 * Note this is an advanced interoperability operator that implies you know the
	 * {@link Publisher} you are converting follows the {@link Mono} semantics and only
	 * ever emits one element.
	 * <p>
	 * {@link Hooks#onEachOperator(String, Function)} and similar assembly hooks are applied
	 * unless the source is already a {@link Mono}.
	 *
	 * @param source the Mono-compatible {@link Publisher} to wrap
	 * @param <I> type of the value emitted by the publisher
	 * @return a wrapped {@link Mono}
	 */
	public static <I> Mono<I> fromDirect(Publisher<? extends I> source){
		//some sources can be considered already assembled monos
		//all conversion methods (from, fromDirect, wrap) must accommodate for this
		if(source instanceof Mono){
			@SuppressWarnings("unchecked")
			Mono<I> m = (Mono<I>)source;
			return m;
		}
		if (source instanceof FluxSourceMono
				|| source instanceof FluxSourceMonoFuseable) {
			@SuppressWarnings("unchecked")
			FluxFromMonoOperator<I, I> wrapper = (FluxFromMonoOperator<I,I>) source;
			@SuppressWarnings("unchecked")
			Mono<I> extracted = (Mono<I>) wrapper.source;
			return extracted;
		}

		//we delegate to `wrap` and apply assembly hooks
		@SuppressWarnings("unchecked") Publisher<I> downcasted = (Publisher<I>) source;
		return onAssembly(wrap(downcasted, false));
	}

	/**
	 * Create a {@link Mono}, producing its value using the provided {@link CompletableFuture}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromFuture.svg" alt="">
	 * <p>
	 * Note that the future is not cancelled when that Mono is cancelled, but that behavior
	 * can be obtained by using a {@link #doFinally(Consumer)} that checks
	 * for a {@link SignalType#CANCEL} and calls {@link CompletableFuture#cancel(boolean)}.
	 *
	 * @param future {@link CompletableFuture} that will produce a value (or a null to
	 * complete immediately)
	 * @param <T> type of the expected value
	 * @return A {@link Mono}.
	 * @see #fromCompletionStage(CompletionStage) fromCompletionStage for a generalization
	 */
	public static <T> Mono<T> fromFuture(CompletableFuture<? extends T> future) {
		return onAssembly(new MonoCompletionStage<>(future));
	}

	/**
	 * Create a {@link Mono} that wraps a {@link CompletableFuture} on subscription,
	 * emitting the value produced by the Future.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromFutureSupplier.svg" alt="">
	 * <p>
	 * Note that the future is not cancelled when that Mono is cancelled, but that behavior
	 * can be obtained by using a {@link #doFinally(Consumer)} that checks
	 * for a {@link SignalType#CANCEL} and calls {@link CompletableFuture#cancel(boolean)}.
	 *
	 * @param futureSupplier The {@link Supplier} of a {@link CompletableFuture} that will produce a value (or a null to
	 * complete immediately). This allows lazy triggering of future-based APIs.
	 * @param <T> type of the expected value
	 * @return A {@link Mono}.
	 * @see #fromCompletionStage(Supplier) fromCompletionStage for a generalization
	 */
	public static <T> Mono<T> fromFuture(Supplier<? extends CompletableFuture<? extends T>> futureSupplier) {
		return defer(() -> onAssembly(new MonoCompletionStage<>(futureSupplier.get())));
	}

	/**
	 * Create a {@link Mono} that completes empty once the provided {@link Runnable} has
	 * been executed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromRunnable.svg" alt="">
	 * <p>
	 * @param runnable {@link Runnable} that will be executed before emitting the completion signal
	 *
	 * @param <T> The generic type of the upstream, which is preserved by this operator
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> fromRunnable(Runnable runnable) {
		return onAssembly(new MonoRunnable<>(runnable));
	}

	/**
	 * Create a {@link Mono}, producing its value using the provided {@link Supplier}. If
	 * the Supplier resolves to {@code null}, the resulting Mono completes empty.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromSupplier.svg" alt="">
	 * <p>
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T> type of the expected value
	 *
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> fromSupplier(Supplier<? extends T> supplier) {
		return onAssembly(new MonoSupplier<>(supplier));
	}


	/**
	 * Create a new {@link Mono} that ignores elements from the source (dropping them),
	 * but completes when the source completes.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/ignoreElementsForMono.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the element from the source.
	 *
	 * @param source the {@link Publisher} to ignore
	 * @param <T> the source type of the ignored data
	 *
	 * @return a new completable {@link Mono}.
	 */
	public static <T> Mono<T> ignoreElements(Publisher<T> source) {
		return onAssembly(new MonoIgnorePublisher<>(source));
	}

	/**
	 * Create a new {@link Mono} that emits the specified item, which is captured at
	 * instantiation time.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/just.svg" alt="">
	 * <p>
	 * @param data the only item to onNext
	 * @param <T> the type of the produced item
	 *
	 * @return a {@link Mono}.
	 */
	public static <T> Mono<T> just(T data) {
		return onAssembly(new MonoJust<>(data));
	}

	/**
	 * Create a new {@link Mono} that emits the specified item if {@link Optional#isPresent()} otherwise only emits
	 * onComplete.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/justOrEmpty.svg" alt="">
	 * <p>
	 * @param data the {@link Optional} item to onNext or onComplete if not present
	 * @param <T> the type of the produced item
	 *
	 * @return a {@link Mono}.
	 */
	public static <T> Mono<T> justOrEmpty(@Nullable Optional<? extends T> data) {
		return data != null && data.isPresent() ? just(data.get()) : empty();
	}

	/**
	 * Create a new {@link Mono} that emits the specified item if non null otherwise only emits
	 * onComplete.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/justOrEmpty.svg" alt="">
	 * <p>
	 * @param data the item to onNext or onComplete if null
	 * @param <T> the type of the produced item
	 *
	 * @return a {@link Mono}.
	 */
	public static <T> Mono<T> justOrEmpty(@Nullable T data) {
		return data != null ? just(data) : empty();
	}


	/**
	 * Return a {@link Mono} that will never signal any data, error or completion signal,
	 * essentially running indefinitely.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/never.svg" alt="">
	 * <p>
	 * @param <T> the {@link Subscriber} type target
	 *
	 * @return a never completing {@link Mono}
	 */
	public static <T> Mono<T> never() {
		return MonoNever.instance();
	}

	/**
	 * Returns a Mono that emits a Boolean value that indicates whether two Publisher sequences are the
	 * same by comparing the items emitted by each Publisher pairwise.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sequenceEqual.svg" alt="">
	 *
	 * @param source1 the first Publisher to compare
	 * @param source2 the second Publisher to compare
	 * @param <T> the type of items emitted by each Publisher
	 * @return a Mono that emits a Boolean value that indicates whether the two sequences are the same
	 */
	public static <T> Mono<Boolean> sequenceEqual(Publisher<? extends T> source1, Publisher<? extends T> source2) {
		return sequenceEqual(source1, source2, equalsBiPredicate(), Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Returns a Mono that emits a Boolean value that indicates whether two Publisher sequences are the
	 * same by comparing the items emitted by each Publisher pairwise based on the results of a specified
	 * equality function.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sequenceEqual.svg" alt="">
	 *
	 * @param source1 the first Publisher to compare
	 * @param source2 the second Publisher to compare
	 * @param isEqual a function used to compare items emitted by each Publisher
	 * @param <T> the type of items emitted by each Publisher
	 * @return a Mono that emits a Boolean value that indicates whether the two Publisher two sequences
	 *         are the same according to the specified function
	 */
	public static <T> Mono<Boolean> sequenceEqual(Publisher<? extends T> source1, Publisher<? extends T> source2,
			BiPredicate<? super T, ? super T> isEqual) {
		return sequenceEqual(source1, source2, isEqual, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Returns a Mono that emits a Boolean value that indicates whether two Publisher sequences are the
	 * same by comparing the items emitted by each Publisher pairwise based on the results of a specified
	 * equality function.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sequenceEqual.svg" alt="">
	 *
	 * @param source1 the first Publisher to compare
	 * @param source2 the second Publisher to compare
	 * @param isEqual a function used to compare items emitted by each Publisher
	 * @param prefetch the number of items to prefetch from the first and second source Publisher
	 * @param <T> the type of items emitted by each Publisher
	 * @return a Mono that emits a Boolean value that indicates whether the two Publisher two sequences
	 *         are the same according to the specified function
	 */
	public static <T> Mono<Boolean> sequenceEqual(Publisher<? extends T> source1,
			Publisher<? extends T> source2,
			BiPredicate<? super T, ? super T> isEqual, int prefetch) {
		return onAssembly(new MonoSequenceEqual<>(source1, source2, isEqual, prefetch));
	}

	/**
	 * Create a {@link Mono} emitting the {@link Context} available on subscribe.
	 * If no Context is available, the mono will simply emit the
	 * {@link Context#empty() empty Context}.
	 *
	 * @return a new {@link Mono} emitting current context
	 * @see #subscribe(CoreSubscriber)
	 * @deprecated Use {@link #deferContextual(Function)} or {@link #transformDeferredContextual(BiFunction)} to materialize
	 * the context. To obtain the same Mono of Context, use {@code Mono.deferContextual(Mono::just)}. To be removed in 3.5.0.
	 */
	@Deprecated
	public static Mono<Context> subscriberContext() {
		return onAssembly(MonoCurrentContext.INSTANCE);
	}

	/**
	 * Uses a resource, generated by a supplier for each individual Subscriber, while streaming the value from a
	 * Mono derived from the same resource and makes sure the resource is released if the
	 * sequence terminates or the Subscriber cancels.
	 * <p>
	 * <ul>
	 *     <li>For eager cleanup, unlike in {@link Flux#using(Callable, Function, Consumer, boolean) Flux},
	 *     in the case of a valued {@link Mono} the cleanup happens just before passing the value to downstream.
	 *     In all cases, exceptions raised by the eager cleanup {@link Consumer} may override the terminal event,
	 *     discarding the element if the derived {@link Mono} was valued.</li>
	 *     <li>Non-eager cleanup will drop any exception.</li>
	 * </ul>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingForMono.svg" alt="">
	 *
	 * @param resourceSupplier a {@link Callable} that is called on subscribe to create the resource
	 * @param sourceSupplier a {@link Mono} factory to create the Mono depending on the created resource
	 * @param resourceCleanup invoked on completion to clean-up the resource
	 * @param eager set to true to clean before any signal (including onNext) is passed downstream
	 * @param <T> emitted type
	 * @param <D> resource type
	 *
	 * @return new {@link Mono}
	 */
	public static <T, D> Mono<T> using(Callable<? extends D> resourceSupplier,
			Function<? super D, ? extends Mono<? extends T>> sourceSupplier,
			Consumer<? super D> resourceCleanup,
			boolean eager) {
		return onAssembly(new MonoUsing<>(resourceSupplier, sourceSupplier,
				resourceCleanup, eager));
	}

	/**
	 * Uses a resource, generated by a supplier for each individual Subscriber, while streaming the value from a
	 * Mono derived from the same resource and makes sure the resource is released if the
	 * sequence terminates or the Subscriber cancels.
	 * <p>
	 * Unlike in {@link Flux#using(Callable, Function, Consumer) Flux}, in the case of a valued {@link Mono} the cleanup
	 * happens just before passing the value to downstream. In all cases, exceptions raised by the cleanup
	 * {@link Consumer} may override the terminal event, discarding the element if the derived {@link Mono} was valued.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingForMono.svg" alt="">
	 *
	 * @param resourceSupplier a {@link Callable} that is called on subscribe to create the resource
	 * @param sourceSupplier a {@link Mono} factory to create the Mono depending on the created resource
	 * @param resourceCleanup invoked on completion to clean-up the resource
	 * @param <T> emitted type
	 * @param <D> resource type
	 *
	 * @return new {@link Mono}
	 */
	public static <T, D> Mono<T> using(Callable<? extends D> resourceSupplier,
			Function<? super D, ? extends Mono<? extends T>> sourceSupplier,
			Consumer<? super D> resourceCleanup) {
		return using(resourceSupplier, sourceSupplier, resourceCleanup, true);
	}


	/**
	 * Uses a resource, generated by a {@link Publisher} for each individual {@link Subscriber},
	 * to derive a {@link Mono}. Note that all steps of the operator chain that would need the
	 * resource to be in an open stable state need to be described inside the {@code resourceClosure}
	 * {@link Function}.
	 * <p>
	 * Unlike in {@link Flux#usingWhen(Publisher, Function, Function) the Flux counterpart}, ALL signals are deferred
	 * until the {@link Mono} terminates and the relevant {@link Function} generates and invokes a "cleanup"
	 * {@link Publisher}. This is because a failure in the cleanup Publisher
	 * must result in a lone {@code onError} signal in the downstream {@link Mono} (any potential value in the
	 * derived {@link Mono} is discarded). Here are the various scenarios that can play out:
	 * <ul>
	 *     <li>empty Mono, asyncCleanup ends with {@code onComplete()}: downstream receives {@code onComplete()}</li>
	 *     <li>empty Mono, asyncCleanup ends with {@code onError(t)}: downstream receives {@code onError(t)}</li>
	 *     <li>valued Mono, asyncCleanup ends with {@code onComplete()}: downstream receives {@code onNext(value),onComplete()}</li>
	 *     <li>valued Mono, asyncCleanup ends with {@code onError(t)}: downstream receives {@code onError(t)}, {@code value} is discarded</li>
	 *     <li>error(e) Mono, asyncCleanup ends with {@code onComplete()}: downstream receives {@code onError(e)}</li>
	 *     <li>error(e) Mono, asyncCleanup ends with {@code onError(t)}: downstream receives {@code onError(t)}, t suppressing e</li>
	 * </ul>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingWhenSuccessForMono.svg" alt="">
	 * <p>
	 * Note that if the resource supplying {@link Publisher} emits more than one resource, the
	 * subsequent resources are dropped ({@link Operators#onNextDropped(Object, Context)}). If
	 * the publisher errors AFTER having emitted one resource, the error is also silently dropped
	 * ({@link Operators#onErrorDropped(Throwable, Context)}).
	 * An empty completion or error without at least one onNext signal (no resource supplied)
	 * triggers a short-circuit of the main sequence with the same terminal signal
	 * (no cleanup is invoked).
	 *
	 * <p><strong>Discard Support:</strong> This operator discards any source element if the {@code asyncCleanup} handler fails.
	 *
	 * @param resourceSupplier a {@link Publisher} that "generates" the resource,
	 * subscribed for each subscription to the main sequence
	 * @param resourceClosure a factory to derive a {@link Mono} from the supplied resource
	 * @param asyncCleanup an asynchronous resource cleanup invoked when the resource
	 * closure terminates (with onComplete, onError or cancel)
	 * @param <T> the type of elements emitted by the resource closure, and thus the main sequence
	 * @param <D> the type of the resource object
	 *
	 * @return a new {@link Mono} built around a "transactional" resource, with deferred emission until the
	 * asynchronous cleanup sequence completes
	 */
	public static <T, D> Mono<T> usingWhen(Publisher<D> resourceSupplier,
			Function<? super D, ? extends Mono<? extends T>> resourceClosure,
			Function<? super D, ? extends Publisher<?>> asyncCleanup) {
		return usingWhen(resourceSupplier, resourceClosure, asyncCleanup,
				(res, error) -> asyncCleanup.apply(res),
				asyncCleanup);
	}

	/**
	 * Uses a resource, generated by a {@link Publisher} for each individual {@link Subscriber},
	 * to derive a {@link Mono}.Note that all steps of the operator chain that would need the
	 * resource to be in an open stable state need to be described inside the {@code resourceClosure}
	 * {@link Function}.
	 * <p>
	 * Unlike in {@link Flux#usingWhen(Publisher, Function, Function, BiFunction, Function) the Flux counterpart},
	 * ALL signals are deferred until the {@link Mono} terminates and the relevant {@link Function}
	 * generates and invokes a "cleanup" {@link Publisher}. This is because a failure in the cleanup Publisher
	 * must result in a lone {@code onError} signal in the downstream {@link Mono} (any potential value in the
	 * derived {@link Mono} is discarded). Here are the various scenarios that can play out:
	 * <ul>
	 *     <li>empty Mono, asyncComplete ends with {@code onComplete()}: downstream receives {@code onComplete()}</li>
	 *     <li>empty Mono, asyncComplete ends with {@code onError(t)}: downstream receives {@code onError(t)}</li>
	 *     <li>valued Mono, asyncComplete ends with {@code onComplete()}: downstream receives {@code onNext(value),onComplete()}</li>
	 *     <li>valued Mono, asyncComplete ends with {@code onError(t)}: downstream receives {@code onError(t)}, {@code value} is discarded</li>
	 *     <li>error(e) Mono, errorComplete ends with {@code onComplete()}: downstream receives {@code onError(e)}</li>
	 *     <li>error(e) Mono, errorComplete ends with {@code onError(t)}: downstream receives {@code onError(t)}, t suppressing e</li>
	 * </ul>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingWhenSuccessForMono.svg" alt="">
	 * <p>
	 * Individual cleanups can also be associated with mono cancellation and
	 * error terminations:
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingWhenFailureForMono.svg" alt="">
	 * <p>
	 * Note that if the resource supplying {@link Publisher} emits more than one resource, the
	 * subsequent resources are dropped ({@link Operators#onNextDropped(Object, Context)}). If
	 * the publisher errors AFTER having emitted one resource, the error is also silently dropped
	 * ({@link Operators#onErrorDropped(Throwable, Context)}).
	 * An empty completion or error without at least one onNext signal (no resource supplied)
	 * triggers a short-circuit of the main sequence with the same terminal signal
	 * (no cleanup is invoked).
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the element if the {@code asyncComplete} handler fails.
	 *
	 * @param resourceSupplier a {@link Publisher} that "generates" the resource,
	 * subscribed for each subscription to the main sequence
	 * @param resourceClosure a factory to derive a {@link Mono} from the supplied resource
	 * @param asyncComplete an asynchronous resource cleanup invoked if the resource closure terminates with onComplete
	 * @param asyncError an asynchronous resource cleanup invoked if the resource closure terminates with onError.
	 * The terminating error is provided to the {@link BiFunction}
	 * @param asyncCancel an asynchronous resource cleanup invoked if the resource closure is cancelled.
	 * When {@code null}, the {@code asyncComplete} path is used instead.
	 * @param <T> the type of elements emitted by the resource closure, and thus the main sequence
	 * @param <D> the type of the resource object
	 *
	 * @return a new {@link Mono} built around a "transactional" resource, with several
	 * termination path triggering asynchronous cleanup sequences
	 *
	 */
	public static <T, D> Mono<T> usingWhen(Publisher<D> resourceSupplier,
			Function<? super D, ? extends Mono<? extends T>> resourceClosure,
			Function<? super D, ? extends Publisher<?>> asyncComplete,
			BiFunction<? super D, ? super Throwable, ? extends Publisher<?>> asyncError,
			//the operator itself accepts null for asyncCancel, but we won't in the public API
			Function<? super D, ? extends Publisher<?>> asyncCancel) {
		return onAssembly(new MonoUsingWhen<>(resourceSupplier, resourceClosure,
				asyncComplete, asyncError, asyncCancel));
	}

	/**
	 * Aggregate given publishers into a new {@literal Mono} that will be fulfilled
	 * when all of the given {@literal sources} have completed. An error will cause
	 * pending results to be cancelled and immediate error emission to the returned {@link Mono}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/when.svg" alt="">
	 * <p>
	 * @param sources The sources to use.
	 *
	 * @return a {@link Mono}.
	 */
	public static Mono<Void> when(Publisher<?>... sources) {
		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return empty(sources[0]);
		}
		return onAssembly(new MonoWhen(false, sources));
	}


	/**
	 * Aggregate given publishers into a new {@literal Mono} that will be
	 * fulfilled when all of the given {@literal Publishers} have completed.
	 * An error will cause pending results to be cancelled and immediate error emission
	 * to the returned {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/when.svg" alt="">
	 * <p>
	 *
	 * @param sources The sources to use.
	 *
	 * @return a {@link Mono}.
	 */
	public static Mono<Void> when(final Iterable<? extends Publisher<?>> sources) {
		return onAssembly(new MonoWhen(false, sources));
	}

	/**
	 * Aggregate given publishers into a new {@literal Mono} that will be
	 * fulfilled when all of the given {@literal sources} have completed. Errors from
	 * the sources are delayed.
	 * If several Publishers error, the exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/whenDelayError.svg" alt="">
	 * <p>
	 *
	 * @param sources The sources to use.
	 *
	 * @return a {@link Mono}.
	 */
	public static Mono<Void> whenDelayError(final Iterable<? extends Publisher<?>> sources) {
		return onAssembly(new MonoWhen(true, sources));
	}

	/**
	 * Merge given publishers into a new {@literal Mono} that will be fulfilled when
	 * all of the given {@literal sources} have completed. Errors from the sources are delayed.
	 * If several Publishers error, the exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/whenDelayError.svg" alt="">
	 * <p>
	 * @param sources The sources to use.
	 *
	 * @return a {@link Mono}.
	 */
	public static Mono<Void> whenDelayError(Publisher<?>... sources) {
		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return empty(sources[0]);
		}
		return onAssembly(new MonoWhen(true, sources));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple2}.
	 * An error or <strong>empty</strong> completion of any source will cause other sources
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForMono.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 *
	 * @return a {@link Mono}.
	 */
	public static <T1, T2> Mono<Tuple2<T1, T2>> zip(Mono<? extends T1> p1, Mono<? extends T2> p2) {
		return zip(p1, p2, Flux.tuple2Function());
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values as defined by the combinator function.
	 * An error or <strong>empty</strong> completion of any source will cause other sources
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipTwoSourcesWithZipperForMono.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param combinator a {@link BiFunction} combinator function when both sources
	 * complete
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <O> output value
	 *
	 * @return a {@link Mono}.
	 */
	public static <T1, T2, O> Mono<O> zip(Mono<? extends T1> p1, Mono<?
			extends T2> p2, BiFunction<? super T1, ? super T2, ? extends O> combinator) {
		return onAssembly(new MonoZip<T1, O>(false, p1, p2, combinator));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple3}.
	 * An error or <strong>empty</strong> completion of any source will cause other sources
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForMono.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3> Mono<Tuple3<T1, T2, T3>> zip(Mono<? extends T1> p1, Mono<? extends T2> p2, Mono<? extends T3> p3) {
		return onAssembly(new MonoZip(false, a -> Tuples.fromArray((Object[])a), p1, p2, p3));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple4}.
	 * An error or <strong>empty</strong> completion of any source will cause other sources
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForMono.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 * @param <T4> type of the value from p4
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4> Mono<Tuple4<T1, T2, T3, T4>> zip(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4) {
		return onAssembly(new MonoZip(false, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple5}.
	 * An error or <strong>empty</strong> completion of any source will cause other sources
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForMono.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 * @param <T4> type of the value from p4
	 * @param <T5> type of the value from p5
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5> Mono<Tuple5<T1, T2, T3, T4, T5>> zip(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5) {
		return onAssembly(new MonoZip(false, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple6}.
	 * An error or <strong>empty</strong> completion of any source will cause other sources
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForMono.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param p6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 * @param <T4> type of the value from p4
	 * @param <T5> type of the value from p5
	 * @param <T6> type of the value from p6
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5, T6> Mono<Tuple6<T1, T2, T3, T4, T5, T6>> zip(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5,
			Mono<? extends T6> p6) {
		return onAssembly(new MonoZip(false, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5, p6));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple7}.
	 * An error or <strong>empty</strong> completion of any source will cause other sources
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForMono.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param p6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param p7 The seventh upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 * @param <T4> type of the value from p4
	 * @param <T5> type of the value from p5
	 * @param <T6> type of the value from p6
	 * @param <T7> type of the value from p7
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5, T6, T7> Mono<Tuple7<T1, T2, T3, T4, T5, T6, T7>> zip(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5,
			Mono<? extends T6> p6,
			Mono<? extends T7> p7) {
		return onAssembly(new MonoZip(false, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5, p6, p7));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple8}.
	 * An error or <strong>empty</strong> completion of any source will cause other sources
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForMono.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param p6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param p7 The seventh upstream {@link Publisher} to subscribe to.
	 * @param p8 The eight upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 * @param <T4> type of the value from p4
	 * @param <T5> type of the value from p5
	 * @param <T6> type of the value from p6
	 * @param <T7> type of the value from p7
	 * @param <T8> type of the value from p8
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5, T6, T7, T8> Mono<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> zip(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5,
			Mono<? extends T6> p6,
			Mono<? extends T7> p7,
			Mono<? extends T8> p8) {
		return onAssembly(new MonoZip(false, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5, p6, p7, p8));
	}

	/**
	 * Aggregate given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal
	 * Monos} have produced an item, aggregating their values according to the provided combinator function.
	 * An error or <strong>empty</strong> completion of any source will cause other sources
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipIterableSourcesForMono.svg" alt="">
	 * <p>
	 *
	 * @param monos The monos to use.
	 * @param combinator the function to transform the combined array into an arbitrary
	 * object.
	 * @param <R> the combined result
	 *
	 * @return a {@link Mono}.
	 */
	public static <R> Mono<R> zip(final Iterable<? extends Mono<?>> monos, Function<? super Object[], ? extends R> combinator) {
		return onAssembly(new MonoZip<>(false, combinator, monos));
	}

	/**
	 * Aggregate given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal
	 * Monos} have produced an item, aggregating their values according to the provided combinator function.
	 * An error or <strong>empty</strong> completion of any source will cause other sources
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipVarSourcesWithZipperForMono.svg" alt="">
	 * <p>
	 * @param monos The monos to use.
	 * @param combinator the function to transform the combined array into an arbitrary
	 * object.
	 * @param <R> the combined result
	 *
	 * @return a {@link Mono}.
	 */
	public static <R> Mono<R> zip(Function<? super Object[], ? extends R> combinator, Mono<?>... monos) {
		if (monos.length == 0) {
			return empty();
		}
		if (monos.length == 1) {
			return monos[0].map(d -> combinator.apply(new Object[]{d}));
		}
		return onAssembly(new MonoZip<>(false, combinator, monos));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple2} and delaying errors.
	 * If a Mono source completes without value, the other source is run to completion then the
	 * resulting {@link Mono} completes empty.
	 * If both Monos error, the two exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipDelayErrorFixedSources.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2> Mono<Tuple2<T1, T2>> zipDelayError(Mono<? extends T1> p1, Mono<? extends T2> p2) {
		return onAssembly(new MonoZip(true, a -> Tuples.fromArray((Object[])a), p1, p2));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have produced an item, aggregating their values into a {@link Tuple3} and delaying errors.
	 * If a Mono source completes without value, all other sources are run to completion then
	 * the resulting {@link Mono} completes empty.
	 * If several Monos error, their exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipDelayErrorFixedSources.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3> Mono<Tuple3<T1, T2, T3>> zipDelayError(Mono<? extends T1> p1, Mono<? extends T2> p2, Mono<? extends T3> p3) {
		return onAssembly(new MonoZip(true, a -> Tuples.fromArray((Object[])a), p1, p2, p3));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple4} and delaying errors.
	 *  If a Mono source completes without value, all other sources are run to completion then
	 *  the resulting {@link Mono} completes empty.
	 * 	If several Monos error, their exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipDelayErrorFixedSources.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 * @param <T4> type of the value from p4
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4> Mono<Tuple4<T1, T2, T3, T4>> zipDelayError(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4) {
		return onAssembly(new MonoZip(true, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple5} and delaying errors.
	 * If a Mono source completes without value, all other sources are run to completion then
	 * the resulting {@link Mono} completes empty.
	 * If several Monos error, their exceptions are combined (as suppressed exceptions on a root exception).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipDelayErrorFixedSources.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 * @param <T4> type of the value from p4
	 * @param <T5> type of the value from p5
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5> Mono<Tuple5<T1, T2, T3, T4, T5>> zipDelayError(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5) {
		return onAssembly(new MonoZip(true, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple6} and delaying errors.
	 * If a Mono source completes without value, all other sources are run to completion then
	 * the resulting {@link Mono} completes empty.
	 * If several Monos error, their exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipDelayErrorFixedSources.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param p6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 * @param <T4> type of the value from p4
	 * @param <T5> type of the value from p5
	 * @param <T6> type of the value from p6
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5, T6> Mono<Tuple6<T1, T2, T3, T4, T5, T6>> zipDelayError(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5,
			Mono<? extends T6> p6) {
		return onAssembly(new MonoZip(true, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5, p6));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple7} and delaying errors.
	 * If a Mono source completes without value, all other sources are run to completion then
	 * the resulting {@link Mono} completes empty.
	 * If several Monos error, their exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipDelayErrorFixedSources.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param p6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param p7 The seventh upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 * @param <T4> type of the value from p4
	 * @param <T5> type of the value from p5
	 * @param <T6> type of the value from p6
	 * @param <T7> type of the value from p7
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5, T6, T7> Mono<Tuple7<T1, T2, T3, T4, T5, T6, T7>> zipDelayError(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5,
			Mono<? extends T6> p6,
			Mono<? extends T7> p7) {
		return onAssembly(new MonoZip(true, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5, p6, p7));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have produced an item, aggregating their values into a {@link Tuple8} and delaying errors.
	 * If a Mono source completes without value, all other sources are run to completion then
	 * the resulting {@link Mono} completes empty.
	 * If several Monos error, their exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipDelayErrorFixedSources.svg" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param p6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param p7 The seventh upstream {@link Publisher} to subscribe to.
	 * @param p8 The eight upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from p1
	 * @param <T2> type of the value from p2
	 * @param <T3> type of the value from p3
	 * @param <T4> type of the value from p4
	 * @param <T5> type of the value from p5
	 * @param <T6> type of the value from p6
	 * @param <T7> type of the value from p7
	 * @param <T8> type of the value from p8
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5, T6, T7, T8> Mono<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> zipDelayError(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5,
			Mono<? extends T6> p6,
			Mono<? extends T7> p7,
			Mono<? extends T8> p8) {
		return onAssembly(new MonoZip(true, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5, p6, p7, p8));
	}

	/**
	 * Aggregate given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal
	 * Monos} have produced an item. Errors from the sources are delayed.
	 * If a Mono source completes without value, all other sources are run to completion then
	 * the resulting {@link Mono} completes empty.
	 * If several Monos error, their exceptions are combined (as suppressed exceptions on a root exception).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipDelayErrorIterableSources.svg" alt="">
	 * <p>
	 *
	 * @param monos The monos to use.
	 * @param combinator the function to transform the combined array into an arbitrary
	 * object.
	 * @param <R> the combined result
	 *
	 * @return a {@link Mono}.
	 */
	public static <R> Mono<R> zipDelayError(final Iterable<? extends Mono<?>> monos, Function<? super Object[], ? extends R> combinator) {
		return onAssembly(new MonoZip<>(true, combinator, monos));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the
	 * given {@literal Monos} have produced an item, aggregating their values according to
	 * the provided combinator function and delaying errors.
	 * If a Mono source completes without value, all other sources are run to completion then
	 * the resulting {@link Mono} completes empty.
	 * If several Monos error, their exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipDelayErrorVarSourcesWithZipper.svg" alt="">
	 * <p>
	 * @param monos The monos to use.
	 * @param combinator the function to transform the combined array into an arbitrary
	 * object.
	 * @param <R> the combined result
	 *
	 * @return a combined {@link Mono}.
	 */
	public static <R> Mono<R> zipDelayError(Function<? super Object[], ? extends R>
			combinator, Mono<?>... monos) {
		if (monos.length == 0) {
			return empty();
		}
		if (monos.length == 1) {
			return monos[0].map(d -> combinator.apply(new Object[]{d}));
		}
		return onAssembly(new MonoZip<>(true, combinator, monos));
	}

//	 ==============================================================================================================
//	 Operators
//	 ==============================================================================================================

	/**
	 * Transform this {@link Mono} into a target type.
	 *
	 * <blockquote><pre>
	 * {@code mono.as(Flux::from).subscribe() }
	 * </pre></blockquote>
	 *
	 * @param transformer the {@link Function} to immediately map this {@link Mono}
	 * into a target type
	 * @param <P> the returned instance type
	 *
	 * @return the {@link Mono} transformed to an instance of P
	 * @see #transformDeferred(Function) transformDeferred(Function) for a lazy transformation of Mono
	 */
	public final <P> P as(Function<? super Mono<T>, P> transformer) {
		return transformer.apply(this);
	}

	/**
	 * Join the termination signals from this mono and another source into the returned
	 * void mono
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/and.svg" alt="">
	 * <p>
	 * @param other the {@link Publisher} to wait for
	 * complete
	 * @return a new combined Mono
	 * @see #when
	 */
	public final Mono<Void> and(Publisher<?> other) {
		if (this instanceof MonoWhen) {
			@SuppressWarnings("unchecked") MonoWhen o = (MonoWhen) this;
			Mono<Void> result = o.whenAdditionalSource(other);
			if (result != null) {
				return result;
			}
		}

		return when(this, other);
	}

	/**
	 * Subscribe to this {@link Mono} and <strong>block indefinitely</strong> until a next signal is
	 * received. Returns that value, or null if the Mono completes empty. In case the Mono
	 * errors, the original exception is thrown (wrapped in a {@link RuntimeException} if
	 * it was a checked exception).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/block.svg" alt="">
	 * <p>
	 * Note that each block() will trigger a new subscription: in other words, the result
	 * might miss signal from hot publishers.
	 *
	 * @return T the result
	 */
	@Nullable
	public T block() {
		BlockingMonoSubscriber<T> subscriber = new BlockingMonoSubscriber<>();
		subscribe((Subscriber<T>) subscriber);
		return subscriber.blockingGet();
	}

	/**
	 * Subscribe to this {@link Mono} and <strong>block</strong> until a next signal is
	 * received or a timeout expires. Returns that value, or null if the Mono completes
	 * empty. In case the Mono errors, the original exception is thrown (wrapped in a
	 * {@link RuntimeException} if it was a checked exception).
	 * If the provided timeout expires, a {@link RuntimeException} is thrown.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/blockWithTimeout.svg" alt="">
	 * <p>
	 * Note that each block() will trigger a new subscription: in other words, the result
	 * might miss signal from hot publishers.
	 *
	 * @param timeout maximum time period to wait for before raising a {@link RuntimeException}
	 *
	 * @return T the result
	 */
	@Nullable
	public T block(Duration timeout) {
		BlockingMonoSubscriber<T> subscriber = new BlockingMonoSubscriber<>();
		subscribe((Subscriber<T>) subscriber);
		return subscriber.blockingGet(timeout.toNanos(), TimeUnit.NANOSECONDS);
	}

	/**
	 * Subscribe to this {@link Mono} and <strong>block indefinitely</strong> until a next signal is
	 * received or the Mono completes empty. Returns an {@link Optional}, which can be used
	 * to replace the empty case with an Exception via {@link Optional#orElseThrow(Supplier)}.
	 * In case the Mono itself errors, the original exception is thrown (wrapped in a
	 * {@link RuntimeException} if it was a checked exception).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/blockOptional.svg" alt="">
	 * <p>
	 * Note that each blockOptional() will trigger a new subscription: in other words, the result
	 * might miss signal from hot publishers.
	 *
	 * @return T the result
	 */
	public Optional<T> blockOptional() {
		BlockingOptionalMonoSubscriber<T> subscriber = new BlockingOptionalMonoSubscriber<>();
		subscribe((Subscriber<T>) subscriber);
		return subscriber.blockingGet();
	}

	/**
	 * Subscribe to this {@link Mono} and <strong>block</strong> until a next signal is
	 * received, the Mono completes empty or a timeout expires. Returns an {@link Optional}
	 * for the first two cases, which can be used to replace the empty case with an
	 * Exception via {@link Optional#orElseThrow(Supplier)}.
	 * In case the Mono itself errors, the original exception is thrown (wrapped in a
	 * {@link RuntimeException} if it was a checked exception).
	 * If the provided timeout expires, a {@link RuntimeException} is thrown.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/blockOptionalWithTimeout.svg" alt="">
	 * <p>
	 * Note that each block() will trigger a new subscription: in other words, the result
	 * might miss signal from hot publishers.
	 *
	 * @param timeout maximum time period to wait for before raising a {@link RuntimeException}
	 *
	 * @return T the result
	 */
	public Optional<T> blockOptional(Duration timeout) {
		BlockingOptionalMonoSubscriber<T> subscriber = new BlockingOptionalMonoSubscriber<>();
		subscribe((Subscriber<T>) subscriber);
		return subscriber.blockingGet(timeout.toNanos(), TimeUnit.NANOSECONDS);
	}

	/**
	 * Cast the current {@link Mono} produced type into a target produced type.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/castForMono.svg" alt="">
	 *
	 * @param <E> the {@link Mono} output type
	 * @param clazz the target type to cast to
	 *
	 * @return a casted {@link Mono}
	 */
	public final <E> Mono<E> cast(Class<E> clazz) {
		Objects.requireNonNull(clazz, "clazz");
		return map(clazz::cast);
	}

	/**
	 * Turn this {@link Mono} into a hot source and cache last emitted signals for further {@link Subscriber}.
	 * Completion and Error will also be replayed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/cacheForMono.svg" alt="">
	 * <p>
	 * Once the first subscription is made to this {@link Mono}, the source is subscribed to and
	 * the signal will be cached, indefinitely. This process cannot be cancelled.
	 * <p>
	 * In the face of multiple concurrent subscriptions, this operator ensures that only one
	 * subscription is made to the source.
	 *
	 * @return a replaying {@link Mono}
	 */
	public final Mono<T> cache() {
		return onAssembly(new MonoCacheTime<>(this));
	}

	/**
	 * Turn this {@link Mono} into a hot source and cache last emitted signals for further
	 * {@link Subscriber}, with an expiry timeout.
	 * <p>
	 * Completion and Error will also be replayed until {@code ttl} triggers in which case
	 * the next {@link Subscriber} will start over a new subscription.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/cacheWithTtlForMono.svg" alt="">
	 * <p>
	 * Cache loading (ie. subscription to the source) is triggered atomically by the first
	 * subscription to an uninitialized or expired cache, which guarantees that a single
	 * cache load happens at a time (and other subscriptions will get notified of the newly
	 * cached value when it arrives).
	 *
	 * @return a replaying {@link Mono}
	 */
	public final Mono<T> cache(Duration ttl) {
		return cache(ttl, Schedulers.parallel());
	}

	/**
	 * Turn this {@link Mono} into a hot source and cache last emitted signals for further
	 * {@link Subscriber}, with an expiry timeout.
	 * <p>
	 * Completion and Error will also be replayed until {@code ttl} triggers in which case
	 * the next {@link Subscriber} will start over a new subscription.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/cacheWithTtlForMono.svg" alt="">
	 * <p>
	 * Cache loading (ie. subscription to the source) is triggered atomically by the first
	 * subscription to an uninitialized or expired cache, which guarantees that a single
	 * cache load happens at a time (and other subscriptions will get notified of the newly
	 * cached value when it arrives).
	 *
	 * @param ttl Time-to-live for each cached item and post termination.
	 * @param timer the {@link Scheduler} on which to measure the duration.
	 *
	 * @return a replaying {@link Mono}
	 */
	public final Mono<T> cache(Duration ttl, Scheduler timer) {
		return onAssembly(new MonoCacheTime<>(this, ttl, timer));
	}

	/**
	 * Turn this {@link Mono} into a hot source and cache last emitted signal for further
	 * {@link Subscriber}, with an expiry timeout (TTL) that depends on said signal.
	 * A TTL of {@link Long#MAX_VALUE} milliseconds is interpreted as indefinite caching of
	 * the signal (no cache cleanup is scheduled, so the signal is retained as long as this
	 * {@link Mono} is not garbage collected).
	 * <p>
	 * Empty completion and Error will also be replayed according to their respective TTL,
	 * so transient errors can be "retried" by letting the {@link Function} return
	 * {@link Duration#ZERO}. Such a transient exception would then be propagated to the first
	 * subscriber but the following subscribers would trigger a new source subscription.
	 * <p>
	 * Exceptions in the TTL generators themselves are processed like the {@link Duration#ZERO}
	 * case, except the original signal is {@link Exceptions#addSuppressed(Throwable, Throwable)  suppressed}
	 * (in case of onError) or {@link Hooks#onNextDropped(Consumer) dropped}
	 * (in case of onNext).
	 * <p>
	 * Note that subscribers that come in perfectly simultaneously could receive the same
	 * cached signal even if the TTL is set to zero.
	 * <p>
	 * Cache loading (ie. subscription to the source) is triggered atomically by the first
	 * subscription to an uninitialized or expired cache, which guarantees that a single
	 * cache load happens at a time (and other subscriptions will get notified of the newly
	 * cached value when it arrives).
	 *
	 * @param ttlForValue the TTL-generating {@link Function} invoked when source is valued
	 * @param ttlForError the TTL-generating {@link Function} invoked when source is erroring
	 * @param ttlForEmpty the TTL-generating {@link Supplier} invoked when source is empty
	 * @return a replaying {@link Mono}
	 */
	public final Mono<T> cache(Function<? super T, Duration> ttlForValue,
			Function<Throwable, Duration> ttlForError,
			Supplier<Duration> ttlForEmpty) {
		return cache(ttlForValue, ttlForError, ttlForEmpty, Schedulers.parallel());
	}

	/**
	 * Turn this {@link Mono} into a hot source and cache last emitted signal for further
	 * {@link Subscriber}, with an expiry timeout (TTL) that depends on said signal.
	 * A TTL of {@link Long#MAX_VALUE} milliseconds is interpreted as indefinite caching of
	 * the signal (no cache cleanup is scheduled, so the signal is retained as long as this
	 * {@link Mono} is not garbage collected).
	 * <p>
	 * Empty completion and Error will also be replayed according to their respective TTL,
	 * so transient errors can be "retried" by letting the {@link Function} return
	 * {@link Duration#ZERO}. Such a transient exception would then be propagated to the first
	 * subscriber but the following subscribers would trigger a new source subscription.
	 * <p>
	 * Exceptions in the TTL generators themselves are processed like the {@link Duration#ZERO}
	 * case, except the original signal is {@link Exceptions#addSuppressed(Throwable, Throwable)  suppressed}
	 * (in case of onError) or {@link Hooks#onNextDropped(Consumer) dropped}
	 * (in case of onNext).
	 * <p>
	 * Note that subscribers that come in perfectly simultaneously could receive the same
	 * cached signal even if the TTL is set to zero.
	 * <p>
	 * Cache loading (ie. subscription to the source) is triggered atomically by the first
	 * subscription to an uninitialized or expired cache, which guarantees that a single
	 * cache load happens at a time (and other subscriptions will get notified of the newly
	 * cached value when it arrives).
	 *
	 * @param ttlForValue the TTL-generating {@link Function} invoked when source is valued
	 * @param ttlForError the TTL-generating {@link Function} invoked when source is erroring
	 * @param ttlForEmpty the TTL-generating {@link Supplier} invoked when source is empty
	 * @param timer the {@link Scheduler} on which to measure the duration.
	 * @return a replaying {@link Mono}
	 */
	public final Mono<T> cache(Function<? super T, Duration> ttlForValue,
			Function<Throwable, Duration> ttlForError,
			Supplier<Duration> ttlForEmpty,
			Scheduler timer) {
		return onAssembly(new MonoCacheTime<>(this, ttlForValue, ttlForError, ttlForEmpty, timer));
	}

	/**
	 * Cache {@link Subscriber#onNext(Object) onNext} signal received from the source and replay it to other subscribers,
	 * while allowing invalidation by verifying the cached value against the given {@link Predicate} each time a late
	 * subscription occurs.
	 * Note that the {@link Predicate} is only evaluated if the cache is currently populated, ie. it is not applied
	 * upon receiving the source {@link Subscriber#onNext(Object) onNext} signal.
	 * For late subscribers, if the predicate returns {@code true} the cache is invalidated and a new subscription is made
	 * to the source in an effort to refresh the cache with a more up-to-date value to be passed to the new subscriber.
	 * <p>
	 * The predicate is not strictly evaluated once per downstream subscriber. Rather, subscriptions happening in concurrent
	 * batches  will trigger a single evaluation of the predicate. Similarly, a batch of subscriptions happening before
	 * the cache is populated (ie. before this operator receives an onNext signal after an invalidation) will always
	 * receive the incoming value without going through the {@link Predicate}. The predicate is only triggered by
	 * subscribers that come in AFTER the cache is populated. Therefore, it is possible that pre-population subscribers
	 * receive an "invalid" value, especially if the object can switch from a valid to an invalid state in a short amount
	 * of time (eg. between creation, cache population and propagation to the downstream subscriber(s)).
	 * <p>
	 * If the cached value needs to be discarded in case of invalidation, the recommended way is to do so in the predicate
	 * directly. Note that some downstream subscribers might still be using or storing the value, for example if they
	 * haven't requested anything yet.
	 * <p>
	 * As this form of caching is explicitly value-oriented, empty source completion signals and error signals are NOT
	 * cached. It is always possible to use {@link #materialize()} to cache these (further using {@link #filter(Predicate)}
	 * if one wants to only consider empty sources or error sources).
	 * <p>
	 * Predicate is applied differently depending on whether the cache is populated or not:
	 * <ul>
	 *  <li>IF EMPTY
	 *   <ul><li>first incoming subscriber creates a new COORDINATOR and adds itself</li></ul>
	 *  </li>
	 *  <li>IF COORDINATOR
	 *   <ol>
	 *       <li>each incoming subscriber is added to the current "batch" (COORDINATOR)</li>
	 *       <li>once the value is received, the predicate is applied ONCE
	 *         <ol>
	 *           <li>mismatch: all the batch is terminated with an error
	 *           -> we're back to init state, next subscriber will trigger a new coordinator and a new subscription</li>
	 *           <li>ok: all the batch is completed with the value -> cache is now POPULATED</li>
	 *         </ol>
	 *       </li>
	 *    </ol>
	 *  </li>
	 *  <li>IF POPULATED
	 *    <ol>
	 *        <li>each incoming subscriber causes the predicate to apply</li>
	 *        <li>if ok: complete that subscriber with the value</li>
	 *        <li>if mismatch, swap the current POPULATED with a new COORDINATOR and add the subscriber to that coordinator</li>
	 *        <li>imagining a race between sub1 and sub2:
	 *            <ol>
	 *                <li>OK NOK will naturally lead to sub1 completing and sub2 being put on wait inside a new COORDINATOR</li>
	 *                <li>NOK NOK will race swap of POPULATED with COORDINATOR1 and COORDINATOR2 respectively
	 *                    <ol>
	 *                        <li>if sub1 swaps, sub2 will dismiss the COORDINATOR2 it failed to swap and loop back, see COORDINATOR1 and add itself</li>
	 *                        <li>if sub2 swaps, the reverse happens</li>
	 *                        <li>if value is populated in the time it takes for sub2 to loop back, sub2 sees a value and triggers the predicate again (hopefully passing)</li>
	 *                    </ol>
	 *                </li>
	 *            </ol>
	 *        </li>
	 *    </ol>
	 *  </li>
	 * </ul>
	 * <p>
	 *  Cancellation is only possible for downstream subscribers when they've been added to a COORDINATOR.
	 *  Subscribers that are received when POPULATED will either be completed right away or (if the predicate fails) end up being added to a COORDINATOR.
	 * <p>
	 *  When cancelling a COORDINATOR-issued subscription:
	 *  <ol>
	 *    <li>removes itself from batch</li>
	 *    <li>if 0 subscribers remaining
	 *        <ol>
	 *          <li>swap COORDINATOR with EMPTY</li>
	 *          <li>COORDINATOR cancels its source</li>
	 *        </ol>
	 *    </li>
	 *  </ol>
	 * <p>
	 * The fact that COORDINATOR cancels its source when no more subscribers remain is important, because it prevents issues with a never() source
	 * or a source that never produces a value passing the predicate (assuming timeouts on the subscriber).
	 *
	 * @param invalidationPredicate the {@link Predicate} used for cache invalidation. Returning {@code true} means the value is invalid and should be
	 * removed from the cache.
	 * @return a new cached {@link Mono} which can be invalidated
	 */
	public final Mono<T> cacheInvalidateIf(Predicate<? super T> invalidationPredicate) {
		return onAssembly(new MonoCacheInvalidateIf<>(this, invalidationPredicate));
	}

	/**
	 * Cache {@link Subscriber#onNext(Object) onNext} signal received from the source and replay it to other subscribers,
	 * while allowing invalidation via a {@link Mono Mono&lt;Void&gt;} companion trigger generated from the currently
	 * cached value.
	 * <p>
	 * As this form of caching is explicitly value-oriented, empty source completion signals and error signals are NOT
	 * cached. It is always possible to use {@link #materialize()} to cache these (further using {@link #filter(Predicate)}
	 * if one wants to only consider empty sources or error sources). The exception is still propagated to the subscribers
	 * that have accumulated between the time the source has been subscribed to and the time the onError/onComplete terminal
	 * signal is received. An empty source is turned into a {@link NoSuchElementException} onError.
	 * <p>
	 * Completion of the trigger will invalidate the cached element, so the next subscriber that comes in will trigger
	 * a new subscription to the source, re-populating the cache and re-creating a new trigger out of that value.
	 * <p>
	 * <ul>
	 *     <li>
	 *         If the trigger completes with an error, all registered subscribers are terminated with the same error.
	 *     </li>
	 *     <li>
	 *         If all the subscribers are cancelled <strong>before</strong> the cache is populated (ie. an attempt to
	 *         cache a {@link Mono#never()}), the source subscription is cancelled.
	 *     </li>
	 *     <li>
	 *         Cancelling a downstream subscriber once the cache has been populated is not necessarily relevant,
	 *         as the value will be immediately replayed on subscription, which usually means within onSubscribe (so
	 *         earlier than any cancellation can happen). That said the operator will make best efforts to detect such
	 *         cancellations and avoid propagating the value to these subscribers.
	 *     </li>
	 * </ul>
	 * <p>
	 * If the cached value needs to be discarded in case of invalidation, use the {@link #cacheInvalidateWhen(Function, Consumer)} version.
	 * Note that some downstream subscribers might still be using or storing the value, for example if they
	 * haven't requested anything yet.
	 * <p>
	 * Trigger is generated only after a subscribers in the COORDINATOR have received the value, and only once.
	 * The only way to get out of the POPULATED state is to use the trigger, so there cannot be multiple trigger subscriptions, nor concurrent triggering.
	 * <p>
	 * Cancellation is only possible for downstream subscribers when they've been added to a COORDINATOR.
	 * Subscribers that are received when POPULATED will either be completed right away or (if the predicate fails) end up being added to a COORDINATOR.
	 * <p>
	 * When cancelling a COORDINATOR-issued subscription:
	 * <ol>
	 *    <li>removes itself from batch</li>
	 *    <li>if 0 subscribers remaining
	 *        <ol>
	 *          <li>swap COORDINATOR with EMPTY</li>
	 *          <li>COORDINATOR cancels its source</li>
	 *        </ol>
	 *    </li>
	 *  </ol>
	 * <p>
	 * The fact that COORDINATOR cancels its source when no more subscribers remain is important, because it prevents issues with a never() source
	 * or a source that never produces a value passing the predicate (assuming timeouts on the subscriber).
	 *
	 * @param invalidationTriggerGenerator the {@link Function} that generates new {@link Mono Mono&lt;Void&gt;} triggers
	 * used for invalidation
	 * @return a new cached {@link Mono} which can be invalidated
	 */
	public final Mono<T> cacheInvalidateWhen(Function<? super T, Mono<Void>> invalidationTriggerGenerator) {
		return onAssembly(new MonoCacheInvalidateWhen<>(this, invalidationTriggerGenerator, null));
	}

	/**
	 * Cache {@link Subscriber#onNext(Object) onNext} signal received from the source and replay it to other subscribers,
	 * while allowing invalidation via a {@link Mono Mono&lt;Void&gt;} companion trigger generated from the currently
	 * cached value.
	 * <p>
	 * As this form of caching is explicitly value-oriented, empty source completion signals and error signals are NOT
	 * cached. It is always possible to use {@link #materialize()} to cache these (further using {@link #filter(Predicate)}
	 * if one wants to only consider empty sources or error sources). The exception is still propagated to the subscribers
	 * that have accumulated between the time the source has been subscribed to and the time the onError/onComplete terminal
	 * signal is received. An empty source is turned into a {@link NoSuchElementException} onError.
	 * <p>
	 * Completion of the trigger will invalidate the cached element, so the next subscriber that comes in will trigger
	 * a new subscription to the source, re-populating the cache and re-creating a new trigger out of that value.
	 * <p>
	 * <ul>
	 *     <li>
	 *         If the trigger completes with an error, all registered subscribers are terminated with the same error.
	 *     </li>
	 *     <li>
	 *         If all the subscribers are cancelled <strong>before</strong> the cache is populated (ie. an attempt to
	 *         cache a {@link Mono#never()}), the source subscription is cancelled.
	 *     </li>
	 *     <li>
	 *         Cancelling a downstream subscriber once the cache has been populated is not necessarily relevant,
	 *         as the value will be immediately replayed on subscription, which usually means within onSubscribe (so
	 *         earlier than any cancellation can happen). That said the operator will make best efforts to detect such
	 *         cancellations and avoid propagating the value to these subscribers.
	 *     </li>
	 * </ul>
	 * <p>
	 * Once a cached value is invalidated, it is passed to the provided {@link Consumer} (which MUST complete normally).
	 * Note that some downstream subscribers might still be using or storing the value, for example if they
	 * haven't requested anything yet.
	 * <p>
	 * Trigger is generated only after a subscribers in the COORDINATOR have received the value, and only once.
	 * The only way to get out of the POPULATED state is to use the trigger, so there cannot be multiple trigger subscriptions, nor concurrent triggering.
	 * <p>
	 * Cancellation is only possible for downstream subscribers when they've been added to a COORDINATOR.
	 * Subscribers that are received when POPULATED will either be completed right away or (if the predicate fails) end up being added to a COORDINATOR.
	 * <p>
	 * When cancelling a COORDINATOR-issued subscription:
	 * <ol>
	 *    <li>removes itself from batch</li>
	 *    <li>if 0 subscribers remaining
	 *        <ol>
	 *          <li>swap COORDINATOR with EMPTY</li>
	 *          <li>COORDINATOR cancels its source</li>
	 *        </ol>
	 *    </li>
	 *  </ol>
	 * <p>
	 * The fact that COORDINATOR cancels its source when no more subscribers remain is important, because it prevents issues with a never() source
	 * or a source that never produces a value passing the predicate (assuming timeouts on the subscriber).
	 *
	 * @param invalidationTriggerGenerator the {@link Function} that generates new {@link Mono Mono&lt;Void&gt;} triggers
	 * used for invalidation
	 * @param onInvalidate the {@link Consumer} that will be applied to cached value upon invalidation
	 * @return a new cached {@link Mono} which can be invalidated
	 */
	public final Mono<T> cacheInvalidateWhen(Function<? super T, Mono<Void>> invalidationTriggerGenerator,
	                                         Consumer<? super T> onInvalidate) {
		return onAssembly(new MonoCacheInvalidateWhen<>(this, invalidationTriggerGenerator, onInvalidate));
	}

	/**
	 * Prepare this {@link Mono} so that subscribers will cancel from it on a
	 * specified
	 * {@link Scheduler}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/cancelOnForMono.svg" alt="">
	 *
	 * @param scheduler the {@link Scheduler} to signal cancel  on
	 *
	 * @return a scheduled cancel {@link Mono}
	 */
	public final Mono<T> cancelOn(Scheduler scheduler) {
		return onAssembly(new MonoCancelOn<>(this, scheduler));
	}

	/**
	 * Activate traceback (full assembly tracing) for this particular {@link Mono}, in case of an error
	 * upstream of the checkpoint. Tracing incurs the cost of an exception stack trace
	 * creation.
	 * <p>
	 * It should be placed towards the end of the reactive chain, as errors
	 * triggered downstream of it cannot be observed and augmented with assembly trace.
	 * <p>
	 * The traceback is attached to the error as a {@link Throwable#getSuppressed() suppressed exception}.
	 * As such, if the error is a {@link Exceptions#isMultiple(Throwable) composite one}, the traceback
	 * would appear as a component of the composite. In any case, the traceback nature can be detected via
	 * {@link Exceptions#isTraceback(Throwable)}.
	 *
	 * @return the assembly tracing {@link Mono}
	 */
	public final Mono<T> checkpoint() {
		return checkpoint(null, true);
	}

	/**
	 * Activate traceback (assembly marker) for this particular {@link Mono} by giving it a description that
	 * will be reflected in the assembly traceback in case of an error upstream of the
	 * checkpoint. Note that unlike {@link #checkpoint()}, this doesn't create a
	 * filled stack trace, avoiding the main cost of the operator.
	 * However, as a trade-off the description must be unique enough for the user to find
	 * out where this Mono was assembled. If you only want a generic description, and
	 * still rely on the stack trace to find the assembly site, use the
	 * {@link #checkpoint(String, boolean)} variant.
	 * <p>
	 * It should be placed towards the end of the reactive chain, as errors
	 * triggered downstream of it cannot be observed and augmented with assembly trace.
	 * <p>
	 * The traceback is attached to the error as a {@link Throwable#getSuppressed() suppressed exception}.
	 * As such, if the error is a {@link Exceptions#isMultiple(Throwable) composite one}, the traceback
	 * would appear as a component of the composite. In any case, the traceback nature can be detected via
	 * {@link Exceptions#isTraceback(Throwable)}.
	 *
	 * @param description a unique enough description to include in the light assembly traceback.
	 * @return the assembly marked {@link Mono}
	 */
	public final Mono<T> checkpoint(String description) {
		return checkpoint(Objects.requireNonNull(description), false);
	}

	/**
	 * Activate traceback (full assembly tracing or the lighter assembly marking depending on the
	 * {@code forceStackTrace} option).
	 * <p>
	 * By setting the {@code forceStackTrace} parameter to {@literal true}, activate assembly
	 * tracing for this particular {@link Mono} and give it a description that
	 * will be reflected in the assembly traceback in case of an error upstream of the
	 * checkpoint. Note that unlike {@link #checkpoint(String)}, this will incur
	 * the cost of an exception stack trace creation. The description could for
	 * example be a meaningful name for the assembled mono or a wider correlation ID,
	 * since the stack trace will always provide enough information to locate where this
	 * Flux was assembled.
	 * <p>
	 * By setting {@code forceStackTrace} to {@literal false}, behaves like
	 * {@link #checkpoint(String)} and is subject to the same caveat in choosing the
	 * description.
	 * <p>
	 * It should be placed towards the end of the reactive chain, as errors
	 * triggered downstream of it cannot be observed and augmented with assembly marker.
	 * <p>
	 * The traceback is attached to the error as a {@link Throwable#getSuppressed() suppressed exception}.
	 * As such, if the error is a {@link Exceptions#isMultiple(Throwable) composite one}, the traceback
	 * would appear as a component of the composite. In any case, the traceback nature can be detected via
	 * {@link Exceptions#isTraceback(Throwable)}.
	 *
	 * @param description a description (must be unique enough if forceStackTrace is set
	 * to false).
	 * @param forceStackTrace false to make a light checkpoint without a stacktrace, true
	 * to use a stack trace.
	 * @return the assembly marked {@link Mono}.
	 */
	public final Mono<T> checkpoint(@Nullable String description, boolean forceStackTrace) {
		final AssemblySnapshot stacktrace;
		if (!forceStackTrace) {
			stacktrace = new CheckpointLightSnapshot(description);
		}
		else {
			stacktrace = new CheckpointHeavySnapshot(description, Traces.callSiteSupplierFactory.get());
		}

		return new MonoOnAssembly<>(this, stacktrace);
	}

	/**
	 * Concatenate emissions of this {@link Mono} with the provided {@link Publisher}
	 * (no interleave).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatWithForMono.svg" alt="">
	 *
	 * @param other the {@link Publisher} sequence to concat after this {@link Flux}
	 *
	 * @return a concatenated {@link Flux}
	 */
	public final Flux<T> concatWith(Publisher<? extends T> other) {
		return Flux.concat(this, other);
	}

	/**
	 * Enrich the {@link Context} visible from downstream for the benefit of upstream
	 * operators, by making all values from the provided {@link ContextView} visible on top
	 * of pairs from downstream.
	 * <p>
	 * A {@link Context} (and its {@link ContextView}) is tied to a given subscription
	 * and is read by querying the downstream {@link Subscriber}. {@link Subscriber} that
	 * don't enrich the context instead access their own downstream's context. As a result,
	 * this operator conceptually enriches a {@link Context} coming from under it in the chain
	 * (downstream, by default an empty one) and makes the new enriched {@link Context}
	 * visible to operators above it in the chain.
	 *
	 * @param contextToAppend the {@link ContextView} to merge with the downstream {@link Context},
	 * resulting in a new more complete {@link Context} that will be visible from upstream.
	 *
	 * @return a contextualized {@link Mono}
	 * @see ContextView
	 */
	public final Mono<T> contextWrite(ContextView contextToAppend) {
		return contextWrite(c -> c.putAll(contextToAppend));
	}

	/**
	 * Enrich the {@link Context} visible from downstream for the benefit of upstream
	 * operators, by applying a {@link Function} to the downstream {@link Context}.
	 * <p>
	 * The {@link Function} takes a {@link Context} for convenience, allowing to easily
	 * call {@link Context#put(Object, Object) write APIs} to return a new {@link Context}.
	 * <p>
	 * A {@link Context} (and its {@link ContextView}) is tied to a given subscription
	 * and is read by querying the downstream {@link Subscriber}. {@link Subscriber} that
	 * don't enrich the context instead access their own downstream's context. As a result,
	 * this operator conceptually enriches a {@link Context} coming from under it in the chain
	 * (downstream, by default an empty one) and makes the new enriched {@link Context}
	 * visible to operators above it in the chain.
	 *
	 * @param contextModifier the {@link Function} to apply to the downstream {@link Context},
	 * resulting in a new more complete {@link Context} that will be visible from upstream.
	 *
	 * @return a contextualized {@link Mono}
	 * @see Context
	 */
	public final Mono<T> contextWrite(Function<Context, Context> contextModifier) {
		return onAssembly(new MonoContextWrite<>(this, contextModifier));
	}

	/**
	 * Provide a default single value if this mono is completed without any data
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/defaultIfEmpty.svg" alt="">
	 * <p>
	 * @param defaultV the alternate value if this sequence is empty
	 *
	 * @return a new {@link Mono}
	 *
	 * @see Flux#defaultIfEmpty(Object)
	 */
	public final Mono<T> defaultIfEmpty(T defaultV) {
	    if (this instanceof Fuseable.ScalarCallable) {
		    try {
			    T v = block();
			    if (v == null) {
				    return Mono.just(defaultV);
			    }
		    }
		    catch (Throwable e) {
			    //leave MonoError returns as this
		    }
		    return this;
	    }
		return onAssembly(new MonoDefaultIfEmpty<>(this, defaultV));
	}

	/**
	 * Delay this {@link Mono} element ({@link Subscriber#onNext} signal) by a given
	 * duration. Empty Monos or error signals are not delayed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delayElement.svg" alt="">
	 *
	 * <p>
	 * Note that the scheduler on which the Mono chain continues execution will be the
	 * {@link Schedulers#parallel() parallel} scheduler if the mono is valued, or the
	 * current scheduler if the mono completes empty or errors.
	 *
	 * @param delay duration by which to delay the {@link Subscriber#onNext} signal
	 * @return a delayed {@link Mono}
	 */
	public final Mono<T> delayElement(Duration delay) {
		return delayElement(delay, Schedulers.parallel());
	}

	/**
	 * Delay this {@link Mono} element ({@link Subscriber#onNext} signal) by a given
	 * {@link Duration}, on a particular {@link Scheduler}. Empty monos or error signals are not delayed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delayElement.svg" alt="">
	 *
	 * <p>
	 * Note that the scheduler on which the mono chain continues execution will be the
	 * scheduler provided if the mono is valued, or the current scheduler if the mono
	 * completes empty or errors.
	 *
	 * @param delay {@link Duration} by which to delay the {@link Subscriber#onNext} signal
	 * @param timer a time-capable {@link Scheduler} instance to delay the value signal on
	 * @return a delayed {@link Mono}
	 */
	public final Mono<T> delayElement(Duration delay, Scheduler timer) {
		return onAssembly(new MonoDelayElement<>(this, delay.toNanos(), TimeUnit.NANOSECONDS, timer));
	}

	/**
	 * Subscribe to this {@link Mono} and another {@link Publisher} that is generated from
	 * this Mono's element and which will be used as a trigger for relaying said element.
	 * <p>
	 * That is to say, the resulting {@link Mono} delays until this Mono's element is
	 * emitted, generates a trigger Publisher and then delays again until the trigger
	 * Publisher terminates.
	 * <p>
	 * Note that contiguous calls to all delayUntil are fused together.
	 * The triggers are generated and subscribed to in sequence, once the previous trigger
	 * completes. Error is propagated immediately
	 * downstream. In both cases, an error in the source is immediately propagated.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delayUntilForMono.svg" alt="">
	 *
	 * @param triggerProvider a {@link Function} that maps this Mono's value into a
	 * {@link Publisher} whose termination will trigger relaying the value.
	 *
	 * @return this Mono, but delayed until the derived publisher terminates.
	 */
	public final Mono<T> delayUntil(Function<? super T, ? extends Publisher<?>> triggerProvider) {
		Objects.requireNonNull(triggerProvider, "triggerProvider required");
		if (this instanceof MonoDelayUntil) {
			return ((MonoDelayUntil<T>) this).copyWithNewTriggerGenerator(false,triggerProvider);
		}
		return onAssembly(new MonoDelayUntil<>(this, triggerProvider));
	}

	/**
	 * Delay the {@link Mono#subscribe(Subscriber) subscription} to this {@link Mono} source until the given
	 * period elapses.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delaySubscriptionForMono.svg" alt="">
	 *
	 * @param delay duration before subscribing this {@link Mono}
	 *
	 * @return a delayed {@link Mono}
	 *
	 */
	public final Mono<T> delaySubscription(Duration delay) {
		return delaySubscription(delay, Schedulers.parallel());
	}

	/**
	 * Delay the {@link Mono#subscribe(Subscriber) subscription} to this {@link Mono} source until the given
	 * {@link Duration} elapses.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delaySubscriptionForMono.svg" alt="">
	 *
	 * @param delay {@link Duration} before subscribing this {@link Mono}
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a delayed {@link Mono}
	 *
	 */
	public final Mono<T> delaySubscription(Duration delay, Scheduler timer) {
		return delaySubscription(Mono.delay(delay, timer));
	}

	/**
	 * Delay the subscription to this {@link Mono} until another {@link Publisher}
	 * signals a value or completes.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delaySubscriptionWithPublisherForMono.svg" alt="">
	 *
	 * @param subscriptionDelay a
	 * {@link Publisher} to signal by next or complete this {@link Mono#subscribe(Subscriber)}
	 * @param <U> the other source type
	 *
	 * @return a delayed {@link Mono}
	 *
	 */
	public final <U> Mono<T> delaySubscription(Publisher<U> subscriptionDelay) {
		return onAssembly(new MonoDelaySubscription<>(this, subscriptionDelay));
	}

	/**
	 * An operator working only if this {@link Mono} emits onNext, onError or onComplete {@link Signal}
	 * instances, transforming these {@link #materialize() materialized} signals into
	 * real signals on the {@link Subscriber}.
	 * The error {@link Signal} will trigger onError and complete {@link Signal} will trigger
	 * onComplete.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/dematerializeForMono.svg" alt="">
	 *
	 * @param <X> the dematerialized type
	 * @return a dematerialized {@link Mono}
	 * @see #materialize()
	 */
	public final <X> Mono<X> dematerialize() {
		@SuppressWarnings("unchecked")
		Mono<Signal<X>> thiz = (Mono<Signal<X>>) this;
		return onAssembly(new MonoDematerialize<>(thiz));
	}

	/**
	 * Add behavior triggered after the {@link Mono} terminates, either by completing downstream successfully or with an error.
	 * The arguments will be null depending on success, success with data and error:
	 * <ul>
	 *     <li>null, null : completed without data</li>
	 *     <li>T, null : completed with data</li>
	 *     <li>null, Throwable : failed with/without data</li>
	 * </ul>
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doAfterSuccessOrError.svg" alt="">
	 * <p>
	 * The relevant signal is propagated downstream, then the {@link BiConsumer} is executed.
	 *
	 * @param afterSuccessOrError the callback to call after {@link Subscriber#onNext}, {@link Subscriber#onComplete} without preceding {@link Subscriber#onNext} or {@link Subscriber#onError}
	 *
	 * @return a new {@link Mono}
	 * @deprecated prefer using {@link #doAfterTerminate(Runnable)} or {@link #doFinally(Consumer)}. will be removed in 3.5.0
	 */
	@Deprecated
	public final Mono<T> doAfterSuccessOrError(BiConsumer<? super T, Throwable> afterSuccessOrError) {
		return doOnTerminalSignal(this, null, null, afterSuccessOrError);
	}

	/**
	 * Add behavior (side-effect) triggered after the {@link Mono} terminates, either by
	 * completing downstream successfully or with an error.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doAfterTerminateForMono.svg" alt="">
	 * <p>
	 * The relevant signal is propagated downstream, then the {@link Runnable} is executed.
	 *
	 * @param afterTerminate the callback to call after {@link Subscriber#onComplete} or {@link Subscriber#onError}
	 *
	 * @return an observed  {@link Mono}
	 */
	public final Mono<T> doAfterTerminate(Runnable afterTerminate) {
		Objects.requireNonNull(afterTerminate, "afterTerminate");
		return onAssembly(new MonoPeekTerminal<>(this, null, null, (s, e)  -> afterTerminate.run()));
	}

	/**
	 * Add behavior (side-effect) triggered <strong>before</strong> the {@link Mono} is
	 * <strong>subscribed to</strong>, which should be the first event after assembly time.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doFirstForMono.svg" alt="">
	 * <p>
	 * Note that when several {@link #doFirst(Runnable)} operators are used anywhere in a
	 * chain of operators, their order of execution is reversed compared to the declaration
	 * order (as subscribe signal flows backward, from the ultimate subscriber to the source
	 * publisher):
	 * <pre><code>
	 * Mono.just(1v)
	 *     .doFirst(() -> System.out.println("three"))
	 *     .doFirst(() -> System.out.println("two"))
	 *     .doFirst(() -> System.out.println("one"));
	 * //would print one two three
	 * </code>
	 * </pre>
	 * <p>
	 * In case the {@link Runnable} throws an exception, said exception will be directly
	 * propagated to the subscribing {@link Subscriber} along with a no-op {@link Subscription},
	 * similarly to what {@link #error(Throwable)} does. Otherwise, after the handler has
	 * executed, the {@link Subscriber} is directly subscribed to the original source
	 * {@link Mono} ({@code this}).
	 * <p>
	 * This side-effect method provides stronger <i>first</i> guarantees compared to
	 * {@link #doOnSubscribe(Consumer)}, which is triggered once the {@link Subscription}
	 * has been set up and passed to the {@link Subscriber}.
	 *
	 * @param onFirst the callback to execute before the {@link Mono} is subscribed to
	 * @return an observed {@link Mono}
	 */
	public final Mono<T> doFirst(Runnable onFirst) {
		Objects.requireNonNull(onFirst, "onFirst");
		if (this instanceof Fuseable) {
			return onAssembly(new MonoDoFirstFuseable<>(this, onFirst));
		}
		return onAssembly(new MonoDoFirst<>(this, onFirst));
	}

	/**
	 * Add behavior triggering <strong>after</strong> the {@link Mono} terminates for any reason,
	 * including cancellation. The terminating event ({@link SignalType#ON_COMPLETE},
	 * {@link SignalType#ON_ERROR} and {@link SignalType#CANCEL}) is passed to the consumer,
	 * which is executed after the signal has been passed downstream.
	 * <p>
	 * Note that the fact that the signal is propagated downstream before the callback is
	 * executed means that several doFinally in a row will be executed in
	 * <strong>reverse order</strong>. If you want to assert the execution of the callback
	 * please keep in mind that the Mono will complete before it is executed, so its
	 * effect might not be visible immediately after eg. a {@link #block()}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doFinallyForMono.svg" alt="">
	 *
	 *
	 * @param onFinally the callback to execute after a terminal signal (complete, error
	 * or cancel)
	 * @return an observed {@link Mono}
	 */
	public final Mono<T> doFinally(Consumer<SignalType> onFinally) {
		Objects.requireNonNull(onFinally, "onFinally");
		if (this instanceof Fuseable) {
			return onAssembly(new MonoDoFinallyFuseable<>(this, onFinally));
		}
		return onAssembly(new MonoDoFinally<>(this, onFinally));
	}

	/**
	 * Add behavior triggered when the {@link Mono} is cancelled.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnCancelForMono.svg" alt="">
	 * <p>
	 * The handler is executed first, then the cancel signal is propagated upstream
	 * to the source.
	 *
	 * @param onCancel the callback to call on {@link Subscription#cancel()}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnCancel(Runnable onCancel) {
		Objects.requireNonNull(onCancel, "onCancel");
		return doOnSignal(this, null, null, null, onCancel);
	}

	/**
	 * Potentially modify the behavior of the <i>whole chain</i> of operators upstream of this one to
	 * conditionally clean up elements that get <i>discarded</i> by these operators.
	 * <p>
	 * The {@code discardHook} MUST be idempotent and safe to use on any instance of the desired
	 * type.
	 * Calls to this method are additive, and the order of invocation of the {@code discardHook}
	 * is the same as the order of declaration (calling {@code .filter(...).doOnDiscard(first).doOnDiscard(second)}
	 * will let the filter invoke {@code first} then {@code second} handlers).
	 * <p>
	 * Two main categories of discarding operators exist:
	 * <ul>
	 *     <li>filtering operators, dropping some source elements as part of their designed behavior</li>
	 *     <li>operators that prefetch a few elements and keep them around pending a request, but get cancelled/in error</li>
	 * </ul>
	 * WARNING: Not all operators support this instruction. The ones that do are identified in the javadoc by
	 * the presence of a <strong>Discard Support</strong> section.
	 *
	 * @param type the {@link Class} of elements in the upstream chain of operators that
	 * this cleanup hook should take into account.
	 * @param discardHook a {@link Consumer} of elements in the upstream chain of operators
	 * that performs the cleanup.
	 * @return a {@link Mono} that cleans up matching elements that get discarded upstream of it.
	 */
	public final <R> Mono<T> doOnDiscard(final Class<R> type, final Consumer<? super R> discardHook) {
		return subscriberContext(Operators.discardLocalAdapter(type, discardHook));
	}

	/**
	 * Add behavior triggered when the {@link Mono} emits a data successfully.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnNextForMono.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onNext signal is propagated
	 * downstream.
	 *
	 * @param onNext the callback to call on {@link Subscriber#onNext}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnNext(Consumer<? super T> onNext) {
		Objects.requireNonNull(onNext, "onNext");
		return doOnSignal(this, null, onNext, null, null);
	}

	/**
	 * Add behavior triggered as soon as the {@link Mono} can be considered to have completed successfully.
	 * The value passed to the {@link Consumer} reflects the type of completion:
	 *
	 * <ul>
	 *     <li>null : completed without data. handler is executed right before onComplete is propagated downstream</li>
	 *     <li>T: completed with data. handler is executed right before onNext is propagated downstream</li>
	 * </ul>
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnSuccess.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed before propagating either onNext or onComplete downstream.
	 *
	 * @param onSuccess the callback to call on, argument is null if the {@link Mono}
	 * completes without data
	 * {@link Subscriber#onNext} or {@link Subscriber#onComplete} without preceding {@link Subscriber#onNext}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnSuccess(Consumer<? super T> onSuccess) {
		Objects.requireNonNull(onSuccess, "onSuccess");
		return doOnTerminalSignal(this, onSuccess, null, null);
	}

	/**
	 * Add behavior triggered when the {@link Mono} emits an item, fails with an error
	 * or completes successfully. All these events are represented as a {@link Signal}
	 * that is passed to the side-effect callback. Note that this is an advanced operator,
	 * typically used for monitoring of a Mono.
	 * These {@link Signal} have a {@link Context} associated to them.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnEachForMono.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the relevant signal is propagated
	 * downstream.
	 *
	 * @param signalConsumer the mandatory callback to call on
	 *   {@link Subscriber#onNext(Object)}, {@link Subscriber#onError(Throwable)} and
	 *   {@link Subscriber#onComplete()}
	 * @return an observed {@link Mono}
	 * @see #doOnNext(Consumer)
	 * @see #doOnError(Consumer)
	 * @see #materialize()
	 * @see Signal
	 */
	public final Mono<T> doOnEach(Consumer<? super Signal<T>> signalConsumer) {
		Objects.requireNonNull(signalConsumer, "signalConsumer");
		if (this instanceof Fuseable) {
			return onAssembly(new MonoDoOnEachFuseable<>(this, signalConsumer));
		}
		return onAssembly(new MonoDoOnEach<>(this, signalConsumer));

	}

	/**
	 * Add behavior triggered when the {@link Mono} completes with an error.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnErrorForMono.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onError signal is propagated
	 * downstream.
	 *
	 * @param onError the error callback to call on {@link Subscriber#onError(Throwable)}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnError(Consumer<? super Throwable> onError) {
		Objects.requireNonNull(onError, "onError");
		return doOnTerminalSignal(this, null, onError, null);
	}


	/**
	 * Add behavior triggered when the {@link Mono} completes with an error matching the given exception type.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnErrorWithClassPredicateForMono.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onError signal is propagated
	 * downstream.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param onError the error handler for relevant errors
	 * @param <E> type of the error to handle
	 *
	 * @return an observed  {@link Mono}
	 *
	 */
	public final <E extends Throwable> Mono<T> doOnError(Class<E> exceptionType,
			final Consumer<? super E> onError) {
		Objects.requireNonNull(exceptionType, "type");
		Objects.requireNonNull(onError, "onError");
		return doOnTerminalSignal(this, null,
				error -> {
					if (exceptionType.isInstance(error)) onError.accept(exceptionType.cast(error));
				},
				null);
	}

	/**
	 * Add behavior triggered when the {@link Mono} completes with an error matching the given predicate.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnErrorWithPredicateForMono.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onError signal is propagated
	 * downstream.
	 *
	 * @param predicate the matcher for exceptions to handle
	 * @param onError the error handler for relevant error
	 *
	 * @return an observed  {@link Mono}
	 *
	 */
	public final Mono<T> doOnError(Predicate<? super Throwable> predicate,
			final Consumer<? super Throwable> onError) {
		Objects.requireNonNull(predicate, "predicate");
		Objects.requireNonNull(onError, "onError");
		return doOnTerminalSignal(this, null,
				error -> {
					if (predicate.test(error)) onError.accept(error);
				},
				null);
	}
	/**
	 * Add behavior triggering a {@link LongConsumer} when the {@link Mono} receives any request.
	 * <p>
	 *     Note that non fatal error raised in the callback will not be propagated and
	 *     will simply trigger {@link Operators#onOperatorError(Throwable, Context)}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnRequestForMono.svg" alt="">
	 * <p>
	 * The {@link LongConsumer} is executed first, then the request signal is propagated
	 * upstream to the parent.
	 *
	 * @param consumer the consumer to invoke on each request
	 *
	 * @return an observed  {@link Mono}
	 */
	public final Mono<T> doOnRequest(final LongConsumer consumer) {
		Objects.requireNonNull(consumer, "consumer");
		return doOnSignal(this, null, null, consumer, null);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Mono} is being subscribed,
	 * that is to say when a {@link Subscription} has been produced by the {@link Publisher}
	 * and is being passed to the {@link Subscriber#onSubscribe(Subscription)}.
	 * <p>
	 * This method is <strong>not</strong> intended for capturing the subscription and calling its methods,
	 * but for side effects like monitoring. For instance, the correct way to cancel a subscription is
	 * to call {@link Disposable#dispose()} on the Disposable returned by {@link Mono#subscribe()}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnSubscribe.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the {@link Subscription} is propagated
	 * downstream to the next subscriber in the chain that is being established.
	 *
	 * @param onSubscribe the callback to call on {@link Subscriber#onSubscribe(Subscription)}
	 *
	 * @return a new {@link Mono}
	 * @see #doFirst(Runnable)
	 */
	public final Mono<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		Objects.requireNonNull(onSubscribe, "onSubscribe");
		return doOnSignal(this, onSubscribe, null, null,  null);
	}

	/**
	 * Add behavior triggered when the {@link Mono} terminates, either by emitting a value,
	 * completing empty or failing with an error.
	 * The value passed to the {@link Consumer} reflects the type of completion:
	 * <ul>
	 *     <li>null, null : completing without data. handler is executed right before onComplete is propagated downstream</li>
	 *     <li>T, null : completing with data. handler is executed right before onNext is propagated downstream</li>
	 *     <li>null, Throwable : failing. handler is executed right before onError is propagated downstream</li>
	 * </ul>
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnSuccessOrError.svg" alt="">
	 * <p>
	 * The {@link BiConsumer} is executed before propagating either onNext, onComplete or onError downstream.
	 *
	 * @param onSuccessOrError the callback to call {@link Subscriber#onNext}, {@link Subscriber#onComplete} without preceding {@link Subscriber#onNext} or {@link Subscriber#onError}
	 *
	 * @return a new {@link Mono}
	 * @deprecated prefer using {@link #doOnNext(Consumer)}, {@link #doOnError(Consumer)}, {@link #doOnTerminate(Runnable)} or {@link #doOnSuccess(Consumer)}. will be removed in 3.5.0
	 */
	@Deprecated
	public final Mono<T> doOnSuccessOrError(BiConsumer<? super T, Throwable> onSuccessOrError) {
		Objects.requireNonNull(onSuccessOrError, "onSuccessOrError");
		return doOnTerminalSignal(this, v -> onSuccessOrError.accept(v, null), e -> onSuccessOrError.accept(null, e), null);
	}

	/**
	 * Add behavior triggered when the {@link Mono} terminates, either by completing with a value,
	 * completing empty or failing with an error. Unlike in {@link Flux#doOnTerminate(Runnable)},
	 * the simple fact that a {@link Mono} emits {@link Subscriber#onNext(Object) onNext} implies
	 * completion, so the handler is invoked BEFORE the element is propagated (same as with {@link #doOnSuccess(Consumer)}).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnTerminateForMono.svg" alt=""><p>
	 * The {@link Runnable} is executed first, then the onNext/onComplete/onError signal is propagated
	 * downstream.
	 *
	 * @param onTerminate the callback to call {@link Subscriber#onNext}, {@link Subscriber#onComplete} without preceding {@link Subscriber#onNext} or {@link Subscriber#onError}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnTerminate(Runnable onTerminate) {
		Objects.requireNonNull(onTerminate, "onTerminate");
		return doOnTerminalSignal(this, ignoreValue -> onTerminate.run(), ignoreError -> onTerminate.run(), null);
	}

	/**
	 * Map this {@link Mono} into {@link reactor.util.function.Tuple2 Tuple2&lt;Long, T&gt;}
	 * of timemillis and source data. The timemillis corresponds to the elapsed time between
	 * the subscribe and the first next signal, as measured by the {@link Schedulers#parallel() parallel} scheduler.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/elapsedForMono.svg" alt="">
	 *
	 * @return a new {@link Mono} that emits a tuple of time elapsed in milliseconds and matching data
	 * @see #timed()
	 */
	public final Mono<Tuple2<Long, T>> elapsed() {
		return elapsed(Schedulers.parallel());
	}

	/**
	 * Map this {@link Mono} sequence into {@link reactor.util.function.Tuple2 Tuple2&lt;Long, T&gt;}
	 * of timemillis and source data. The timemillis corresponds to the elapsed time between the subscribe and the first
	 * next signal, as measured by the provided {@link Scheduler}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/elapsedForMono.svg" alt="">
	 *
	 * @param scheduler a {@link Scheduler} instance to read time from
	 * @return a new {@link Mono} that emits a tuple of time elapsed in milliseconds and matching data
	 * @see #timed(Scheduler)
	 */
	public final Mono<Tuple2<Long, T>> elapsed(Scheduler scheduler) {
		Objects.requireNonNull(scheduler, "scheduler");
		return onAssembly(new MonoElapsed<>(this, scheduler));
	}

	/**
	 * Recursively expand elements into a graph and emit all the resulting element,
	 * in a depth-first traversal order.
	 * <p>
	 * That is: emit the value from this {@link Mono}, expand it and emit the first value
	 * at this first level of recursion, and so on... When no more recursion is possible,
	 * backtrack to the previous level and re-apply the strategy.
	 * <p>
	 * For example, given the hierarchical structure
	 * <pre>
	 *  A
	 *   - AA
	 *     - aa1
	 *   - AB
	 *     - ab1
	 *   - a1
	 * </pre>
	 *
	 * Expands {@code Mono.just(A)} into
	 * <pre>
	 *  A
	 *  AA
	 *  aa1
	 *  AB
	 *  ab1
	 *  a1
	 * </pre>
	 *
	 * @param expander the {@link Function} applied at each level of recursion to expand
	 * values into a {@link Publisher}, producing a graph.
	 * @param capacityHint a capacity hint to prepare the inner queues to accommodate n
	 * elements per level of recursion.
	 *
	 * @return this Mono expanded depth-first to a {@link Flux}
	 */
	public final Flux<T> expandDeep(Function<? super T, ? extends Publisher<? extends T>> expander,
			int capacityHint) {
		return Flux.onAssembly(new MonoExpand<>(this, expander, false, capacityHint));
	}

	/**
	 * Recursively expand elements into a graph and emit all the resulting element,
	 * in a depth-first traversal order.
	 * <p>
	 * That is: emit the value from this {@link Mono}, expand it and emit the first value
	 * at this first level of recursion, and so on... When no more recursion is possible,
	 * backtrack to the previous level and re-apply the strategy.
	 * <p>
	 * For example, given the hierarchical structure
	 * <pre>
	 *  A
	 *   - AA
	 *     - aa1
	 *   - AB
	 *     - ab1
	 *   - a1
	 * </pre>
	 *
	 * Expands {@code Mono.just(A)} into
	 * <pre>
	 *  A
	 *  AA
	 *  aa1
	 *  AB
	 *  ab1
	 *  a1
	 * </pre>
	 *
	 * @param expander the {@link Function} applied at each level of recursion to expand
	 * values into a {@link Publisher}, producing a graph.
	 *
	 * @return this Mono expanded depth-first to a {@link Flux}
	 */
	public final Flux<T> expandDeep(Function<? super T, ? extends Publisher<? extends T>> expander) {
		return expandDeep(expander, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Recursively expand elements into a graph and emit all the resulting element using
	 * a breadth-first traversal strategy.
	 * <p>
	 * That is: emit the value from this {@link Mono} first, then expand it at a first level of
	 * recursion and emit all of the resulting values, then expand all of these at a
	 * second level and so on...
	 * <p>
	 * For example, given the hierarchical structure
	 * <pre>
	 *  A
	 *   - AA
	 *     - aa1
	 *   - AB
	 *     - ab1
	 *   - a1
	 * </pre>
	 *
	 * Expands {@code Mono.just(A)} into
	 * <pre>
	 *  A
	 *  AA
	 *  AB
	 *  a1
	 *  aa1
	 *  ab1
	 * </pre>
	 *
	 * @param expander the {@link Function} applied at each level of recursion to expand
	 * values into a {@link Publisher}, producing a graph.
	 * @param capacityHint a capacity hint to prepare the inner queues to accommodate n
	 * elements per level of recursion.
	 *
	 * @return this Mono expanded breadth-first to a {@link Flux}
	 */
	public final Flux<T> expand(Function<? super T, ? extends Publisher<? extends T>> expander,
			int capacityHint) {
		return Flux.onAssembly(new MonoExpand<>(this, expander, true, capacityHint));
	}

	/**
	 * Recursively expand elements into a graph and emit all the resulting element using
	 * a breadth-first traversal strategy.
	 * <p>
	 * That is: emit the value from this {@link Mono} first, then expand it at a first level of
	 * recursion and emit all of the resulting values, then expand all of these at a
	 * second level and so on...
	 * <p>
	 * For example, given the hierarchical structure
	 * <pre>
	 *  A
	 *   - AA
	 *     - aa1
	 *   - AB
	 *     - ab1
	 *   - a1
	 * </pre>
	 *
	 * Expands {@code Mono.just(A)} into
	 * <pre>
	 *  A
	 *  AA
	 *  AB
	 *  a1
	 *  aa1
	 *  ab1
	 * </pre>
	 *
	 * @param expander the {@link Function} applied at each level of recursion to expand
	 * values into a {@link Publisher}, producing a graph.
	 *
	 * @return this Mono expanded breadth-first to a {@link Flux}
	 */
	public final Flux<T> expand(Function<? super T, ? extends Publisher<? extends T>> expander) {
		return expand(expander, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * If this {@link Mono} is valued, test the result and replay it if predicate returns true.
	 * Otherwise complete without value.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/filterForMono.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the element if it does not match the filter. It
	 * also discards upon cancellation or error triggered by a data signal.
	 *
	 * @param tester the predicate to evaluate
	 *
	 * @return a filtered {@link Mono}
	 */
	public final Mono<T> filter(final Predicate<? super T> tester) {
		if (this instanceof Fuseable) {
			return onAssembly(new MonoFilterFuseable<>(this, tester));
		}
		return onAssembly(new MonoFilter<>(this, tester));
	}

	/**
	 * If this {@link Mono} is valued, test the value asynchronously using a generated
	 * {@code Publisher<Boolean>} test. The value from the Mono is replayed if the
	 * first item emitted by the test is {@literal true}. It is dropped if the test is
	 * either empty or its first emitted value is {@literal false}.
	 * <p>
	 * Note that only the first value of the test publisher is considered, and unless it
	 * is a {@link Mono}, test will be cancelled after receiving that first value.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/filterWhenForMono.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the element if it does not match the filter. It
	 * also discards upon cancellation or error triggered by a data signal.
	 *
	 * @param asyncPredicate the function generating a {@link Publisher} of {@link Boolean}
	 * to filter the Mono with
	 *
	 * @return a filtered {@link Mono}
	 */
	public final Mono<T> filterWhen(Function<? super T, ? extends Publisher<Boolean>> asyncPredicate) {
		return onAssembly(new MonoFilterWhen<>(this, asyncPredicate));
	}

	/**
	 * Transform the item emitted by this {@link Mono} asynchronously, returning the
	 * value emitted by another {@link Mono} (possibly changing the value type).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapForMono.svg" alt="">
	 *
	 * @param transformer the function to dynamically bind a new {@link Mono}
	 * @param <R> the result type bound
	 *
	 * @return a new {@link Mono} with an asynchronously mapped value.
	 */
	public final <R> Mono<R> flatMap(Function<? super T, ? extends Mono<? extends R>>
			transformer) {
		return onAssembly(new MonoFlatMap<>(this, transformer));
	}

	/**
	 * Transform the item emitted by this {@link Mono} into a Publisher, then forward
	 * its emissions into the returned {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapMany.svg" alt="">
	 *
	 * @param mapper the
	 * {@link Function} to produce a sequence of R from the eventual passed {@link Subscriber#onNext}
	 * @param <R> the merged sequence type
	 *
	 * @return a new {@link Flux} as the sequence is not guaranteed to be single at most
	 */
	public final <R> Flux<R> flatMapMany(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return Flux.onAssembly(new MonoFlatMapMany<>(this, mapper));
	}

	/**
	 * Transform the signals emitted by this {@link Mono} into signal-specific Publishers,
	 * then forward the applicable Publisher's emissions into the returned {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapManyWithMappersOnTerminalEvents.svg" alt="">
	 *
	 * @param mapperOnNext the {@link Function} to call on next data and returning a sequence to merge
	 * @param mapperOnError the {@link Function} to call on error signal and returning a sequence to merge
	 * @param mapperOnComplete the {@link Function} to call on complete signal and returning a sequence to merge
	 * @param <R> the type of the produced inner sequence
	 *
	 * @return a new {@link Flux} as the sequence is not guaranteed to be single at most
	 *
	 * @see Flux#flatMap(Function, Function, Supplier)
	 */
	public final <R> Flux<R> flatMapMany(Function<? super T, ? extends Publisher<? extends R>> mapperOnNext,
			Function<? super Throwable, ? extends Publisher<? extends R>> mapperOnError,
			Supplier<? extends Publisher<? extends R>> mapperOnComplete) {
		return flux().flatMap(mapperOnNext, mapperOnError, mapperOnComplete);
	}

	/**
	 * Transform the item emitted by this {@link Mono} into {@link Iterable}, then forward
	 * its elements into the returned {@link Flux}.
	 * The {@link Iterable#iterator()} method will be called at least once and at most twice.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapIterableForMono.svg" alt="">
	 * <p>
	 * This operator inspects each {@link Iterable}'s {@link Spliterator} to assess if the iteration
	 * can be guaranteed to be finite (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 * Since the default Spliterator wraps the Iterator we can have two {@link Iterable#iterator()}
	 * calls per iterable. This second invocation is skipped on a {@link Collection } however, a type which is
	 * assumed to be always finite.
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator discards {@code T} elements it prefetched and, in
	 * some cases, attempts to discard remainder of the currently processed {@link Iterable} (if it can
	 * safely ensure the iterator is finite). Note that this means each {@link Iterable}'s {@link Iterable#iterator()}
	 * method could be invoked twice.
	 *
	 * @param mapper the {@link Function} to transform input item into a sequence {@link Iterable}
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}
	 *
	 */
	public final <R> Flux<R> flatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper) {
		return Flux.onAssembly(new MonoFlattenIterable<>(this, mapper, Integer
				.MAX_VALUE, Queues.one()));
	}

	/**
	 * Convert this {@link Mono} to a {@link Flux}
	 *
	 * @return a {@link Flux} variant of this {@link Mono}
	 */
    public final Flux<T> flux() {
	    if (this instanceof Callable && !(this instanceof Fuseable.ScalarCallable)) {
		    @SuppressWarnings("unchecked") Callable<T> thiz = (Callable<T>) this;
		    return Flux.onAssembly(new FluxCallable<>(thiz));
	    }
		return Flux.from(this);
	}

	/**
	 * Emit a single boolean true if this {@link Mono} has an element.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/hasElementForMono.svg" alt="">
	 *
	 * @return a new {@link Mono} with <code>true</code> if a value is emitted and <code>false</code>
	 * otherwise
	 */
	public final Mono<Boolean> hasElement() {
		return onAssembly(new MonoHasElement<>(this));
	}

	/**
	 * Handle the items emitted by this {@link Mono} by calling a biconsumer with the
	 * output sink for each onNext. At most one {@link SynchronousSink#next(Object)}
	 * call must be performed and/or 0 or 1 {@link SynchronousSink#error(Throwable)} or
	 * {@link SynchronousSink#complete()}.
	 *
	 * @param handler the handling {@link BiConsumer}
	 * @param <R> the transformed type
	 *
	 * @return a transformed {@link Mono}
	 */
	public final <R> Mono<R> handle(BiConsumer<? super T, SynchronousSink<R>> handler) {
		if (this instanceof Fuseable) {
			return onAssembly(new MonoHandleFuseable<>(this, handler));
		}
		return onAssembly(new MonoHandle<>(this, handler));
	}

	/**
	 * Hides the identity of this {@link Mono} instance.
	 *
	 * <p>The main purpose of this operator is to prevent certain identity-based
	 * optimizations from happening, mostly for diagnostic purposes.
	 *
	 * @return a new {@link Mono} preventing {@link Publisher} / {@link Subscription} based Reactor optimizations
	 */
	public final Mono<T> hide() {
	    return onAssembly(new MonoHide<>(this));
	}

	/**
	 * Ignores onNext signal (dropping it) and only propagates termination events.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/ignoreElementForMono.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the source element.
	 *
	 * @return a new empty {@link Mono} representing the completion of this {@link Mono}.
	 */
	public final Mono<T> ignoreElement() {
		return onAssembly(new MonoIgnoreElement<>(this));
	}

	/**
	 * Observe all Reactive Streams signals and trace them using {@link Logger} support.
	 * Default will use {@link Level#INFO} and {@code java.util.logging}.
	 * If SLF4J is available, it will be used instead.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForMono.svg" alt="">
	 * <p>
	 * The default log category will be "reactor.Mono", followed by a suffix generated from
	 * the source operator, e.g. "reactor.Mono.Map".
	 *
	 * @return a new {@link Mono} that logs signals
	 *
	 * @see Flux#log()
	 */
	public final Mono<T> log() {
		return log(null, Level.INFO);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace implementation. Default will
	 * use {@link Level#INFO} and java.util.logging. If SLF4J is available, it will be used instead.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForMono.svg" alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator
	 * suffix will complete, e.g. "reactor.Flux.Map".
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> log(@Nullable String category) {
		return log(category, Level.INFO);
	}

	/**
	 * Observe Reactive Streams signals matching the passed flags {@code options} and use
	 * {@link Logger} support to handle trace implementation. Default will use the passed
	 * {@link Level} and java.util.logging. If SLF4J is available, it will be used instead.
	 *
	 * Options allow fine grained filtering of the traced signal, for instance to only capture onNext and onError:
	 * <pre>
	 *     mono.log("category", SignalType.ON_NEXT, SignalType.ON_ERROR)
	 * </pre>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForMono.svg" alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator
	 * suffix will complete, e.g. "reactor.Flux.Map".
	 * @param level the {@link Level} to enforce for this tracing Mono (only FINEST, FINE,
	 * INFO, WARNING and SEVERE are taken into account)
	 * @param options a vararg {@link SignalType} option to filter log messages
	 *
	 * @return a new {@link Mono}
	 *
	 */
	public final Mono<T> log(@Nullable String category, Level level, SignalType... options) {
		return log(category, level, false, options);
	}

	/**
	 * Observe Reactive Streams signals matching the passed filter {@code options} and
	 * use {@link Logger} support to
	 * handle trace
	 * implementation. Default will
	 * use the passed {@link Level} and java.util.logging. If SLF4J is available, it will be used instead.
	 *
	 * Options allow fine grained filtering of the traced signal, for instance to only capture onNext and onError:
	 * <pre>
	 *     mono.log("category", Level.INFO, SignalType.ON_NEXT, SignalType.ON_ERROR)
	 * </pre>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForMono.svg" alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator
	 * suffix will complete, e.g. "reactor.Mono.Map".
	 * @param level the {@link Level} to enforce for this tracing Mono (only FINEST, FINE,
	 * INFO, WARNING and SEVERE are taken into account)
	 * @param showOperatorLine capture the current stack to display operator
	 * class/line number.
	 * @param options a vararg {@link SignalType} option to filter log messages
	 *
	 * @return a new unaltered {@link Mono}
	 */
	public final Mono<T> log(@Nullable String category,
			Level level,
			boolean showOperatorLine,
			SignalType... options) {
		SignalLogger<T> log = new SignalLogger<>(this, category, level,
				showOperatorLine, options);

		if (this instanceof Fuseable) {
			return onAssembly(new MonoLogFuseable<>(this, log));
		}
		return onAssembly(new MonoLog<>(this, log));
	}


	/**
	 * Observe Reactive Streams signals matching the passed filter {@code options} and
	 * trace them using a specific user-provided {@link Logger}, at {@link Level#INFO} level.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForMono.svg" alt="">
	 *
	 * @param logger the {@link Logger} to use, instead of resolving one through a category.
	 *
	 * @return a new {@link Mono} that logs signals
	 */
	public final Mono<T> log(Logger logger) {
		return log(logger, Level.INFO, false);
	}

	/**
	 * Observe Reactive Streams signals matching the passed filter {@code options} and
	 * trace them using a specific user-provided {@link Logger}, at the given {@link Level}.
	 * <p>
	 * Options allow fine grained filtering of the traced signal, for instance to only
	 * capture onNext and onError:
	 * <pre>
	 *     flux.log(myCustomLogger, Level.INFO, SignalType.ON_NEXT, SignalType.ON_ERROR)
	 * </pre>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForMono.svg" alt="">
	 *
	 * @param logger the {@link Logger} to use, instead of resolving one through a category.
	 * @param level the {@link Level} to enforce for this tracing Flux (only FINEST, FINE,
	 * INFO, WARNING and SEVERE are taken into account)
	 * @param showOperatorLine capture the current stack to display operator class/line number.
	 * @param options a vararg {@link SignalType} option to filter log messages
	 *
	 * @return a new {@link Mono} that logs signals
	 */
	public final Mono<T> log(Logger logger,
			Level level,
			boolean showOperatorLine,
			SignalType... options) {
		SignalLogger<T> log = new SignalLogger<>(this, "IGNORED", level,
				showOperatorLine,
				s -> logger,
				options);

		if (this instanceof Fuseable) {
			return onAssembly(new MonoLogFuseable<>(this, log));
		}
		return onAssembly(new MonoLog<>(this, log));
	}

	/**
	 * Transform the item emitted by this {@link Mono} by applying a synchronous function to it.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mapForMono.svg" alt="">
	 *
	 * @param mapper the synchronous transforming {@link Function}
	 * @param <R> the transformed type
	 *
	 * @return a new {@link Mono}
	 */
	public final <R> Mono<R> map(Function<? super T, ? extends R> mapper) {
		if (this instanceof Fuseable) {
			return onAssembly(new MonoMapFuseable<>(this, mapper));
		}
		return onAssembly(new MonoMap<>(this, mapper));
	}

	/**
	 * Transform the item emitted by this {@link Mono} by applying a synchronous function to it, which is allowed
	 * to produce a {@code null} value. In that case, the resulting Mono completes immediately.
	 * This operator effectively behaves like {@link #map(Function)} followed by {@link #filter(Predicate)}
	 * although {@code null} is not a supported value, so it can't be filtered out.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mapNotNullForMono.svg" alt="">
	 *
	 * @param mapper the synchronous transforming {@link Function}
	 * @param <R> the transformed type
	 *
	 * @return a new {@link Mono}
	 */
	public final <R> Mono<R> mapNotNull(Function <? super T, ? extends R> mapper) {
		return this.handle((t, sink) -> {
			R r = mapper.apply(t);
			if (r != null) {
				sink.next(r);
			}
		});
	}

	/**
	 * Transform incoming onNext, onError and onComplete signals into {@link Signal} instances,
	 * materializing these signals.
	 * Since the error is materialized as a {@code Signal}, the propagation will be stopped and onComplete will be
	 * emitted. Complete signal will first emit a {@code Signal.complete()} and then effectively complete the flux.
	 * All these {@link Signal} have a {@link Context} associated to them.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/materializeForMono.svg" alt="">
	 *
	 * @return a {@link Mono} of materialized {@link Signal}
	 * @see #dematerialize()
	 */
	public final Mono<Signal<T>> materialize() {
		return onAssembly(new MonoMaterialize<>(this));
	}

	/**
	 * Merge emissions of this {@link Mono} with the provided {@link Publisher}.
	 * The element from the Mono may be interleaved with the elements of the Publisher.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeWithForMono.svg" alt="">
	 *
	 * @param other the {@link Publisher} to merge with
	 *
	 * @return a new {@link Flux} as the sequence is not guaranteed to be at most 1
	 */
	public final Flux<T> mergeWith(Publisher<? extends T> other) {
		return Flux.merge(this, other);
	}

	/**
	 * Activate metrics for this sequence, provided there is an instrumentation facade
	 * on the classpath (otherwise this method is a pure no-op).
	 * <p>
	 * Metrics are gathered on {@link Subscriber} events, and it is recommended to also
	 * {@link #name(String) name} (and optionally {@link #tag(String, String) tag}) the
	 * sequence.
	 * <p>
	 * The name serves as a prefix in the reported metrics names. In case no name has been provided, the default name "reactor" will be applied.
	 * <p>
	 * The {@link MeterRegistry} used by reactor can be configured via
	 * {@link reactor.util.Metrics.MicrometerConfiguration#useRegistry(MeterRegistry)}
	 * prior to using this operator, the default being
	 * {@link io.micrometer.core.instrument.Metrics#globalRegistry}.
	 * </p>
	 *
	 * @return an instrumented {@link Mono}
	 *
	 * @see #name(String)
	 * @see #tag(String, String)
	 */
	public final Mono<T> metrics() {
		if (!Metrics.isInstrumentationAvailable()) {
			return this;
		}

		if (this instanceof Fuseable) {
			return onAssembly(new MonoMetricsFuseable<>(this));
		}
		return onAssembly(new MonoMetrics<>(this));
	}

	/**
	 * Give a name to this sequence, which can be retrieved using {@link Scannable#name()}
	 * as long as this is the first reachable {@link Scannable#parents()}.
	 * <p>
	 * If {@link #metrics()} operator is called later in the chain, this name will be used as a prefix for meters' name.
	 *
	 * @param name a name for the sequence
	 *
	 * @return the same sequence, but bearing a name
	 *
	 * @see #metrics()
	 * @see #tag(String, String)
	 */
	public final Mono<T> name(String name) {
		return MonoName.createOrAppend(this, name);
	}

	/**
	 * Emit the first available signal from this mono or the other mono.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/orForMono.svg" alt="">
	 *
	 * @param other the racing other {@link Mono} to compete with for the signal
	 *
	 * @return a new {@link Mono}
	 * @see #firstWithSignal
	 */
	public final Mono<T> or(Mono<? extends T> other) {
		if (this instanceof MonoFirstWithSignal) {
			MonoFirstWithSignal<T> a = (MonoFirstWithSignal<T>) this;
			Mono<T> result =  a.orAdditionalSource(other);
			if (result != null) {
				return result;
			}
		}
		return firstWithSignal(this, other);
	}

	/**
	 * Evaluate the emitted value against the given {@link Class} type. If the
	 * value matches the type, it is passed into the new {@link Mono}. Otherwise the
	 * value is ignored.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/ofTypeForMono.svg" alt="">
	 *
	 * @param clazz the {@link Class} type to test values against
	 *
	 * @return a new {@link Mono} filtered on the requested type
	 */
	public final <U> Mono<U> ofType(final Class<U> clazz) {
		Objects.requireNonNull(clazz, "clazz");
		return filter(o -> clazz.isAssignableFrom(o.getClass())).cast(clazz);
	}

	/**
	 * Let compatible operators <strong>upstream</strong> recover from errors by dropping the
	 * incriminating element from the sequence and continuing with subsequent elements.
	 * The recovered error and associated value are notified via the provided {@link BiConsumer}.
	 * Alternatively, throwing from that biconsumer will propagate the thrown exception downstream
	 * in place of the original error, which is added as a suppressed exception to the new one.
	 * <p>
	 * This operator is offered on {@link Mono}	mainly as a way to propagate the configuration to
	 * upstream {@link Flux}. The mode doesn't really make sense on a {@link Mono}, since we're sure
	 * there will be no further value to continue with.
	 * {@link #onErrorResume(Function)} is a more classical fit.
	 * <p>
	 * Note that onErrorContinue() is a specialist operator that can make the behaviour of your
	 * reactive chain unclear. It operates on upstream, not downstream operators, it requires specific
	 * operator support to work, and the scope can easily propagate upstream into library code
	 * that didn't anticipate it (resulting in unintended behaviour.)
	 * <p>
	 * In most cases, you should instead handle the error inside the specific function which may cause
	 * it. Specifically, on each inner publisher you can use {@code doOnError} to log the error, and
	 * {@code onErrorResume(e -> Mono.empty())} to drop erroneous elements:
	 * <p>
	 * <pre>
	 * .flatMap(id -> repository.retrieveById(id)
	 *                          .doOnError(System.err::println)
	 *                          .onErrorResume(e -> Mono.empty()))
	 * </pre>
	 * <p>
	 * This has the advantage of being much clearer, has no ambiguity with regards to operator support,
	 * and cannot leak upstream.
	 *
	 * @param errorConsumer a {@link BiConsumer} fed with errors matching the {@link Class}
	 * and the value that triggered the error.
	 * @return a {@link Mono} that attempts to continue processing on errors.
	 */
	public final Mono<T> onErrorContinue(BiConsumer<Throwable, Object> errorConsumer) {
		BiConsumer<Throwable, Object> genericConsumer = errorConsumer;
		return subscriberContext(Context.of(
				OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY,
				OnNextFailureStrategy.resume(genericConsumer)
		));
	}

	/**
	 * Let compatible operators <strong>upstream</strong> recover from errors by dropping the
	 * incriminating element from the sequence and continuing with subsequent elements.
	 * Only errors matching the specified {@code type} are recovered from.
	 * The recovered error and associated value are notified via the provided {@link BiConsumer}.
	 * Alternatively, throwing from that biconsumer will propagate the thrown exception downstream
	 * in place of the original error, which is added as a suppressed exception to the new one.
	 * <p>
	 * This operator is offered on {@link Mono}	mainly as a way to propagate the configuration to
	 * upstream {@link Flux}. The mode doesn't really make sense on a {@link Mono}, since we're sure
	 * there will be no further value to continue with.
	 * {@link #onErrorResume(Function)} is a more classical fit.
	 * <p>
	 * Note that onErrorContinue() is a specialist operator that can make the behaviour of your
	 * reactive chain unclear. It operates on upstream, not downstream operators, it requires specific
	 * operator support to work, and the scope can easily propagate upstream into library code
	 * that didn't anticipate it (resulting in unintended behaviour.)
	 * <p>
	 * In most cases, you should instead handle the error inside the specific function which may cause
	 * it. Specifically, on each inner publisher you can use {@code doOnError} to log the error, and
	 * {@code onErrorResume(e -> Mono.empty())} to drop erroneous elements:
	 * <p>
	 * <pre>
	 * .flatMap(id -> repository.retrieveById(id)
	 *                          .doOnError(MyException.class, System.err::println)
	 *                          .onErrorResume(MyException.class, e -> Mono.empty()))
	 * </pre>
	 * <p>
	 * This has the advantage of being much clearer, has no ambiguity with regards to operator support,
	 * and cannot leak upstream.
	 *
	 * @param type the {@link Class} of {@link Exception} that are resumed from.
	 * @param errorConsumer a {@link BiConsumer} fed with errors matching the {@link Class}
	 * and the value that triggered the error.
	 * @return a {@link Mono} that attempts to continue processing on some errors.
	 */
	public final <E extends Throwable> Mono<T> onErrorContinue(Class<E> type, BiConsumer<Throwable, Object> errorConsumer) {
		return onErrorContinue(type::isInstance, errorConsumer);
	}

	/**
	 * Let compatible operators <strong>upstream</strong> recover from errors by dropping the
	 * incriminating element from the sequence and continuing with subsequent elements.
	 * Only errors matching the {@link Predicate} are recovered from (note that this
	 * predicate can be applied several times and thus must be idempotent).
	 * The recovered error and associated value are notified via the provided {@link BiConsumer}.
	 * Alternatively, throwing from that biconsumer will propagate the thrown exception downstream
	 * in place of the original error, which is added as a suppressed exception to the new one.
	 * <p>
	 * This operator is offered on {@link Mono}	mainly as a way to propagate the configuration to
	 * upstream {@link Flux}. The mode doesn't really make sense on a {@link Mono}, since we're sure
	 * there will be no further value to continue with.
	 * {@link #onErrorResume(Function)} is a more classical fit.
	 * <p>
	 * Note that onErrorContinue() is a specialist operator that can make the behaviour of your
	 * reactive chain unclear. It operates on upstream, not downstream operators, it requires specific
	 * operator support to work, and the scope can easily propagate upstream into library code
	 * that didn't anticipate it (resulting in unintended behaviour.)
	 * <p>
	 * In most cases, you should instead handle the error inside the specific function which may cause
	 * it. Specifically, on each inner publisher you can use {@code doOnError} to log the error, and
	 * {@code onErrorResume(e -> Mono.empty())} to drop erroneous elements:
	 * <p>
	 * <pre>
	 * .flatMap(id -> repository.retrieveById(id)
	 *                          .doOnError(errorPredicate, System.err::println)
	 *                          .onErrorResume(errorPredicate, e -> Mono.empty()))
	 * </pre>
	 * <p>
	 * This has the advantage of being much clearer, has no ambiguity with regards to operator support,
	 * and cannot leak upstream.
	 *
	 * @param errorPredicate a {@link Predicate} used to filter which errors should be resumed from.
	 * This MUST be idempotent, as it can be used several times.
	 * @param errorConsumer a {@link BiConsumer} fed with errors matching the predicate and the value
	 * that triggered the error.
	 * @return a {@link Mono} that attempts to continue processing on some errors.
	 */
	public final <E extends Throwable> Mono<T> onErrorContinue(Predicate<E> errorPredicate,
			BiConsumer<Throwable, Object> errorConsumer) {
		//this cast is ok as only T values will be propagated in this sequence
		@SuppressWarnings("unchecked")
		Predicate<Throwable> genericPredicate = (Predicate<Throwable>) errorPredicate;
		BiConsumer<Throwable, Object> genericErrorConsumer = errorConsumer;
		return subscriberContext(Context.of(
				OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY,
				OnNextFailureStrategy.resumeIf(genericPredicate, genericErrorConsumer)
		));
	}

	/**
	 * If an {@link #onErrorContinue(BiConsumer)} variant has been used downstream, reverts
	 * to the default 'STOP' mode where errors are terminal events upstream. It can be
	 * used for easier scoping of the on next failure strategy or to override the
	 * inherited strategy in a sub-stream (for example in a flatMap). It has no effect if
	 * {@link #onErrorContinue(BiConsumer)} has not been used downstream.
	 *
	 * @return a {@link Mono} that terminates on errors, even if {@link #onErrorContinue(BiConsumer)}
	 * was used downstream
	 */
	public final Mono<T> onErrorStop() {
		return subscriberContext(Context.of(
				OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY,
				OnNextFailureStrategy.stop()));
	}

	/**
	 * Transform an error emitted by this {@link Mono} by synchronously applying a function
	 * to it if the error matches the given predicate. Otherwise let the error pass through.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorMapWithPredicateForMono.svg" alt="">
	 *
	 * @param predicate the error predicate
	 * @param mapper the error transforming {@link Function}
	 *
	 * @return a {@link Mono} that transforms some source errors to other errors
	 */
	public final Mono<T> onErrorMap(Predicate<? super Throwable> predicate,
			Function<? super Throwable, ? extends Throwable> mapper) {
		return onErrorResume(predicate, e -> Mono.error(mapper.apply(e)));
	}


	/**
	 * Transform any error emitted by this {@link Mono} by synchronously applying a function to it.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorMapForMono.svg" alt="">
	 *
	 * @param mapper the error transforming {@link Function}
	 *
	 * @return a {@link Mono} that transforms source errors to other errors
	 */
	public final Mono<T> onErrorMap(Function<? super Throwable, ? extends Throwable> mapper) {
		return onErrorResume(e -> Mono.error(mapper.apply(e)));
	}

	/**
	 * Transform an error emitted by this {@link Mono} by synchronously applying a function
	 * to it if the error matches the given type. Otherwise let the error pass through.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorMapWithClassPredicateForMono.svg" alt="">
	 *
	 * @param type the class of the exception type to react to
	 * @param mapper the error transforming {@link Function}
	 * @param <E> the error type
	 *
	 * @return a {@link Mono} that transforms some source errors to other errors
	 */
	public final <E extends Throwable> Mono<T> onErrorMap(Class<E> type,
			Function<? super E, ? extends Throwable> mapper) {
		@SuppressWarnings("unchecked")
		Function<Throwable, Throwable> handler = (Function<Throwable, Throwable>)mapper;
		return onErrorMap(type::isInstance, handler);
	}

	/**
	 * Subscribe to a fallback publisher when any error occurs, using a function to
	 * choose the fallback depending on the error.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorResumeForMono.svg" alt="">
	 *
	 * @param fallback the function to choose the fallback to an alternative {@link Mono}
	 *
	 * @return a {@link Mono} falling back upon source onError
	 *
	 * @see Flux#onErrorResume
	 */
	public final Mono<T> onErrorResume(Function<? super Throwable, ? extends Mono<? extends
			T>> fallback) {
		return onAssembly(new MonoOnErrorResume<>(this, fallback));
	}

	/**
	 * Subscribe to a fallback publisher when an error matching the given type
	 * occurs, using a function to choose the fallback depending on the error.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorResumeForMono.svg" alt="">
	 *
	 * @param type the error type to match
	 * @param fallback the function to choose the fallback to an alternative {@link Mono}
	 * @param <E> the error type
	 *
	 * @return a {@link Mono} falling back upon source onError
	 * @see Flux#onErrorResume
	 */
	public final <E extends Throwable> Mono<T> onErrorResume(Class<E> type,
			Function<? super E, ? extends Mono<? extends T>> fallback) {
		Objects.requireNonNull(type, "type");
		@SuppressWarnings("unchecked")
		Function<? super Throwable, Mono<? extends T>> handler = (Function<? super
				Throwable, Mono<? extends T>>)fallback;
		return onErrorResume(type::isInstance, handler);
	}

	/**
	 * Subscribe to a fallback publisher when an error matching a given predicate
	 * occurs.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorResumeForMono.svg" alt="">
	 *
	 * @param predicate the error predicate to match
	 * @param fallback the function to choose the fallback to an alternative {@link Mono}
	 * @return a {@link Mono} falling back upon source onError
	 * @see Flux#onErrorResume
	 */
	public final Mono<T> onErrorResume(Predicate<? super Throwable> predicate,
			Function<? super Throwable, ? extends Mono<? extends T>> fallback) {
		Objects.requireNonNull(predicate, "predicate");
		return onErrorResume(e -> predicate.test(e) ? fallback.apply(e) : error(e));
	}

	/**
	 * Simply emit a captured fallback value when any error is observed on this {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorReturnForMono.svg" alt="">
	 *
	 * @param fallback the value to emit if an error occurs
	 *
	 * @return a new falling back {@link Mono}
	 */
	public final Mono<T> onErrorReturn(final T fallback) {
		return onErrorResume(throwable -> just(fallback));
	}

	/**
	 * Simply emit a captured fallback value when an error of the specified type is
	 * observed on this {@link Mono}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorReturnForMono.svg" alt="">
	 *
	 * @param type the error type to match
	 * @param fallbackValue the value to emit if an error occurs that matches the type
	 * @param <E> the error type
	 *
	 * @return a new falling back {@link Mono}
	 */
	public final <E extends Throwable> Mono<T> onErrorReturn(Class<E> type, T fallbackValue) {
		return onErrorResume(type, throwable -> just(fallbackValue));
	}

	/**
	 * Simply emit a captured fallback value when an error matching the given predicate is
	 * observed on this {@link Mono}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorReturnForMono.svg" alt="">
	 *
	 * @param predicate the error predicate to match
	 * @param fallbackValue the value to emit if an error occurs that matches the predicate
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> onErrorReturn(Predicate<? super Throwable> predicate, T fallbackValue) {
		return onErrorResume(predicate,  throwable -> just(fallbackValue));
	}

	/**
	 * Detaches both the child {@link Subscriber} and the {@link Subscription} on
	 * termination or cancellation.
	 * <p>This should help with odd retention scenarios when running
	 * with non-reactor {@link Subscriber}.
	 *
	 * @return a detachable {@link Mono}
	 */
	public final Mono<T> onTerminateDetach() {
		return new MonoDetach<>(this);
	}

	/**
	 * Share a {@link Mono} for the duration of a function that may transform it and
	 * consume it as many times as necessary without causing multiple subscriptions
	 * to the upstream.
	 *
	 * @param transform the transformation function
	 * @param <R> the output value type
	 *
	 * @return a new {@link Mono}
	 */
	public final <R> Mono<R> publish(Function<? super Mono<T>, ? extends Mono<? extends
			R>> transform) {
		return onAssembly(new MonoPublishMulticast<>(this, transform));
	}

	/**
	 * Run onNext, onComplete and onError on a supplied {@link Scheduler}
	 * {@link Worker Worker}.
	 * <p>
	 * This operator influences the threading context where the rest of the operators in
	 * the chain below it will execute, up to a new occurrence of {@code publishOn}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/publishOnForMono.svg" alt="">
	 * <p>
	 * Typically used for fast publisher, slow consumer(s) scenarios.
	 *
	 * <blockquote><pre>
	 * {@code mono.publishOn(Schedulers.single()).subscribe() }
	 * </pre></blockquote>
	 *
	 * @param scheduler a {@link Scheduler} providing the {@link Worker} where to publish
	 *
	 * @return an asynchronously producing {@link Mono}
	 */
	public final Mono<T> publishOn(Scheduler scheduler) {
		if(this instanceof Callable) {
			if (this instanceof Fuseable.ScalarCallable) {
				try {
					T value = block();
					return onAssembly(new MonoSubscribeOnValue<>(value, scheduler));
				}
				catch (Throwable t) {
					//leave MonoSubscribeOnCallable defer error
				}
			}
			@SuppressWarnings("unchecked")
			Callable<T> c = (Callable<T>)this;
			return onAssembly(new MonoSubscribeOnCallable<>(c, scheduler));
		}
		return onAssembly(new MonoPublishOn<>(this, scheduler));
	}

	/**
	 * Repeatedly and indefinitely subscribe to the source upon completion of the
	 * previous subscription.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatForMono.svg" alt="">
	 *
	 * @return an indefinitely repeated {@link Flux} on onComplete
	 */
	public final Flux<T> repeat() {
		return repeat(Flux.ALWAYS_BOOLEAN_SUPPLIER);
	}

	/**
	 * Repeatedly subscribe to the source if the predicate returns true after completion of the previous subscription.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatWithPredicateForMono.svg" alt="">
	 *
	 * @param predicate the boolean to evaluate on onComplete.
	 *
	 * @return a {@link Flux} that repeats on onComplete while the predicate matches
	 *
	 */
	public final Flux<T> repeat(BooleanSupplier predicate) {
		return Flux.onAssembly(new MonoRepeatPredicate<>(this, predicate));
	}

	/**
	 * Repeatedly subscribe to the source {@literal numRepeat} times. This results in
	 * {@code numRepeat + 1} total subscriptions to the original source. As a consequence,
	 * using 0 plays the original sequence once.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatWithAttemptsForMono.svg" alt="">
	 *
	 * @param numRepeat the number of times to re-subscribe on onComplete (positive, or 0 for original sequence only)
	 * @return a {@link Flux} that repeats on onComplete, up to the specified number of repetitions
	 */
	public final Flux<T> repeat(long numRepeat) {
		if (numRepeat == 0) {
			return this.flux();
		}
		return Flux.onAssembly(new MonoRepeat<>(this, numRepeat));
	}

	/**
	 * Repeatedly subscribe to the source if the predicate returns true after completion of the previous
	 * subscription. A specified maximum of repeat will limit the number of re-subscribe.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatWithAttemptsAndPredicateForMono.svg" alt="">
	 *
	 * @param numRepeat the number of times to re-subscribe on complete (positive, or 0 for original sequence only)
	 * @param predicate the boolean to evaluate on onComplete
	 *
	 * @return a {@link Flux} that repeats on onComplete while the predicate matches,
	 * up to the specified number of repetitions
	 */
	public final Flux<T> repeat(long numRepeat, BooleanSupplier predicate) {
		if (numRepeat < 0L) {
			throw new IllegalArgumentException("numRepeat >= 0 required");
		}
		if (numRepeat == 0) {
			return this.flux();
		}
		return Flux.defer(() -> repeat(Flux.countingBooleanSupplier(predicate, numRepeat)));
	}

	/**
	 * Repeatedly subscribe to this {@link Mono} when a companion sequence emits elements in
	 * response to the flux completion signal. Any terminal signal from the companion
	 * sequence will terminate the resulting {@link Flux} with the same signal immediately.
	 * <p>If the companion sequence signals when this {@link Mono} is active, the repeat
	 * attempt is suppressed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatWhenForMono.svg" alt="">
	 * <p>
	 * Note that if the companion {@link Publisher} created by the {@code repeatFactory}
	 * emits {@link Context} as trigger objects, the content of these Context will be added
	 * to the operator's own {@link Context}.
	 *
	 * @param repeatFactory the {@link Function} that returns the associated {@link Publisher}
	 * companion, given a {@link Flux} that signals each onComplete as a {@link Long}
	 * representing the number of source elements emitted in the latest attempt (0 or 1).
	 *
	 * @return a {@link Flux} that repeats on onComplete when the companion {@link Publisher} produces an
	 * onNext signal
	 */
	public final Flux<T> repeatWhen(Function<Flux<Long>, ? extends Publisher<?>> repeatFactory) {
		return Flux.onAssembly(new MonoRepeatWhen<>(this, repeatFactory));
	}

	/**
	 * Repeatedly subscribe to this {@link Mono} as long as the current subscription to this
	 * {@link Mono} completes empty and the companion {@link Publisher} produces an onNext signal.
	 * <p>
	 * Any terminal signal will terminate the resulting {@link Mono} with the same signal immediately.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatWhenEmpty.svg" alt="">
	 *
	 * @param repeatFactory the {@link Function} that returns the associated {@link Publisher}
	 * companion, given a {@link Flux} that signals each onComplete as a 0-based incrementing {@link Long}.
	 *
	 * @return a {@link Mono} that resubscribes to this {@link Mono} if the previous subscription was empty,
	 * as long as the companion {@link Publisher} produces an onNext signal
	 *
	 */
	public final Mono<T> repeatWhenEmpty(Function<Flux<Long>, ? extends Publisher<?>> repeatFactory) {
		return repeatWhenEmpty(Integer.MAX_VALUE, repeatFactory);
	}

	/**
	 * Repeatedly subscribe to this {@link Mono} as long as the current subscription to this
	 * {@link Mono} completes empty and the companion {@link Publisher} produces an onNext signal.
	 * <p>
	 * Any terminal signal will terminate the resulting {@link Mono} with the same signal immediately.
	 * <p>
	 * Emits an {@link IllegalStateException} if {@code maxRepeat} is exceeded (provided
	 * it is different from {@code Integer.MAX_VALUE}).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatWhenEmpty.svg" alt="">
	 *
	 * @param maxRepeat the maximum number of repeats (infinite if {@code Integer.MAX_VALUE})
	 * @param repeatFactory the {@link Function} that returns the associated {@link Publisher}
	 * companion, given a {@link Flux} that signals each onComplete as a 0-based incrementing {@link Long}.
	 *
	 * @return a {@link Mono} that resubscribes to this {@link Mono} if the previous subscription was empty,
	 * as long as the companion {@link Publisher} produces an onNext signal and the maximum number of repeats isn't exceeded.
	 */
	public final Mono<T> repeatWhenEmpty(int maxRepeat, Function<Flux<Long>, ? extends Publisher<?>> repeatFactory) {
		return Mono.defer(() -> this.repeatWhen(o -> {
			if (maxRepeat == Integer.MAX_VALUE) {
				return repeatFactory.apply(o.index().map(Tuple2::getT1));
			}
			else {
				return repeatFactory.apply(o.index().map(Tuple2::getT1)
						.take(maxRepeat)
						.concatWith(Flux.error(() -> new IllegalStateException("Exceeded maximum number of repeats"))));
			}
		}).next());
	}


	/**
	 * Re-subscribes to this {@link Mono} sequence if it signals any error, indefinitely.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/retryForMono.svg" alt="">
	 *
	 * @return a {@link Mono} that retries on onError
	 */
	public final Mono<T> retry() {
		return retry(Long.MAX_VALUE);
	}

	/**
	 * Re-subscribes to this {@link Mono} sequence if it signals any error, for a fixed
	 * number of times.
	 * <p>
	 * Note that passing {@literal Long.MAX_VALUE} is treated as infinite retry.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/retryWithAttemptsForMono.svg" alt="">
	 *
	 * @param numRetries the number of times to tolerate an error
	 *
	 * @return a {@link Mono} that retries on onError up to the specified number of retry attempts.
	 */
	public final Mono<T> retry(long numRetries) {
		return onAssembly(new MonoRetry<>(this, numRetries));
	}

	/**
	 * Retries this {@link Mono} in response to signals emitted by a companion {@link Publisher}.
	 * The companion is generated by the provided {@link Retry} instance, see {@link Retry#max(long)}, {@link Retry#maxInARow(long)}
	 * and {@link Retry#backoff(long, Duration)} for readily available strategy builders.
	 * <p>
	 * The operator generates a base for the companion, a {@link Flux} of {@link reactor.util.retry.Retry.RetrySignal}
	 * which each give metadata about each retryable failure whenever this {@link Mono} signals an error. The final companion
	 * should be derived from that base companion and emit data in response to incoming onNext (although it can emit less
	 * elements, or delay the emissions).
	 * <p>
	 * Terminal signals in the companion terminate the sequence with the same signal, so emitting an {@link Subscriber#onError(Throwable)}
	 * will fail the resulting {@link Mono} with that same error.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/retryWhenSpecForMono.svg" alt="">
	 * <p>
	 * Note that the {@link reactor.util.retry.Retry.RetrySignal} state can be transient and change between each source
	 * {@link org.reactivestreams.Subscriber#onError(Throwable) onError} or
	 * {@link org.reactivestreams.Subscriber#onNext(Object) onNext}. If processed with a delay,
	 * this could lead to the represented state being out of sync with the state at which the retry
	 * was evaluated. Map it to {@link reactor.util.retry.Retry.RetrySignal#copy()} right away to mediate this.
	 * <p>
	 * Note that if the companion {@link Publisher} created by the {@code whenFactory}
	 * emits {@link Context} as trigger objects, these {@link Context} will be merged with
	 * the previous Context:
	 * <blockquote>
	 * <pre>
	 * {@code
	 * Retry customStrategy = Retry.from(companion -> companion.handle((retrySignal, sink) -> {
	 * 	    Context ctx = sink.currentContext();
	 * 	    int rl = ctx.getOrDefault("retriesLeft", 0);
	 * 	    if (rl > 0) {
	 *		    sink.next(Context.of(
	 *		        "retriesLeft", rl - 1,
	 *		        "lastError", retrySignal.failure()
	 *		    ));
	 * 	    } else {
	 * 	        sink.error(Exceptions.retryExhausted("retries exhausted", retrySignal.failure()));
	 * 	    }
	 * }));
	 * Mono<T> retried = originalMono.retryWhen(customStrategy);
	 * }</pre>
	 * </blockquote>
	 *
	 * @param retrySpec the {@link Retry} strategy that will generate the companion {@link Publisher},
	 * given a {@link Flux} that signals each onError as a {@link reactor.util.retry.Retry.RetrySignal}.
	 *
	 * @return a {@link Mono} that retries on onError when a companion {@link Publisher} produces an onNext signal
	 * @see Retry#max(long)
	 * @see Retry#maxInARow(long)
	 * @see Retry#backoff(long, Duration)
	 */
	public final Mono<T> retryWhen(Retry retrySpec) {
		return onAssembly(new MonoRetryWhen<>(this, retrySpec));
	}

	/**
	 * Prepare a {@link Mono} which shares this {@link Mono} result similar to {@link Flux#shareNext()}.
	 * This will effectively turn this {@link Mono} into a hot task when the first
	 * {@link Subscriber} subscribes using {@link #subscribe()} API. Further {@link Subscriber} will share the same {@link Subscription}
	 * and therefore the same result.
	 * It's worth noting this is an un-cancellable {@link Subscription}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/shareForMono.svg" alt="">
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> share() {
		if (this instanceof Fuseable.ScalarCallable) {
			return this;
		}

		if (this instanceof NextProcessor && ((NextProcessor<?>) this).isRefCounted) { //TODO should we check whether the NextProcessor has a source or not?
			return this;
		}
		return new NextProcessor<>(this, true);
	}

	/**
	 * Expect exactly one item from this {@link Mono} source or signal
	 * {@link java.util.NoSuchElementException} for an empty source.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/singleForMono.svg" alt="">
	 * <p>
	 * Note Mono doesn't need {@link Flux#single(Object)}, since it is equivalent to
	 * {@link #defaultIfEmpty(Object)} in a {@link Mono}.
	 *
	 * @return a {@link Mono} with the single item or an error signal
	 */
	public final Mono<T> single() {
		if (this instanceof Callable) {
			if (this instanceof Fuseable.ScalarCallable) {
				@SuppressWarnings("unchecked")
				Fuseable.ScalarCallable<T> scalarCallable = (Fuseable.ScalarCallable<T>) this;

				T v;
				try {
					v = scalarCallable.call();
				}
				catch (Exception e) {
					return Mono.error(Exceptions.unwrap(e));
				}
				if (v == null) {
					return Mono.error(new NoSuchElementException("Source was a (constant) empty"));
				}
				return Mono.just(v);
			}
			@SuppressWarnings("unchecked")
			Callable<T> thiz = (Callable<T>)this;
			return Mono.onAssembly(new MonoSingleCallable<>(thiz));
		}
		return Mono.onAssembly(new MonoSingleMono<>(this));
	}

	/**
	 * Subscribe to this {@link Mono} and request unbounded demand.
	 * <p>
	 * This version doesn't specify any consumption behavior for the events from the
	 * chain, especially no error handling, so other variants should usually be preferred.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeIgoringAllSignalsForMono.svg" alt="">
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 */
	public final Disposable subscribe() {
		if(this instanceof NextProcessor){
			NextProcessor<T> s = (NextProcessor<T>)this;
			// Mono#share() should return a new lambda subscriber during subscribe.
			// This can now be precisely detected with source != null in combination to the isRefCounted boolean being true.
			// `Sinks.one().subscribe()` case is now split into a separate implementation.
			// Otherwise, this is a (legacy) #toProcessor() usage, and we return the processor itself below (and don't forget to connect() it):
			if (s.source != null && !s.isRefCounted) {
				s.subscribe(new LambdaMonoSubscriber<>(null, null, null, null, null));
				s.connect();
				return s;
			}
		}
		return subscribeWith(new LambdaMonoSubscriber<>(null, null, null, null, null));
	}

	/**
	 * Subscribe a {@link Consumer} to this {@link Mono} that will consume all the
	 * sequence. It will request an unbounded demand ({@code Long.MAX_VALUE}).
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnNext(java.util.function.Consumer)}.
	 * <p>
	 * Keep in mind that since the sequence can be asynchronous, this will immediately
	 * return control to the calling thread. This can give the impression the consumer is
	 * not invoked when executing in a main thread or a unit test for instance.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeWithOnNextForMono.svg" alt="">
	 *
	 * @param consumer the consumer to invoke on each value (onNext signal)
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 */
	public final Disposable subscribe(Consumer<? super T> consumer) {
		Objects.requireNonNull(consumer, "consumer");
		return subscribe(consumer, null, null);
	}

	/**
	 * Subscribe to this {@link Mono} with a {@link Consumer} that will consume all the
	 * elements in the sequence, as well as a {@link Consumer} that will handle errors.
	 * The subscription will request an unbounded demand ({@code Long.MAX_VALUE}).
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnSuccess(Consumer)} and
	 * {@link #doOnError(java.util.function.Consumer)}.
	 * <p>
	 * Keep in mind that since the sequence can be asynchronous, this will immediately
	 * return control to the calling thread. This can give the impression the consumer is
	 * not invoked when executing in a main thread or a unit test for instance.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeWithOnNextAndOnErrorForMono.svg" alt="">
	 *
	 * @param consumer the consumer to invoke on each next signal
	 * @param errorConsumer the consumer to invoke on error signal
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 */
	public final Disposable subscribe(@Nullable Consumer<? super T> consumer, Consumer<? super Throwable> errorConsumer) {
		Objects.requireNonNull(errorConsumer, "errorConsumer");
		return subscribe(consumer, errorConsumer, null);
	}

	/**
	 * Subscribe {@link Consumer} to this {@link Mono} that will respectively consume all the
	 * elements in the sequence, handle errors and react to completion. The subscription
	 * will request unbounded demand ({@code Long.MAX_VALUE}).
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnSuccess(Consumer)} and
	 * {@link #doOnError(java.util.function.Consumer)}.
	 * <p>
	 * Keep in mind that since the sequence can be asynchronous, this will immediately
	 * return control to the calling thread. This can give the impression the consumer is
	 * not invoked when executing in a main thread or a unit test for instance.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeWithOnNextAndOnErrorAndOnCompleteForMono.svg" alt="">
	 *
	 * @param consumer the consumer to invoke on each value
	 * @param errorConsumer the consumer to invoke on error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 */
	public final Disposable subscribe(
			@Nullable Consumer<? super T> consumer,
			@Nullable Consumer<? super Throwable> errorConsumer,
			@Nullable Runnable completeConsumer) {
		return subscribe(consumer, errorConsumer, completeConsumer, (Context) null);
	}

	/**
	 * Subscribe {@link Consumer} to this {@link Mono} that will respectively consume all the
	 * elements in the sequence, handle errors, react to completion, and request upon subscription.
	 * It will let the provided {@link Subscription subscriptionConsumer}
	 * request the adequate amount of data, or request unbounded demand
	 * {@code Long.MAX_VALUE} if no such consumer is provided.
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnSuccess(Consumer)} and
	 * {@link #doOnError(java.util.function.Consumer)}.
	 * <p>
	 * Keep in mind that since the sequence can be asynchronous, this will immediately
	 * return control to the calling thread. This can give the impression the consumer is
	 * not invoked when executing in a main thread or a unit test for instance.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeForMono.svg" alt="">
	 *
	 * @param consumer the consumer to invoke on each value
	 * @param errorConsumer the consumer to invoke on error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 * @param subscriptionConsumer the consumer to invoke on subscribe signal, to be used
	 * for the initial {@link Subscription#request(long) request}, or null for max request
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 */ //TODO maybe deprecate in 3.4, provided there is at least an alternative for tests
	public final Disposable subscribe(
			@Nullable Consumer<? super T> consumer,
			@Nullable Consumer<? super Throwable> errorConsumer,
			@Nullable Runnable completeConsumer,
			@Nullable Consumer<? super Subscription> subscriptionConsumer) {
		return subscribeWith(new LambdaMonoSubscriber<>(consumer, errorConsumer,
				completeConsumer, subscriptionConsumer, null));
	}

	/**
	 * Subscribe {@link Consumer} to this {@link Mono} that will respectively consume all the
	 * elements in the sequence, handle errors and react to completion. Additionally, a {@link Context}
	 * is tied to the subscription. At subscription, an unbounded request is implicitly made.
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnSuccess(Consumer)} and
	 * {@link #doOnError(java.util.function.Consumer)}.
	 * <p>
	 * Keep in mind that since the sequence can be asynchronous, this will immediately
	 * return control to the calling thread. This can give the impression the consumer is
	 * not invoked when executing in a main thread or a unit test for instance.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeForMono.svg" alt="">
	 *
	 * @param consumer the consumer to invoke on each value
	 * @param errorConsumer the consumer to invoke on error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 * @param initialContext the {@link Context} for the subscription
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 */
	public final Disposable subscribe(
			@Nullable Consumer<? super T> consumer,
			@Nullable Consumer<? super Throwable> errorConsumer,
			@Nullable Runnable completeConsumer,
			@Nullable Context initialContext) {
		return subscribeWith(new LambdaMonoSubscriber<>(consumer, errorConsumer,
				completeConsumer, null, initialContext));
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void subscribe(Subscriber<? super T> actual) {
		CorePublisher publisher = Operators.onLastAssembly(this);
		CoreSubscriber subscriber = Operators.toCoreSubscriber(actual);

		try {
			if (publisher instanceof OptimizableOperator) {
				OptimizableOperator operator = (OptimizableOperator) publisher;
				while (true) {
					subscriber = operator.subscribeOrReturn(subscriber);
					if (subscriber == null) {
						// null means "I will subscribe myself", returning...
						return;
					}

					OptimizableOperator newSource = operator.nextOptimizableSource();
					if (newSource == null) {
						publisher = operator.source();
						break;
					}
					operator = newSource;
				}
			}

			publisher.subscribe(subscriber);
		}
		catch (Throwable e) {
			Operators.reportThrowInSubscribe(subscriber, e);
			return;
		}
	}

	/**
	 * An internal {@link Publisher#subscribe(Subscriber)} that will bypass
	 * {@link Hooks#onLastOperator(Function)} pointcut.
	 * <p>
	 * In addition to behave as expected by {@link Publisher#subscribe(Subscriber)}
	 * in a controlled manner, it supports direct subscribe-time {@link Context} passing.
	 *
	 * @param actual the {@link Subscriber} interested into the published sequence
	 * @see Publisher#subscribe(Subscriber)
	 */
	public abstract void subscribe(CoreSubscriber<? super T> actual);

	/**
	 * Enrich a potentially empty downstream {@link Context} by adding all values
	 * from the given {@link Context}, producing a new {@link Context} that is propagated
	 * upstream.
	 * <p>
	 * The {@link Context} propagation happens once per subscription (not on each onNext):
	 * it is done during the {@code subscribe(Subscriber)} phase, which runs from
	 * the last operator of a chain towards the first.
	 * <p>
	 * So this operator enriches a {@link Context} coming from under it in the chain
	 * (downstream, by default an empty one) and makes the new enriched {@link Context}
	 * visible to operators above it in the chain.
	 *
	 * @param mergeContext the {@link Context} to merge with a previous {@link Context}
	 * state, returning a new one.
	 *
	 * @return a contextualized {@link Mono}
	 * @see Context
	 * @deprecated Use {@link #contextWrite(ContextView)} instead. To be removed in 3.5.0.
	 */
	@Deprecated
	public final Mono<T> subscriberContext(Context mergeContext) {
		return subscriberContext(c -> c.putAll(mergeContext.readOnly()));
	}

	/**
	 * Enrich a potentially empty downstream {@link Context} by applying a {@link Function}
	 * to it, producing a new {@link Context} that is propagated upstream.
	 * <p>
	 * The {@link Context} propagation happens once per subscription (not on each onNext):
	 * it is done during the {@code subscribe(Subscriber)} phase, which runs from
	 * the last operator of a chain towards the first.
	 * <p>
	 * So this operator enriches a {@link Context} coming from under it in the chain
	 * (downstream, by default an empty one) and makes the new enriched {@link Context}
	 * visible to operators above it in the chain.
	 *
	 * @param doOnContext the function taking a previous {@link Context} state
	 *  and returning a new one.
	 *
	 * @return a contextualized {@link Mono}
	 * @see Context
	 * @deprecated Use {@link #contextWrite(Function)} instead. To be removed in 3.5.0.
	 */
	@Deprecated
	public final Mono<T> subscriberContext(Function<Context, Context> doOnContext) {
		return new MonoContextWrite<>(this, doOnContext);
	}

	/**
	 * Run subscribe, onSubscribe and request on a specified {@link Scheduler}'s {@link Worker}.
	 * As such, placing this operator anywhere in the chain will also impact the execution
	 * context of onNext/onError/onComplete signals from the beginning of the chain up to
	 * the next occurrence of a {@link #publishOn(Scheduler) publishOn}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeOnForMono.svg" alt="">
	 *
	 * <blockquote><pre>
	 * {@code mono.subscribeOn(Schedulers.parallel()).subscribe()) }
	 * </pre></blockquote>
	 *
	 * @param scheduler a {@link Scheduler} providing the {@link Worker} where to subscribe
	 *
	 * @return a {@link Mono} requesting asynchronously
	 * @see #publishOn(Scheduler)
	 */
	public final Mono<T> subscribeOn(Scheduler scheduler) {
		if(this instanceof Callable) {
			if (this instanceof Fuseable.ScalarCallable) {
				try {
					T value = block();
					return onAssembly(new MonoSubscribeOnValue<>(value, scheduler));
				}
				catch (Throwable t) {
					//leave MonoSubscribeOnCallable defer error
				}
			}
			@SuppressWarnings("unchecked")
			Callable<T> c = (Callable<T>)this;
			return onAssembly(new MonoSubscribeOnCallable<>(c,
					scheduler));
		}
		return onAssembly(new MonoSubscribeOn<>(this, scheduler));
	}

	/**
	 * Subscribe the given {@link Subscriber} to this {@link Mono} and return said
	 * {@link Subscriber} (eg. a {@link MonoProcessor}).
	 *
	 * @param subscriber the {@link Subscriber} to subscribe with
	 * @param <E> the reified type of the {@link Subscriber} for chaining
	 *
	 * @return the passed {@link Subscriber} after subscribing it to this {@link Mono}
	 */
	public final <E extends Subscriber<? super T>> E subscribeWith(E subscriber) {
		subscribe(subscriber);
		return subscriber;
	}

	/**
	 * Fallback to an alternative {@link Mono} if this mono is completed without data
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/switchIfEmptyForMono.svg" alt="">
	 *
	 * @param alternate the alternate mono if this mono is empty
	 *
	 * @return a {@link Mono} falling back upon source completing without elements
	 * @see Flux#switchIfEmpty
	 */
	public final Mono<T> switchIfEmpty(Mono<? extends T> alternate) {
		return onAssembly(new MonoSwitchIfEmpty<>(this, alternate));
	}

	/**
	 * Tag this mono with a key/value pair. These can be retrieved as a {@link Set} of
	 * all tags throughout the publisher chain by using {@link Scannable#tags()} (as
	 * traversed
	 * by {@link Scannable#parents()}).
	 * <p>
	 * Note that some monitoring systems like Prometheus require to have the exact same set of
	 * tags for each meter bearing the same name.
	 *
	 * @param key a tag key
	 * @param value a tag value
	 *
	 * @return the same sequence, but bearing tags
	 *
	 * @see #name(String)
	 * @see #metrics()
	 */
	public final Mono<T> tag(String key, String value) {
		return MonoName.createOrAppend(this, key, value);
	}

	/**
	 * Give this Mono a chance to resolve within a specified time frame but complete if it
	 * doesn't. This works a bit like {@link #timeout(Duration)} except that the resulting
	 * {@link Mono} completes rather than errors when the timer expires.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeWithTimespanForMono.svg" alt="">
	 * <p>
	 * The timeframe is evaluated using the {@link Schedulers#parallel() parallel Scheduler}.
	 *
	 * @param duration the maximum duration to wait for the source Mono to resolve.
	 * @return a new {@link Mono} that will propagate the signals from the source unless
	 * no signal is received for {@code duration}, in which case it completes.
	 */
	public final Mono<T> take(Duration duration) {
		return take(duration, Schedulers.parallel());
	}

	/**
	 * Give this Mono a chance to resolve within a specified time frame but complete if it
	 * doesn't. This works a bit like {@link #timeout(Duration)} except that the resulting
	 * {@link Mono} completes rather than errors when the timer expires.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeWithTimespanForMono.svg" alt="">
	 * <p>
	 * The timeframe is evaluated using the provided {@link Scheduler}.
	 *
	 * @param duration the maximum duration to wait for the source Mono to resolve.
	 * @param timer the {@link Scheduler} on which to measure the duration.
	 *
	 * @return a new {@link Mono} that will propagate the signals from the source unless
	 * no signal is received for {@code duration}, in which case it completes.
	 */
	public final Mono<T> take(Duration duration, Scheduler timer) {
		return takeUntilOther(Mono.delay(duration, timer));
	}

	/**
	 * Give this Mono a chance to resolve before a companion {@link Publisher} emits. If
	 * the companion emits before any signal from the source, the resulting Mono will
	 * complete. Otherwise, it will relay signals from the source.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeUntilOtherForMono.svg" alt="">
	 *
	 * @param other a companion {@link Publisher} that shortcircuits the source with an
	 * onComplete signal if it emits before the source emits.
	 *
	 * @return a new {@link Mono} that will propagate the signals from the source unless
	 * a signal is first received from the companion {@link Publisher}, in which case it
	 * completes.
	 */
	public final Mono<T> takeUntilOther(Publisher<?> other) {
		return onAssembly(new MonoTakeUntilOther<>(this, other));
	}

	/**
	 * Return a {@code Mono<Void>} which only replays complete and error signals
	 * from this {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/thenForMono.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the element from the source.
	 *
	 * @return a {@link Mono} ignoring its payload (actively dropping)
	 */
	public final Mono<Void> then() {
		return empty(this);
	}

	/**
	 * Let this {@link Mono} complete then play another Mono.
	 * <p>
	 * In other words ignore element from this {@link Mono} and transform its completion signal into the
	 * emission and completion signal of a provided {@code Mono<V>}. Error signal is
	 * replayed in the resulting {@code Mono<V>}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/thenWithMonoForMono.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the element from the source.
	 *
	 * @param other a {@link Mono} to emit from after termination
	 * @param <V> the element type of the supplied Mono
	 *
	 * @return a new {@link Mono} that emits from the supplied {@link Mono}
	 */
	public final <V> Mono<V> then(Mono<V> other) {
		if (this instanceof MonoIgnoreThen) {
            MonoIgnoreThen<T> a = (MonoIgnoreThen<T>) this;
            return a.shift(other);
		}
		return onAssembly(new MonoIgnoreThen<>(new Publisher[] { this }, other));
	}

	/**
	 * Let this {@link Mono} complete successfully, then emit the provided value. On an error in the original {@link Mono}, the error signal is propagated instead.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/thenReturn.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the element from the source.
	 *
	 * @param value a value to emit after successful termination
	 * @param <V> the element type of the supplied value
	 *
	 * @return a new {@link Mono} that emits the supplied value
	 */
	public final <V> Mono<V> thenReturn(V value) {
	    return then(Mono.just(value));
	}

	/**
	 * Return a {@code Mono<Void>} that waits for this {@link Mono} to complete then
	 * for a supplied {@link Publisher Publisher&lt;Void&gt;} to also complete. The
	 * second completion signal is replayed, or any error signal that occurs instead.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/thenEmptyForMono.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the element from the source.
	 *
	 * @param other a {@link Publisher} to wait for after this Mono's termination
	 * @return a new {@link Mono} completing when both publishers have completed in
	 * sequence
	 */
	public final Mono<Void> thenEmpty(Publisher<Void> other) {
		return then(fromDirect(other));
	}

	/**
	 * Let this {@link Mono} complete successfully then play another {@link Publisher}. On an error in the original {@link Mono}, the error signal is propagated instead.
	 * <p>
	 * In other words ignore the element from this mono and transform the completion signal into a
	 * {@code Flux<V>} that will emit elements from the provided {@link Publisher}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/thenManyForMono.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the element from the source.
	 *
	 * @param other a {@link Publisher} to emit from after termination
	 * @param <V> the element type of the supplied Publisher
	 *
	 * @return a new {@link Flux} that emits from the supplied {@link Publisher} after
	 * this Mono completes.
	 */
	public final <V> Flux<V> thenMany(Publisher<V> other) {
		@SuppressWarnings("unchecked")
		Flux<V> concat = (Flux<V>)Flux.concat(ignoreElement(), other);
		return Flux.onAssembly(concat);
	}


	/**
	 * Times this {@link Mono} {@link Subscriber#onNext(Object)} event, encapsulated into a {@link Timed} object
	 * that lets downstream consumer look at various time information gathered with nanosecond
	 * resolution using the default clock ({@link Schedulers#parallel()}):
	 * <ul>
	 *     <li>{@link Timed#elapsed()}: the time in nanoseconds since subscription, as a {@link Duration}.
	 *     This is functionally equivalent to {@link #elapsed()}, with a more expressive and precise
	 *     representation than a {@link Tuple2} with a long.</li>
	 *     <li>{@link Timed#timestamp()}: the timestamp of this onNext, as an {@link java.time.Instant}
	 *     (with nanoseconds part). This is functionally equivalent to {@link #timestamp()}, with a more
	 *     expressive and precise representation than a {@link Tuple2} with a long.</li>
	 *     <li>{@link Timed#elapsedSinceSubscription()}: for {@link Mono} this is the same as
	 *     {@link Timed#elapsed()}.</li>
	 * </ul>
	 * <p>
	 * The {@link Timed} object instances are safe to store and use later, as they are created as an
	 * immutable wrapper around the {@code <T>} value and immediately passed downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timedForMono.svg" alt="">
	 *
	 * @return a timed {@link Mono}
	 * @see #elapsed()
	 * @see #timestamp()
	 */
	public final Mono<Timed<T>> timed() {
		return this.timed(Schedulers.parallel());
	}

	 /**
	 * Times this {@link Mono} {@link Subscriber#onNext(Object)} event, encapsulated into a {@link Timed} object
	 * that lets downstream consumer look at various time information gathered with nanosecond
	 * resolution using the provided {@link Scheduler} as a clock:
	 * <ul>
	 *     <li>{@link Timed#elapsed()}: the time in nanoseconds since subscription, as a {@link Duration}.
	 *     This is functionally equivalent to {@link #elapsed()}, with a more expressive and precise
	  *    representation than a {@link Tuple2} with a long.</li>
	 *     <li>{@link Timed#timestamp()}: the timestamp of this onNext, as an {@link java.time.Instant}
	 *     (with nanoseconds part). This is functionally equivalent to {@link #timestamp()}, with a more
	 *     expressive and precise representation than a {@link Tuple2} with a long.</li>
	 *     <li>{@link Timed#elapsedSinceSubscription()}: for {@link Mono} this is the same as
	 *     {@link Timed#elapsed()}.</li>
	 * </ul>
	 * <p>
	 * The {@link Timed} object instances are safe to store and use later, as they are created as an
	 * immutable wrapper around the {@code <T>} value and immediately passed downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timedForMono.svg" alt="">
	 *
	 * @return a timed {@link Mono}
	 * @see #elapsed(Scheduler)
	 * @see #timestamp(Scheduler)
	 */
	public final Mono<Timed<T>> timed(Scheduler clock) {
		return onAssembly(new MonoTimed<>(this, clock));
	}

	/**
	 * Propagate a {@link TimeoutException} in case no item arrives within the given
	 * {@link Duration}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutForMono.svg" alt="">
	 *
	 * @param timeout the timeout before the onNext signal from this {@link Mono}
	 *
	 * @return a {@link Mono} that can time out
	 */
	public final Mono<T> timeout(Duration timeout) {
		return timeout(timeout, Schedulers.parallel());
	}

	/**
	 * Switch to a fallback {@link Mono} in case no item arrives within the given {@link Duration}.
	 *
	 * <p>
	 * If the fallback {@link Mono} is null, signal a {@link TimeoutException} instead.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutFallbackForMono.svg" alt="">
	 *
	 * @param timeout the timeout before the onNext signal from this {@link Mono}
	 * @param fallback the fallback {@link Mono} to subscribe to when a timeout occurs
	 *
	 * @return a {@link Mono} that will fallback to a different {@link Mono} in case of timeout
	 */
	public final Mono<T> timeout(Duration timeout, Mono<? extends T> fallback) {
		return timeout(timeout, fallback, Schedulers.parallel());
	}

	/**
	 * Signal a {@link TimeoutException} error in case an item doesn't arrive before the given period,
	 * as measured on the provided {@link Scheduler}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutForMono.svg" alt="">
	 *
	 * @param timeout the timeout before the onNext signal from this {@link Mono}
	 * @param timer a time-capable {@link Scheduler} instance to run the delay on
	 *
	 * @return an expirable {@link Mono}
	 */
	public final Mono<T> timeout(Duration timeout, Scheduler timer) {
		return timeout(timeout, null, timer);
	}

	/**
	 * Switch to a fallback {@link Mono} in case an item doesn't arrive before the given period,
	 * as measured on the provided {@link Scheduler}.
	 *
	 * <p> If the given {@link Mono} is null, signal a {@link TimeoutException}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutFallbackForMono.svg" alt="">
	 *
	 * @param timeout the timeout before the onNext signal from this {@link Mono}
	 * @param fallback the fallback {@link Mono} to subscribe when a timeout occurs
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return an expirable {@link Mono} with a fallback {@link Mono}
	 */
	public final Mono<T> timeout(Duration timeout, @Nullable Mono<? extends T> fallback,
			Scheduler timer) {
		final Mono<Long> _timer = Mono.delay(timeout, timer).onErrorReturn(0L);

		if(fallback == null) {
			return onAssembly(new MonoTimeout<>(this, _timer, timeout.toMillis() + "ms"));
		}
		return onAssembly(new MonoTimeout<>(this, _timer, fallback));
	}

	/**
	 * Signal a {@link TimeoutException} in case the item from this {@link Mono} has
	 * not been emitted before the given {@link Publisher} emits.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutPublisher.svg" alt="">
	 *
	 * @param firstTimeout the timeout {@link Publisher} that must not emit before the first signal from this {@link Mono}
	 * @param <U> the element type of the timeout Publisher
	 *
	 * @return an expirable {@link Mono} if the item does not come before a {@link Publisher} signals
	 *
	 */
	public final <U> Mono<T> timeout(Publisher<U> firstTimeout) {
		return onAssembly(new MonoTimeout<>(this, firstTimeout, "first signal from a Publisher"));
	}

	/**
	 * Switch to a fallback {@link Publisher} in case the  item from this {@link Mono} has
	 * not been emitted before the given {@link Publisher} emits.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutPublisherAndFallbackForMono.svg" alt="">
	 *
	 * @param firstTimeout the timeout
	 * {@link Publisher} that must not emit before the first signal from this {@link Mono}
	 * @param fallback the fallback {@link Publisher} to subscribe when a timeout occurs
	 * @param <U> the element type of the timeout Publisher
	 *
	 * @return an expirable {@link Mono} with a fallback {@link Mono} if the item doesn't
	 * come before a {@link Publisher} signals
	 *
	 */
	public final <U> Mono<T> timeout(Publisher<U> firstTimeout, Mono<? extends T> fallback) {
		return onAssembly(new MonoTimeout<>(this, firstTimeout, fallback));
	}

	/**
	 * If this {@link Mono} is valued, emit a {@link reactor.util.function.Tuple2} pair of
	 * T1 the current clock time in millis (as a {@link Long} measured by the
	 * {@link Schedulers#parallel() parallel} Scheduler) and T2 the emitted data (as a {@code T}).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timestampForMono.svg" alt="">
	 *
	 * @return a timestamped {@link Mono}
	 * @see #timed()
	 */
	public final Mono<Tuple2<Long, T>> timestamp() {
		return timestamp(Schedulers.parallel());
	}

	/**
	 * If this {@link Mono} is valued, emit a {@link reactor.util.function.Tuple2} pair of
	 * T1 the current clock time in millis (as a {@link Long} measured by the
	 * provided {@link Scheduler}) and T2 the emitted data (as a {@code T}).
	 *
	 * <p>The provider {@link Scheduler} will be asked to {@link Scheduler#now(TimeUnit) provide time}
	 * with a granularity of {@link TimeUnit#MILLISECONDS}. In order for this operator to work as advertised, the
	 * provided Scheduler should thus return results that can be interpreted as unix timestamps.</p>
	 * <p>
	 *
	 * <img class="marble" src="doc-files/marbles/timestampForMono.svg" alt="">
	 *
	 * @param scheduler a {@link Scheduler} instance to read time from
	 * @return a timestamped {@link Mono}
	 * @see Scheduler#now(TimeUnit)
	 * @see #timed(Scheduler)
	 */
	public final Mono<Tuple2<Long, T>> timestamp(Scheduler scheduler) {
		Objects.requireNonNull(scheduler, "scheduler");
		return map(d -> Tuples.of(scheduler.now(TimeUnit.MILLISECONDS), d));
	}

	/**
	 * Transform this {@link Mono} into a {@link CompletableFuture} completing on onNext or onComplete and failing on
	 * onError.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/toFuture.svg" alt="">
	 *
	 * @return a {@link CompletableFuture}
	 */
	public final CompletableFuture<T> toFuture() {
		return subscribeWith(new MonoToCompletableFuture<>(false));
	}

	/**
	 * Wrap this {@link Mono} into a {@link MonoProcessor} (turning it hot and allowing to block,
	 * cancel, as well as many other operations). Note that the {@link MonoProcessor}
	 * is subscribed to its parent source if any.
	 *
	 * @return a {@link MonoProcessor} to use to either retrieve value or cancel the underlying {@link Subscription}
	 * @deprecated prefer {@link #share()} to share a parent subscription, or use {@link Sinks}
	 */
	@Deprecated
	public final MonoProcessor<T> toProcessor() {
		if (this instanceof MonoProcessor) {
			return (MonoProcessor<T>) this;
		}
		else {
			NextProcessor<T> result = new NextProcessor<>(this);
			result.connect();
			return result;
		}
	}

	/**
	 * Transform this {@link Mono} in order to generate a target {@link Mono}. Unlike {@link #transformDeferred(Function)}, the
	 * provided function is executed as part of assembly.
	 *
	 * <pre>
	 * Function<Mono, Mono> applySchedulers = mono -> mono.subscribeOn(Schedulers.io())
	 *                                                    .publishOn(Schedulers.parallel());
	 * mono.transform(applySchedulers).map(v -> v * v).subscribe();
	 * </pre>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/transformForMono.svg" alt="">
	 *
	 * @param transformer the {@link Function} to immediately map this {@link Mono} into a target {@link Mono}
	 * instance.
	 * @param <V> the item type in the returned {@link Mono}
	 *
	 * @return a new {@link Mono}
	 * @see #transformDeferred(Function) transformDeferred(Function) for deferred composition of Mono for each Subscriber
	 * @see #as(Function) as(Function) for a loose conversion to an arbitrary type
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public final <V> Mono<V> transform(Function<? super Mono<T>, ? extends Publisher<V>> transformer) {
		if (Hooks.DETECT_CONTEXT_LOSS) {
			transformer = new ContextTrackingFunctionWrapper(transformer);
		}
		return onAssembly(from(transformer.apply(this)));
	}

	/**
	 * Defer the given transformation to this {@link Mono} in order to generate a
	 * target {@link Mono} type. A transformation will occur for each
	 * {@link Subscriber}. For instance:
	 *
	 * <blockquote><pre>
	 * mono.transformDeferred(original -> original.log());
	 * </pre></blockquote>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/transformDeferredForMono.svg" alt="">
	 *
	 * @param transformer the {@link Function} to lazily map this {@link Mono} into a target {@link Mono}
	 * instance upon subscription.
	 * @param <V> the item type in the returned {@link Publisher}
	 *
	 * @return a new {@link Mono}
	 * @see #transform(Function) transform(Function) for immmediate transformation of Mono
	 * @see #transformDeferredContextual(BiFunction) transformDeferredContextual(BiFunction) for a similarly deferred transformation of Mono reading the ContextView
	 * @see #as(Function) as(Function) for a loose conversion to an arbitrary type
	 */
	public final <V> Mono<V> transformDeferred(Function<? super Mono<T>, ? extends Publisher<V>> transformer) {
		return defer(() -> {
			if (Hooks.DETECT_CONTEXT_LOSS) {
				@SuppressWarnings({"unchecked", "rawtypes"})
				Mono<V> result = from(new ContextTrackingFunctionWrapper<T, V>((Function) transformer).apply(this));
				return result;
			}
			return from(transformer.apply(this));
		});
	}

	/**
	 * Defer the given transformation to this {@link Mono} in order to generate a
	 * target {@link Mono} type. A transformation will occur for each
	 * {@link Subscriber}. In addition, the transforming {@link BiFunction} exposes
	 * the {@link ContextView} of each {@link Subscriber}. For instance:
	 *
	 * <blockquote><pre>
	 * Mono&lt;T> monoLogged = mono.transformDeferredContextual((original, ctx) -> original.log("for RequestID" + ctx.get("RequestID"))
	 * //...later subscribe. Each subscriber has its Context with a RequestID entry
	 * monoLogged.contextWrite(Context.of("RequestID", "requestA").subscribe();
	 * monoLogged.contextWrite(Context.of("RequestID", "requestB").subscribe();
	 * </pre></blockquote>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/transformDeferredForMono.svg" alt="">
	 *
	 * @param transformer the {@link BiFunction} to lazily map this {@link Mono} into a target {@link Mono}
	 * instance upon subscription, with access to {@link ContextView}
	 * @param <V> the item type in the returned {@link Publisher}
	 * @return a new {@link Mono}
	 * @see #transform(Function) transform(Function) for immmediate transformation of Mono
	 * @see #transformDeferred(Function) transformDeferred(Function) for a similarly deferred transformation of Mono without the ContextView
	 * @see #as(Function) as(Function) for a loose conversion to an arbitrary type
	 */
	public final <V> Mono<V> transformDeferredContextual(BiFunction<? super Mono<T>, ? super ContextView, ? extends Publisher<V>> transformer) {
		return deferContextual(ctxView -> {
			if (Hooks.DETECT_CONTEXT_LOSS) {
				ContextTrackingFunctionWrapper<T, V> wrapper = new ContextTrackingFunctionWrapper<>(
						publisher -> transformer.apply(wrap(publisher, false), ctxView),
						transformer.toString()
				);
				return wrap(wrapper.apply(this), true);
			}
			return from(transformer.apply(this, ctxView));
		});
	}

	/**
	 * Wait for the result from this mono, use it to create a second mono via the
	 * provided {@code rightGenerator} function and combine both results into a {@link Tuple2}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipWhenForMono.svg" alt="">
	 *
	 * @param rightGenerator the {@link Function} to generate a {@code Mono} to combine with
	 * @param <T2> the element type of the other Mono instance
	 *
	 * @return a new combined Mono
	 */
	public final <T2> Mono<Tuple2<T, T2>> zipWhen(Function<T, Mono<? extends T2>> rightGenerator) {
		return zipWhen(rightGenerator, Tuples::of);
	}

	/**
	 * Wait for the result from this mono, use it to create a second mono via the
	 * provided {@code rightGenerator} function and combine both results into an arbitrary
	 * {@code O} object, as defined by the provided {@code combinator} function.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipWhenWithZipperForMono.svg" alt="">
	 *
	 * @param rightGenerator the {@link Function} to generate a {@code Mono} to combine with
	 * @param combinator a {@link BiFunction} combinator function when both sources complete
	 * @param <T2> the element type of the other Mono instance
	 * @param <O> the element type of the combination
	 *
	 * @return a new combined Mono
	 */
	public final <T2, O> Mono<O> zipWhen(Function<T, Mono<? extends T2>> rightGenerator,
			BiFunction<T, T2, O> combinator) {
		Objects.requireNonNull(rightGenerator, "rightGenerator function is mandatory to get the right-hand side Mono");
		Objects.requireNonNull(combinator, "combinator function is mandatory to combine results from both Monos");
		return flatMap(t -> rightGenerator.apply(t).map(t2 -> combinator.apply(t, t2)));
	}

	/**
	 * Combine the result from this mono and another into a {@link Tuple2}.
	 * <p>
	 * An error or <strong>empty</strong> completion of any source will cause the other source
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipWithOtherForMono.svg" alt="">
	 *
	 * @param other the {@link Mono} to combine with
	 * @param <T2> the element type of the other Mono instance
	 *
	 * @return a new combined Mono
	 */
	public final <T2> Mono<Tuple2<T, T2>> zipWith(Mono<? extends T2> other) {
		return zipWith(other, Flux.tuple2Function());
	}

	/**
	 * Combine the result from this mono and another into an arbitrary {@code O} object,
	 * as defined by the provided {@code combinator} function.
	 * <p>
	 * An error or <strong>empty</strong> completion of any source will cause the other source
	 * to be cancelled and the resulting Mono to immediately error or complete, respectively.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipWithOtherUsingZipperForMono.svg" alt="">
	 *
	 * @param other the {@link Mono} to combine with
	 * @param combinator a {@link BiFunction} combinator function when both sources
	 * complete
	 * @param <T2> the element type of the other Mono instance
	 * @param <O> the element type of the combination
	 *
	 * @return a new combined Mono
	 */
	public final <T2, O> Mono<O> zipWith(Mono<? extends T2> other,
			BiFunction<? super T, ? super T2, ? extends O> combinator) {
		if (this instanceof MonoZip) {
			@SuppressWarnings("unchecked") MonoZip<T, O> o = (MonoZip<T, O>) this;
			Mono<O> result = o.zipAdditionalSource(other, combinator);
			if (result != null) {
				return result;
			}
		}

		return zip(this, other, combinator);
	}

	/**
	 * To be used by custom operators: invokes assembly {@link Hooks} pointcut given a
	 * {@link Mono}, potentially returning a new {@link Mono}. This is for example useful
	 * to activate cross-cutting concerns at assembly time, eg. a generalized
	 * {@link #checkpoint()}.
	 *
	 * @param <T> the value type
	 * @param source the source to apply assembly hooks onto
	 *
	 * @return the source, potentially wrapped with assembly time cross-cutting behavior
	 */
	@SuppressWarnings("unchecked")
	protected static <T> Mono<T> onAssembly(Mono<T> source) {
		Function<Publisher, Publisher> hook = Hooks.onEachOperatorHook;
		if(hook != null) {
			source = (Mono<T>) hook.apply(source);
		}
		if (Hooks.GLOBAL_TRACE) {
			AssemblySnapshot stacktrace = new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get());
			source = (Mono<T>) Hooks.addAssemblyInfo(source, stacktrace);
		}
		return source;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}


	static <T> Mono<Void> empty(Publisher<T> source) {
		@SuppressWarnings("unchecked")
		Mono<Void> then = (Mono<Void>)ignoreElements(source);
		return then;
	}

	static <T> Mono<T> doOnSignal(Mono<T> source,
			@Nullable Consumer<? super Subscription> onSubscribe,
			@Nullable Consumer<? super T> onNext,
			@Nullable LongConsumer onRequest,
			@Nullable Runnable onCancel) {
		if (source instanceof Fuseable) {
			return onAssembly(new MonoPeekFuseable<>(source,
					onSubscribe,
					onNext,
					onRequest,
					onCancel));
		}
		return onAssembly(new MonoPeek<>(source,
				onSubscribe,
				onNext,
				onRequest,
				onCancel));
	}

	static <T> Mono<T> doOnTerminalSignal(Mono<T> source,
			@Nullable Consumer<? super T> onSuccess,
			@Nullable Consumer<? super Throwable> onError,
			@Nullable BiConsumer<? super T, Throwable> onAfterTerminate) {
		return onAssembly(new MonoPeekTerminal<>(source, onSuccess, onError, onAfterTerminate));
	}

	/**
	 * Unchecked wrap of {@link Publisher} as {@link Mono}, supporting {@link Fuseable} sources.
	 * When converting a {@link Mono} or {@link Mono Monos} that have been converted to a {@link Flux} and back,
	 * the original {@link Mono} is returned unwrapped.
	 * Note that this bypasses {@link Hooks#onEachOperator(String, Function) assembly hooks}.
	 *
	 * @param source the {@link Publisher} to wrap
	 * @param enforceMonoContract {@code} true to wrap publishers without assumption about their cardinality
	 * (first {@link Subscriber#onNext(Object)} will cancel the source), {@code false} to behave like {@link #fromDirect(Publisher)}.
	 * @param <T> input upstream type
	 * @return a wrapped {@link Mono}
	 */
	static <T> Mono<T> wrap(Publisher<T> source, boolean enforceMonoContract) {
		//some sources can be considered already assembled monos
		//all conversion methods (from, fromDirect, wrap) must accommodate for this
		if (source instanceof Mono) {
			return (Mono<T>) source;
		}
		if (source instanceof FluxSourceMono
				|| source instanceof FluxSourceMonoFuseable) {
			@SuppressWarnings("unchecked")
			Mono<T> extracted = (Mono<T>) ((FluxFromMonoOperator<T,T>) source).source;
			return extracted;
		}

		//equivalent to what from used to be, without assembly hooks
		if (enforceMonoContract) {
			if (source instanceof Flux && source instanceof Callable) {
					@SuppressWarnings("unchecked") Callable<T> m = (Callable<T>) source;
					return Flux.wrapToMono(m);
			}
			if (source instanceof Flux) {
				return new MonoNext<>((Flux<T>) source);
			}
			return new MonoFromPublisher<>(source);
		}

		//equivalent to what fromDirect used to be without onAssembly
		if(source instanceof Flux && source instanceof Fuseable) {
			return new MonoSourceFluxFuseable<>((Flux<T>) source);
		}
		if (source instanceof Flux) {
			return new MonoSourceFlux<>((Flux<T>) source);
		}
		if(source instanceof Fuseable) {
			return new MonoSourceFuseable<>(source);
		}
		return new MonoSource<>(source);
	}

	@SuppressWarnings("unchecked")
	static <T> BiPredicate<? super T, ? super T> equalsBiPredicate(){
		return EQUALS_BIPREDICATE;
	}
	static final BiPredicate EQUALS_BIPREDICATE = Object::equals;
}
