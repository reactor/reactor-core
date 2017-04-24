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

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
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
import java.util.stream.LongStream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuple5;
import reactor.util.function.Tuple6;
import reactor.util.function.Tuples;

/**
 * A Reactive Streams {@link Publisher} with basic rx operators that completes successfully by emitting an element, or
 * with an error.
 *
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/mono.png" alt="">
 * <p>
 *
 * <p>The rx operators will offer aliases for input {@link Mono} type to preserve the "at most one"
 * property of the resulting {@link Mono}. For instance {@link Mono#flatMap flatMap} returns a
 * {@link Mono}, while there is a {@link Mono#flatMapMany flatMapMany} alias with possibly more than 1 emission.
 *
 * <p>{@code Mono<Void>} should be used for {@link Publisher} that just completes without any value.
 *
 * <p>It is intended to be used in implementations and return types, input parameters should keep using raw {@link
 * Publisher} as much as possible.
 *
 * <p>Note that using state in the {@code java.util.function} / lambdas used within Mono operators
 * should be avoided, as these may be shared between several {@link Subscriber Subscribers}.
 *
 * @param <T> the type of the single value of this class
 * 
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @author David Karnok
 * @author Simon Baslé
 *
 * @see Flux
 */
public abstract class Mono<T> implements Publisher<T> {

//	 ==============================================================================================================
//	 Static Generators
//	 ==============================================================================================================

	/**
	 * Creates a deferred emitter that can be used with callback-based
	 * APIs to signal at most one value, a complete or an error signal.
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
	 *             sink.error(new RuntimeExeption("Failed"));
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
     * <code></pre>
	 *
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/defer1.png" alt="">
	 * <p>
	 * @param supplier a {@link Mono} factory
	 * 
	 * @param <T> the element type of the returned Mono instance
	 *
	 * @return a new {@link Mono} factory
	 */
	public static <T> Mono<T> defer(Supplier<? extends Mono<? extends T>> supplier) {
		return onAssembly(new MonoDefer<>(supplier));
	}

	/**
	 * Create a Mono which delays an onNext signal by a given {@link Duration duration}
	 * on a default Scheduler and completes.
	 * If the demand cannot be produced in time, an onError will be signalled instead.
	 * The delay is introduced through the {@link Schedulers#parallel() parallel} default Scheduler.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/delay.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/delay.png" alt="">
	 * <p>
	 * @param duration the {@link Duration} of the delay
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a new {@link Mono}
	 */
	public static Mono<Long> delay(Duration duration, Scheduler timer) {
		return onAssembly(new MonoDelay(duration.toMillis(), TimeUnit.MILLISECONDS, timer));
	}

	/**
	 * Create a {@link Mono} that completes without emitting any item.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/empty.png" alt="">
	 * <p>
	 * @param <T> the reified {@link Subscriber} type
	 *
	 * @return a completed {@link Mono}
	 */
	public static <T> Mono<T> empty() {
		return MonoEmpty.instance();
	}

	/**
	 * Create a new {@link Mono} from a {@link Publisher} that ignores its onNext signals
	 * (dropping the emitted items) and only react to completion signal.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/thens.png" alt="">
	 * <p>
	 * @param source the {@link Publisher to ignore}
	 * @param <T> the reified {@link Publisher} type
	 *
	 * @return a new completable {@link Mono}.
	 */
	public static <T> Mono<Void> empty(Publisher<T> source) {
		@SuppressWarnings("unchecked")
		Mono<Void> then = (Mono<Void>)new MonoIgnoreEmpty<>(source);
		return onAssembly(then);
	}

	/**
	 * Create a {@link Mono} that terminates with the specified error immediately after
	 * being subscribed to.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/error.png" alt="">
	 * <p>
	 * @param error the onError signal
	 * @param <T> the reified {@link Subscriber} type
	 *
	 * @return a failed {@link Mono}
	 */
	public static <T> Mono<T> error(Throwable error) {
		return onAssembly(new MonoError<>(error));
	}

	/**
	 * Pick the first {@link Mono} to emit any signal (value, empty completion or error)
	 * and replay that signal, effectively behaving like the fastest of these competing
	 * sources.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/first.png" alt="">
	 * <p>
	 * @param monos The deferred monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a new {@link Mono} behaving like the fastest of its sources.
	 */
	@SafeVarargs
	public static <T> Mono<T> first(Mono<? extends T>... monos) {
		return onAssembly(new MonoFirst<>(monos));
	}

	/**
	 * Pick the first available result coming from any of the given monos and populate a new {@literal Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/first.png" alt="">
	 * <p>
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	public static <T> Mono<T> first(Iterable<? extends Mono<? extends T>> monos) {
		return onAssembly(new MonoFirst<>(monos));
	}

	/**
	 * Expose the specified {@link Publisher} with the {@link Mono} API, and ensure it will emit 0 or 1 item.
	 * The source emitter will be cancelled on the first `onNext`.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/from1.png" alt="">
	 * <p>
	 * @param source the {@link Publisher} source
	 * @param <T> the source type
	 *
	 * @return the next item emitted as a {@link Mono}
	 */
	public static <T> Mono<T> from(Publisher<? extends T> source) {
		if (source instanceof Mono) {
			@SuppressWarnings("unchecked")
			Mono<T> casted = (Mono<T>) source;
			return casted;
		}
		if (source instanceof Fuseable.ScalarCallable) {
			@SuppressWarnings("unchecked")
            T t = ((Fuseable.ScalarCallable<T>) source).call();
            if (t != null) {
                return just(t);
            }
			return empty();
		}
		return onAssembly(new MonoNext<>(source));
	}

	/**
	 * Create a {@link Mono} producing its value using the provided {@link Callable}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/fromcallable.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/fromfuture.png" alt="">
	 * <p>
	 * @param completionStage {@link CompletionStage} that will produce the value or null to
	 * complete immediately
	 * @param <T> type of the expected value
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> fromCompletionStage(CompletionStage<? extends T> completionStage) {
		return onAssembly(new MonoCompletionStage<>(completionStage));
	}

	/**
	 * Convert a {@link Publisher} to a {@link Mono} without any cardinality check
	 * (ie this method doesn't check if the source is already a Mono, nor cancels the
	 * source past the first element). Conversion supports {@link Fuseable} sources.
	 * Note this is an advanced interoperability operator that implies you know the
	 * {@link Publisher} you are converting follows the {@link Mono} semantics and only
	 * ever emits one element.
	 *
	 * @param source the Mono-compatible {@link Publisher} to wrap
	 * @param <I> type of the value emitted by the publisher
	 * @return a wrapped {@link Mono}
	 */
	public static <I> Mono<I> fromDirect(Publisher<? extends I> source){
		if(source instanceof Fuseable){
			return new MonoSource.FuseableMonoSource<>(source);
		}
		return new MonoSource<>(source);
	}

	/**
	 * Create a {@link Mono}, producing its value using the provided {@link CompletableFuture}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/fromfuture.png" alt="">
	 * <p>
	 * @param future {@link CompletableFuture} that will produce the value or null to
	 * complete immediately
	 * @param <T> type of the expected value
	 * @return A {@link Mono}.
	 * @see #fromCompletionStage(CompletionStage) fromCompletionStage for a generalization
	 */
	public static <T> Mono<T> fromFuture(CompletableFuture<? extends T> future) {
		return onAssembly(new MonoCompletionStage<>(future));
	}

	/**
	 * Create a {@link Mono} that completes empty once the provided {@link Runnable} has
	 * been executed.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/fromrunnable.png" alt="">
	 * <p>
	 * @param runnable {@link Runnable} that will be executed before emitting the completion signal
	 *
	 * @return A {@link Mono}.
	 */
	public static Mono<Void> fromRunnable(Runnable runnable) {
		return onAssembly(new MonoRunnable(runnable));
	}

	/**
	 * Create a {@link Mono}, producing its value using the provided {@link Supplier}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/fromsupplier.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/ignoreelements.png" alt="">
	 * <p>
	 * @param source the {@link Publisher} to ignore
	 * @param <T> the source type of the ignored data
	 *
	 * @return a new completable {@link Mono}.
	 */
	public static <T> Mono<T> ignoreElements(Publisher<T> source) {
		return onAssembly(new MonoIgnoreEmpty<>(source));
	}

	/**
	 * Create a new {@link Mono} that emits the specified item, which is captured at
	 * instantiation time.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/just.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/justorempty.png" alt="">
	 * <p>
	 * @param data the {@link Optional} item to onNext or onComplete if not present
	 * @param <T> the type of the produced item
	 *
	 * @return a {@link Mono}.
	 */
	public static <T> Mono<T> justOrEmpty(Optional<? extends T> data) {
		return data != null && data.isPresent() ? just(data.get()) : empty();
	}

	/**
	 * Create a new {@link Mono} that emits the specified item if non null otherwise only emits
	 * onComplete.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/justorempty.png" alt="">
	 * <p>
	 * @param data the item to onNext or onComplete if null
	 * @param <T> the type of the produced item
	 *
	 * @return a {@link Mono}.
	 */
	public static <T> Mono<T> justOrEmpty(T data) {
		return data != null ? just(data) : empty();
	}


	/**
	 * Return a {@link Mono} that will never signal any data, error or completion signal,
	 * essentially running indefinitely.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/never.png" alt="">
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
	 * 
	 * @param source1
	 *            the first Publisher to compare
	 * @param source2
	 *            the second Publisher to compare
	 * @param <T>
	 *            the type of items emitted by each Publisher
	 * @return a Mono that emits a Boolean value that indicates whether the two sequences are the same
	 */
	public static <T> Mono<Boolean> sequenceEqual(Publisher<? extends T> source1, Publisher<? extends T> source2) {
		return sequenceEqual(source1, source2, equalsBiPredicate(), QueueSupplier.SMALL_BUFFER_SIZE);
	}

	/**
	 * Returns a Mono that emits a Boolean value that indicates whether two Publisher sequences are the
	 * same by comparing the items emitted by each Publisher pairwise based on the results of a specified
	 * equality function.
	 *
	 * @param source1
	 *            the first Publisher to compare
	 * @param source2
	 *            the second Publisher to compare
	 * @param isEqual
	 *            a function used to compare items emitted by each Publisher
	 * @param <T>
	 *            the type of items emitted by each Publisher
	 * @return a Mono that emits a Boolean value that indicates whether the two Publisher two sequences
	 *         are the same according to the specified function
	 */
	public static <T> Mono<Boolean> sequenceEqual(Publisher<? extends T> source1, Publisher<? extends T> source2,
			BiPredicate<? super T, ? super T> isEqual) {
		return sequenceEqual(source1, source2, isEqual, QueueSupplier.SMALL_BUFFER_SIZE);
	}

	/**
	 * Returns a Mono that emits a Boolean value that indicates whether two Publisher sequences are the
	 * same by comparing the items emitted by each Publisher pairwise based on the results of a specified
	 * equality function.
	 *
	 * @param source1
	 *            the first Publisher to compare
	 * @param source2
	 *            the second Publisher to compare
	 * @param isEqual
	 *            a function used to compare items emitted by each Publisher
	 * @param bufferSize
	 *            the number of items to prefetch from the first and second source Publisher
	 * @param <T>
	 *            the type of items emitted by each Publisher
	 * @return a Mono that emits a Boolean value that indicates whether the two Publisher two sequences
	 *         are the same according to the specified function
	 */
	public static <T> Mono<Boolean> sequenceEqual(Publisher<? extends T> source1, 
			Publisher<? extends T> source2,
			BiPredicate<? super T, ? super T> isEqual, int bufferSize) {
		return onAssembly(new MonoSequenceEqual<>(source1, source2, isEqual, bufferSize));
	}

	/**
	 * Uses a resource, generated by a supplier for each individual Subscriber, while streaming the value from a
	 * Mono derived from the same resource and makes sure the resource is released if the
	 * sequence terminates or the Subscriber cancels.
	 * <p>
	 * <ul> <li>Eager resource cleanup happens just before the source termination and exceptions raised by the cleanup
	 * Consumer may override the terminal event.</li> <li>Non-eager cleanup will drop any exception.</li> </ul>
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/using.png"
	 * alt="">
	 *
	 * @param resourceSupplier a {@link Callable} that is called on subscribe to create the resource
	 * @param sourceSupplier a {@link Mono} factory to create the Mono depending on the created resource
	 * @param resourceCleanup invoked on completion to clean-up the resource
	 * @param eager set to true to clean before terminating downstream subscribers
	 * @param <T> emitted type
	 * @param <D> resource type
	 *
	 * @return new {@link Mono}
	 */
	public static <T, D> Mono<T> using(Callable<? extends D> resourceSupplier, Function<?
			super D, ? extends
			Mono<? extends T>> sourceSupplier, Consumer<? super D> resourceCleanup, boolean eager) {
		return onAssembly(new MonoUsing<>(resourceSupplier, sourceSupplier,
				resourceCleanup, eager));
	}

	/**
	 * Uses a resource, generated by a supplier for each individual Subscriber, while streaming the value from a
	 * Mono derived from the same resource and makes sure the resource is released if the
	 * sequence terminates or the Subscriber cancels.
	 * <p>
	 * Eager resource cleanup happens just before the source termination and exceptions raised by the cleanup Consumer
	 * may override the terminal event.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/using.png"
	 * alt="">
	 *
	 * @param resourceSupplier a {@link Callable} that is called on subscribe to create the resource
	 * @param sourceSupplier a {@link Mono} factory to create the Mono depending on the created resource
	 * @param resourceCleanup invoked on completion to clean-up the resource
	 * @param <T> emitted type
	 * @param <D> resource type
	 *
	 * @return new {@link Mono}
	 */
	public static <T, D> Mono<T> using(Callable<? extends D> resourceSupplier, Function<?
			super D, ? extends
			Mono<? extends T>> sourceSupplier, Consumer<? super D> resourceCleanup) {
		return using(resourceSupplier, sourceSupplier, resourceCleanup, true);
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled, aggregating their values into a {@link Tuple2}.
	 * An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 *
	 * @return a {@link Mono}.
	 */
	public static <T1, T2> Mono<Tuple2<T1, T2>> when(Mono<? extends T1> p1, Mono<? extends T2> p2) {
		return when(p1, p2, Flux.tuple2Function());
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled, aggregating their values as defined by the combinator function.
	 * An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param combinator a {@link BiFunction} combinator function when both sources
	 * complete
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <O> output value
	 *
	 * @return a {@link Mono}.
	 */
	public static <T1, T2, O> Mono<O> when(Mono<? extends T1> p1, Mono<?
			extends T2> p2, BiFunction<? super T1, ? super T2, ? extends O> combinator) {
		return onAssembly(new MonoWhen<T1, O>(false, p1, p2, combinator));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled, aggregating their values into a {@link Tuple3}.
	 * An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3> Mono<Tuple3<T1, T2, T3>> when(Mono<? extends T1> p1, Mono<? extends T2> p2, Mono<? extends T3> p3) {
		return onAssembly(new MonoWhen(false, a -> Tuples.fromArray((Object[])a), p1, p2, p3));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled, aggregating their values into a {@link Tuple4}.
	 * An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4> Mono<Tuple4<T1, T2, T3, T4>> when(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4) {
		return onAssembly(new MonoWhen(false, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled, aggregating their values into a {@link Tuple5}.
	 * An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5> Mono<Tuple5<T1, T2, T3, T4, T5>> when(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5) {
		return onAssembly(new MonoWhen(false, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled, aggregating their values into a {@link Tuple6}.
	 * An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param p6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 * @param <T6> type of the value from source6
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5, T6> Mono<Tuple6<T1, T2, T3, T4, T5, T6>> when(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5,
			Mono<? extends T6> p6) {
        return onAssembly(new MonoWhen(false, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5, p6));
	}

	/**
	 * Aggregate given void publishers into a new {@literal Mono} that will be
	 * fulfilled when all of the given {@literal Publishers} have been fulfilled.
	 * An error will cause pending results to be cancelled and immediate error emission
	 * to the returned {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 *
	 * @param sources The sources to use.
	 *
	 * @return a {@link Mono}.
	 */
	public static Mono<Void> when(final Iterable<? extends Publisher<Void>> sources) {
		return onAssembly(new MonoWhen<>(false, VOID_FUNCTION, sources));
	}

	/**
	 * Aggregate given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal
	 * Monos} have been fulfilled, aggregating their values according to the provided combinator function.
	 * If any Mono terminates without value, the returned sequence will be terminated immediately and pending results cancelled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 *
	 * @param monos The monos to use.
	 * @param combinator the function to transform the combined array into an arbitrary
	 * object.
	 * @param <R> the combined result
	 *
	 * @return a {@link Mono}.
	 */
	public static <R> Mono<R> when(final Iterable<? extends Mono<?>> monos, Function<? super Object[], ? extends R> combinator) {
		return onAssembly(new MonoWhen<>(false, combinator, monos));
	}

	/**
	 * Aggregate given void publishers into a new {@literal Mono} that will be fulfilled
	 * when all of the given {@literal sources} have been fulfilled. An error will cause
	 * pending results to be cancelled and immediate error emission to the returned {@link Mono}.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param sources The sources to use.
	 *
	 * @return a {@link Mono}.
	 */
	@SafeVarargs
	public static Mono<Void> when(Publisher<Void>... sources) {
		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from(sources[0]);
		}
		return onAssembly(new MonoWhen<>(false, VOID_FUNCTION, sources));
	}

	/**
	 * Aggregate given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal
	 * Monos} have been fulfilled, aggregating their values according to the provided combinator function.
	 * An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Mono}.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param monos The monos to use.
	 * @param combinator the function to transform the combined array into an arbitrary
	 * object.
	 * @param <R> the combined result
	 *
	 * @return a {@link Mono}.
	 */
	public static <R> Mono<R> when(Function<? super Object[], ? extends R> combinator, Mono<?>... monos) {
		if (monos.length == 0) {
			return empty();
		}
		if (monos.length == 1) {
			return monos[0].map(d -> combinator.apply(new Object[]{d}));
		}
		return onAssembly(new MonoWhen<>(false, combinator, monos));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled, aggregating their values into a {@link Tuple2} and delaying errors.
	 * If both Monos error, the two exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2> Mono<Tuple2<T1, T2>> whenDelayError(Mono<? extends T1> p1, Mono<? extends T2> p2) {
		return onAssembly(new MonoWhen(true, a -> Tuples.fromArray((Object[])a), p1, p2));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled, aggregating their values into a {@link Tuple3} and delaying errors.
	 * If several Monos error, the two exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3> Mono<Tuple3<T1, T2, T3>> whenDelayError(Mono<? extends T1> p1, Mono<? extends T2> p2, Mono<? extends T3> p3) {
		return onAssembly(new MonoWhen(true, a -> Tuples.fromArray((Object[])a), p1, p2, p3));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled, aggregating their values into a {@link Tuple4} and delaying errors.
	 * If several Monos error, the exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4> Mono<Tuple4<T1, T2, T3, T4>> whenDelayError(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4) {
		return onAssembly(new MonoWhen(true, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled, aggregating their values into a {@link Tuple5} and delaying errors.
	 * If several Monos error, the exceptions are combined (as suppressed exceptions on a root exception).
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5> Mono<Tuple5<T1, T2, T3, T4, T5>> whenDelayError(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5) {
		return onAssembly(new MonoWhen(true, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5));
	}

	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled, aggregating their values into a {@link Tuple6} and delaying errors.
	 * If several Monos error, the exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param p3 The third upstream {@link Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param p6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 * @param <T6> type of the value from source6
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T1, T2, T3, T4, T5, T6> Mono<Tuple6<T1, T2, T3, T4, T5, T6>> whenDelayError(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5,
			Mono<? extends T6> p6) {
		return onAssembly(new MonoWhen(true, a -> Tuples.fromArray((Object[])a), p1, p2, p3, p4, p5, p6));
	}

	/**
	 * Aggregate given void publishers into a new {@literal Mono} that will be
	 * fulfilled when all of the given {@literal sources} have been fulfilled. If any Publisher
	 * terminates without value, the returned sequence will be terminated immediately and
	 * pending results cancelled. Errors from the sources are delayed.
	 * If several Publishers error, the exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 *
	 * @param sources The sources to use.
	 *
	 * @return a {@link Mono}.
	 */
	public static Mono<Void> whenDelayError(final Iterable<? extends Publisher<Void>> sources) {
		return onAssembly(new MonoWhen<>(true, VOID_FUNCTION, sources));
	}

	/**
	 * Aggregate given monos into a new {@literal Mono} that will be fulfilled when all of the given {@literal
	 * Monos} have been fulfilled. If any Mono terminates without value, the returned sequence will be terminated
	 * immediately and pending results cancelled. Errors from the sources are delayed.
	 * If several Monos error, the exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 *
	 * @param monos The monos to use.
	 * @param combinator the function to transform the combined array into an arbitrary
	 * object.
	 * @param <R> the combined result
	 *
	 * @return a {@link Mono}.
	 */
	public static <R> Mono<R> whenDelayError(final Iterable<? extends Mono<?>> monos, Function<? super Object[], ? extends R> combinator) {
		return onAssembly(new MonoWhen<>(true, combinator, monos));
	}

	/**
	 * Merge given void publishers into a new {@literal Mono} that will be fulfilled when
	 * all of the given {@literal sources} have been fulfilled. Errors from the sources are delayed.
	 * If several Publishers error, the exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param sources The sources to use.
	 *
	 * @return a {@link Mono}.
	 */
	@SafeVarargs
	public static  Mono<Void> whenDelayError(Publisher<Void>... sources) {
		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from(sources[0]);
		}
		return onAssembly(new MonoWhen<>(true, VOID_FUNCTION, sources));
	}


	/**
	 * Merge given monos into a new {@literal Mono} that will be fulfilled when all of the
	 * given {@literal Monos} have been fulfilled, aggregating their values according to
	 * the provided combinator function and delaying errors.
	 * If several Monos error, the exceptions are combined (as suppressed exceptions on a root exception).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/whent.png" alt="">
	 * <p>
	 * @param monos The monos to use.
	 * @param combinator the function to transform the combined array into an arbitrary
	 * object.
	 * @param <R> the combined result
	 *
	 * @return a combined {@link Mono}.
	 */
	public static <R>  Mono<R> whenDelayError(Function<? super Object[], ? extends R>
			combinator, Mono<?>... monos) {
		if (monos.length == 0) {
			return empty();
		}
		if (monos.length == 1) {
			return monos[0].map(d -> combinator.apply(new Object[]{d}));
		}
		return onAssembly(new MonoWhen<>(true, combinator, monos));
	}

	/**
	 * Zip the values from given monos together using a combinator function, producing a
	 * new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled.
	 * If any Mono terminates without value, the returned sequence will be terminated
	 * immediately and pending results cancelled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/zip1.png" alt="">
	 * <p>
	 * @param combinator the combinator {@link Function}
	 * @param monos The monos to use.
	 * @param <T> The super incoming type
	 * @param <V> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SafeVarargs
	public static <T, V> Mono<V> zip(Function<? super Object[], ? extends V> combinator, Mono<? extends T>... monos) {
		return MonoSource.wrap(new FluxZip<>(monos, combinator, QueueSupplier.one(), 1));
	}

	/**
	 * Zip the values from given monos together using a combinator function, producing a
	 * new {@literal Mono} that will be fulfilled when all of the given {@literal Monos}
	 * have been fulfilled.
	 * If any Mono terminates without value, the returned sequence will be terminated immediately and pending results cancelled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/zip1.png" alt="">
	 * <p>
	 *
	 * @param combinator the combinator {@link Function}
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 * @param <V> The result type
	 *
	 * @return a {@link Mono}.
	 */
	public static <T, V> Mono<V> zip(final Function<? super Object[], ? extends V> combinator, final Iterable<?
			extends Mono<? extends T>> monos) {
		return MonoSource.wrap(new FluxZip<>(monos, combinator, QueueSupplier.<T>one(), 1));
	}

//	 ==============================================================================================================
//	 Operators
//	 ==============================================================================================================

	/**
	 * Transform this {@link Mono} into a target type.
	 *
	 * {@code mono.as(Flux::from).subscribe() }
	 *
	 * @param transformer the {@link Function} to immediately map this {@link Mono}
	 * into a target type
	 * @param <P> the returned instance type
	 *
	 * @return the {@link Mono} transformed to an instance of P
	 * @see #compose for a bounded conversion to {@link Publisher}
	 */
	public final <P> P as(Function<? super Mono<T>, P> transformer) {
		return transformer.apply(this);
	}

	/**
	 * Combine the result from this mono and another into a {@link Tuple2}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/and.png" alt="">
	 * <p>
	 * @param other the {@link Mono} to combine with
	 * @param <T2> the element type of the other Mono instance
	 * 
	 * @return a new combined Mono
	 * @see #when
	 */
	public final <T2> Mono<Tuple2<T, T2>> and(Mono<? extends T2> other) {
		return and(other, Flux.tuple2Function());
	}

	/**
	 * Combine the result from this mono and another into an arbitrary {@code O} object,
	 * as defined by the provided {@code combinator} function.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/and.png" alt="">
	 * <p>
	 * @param other the {@link Mono} to combine with
	 * @param combinator a {@link BiFunction} combinator function when both sources
	 * complete
	 * @param <T2> the element type of the other Mono instance
	 * @param <O> the element type of the combination
	 *
	 * @return a new combined Mono
	 * @see #when
	 */
	public final <T2, O> Mono<O> and(Mono<? extends T2> other, BiFunction<?
			super T, ? super T2, ? extends O> combinator) {
		if (this instanceof MonoWhen) {
			@SuppressWarnings("unchecked")
			MonoWhen<T, O> o = (MonoWhen<T, O>) this;
			Mono<O> result = o.whenAdditionalSource(other, combinator);
			if (result != null) {
				return result;
			}
		}

		return when(this, other, combinator);
	}

	/**
	 * Wait for the result from this mono, use it to create a second mono via the
	 * provided {@code rightGenerator} function and combine both results into a {@link Tuple2}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/and.png" alt="">
	 * <p>
	 * @param rightGenerator the {@link Function} to generate a {@code Mono} to combine with
	 * @param <T2> the element type of the other Mono instance
	 *
	 * @return a new combined Mono
	 */
	public final <T2> Mono<Tuple2<T, T2>> and(Function<T, Mono<? extends T2>> rightGenerator) {
		return and(rightGenerator, Tuples::of);
	}

	/**
	 * Wait for the result from this mono, use it to create a second mono via the
	 * provided {@code rightGenerator} function and combine both results into an arbitrary
	 * {@code O} object, as defined by the provided {@code combinator} function.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/and.png" alt="">
	 * <p>
	 * @param rightGenerator the {@link Function} to generate a {@code Mono} to combine with
	 * @param combinator a {@link BiFunction} combinator function when both sources complete
	 * @param <T2> the element type of the other Mono instance
	 * @param <O> the element type of the combination
	 *
	 * @return a new combined Mono
	 */
	public final <T2, O> Mono<O> and(Function<T, Mono<? extends T2>> rightGenerator,
			BiFunction<T, T2, O> combinator) {
		Objects.requireNonNull(rightGenerator, "rightGenerator function is mandatory to get the right-hand side Mono");
		Objects.requireNonNull(combinator, "combinator function is mandatory to combine results from both Monos");
		return flatMap(t -> rightGenerator.apply(t).map(t2 -> combinator.apply(t, t2)));
	}

	/**
	 * Intercepts the onSubscribe call and makes sure calls to Subscription methods
	 * only happen after the child Subscriber has returned from its onSubscribe method.
	 *
	 * <p>This helps with child Subscribers that don't expect a recursive call from
	 * onSubscribe into their onNext because, for example, they request immediately from
	 * their onSubscribe but don't finish their preparation before that and onNext
	 * runs into a half-prepared state. This can happen with non Reactor based
	 * Subscribers.
	 *
	 * @return non reentrant onSubscribe {@link Mono}
	 */
	public final Mono<T> awaitOnSubscribe() {
		return onAssembly(new MonoAwaitOnSubscribe<>(this));
	}

	/**
	 * Subscribe to this {@link Mono} and <strong>block indefinitely</strong> until a next signal is
	 * received. Returns that value, or null if the Mono completes empty. In case the Mono
	 * errors, the original exception is thrown (wrapped in a {@link RuntimeException} if
	 * it was a checked exception).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/block.png" alt="">
	 * <p>
	 * Note that each block() will trigger a new subscription: in other words, the result
	 * might miss signal from hot publishers.
	 *
	 * @return T the result
	 */
	public T block() {
		BlockingMonoSubscriber<T> subscriber = new BlockingMonoSubscriber<>();
		subscribe(subscriber);
		return subscriber.blockingGet();
	}

	/**
	 * Subscribe to this {@link Mono} and <strong>block</strong> until a next signal is
	 * received or a timeout expires. Returns that value, or null if the Mono completes
	 * empty. In case the Mono errors, the original exception is thrown (wrapped in a
	 * {@link RuntimeException} if it was a checked exception).
	 * If the provided timeout expires,a {@link RuntimeException} is thrown.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/block.png" alt="">
	 * <p>
	 * Note that each block() will trigger a new subscription: in other words, the result
	 * might miss signal from hot publishers.
	 *
	 * @param timeout maximum time period to wait for before raising a {@link RuntimeException}
	 *
	 * @return T the result
	 */
	public T block(Duration timeout) {
		BlockingMonoSubscriber<T> subscriber = new BlockingMonoSubscriber<>();
		subscribe(subscriber);
		return subscriber.blockingGet(timeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	/**
	 * Cast the current {@link Mono} produced type into a target produced type.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/cast1.png" alt="">
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
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/cache1.png"
	 * alt="">
	 *
	 * @return a replaying {@link Mono}
	 */
	public final Mono<T> cache() {
		return onAssembly(new MonoProcessor<>(this));
	}

	/**
	 * Prepare this {@link Mono} so that subscribers will cancel from it on a
	 * specified
	 * {@link Scheduler}.
	 *
	 * @param scheduler the {@link Scheduler} to signal cancel  on
	 *
	 * @return a scheduled cancel {@link Mono}
	 */
	public final Mono<T> cancelOn(Scheduler scheduler) {
		return onAssembly(new MonoCancelOn<>(this, scheduler));
	}

	/**
	 * Activate assembly tracing for this particular {@link Mono}, in case of an error
	 * upstream of the checkpoint.
	 * <p>
	 * It should be placed towards the end of the reactive chain, as errors
	 * triggered downstream of it cannot be observed and augmented with assembly trace.
	 *
	 * @return the assembly tracing {@link Mono}
	 */
	public final Mono<T> checkpoint() {
		return new MonoOnAssembly<>(this, null);
	}

	/**
	 * Activate assembly tracing for this particular {@link Mono} and give it
	 * a description that will be reflected in the assembly traceback, in case of an error
	 * upstream of the checkpoint.
	 * <p>
	 * It should be placed towards the end of the reactive chain, as errors
	 * triggered downstream of it cannot be observed and augmented with assembly trace.
	 * <p>
	 * The description could for example be a meaningful name for the assembled
	 * mono or a wider correlation ID.
	 *
	 * @param description a description to include in the assembly traceback.
	 * @return the assembly tracing {@link Mono}
	 */
	public final Mono<T> checkpoint(String description) {
		return new MonoOnAssembly<>(this, description);
	}

	/**
	 * Defer the given transformation to this {@link Mono} in order to generate a
	 * target {@link Mono} type. A transformation will occur for each
	 * {@link Subscriber}. For instance:
	 *
	 * {@code flux.compose(Mono::from).subscribe() }
	 *
	 * @param transformer the {@link Function} to lazily map this {@link Mono} into a target {@link Mono}
	 * instance upon subscription.
	 * @param <V> the item type in the returned {@link Publisher}
	 *
	 * @return a new {@link Mono}
	 * @see #as as() for a loose conversion to an arbitrary type
	 * @see #transform(Function)
	 */
	public final <V> Mono<V> compose(Function<? super Mono<T>, ? extends Publisher<V>> transformer) {
		return defer(() -> from(transformer.apply(this)));
	}

	/**
	 * Concatenate emissions of this {@link Mono} with the provided {@link Publisher}
	 * (no interleave).
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/concat1.png" alt="">
	 *
	 * @param other the {@link Publisher} sequence to concat after this {@link Flux}
	 *
	 * @return a concatenated {@link Flux}
	 */
	public final Flux<T> concatWith(Publisher<? extends T> other) {
		return Flux.concat(this, other);
	}

	/**
	 * Provide a default single value if this mono is completed without any data
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/defaultifempty.png" alt="">
	 * <p>
	 * @param defaultV the alternate value if this sequence is empty
	 *
	 * @return a new {@link Mono}
	 *
	 * @see Flux#defaultIfEmpty(Object)
	 */
	public final Mono<T> defaultIfEmpty(T defaultV) {
	    if (this instanceof Fuseable.ScalarCallable) {
            T v = block();
	        if (v == null) {
	            return Mono.just(defaultV);
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/delayonnext.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/delayonnext.png" alt="">
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
		return onAssembly(new MonoDelayElement<>(this, delay.toMillis(), TimeUnit.MILLISECONDS, timer));
	}

	/**
	 * Delay the {@link Mono#subscribe(Subscriber) subscription} to this {@link Mono} source until the given
	 * period elapses.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/delaysubscription1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/delaysubscription1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/delaysubscriptionp1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/dematerialize1.png" alt="">
	 * @param <X> the dematerialized type
	 *
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/doafterterminate1.png" alt="">
	 * <p>
	 * @param afterTerminate the callback to call after {@link Subscriber#onNext}, {@link Subscriber#onComplete} without preceding {@link Subscriber#onNext} or {@link Subscriber#onError}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doAfterTerminate(BiConsumer<? super T, Throwable> afterTerminate) {
		return onAssembly(new MonoPeekTerminal<>(this, null, null, afterTerminate));
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
	 *
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/dooncancel.png" alt="">
	 * <p>
	 * @param onCancel the callback to call on {@link Subscription#cancel()}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnCancel(Runnable onCancel) {
		Objects.requireNonNull(onCancel, "onCancel");
		return doOnSignal(this, null, null, null, null, null, null, onCancel);
	}


	/**
	 * Add behavior triggered when the {@link Mono} emits a data successfully.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/doonnext.png" alt="">
	 * <p>
	 * @param onNext the callback to call on {@link Subscriber#onNext}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnNext(Consumer<? super T> onNext) {
		Objects.requireNonNull(onNext, "onNext");
		return doOnSignal(this, null, onNext, null, null, null, null, null);
	}

	/**
	 * Add behavior triggered when the {@link Mono} completes successfully.
	 *
	 * <ul>
	 *     <li>null : completed without data</li>
	 *     <li>T: completed with data</li>
	 * </ul>
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/doonsuccess.png" alt="">
	 * <p>
	 * @param onSuccess the callback to call on, argument is null if the {@link Mono}
	 * completes without data
	 * {@link Subscriber#onNext} or {@link Subscriber#onComplete} without preceding {@link Subscriber#onNext}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnSuccess(Consumer<? super T> onSuccess) {
		Objects.requireNonNull(onSuccess, "onSuccess");
		return onAssembly(new MonoPeekTerminal<>(this, onSuccess, null, null));
	}

	/**
	 * Add behavior triggered when the {@link Mono} emits an item, fails with an error
	 * or completes successfully. All these events are represented as a {@link Signal}
	 * that is passed to the side-effect callback. Note that this is an advanced operator,
	 * typically used for monitoring of a Mono.
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
		return doOnSignal(this,
				null,
				v -> signalConsumer.accept(Signal.next(v)),
				e -> signalConsumer.accept(Signal.<T>error(e)),
				() -> signalConsumer.accept(Signal.<T>complete()),
				null, null, null);
	}

	/**
	 * Add behavior triggered when the {@link Mono} completes with an error.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/doonerror1.png" alt="">
	 * <p>
	 * @param onError the error callback to call on {@link Subscriber#onError(Throwable)}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnError(Consumer<? super Throwable> onError) {
		Objects.requireNonNull(onError, "onError");
		return doOnSignal(this, null, null, onError, null, null, null, null);
	}


	/**
	 * Add behavior triggered when the {@link Mono} completes with an error matching the given exception type.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/doonerrorw.png" alt="">
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
		@SuppressWarnings("unchecked")
		Consumer<Throwable> handler = (Consumer<Throwable>)onError;
		return doOnError(exceptionType::isInstance, handler);
	}

	/**
	 * Add behavior triggered when the {@link Mono} completes with an error matching the given predicate.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/doonerrorw.png" alt="">
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
		return doOnError(t -> {
			if (predicate.test(t)) {
				onError.accept(t);
			}
		});
	}
	/**
	 * Add behavior triggering a {@link LongConsumer} when the {@link Mono} receives any request.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/doonrequest1.png" alt="">
	 *
	 * @param consumer the consumer to invoke on each request
	 *
	 * @return an observed  {@link Mono}
	 */
	public final Mono<T> doOnRequest(final LongConsumer consumer) {
		Objects.requireNonNull(consumer, "consumer");
		return doOnSignal(this, null, null, null, null, null, consumer, null);
	}

	/**
	 * Add behavior triggered when the {@link Mono} is subscribed.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/doonsubscribe.png" alt="">
	 * <p>
	 * @param onSubscribe the callback to call on {@link Subscriber#onSubscribe(Subscription)}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		Objects.requireNonNull(onSubscribe, "onSubscribe");
		return doOnSignal(this, onSubscribe, null, null,  null, null, null, null);
	}

	/**
	 * Add behavior triggered when the {@link Mono} terminates, either by completing successfully or with an error.
	 *
	 * <ul>
	 *     <li>null, null : completing without data</li>
	 *     <li>T, null : completing with data</li>
	 *     <li>null, Throwable : failing with/without data</li>
	 * </ul>
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/doonterminate1.png" alt="">
	 * <p>
	 * @param onTerminate the callback to call {@link Subscriber#onNext}, {@link Subscriber#onComplete} without preceding {@link Subscriber#onNext} or {@link Subscriber#onError}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnTerminate(BiConsumer<? super T, Throwable> onTerminate) {
		Objects.requireNonNull(onTerminate, "onTerminate");
		return onAssembly(new MonoPeekTerminal<>(this, null, onTerminate, null));
	}

	/**
	 * Map this {@link Mono} into {@link reactor.util.function.Tuple2 Tuple2&lt;Long, T&gt;}
	 * of timemillis and source data. The timemillis corresponds to the elapsed time between
	 * the subscribe and the first next signal, as measured by the {@link Schedulers#parallel() parallel} scheduler.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/elapsed1.png" alt="">
	 *
	 * @return a new {@link Mono} that emits a tuple of time elapsed in milliseconds and matching data
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/elapsed1.png" alt="">
	 *
	 * @param scheduler a {@link Scheduler} instance to read time from
	 * @return a new {@link Mono} that emits a tuple of time elapsed in milliseconds and matching data
	 */
	public final Mono<Tuple2<Long, T>> elapsed(Scheduler scheduler) {
		Objects.requireNonNull(scheduler, "scheduler");
		return onAssembly(new MonoElapsed<>(this, scheduler));
	}

	/**
	 * If this {@link Mono} is valued, test the result and replay it if predicate returns true.
	 * Otherwise complete without value.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/filter1.png" alt="">
	 * <p>
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
	 * @param asyncPredicate the function generating a {@link Publisher} of {@link Boolean}
	 * to filter the Mono with
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/then.png" alt="">
	 * <p>
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/flatmap1.png" alt="">
	 * <p>
	 * @param mapper the
	 * {@link Function} to produce a sequence of R from the the eventual passed {@link Subscriber#onNext}
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/flatmaps1.png" alt="">
	 * <p>
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

		return Flux.onAssembly(new FluxFlatMap<>(
				new FluxMapSignal<>(this, mapperOnNext, mapperOnError, mapperOnComplete),
				Flux.identityFunction(),
				false,
				Integer.MAX_VALUE,
				QueueSupplier.xs(), QueueSupplier.XS_BUFFER_SIZE,
				QueueSupplier.xs()
		));
	}

	/**
	 * Transform the item emitted by this {@link Mono} into {@link Iterable}, then forward
	 * its elements into the returned {@link Flux}. The prefetch argument allows to
	 * give an arbitrary prefetch size to the inner {@link Iterable}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/flatmap.png" alt="">
	 *
	 * @param mapper the {@link Function} to transform input item into a sequence {@link Iterable}
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}
	 *
	 */
	public final <R> Flux<R> flatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper) {
		return Flux.onAssembly(new FluxFlattenIterable<>(this, mapper, Integer
				.MAX_VALUE, QueueSupplier.one()));
	}

	/**
	 * Convert this {@link Mono} to a {@link Flux}
	 *
	 * @return a {@link Flux} variant of this {@link Mono}
	 */
    public final Flux<T> flux() {
	    if (this instanceof Callable) {
	        if (this instanceof Fuseable.ScalarCallable) {
	            T v = block();
	            if (v == null) {
	                return Flux.empty();
	            }
	            return Flux.just(v);
	        }
		    @SuppressWarnings("unchecked") Callable<T> thiz = (Callable<T>) this;
		    return Flux.onAssembly(new FluxCallable<>(thiz));
	    }
		return FluxSource.wrap(this);
	}

	/**
	 * Emit a single boolean true if this {@link Mono} has an element.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/haselement.png" alt="">
	 *
	 * @return a new {@link Mono} with <code>true</code> if a value is emitted and <code>false</code>
	 * otherwise
	 */
	public final Mono<Boolean> hasElement() {
		return onAssembly(new MonoHasElements<>(this));
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/ignoreelement.png" alt="">
	 * <p>
	 *
	 * @return a new empty {@link Mono} representing the completion of this {@link Mono}.
	 */
	public final Mono<T> ignoreElement() {
		return ignoreElements(this);
	}

	/**
	 * Observe all Reactive Streams signals and trace them using {@link Logger} support.
	 * Default will use {@link Level#INFO} and {@code java.util.logging}.
	 * If SLF4J is available, it will be used instead.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/log1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/log1.png" alt="">
	 * <p>
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator
	 * suffix will complete, e.g. "reactor.Flux.Map".
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> log(String category) {
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
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/log1.png" alt="">
	 * <p>
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
	public final Mono<T> log(String category, Level level, SignalType... options) {
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
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/log.png" alt="">
	 * <p>
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
	public final Mono<T> log(String category,
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
	 * Transform the item emitted by this {@link Mono} by applying a synchronous function to it.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/map1.png" alt="">
	 * <p>
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
	 * Transform incoming onNext, onError and onComplete signals into {@link Signal} instances,
	 * materializing these signals.
	 * Since the error is materialized as a {@code Signal}, the propagation will be stopped and onComplete will be
	 * emitted. Complete signal will first emit a {@code Signal.complete()} and then effectively complete the flux.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/materialize1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/merge1.png" alt="">
	 * <p>
	 * @param other the {@link Publisher} to merge with
	 *
	 * @return a new {@link Flux} as the sequence is not guaranteed to be at most 1
	 */
	public final Flux<T> mergeWith(Publisher<? extends T> other) {
		return Flux.merge(this, other);
	}

	/**
	 * Transform any error emitted by this {@link Mono} by synchronously applying a function to it.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/maperror.png" alt="">
	 * <p>
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/maperror.png" alt="">
	 * <p>
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
	 * Transform an error emitted by this {@link Mono} by synchronously applying a function
	 * to it if the error matches the given predicate. Otherwise let the error pass through.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/maperror.png"
	 * alt="">
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
	 * Emit the first available result from this mono or the other mono.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/or.png" alt="">
	 * <p>
	 * @param other the racing other {@link Mono} to compete with for the result
	 *
	 * @return a new {@link Mono}
	 * @see #first
	 */
	public final Mono<T> or(Mono<? extends T> other) {
		if (this instanceof MonoFirst) {
			MonoFirst<T> a = (MonoFirst<T>) this;
			Mono<T> result =  a.orAdditionalSource(other);
			if (result != null) {
				return result;
			}
		}
		return first(this, other);
	}

	/**
	 * Evaluate the emitted value against the given {@link Class} type. If the
	 * value matches the type, it is passed into the new {@link Mono}. Otherwise the
	 * value is ignored.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/filter.png" alt="">
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
		return MonoSource.wrap(new FluxPublishMulticast<>(this, f -> transform.apply(from(f)),
				Integer
				.MAX_VALUE,
				QueueSupplier
				.one()));
	}

	/**
	 * Run onNext, onComplete and onError on a supplied {@link Scheduler}
	 * {@link Worker Worker}.
	 * <p>
	 * This operator influences the threading context where the rest of the operators in
	 * the chain below it will execute, up to a new occurrence of {@code publishOn}.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/publishon1.png" alt="">
	 * <p>
	 * Typically used for fast publisher, slow consumer(s) scenarios.
	 *
	 * {@code mono.publishOn(Schedulers.single()).subscribe() }
	 *
	 * @param scheduler a {@link Scheduler} providing the {@link Worker} where to publish
	 *
	 * @return an asynchronously producing {@link Mono}
	 */
	public final Mono<T> publishOn(Scheduler scheduler) {
		if(this instanceof Callable) {
			if (this instanceof Fuseable.ScalarCallable) {
				T value = block();
				return onAssembly(new MonoSubscribeOnValue<>(value, scheduler));
			}
			@SuppressWarnings("unchecked")
			Callable<T> c = (Callable<T>)this;
			return onAssembly(new MonoSubscribeOnCallable<>(c,
					scheduler));
		}
		return onAssembly(new MonoPublishOn<>(this, scheduler));
	}

	/**
	 * Repeatedly and indefinitely subscribe to the source upon completion of the
	 * previous subscription.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/repeat.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/repeatb.png" alt="">
	 *
	 * @param predicate the boolean to evaluate on onComplete.
	 *
	 * @return a {@link Flux} that repeats on onComplete while the predicate matches
	 *
	 */
	public final Flux<T> repeat(BooleanSupplier predicate) {
		return Flux.onAssembly(new FluxRepeatPredicate<>(this, predicate));
	}

	/**
	 * Repeatedly subscribe to the source {@literal numRepeat} times.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/repeatn.png" alt="">
	 *
	 * @param numRepeat the number of times to re-subscribe on onComplete
	 *
	 * @return a {@link Flux} that repeats on onComplete, up to the specified number of repetitions
	 */
	public final Flux<T> repeat(long numRepeat) {
		return Flux.onAssembly(new FluxRepeat<>(this, numRepeat));
	}

	/**
	 * Repeatedly subscribe to the source if the predicate returns true after completion of the previous
	 * subscription. A specified maximum of repeat will limit the number of re-subscribe.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/repeatnb.png" alt="">
	 *
	 * @param numRepeat the number of times to re-subscribe on complete
	 * @param predicate the boolean to evaluate on onComplete
	 *
	 * @return a {@link Flux} that repeats on onComplete while the predicate matches,
	 * up to the specified number of repetitions
	 */
	public final Flux<T> repeat(long numRepeat, BooleanSupplier predicate) {
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/repeatwhen.png" alt="">
	 *
	 * @param repeatFactory the {@link Function} that returns the associated {@link Publisher}
	 * companion, given a {@link Flux} that signals each onComplete as a 0-based incrementing {@link Long}.
	 *
	 * @return a {@link Flux} that repeats on onComplete when the companion {@link Publisher} produces an
	 * onNext signal
	 */
	public final Flux<T> repeatWhen(Function<Flux<Long>, ? extends Publisher<?>> repeatFactory) {
		return Flux.onAssembly(new FluxRepeatWhen<>(this, repeatFactory));
	}

	/**
	 * Repeatedly subscribe to this {@link Mono} as long as the current subscription to this
	 * {@link Mono} completes empty and the companion {@link Publisher} produces an onNext signal.
	 * <p>
	 * Any terminal signal will terminate the resulting {@link Mono} with the same signal immediately.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/repeatwhenempty.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/repeatwhen1.png" alt="">
	 *
	 * @param maxRepeat the maximum number of repeats (infinite if {@code Integer.MAX_VALUE})
	 * @param repeatFactory the {@link Function} that returns the associated {@link Publisher}
	 * companion, given a {@link Flux} that signals each onComplete as a 0-based incrementing {@link Long}.
	 *
	 * @return a {@link Mono} that resubscribes to this {@link Mono} if the previous subscription was empty,
	 * as long as the companion {@link Publisher} produces an onNext signal and the maximum number of repeats isn't exceeded.
	 */
	public final Mono<T> repeatWhenEmpty(int maxRepeat, Function<Flux<Long>, ? extends Publisher<?>> repeatFactory) {
		return Mono.defer(() -> {
			Flux<Long> iterations;

			if(maxRepeat == Integer.MAX_VALUE) {
				iterations = Flux.fromStream(LongStream.range(0, Long.MAX_VALUE)
				                                       .mapToObj(Long::new));
			} else {
				iterations = Flux
					.range(0, maxRepeat)
					.map(Integer::longValue)
					.concatWith(Flux.error(new IllegalStateException("Exceeded maximum number of repeats"), true));
			}

			return this.repeatWhen(o -> repeatFactory.apply(o
						.zipWith(iterations, 1, (c, i) -> i)))
			           .next();
		});
	}


	/**
	 * Re-subscribes to this {@link Mono} sequence if it signals any error, indefinitely.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/retry1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/retryn1.png" alt="">
	 *
	 * @param numRetries the number of times to tolerate an error
	 *
	 * @return a {@link Mono} that retries on onError up to the specified number of retry attempts.
	 */
	public final Mono<T> retry(long numRetries) {
		return onAssembly(new MonoRetry<>(this, numRetries));
	}

	/**
	 * Re-subscribes to this {@link Mono} sequence if it signals any error
	 * that matches the given {@link Predicate}, otherwise push the error downstream.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/retryb1.png" alt="">
	 *
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 *
	 * @return a {@link Mono} that retries on onError if the predicates matches.
	 */
	public final Mono<T> retry(Predicate<? super Throwable> retryMatcher) {
		return onAssembly(new MonoRetryPredicate<>(this, retryMatcher));
	}

	/**
	 * Re-subscribes to this {@link Mono} sequence up to the specified number of retries if it signals any
	 * error that match the given {@link Predicate}, otherwise push the error downstream.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/retrynb1.png" alt="">
	 *
	 * @param numRetries the number of times to tolerate an error
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 *
	 * @return a {@link Mono} that retries on onError up to the specified number of retry
	 * attempts, only if the predicate matches.
	 *
	 */
	public final Mono<T> retry(long numRetries, Predicate<? super Throwable> retryMatcher) {
		return defer(() -> retry(Flux.countingPredicate(retryMatcher, numRetries)));
	}

	/**
	 * Retries this {@link Mono} when a companion sequence signals
	 * an item in response to this {@link Mono} error signal
	 * <p>If the companion sequence signals when the {@link Mono} is active, the retry
	 * attempt is suppressed and any terminal signal will terminate the {@link Mono} source with the same signal
	 * immediately.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/retrywhen1.png" alt="">
	 *
	 * @param whenFactory the {@link Function} that returns the associated {@link Publisher}
	 * companion, given a {@link Flux} that signals each onError as a 0-based incrementing {@link Long}.
	 *
	 * @return a {@link Mono} that retries on onError when the companion {@link Publisher} produces an
	 * onNext signal
	 */
	public final Mono<T> retryWhen(Function<Flux<Throwable>, ? extends Publisher<?>> whenFactory) {
		return onAssembly(new MonoRetryWhen<>(this, whenFactory));
	}

	/**
	 * Subscribe to this {@link Mono} and request unbounded demand, then represent the
	 * subscription as a {@link MonoProcessor} (allowing to block, cancel as well as
	 * many other operations).
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/unbounded1.png" alt="">
	 * <p>
	 *
	 * @return a {@link MonoProcessor} to use to either retrieve value or cancel the underlying {@link Subscription}
	 */
	public final MonoProcessor<T> subscribe() {
		MonoProcessor<T> s;
		if(this instanceof MonoProcessor){
			s = (MonoProcessor<T>)this;
		}
		else{
			s = new MonoProcessor<>(this);
		}
		s.connect();
		return s;
	}
//TODO
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/subscribe1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/subscribeerror1.png" alt="">
	 *
	 * @param consumer the consumer to invoke on each next signal
	 * @param errorConsumer the consumer to invoke on error signal
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 */
	public final Disposable subscribe(Consumer<? super T> consumer, Consumer<? super Throwable> errorConsumer) {
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/subscribecomplete1.png" alt="">
	 *
	 * @param consumer the consumer to invoke on each value
	 * @param errorConsumer the consumer to invoke on error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 */
	public final Disposable subscribe(Consumer<? super T> consumer,
			Consumer<? super Throwable> errorConsumer,
			Runnable completeConsumer) {
		return subscribe(consumer, errorConsumer, completeConsumer, null);
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/subscribecomplete1.png" alt="">
	 *
	 * @param consumer the consumer to invoke on each value
	 * @param errorConsumer the consumer to invoke on error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 * @param subscriptionConsumer the consumer to invoke on subscribe signal, to be used
	 * for the initial {@link Subscription#request(long) request}, or null for max request
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 */
	public final Disposable subscribe(Consumer<? super T> consumer,
			Consumer<? super Throwable> errorConsumer,
			Runnable completeConsumer,
			Consumer<? super Subscription> subscriptionConsumer) {
		return subscribeWith(new LambdaMonoSubscriber<>(consumer, errorConsumer,
				completeConsumer, subscriptionConsumer));
	}

	/**
	 * Run subscribe, onSubscribe and request on a specified {@link Scheduler}'s {@link Worker}.
	 * As such, placing this operator anywhere in the chain will also impact the execution
	 * context of onNext/onError/onComplete signals from the beginning of the chain up to
	 * the next occurrence of a {@link #publishOn(Scheduler) publishOn}.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/subscribeon1.png" alt="">
	 * <p>
	 * {@code mono.subscribeOn(Schedulers.parallel()).subscribe()) }
	 *
	 * @param scheduler a {@link Scheduler} providing the {@link Worker} where to subscribe
	 *
	 * @return a {@link Flux} requesting asynchronously
	 * @see #publishOn(Scheduler)
	 */
	public final Mono<T> subscribeOn(Scheduler scheduler) {
		if(this instanceof Callable) {
			if (this instanceof Fuseable.ScalarCallable) {
				T value = block();
				return onAssembly(new MonoSubscribeOnValue<>(value, scheduler));
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
	 * Subscribe to a fallback publisher when any error occurs, using a function to
	 * choose the fallback depending on the error.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/otherwise.png" alt="">
	 * <p>
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/otherwise.png"
	 * alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/otherwise.png"
	 * alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/otherwisereturn.png" alt="">
	 * <p>
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/otherwisereturn.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/otherwisereturn.png" alt="">
	 * @param predicate the error predicate to match
	 * @param fallbackValue the value to emit if an error occurs that matches the predicate
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> onErrorReturn(Predicate<? super Throwable> predicate, T fallbackValue) {
		return onErrorResume(predicate,  throwable -> just(fallbackValue));
	}

	/**
	 * Fallback to an alternative {@link Mono} if this mono is completed without data
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/otherwiseempty.png" alt="">
	 * <p>
	 * @param alternate the alternate mono if this mono is empty
	 *
	 * @return a {@link Mono} falling back upon source completing without elements
	 * @see Flux#switchIfEmpty
	 */
	public final Mono<T> switchIfEmpty(Mono<? extends T> alternate) {
		return onAssembly(new MonoSwitchIfEmpty<>(this, alternate));
	}

	/**
	 * Return a {@code Mono<Void>} which only replays complete and error signals
	 * from this {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/ignorethen.png" alt="">
	 * <p>
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/ignorethen1.png" alt="">
	 *
	 * @param other a {@link Mono} to emit from after termination
	 * @param <V> the element type of the supplied Mono
	 *
	 * @return a new {@link Mono} that emits from the supplied {@link Mono}
	 */
	public final <V> Mono<V> then(Mono<V> other) {
		if (this instanceof MonoThenIgnore) {
            MonoThenIgnore<T> a = (MonoThenIgnore<T>) this;
            return a.shift(other);
		}
		return onAssembly(new MonoThenIgnore<>(new Mono[] { this }, other));
	}

	/**
	 * Return a {@code Mono<Void>} that waits for this {@link Mono} to complete then
	 * for a supplied {@link Publisher Publisher&lt;Void&gt;} to also complete. The
	 * second completion signal is replayed, or any error signal that occurs instead.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/ignorethen.png"
	 * alt="">
	 *
	 * @param other a {@link Publisher} to wait for after this Mono's termination
	 * @return a new {@link Mono} completing when both publishers have completed in
	 * sequence
	 */
	public final Mono<Void> thenEmpty(Publisher<Void> other) {
		MonoIgnoreEmpty<T> ignored = new MonoIgnoreEmpty<>(this);
		Mono<Void> then = ignored.then(MonoSource.wrap(other));
		return Mono.onAssembly(then);
	}

	/**
	 * Let this {@link Mono} complete then play another {@link Publisher}.
	 * <p>
	 * In other words ignore element from this mono and transform the completion signal into a
	 * {@code Flux<V>} that will emit elements from the provided {@link Publisher}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/ignorethens.png" alt="">
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
	 * Propagate a {@link TimeoutException} in case no item arrives within the given
	 * {@link Duration}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/timeouttime1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/timeouttimefallback1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/timeouttime1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/timeouttimefallback1.png" alt="">
	 *
	 * @param timeout the timeout before the onNext signal from this {@link Mono}
	 * @param fallback the fallback {@link Mono} to subscribe when a timeout occurs
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return an expirable {@link Mono} with a fallback {@link Mono}
	 */
	public final Mono<T> timeout(Duration timeout, Mono<? extends T> fallback,
			Scheduler timer) {
		final Mono<Long> _timer = Mono.delay(timeout, timer).onErrorReturn(0L);

		if(fallback == null) {
			return onAssembly(new MonoTimeout<>(this, _timer));
		}
		return onAssembly(new MonoTimeout<>(this, _timer, fallback));
	}

	/**
	 * Signal a {@link TimeoutException} in case the item from this {@link Mono} has
	 * not been emitted before the given {@link Publisher} emits.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/timeoutp1.png" alt="">
	 *
	 * @param firstTimeout the timeout {@link Publisher} that must not emit before the first signal from this {@link Mono}
	 * @param <U> the element type of the timeout Publisher
	 *
	 * @return an expirable {@link Mono} if the item does not come before a {@link Publisher} signals
	 *
	 */
	public final <U> Mono<T> timeout(Publisher<U> firstTimeout) {
		return onAssembly(new MonoTimeout<>(this, firstTimeout));
	}

	/**
	 * Switch to a fallback {@link Publisher} in case the  item from this {@link Mono} has
	 * not been emitted before the given {@link Publisher} emits.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/timeoutfallbackp1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/timestamp1.png" alt="">
	 *
	 * @return a timestamped {@link Mono}
	 */
	public final Mono<Tuple2<Long, T>> timestamp() {
		return timestamp(Schedulers.parallel());
	}

	/**
	 * If this {@link Mono} is valued, emit a {@link reactor.util.function.Tuple2} pair of
	 * T1 the current clock time in millis (as a {@link Long} measured by the
	 * provided {@link Scheduler}) and T2 the emitted data (as a {@code T}).
	 *
	 * Emit a {@link reactor.util.function.Tuple2} pair of T1 {@link Long} current system time in
	 * millis and T2 {@code T} associated data for the eventual item from this {@link Mono}
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/timestamp1.png" alt="">
	 *
	 * @param scheduler a {@link Scheduler} instance to read time from
	 * @return a timestamped {@link Mono}
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/tofuture.png" alt="">
	 * <p>
	 *
	 * @return a {@link CompletableFuture}
	 */
	public final CompletableFuture<T> toFuture() {
		return subscribeWith(new MonoToCompletableFuture<>());
	}

	/**
	 * Transform this {@link Mono} in order to generate a target {@link Mono}. Unlike {@link #compose(Function)}, the
	 * provided function is executed as part of assembly.
	 *
	 * {@code Function<Mono, Mono> applySchedulers = mono -> mono.subscribeOn(Schedulers.io()).publishOn(Schedulers.parallel());
	 *        mono.transform(applySchedulers).map(v -> v * v).subscribe()}
	 *
	 * @param transformer the {@link Function} to immediately map this {@link Mono} into a target {@link Mono}
	 * instance.
	 * @param <V> the item type in the returned {@link Mono}
	 *
	 * @return a new {@link Mono}
	 * @see #compose(Function) compose(Function) for deferred composition of {@link Mono} for each {@link Subscriber}
	 * @see #as(Function) as(Function) for a loose conversion to an arbitrary type
	 */
	public final <V> Mono<V> transform(Function<? super Mono<T>, ? extends Publisher<V>> transformer) {
		return onAssembly(from(transformer.apply(this)));
	}

	/**
	 * Subscribe to this Mono and another Publisher, which will be used as a trigger for
	 * the emission of this Mono's element. That is to say, this Mono's element is delayed
	 * until the trigger Publisher emits for the first time (or terminates empty).
	 *
	 * @param anyPublisher the publisher which first emission or termination will trigger
	 * the emission of this Mono's value.
	 * @return this Mono, but delayed until the given publisher emits first or terminates.
	 */
	public Mono<T> untilOther(Publisher<?> anyPublisher) {
		Objects.requireNonNull(anyPublisher, "anyPublisher required");
		if (this instanceof MonoUntilOther) {
			return ((MonoUntilOther<T>) this).addTrigger(anyPublisher);
		}
		return onAssembly(new MonoUntilOther<>(false, this, anyPublisher));
	}

	/**
	 * Subscribe to this Mono and another Publisher, which will be used as a trigger for
	 * the emission of this Mono's element, mapped through a provided function.
	 * That is to say, this Mono's element is delayed until the trigger Publisher emits
	 * for the first time (or terminates empty). Any error is delayed until all publishers
	 * have triggered, and multiple errors are combined into one.
	 *
	 * @param anyPublisher the publisher which first emission or termination will trigger
	 * the emission of this Mono's value.
	 * @return this Mono, but delayed until the given publisher emits first or terminates.
	 */
	public Mono<T> untilOtherDelayError(Publisher<?> anyPublisher) {
		Objects.requireNonNull(anyPublisher, "anyPublisher required");
		if (this instanceof MonoUntilOther) {
			return ((MonoUntilOther<T>) this).addTrigger(anyPublisher);
		}
		return onAssembly(new MonoUntilOther<>(true, this, anyPublisher));
	}

	/**
	 * Invoke {@link Hooks} pointcut given a {@link Mono} and returning an eventually
	 * new {@link Mono}
	 *
	 * @param <T> the value type
	 * @param source the source to wrap
	 *
	 * @return the potentially wrapped source
	 */
	@SuppressWarnings("unchecked")
	protected static <T> Mono<T> onAssembly(Mono<T> source) {
		Hooks.OnOperatorHook hook = Hooks.onOperatorHook;
		if(hook == null) {
			return source;
		}
		return (Mono<T>)hook.apply(source);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}

	@SuppressWarnings("unchecked")
	static <T> Mono<T> doOnSignal(Mono<T> source,
			Consumer<? super Subscription> onSubscribe,
			Consumer<? super T> onNext,
			Consumer<? super Throwable> onError,
			Runnable onComplete,
			Runnable onAfterTerminate,
			LongConsumer onRequest,
			Runnable onCancel) {
		if (source instanceof Fuseable) {
			return onAssembly(new MonoPeekFuseable<>(source,
					onSubscribe,
					onNext,
					onError,
					onComplete,
					onAfterTerminate,
					onRequest,
					onCancel));
		}
		return onAssembly(new MonoPeek<>(source,
				onSubscribe,
				onNext,
				onError,
				onComplete,
				onAfterTerminate,
				onRequest,
				onCancel));
	}
	
	static final Function<? super Object[], Void> VOID_FUNCTION = t -> null;

	@SuppressWarnings("unchecked")
	static <T> BiPredicate<? super T, ? super T> equalsBiPredicate(){
		return EQUALS_BIPREDICATE;
	}
	static final BiPredicate EQUALS_BIPREDICATE = Object::equals;
}
