/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.io.File;
import java.lang.reflect.ParameterizedType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.Callable;
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
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshot;
import reactor.core.publisher.FluxOnAssembly.CheckpointHeavySnapshot;
import reactor.core.publisher.FluxOnAssembly.CheckpointLightSnapshot;
import reactor.core.publisher.FluxSink.OverflowStrategy;
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
import reactor.core.observability.SignalListener;
import reactor.core.observability.SignalListenerFactory;
import reactor.util.retry.Retry;

/**
 * A Reactive Streams {@link Publisher} with rx operators that emits 0 to N elements, and then completes
 * (successfully or with an error).
 * <p>
 * The recommended way to learn about the {@link Flux} API and discover new operators is
 * through the reference documentation, rather than through this javadoc (as opposed to
 * learning more about individual operators). See the <a href="https://projectreactor.io/docs/core/release/reference/docs/index.html#which-operator">
 * "which operator do I need?" appendix</a>.
 *
 * <p>
 * <img class="marble" src="doc-files/marbles/flux.svg" alt="">
 *
 * <p>It is intended to be used in implementations and return types. Input parameters should keep using raw
 * {@link Publisher} as much as possible.
 *
 * <p>If it is known that the underlying {@link Publisher} will emit 0 or 1 element, {@link Mono} should be used
 * instead.
 *
 * <p>Note that using state in the {@code java.util.function} / lambdas used within Flux operators
 * should be avoided, as these may be shared between several {@link Subscriber Subscribers}.
 *
 * <p> {@link #subscribe(CoreSubscriber)} is an internal extension to
 * {@link #subscribe(Subscriber)} used internally for {@link Context} passing. User
 * provided {@link Subscriber} may
 * be passed to this "subscribe" extension but will loose the available
 * per-subscribe {@link Hooks#onLastOperator}.
 *
 * @param <T> the element type of this Reactive Streams {@link Publisher}
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @author David Karnok
 * @author Simon Baslé
 * @author Injae Kim
 *
 * @see Mono
 */
public abstract class Flux<T> implements CorePublisher<T> {

//	 ==============================================================================================================
//	 Static Generators
//	 ==============================================================================================================

	/**
	 * Build a {@link Flux} whose data are generated by the combination of <strong>the
	 * most recently published</strong> value from each of the {@link Publisher} sources.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/combineLatest.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator is NOT suited for types that need guaranteed discard of unpropagated elements, as
	 * it doesn't track which elements have been used by the combinator and which haven't. Furthermore, elements can and
	 * will be passed to the combinator multiple times.
	 *
	 * @param sources The {@link Publisher} sources to combine values from
	 * @param combinator The aggregate function that will receive the latest value from each upstream and return the value
	 * to signal downstream
	 * @param <T> type of the value from sources
	 * @param <V> The produced output after transformation by the given combinator
	 *
	 * @return a {@link Flux} based on the produced combinations
	 */
	@SafeVarargs
	public static <T, V> Flux<V> combineLatest(Function<Object[], V> combinator, Publisher<? extends T>... sources) {
		return combineLatest(combinator, Queues.XS_BUFFER_SIZE, sources);
	}

	/**
	 * Build a {@link Flux} whose data are generated by the combination of <strong>the
	 * most recently published</strong> value from each of the {@link Publisher} sources.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/combineLatest.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator is NOT suited for types that need guaranteed discard of unpropagated elements, as
	 * it doesn't track which elements have been used by the combinator and which haven't. Furthermore, elements can and
	 * will be passed to the combinator multiple times.
	 *
	 * @param sources The {@link Publisher} sources to combine values from
	 * @param prefetch The demand sent to each combined source {@link Publisher}
	 * @param combinator The aggregate function that will receive the latest value from each upstream and return the value
	 * to signal downstream
	 * @param <T> type of the value from sources
	 * @param <V> The produced output after transformation by the given combinator
	 *
	 * @return a {@link Flux} based on the produced combinations
	 */
	@SafeVarargs
	public static <T, V> Flux<V> combineLatest(Function<Object[], V> combinator, int prefetch,
			Publisher<? extends T>... sources) {
		if (sources.length == 0) {
			return empty();
		}

		if (sources.length == 1) {
            Publisher<? extends T> source = sources[0];
            if (source instanceof Fuseable) {
	            return onAssembly(new FluxMapFuseable<>(from(source),
			            v -> combinator.apply(new Object[]{v})));
            }
			return onAssembly(new FluxMap<>(from(source),
					v -> combinator.apply(new Object[]{v})));
		}

		return onAssembly(new FluxCombineLatest<>(sources,
				combinator, Queues.get(prefetch), prefetch));
	}

	/**
	 * Build a {@link Flux} whose data are generated by the combination of <strong>the
	 * most recently published</strong> value from each of two {@link Publisher} sources.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/combineLatest.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator is NOT suited for types that need guaranteed discard of unpropagated elements, as
	 * it doesn't track which elements have been used by the combinator and which haven't. Furthermore, elements can and
	 * will be passed to the combinator multiple times.
	 *
	 * @param source1 The first {@link Publisher} source to combine values from
	 * @param source2 The second {@link Publisher} source to combine values from
	 * @param combinator The aggregate function that will receive the latest value from each upstream and return the value
	 * to signal downstream
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <V> The produced output after transformation by the given combinator
	 *
	 * @return a {@link Flux} based on the produced combinations
	 */
    @SuppressWarnings("unchecked")
    public static <T1, T2, V> Flux<V> combineLatest(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			BiFunction<? super T1, ? super T2, ? extends V> combinator) {
	    return combineLatest(tuple -> combinator.apply((T1)tuple[0], (T2)tuple[1]), source1, source2);
	}

	/**
	 * Build a {@link Flux} whose data are generated by the combination of <strong>the
	 * most recently published</strong> value from each of three {@link Publisher} sources.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/combineLatest.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator is NOT suited for types that need guaranteed discard of unpropagated elements, as
	 * it doesn't track which elements have been used by the combinator and which haven't. Furthermore, elements can and
	 * will be passed to the combinator multiple times.
	 *
	 * @param source1 The first {@link Publisher} source to combine values from
	 * @param source2 The second {@link Publisher} source to combine values from
	 * @param source3 The third {@link Publisher} source to combine values from
	 * @param combinator The aggregate function that will receive the latest value from each upstream and return the value
	 * to signal downstream
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <V> The produced output after transformation by the given combinator
	 *
	 * @return a {@link Flux} based on the produced combinations
	 */
	public static <T1, T2, T3, V> Flux<V> combineLatest(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Function<Object[], V> combinator) {
		return combineLatest(combinator, source1, source2, source3);
	}

	/**
	 * Build a {@link Flux} whose data are generated by the combination of <strong>the
	 * most recently published</strong> value from each of four {@link Publisher} sources.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/combineLatest.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator is NOT suited for types that need guaranteed discard of unpropagated elements, as
	 * it doesn't track which elements have been used by the combinator and which haven't. Furthermore, elements can and
	 * will be passed to the combinator multiple times.
	 *
	 * @param source1 The first {@link Publisher} source to combine values from
	 * @param source2 The second {@link Publisher} source to combine values from
	 * @param source3 The third {@link Publisher} source to combine values from
	 * @param source4 The fourth {@link Publisher} source to combine values from
	 * @param combinator The aggregate function that will receive the latest value from each upstream and return the value
	 * to signal downstream
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <V> The produced output after transformation by the given combinator
	 *
	 * @return a {@link Flux} based on the produced combinations
	 */
	public static <T1, T2, T3, T4, V> Flux<V> combineLatest(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Function<Object[], V> combinator) {
		return combineLatest(combinator, source1, source2, source3, source4);
	}

	/**
	 * Build a {@link Flux} whose data are generated by the combination of <strong>the
	 * most recently published</strong> value from each of five {@link Publisher} sources.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/combineLatest.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator is NOT suited for types that need guaranteed discard of unpropagated elements, as
	 * it doesn't track which elements have been used by the combinator and which haven't. Furthermore, elements can and
	 * will be passed to the combinator multiple times.
	 *
	 * @param source1 The first {@link Publisher} source to combine values from
	 * @param source2 The second {@link Publisher} source to combine values from
	 * @param source3 The third {@link Publisher} source to combine values from
	 * @param source4 The fourth {@link Publisher} source to combine values from
	 * @param source5 The fifth {@link Publisher} source to combine values from
	 * @param combinator The aggregate function that will receive the latest value from each upstream and return the value
	 * to signal downstream
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 * @param <V> The produced output after transformation by the given combinator
	 *
	 * @return a {@link Flux} based on the produced combinations
	 */
	public static <T1, T2, T3, T4, T5, V> Flux<V> combineLatest(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Function<Object[], V> combinator) {
		return combineLatest(combinator, source1, source2, source3, source4, source5);
	}

	/**
	 * Build a {@link Flux} whose data are generated by the combination of <strong>the
	 * most recently published</strong> value from each of six {@link Publisher} sources.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/combineLatest.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator is NOT suited for types that need guaranteed discard of unpropagated elements, as
	 * it doesn't track which elements have been used by the combinator and which haven't. Furthermore, elements can and
	 * will be passed to the combinator multiple times.
	 *
	 * @param source1 The first {@link Publisher} source to combine values from
	 * @param source2 The second {@link Publisher} source to combine values from
	 * @param source3 The third {@link Publisher} source to combine values from
	 * @param source4 The fourth {@link Publisher} source to combine values from
	 * @param source5 The fifth {@link Publisher} source to combine values from
	 * @param source6 The sixth {@link Publisher} source to combine values from
	 * @param combinator The aggregate function that will receive the latest value from each upstream and return the value
	 * to signal downstream
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 * @param <T6> type of the value from source6
	 * @param <V> The produced output after transformation by the given combinator
	 *
	 * @return a {@link Flux} based on the produced combinations
	 */
	public static <T1, T2, T3, T4, T5, T6, V> Flux<V> combineLatest(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Publisher<? extends T6> source6,
			Function<Object[], V> combinator) {
		return combineLatest(combinator, source1, source2, source3, source4, source5, source6);
	}

	/**
	 * Build a {@link Flux} whose data are generated by the combination of <strong>the
	 * most recently published</strong> value from each
	 * of the {@link Publisher} sources provided in an {@link Iterable}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/combineLatest.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator is NOT suited for types that need guaranteed discard of unpropagated elements, as
	 * it doesn't track which elements have been used by the combinator and which haven't. Furthermore, elements can and
	 * will be passed to the combinator multiple times.
	 *
	 * @param sources The list of {@link Publisher} sources to combine values from
	 * @param combinator The aggregate function that will receive the latest value from each upstream and return the value
	 * to signal downstream
	 * @param <T> The common base type of the values from sources
	 * @param <V> The produced output after transformation by the given combinator
	 *
	 * @return a {@link Flux} based on the produced combinations
	 */
	public static <T, V> Flux<V> combineLatest(Iterable<? extends Publisher<? extends T>> sources,
			Function<Object[], V> combinator) {
		return combineLatest(sources, Queues.XS_BUFFER_SIZE, combinator);
	}

	/**
	 * Build a {@link Flux} whose data are generated by the combination of <strong>the
	 * most recently published</strong> value from each
	 * of the {@link Publisher} sources provided in an {@link Iterable}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/combineLatest.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator is NOT suited for types that need guaranteed discard of unpropagated elements, as
	 * it doesn't track which elements have been used by the combinator and which haven't. Furthermore, elements can and
	 * will be passed to the combinator multiple times.
	 *
	 * @param sources The list of {@link Publisher} sources to combine values from
	 * @param prefetch demand produced to each combined source {@link Publisher}
	 * @param combinator The aggregate function that will receive the latest value from each upstream and return the value
	 * to signal downstream
	 * @param <T> The common base type of the values from sources
	 * @param <V> The produced output after transformation by the given combinator
	 *
	 * @return a {@link Flux} based on the produced combinations
	 */
	public static <T, V> Flux<V> combineLatest(Iterable<? extends Publisher<? extends T>> sources,
			int prefetch,
			Function<Object[], V> combinator) {

		return onAssembly(new FluxCombineLatest<>(sources,
				combinator,
				Queues.get(prefetch), prefetch));
	}

	/**
	 * Concatenate all sources provided in an {@link Iterable}, forwarding elements
	 * emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Any error interrupts the sequence immediately and is
	 * forwarded downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatVarSources.svg" alt="">
	 *
	 * @param sources The {@link Iterable} of {@link Publisher} to concatenate
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all source sequences
	 */
	public static <T> Flux<T> concat(Iterable<? extends Publisher<? extends T>> sources) {
		return onAssembly(new FluxConcatIterable<>(sources));
	}

	/**
	 * Concatenates the values to the end of the {@link Flux}
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatWithValues.svg" alt="">
	 *
	 * @param values The values to concatenate
	 *
	 * @return a new {@link Flux} concatenating all source sequences
	 */
	@SafeVarargs
	public final Flux<T> concatWithValues(T... values) {
	    return concatWith(Flux.fromArray(values));
	}

	/**
	 * Concatenate all sources emitted as an onNext signal from a parent {@link Publisher},
	 * forwarding elements emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Any error interrupts the sequence immediately and is
	 * forwarded downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatAsyncSources.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concatenate
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all inner sources sequences
	 */
	public static <T> Flux<T> concat(Publisher<? extends Publisher<? extends T>> sources) {
		return concat(sources, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Concatenate all sources emitted as an onNext signal from a parent {@link Publisher},
	 * forwarding elements emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Any error interrupts the sequence immediately and is
	 * forwarded downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatAsyncSources.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concatenate
	 * @param prefetch the number of Publishers to prefetch from the outer {@link Publisher}
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all inner sources sequences
	 */
	public static <T> Flux<T> concat(Publisher<? extends Publisher<? extends T>> sources, int prefetch) {
		return from(sources).concatMap(identityFunction(), prefetch);
	}

	/**
	 * Concatenate all sources provided as a vararg, forwarding elements emitted by the
	 * sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Any error interrupts the sequence immediately and is
	 * forwarded downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatVarSources.svg" alt="">
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concat
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all source sequences
	 */
	@SafeVarargs
	public static <T> Flux<T> concat(Publisher<? extends T>... sources) {
		return onAssembly(new FluxConcatArray<>(false, sources));
	}

	/**
	 * Concatenate all sources emitted as an onNext signal from a parent {@link Publisher},
	 * forwarding elements emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Errors do not interrupt the main sequence but are propagated
	 * after the rest of the sources have had a chance to be concatenated.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatAsyncSources.svg" alt="">
	 *
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concatenate
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all inner sources sequences, delaying errors
	 */
	public static <T> Flux<T> concatDelayError(Publisher<? extends Publisher<? extends T>> sources) {
		return concatDelayError(sources, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Concatenate all sources emitted as an onNext signal from a parent {@link Publisher},
	 * forwarding elements emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Errors do not interrupt the main sequence but are propagated
	 * after the rest of the sources have had a chance to be concatenated.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatAsyncSources.svg" alt="">
	 * <p>
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concatenate
	 * @param prefetch number of elements to prefetch from the source, to be turned into inner Publishers
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all inner sources sequences until complete or error
	 */
	public static <T> Flux<T> concatDelayError(Publisher<? extends Publisher<? extends T>> sources, int prefetch) {
		return from(sources).concatMapDelayError(identityFunction(), prefetch);
	}

	/**
	 * Concatenate all sources emitted as an onNext signal from a parent {@link Publisher},
	 * forwarding elements emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes.
	 * <p>
	 * Errors do not interrupt the main sequence but are propagated after the current
	 * concat backlog if {@code delayUntilEnd} is {@literal false} or after all sources
	 * have had a chance to be concatenated if {@code delayUntilEnd} is {@literal true}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatAsyncSources.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concatenate
	 * @param delayUntilEnd delay error until all sources have been consumed instead of
	 * after the current source
	 * @param prefetch the number of Publishers to prefetch from the outer {@link Publisher}
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all inner sources sequences until complete or error
	 */
	public static <T> Flux<T> concatDelayError(Publisher<? extends Publisher<? extends
			T>> sources, boolean delayUntilEnd, int prefetch) {
		return from(sources).concatMapDelayError(identityFunction(), delayUntilEnd, prefetch);
	}

	/**
	 * Concatenate all sources provided as a vararg, forwarding elements emitted by the
	 * sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Errors do not interrupt the main sequence but are propagated
	 * after the rest of the sources have had a chance to be concatenated.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatVarSources.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concat
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all source sequences
	 */
	@SafeVarargs
	public static <T> Flux<T> concatDelayError(Publisher<? extends T>... sources) {
		return onAssembly(new FluxConcatArray<>(true, sources));
	}

	/**
	 * Programmatically create a {@link Flux} with the capability of emitting multiple
	 * elements in a synchronous or asynchronous manner through the {@link FluxSink} API.
	 * This includes emitting elements from multiple threads.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/createForFlux.svg" alt="">
	 * <p>
	 * This Flux factory is useful if one wants to adapt some other multi-valued async API
	 * and not worry about cancellation and backpressure (which is handled by buffering
	 * all signals if the downstream can't keep up).
	 * <p>
	 * For example:
	 *
	 * <pre><code>
	 * Flux.&lt;String&gt;create(emitter -&gt; {
	 *
	 *     ActionListener al = e -&gt; {
	 *         emitter.next(textField.getText());
	 *     };
	 *     // without cleanup support:
	 *
	 *     button.addActionListener(al);
	 *
	 *     // with cleanup support:
	 *
	 *     button.addActionListener(al);
	 *     emitter.onDispose(() -> {
	 *         button.removeListener(al);
	 *     });
	 * });
	 * </code></pre>
	 *
	 * <p><strong>Discard Support:</strong> The {@link FluxSink} exposed by this operator buffers in case of
	 * overflow. The buffer is discarded when the main sequence is cancelled.
	 *
	 * @param <T> The type of values in the sequence
	 * @param emitter Consume the {@link FluxSink} provided per-subscriber by Reactor to generate signals.
	 * @return a {@link Flux}
	 * @see #push(Consumer)
	 */
    public static <T> Flux<T> create(Consumer<? super FluxSink<T>> emitter) {
	    return create(emitter, OverflowStrategy.BUFFER);
    }

	/**
	 * Programmatically create a {@link Flux} with the capability of emitting multiple
	 * elements in a synchronous or asynchronous manner through the {@link FluxSink} API.
	 * This includes emitting elements from multiple threads.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/createWithOverflowStrategy.svg" alt="">
	 * <p>
	 * This Flux factory is useful if one wants to adapt some other multi-valued async API
	 * and not worry about cancellation and backpressure (which is handled by buffering
	 * all signals if the downstream can't keep up).
	 * <p>
	 * For example:
	 *
	 * <pre><code>
	 * Flux.&lt;String&gt;create(emitter -&gt; {
	 *
	 *     ActionListener al = e -&gt; {
	 *         emitter.next(textField.getText());
	 *     };
	 *     // without cleanup support:
	 *
	 *     button.addActionListener(al);
	 *
	 *     // with cleanup support:
	 *
	 *     button.addActionListener(al);
	 *     emitter.onDispose(() -> {
	 *         button.removeListener(al);
	 *     });
	 * }, FluxSink.OverflowStrategy.LATEST);
	 * </code></pre>
	 *
	 * <p><strong>Discard Support:</strong> The {@link FluxSink} exposed by this operator discards elements
	 * as relevant to the chosen {@link OverflowStrategy}. For example, the {@link OverflowStrategy#DROP}
	 * discards each items as they are being dropped, while {@link OverflowStrategy#BUFFER}
	 * will discard the buffer upon cancellation.
	 *
	 * @param <T> The type of values in the sequence
	 * @param backpressure the backpressure mode, see {@link OverflowStrategy} for the
	 * available backpressure modes
	 * @param emitter Consume the {@link FluxSink} provided per-subscriber by Reactor to generate signals.
	 * @return a {@link Flux}
	 * @see #push(Consumer, reactor.core.publisher.FluxSink.OverflowStrategy)
	 */
	public static <T> Flux<T> create(Consumer<? super FluxSink<T>> emitter, OverflowStrategy backpressure) {
		return onAssembly(new FluxCreate<>(emitter, backpressure, FluxCreate.CreateMode.PUSH_PULL));
	}

	/**
	 * Programmatically create a {@link Flux} with the capability of emitting multiple
	 * elements from a single-threaded producer through the {@link FluxSink} API. For
	 * a multi-threaded capable alternative, see {@link #create(Consumer)}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/push.svg" alt="">
	 * <p>
	 * This Flux factory is useful if one wants to adapt some other single-threaded
	 * multi-valued async API and not worry about cancellation and backpressure (which is
	 * handled by buffering all signals if the downstream can't keep up).
	 * <p>
	 * For example:
	 *
	 * <pre><code>
	 * Flux.&lt;String&gt;push(emitter -&gt; {
	 *
	 *	 ActionListener al = e -&gt; {
	 *		 emitter.next(textField.getText());
	 *	 };
	 *	 // without cleanup support:
	 *
	 *	 button.addActionListener(al);
	 *
	 *	 // with cleanup support:
	 *
	 *	 button.addActionListener(al);
	 *	 emitter.onDispose(() -> {
	 *		 button.removeListener(al);
	 *	 });
	 * });
	 * </code></pre>
	 *
	 * <p><strong>Discard Support:</strong> The {@link FluxSink} exposed by this operator buffers in case of
	 * overflow. The buffer is discarded when the main sequence is cancelled.
	 *
	 * @param <T> The type of values in the sequence
	 * @param emitter Consume the {@link FluxSink} provided per-subscriber by Reactor to generate signals.
	 * @return a {@link Flux}
	 * @see #create(Consumer)
	 */
	public static <T> Flux<T> push(Consumer<? super FluxSink<T>> emitter) {
		return push(emitter, OverflowStrategy.BUFFER);
	}

	/**
	 * Programmatically create a {@link Flux} with the capability of emitting multiple
	 * elements from a single-threaded producer through the {@link FluxSink} API. For
	 * a multi-threaded capable alternative, see {@link #create(Consumer, reactor.core.publisher.FluxSink.OverflowStrategy)}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/pushWithOverflowStrategy.svg" alt="">
	 * <p>
	 * This Flux factory is useful if one wants to adapt some other single-threaded
	 * multi-valued async API and not worry about cancellation and backpressure (which is
	 * handled by buffering all signals if the downstream can't keep up).
	 * <p>
	 * For example:
	 *
	 * <pre><code>
	 * Flux.&lt;String&gt;push(emitter -&gt; {
	 *
	 *	 ActionListener al = e -&gt; {
	 *		 emitter.next(textField.getText());
	 *	 };
	 *	 // without cleanup support:
	 *
	 *	 button.addActionListener(al);
	 *
	 *	 // with cleanup support:
	 *
	 *	 button.addActionListener(al);
	 *	 emitter.onDispose(() -> {
	 *		 button.removeListener(al);
	 *	 });
	 * }, FluxSink.OverflowStrategy.LATEST);
	 * </code></pre>
	 *
	 * <p><strong>Discard Support:</strong> The {@link FluxSink} exposed by this operator discards elements
	 * as relevant to the chosen {@link OverflowStrategy}. For example, the {@link OverflowStrategy#DROP}
	 * discards each items as they are being dropped, while {@link OverflowStrategy#BUFFER}
	 * will discard the buffer upon cancellation.
	 *
	 * @param <T> The type of values in the sequence
	 * @param backpressure the backpressure mode, see {@link OverflowStrategy} for the
	 * available backpressure modes
	 * @param emitter Consume the {@link FluxSink} provided per-subscriber by Reactor to generate signals.
	 * @return a {@link Flux}
	 * @see #create(Consumer, reactor.core.publisher.FluxSink.OverflowStrategy)
	 */
	public static <T> Flux<T> push(Consumer<? super FluxSink<T>> emitter, OverflowStrategy backpressure) {
		return onAssembly(new FluxCreate<>(emitter, backpressure, FluxCreate.CreateMode.PUSH_ONLY));
	}

	/**
	 * Lazily supply a {@link Publisher} every time a {@link Subscription} is made on the
	 * resulting {@link Flux}, so the actual source instantiation is deferred until each
	 * subscribe and the {@link Supplier} can create a subscriber-specific instance.
	 * If the supplier doesn't generate a new instance however, this operator will
	 * effectively behave like {@link #from(Publisher)}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/deferForFlux.svg" alt="">
	 *
	 * @param supplier the {@link Publisher} {@link Supplier} to call on subscribe
	 * @param <T>      the type of values passing through the {@link Flux}
	 *
	 * @return a deferred {@link Flux}
	 * @see #deferContextual(Function)
	 */
	public static <T> Flux<T> defer(Supplier<? extends Publisher<T>> supplier) {
		return onAssembly(new FluxDefer<>(supplier));
	}

	/**
	 * Lazily supply a {@link Publisher} every time a {@link Subscription} is made on the
	 * resulting {@link Flux}, so the actual source instantiation is deferred until each
	 * subscribe and the {@link Function} can create a subscriber-specific instance.
	 * This operator behaves the same way as {@link #defer(Supplier)},
	 * but accepts a {@link Function} that will receive the current {@link ContextView} as an argument.
	 * If the function doesn't generate a new instance however, this operator will
	 * effectively behave like {@link #from(Publisher)}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/deferForFlux.svg" alt="">
	 *
	 * @param contextualPublisherFactory the {@link Publisher} {@link Function} to call on subscribe
	 * @param <T>      the type of values passing through the {@link Flux}
	 * @return a deferred {@link Flux} deriving actual {@link Flux} from context values for each subscription
	 */
	public static <T> Flux<T> deferContextual(Function<ContextView, ? extends Publisher<T>> contextualPublisherFactory) {
		return onAssembly(new FluxDeferContextual<>(contextualPublisherFactory));
	}

	/**
	 * Create a {@link Flux} that completes without emitting any item.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/empty.svg" alt="">
	 *
	 * @param <T> the reified type of the target {@link Subscriber}
	 *
	 * @return an empty {@link Flux}
	 */
	public static <T> Flux<T> empty() {
		return FluxEmpty.instance();
	}

	/**
	 * Create a {@link Flux} that terminates with the specified error immediately after
	 * being subscribed to.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/error.svg" alt="">
	 *
	 * @param error the error to signal to each {@link Subscriber}
	 * @param <T> the reified type of the target {@link Subscriber}
	 *
	 * @return a new failing {@link Flux}
	 */
	public static <T> Flux<T> error(Throwable error) {
		return error(error, false);
	}

	/**
	 * Create a {@link Flux} that terminates with an error immediately after being
	 * subscribed to. The {@link Throwable} is generated by a {@link Supplier}, invoked
	 * each time there is a subscription and allowing for lazy instantiation.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/errorWithSupplier.svg" alt="">
	 *
	 * @param errorSupplier the error signal {@link Supplier} to invoke for each {@link Subscriber}
	 * @param <T> the reified type of the target {@link Subscriber}
	 *
	 * @return a new failing {@link Flux}
	 */
	public static <T> Flux<T> error(Supplier<? extends Throwable> errorSupplier) {
		return onAssembly(new FluxErrorSupplied<>(errorSupplier));
	}

	/**
	 * Create a {@link Flux} that terminates with the specified error, either immediately
	 * after being subscribed to or after being first requested.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/errorWhenRequested.svg" alt="">
	 *
	 * @param throwable the error to signal to each {@link Subscriber}
	 * @param whenRequested if true, will onError on the first request instead of subscribe().
	 * @param <O> the reified type of the target {@link Subscriber}
	 *
	 * @return a new failing {@link Flux}
	 */
	public static <O> Flux<O> error(Throwable throwable, boolean whenRequested) {
		if (whenRequested) {
			return onAssembly(new FluxErrorOnRequest<>(throwable));
		}
		else {
			return onAssembly(new FluxError<>(throwable));
		}
	}

	/**
	 * Pick the first {@link Publisher} to emit any signal (onNext/onError/onComplete) and
	 * replay all signals from that {@link Publisher}, effectively behaving like the
	 * fastest of these competing sources.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/firstWithSignalForFlux.svg" alt="">
	 *
	 * @param sources The competing source publishers
	 * @param <I> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} behaving like the fastest of its sources
	 * @deprecated use {@link #firstWithSignal(Publisher[])}. To be removed in reactor 3.5.
	 */
	@SafeVarargs
	@Deprecated
	public static <I> Flux<I> first(Publisher<? extends I>... sources) {
		return firstWithSignal(sources);
	}

	/**
	 * Pick the first {@link Publisher} to emit any signal (onNext/onError/onComplete) and
	 * replay all signals from that {@link Publisher}, effectively behaving like the
	 * fastest of these competing sources.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/firstWithSignalForFlux.svg" alt="">
	 *
	 * @param sources The competing source publishers
	 * @param <I> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} behaving like the fastest of its sources
	 * @deprecated use {@link #firstWithSignal(Iterable)}. To be removed in reactor 3.5.
	 */
	@Deprecated
	public static <I> Flux<I> first(Iterable<? extends Publisher<? extends I>> sources) {
		return firstWithSignal(sources);
	}

	/**
	 * Pick the first {@link Publisher} to emit any signal (onNext/onError/onComplete) and
	 * replay all signals from that {@link Publisher}, effectively behaving like the
	 * fastest of these competing sources.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/firstWithSignalForFlux.svg" alt="">
	 *
	 * @param sources The competing source publishers
	 * @param <I> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} behaving like the fastest of its sources
	 */
	@SafeVarargs
	public static <I> Flux<I> firstWithSignal(Publisher<? extends I>... sources) {
		return onAssembly(new FluxFirstWithSignal<>(sources));
	}

	/**
	 * Pick the first {@link Publisher} to emit any signal (onNext/onError/onComplete) and
	 * replay all signals from that {@link Publisher}, effectively behaving like the
	 * fastest of these competing sources.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/firstWithSignalForFlux.svg" alt="">
	 *
	 * @param sources The competing source publishers
	 * @param <I> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} behaving like the fastest of its sources
	 */
	public static <I> Flux<I> firstWithSignal(Iterable<? extends Publisher<? extends I>> sources) {
		return onAssembly(new FluxFirstWithSignal<>(sources));
	}

	/**
	 * Pick the first {@link Publisher} to emit any value and replay all values
	 * from that {@link Publisher}, effectively behaving like the source that first
	 * emits an {@link Subscriber#onNext(Object) onNext}.
	 *
	 * <p>
	 * Sources with values always "win" over empty sources (ones that only emit onComplete)
	 * or failing sources (ones that only emit onError).
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
	 * <img class="marble" src="doc-files/marbles/firstWithValueForFlux.svg" alt="">
	 *
	 * @param sources An {@link Iterable} of the competing source publishers
	 * @param <I> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} behaving like the fastest of its sources
	 */
	public static <I> Flux<I> firstWithValue(Iterable<? extends Publisher<? extends I>> sources) {
		return onAssembly(new FluxFirstWithValue<>(sources));
	}

	/**
	 * Pick the first {@link Publisher} to emit any value and replay all values
	 * from that {@link Publisher}, effectively behaving like the source that first
	 * emits an {@link Subscriber#onNext(Object) onNext}.
	 * <p>
	 * Sources with values always "win" over an empty source (ones that only emit onComplete)
	 * or failing sources (ones that only emit onError).
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
	 * Note that like in {@link #firstWithSignal(Publisher[])}, an infinite source can be problematic
	 * if no other source emits onNext.
	 * In case the {@code first} source is already an array-based {@link #firstWithValue(Publisher, Publisher[])}
	 * instance, nesting is avoided: a single new array-based instance is created with all the
	 * sources from {@code first} plus all the {@code others} sources at the same level.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/firstWithValueForFlux.svg" alt="">
	 *
	 * @param first The first competing source publisher
	 * @param others The other competing source publishers
	 * @param <I> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} behaving like the fastest of its sources
	 */
	@SafeVarargs
	public static <I> Flux<I> firstWithValue(Publisher<? extends I> first, Publisher<? extends I>... others) {
		if (first instanceof FluxFirstWithValue) {
			@SuppressWarnings("unchecked")
			FluxFirstWithValue<I> orPublisher = (FluxFirstWithValue<I>) first;

			FluxFirstWithValue<I> result = orPublisher.firstValuedAdditionalSources(others);
			if (result != null) {
				return result;
			}
		}
		return onAssembly(new FluxFirstWithValue<>(first, others));
	}

	/**
	 * Decorate the specified {@link Publisher} with the {@link Flux} API.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromForFlux.svg" alt="">
	 * <p>
	 * {@link Hooks#onEachOperator(String, Function)} and similar assembly hooks are applied
	 * unless the source is already a {@link Flux}.
	 *
	 * @param source the source to decorate
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> from(Publisher<? extends T> source) {
		//duplicated in wrap, but necessary to detect early and thus avoid applying assembly
		if (source instanceof Flux && !ContextPropagationSupport.shouldWrapPublisher(source)) {
			@SuppressWarnings("unchecked")
			Flux<T> casted = (Flux<T>) source;
			return casted;
		}

		//all other cases (including ScalarCallable) are managed without assembly in wrap
		//let onAssembly point to Flux.from:
		return onAssembly(wrap(source));
	}

	/**
	 * Create a {@link Flux} that emits the items contained in the provided array.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromArray.svg" alt="">
	 *
	 * @param array the array to read data from
	 * @param <T> The type of values in the source array and resulting Flux
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> fromArray(T[] array) {
		if (array.length == 0) {
			return empty();
		}
		if (array.length == 1) {
			return just(array[0]);
		}
		return onAssembly(new FluxArray<>(array));
	}

	/**
	 * Create a {@link Flux} that emits the items contained in the provided {@link Iterable}.
	 * The {@link Iterable#iterator()} method will be invoked at least once and at most twice
	 * for each subscriber.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromIterable.svg" alt="">
	 * <p>
	 * This operator inspects the {@link Iterable}'s {@link Spliterator} to assess if the iteration
	 * can be guaranteed to be finite (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 * Since the default Spliterator wraps the Iterator we can have two {@link Iterable#iterator()}
	 * calls. This second invocation is skipped on a {@link Collection} however, a type which is
	 * assumed to be always finite.
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator attempts to discard the remainder of the
	 * {@link Iterable} if it can safely ensure the iterator is finite.
	 * Note that this means the {@link Iterable#iterator()} method could be invoked twice.
	 *
	 * @param it the {@link Iterable} to read data from
	 * @param <T> The type of values in the source {@link Iterable} and resulting Flux
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> fromIterable(Iterable<? extends T> it) {
		return onAssembly(new FluxIterable<>(it));
	}

	/**
	 * Create a {@link Flux} that emits the items contained in the provided {@link Stream}.
	 * Keep in mind that a {@link Stream} cannot be re-used, which can be problematic in
	 * case of multiple subscriptions or re-subscription (like with {@link #repeat()} or
	 * {@link #retry()}). The {@link Stream} is {@link Stream#close() closed} automatically
	 * by the operator on cancellation, error or completion.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromStream.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator attempts to discard remainder of the
	 * {@link Stream} through its open {@link Spliterator}, if it can safely ensure it is finite
	 * (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 *
	 * @param s the {@link Stream} to read data from
	 * @param <T> The type of values in the source {@link Stream} and resulting Flux
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> fromStream(Stream<? extends T> s) {
		Objects.requireNonNull(s, "Stream s must be provided");
		return onAssembly(new FluxStream<>(() -> s));
	}

	/**
	 * Create a {@link Flux} that emits the items contained in a {@link Stream} created by
	 * the provided {@link Supplier} for each subscription. The {@link Stream} is
	 * {@link Stream#close() closed} automatically by the operator on cancellation, error
	 * or completion.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/fromStream.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator attempts to discard remainder of the
	 * {@link Stream} through its open {@link Spliterator}, if it can safely ensure it is finite
	 * (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 *
	 * @param streamSupplier the {@link Supplier} that generates the {@link Stream} from
	 * which to read data
	 * @param <T> The type of values in the source {@link Stream} and resulting Flux
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> fromStream(Supplier<Stream<? extends T>> streamSupplier) {
		return onAssembly(new FluxStream<>(streamSupplier));
	}

	/**
	 * Programmatically create a {@link Flux} by generating signals one-by-one via a
	 * consumer callback.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/generateStateless.svg" alt="">
	 *
	 * @param <T> the value type emitted
	 * @param generator Consume the {@link SynchronousSink} provided per-subscriber by Reactor
	 * to generate a <strong>single</strong> signal on each pass.
	 *
	 * @return a {@link Flux}
	 */
	public static <T> Flux<T> generate(Consumer<SynchronousSink<T>> generator) {
		Objects.requireNonNull(generator, "generator");
		return onAssembly(new FluxGenerate<>(generator));
	}

	/**
	 * Programmatically create a {@link Flux} by generating signals one-by-one via a
	 * consumer callback and some state. The {@code stateSupplier} may return {@literal null}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/generate.svg" alt="">
	 *
	 * @param <T> the value type emitted
	 * @param <S> the per-subscriber custom state type
	 * @param stateSupplier called for each incoming Subscriber to provide the initial state for the generator bifunction
	 * @param generator Consume the {@link SynchronousSink} provided per-subscriber by Reactor
	 * as well as the current state to generate a <strong>single</strong> signal on each pass
	 * and return a (new) state.
	 * @return a {@link Flux}
	 */
	public static <T, S> Flux<T> generate(Callable<S> stateSupplier, BiFunction<S, SynchronousSink<T>, S> generator) {
		return onAssembly(new FluxGenerate<>(stateSupplier, generator));
	}

	/**
	 * Programmatically create a {@link Flux} by generating signals one-by-one via a
	 * consumer callback and some state, with a final cleanup callback. The
	 * {@code stateSupplier} may return {@literal null} but your cleanup {@code stateConsumer}
	 * will need to handle the null case.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/generateWithCleanup.svg" alt="">
	 *
	 * @param <T> the value type emitted
	 * @param <S> the per-subscriber custom state type
	 * @param stateSupplier called for each incoming Subscriber to provide the initial state for the generator bifunction
	 * @param generator Consume the {@link SynchronousSink} provided per-subscriber by Reactor
	 * as well as the current state to generate a <strong>single</strong> signal on each pass
	 * and return a (new) state.
	 * @param stateConsumer called after the generator has terminated or the downstream cancelled, receiving the last
	 * state to be handled (i.e., release resources or do other cleanup).
	 *
	 * @return a {@link Flux}
	 */
	public static <T, S> Flux<T> generate(Callable<S> stateSupplier, BiFunction<S, SynchronousSink<T>, S> generator, Consumer<? super S> stateConsumer) {
		return onAssembly(new FluxGenerate<>(stateSupplier, generator, stateConsumer));
	}

	/**
	 * Create a {@link Flux} that emits long values starting with 0 and incrementing at
	 * specified time intervals on the global timer. The first element is emitted after
	 * an initial delay equal to the {@code period}. If demand is not produced in time,
	 * an onError will be signalled with an {@link Exceptions#isOverflow(Throwable) overflow}
	 * {@code IllegalStateException} detailing the tick that couldn't be emitted.
	 * In normal conditions, the {@link Flux} will never complete.
	 * <p>
	 * Runs on the {@link Schedulers#parallel()} Scheduler.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/interval.svg" alt="">
	 *
	 * @param period the period {@link Duration} between each increment
	 * @return a new {@link Flux} emitting increasing numbers at regular intervals
	 */
	public static Flux<Long> interval(Duration period) {
		return interval(period, Schedulers.parallel());
	}

	/**
	 * Create a {@link Flux} that emits long values starting with 0 and incrementing at
	 * specified time intervals, after an initial delay, on the global timer. If demand is
	 * not produced in time, an onError will be signalled with an
	 * {@link Exceptions#isOverflow(Throwable) overflow} {@code IllegalStateException}
	 * detailing the tick that couldn't be emitted. In normal conditions, the {@link Flux}
	 * will never complete.
	 * <p>
	 * Runs on the {@link Schedulers#parallel()} Scheduler.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/intervalWithDelay.svg" alt="">
	 *
	 * @param delay  the {@link Duration} to wait before emitting 0l
	 * @param period the period {@link Duration} before each following increment
	 *
	 * @return a new {@link Flux} emitting increasing numbers at regular intervals
	 */
	public static Flux<Long> interval(Duration delay, Duration period) {
		return interval(delay, period, Schedulers.parallel());
	}

	/**
	 * Create a {@link Flux} that emits long values starting with 0 and incrementing at
	 * specified time intervals, on the specified {@link Scheduler}. The first element is
	 * emitted after an initial delay equal to the {@code period}. If demand is not
	 * produced in time, an onError will be signalled with an {@link Exceptions#isOverflow(Throwable) overflow}
	 * {@code IllegalStateException} detailing the tick that couldn't be emitted.
	 * In normal conditions, the {@link Flux} will never complete.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/interval.svg" alt="">
	 *
	 * @param period the period {@link Duration} between each increment
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a new {@link Flux} emitting increasing numbers at regular intervals
	 */
	public static Flux<Long> interval(Duration period, Scheduler timer) {
		return interval(period, period, timer);
	}

	/**
	 * Create a {@link Flux} that emits long values starting with 0 and incrementing at
	 * specified time intervals, after an initial delay, on the specified {@link Scheduler}.
	 * If demand is not produced in time, an onError will be signalled with an
	 * {@link Exceptions#isOverflow(Throwable) overflow} {@code IllegalStateException}
	 * detailing the tick that couldn't be emitted. In normal conditions, the {@link Flux}
	 * will never complete.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/intervalWithDelay.svg" alt="">
	 *
	 * @param delay  the {@link Duration} to wait before emitting 0l
	 * @param period the period {@link Duration} before each following increment
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a new {@link Flux} emitting increasing numbers at regular intervals
	 */
	public static Flux<Long> interval(Duration delay, Duration period, Scheduler timer) {
		return onAssembly(new FluxInterval(delay.toNanos(), period.toNanos(), TimeUnit.NANOSECONDS, timer));
	}

	/**
	 * Create a {@link Flux} that emits the provided elements and then completes.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/justMultiple.svg" alt="">
	 *
	 * @param data the elements to emit, as a vararg
	 * @param <T> the emitted data type
	 *
	 * @return a new {@link Flux}
	 */
	@SafeVarargs
	public static <T> Flux<T> just(T... data) {
		return fromArray(data);
	}

	/**
	 * Create a new {@link Flux} that will only emit a single element then onComplete.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/just.svg" alt="">
	 *
	 * @param data the single element to emit
	 * @param <T> the emitted data type
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> just(T data) {
		return onAssembly(new FluxJust<>(data));
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an interleaved merged sequence. Unlike {@link #concat(Publisher) concat}, inner
	 * sources are subscribed to eagerly.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeAsyncSources.svg" alt="">
	 *
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param source a {@link Publisher} of {@link Publisher} sources to merge
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}
	 */
	public static <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source) {
		return merge(source,
				Queues.SMALL_BUFFER_SIZE,
				Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an interleaved merged sequence. Unlike {@link #concat(Publisher) concat}, inner
	 * sources are subscribed to eagerly (but at most {@code concurrency} sources are
	 * subscribed to at the same time).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeAsyncSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param source a {@link Publisher} of {@link Publisher} sources to merge
	 * @param concurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}
	 */
	public static <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source, int concurrency) {
		return merge(source, concurrency, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an interleaved merged sequence. Unlike {@link #concat(Publisher) concat}, inner
	 * sources are subscribed to eagerly (but at most {@code concurrency} sources are
	 * subscribed to at the same time).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeAsyncSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param source a {@link Publisher} of {@link Publisher} sources to merge
	 * @param concurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param prefetch the inner source request size
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}
	 */
	public static <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source, int concurrency, int prefetch) {
		return onAssembly(new FluxFlatMap<>(
				from(source),
				identityFunction(),
				false,
				concurrency,
				Queues.get(concurrency),
				prefetch,
				Queues.get(prefetch)));
	}

	/**
	 * Merge data from {@link Publisher} sequences contained in an {@link Iterable}
	 * into an interleaved merged sequence. Unlike {@link #concat(Publisher) concat}, inner
	 * sources are subscribed to eagerly.
	 * A new {@link Iterator} will be created for each subscriber.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeFixedSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param sources the {@link Iterable} of sources to merge (will be lazily iterated on subscribe)
	 * @param <I> The source type of the data sequence
	 *
	 * @return a merged {@link Flux}
	 */
	public static <I> Flux<I> merge(Iterable<? extends Publisher<? extends I>> sources) {
		return merge(fromIterable(sources));
	}

	/**
	 * Merge data from {@link Publisher} sequences contained in an array / vararg
	 * into an interleaved merged sequence. Unlike {@link #concat(Publisher) concat},
	 * sources are subscribed to eagerly.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeFixedSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param sources the array of {@link Publisher} sources to merge
	 * @param <I> The source type of the data sequence
	 *
	 * @return a merged {@link Flux}
	 */
	@SafeVarargs
	public static <I> Flux<I> merge(Publisher<? extends I>... sources) {
		return merge(Queues.XS_BUFFER_SIZE, sources);
	}

	/**
	 * Merge data from {@link Publisher} sequences contained in an array / vararg
	 * into an interleaved merged sequence. Unlike {@link #concat(Publisher) concat},
	 * sources are subscribed to eagerly.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeFixedSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param sources the array of {@link Publisher} sources to merge
	 * @param prefetch the inner source request size
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive {@link Flux} publisher ready to be subscribed
	 */
	@SafeVarargs
	public static <I> Flux<I> merge(int prefetch, Publisher<? extends I>... sources) {
		return merge(prefetch, false, sources);
	}

	/**
	 * Merge data from {@link Publisher} sequences contained in an array / vararg
	 * into an interleaved merged sequence. Unlike {@link #concat(Publisher) concat},
	 * sources are subscribed to eagerly.
	 * This variant will delay any error until after the rest of the merge backlog has been processed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeFixedSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param sources the array of {@link Publisher} sources to merge
	 * @param prefetch the inner source request size
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive {@link Flux} publisher ready to be subscribed
	 */
	@SafeVarargs
	public static <I> Flux<I> mergeDelayError(int prefetch, Publisher<? extends I>... sources) {
		return merge(prefetch, true, sources);
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by their natural order) <strong>as they arrive</strong>.
	 * This is not a {@link #sort()}, as it doesn't consider the whole of each sequences. Unlike mergeComparing,
	 * this operator does <em>not</em> wait for a value from each source to arrive either.
	 * <p>
	 * While this operator does retrieve at most one value from each source, it only compares values when two or more
	 * sources emit at the same time. In that case it picks the smallest of these competing values and continues doing so
	 * as long as there is demand. It is therefore best suited for asynchronous sources where you do not want to wait
	 * for a value from each source before emitting a value downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergePriorityNaturalOrder.svg" alt="">
	 *
	 * @param sources {@link Publisher} sources of {@link Comparable} to merge
	 * @param <I> a {@link Comparable} merged type that has a {@link Comparator#naturalOrder() natural order}
	 * @return a merged {@link Flux} that compares the latest available value from each source, publishing the
	 * smallest value and replenishing the source that produced it.
	 */
	@SafeVarargs
	public static <I extends Comparable<? super I>> Flux<I> mergePriority(Publisher<? extends I>... sources) {
		return mergePriority(Queues.SMALL_BUFFER_SIZE, Comparator.naturalOrder(), sources);
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by the provided
	 * {@link Comparator}) <strong>as they arrive</strong>. This is not a {@link #sort(Comparator)}, as it doesn't consider
	 * the whole of each sequences. Unlike mergeComparing, this operator does <em>not</em> wait for a value from each
	 * source to arrive either.
	 * <p>
	 * While this operator does retrieve at most one value from each source, it only compares values when two or more
	 * sources emit at the same time. In that case it picks the smallest of these competing values and continues doing so
	 * as long as there is demand. It is therefore best suited for asynchronous sources where you do not want to wait
	 * for a value from each source before emitting a value downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergePriority.svg" alt="">
	 *
	 * @param comparator the {@link Comparator} to use to find the smallest value
	 * @param sources {@link Publisher} sources to merge
	 * @param <T> the merged type
	 * @return a merged {@link Flux} that compares the latest available value from each source, publishing the
	 * smallest value and replenishing the source that produced it.
	 */
	@SafeVarargs
	public static <T> Flux<T> mergePriority(Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		return mergePriority(Queues.SMALL_BUFFER_SIZE, comparator, sources);
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by the provided
	 * {@link Comparator}) <strong>as they arrive</strong>. This is not a {@link #sort(Comparator)}, as it doesn't consider
	 * the whole of each sequences. Unlike mergeComparing, this operator does <em>not</em> wait for a value from each
	 * source to arrive either.
	 * <p>
	 * While this operator does retrieve at most one value from each source, it only compares values when two or more
	 * sources emit at the same time. In that case it picks the smallest of these competing values and continues doing so
	 * as long as there is demand. It is therefore best suited for asynchronous sources where you do not want to wait
	 * for a value from each source before emitting a value downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergePriority.svg" alt="">
	 *
	 * @param prefetch the number of elements to prefetch from each source (avoiding too
	 * many small requests to the source when picking)
	 * @param comparator the {@link Comparator} to use to find the smallest value
	 * @param sources {@link Publisher} sources to merge
	 * @param <T> the merged type
	 * @return a merged {@link Flux} that compares the latest available value from each source, publishing the
	 * smallest value and replenishing the source that produced it.
	 */
	@SafeVarargs
	public static <T> Flux<T> mergePriority(int prefetch, Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from(sources[0]);
		}
		return onAssembly(new FluxMergeComparing<>(prefetch, comparator, false,  false, sources));
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by the provided
	 * {@link Comparator}) <strong>as they arrive</strong>. This is not a {@link #sort(Comparator)}, as it doesn't consider
	 * the whole of each sequences. Unlike mergeComparing, this operator does <em>not</em> wait for a value from each
	 * source to arrive either.
	 * <p>
	 * While this operator does retrieve at most one value from each source, it only compares values when two or more
	 * sources emit at the same time. In that case it picks the smallest of these competing values and continues doing so
	 * as long as there is demand. It is therefore best suited for asynchronous sources where you do not want to wait
	 * for a value from each source before emitting a value downstream.
	 * <p>
	 * Note that it is delaying errors until all data is consumed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergePriority.svg" alt="">
	 *
	 * @param prefetch the number of elements to prefetch from each source (avoiding too
	 * many small requests to the source when picking)
	 * @param comparator the {@link Comparator} to use to find the smallest value
	 * @param sources {@link Publisher} sources to merge
	 * @param <T> the merged type
	 * @return a merged {@link Flux} that compares the latest available value from each source, publishing the
	 * smallest value and replenishing the source that produced it.
	 */
	@SafeVarargs
	public static <T> Flux<T> mergePriorityDelayError(int prefetch, Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from(sources[0]);
		}
		return onAssembly(new FluxMergeComparing<>(prefetch, comparator, true, false, sources));
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by their natural order).
	 * This is not a {@link #sort()}, as it doesn't consider the whole of each sequences.
	 * <p>
	 * Instead, this operator considers only one value from each source and picks the
	 * smallest of all these values, then replenishes the slot for that picked source.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparingNaturalOrder.svg" alt="">
	 *
	 * @param sources {@link Publisher} sources of {@link Comparable} to merge
	 * @param <I> a {@link Comparable} merged type that has a {@link Comparator#naturalOrder() natural order}
	 * @return a merged {@link Flux} that , subscribing early but keeping the original ordering
	 */
	@SafeVarargs
	public static <I extends Comparable<? super I>> Flux<I> mergeComparing(Publisher<? extends I>... sources) {
		return mergeComparing(Queues.SMALL_BUFFER_SIZE, Comparator.naturalOrder(), sources);
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by the provided
	 * {@link Comparator}). This is not a {@link #sort(Comparator)}, as it doesn't consider
	 * the whole of each sequences.
	 * <p>
	 * Instead, this operator considers only one value from each source and picks the
	 * smallest of all these values, then replenishes the slot for that picked source.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparing.svg" alt="">
	 *
	 * @param comparator the {@link Comparator} to use to find the smallest value
	 * @param sources {@link Publisher} sources to merge
	 * @param <T> the merged type
	 * @return a merged {@link Flux} that compares latest values from each source, using the
	 * smallest value and replenishing the source that produced it
	 */
	@SafeVarargs
	public static <T> Flux<T> mergeComparing(Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		return mergeComparing(Queues.SMALL_BUFFER_SIZE, comparator, sources);
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by the provided
	 * {@link Comparator}). This is not a {@link #sort(Comparator)}, as it doesn't consider
	 * the whole of each sequences.
	 * <p>
	 * Instead, this operator considers only one value from each source and picks the
	 * smallest of all these values, then replenishes the slot for that picked source.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparing.svg" alt="">
	 *
	 * @param prefetch the number of elements to prefetch from each source (avoiding too
	 * many small requests to the source when picking)
	 * @param comparator the {@link Comparator} to use to find the smallest value
	 * @param sources {@link Publisher} sources to merge
	 * @param <T> the merged type
	 * @return a merged {@link Flux} that compares latest values from each source, using the
	 * smallest value and replenishing the source that produced it
	 */
	@SafeVarargs
	public static <T> Flux<T> mergeComparing(int prefetch, Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from(sources[0]);
		}
		return onAssembly(new FluxMergeComparing<>(prefetch, comparator, false, true, sources));
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by the provided
	 * {@link Comparator}). This is not a {@link #sort(Comparator)}, as it doesn't consider
	 * the whole of each sequences.
	 * <p>
	 * Instead, this operator considers only one value from each source and picks the
	 * smallest of all these values, then replenishes the slot for that picked source.
	 * <p>
	 * Note that it is delaying errors until all data is consumed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparing.svg" alt="">
	 *
	 * @param prefetch the number of elements to prefetch from each source (avoiding too
	 * many small requests to the source when picking)
	 * @param comparator the {@link Comparator} to use to find the smallest value
	 * @param sources {@link Publisher} sources to merge
	 * @param <T> the merged type
	 * @return a merged {@link Flux} that compares latest values from each source, using the
	 * smallest value and replenishing the source that produced it
	 */
	@SafeVarargs
	public static <T> Flux<T> mergeComparingDelayError(int prefetch, Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from(sources[0]);
		}
		return onAssembly(new FluxMergeComparing<>(prefetch, comparator, true, true, sources));
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by their natural order).
	 * This is not a {@link #sort()}, as it doesn't consider the whole of each sequences.
	 * <p>
	 * Instead, this operator considers only one value from each source and picks the
	 * smallest of all these values, then replenishes the slot for that picked source.
	 * <p>
	 * Note that it is delaying errors until all data is consumed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparingNaturalOrder.svg" alt="">
	 *
	 * @param sources {@link Publisher} sources of {@link Comparable} to merge
	 * @param <I> a {@link Comparable} merged type that has a {@link Comparator#naturalOrder() natural order}
	 * @return a merged {@link Flux} that compares latest values from each source, using the
	 * smallest value and replenishing the source that produced it
	 * @deprecated Use {@link #mergeComparingDelayError(int, Comparator, Publisher[])} instead
	 * (as {@link #mergeComparing(Publisher[])} don't have this operator's delayError behavior).
	 * To be removed in 3.6.0 at the earliest.
	 */
	@SafeVarargs
	@Deprecated
	public static <I extends Comparable<? super I>> Flux<I> mergeOrdered(Publisher<? extends I>... sources) {
		return mergeOrdered(Queues.SMALL_BUFFER_SIZE, Comparator.naturalOrder(), sources);
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by the provided
	 * {@link Comparator}). This is not a {@link #sort(Comparator)}, as it doesn't consider
	 * the whole of each sequences.
	 * <p>
	 * Instead, this operator considers only one value from each source and picks the
	 * smallest of all these values, then replenishes the slot for that picked source.
	 * <p>
	 * Note that it is delaying errors until all data is consumed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparing.svg" alt="">
	 *
	 * @param comparator the {@link Comparator} to use to find the smallest value
	 * @param sources {@link Publisher} sources to merge
	 * @param <T> the merged type
	 * @return a merged {@link Flux} that compares latest values from each source, using the
	 * smallest value and replenishing the source that produced it
	 * @deprecated Use {@link #mergeComparingDelayError(int, Comparator, Publisher[])} instead
	 * (as {@link #mergeComparing(Publisher[])} don't have this operator's delayError behavior).
	 * To be removed in 3.6.0 at the earliest.
	 */
	@SafeVarargs
	@Deprecated
	public static <T> Flux<T> mergeOrdered(Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		return mergeOrdered(Queues.SMALL_BUFFER_SIZE, comparator, sources);
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by the provided
	 * {@link Comparator}). This is not a {@link #sort(Comparator)}, as it doesn't consider
	 * the whole of each sequences.
	 * <p>
	 * Instead, this operator considers only one value from each source and picks the
	 * smallest of all these values, then replenishes the slot for that picked source.
	 * <p>
	 * Note that it is delaying errors until all data is consumed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparing.svg" alt="">
	 *
	 * @param prefetch the number of elements to prefetch from each source (avoiding too
	 * many small requests to the source when picking)
	 * @param comparator the {@link Comparator} to use to find the smallest value
	 * @param sources {@link Publisher} sources to merge
	 * @param <T> the merged type
	 * @return a merged {@link Flux} that compares latest values from each source, using the
	 * smallest value and replenishing the source that produced it
	 * @deprecated Use {@link #mergeComparingDelayError(int, Comparator, Publisher[])} instead
	 * (as {@link #mergeComparing(Publisher[])} don't have this operator's delayError behavior).
	 * To be removed in 3.6.0 at the earliest.
	 */
	@SafeVarargs
	@Deprecated
	public static <T> Flux<T> mergeOrdered(int prefetch, Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from(sources[0]);
		}
		return onAssembly(new FluxMergeComparing<>(prefetch, comparator, true, true, sources));
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an ordered merged sequence. Unlike concat, the inner publishers are subscribed to
	 * eagerly. Unlike merge, their emitted values are merged into the final sequence in
	 * subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialAsyncSources.svg" alt="">
	 *
	 * @param sources a {@link Publisher} of {@link Publisher} sources to merge
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public static <T> Flux<T> mergeSequential(Publisher<? extends Publisher<? extends T>> sources) {
		return mergeSequential(sources, false, Queues.SMALL_BUFFER_SIZE,
				Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an ordered merged sequence. Unlike concat, the inner publishers are subscribed to
	 * eagerly (but at most {@code maxConcurrency} sources at a time). Unlike merge, their
	 * emitted values are merged into the final sequence in subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialAsyncSources.svg" alt="">
	 *
	 * @param sources a {@link Publisher} of {@link Publisher} sources to merge
	 * @param prefetch the inner source request size
	 * @param maxConcurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public static <T> Flux<T> mergeSequential(Publisher<? extends Publisher<? extends T>> sources,
			int maxConcurrency, int prefetch) {
		return mergeSequential(sources, false, maxConcurrency, prefetch);
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an ordered merged sequence. Unlike concat, the inner publishers are subscribed to
	 * eagerly (but at most {@code maxConcurrency} sources at a time). Unlike merge, their
	 * emitted values are merged into the final sequence in subscription order.
	 * This variant will delay any error until after the rest of the mergeSequential backlog has been processed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialAsyncSources.svg" alt="">
	 *
	 * @param sources a {@link Publisher} of {@link Publisher} sources to merge
	 * @param prefetch the inner source request size
	 * @param maxConcurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public static <T> Flux<T> mergeSequentialDelayError(Publisher<? extends Publisher<? extends T>> sources,
			int maxConcurrency, int prefetch) {
		return mergeSequential(sources, true, maxConcurrency, prefetch);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an array/vararg
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly. Unlike merge, their emitted values are merged into the final sequence in subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param sources a number of {@link Publisher} sequences to merge
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	@SafeVarargs
	public static <I> Flux<I> mergeSequential(Publisher<? extends I>... sources) {
		return mergeSequential(Queues.XS_BUFFER_SIZE, false, sources);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an array/vararg
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly. Unlike merge, their emitted values are merged into the final sequence in subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param prefetch the inner source request size
	 * @param sources a number of {@link Publisher} sequences to merge
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	@SafeVarargs
	public static <I> Flux<I> mergeSequential(int prefetch, Publisher<? extends I>... sources) {
		return mergeSequential(prefetch, false, sources);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an array/vararg
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly. Unlike merge, their emitted values are merged into the final sequence in subscription order.
	 * This variant will delay any error until after the rest of the mergeSequential backlog
	 * has been processed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param prefetch the inner source request size
	 * @param sources a number of {@link Publisher} sequences to merge
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	@SafeVarargs
	public static <I> Flux<I> mergeSequentialDelayError(int prefetch, Publisher<? extends I>... sources) {
		return mergeSequential(prefetch, true, sources);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an {@link Iterable}
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly. Unlike merge, their emitted values are merged into the final sequence in subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param sources an {@link Iterable} of {@link Publisher} sequences to merge
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public static <I> Flux<I> mergeSequential(Iterable<? extends Publisher<? extends I>> sources) {
		return mergeSequential(sources, false, Queues.SMALL_BUFFER_SIZE,
				Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an {@link Iterable}
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly (but at most {@code maxConcurrency} sources at a time). Unlike merge, their
	 * emitted values are merged into the final sequence in subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param sources an {@link Iterable} of {@link Publisher} sequences to merge
	 * @param maxConcurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param prefetch the inner source request size
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public static <I> Flux<I> mergeSequential(Iterable<? extends Publisher<? extends I>> sources,
			int maxConcurrency, int prefetch) {
		return mergeSequential(sources, false, maxConcurrency, prefetch);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an {@link Iterable}
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly (but at most {@code maxConcurrency} sources at a time). Unlike merge, their
	 * emitted values are merged into the final sequence in subscription order.
	 * This variant will delay any error until after the rest of the mergeSequential backlog
	 * has been processed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param sources an {@link Iterable} of {@link Publisher} sequences to merge
	 * @param maxConcurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param prefetch the inner source request size
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public static <I> Flux<I> mergeSequentialDelayError(Iterable<? extends Publisher<? extends I>> sources,
			int maxConcurrency, int prefetch) {
		return mergeSequential(sources, true, maxConcurrency, prefetch);
	}

	/**
	 * Create a {@link Flux} that will never signal any data, error or completion signal.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/never.svg" alt="">
	 *
	 * @param <T> the {@link Subscriber} type target
	 *
	 * @return a never completing {@link Flux}
	 */
	public static <T> Flux<T> never() {
		return FluxNever.instance();
	}

	/**
	 * Build a {@link Flux} that will only emit a sequence of {@code count} incrementing integers,
	 * starting from {@code start}. That is, emit integers between {@code start} (included)
	 * and {@code start + count} (excluded) then complete.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/range.svg" alt="">
	 *
	 * @param start the first integer to be emit
	 * @param count the total number of incrementing values to emit, including the first value
	 * @return a ranged {@link Flux}
	 */
	public static Flux<Integer> range(int start, int count) {
		if (count == 1) {
			return just(start);
		}
		if (count == 0) {
			return empty();
		}
		return onAssembly(new FluxRange(start, count));
	}

	/**
	 * Creates a {@link Flux} that mirrors the most recently emitted {@link Publisher},
	 * forwarding its data until a new {@link Publisher} comes in the source.
	 * <p>
	 * The resulting {@link Flux} will complete once there are no new {@link Publisher} in
	 * the source (source has completed) and the last mirrored {@link Publisher} has also
	 * completed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/switchOnNext.svg" alt="">
	 * <p>
	 * This operator requests the {@code mergedPublishers} source for an unbounded amount of inner publishers,
	 * but doesn't request each inner {@link Publisher} unless the downstream has made
	 * a corresponding request (no prefetch on publishers emitted by {@code mergedPublishers}).
	 *
	 * @param mergedPublishers The {@link Publisher} of {@link Publisher} to switch on and mirror.
	 * @param <T> the produced type
	 *
	 * @return a {@link Flux} accepting publishers and producing T
	 */
	public static <T> Flux<T> switchOnNext(Publisher<? extends Publisher<? extends T>> mergedPublishers) {
		return onAssembly(new FluxSwitchMapNoPrefetch<>(from(mergedPublishers),
			identityFunction()));
	}

	/**
	 * Creates a {@link Flux} that mirrors the most recently emitted {@link Publisher},
	 * forwarding its data until a new {@link Publisher} comes in the source.
	 * <p>
	 * The resulting {@link Flux} will complete once there are no new {@link Publisher} in
	 * the source (source has completed) and the last mirrored {@link Publisher} has also
	 * completed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/switchOnNext.svg" alt="">
	 *
	 * @param mergedPublishers The {@link Publisher} of {@link Publisher} to switch on and mirror.
	 * @param prefetch the inner source request size
	 * @param <T> the produced type
	 *
	 * @return a {@link Flux} accepting publishers and producing T
	 *
	 * @deprecated to be removed in 3.6.0 at the earliest. In 3.5.0, you should replace
	 * calls with prefetch=0 with calls to switchOnNext(mergedPublishers), as the default
	 * behavior of the single-parameter variant will then change to prefetch=0.
	 */
	@Deprecated
	public static <T> Flux<T> switchOnNext(Publisher<? extends Publisher<? extends T>> mergedPublishers, int prefetch) {
		if (prefetch == 0) {
			return onAssembly(new FluxSwitchMapNoPrefetch<>(from(mergedPublishers),
					identityFunction()));
		}
		return onAssembly(new FluxSwitchMap<>(from(mergedPublishers),
				identityFunction(),
				Queues.unbounded(prefetch), prefetch));
	}

	/**
	 * Uses a resource, generated by a supplier for each individual Subscriber, while streaming the values from a
	 * Publisher derived from the same resource and makes sure the resource is released if the sequence terminates or
	 * the Subscriber cancels.
	 * <p>
	 * Eager resource cleanup happens just before the source termination and exceptions raised by the cleanup Consumer
	 * may override the terminal event.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingForFlux.svg" alt="">
	 * <p>
	 * For an asynchronous version of the cleanup, with distinct path for onComplete, onError
	 * and cancel terminations, see {@link #usingWhen(Publisher, Function, Function, BiFunction, Function)}.
	 *
	 * @param resourceSupplier a {@link Callable} that is called on subscribe to generate the resource
	 * @param sourceSupplier a factory to derive a {@link Publisher} from the supplied resource
	 * @param resourceCleanup a resource cleanup callback invoked on completion
	 * @param <T> emitted type
	 * @param <D> resource type
	 *
	 * @return a new {@link Flux} built around a disposable resource
	 * @see #usingWhen(Publisher, Function, Function, BiFunction, Function)
	 * @see #usingWhen(Publisher, Function, Function)
	 */
	public static <T, D> Flux<T> using(Callable<? extends D> resourceSupplier, Function<? super D, ? extends
			Publisher<? extends T>> sourceSupplier, Consumer<? super D> resourceCleanup) {
		return using(resourceSupplier, sourceSupplier, resourceCleanup, true);
	}

	/**
	 * Uses a resource, generated by a supplier for each individual Subscriber, while streaming the values from a
	 * Publisher derived from the same resource and makes sure the resource is released if the sequence terminates or
	 * the Subscriber cancels.
	 * <p>
	 * <ul> <li>Eager resource cleanup happens just before the source termination and exceptions raised by the cleanup
	 * Consumer may override the terminal event.</li> <li>Non-eager cleanup will drop any exception.</li> </ul>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingForFlux.svg" alt="">
	 * <p>
	 * For an asynchronous version of the cleanup, with distinct path for onComplete, onError
	 * and cancel terminations, see {@link #usingWhen(Publisher, Function, Function, BiFunction, Function)}.
	 *
	 * @param resourceSupplier a {@link Callable} that is called on subscribe to generate the resource
	 * @param sourceSupplier a factory to derive a {@link Publisher} from the supplied resource
	 * @param resourceCleanup a resource cleanup callback invoked on completion
	 * @param eager true to clean before terminating downstream subscribers
	 * @param <T> emitted type
	 * @param <D> resource type
	 *
	 * @return a new {@link Flux} built around a disposable resource
	 * @see #usingWhen(Publisher, Function, Function, BiFunction, Function)
	 * @see #usingWhen(Publisher, Function, Function)
	 */
	public static <T, D> Flux<T> using(Callable<? extends D> resourceSupplier, Function<? super D, ? extends
			Publisher<? extends T>> sourceSupplier, Consumer<? super D> resourceCleanup, boolean eager) {
		return onAssembly(new FluxUsing<>(resourceSupplier,
				sourceSupplier,
				resourceCleanup,
				eager));
	}

	/**
	 * Uses an {@link AutoCloseable} resource, generated by a supplier for each individual Subscriber,
	 * while streaming the values from a Publisher derived from the same resource and makes sure
	 * the resource is released if the sequence terminates or the Subscriber cancels.
	 * <p>
	 * Eager {@link AutoCloseable} resource cleanup happens just before the source termination and exceptions raised
	 * by the cleanup Consumer may override the terminal event.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingForFlux.svg" alt="">
	 * <p>
	 * For an asynchronous version of the cleanup, with distinct path for onComplete, onError
	 * and cancel terminations, see {@link #usingWhen(Publisher, Function, Function, BiFunction, Function)}.
	 *
	 * @param resourceSupplier a {@link Callable} that is called on subscribe to generate the resource
	 * @param sourceSupplier a factory to derive a {@link Publisher} from the supplied resource
	 * @param <T> emitted type
	 * @param <D> resource type
	 *
	 * @return a new {@link Flux} built around a disposable resource
	 * @see #usingWhen(Publisher, Function, Function, BiFunction, Function)
	 * @see #usingWhen(Publisher, Function, Function)
	 */
	public static <T, D extends AutoCloseable> Flux<T> using(Callable<? extends D> resourceSupplier,
			Function<? super D, ? extends Publisher<? extends T>> sourceSupplier) {
		return using(resourceSupplier, sourceSupplier, true);
	}

	/**
	 * Uses an {@link AutoCloseable} resource, generated by a supplier for each individual Subscriber,
	 * while streaming the values from a Publisher derived from the same resource and makes sure
	 * the resource is released if the sequence terminates or the Subscriber cancels.
	 * <p>
	 * <ul>
	 *     <li>Eager {@link AutoCloseable} resource cleanup happens just before the source termination and exceptions raised
	 *     by the cleanup Consumer may override the terminal event.</li>
	 *     <li>Non-eager cleanup will drop any exception.</li>
	 * </ul>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingForFlux.svg" alt="">
	 * <p>
	 * For an asynchronous version of the cleanup, with distinct path for onComplete, onError
	 * and cancel terminations, see {@link #usingWhen(Publisher, Function, Function, BiFunction, Function)}.
	 *
	 * @param resourceSupplier a {@link Callable} that is called on subscribe to generate the resource
	 * @param sourceSupplier a factory to derive a {@link Publisher} from the supplied resource
	 * @param eager true to clean before terminating downstream subscribers
	 * @param <T> emitted type
	 * @param <D> resource type
	 *
	 * @return a new {@link Flux} built around a disposable resource
	 * @see #usingWhen(Publisher, Function, Function, BiFunction, Function)
	 * @see #usingWhen(Publisher, Function, Function)
	 */
	public static <T, D extends AutoCloseable> Flux<T> using(Callable<? extends D> resourceSupplier,
			Function<? super D, ? extends Publisher<? extends T>> sourceSupplier, boolean eager) {
		return using(resourceSupplier, sourceSupplier, Exceptions.AUTO_CLOSE, eager);
	}

	/**
	 * Uses a resource, generated by a {@link Publisher} for each individual {@link Subscriber},
	 * while streaming the values from a {@link Publisher} derived from the same resource.
	 * Whenever the resulting sequence terminates, a provided {@link Function} generates
	 * a "cleanup" {@link Publisher} that is invoked but doesn't change the content of the
	 * main sequence. Instead it just defers the termination (unless it errors, in which case
	 * the error suppresses the original termination signal).
	 * <p>
	 * Note that if the resource supplying {@link Publisher} emits more than one resource, the
	 * subsequent resources are dropped ({@link Operators#onNextDropped(Object, Context)}). If
	 * the publisher errors AFTER having emitted one resource, the error is also silently dropped
	 * ({@link Operators#onErrorDropped(Throwable, Context)}).
	 * An empty completion or error without at least one onNext signal triggers a short-circuit
	 * of the main sequence with the same terminal signal (no resource is established, no
	 * cleanup is invoked).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingWhenSuccessForFlux.svg" alt="">
	 *
	 * @param resourceSupplier a {@link Publisher} that "generates" the resource,
	 * subscribed for each subscription to the main sequence
	 * @param resourceClosure a factory to derive a {@link Publisher} from the supplied resource
	 * @param asyncCleanup an asynchronous resource cleanup invoked when the resource
	 * closure terminates (with onComplete, onError or cancel)
	 * @param <T> the type of elements emitted by the resource closure, and thus the main sequence
	 * @param <D> the type of the resource object
	 * @return a new {@link Flux} built around a "transactional" resource, with asynchronous
	 * cleanup on all terminations (onComplete, onError, cancel)
	 */
	public static <T, D> Flux<T> usingWhen(Publisher<D> resourceSupplier,
			Function<? super D, ? extends Publisher<? extends T>> resourceClosure,
			Function<? super D, ? extends Publisher<?>> asyncCleanup) {
		return usingWhen(resourceSupplier, resourceClosure, asyncCleanup, (resource, error) -> asyncCleanup.apply(resource), asyncCleanup);
	}

	/**
	 * Uses a resource, generated by a {@link Publisher} for each individual {@link Subscriber},
	 * while streaming the values from a {@link Publisher} derived from the same resource.
	 * Note that all steps of the operator chain that would need the resource to be in an open
	 * stable state need to be described inside the {@code resourceClosure} {@link Function}.
	 * <p>
	 * Whenever the resulting sequence terminates, the relevant {@link Function} generates
	 * a "cleanup" {@link Publisher} that is invoked but doesn't change the content of the
	 * main sequence. Instead it just defers the termination (unless it errors, in which case
	 * the error suppresses the original termination signal).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingWhenSuccessForFlux.svg" alt="">
	 * <p>
	 * Individual cleanups can also be associated with main sequence cancellation and
	 * error terminations:
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingWhenFailureForFlux.svg" alt="">
	 * <p>
	 * Note that if the resource supplying {@link Publisher} emits more than one resource, the
	 * subsequent resources are dropped ({@link Operators#onNextDropped(Object, Context)}). If
	 * the publisher errors AFTER having emitted one resource, the error is also silently dropped
	 * ({@link Operators#onErrorDropped(Throwable, Context)}).
	 * An empty completion or error without at least one onNext signal triggers a short-circuit
	 * of the main sequence with the same terminal signal (no resource is established, no
	 * cleanup is invoked).
	 * <p>
	 * Additionally, the terminal signal is replaced by any error that might have happened
	 * in the terminating {@link Publisher}:
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingWhenCleanupErrorForFlux.svg" alt="">
	 * <p>
	 * Finally, early cancellations will cancel the resource supplying {@link Publisher}:
	 * <p>
	 * <img class="marble" src="doc-files/marbles/usingWhenEarlyCancelForFlux.svg" alt="">
	 *
	 * @param resourceSupplier a {@link Publisher} that "generates" the resource,
	 * subscribed for each subscription to the main sequence
	 * @param resourceClosure a factory to derive a {@link Publisher} from the supplied resource
	 * @param asyncComplete an asynchronous resource cleanup invoked if the resource closure terminates with onComplete
	 * @param asyncError an asynchronous resource cleanup invoked if the resource closure terminates with onError.
	 * The terminating error is provided to the {@link BiFunction}
	 * @param asyncCancel an asynchronous resource cleanup invoked if the resource closure is cancelled.
	 * When {@code null}, the {@code asyncComplete} path is used instead.
	 * @param <T> the type of elements emitted by the resource closure, and thus the main sequence
	 * @param <D> the type of the resource object
	 * @return a new {@link Flux} built around a "transactional" resource, with several
	 * termination path triggering asynchronous cleanup sequences
	 * @see #usingWhen(Publisher, Function, Function)
	 */
	public static <T, D> Flux<T> usingWhen(Publisher<D> resourceSupplier,
			Function<? super D, ? extends Publisher<? extends T>> resourceClosure,
			Function<? super D, ? extends Publisher<?>> asyncComplete,
			BiFunction<? super D, ? super Throwable, ? extends Publisher<?>> asyncError,
			//the operator itself accepts null for asyncCancel, but we won't in the public API
			Function<? super D, ? extends Publisher<?>> asyncCancel) {
		return onAssembly(new FluxUsingWhen<>(resourceSupplier, resourceClosure,
				asyncComplete, asyncError, asyncCancel));
	}

	/**
	 * Zip two sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into an output value (constructed by the provided
	 * combinator). The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipTwoSourcesWithZipperForFlux.svg" alt="">
	 *
	 * @param source1 The first {@link Publisher} source to zip.
	 * @param source2 The second {@link Publisher} source to zip.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 * value to signal downstream
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <O> The produced output after transformation by the combinator
	 *
	 * @return a zipped {@link Flux}
	 */
    public static <T1, T2, O> Flux<O> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			final BiFunction<? super T1, ? super T2, ? extends O> combinator) {

		return onAssembly(new FluxZip<T1, O>(source1,
				source2,
				combinator,
				Queues.xs(),
				Queues.XS_BUFFER_SIZE));
	}

	/**
	 * Zip two sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into a {@link Tuple2}.
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForFlux.svg" alt="">
	 *
	 * @param source1 The first {@link Publisher} source to zip.
	 * @param source2 The second {@link Publisher} source to zip.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 *
	 * @return a zipped {@link Flux}
	 */
	public static <T1, T2> Flux<Tuple2<T1, T2>> zip(Publisher<? extends T1> source1, Publisher<? extends T2> source2) {
		return zip(source1, source2, tuple2Function());
	}

	/**
	 * Zip three sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into a {@link Tuple3}.
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForFlux.svg" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 *
	 * @return a zipped {@link Flux}
	 */
	public static <T1, T2, T3> Flux<Tuple3<T1, T2, T3>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3) {
		return zip(Tuples.fn3(), source1, source2, source3);
	}

	/**
	 * Zip four sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into a {@link Tuple4}.
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForFlux.svg" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 *
	 * @return a zipped {@link Flux}
	 */
	public static <T1, T2, T3, T4> Flux<Tuple4<T1, T2, T3, T4>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4) {
		return zip(Tuples.fn4(), source1, source2, source3, source4);
	}

	/**
	 * Zip five sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into a {@link Tuple5}.
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForFlux.svg" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 *
	 * @return a zipped {@link Flux}
	 */
	public static <T1, T2, T3, T4, T5> Flux<Tuple5<T1, T2, T3, T4, T5>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5) {
		return zip(Tuples.fn5(), source1, source2, source3, source4, source5);
	}

	/**
	 * Zip six sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into a {@link Tuple6}.
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForFlux.svg" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 * @param <T6> type of the value from source6
	 *
	 * @return a zipped {@link Flux}
	 */
	public static <T1, T2, T3, T4, T5, T6> Flux<Tuple6<T1, T2, T3, T4, T5, T6>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Publisher<? extends T6> source6) {
		return zip(Tuples.fn6(), source1, source2, source3, source4, source5, source6);
	}

	/**
	 * Zip seven sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into a {@link Tuple7}.
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForFlux.svg" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param source7 The seventh upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 * @param <T6> type of the value from source6
	 * @param <T7> type of the value from source7
	 *
	 * @return a zipped {@link Flux}
	 */
	public static <T1, T2, T3, T4, T5, T6, T7> Flux<Tuple7<T1, T2, T3, T4, T5, T6, T7>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Publisher<? extends T6> source6,
			Publisher<? extends T7> source7) {
		return zip(Tuples.fn7(), source1, source2, source3, source4, source5, source6, source7);
	}

	/**
	 * Zip eight sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into a {@link Tuple8}.
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipFixedSourcesForFlux.svg" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param source7 The seventh upstream {@link Publisher} to subscribe to.
	 * @param source8 The eight upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 * @param <T6> type of the value from source6
	 * @param <T7> type of the value from source7
	 * @param <T8> type of the value from source8
	 *
	 * @return a zipped {@link Flux}
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8> Flux<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Publisher<? extends T6> source6,
			Publisher<? extends T7> source7,
			Publisher<? extends T8> source8) {
		return zip(Tuples.fn8(), source1, source2, source3, source4, source5, source6, source7, source8);
	}

	/**
	 * Zip multiple sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into an output value (constructed by the provided
	 * combinator).
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 *
	 * The {@link Iterable#iterator()} will be called on each {@link Publisher#subscribe(Subscriber)}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipIterableSourcesForFlux.svg" alt="">
	 *
	 * @param sources the {@link Iterable} providing sources to zip
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the value
	 * to signal downstream
	 * @param <O> the combined produced type
	 *
	 * @return a zipped {@link Flux}
	 */
	public static <O> Flux<O> zip(Iterable<? extends Publisher<?>> sources,
			final Function<? super Object[], ? extends O> combinator) {

		return zip(sources, Queues.XS_BUFFER_SIZE, combinator);
	}

	/**
	 * Zip multiple sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into an output value (constructed by the provided
	 * combinator).
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 *
	 * The {@link Iterable#iterator()} will be called on each {@link Publisher#subscribe(Subscriber)}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipIterableSourcesForFlux.svg" alt="">
	 *
	 * @param sources the {@link Iterable} providing sources to zip
	 * @param prefetch the inner source request size
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the value
	 * to signal downstream
	 * @param <O> the combined produced type
	 *
	 * @return a zipped {@link Flux}
	 */
	public static <O> Flux<O> zip(Iterable<? extends Publisher<?>> sources,
			int prefetch,
			final Function<? super Object[], ? extends O> combinator) {

		return onAssembly(new FluxZip<>(sources,
			combinator,
			Queues.get(prefetch),
			prefetch));
	}

	/**
	 * Zip multiple sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into an output value (constructed by the provided
	 * combinator).
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipIterableSourcesForFlux.svg" alt="">
	 *
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 * value to signal downstream
	 * @param sources the array providing sources to zip
	 * @param <I> the type of the input sources
	 * @param <O> the combined produced type
	 *
	 * @return a zipped {@link Flux}
	 */
	@SafeVarargs
	public static <I, O> Flux<O> zip(
			final Function<? super Object[], ? extends O> combinator, Publisher<? extends I>... sources) {
		return zip(combinator, Queues.XS_BUFFER_SIZE, sources);
	}

	/**
	 * Zip multiple sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into an output value (constructed by the provided
	 * combinator).
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipIterableSourcesForFlux.svg" alt="">
	 *
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 * value to signal downstream
	 * @param prefetch individual source request size
	 * @param sources the array providing sources to zip
	 * @param <I> the type of the input sources
	 * @param <O> the combined produced type
	 *
	 * @return a zipped {@link Flux}
	 */
	@SafeVarargs
	public static <I, O> Flux<O> zip(final Function<? super Object[], ? extends O> combinator,
			int prefetch,
			Publisher<? extends I>... sources) {

		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
		    Publisher<? extends I> source = sources[0];
		    if (source instanceof Fuseable) {
			    return onAssembly(new FluxMapFuseable<>(from(source),
					    v -> combinator.apply(new Object[]{v})));
		    }
			return onAssembly(new FluxMap<>(from(source),
					v -> combinator.apply(new Object[]{v})));
		}

		return onAssembly(new FluxZip<>(sources,
				combinator,
				Queues.get(prefetch),
				prefetch));
	}

	/**
	 * Zip multiple sources together, that is to say wait for all the sources to emit one
	 * element and combine these elements once into an output value (constructed by the provided
	 * combinator).
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * Note that the {@link Publisher} sources from the outer {@link Publisher} will
	 * accumulate into an exhaustive list before starting zip operation.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipAsyncSourcesForFlux.svg" alt="">
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} sources to zip. A finite publisher is required.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the value
	 * to signal downstream
	 * @param <TUPLE> the raw tuple type
	 * @param <V> The produced output after transformation by the given combinator
	 *
	 * @return a {@link Flux} based on the produced value
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
    public static <TUPLE extends Tuple2, V> Flux<V> zip(Publisher<? extends
			Publisher<?>> sources,
			final Function<? super TUPLE, ? extends V> combinator) {

		return onAssembly(new FluxBuffer<>(from(sources), Integer.MAX_VALUE, listSupplier())
		                    .flatMap(new Function<List<? extends Publisher<?>>, Publisher<V>>() {
			                    @Override
			                    public Publisher<V> apply(List<? extends Publisher<?>> publishers) {
				                    return zip(Tuples.fnAny((Function<Tuple2, V>)
						                    combinator), publishers.toArray(new Publisher[publishers
						                    .size()]));
			                    }
		                    }));
	}

	/**
	 *
	 * Emit a single boolean true if all values of this sequence match
	 * the {@link Predicate}.
	 * <p>
	 * The implementation uses short-circuit logic and completes with false if
	 * the predicate doesn't match a value.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/all.svg" alt="">
	 *
	 * @param predicate the {@link Predicate} that needs to apply to all emitted items
	 *
	 * @return a new {@link Mono} with <code>true</code> if all values satisfies a predicate and <code>false</code>
	 * otherwise
	 */
	public final Mono<Boolean> all(Predicate<? super T> predicate) {
		return Mono.onAssembly(new MonoAll<>(this, predicate));
	}

	/**
	 * Emit a single boolean true if any of the values of this {@link Flux} sequence match
	 * the predicate.
	 * <p>
	 * The implementation uses short-circuit logic and completes with true if
	 * the predicate matches a value.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/any.svg" alt="">
	 *
	 * @param predicate the {@link Predicate} that needs to apply to at least one emitted item
	 *
	 * @return a new {@link Mono} with <code>true</code> if any value satisfies a predicate and <code>false</code>
	 * otherwise
	 */
	public final Mono<Boolean> any(Predicate<? super T> predicate) {
		return Mono.onAssembly(new MonoAny<>(this, predicate));
	}

	/**
	 * Transform this {@link Flux} into a target type.
	 * <blockquote><pre>
	 * {@code flux.as(Mono::from).subscribe() }
	 * </pre></blockquote>
	 *
	 * @param transformer the {@link Function} to immediately map this {@link Flux}
	 * into a target type instance.
	 * @param <P> the returned instance type
	 *
	 * @return the {@link Flux} transformed to an instance of P
	 * @see #transformDeferred(Function) transformDeferred(Function) for a lazy transformation of Flux
	 */
	public final <P> P as(Function<? super Flux<T>, P> transformer) {
		return transformer.apply(this);
	}

	/**
	 * Subscribe to this {@link Flux} and <strong>block indefinitely</strong>
	 * until the upstream signals its first value or completes. Returns that value,
	 * or null if the Flux completes empty. In case the Flux errors, the original
	 * exception is thrown (wrapped in a {@link RuntimeException} if it was a checked
	 * exception).
	 * <p>
	 * Note that each blockFirst() will trigger a new subscription: in other words,
	 * the result might miss signal from hot publishers.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/blockFirst.svg" alt="">
	 *
	 * @return the first value or null
	 */
	@Nullable
	public final T blockFirst() {
		Context context = ContextPropagationSupport.shouldPropagateContextToThreadLocals()
				? ContextPropagation.contextCaptureToEmpty() : Context.empty();
		BlockingFirstSubscriber<T> subscriber = new BlockingFirstSubscriber<>(context);
		subscribe((Subscriber<T>) subscriber);
		return subscriber.blockingGet();
	}

	/**
	 * Subscribe to this {@link Flux} and <strong>block</strong> until the upstream
	 * signals its first value, completes or a timeout expires. Returns that value,
	 * or null if the Flux completes empty. In case the Flux errors, the original
	 * exception is thrown (wrapped in a {@link RuntimeException} if it was a checked
	 * exception). If the provided timeout expires, a {@link RuntimeException} is thrown
	 * with a {@link TimeoutException} as the cause.
	 * <p>
	 * Note that each blockFirst() will trigger a new subscription: in other words,
	 * the result might miss signal from hot publishers.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/blockFirstWithTimeout.svg" alt="">
	 *
 	 * @param timeout maximum time period to wait for before raising a {@link RuntimeException}
	 * with a {@link TimeoutException} as the cause
	 * @return the first value or null
	 */
	@Nullable
	public final T blockFirst(Duration timeout) {
		Context context = ContextPropagationSupport.shouldPropagateContextToThreadLocals()
				? ContextPropagation.contextCaptureToEmpty() : Context.empty();
		BlockingFirstSubscriber<T> subscriber = new BlockingFirstSubscriber<>(context);
		subscribe((Subscriber<T>) subscriber);
		return subscriber.blockingGet(timeout.toNanos(), TimeUnit.NANOSECONDS);
	}

	/**
	 * Subscribe to this {@link Flux} and <strong>block indefinitely</strong>
	 * until the upstream signals its last value or completes. Returns that value,
	 * or null if the Flux completes empty. In case the Flux errors, the original
	 * exception is thrown (wrapped in a {@link RuntimeException} if it was a checked
	 * exception).
	 * <p>
	 * Note that each blockLast() will trigger a new subscription: in other words,
	 * the result might miss signal from hot publishers.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/blockLast.svg" alt="">
	 *
	 * @return the last value or null
	 */
	@Nullable
	public final T blockLast() {
		Context context = ContextPropagationSupport.shouldPropagateContextToThreadLocals()
				? ContextPropagation.contextCaptureToEmpty() : Context.empty();
		BlockingLastSubscriber<T> subscriber = new BlockingLastSubscriber<>(context);
		subscribe((Subscriber<T>) subscriber);
		return subscriber.blockingGet();
	}


	/**
	 * Subscribe to this {@link Flux} and <strong>block</strong> until the upstream
	 * signals its last value, completes or a timeout expires. Returns that value,
	 * or null if the Flux completes empty. In case the Flux errors, the original
	 * exception is thrown (wrapped in a {@link RuntimeException} if it was a checked
	 * exception). If the provided timeout expires, a {@link RuntimeException} is thrown
	 * with a {@link TimeoutException} as the cause.
	 * <p>
	 * Note that each blockLast() will trigger a new subscription: in other words,
	 * the result might miss signal from hot publishers.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/blockLastWithTimeout.svg" alt="">
	 *
	 * @param timeout maximum time period to wait for before raising a {@link RuntimeException}
	 * with a {@link TimeoutException} as the cause
	 * @return the last value or null
	 */
	@Nullable
	public final T blockLast(Duration timeout) {
		Context context = ContextPropagationSupport.shouldPropagateContextToThreadLocals()
				? ContextPropagation.contextCaptureToEmpty() : Context.empty();
		BlockingLastSubscriber<T> subscriber = new BlockingLastSubscriber<>(context);
		subscribe((Subscriber<T>) subscriber);
		return subscriber.blockingGet(timeout.toNanos(), TimeUnit.NANOSECONDS);
	}

	/**
	 * Collect all incoming values into a single {@link List} buffer that will be emitted
	 * by the returned {@link Flux} once this Flux completes.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/buffer.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the buffer upon cancellation or error triggered by a data signal.
	 *
	 * @return a buffered {@link Flux} of at most one {@link List}
	 * @see #collectList() for an alternative collecting algorithm returning {@link Mono}
	 */
    public final Flux<List<T>> buffer() {
	    return buffer(Integer.MAX_VALUE);
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted
	 * by the returned {@link Flux} each time the given max size is reached or once this
	 * Flux completes.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSize.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the maximum collected size
	 *
	 * @return a microbatched {@link Flux} of {@link List}
	 */
	public final Flux<List<T>> buffer(int maxSize) {
		return buffer(maxSize, listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers that
	 * will be emitted by the returned {@link Flux} each time the given max size is reached
	 * or once this Flux completes.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSize.svg" alt="">
	 * <p>
	 * Note that if buffers provided by the bufferSupplier return {@literal false} upon invocation
	 * of {@link Collection#add(Object)} for a given element, that element will be discarded.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal,
	 * as well as latest unbuffered element if the bufferSupplier fails.
	 *
	 * @param maxSize the maximum collected size
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a microbatched {@link Flux} of {@link Collection}
	 */
	public final <C extends Collection<? super T>> Flux<C> buffer(int maxSize, Supplier<C> bufferSupplier) {
		return onAssembly(new FluxBuffer<>(this, maxSize, bufferSupplier));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted
	 * by the returned {@link Flux} each time the given max size is reached or once this
	 * Flux completes. Buffers can be created with gaps, as a new buffer will be created
	 * every time {@code skip} values have been emitted by the source.
	 * <p>
	 * When maxSize < skip : dropping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeLessThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize > skip : overlapping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeGreaterThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize == skip : exact buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeEqualsSkipSize.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements in between buffers (in the case of
	 * dropping buffers). It also discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * Note however that overlapping buffer variant DOES NOT discard, as this might result in an element
	 * being discarded from an early buffer while it is still valid in a more recent buffer.
	 *
	 * @param skip the number of items to count before creating a new buffer
	 * @param maxSize the max collected size
	 *
	 * @return a microbatched {@link Flux} of possibly overlapped or gapped {@link List}
	 */
	public final Flux<List<T>> buffer(int maxSize, int skip) {
		return buffer(maxSize, skip, listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers that
	 * will be emitted by the returned {@link Flux} each time the given max size is reached
	 * or once this Flux completes. Buffers can be created with gaps, as a new buffer will
	 * be created every time {@code skip} values have been emitted by the source
	 * <p>
	 * When maxSize < skip : dropping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeLessThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize > skip : overlapping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeGreaterThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize == skip : exact buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeEqualsSkipSize.svg" alt="">
	 *
	 * <p>
	 * Note for exact buffers: If buffers provided by the bufferSupplier return {@literal false} upon invocation
	 * of {@link Collection#add(Object)} for a given element, that element will be discarded.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements in between buffers (in the case of
	 * dropping buffers). It also discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * Note however that overlapping buffer variant DOES NOT discard, as this might result in an element
	 * being discarded from an early buffer while it is still valid in a more recent buffer.
	 *
	 * @param skip the number of items to count before creating a new buffer
	 * @param maxSize the max collected size
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a microbatched {@link Flux} of possibly overlapped or gapped
	 * {@link Collection}
	 */
	public final <C extends Collection<? super T>> Flux<C> buffer(int maxSize,
			int skip, Supplier<C> bufferSupplier) {
		return onAssembly(new FluxBuffer<>(this, maxSize, skip, bufferSupplier));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers, as delimited by the
	 * signals of a companion {@link Publisher} this operator will subscribe to.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithBoundary.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param other the companion {@link Publisher} whose signals trigger new buffers
	 *
	 * @return a microbatched {@link Flux} of {@link List} delimited by signals from a {@link Publisher}
	 */
	public final Flux<List<T>> buffer(Publisher<?> other) {
		return buffer(other, listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers, as
	 * delimited by the signals of a companion {@link Publisher} this operator will
	 * subscribe to.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithBoundary.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal,
	 * and the last received element when the bufferSupplier fails.
	 *
	 * @param other the companion {@link Publisher} whose signals trigger new buffers
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a microbatched {@link Flux} of {@link Collection} delimited by signals from a {@link Publisher}
	 */
	public final <C extends Collection<? super T>> Flux<C> buffer(Publisher<?> other, Supplier<C> bufferSupplier) {
		return onAssembly(new FluxBufferBoundary<>(this, other, bufferSupplier));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted by
	 * the returned {@link Flux} every {@code bufferingTimespan}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param bufferingTimespan the duration from buffer creation until a buffer is closed and emitted
	 *
	 * @return a microbatched {@link Flux} of {@link List} delimited by the given time span
	 */
	public final Flux<List<T>> buffer(Duration bufferingTimespan) {
		return buffer(bufferingTimespan, Schedulers.parallel());
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers created at a given
	 * {@code openBufferEvery} period. Each buffer will last until the {@code bufferingTimespan} has elapsed,
	 * thus emitting the bucket in the resulting {@link Flux}.
	 * <p>
	 * When bufferingTimespan < openBufferEvery : dropping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanLessThanOpenBufferEvery.svg" alt="">
	 * <p>
	 * When bufferingTimespan > openBufferEvery : overlapping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanGreaterThanOpenBufferEvery.svg" alt="">
	 * <p>
	 * When bufferingTimespan == openBufferEvery : exact buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanEqualsOpenBufferEvery.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * It DOES NOT provide strong guarantees in the case of overlapping buffers, as elements
	 * might get discarded too early (from the first of two overlapping buffers for instance).
	 *
	 * @param bufferingTimespan the duration from buffer creation until a buffer is closed and emitted
	 * @param openBufferEvery the interval at which to create a new buffer
	 *
	 * @return a microbatched {@link Flux} of {@link List} delimited by the given period openBufferEvery and sized by bufferingTimespan
	 */
	public final Flux<List<T>> buffer(Duration bufferingTimespan, Duration openBufferEvery) {
		return buffer(bufferingTimespan, openBufferEvery, Schedulers.parallel());
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted by
	 * the returned {@link Flux} every {@code bufferingTimespan}, as measured on the provided {@link Scheduler}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param bufferingTimespan the duration from buffer creation until a buffer is closed and emitted
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a microbatched {@link Flux} of {@link List} delimited by the given period
	 */
	public final Flux<List<T>> buffer(Duration bufferingTimespan, Scheduler timer) {
		return buffer(interval(bufferingTimespan, timer));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers created at a given
	 * {@code openBufferEvery} period, as measured on the provided {@link Scheduler}. Each
	 * buffer will last until the {@code bufferingTimespan} has elapsed (also measured on the scheduler),
	 * thus emitting the bucket in the resulting {@link Flux}.
	 * <p>
	 * When bufferingTimespan < openBufferEvery : dropping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanLessThanOpenBufferEvery.svg" alt="">
	 * <p>
	 * When bufferingTimespan > openBufferEvery : overlapping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanGreaterThanOpenBufferEvery.svg" alt="">
	 * <p>
	 * When bufferingTimespan == openBufferEvery : exact buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanEqualsOpenBufferEvery.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * It DOES NOT provide strong guarantees in the case of overlapping buffers, as elements
	 * might get discarded too early (from the first of two overlapping buffers for instance).
	 *
	 * @param bufferingTimespan the duration from buffer creation until a buffer is closed and emitted
	 * @param openBufferEvery the interval at which to create a new buffer
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a microbatched {@link Flux} of {@link List} delimited by the given period openBufferEvery and sized by bufferingTimespan
	 */
	public final Flux<List<T>> buffer(Duration bufferingTimespan, Duration openBufferEvery, Scheduler timer) {
		if (bufferingTimespan.equals(openBufferEvery)) {
			return buffer(bufferingTimespan, timer);
		}
		return bufferWhen(interval(Duration.ZERO, openBufferEvery, timer), aLong -> Mono
				.delay(bufferingTimespan, timer));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted
	 * by the returned {@link Flux} each time the buffer reaches a maximum size OR the
	 * maxTime {@link Duration} elapses.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 *
	 * @return a microbatched {@link Flux} of {@link List} delimited by given size or a given period timeout
	 */
	public final Flux<List<T>> bufferTimeout(int maxSize, Duration maxTime) {
		return bufferTimeout(maxSize, maxTime, listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers that
	 * will be emitted by the returned {@link Flux} each time the buffer reaches a maximum
	 * size OR the maxTime {@link Duration} elapses.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a microbatched {@link Flux} of {@link Collection} delimited by given size or a given period timeout
	 */
	public final <C extends Collection<? super T>> Flux<C> bufferTimeout(int maxSize, Duration maxTime, Supplier<C> bufferSupplier) {
		return bufferTimeout(maxSize, maxTime, Schedulers.parallel(),
				bufferSupplier);
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted
	 * by the returned {@link Flux} each time the buffer reaches a maximum size OR the
	 * maxTime {@link Duration} elapses, as measured on the provided {@link Scheduler}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a microbatched {@link Flux} of {@link List} delimited by given size or a given period timeout
	 */
	public final Flux<List<T>> bufferTimeout(int maxSize, Duration maxTime, Scheduler timer) {
		return bufferTimeout(maxSize, maxTime, timer, listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers that
	 * will be emitted by the returned {@link Flux} each time the buffer reaches a maximum
	 * size OR the maxTime {@link Duration} elapses, as measured on the provided {@link Scheduler}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a microbatched {@link Flux} of {@link Collection} delimited by given size or a given period timeout
	 */
	public final  <C extends Collection<? super T>> Flux<C> bufferTimeout(int maxSize, Duration maxTime,
			Scheduler timer, Supplier<C> bufferSupplier) {
		return onAssembly(new FluxBufferTimeout<>(this, maxSize, maxTime.toNanos(), TimeUnit.NANOSECONDS, timer, bufferSupplier,
				false));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted
	 * by the returned {@link Flux} each time the buffer reaches a maximum size OR the
	 * maxTime {@link Duration} elapses.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 * @param fairBackpressure If {@code true}, prefetches {@code maxSize * 4} from upstream and replenishes the buffer when the downstream demand is satisfactory.
	 *                         When {@code false}, no prefetching takes place and a single buffer is always ready to be pushed downstream.
	 *
	 * @return a microbatched {@link Flux} of {@link List} delimited by given size or a given period timeout
	 */
	public final Flux<List<T>> bufferTimeout(int maxSize,
			Duration maxTime, boolean fairBackpressure) {
		return bufferTimeout(maxSize, maxTime, Schedulers.parallel(),
				listSupplier(), fairBackpressure);
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted
	 * by the returned {@link Flux} each time the buffer reaches a maximum size OR the
	 * maxTime {@link Duration} elapses, as measured on the provided {@link Scheduler}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 * @param fairBackpressure If {@code true}, prefetches {@code maxSize * 4} from upstream and replenishes the buffer when the downstream demand is satisfactory.
	 *                         When {@code false}, no prefetching takes place and a single buffer is always ready to be pushed downstream.
	 *
	 * @return a microbatched {@link Flux} of {@link List} delimited by given size or a given period timeout
	 */
	public final Flux<List<T>> bufferTimeout(int maxSize, Duration maxTime,
			Scheduler timer, boolean fairBackpressure) {
		return bufferTimeout(maxSize, maxTime, timer, listSupplier(), fairBackpressure);
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers that
	 * will be emitted by the returned {@link Flux} each time the buffer reaches a maximum
	 * size OR the maxTime {@link Duration} elapses.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 * @param fairBackpressure If {@code true}, prefetches {@code maxSize * 4} from upstream and replenishes the buffer when the downstream demand is satisfactory.
	 *                         When {@code false}, no prefetching takes place and a single buffer is always ready to be pushed downstream.
	 *
	 * @return a microbatched {@link Flux} of {@link Collection} delimited by given size or a given period timeout
	 */
	public final  <C extends Collection<? super T>> Flux<C> bufferTimeout(int maxSize, Duration maxTime,
			Supplier<C> bufferSupplier, boolean fairBackpressure) {
		return bufferTimeout(maxSize, maxTime, Schedulers.parallel(), bufferSupplier,
				fairBackpressure);
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers that
	 * will be emitted by the returned {@link Flux} each time the buffer reaches a maximum
	 * size OR the maxTime {@link Duration} elapses, as measured on the provided {@link Scheduler}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 * @param fairBackpressure If {@code true}, prefetches {@code maxSize * 4} from upstream and replenishes the buffer when the downstream demand is satisfactory.
	 *                         When {@code false}, no prefetching takes place and a single buffer is always ready to be pushed downstream.
	 *
	 * @return a microbatched {@link Flux} of {@link Collection} delimited by given size or a given period timeout
	 */
	public final  <C extends Collection<? super T>> Flux<C> bufferTimeout(int maxSize, Duration maxTime,
			Scheduler timer, Supplier<C> bufferSupplier, boolean fairBackpressure) {
		return onAssembly(new FluxBufferTimeout<>(this, maxSize, maxTime.toNanos(), TimeUnit.NANOSECONDS, timer, bufferSupplier,
				fairBackpressure));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted by
	 * the resulting {@link Flux} each time the given predicate returns true. Note that
	 * the element that triggers the predicate to return true (and thus closes a buffer)
	 * is included as last element in the emitted buffer.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferUntil.svg" alt="">
	 * <p>
	 * On completion, if the latest buffer is non-empty and has not been closed it is
	 * emitted. However, such a "partial" buffer isn't emitted in case of onError
	 * termination.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param predicate a predicate that triggers the next buffer when it becomes true.
	 *
	 * @return a microbatched {@link Flux} of {@link List}
	 */
	public final Flux<List<T>> bufferUntil(Predicate<? super T> predicate) {
		return onAssembly(new FluxBufferPredicate<>(this, predicate,
				listSupplier(), FluxBufferPredicate.Mode.UNTIL));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted by
	 * the resulting {@link Flux} each time the given predicate returns true. Note that
	 * the buffer into which the element that triggers the predicate to return true
	 * (and thus closes a buffer) is included depends on the {@code cutBefore} parameter:
	 * set it to true to include the boundary element in the newly opened buffer, false to
	 * include it in the closed buffer (as in {@link #bufferUntil(Predicate)}).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferUntilWithCutBefore.svg" alt="">
	 * <p>
	 * On completion, if the latest buffer is non-empty and has not been closed it is
	 * emitted. However, such a "partial" buffer isn't emitted in case of onError
	 * termination.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param predicate a predicate that triggers the next buffer when it becomes true.
	 * @param cutBefore set to true to include the triggering element in the new buffer rather than the old.
	 *
	 * @return a microbatched {@link Flux} of {@link List}
	 */
	public final Flux<List<T>> bufferUntil(Predicate<? super T> predicate, boolean cutBefore) {
		return onAssembly(new FluxBufferPredicate<>(this, predicate, listSupplier(),
				cutBefore ? FluxBufferPredicate.Mode.UNTIL_CUT_BEFORE
						  : FluxBufferPredicate.Mode.UNTIL));
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another) into multiple {@link List} buffers that will be emitted by the
	 * resulting {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferUntilChanged.svg" alt="">
	 * <p>
	 *
	 * @return a microbatched {@link Flux} of {@link List}
	 */
	public final Flux<List<T>> bufferUntilChanged() {
		return bufferUntilChanged(identityFunction());
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another), as compared by a key extracted through the user provided {@link
	 * Function}, into multiple {@link List} buffers that will be emitted by the
	 * resulting {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferUntilChangedWithKey.svg" alt="">
	 * <p>
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @return a microbatched {@link Flux} of {@link List}
	 */
	public final <V> Flux<List<T>> bufferUntilChanged(Function<? super T, ? extends V> keySelector) {
		return bufferUntilChanged(keySelector, equalPredicate());
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another), as compared by a key extracted through the user provided {@link
	 * Function} and compared using a supplied {@link BiPredicate}, into multiple
	 * {@link List} buffers that will be emitted by the resulting {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferUntilChangedWithKey.svg" alt="">
	 * <p>
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @param keyComparator predicate used to compare keys
	 * @return a microbatched {@link Flux} of {@link List}
	 */
	public final <V> Flux<List<T>> bufferUntilChanged(Function<? super T, ? extends V> keySelector,
			BiPredicate<? super V, ? super V> keyComparator) {
		return Flux.defer(() -> bufferUntil(new FluxBufferPredicate.ChangedPredicate<T, V>(keySelector,
				keyComparator), true));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted by
	 * the resulting {@link Flux}. Each buffer continues aggregating values while the
	 * given predicate returns true, and a new buffer is created as soon as the
	 * predicate returns false... Note that the element that triggers the predicate
	 * to return false (and thus closes a buffer) is NOT included in any emitted buffer.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWhile.svg" alt="">
	 * <p>
	 * On completion, if the latest buffer is non-empty and has not been closed it is
	 * emitted. However, such a "partial" buffer isn't emitted in case of onError
	 * termination.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal,
	 * as well as the buffer-triggering element.
	 *
	 * @param predicate a predicate that triggers the next buffer when it becomes false.
	 *
	 * @return a microbatched {@link Flux} of {@link List}
	 */
	public final Flux<List<T>> bufferWhile(Predicate<? super T> predicate) {
		return onAssembly(new FluxBufferPredicate<>(this, predicate,
				listSupplier(), FluxBufferPredicate.Mode.WHILE));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers started each time an opening
	 * companion {@link Publisher} emits. Each buffer will last until the corresponding
	 * closing companion {@link Publisher} emits, thus releasing the buffer to the resulting {@link Flux}.
	 * <p>
	 * When Open signal is strictly not overlapping Close signal : dropping buffers (see green marbles in diagram below).
	 * <p>
	 * When Open signal is strictly more frequent than Close signal : overlapping buffers (see second and third buffers in diagram below).
	 * <p>
	 * When Open signal is exactly coordinated with Close signal : exact buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWhen.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * It DOES NOT provide strong guarantees in the case of overlapping buffers, as elements
	 * might get discarded too early (from the first of two overlapping buffers for instance).
	 *
	 * @param bucketOpening a companion {@link Publisher} to subscribe for buffer creation signals.
	 * @param closeSelector a factory that, given a buffer opening signal, returns a companion
	 * {@link Publisher} to subscribe to for buffer closure and emission signals.
	 * @param <U> the element type of the buffer-opening sequence
	 * @param <V> the element type of the buffer-closing sequence
	 *
	 * @return a microbatched {@link Flux} of {@link List} delimited by an opening {@link Publisher} and a relative
	 * closing {@link Publisher}
	 */
	public final <U, V> Flux<List<T>> bufferWhen(Publisher<U> bucketOpening,
			Function<? super U, ? extends Publisher<V>> closeSelector) {
		return bufferWhen(bucketOpening, closeSelector, listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers started each time an opening
	 * companion {@link Publisher} emits. Each buffer will last until the corresponding
	 * closing companion {@link Publisher} emits, thus releasing the buffer to the resulting {@link Flux}.
	 * <p>
	 * When Open signal is strictly not overlapping Close signal : dropping buffers (see green marbles in diagram below).
	 * <p>
	 * When Open signal is strictly more frequent than Close signal : overlapping buffers (see second and third buffers in diagram below).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWhenWithSupplier.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * It DOES NOT provide strong guarantees in the case of overlapping buffers, as elements
	 * might get discarded too early (from the first of two overlapping buffers for instance).
	 *
	 * @param bucketOpening a companion {@link Publisher} to subscribe for buffer creation signals.
	 * @param closeSelector a factory that, given a buffer opening signal, returns a companion
	 * {@link Publisher} to subscribe to for buffer closure and emission signals.
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <U> the element type of the buffer-opening sequence
	 * @param <V> the element type of the buffer-closing sequence
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a microbatched {@link Flux} of {@link Collection} delimited by an opening {@link Publisher} and a relative
	 * closing {@link Publisher}
	 */
	public final <U, V, C extends Collection<? super T>> Flux<C> bufferWhen(Publisher<U> bucketOpening,
			Function<? super U, ? extends Publisher<V>> closeSelector, Supplier<C> bufferSupplier) {
		return onAssembly(new FluxBufferWhen<>(this, bucketOpening, closeSelector,
				bufferSupplier, Queues.unbounded(Queues.XS_BUFFER_SIZE)));
	}

	/**
	 * Turn this {@link Flux} into a hot source and cache last emitted signals for further {@link Subscriber}. Will
	 * retain an unbounded volume of onNext signals. Completion and Error will also be
	 * replayed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/cacheForFlux.svg" alt="">
	 *
	 * @return a replaying {@link Flux}
	 */
	public final Flux<T> cache() {
		return cache(Integer.MAX_VALUE);
	}

	/**
	 * Turn this {@link Flux} into a hot source and cache last emitted signals for further {@link Subscriber}.
	 * Will retain up to the given history size onNext signals. Completion and Error will also be
	 * replayed.
	 * <p>
	 *     Note that {@code cache(0)} will only cache the terminal signal without
	 *     expiration.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/cacheWithHistoryLimitForFlux.svg" alt="">
	 *
	 * @param history number of elements retained in cache
	 *
	 * @return a replaying {@link Flux}
	 *
	 */
	public final Flux<T> cache(int history) {
		return replay(history).autoConnect();
	}

	/**
	 * Turn this {@link Flux} into a hot source and cache last emitted signals for further
	 * {@link Subscriber}. Will retain an unbounded history but apply a per-item expiry timeout
	 * <p>
	 *   Completion and Error will also be replayed until {@code ttl} triggers in which case
	 *   the next {@link Subscriber} will start over a new subscription.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/cacheWithTtlForFlux.svg" alt="">
	 *
	 * @param ttl Time-to-live for each cached item and post termination.
	 *
	 * @return a replaying {@link Flux}
	 */
	public final Flux<T> cache(Duration ttl) {
		return cache(ttl, Schedulers.parallel());
	}

	/**
	 * Turn this {@link Flux} into a hot source and cache last emitted signals for further
	 * {@link Subscriber}. Will retain an unbounded history but apply a per-item expiry timeout
	 * <p>
	 *   Completion and Error will also be replayed until {@code ttl} triggers in which case
	 *   the next {@link Subscriber} will start over a new subscription.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/cacheWithTtlForFlux.svg" alt="">
	 *
	 * @param ttl Time-to-live for each cached item and post termination.
	 * @param timer the {@link Scheduler} on which to measure the duration.
	 *
	 * @return a replaying {@link Flux}
	 */
	public final Flux<T> cache(Duration ttl, Scheduler timer) {
		return cache(Integer.MAX_VALUE, ttl, timer);
	}

	/**
	 * Turn this {@link Flux} into a hot source and cache last emitted signals for further
	 * {@link Subscriber}. Will retain up to the given history size and apply a per-item expiry
	 * timeout.
	 * <p>
	 *   Completion and Error will also be replayed until {@code ttl} triggers in which case
	 *   the next {@link Subscriber} will start over a new subscription.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/cacheWithTtlAndMaxLimitForFlux.svg" alt="">
	 *
	 * @param history number of elements retained in cache
	 * @param ttl Time-to-live for each cached item and post termination.
	 *
	 * @return a replaying {@link Flux}
	 */
	public final Flux<T> cache(int history, Duration ttl) {
		return cache(history, ttl, Schedulers.parallel());
	}

	/**
	 * Turn this {@link Flux} into a hot source and cache last emitted signals for further
	 * {@link Subscriber}. Will retain up to the given history size and apply a per-item expiry
	 * timeout.
	 * <p>
	 *   Completion and Error will also be replayed until {@code ttl} triggers in which case
	 *   the next {@link Subscriber} will start over a new subscription.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/cacheWithTtlAndMaxLimitForFlux.svg"
	 * alt="">
	 *
	 * @param history number of elements retained in cache
	 * @param ttl Time-to-live for each cached item and post termination.
	 * @param timer the {@link Scheduler} on which to measure the duration.
	 *
	 * @return a replaying {@link Flux}
	 */
	public final Flux<T> cache(int history, Duration ttl, Scheduler timer) {
		return replay(history, ttl, timer).autoConnect();
	}

	/**
	 * Cast the current {@link Flux} produced type into a target produced type.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/castForFlux.svg" alt="">
	 *
	 * @param <E> the {@link Flux} output type
	 * @param clazz the target class to cast to
	 *
	 * @return a casted {@link Flux}
	 */
	public final <E> Flux<E> cast(Class<E> clazz) {
		Objects.requireNonNull(clazz, "clazz");
		return map(clazz::cast);
	}

	/**
	 * Prepare this {@link Flux} so that subscribers will cancel from it on a
	 * specified
	 * {@link Scheduler}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/cancelOnForFlux.svg" alt="">
	 *
	 * @param scheduler the {@link Scheduler} to signal cancel  on
	 *
	 * @return a scheduled cancel {@link Flux}
	 */
	public final Flux<T> cancelOn(Scheduler scheduler) {
		return onAssembly(new FluxCancelOn<>(this, scheduler));
	}

	/**
	 * Activate traceback (full assembly tracing) for this particular {@link Flux}, in case of an error
	 * upstream of the checkpoint. Tracing incurs the cost of an exception stack trace
	 * creation.
	 * <p>
	 * It should be placed towards the end of the reactive chain, as errors
	 * triggered downstream of it cannot be observed and augmented with the traceback.
	 * <p>
	 * The traceback is attached to the error as a {@link Throwable#getSuppressed() suppressed exception}.
	 * As such, if the error is a {@link Exceptions#isMultiple(Throwable) composite one}, the traceback
	 * would appear as a component of the composite. In any case, the traceback nature can be detected via
	 * {@link Exceptions#isTraceback(Throwable)}.
	 *
	 * @return the assembly tracing {@link Flux}.
	 */
	public final Flux<T> checkpoint() {
		return checkpoint(null, true);
	}

	/**
	 * Activate traceback (assembly marker) for this particular {@link Flux} by giving it a description that
	 * will be reflected in the assembly traceback in case of an error upstream of the
	 * checkpoint. Note that unlike {@link #checkpoint()}, this doesn't create a
	 * filled stack trace, avoiding the main cost of the operator.
	 * However, as a trade-off the description must be unique enough for the user to find
	 * out where this Flux was assembled. If you only want a generic description, and
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
	 * @return the assembly marked {@link Flux}
	 */
	public final Flux<T> checkpoint(String description) {
		return checkpoint(Objects.requireNonNull(description), false);
	}

	/**
	 * Activate traceback (full assembly tracing or the lighter assembly marking depending on the
	 * {@code forceStackTrace} option).
	 * <p>
	 * By setting the {@code forceStackTrace} parameter to {@literal true}, activate assembly
	 * tracing for this particular {@link Flux} and give it a description that
	 * will be reflected in the assembly traceback in case of an error upstream of the
	 * checkpoint. Note that unlike {@link #checkpoint(String)}, this will incur
	 * the cost of an exception stack trace creation. The description could for
	 * example be a meaningful name for the assembled flux or a wider correlation ID,
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
	 * @return the assembly marked {@link Flux}.
	 */
	public final Flux<T> checkpoint(@Nullable String description, boolean forceStackTrace) {
		final AssemblySnapshot stacktrace;
		if (!forceStackTrace) {
			stacktrace = new CheckpointLightSnapshot(description);
		}
		else {
			stacktrace = new CheckpointHeavySnapshot(description, Traces.callSiteSupplierFactory.get());
		}

		return new FluxOnAssembly<>(this, stacktrace);
	}

	/**
	 * Collect all elements emitted by this {@link Flux} into a user-defined container,
	 * by applying a collector {@link BiConsumer} taking the container and each element.
	 * The collected result will be emitted when this sequence completes, emitting the
	 * empty container if the sequence was empty.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/collect.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the container upon cancellation or error triggered by a data signal.
	 * Either the container type is a {@link Collection} (in which case individual elements are discarded)
	 * or not (in which case the entire container is discarded). In case the collector {@link BiConsumer} fails
	 * to accumulate an element, the container is discarded as above and the triggering element is also discarded.
	 *
	 * @param <E> the container type
	 * @param containerSupplier the supplier of the container instance for each Subscriber
	 * @param collector a consumer of both the container instance and the value being currently collected
	 *
	 * @return a {@link Mono} of the collected container on complete
	 *
	 */
	public final <E> Mono<E> collect(Supplier<E> containerSupplier, BiConsumer<E, ? super T> collector) {
		return Mono.onAssembly(new MonoCollect<>(this, containerSupplier, collector));
	}

	/**
	 * Collect all elements emitted by this {@link Flux} into a container,
	 * by applying a Java 8 Stream API {@link Collector}
	 * The collected result will be emitted when this sequence completes, emitting
	 * the empty container if the sequence was empty.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/collectWithCollector.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the intermediate container (see {@link Collector#supplier()}) upon
	 * cancellation, error or exception while applying the {@link Collector#finisher()}. Either the container type
	 * is a {@link Collection} (in which case individual elements are discarded) or not (in which case the entire
	 * container is discarded). In case the accumulator {@link BiConsumer} of the collector fails to accumulate
	 * an element into the intermediate container, the container is discarded as above and the triggering element
	 * is also discarded.
	 *
	 * @param collector the {@link Collector}
	 * @param <A> The mutable accumulation type
	 * @param <R> the container type
	 *
	 * @return a {@link Mono} of the collected container on complete
	 *
	 */
	public final <R, A> Mono<R> collect(Collector<? super T, A, ? extends R> collector) {
		return Mono.onAssembly(new MonoStreamCollector<>(this, collector));
	}

	/**
	 * Collect all elements emitted by this {@link Flux} into a {@link List} that is
	 * emitted by the resulting {@link Mono} when this sequence completes, emitting the
	 * empty {@link List} if the sequence was empty.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/collectList.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the elements in the {@link List} upon
	 * cancellation or error triggered by a data signal.
	 *
	 * @return a {@link Mono} of a {@link List} of all values from this {@link Flux}
	 */
	public final Mono<List<T>> collectList() {
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
				return Mono.onAssembly(new MonoCallable<>(() -> {
					List<T> list = Flux.<T>listSupplier().get();
					if (v != null) {
						list.add(v);
					}
					return list;
				}));

			}
			@SuppressWarnings("unchecked")
			Callable<T> thiz = (Callable<T>)this;
			return Mono.onAssembly(new MonoCallable<>(() -> {
				List<T> list = Flux.<T>listSupplier().get();
				T u = thiz.call();
				if (u != null) {
					list.add(u);
				}
				return list;
			}));
		}
		return Mono.onAssembly(new MonoCollectList<>(this));
	}

	/**
	 * Collect all elements emitted by this {@link Flux} into a hashed {@link Map} that is
	 * emitted by the resulting {@link Mono} when this sequence completes, emitting the
	 * empty {@link Map} if the sequence was empty.
	 * The key is extracted from each element by applying the {@code keyExtractor}
	 * {@link Function}. In case several elements map to the same key, the associated value
	 * will be the most recently emitted element.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/collectMapWithKeyExtractor.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the whole {@link Map} upon cancellation or error
	 * triggered by a data signal, so discard handlers will have to unpack the map.
	 *
	 * @param keyExtractor a {@link Function} to map elements to a key for the {@link Map}
	 * @param <K> the type of the key extracted from each source element
	 *
	 * @return a {@link Mono} of a {@link Map} of key-element pairs (only including latest
	 * element in case of key conflicts)
	 */
	public final <K> Mono<Map<K, T>> collectMap(Function<? super T, ? extends K> keyExtractor) {
		return collectMap(keyExtractor, identityFunction());
	}

	/**
	 * Collect all elements emitted by this {@link Flux} into a hashed {@link Map} that is
	 * emitted by the resulting {@link Mono} when this sequence completes, emitting the
	 * empty {@link Map} if the sequence was empty.
	 * The key is extracted from each element by applying the {@code keyExtractor}
	 * {@link Function}, and the value is extracted by the {@code valueExtractor} Function.
	 * In case several elements map to the same key, the associated value will be derived
	 * from the most recently emitted element.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/collectMapWithKeyAndValueExtractors.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the whole {@link Map} upon cancellation or error
	 * triggered by a data signal, so discard handlers will have to unpack the map.
	 *
	 * @param keyExtractor a {@link Function} to map elements to a key for the {@link Map}
	 * @param valueExtractor a {@link Function} to map elements to a value for the {@link Map}
	 *
	 * @param <K> the type of the key extracted from each source element
	 * @param <V> the type of the value extracted from each source element
	 *
	 * @return a {@link Mono} of a {@link Map} of key-element pairs (only including latest
	 * element's value in case of key conflicts)
	 */
	public final <K, V> Mono<Map<K, V>> collectMap(Function<? super T, ? extends K> keyExtractor,
			Function<? super T, ? extends V> valueExtractor) {
		return collectMap(keyExtractor, valueExtractor, () -> new HashMap<>());
	}

	/**
	 * Collect all elements emitted by this {@link Flux} into a user-defined {@link Map} that is
	 * emitted by the resulting {@link Mono} when this sequence completes, emitting the
	 * empty {@link Map} if the sequence was empty.
	 * The key is extracted from each element by applying the {@code keyExtractor}
	 * {@link Function}, and the value is extracted by the {@code valueExtractor} Function.
	 * In case several elements map to the same key, the associated value will be derived
	 * from the most recently emitted element.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/collectMapWithKeyAndValueExtractors.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the whole {@link Map} upon cancellation or error
	 * triggered by a data signal, so discard handlers will have to unpack the map.
	 *
	 * @param keyExtractor a {@link Function} to map elements to a key for the {@link Map}
	 * @param valueExtractor a {@link Function} to map elements to a value for the {@link Map}
	 * @param mapSupplier a {@link Map} factory called for each {@link Subscriber}
	 *
	 * @param <K> the type of the key extracted from each source element
	 * @param <V> the type of the value extracted from each source element
	 *
	 * @return a {@link Mono} of a {@link Map} of key-value pairs (only including latest
	 * element's value in case of key conflicts)
	 */
	public final <K, V> Mono<Map<K, V>> collectMap(
			final Function<? super T, ? extends K> keyExtractor,
			final Function<? super T, ? extends V> valueExtractor,
			Supplier<Map<K, V>> mapSupplier) {
		Objects.requireNonNull(keyExtractor, "Key extractor is null");
		Objects.requireNonNull(valueExtractor, "Value extractor is null");
		Objects.requireNonNull(mapSupplier, "Map supplier is null");
		return collect(mapSupplier, (m, d) -> m.put(keyExtractor.apply(d), valueExtractor.apply(d)));
	}

	/**
	 * Collect all elements emitted by this {@link Flux} into a {@link Map multimap} that is
	 * emitted by the resulting {@link Mono} when this sequence completes, emitting the
	 * empty {@link Map multimap} if the sequence was empty.
	 * The key is extracted from each element by applying the {@code keyExtractor}
	 * {@link Function}, and every element mapping to the same key is stored in the {@link List}
	 * associated to said key.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/collectMultiMapWithKeyExtractor.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the whole {@link Map} upon cancellation or error
	 * triggered by a data signal, so discard handlers will have to unpack the list values in the map.
	 *
	 * @param keyExtractor a {@link Function} to map elements to a key for the {@link Map}
	 *
	 * @param <K> the type of the key extracted from each source element
	 *
	 * @return a {@link Mono} of a {@link Map} of key-List(elements) pairs
	 */
	public final <K> Mono<Map<K, Collection<T>>> collectMultimap(Function<? super T, ? extends K> keyExtractor) {
		return collectMultimap(keyExtractor, identityFunction());
	}

	/**
	 * Collect all elements emitted by this {@link Flux} into a {@link Map multimap} that is
	 * emitted by the resulting {@link Mono} when this sequence completes, emitting the
	 * empty {@link Map multimap} if the sequence was empty.
	 * The key is extracted from each element by applying the {@code keyExtractor}
	 * {@link Function}, and every element mapping to the same key is converted by the
	 * {@code valueExtractor} Function to a value stored in the {@link List} associated to
	 * said key.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/collectMultiMapWithKeyAndValueExtractors.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the whole {@link Map} upon cancellation or error
	 * triggered by a data signal, so discard handlers will have to unpack the list values in the map.
	 *
	 * @param keyExtractor a {@link Function} to map elements to a key for the {@link Map}
	 * @param valueExtractor a {@link Function} to map elements to a value for the {@link Map}
	 *
	 * @param <K> the type of the key extracted from each source element
	 * @param <V> the type of the value extracted from each source element
	 *
	 * @return a {@link Mono} of a {@link Map} of key-List(values) pairs
	 */
	public final <K, V> Mono<Map<K, Collection<V>>> collectMultimap(Function<? super T, ? extends K> keyExtractor,
			Function<? super T, ? extends V> valueExtractor) {
		return collectMultimap(keyExtractor, valueExtractor, () -> new HashMap<>());
	}

	/**
	 * Collect all elements emitted by this {@link Flux} into a user-defined {@link Map multimap} that is
	 * emitted by the resulting {@link Mono} when this sequence completes, emitting the
	 * empty {@link Map multimap} if the sequence was empty.
	 * The key is extracted from each element by applying the {@code keyExtractor}
	 * {@link Function}, and every element mapping to the same key is converted by the
	 * {@code valueExtractor} Function to a value stored in the {@link Collection} associated to
	 * said key.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/collectMultiMapWithKeyAndValueExtractors.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the whole {@link Map} upon cancellation or error
	 * triggered by a data signal, so discard handlers will have to unpack the list values in the map.
	 *
	 * @param keyExtractor a {@link Function} to map elements to a key for the {@link Map}
	 * @param valueExtractor a {@link Function} to map elements to a value for the {@link Map}
	 * @param mapSupplier a multimap ({@link Map} of {@link Collection}) factory called
	 * for each {@link Subscriber}
	 *
	 * @param <K> the type of the key extracted from each source element
	 * @param <V> the type of the value extracted from each source element
	 *
	 * @return a {@link Mono} of a {@link Map} of key-Collection(values) pairs
	 *
	 */
	public final <K, V> Mono<Map<K, Collection<V>>> collectMultimap(
			final Function<? super T, ? extends K> keyExtractor,
			final Function<? super T, ? extends V> valueExtractor,
			Supplier<Map<K, Collection<V>>> mapSupplier) {
		Objects.requireNonNull(keyExtractor, "Key extractor is null");
		Objects.requireNonNull(valueExtractor, "Value extractor is null");
		Objects.requireNonNull(mapSupplier, "Map supplier is null");
		return collect(mapSupplier, (m, d) -> {
			K key = keyExtractor.apply(d);
			Collection<V> values = m.computeIfAbsent(key, k -> new ArrayList<>());
			values.add(valueExtractor.apply(d));
		});
	}

	/**
	 * Collect all elements emitted by this {@link Flux} until this sequence completes,
	 * and then sort them in natural order into a {@link List} that is emitted by the
	 * resulting {@link Mono}. If the sequence was empty, empty {@link List} will be emitted.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/collectSortedList.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator is based on {@link #collectList()}, and as such discards the
	 * elements in the {@link List} individually upon cancellation or error triggered by a data signal.
	 *
	 * @return a {@link Mono} of a sorted {@link List} of all values from this {@link Flux}, in natural order
	 */
	public final Mono<List<T>> collectSortedList() {
		return collectSortedList(null);
	}

	/**
	 * Collect all elements emitted by this {@link Flux} until this sequence completes,
	 * and then sort them using a {@link Comparator} into a {@link List} that is emitted
	 * by the resulting {@link Mono}. If the sequence was empty, empty {@link List} will be emitted.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/collectSortedListWithComparator.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator is based on {@link #collectList()}, and as such discards the
	 * elements in the {@link List} individually upon cancellation or error triggered by a data signal.
	 *
	 * @param comparator a {@link Comparator} to sort the items of this sequences
	 *
	 * @return a {@link Mono} of a sorted {@link List} of all values from this {@link Flux}
	 */
	public final Mono<List<T>> collectSortedList(@Nullable Comparator<? super T> comparator) {
		return collectList().doOnNext(list -> {
			// Note: this assumes the list emitted by buffer() is mutable
			list.sort(comparator);
		});
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, sequentially and
	 * preserving order using concatenation.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMap(Function) flatMap} and {@link #flatMapSequential(Function) flatMapSequential}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator waits for one
	 *     inner to complete before generating the next one and subscribing to it.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator naturally preserves
	 *     the same order as the source elements, concatenating the inners from each source
	 *     element sequentially.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (concatenation).</li>
	 * </ul>
	 *
	 * <p>
	 * Errors will immediately short circuit current concat backlog.
	 * Note that no prefetching is done on the source, which gets requested only if there
	 * is downstream demand.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMap.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param mapper the function to transform this sequence of T into concatenated sequences of V
	 * @param <V> the produced concatenated type
	 *
	 * @return a concatenated {@link Flux}
	 */
	public final <V> Flux<V> concatMap(Function<? super T, ? extends Publisher<? extends V>> mapper) {
		return onAssembly(new FluxConcatMapNoPrefetch<>(this, mapper, FluxConcatMap.ErrorMode.IMMEDIATE));
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, sequentially and
	 * preserving order using concatenation.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMap(Function) flatMap} and {@link #flatMapSequential(Function) flatMapSequential}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator waits for one
	 *     inner to complete before generating the next one and subscribing to it.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator naturally preserves
	 *     the same order as the source elements, concatenating the inners from each source
	 *     element sequentially.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (concatenation).</li>
	 * </ul>
	 *
	 * <p>
	 * Errors will immediately short circuit current concat backlog. The prefetch argument
	 * allows to give an arbitrary prefetch size to the upstream source, or to disable
	 * prefetching with {@code 0}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMap.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param mapper the function to transform this sequence of T into concatenated sequences of V
	 * @param prefetch the number of values to prefetch from upstream source, or {@code 0} to disable prefetching
	 * @param <V> the produced concatenated type
	 *
	 * @return a concatenated {@link Flux}
	 */
	public final <V> Flux<V> concatMap(Function<? super T, ? extends Publisher<? extends V>> mapper, int prefetch) {
		if (prefetch == 0) {
			return onAssembly(new FluxConcatMapNoPrefetch<>(this, mapper, FluxConcatMap.ErrorMode.IMMEDIATE));
		}
		return onAssembly(new FluxConcatMap<>(this, mapper, Queues.get(prefetch), prefetch,
				FluxConcatMap.ErrorMode.IMMEDIATE));
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, sequentially and
	 * preserving order using concatenation.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMap(Function) flatMap} and {@link #flatMapSequential(Function) flatMapSequential}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator waits for one
	 *     inner to complete before generating the next one and subscribing to it.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator naturally preserves
	 *     the same order as the source elements, concatenating the inners from each source
	 *     element sequentially.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (concatenation).</li>
	 * </ul>
	 *
	 * <p>
	 * Errors in the individual publishers will be delayed at the end of the whole concat
	 * sequence (possibly getting combined into a {@link Exceptions#isMultiple(Throwable) composite})
	 * if several sources error.
	 * Note that no prefetching is done on the source, which gets requested only if there
	 * is downstream demand.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMap.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param mapper the function to transform this sequence of T into concatenated sequences of V
	 * @param <V> the produced concatenated type
	 *
	 * @return a concatenated {@link Flux}
	 *
	 */
	public final <V> Flux<V> concatMapDelayError(Function<? super T, ? extends Publisher<? extends V>> mapper) {
		return concatMapDelayError(mapper, 0);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, sequentially and
	 * preserving order using concatenation.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMap(Function) flatMap} and {@link #flatMapSequential(Function) flatMapSequential}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator waits for one
	 *     inner to complete before generating the next one and subscribing to it.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator naturally preserves
	 *     the same order as the source elements, concatenating the inners from each source
	 *     element sequentially.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (concatenation).</li>
	 * </ul>
	 *
	 * <p>
	 * Errors in the individual publishers will be delayed at the end of the whole concat
	 * sequence (possibly getting combined into a {@link Exceptions#isMultiple(Throwable) composite})
	 * if several sources error.
	 * The prefetch argument allows to give an arbitrary prefetch size to the upstream source,
	 * or to disable prefetching with {@code 0}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMap.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param mapper the function to transform this sequence of T into concatenated sequences of V
	 * @param prefetch the number of values to prefetch from upstream source, or {@code 0} to disable prefetching
	 * @param <V> the produced concatenated type
	 *
	 * @return a concatenated {@link Flux}
	 *
	 */
	public final <V> Flux<V> concatMapDelayError(Function<? super T, ? extends Publisher<? extends V>> mapper, int prefetch) {
		return concatMapDelayError(mapper, true, prefetch);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, sequentially and
	 * preserving order using concatenation.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMap(Function) flatMap} and {@link #flatMapSequential(Function) flatMapSequential}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator waits for one
	 *     inner to complete before generating the next one and subscribing to it.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator naturally preserves
	 *     the same order as the source elements, concatenating the inners from each source
	 *     element sequentially.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (concatenation).</li>
	 * </ul>
	 *
	 * <p>
	 * Errors in the individual publishers will be delayed after the current concat
	 * backlog if delayUntilEnd is false or after all sources if delayUntilEnd is true.
	 * The prefetch argument allows to give an arbitrary prefetch size to the upstream source,
	 * or to disable prefetching with {@code 0}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMap.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param mapper the function to transform this sequence of T into concatenated sequences of V
	 * @param delayUntilEnd delay error until all sources have been consumed instead of
	 * after the current source
	 * @param prefetch the number of values to prefetch from upstream source, or {@code 0} to disable prefetching
	 * @param <V> the produced concatenated type
	 *
	 * @return a concatenated {@link Flux}
	 *
	 */
	public final <V> Flux<V> concatMapDelayError(Function<? super T, ? extends Publisher<? extends V>> mapper,
												 boolean delayUntilEnd, int prefetch) {
		FluxConcatMap.ErrorMode errorMode = delayUntilEnd ? FluxConcatMap.ErrorMode.END : FluxConcatMap.ErrorMode.BOUNDARY;
		if (prefetch == 0) {
			return onAssembly(new FluxConcatMapNoPrefetch<>(this, mapper, errorMode));
		}
		return onAssembly(new FluxConcatMap<>(this, mapper, Queues.get(prefetch), prefetch, errorMode));
	}

	/**
	 * Transform the items emitted by this {@link Flux} into {@link Iterable}, then flatten the elements from those by
	 * concatenating them into a single {@link Flux}. For each iterable, {@link Iterable#iterator()} will be called
	 * at least once and at most twice.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMapIterable.svg" alt="">
	 * <p>
	 * This operator inspects each {@link Iterable}'s {@link Spliterator} to assess if the iteration
	 * can be guaranteed to be finite (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 * Since the default Spliterator wraps the Iterator we can have two {@link Iterable#iterator()}
	 * calls per iterable. This second invocation is skipped on a {@link Collection} however, a type which is
	 * assumed to be always finite.
	 * <p>
	 * Note that unlike {@link #flatMap(Function)} and {@link #concatMap(Function)}, with Iterable there is
	 * no notion of eager vs lazy inner subscription. The content of the Iterables are all played sequentially.
	 * Thus {@code flatMapIterable} and {@code concatMapIterable} are equivalent offered as a discoverability
	 * improvement for users that explore the API with the concat vs flatMap expectation.
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator discards {@code T} elements it prefetched and, in
	 * some cases, attempts to discard remainder of the currently processed {@link Iterable} (if it can
	 * safely ensure the iterator is finite). Note that this means each {@link Iterable}'s {@link Iterable#iterator()}
	 * method could be invoked twice.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * (including when fusion is enabled). Exceptions thrown by the consumer are passed to
	 * the {@link #onErrorContinue(BiConsumer)} error consumer (the value consumer
	 * is not invoked, as the source element will be part of the sequence). The onNext
	 * signal is then propagated as normal.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N {@link Iterable}
	 * @param <R> the merged output sequence type
	 *
	 * @return a concatenation of the values from the Iterables obtained from each element in this {@link Flux}
	 */
	public final <R> Flux<R> concatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper) {
		return concatMapIterable(mapper, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Transform the items emitted by this {@link Flux} into {@link Iterable}, then flatten the emissions from those by
	 * concatenating them into a single {@link Flux}.
	 * The prefetch argument allows to give an arbitrary prefetch size to the upstream source.
	 * For each iterable, {@link Iterable#iterator()} will be called at least once and at most twice.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMapIterable.svg" alt="">
	 * <p>
	 * This operator inspects each {@link Iterable}'s {@link Spliterator} to assess if the iteration
	 * can be guaranteed to be finite (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 * Since the default Spliterator wraps the Iterator we can have two {@link Iterable#iterator()}
	 * calls per iterable. This second invocation is skipped on a {@link Collection} however, a type which is
	 * assumed to be always finite.
	 * <p>
	 * Note that unlike {@link #flatMap(Function)} and {@link #concatMap(Function)}, with Iterable there is
	 * no notion of eager vs lazy inner subscription. The content of the Iterables are all played sequentially.
	 * Thus {@code flatMapIterable} and {@code concatMapIterable} are equivalent offered as a discoverability
	 * improvement for users that explore the API with the concat vs flatMap expectation.
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator discards {@code T} elements it prefetched and, in
	 * some cases, attempts to discard remainder of the currently processed {@link Iterable} (if it can
	 * safely ensure the iterator is finite). Note that this means each {@link Iterable}'s {@link Iterable#iterator()}
	 * method could be invoked twice.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * (including when fusion is enabled). Exceptions thrown by the consumer are passed to
	 * the {@link #onErrorContinue(BiConsumer)} error consumer (the value consumer
	 * is not invoked, as the source element will be part of the sequence). The onNext
	 * signal is then propagated as normal.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N {@link Iterable}
	 * @param prefetch the number of values to request from the source upon subscription, to be transformed to {@link Iterable}
	 * @param <R> the merged output sequence type
	 *
	 * @return a concatenation of the values from the Iterables obtained from each element in this {@link Flux}
	 */
	public final <R> Flux<R> concatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper,
			int prefetch) {
		return onAssembly(new FluxFlattenIterable<>(this, mapper, prefetch,
				Queues.get(prefetch)));
	}

	/**
	 * Concatenate emissions of this {@link Flux} with the provided {@link Publisher} (no interleave).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatWithForFlux.svg" alt="">
	 *
	 * @param other the {@link Publisher} sequence to concat after this {@link Flux}
	 *
	 * @return a concatenated {@link Flux}
	 */
	public final Flux<T> concatWith(Publisher<? extends T> other) {
		if (this instanceof FluxConcatArray) {
			FluxConcatArray<T> fluxConcatArray = (FluxConcatArray<T>) this;

			return fluxConcatArray.concatAdditionalSourceLast(other);
		}
		return concat(this, other);
	}

	/**
	 * If <a href="https://github.com/micrometer-metrics/context-propagation">context-propagation library</a>
	 * is on the classpath, this is a convenience shortcut to capture thread local values during the
	 * subscription phase and put them in the {@link Context} that is visible upstream of this operator.
	 * <p>
	 * As a result this operator should generally be used as close as possible to the end of
	 * the chain / subscription point.
	 * <p>
	 * If the {@link ContextView} visible upstream is not empty, a small subset of operators will automatically
	 * restore the context snapshot ({@link #handle(BiConsumer) handle}, {@link #tap(SignalListenerFactory) tap}).
	 * If context-propagation is not available at runtime, this operator simply returns the current {@link Flux}
	 * instance.
	 *
	 * @return a new {@link Flux} where context-propagation API has been used to capture entries and
	 * inject them into the {@link Context}
	 * @see #handle(BiConsumer)
	 * @see #tap(SignalListenerFactory)
	 */
	public final Flux<T> contextCapture() {
		if (!ContextPropagationSupport.isContextPropagationAvailable()) {
			return this;
		}
		if (ContextPropagationSupport.propagateContextToThreadLocals) {
			return onAssembly(new FluxContextWriteRestoringThreadLocals<>(
					this, ContextPropagation.contextCapture()
			));
		}
		return onAssembly(new FluxContextWrite<>(this, ContextPropagation.contextCapture()));
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
	 * @return a contextualized {@link Flux}
	 * @see ContextView
	 */
	public final Flux<T> contextWrite(ContextView contextToAppend) {
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
	 * @return a contextualized {@link Flux}
	 * @see Context
	 */
	public final Flux<T> contextWrite(Function<Context, Context> contextModifier) {
		if (ContextPropagationSupport.shouldPropagateContextToThreadLocals()) {
			return onAssembly(new FluxContextWriteRestoringThreadLocals<>(
					this, contextModifier
			));
		}
		return onAssembly(new FluxContextWrite<>(this, contextModifier));
	}

	private final Flux<T> contextWriteSkippingContextPropagation(ContextView contextToAppend) {
		return contextWriteSkippingContextPropagation(c -> c.putAll(contextToAppend));
	}

	private final Flux<T> contextWriteSkippingContextPropagation(Function<Context, Context> contextModifier) {
		return onAssembly(new FluxContextWrite<>(this, contextModifier));
	}

	/**
	 * Counts the number of values in this {@link Flux}.
	 * The count will be emitted when onComplete is observed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/count.svg" alt="">
	 *
	 * @return a new {@link Mono} of {@link Long} count
	 */
	public final Mono<Long> count() {
		return Mono.onAssembly(new MonoCount<>(this));
	}

	/**
	 * Provide a default unique value if this sequence is completed without any data
	 * <p>
	 * <img class="marble" src="doc-files/marbles/defaultIfEmpty.svg" alt="">
	 *
	 * @param defaultV the alternate value if this sequence is empty
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> defaultIfEmpty(T defaultV) {
		return onAssembly(new FluxDefaultIfEmpty<>(this, defaultV));
	}

	/**
	 * Delay each of this {@link Flux} elements ({@link Subscriber#onNext} signals)
	 * by a given {@link Duration}. Signals are delayed and continue on the
	 * {@link Schedulers#parallel() parallel} default Scheduler, but empty sequences or
	 * immediate error signals are not delayed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delayElements.svg" alt="">
	 *
	 * @param delay duration by which to delay each {@link Subscriber#onNext} signal
	 * @return a delayed {@link Flux}
	 * @see #delaySubscription(Duration) delaySubscription to introduce a delay at the beginning of the sequence only
	 */
	public final Flux<T> delayElements(Duration delay) {
		return delayElements(delay, Schedulers.parallel());
	}

	/**
	 * Delay each of this {@link Flux} elements ({@link Subscriber#onNext} signals)
	 * by a given {@link Duration}. Signals are delayed and continue on a user-specified
	 * {@link Scheduler}, but empty sequences or immediate error signals are not delayed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delayElements.svg" alt="">
	 *
	 * @param delay period to delay each {@link Subscriber#onNext} signal
	 * @param timer a time-capable {@link Scheduler} instance to delay each signal on
	 * @return a delayed {@link Flux}
	 */
	public final Flux<T> delayElements(Duration delay, Scheduler timer) {
		return delayUntil(d -> Mono.delay(delay, timer));
	}

	/**
	 * Shift this {@link Flux} forward in time by a given {@link Duration}.
	 * Unlike with {@link #delayElements(Duration)}, elements are shifted forward in time
	 * as they are emitted, always resulting in the delay between two elements being
	 * the same as in the source (only the first element is visibly delayed from the
	 * previous event, that is the subscription).
	 * Signals are delayed and continue on the {@link Schedulers#parallel() parallel}
	 * {@link Scheduler}, but empty sequences or immediate error signals are not delayed.
	 * <p>
	 * With this operator, a source emitting at 10Hz with a delaySequence {@link Duration}
	 * of 1s will still emit at 10Hz, with an initial "hiccup" of 1s.
	 * On the other hand, {@link #delayElements(Duration)} would end up emitting
	 * at 1Hz.
	 * <p>
	 * This is closer to {@link #delaySubscription(Duration)}, except the source
	 * is subscribed to immediately.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delaySequence.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements currently being delayed
	 * 	 * if the sequence is cancelled during the delay.
	 *
	 * @param delay {@link Duration} to shift the sequence by
	 *
	 * @return a shifted {@link Flux} emitting at the same frequency as the source
	 */
	public final Flux<T> delaySequence(Duration delay) {
		return delaySequence(delay, Schedulers.parallel());
	}

	/**
	 * Shift this {@link Flux} forward in time by a given {@link Duration}.
	 * Unlike with {@link #delayElements(Duration, Scheduler)}, elements are shifted forward in time
	 * as they are emitted, always resulting in the delay between two elements being
	 * the same as in the source (only the first element is visibly delayed from the
	 * previous event, that is the subscription).
	 * Signals are delayed and continue on a user-specified {@link Scheduler}, but empty
	 * sequences or immediate error signals are not delayed.
	 * <p>
	 * With this operator, a source emitting at 10Hz with a delaySequence {@link Duration}
	 * of 1s will still emit at 10Hz, with an initial "hiccup" of 1s.
	 * On the other hand, {@link #delayElements(Duration, Scheduler)} would end up emitting
	 * at 1Hz.
	 * <p>
	 * This is closer to {@link #delaySubscription(Duration, Scheduler)}, except the source
	 * is subscribed to immediately.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delaySequence.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements currently being delayed
	 * if the sequence is cancelled during the delay.
	 *
	 * @param delay {@link Duration} to shift the sequence by
	 * @param timer a time-capable {@link Scheduler} instance to delay signals on
	 *
	 * @return a shifted {@link Flux} emitting at the same frequency as the source
	 */
	public final Flux<T> delaySequence(Duration delay, Scheduler timer) {
		return onAssembly(new FluxDelaySequence<>(this, delay, timer));
	}

	/**
	 * Subscribe to this {@link Flux} and generate a {@link Publisher} from each of this
	 * Flux elements, each acting as a trigger for relaying said element.
	 * <p>
	 * That is to say, the resulting {@link Flux} delays each of its emission until the
	 * associated trigger Publisher terminates.
	 * <p>
	 * In case of an error either in the source or in a trigger, that error is propagated
	 * immediately downstream.
	 * Note that unlike with the {@link Mono#delayUntil(Function) Mono variant} there is
	 * no fusion of subsequent calls.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delayUntilForFlux.svg" alt="">
	 *
	 * @param triggerProvider a {@link Function} that maps each element into a
	 * {@link Publisher} whose termination will trigger relaying the value.
	 *
	 * @return this Flux, but with elements delayed until their derived publisher terminates.
	 */
	public final Flux<T> delayUntil(Function<? super T, ? extends Publisher<?>> triggerProvider) {
		return concatMap(v -> Mono.just(v)
		                          .delayUntil(triggerProvider));
	}

	/**
	 * Delay the {@link Flux#subscribe(Subscriber) subscription} to this {@link Flux} source until the given
	 * period elapses. The delay is introduced through the {@link Schedulers#parallel() parallel} default Scheduler.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delaySubscriptionForFlux.svg" alt="">
	 *
	 * @param delay duration before subscribing this {@link Flux}
	 *
	 * @return a delayed {@link Flux}
	 *
	 */
	public final Flux<T> delaySubscription(Duration delay) {
		return delaySubscription(delay, Schedulers.parallel());
	}

	/**
	 * Delay the {@link Flux#subscribe(Subscriber) subscription} to this {@link Flux} source until the given
	 * period elapses, as measured on the user-provided {@link Scheduler}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delaySubscriptionForFlux.svg" alt="">
	 *
	 * @param delay {@link Duration} before subscribing this {@link Flux}
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a delayed {@link Flux}
	 */
	public final Flux<T> delaySubscription(Duration delay, Scheduler timer) {
		return delaySubscription(Mono.delay(delay, timer));
	}

	/**
	 * Delay the {@link Flux#subscribe(Subscriber) subscription} to this {@link Flux}
	 * source until another {@link Publisher} signals a value or completes.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/delaySubscriptionWithPublisherForFlux.svg" alt="">
	 *
	 * @param subscriptionDelay a companion {@link Publisher} whose onNext/onComplete signal will trigger the {@link Flux#subscribe(Subscriber) subscription}
	 * @param <U> the other source type
	 *
	 * @return a delayed {@link Flux}
	 *
	 */
	public final <U> Flux<T> delaySubscription(Publisher<U> subscriptionDelay) {
		return onAssembly(new FluxDelaySubscription<>(this, subscriptionDelay));
	}

	/**
	 * An operator working only if this {@link Flux} emits onNext, onError or onComplete {@link Signal}
	 * instances, transforming these {@link #materialize() materialized} signals into
	 * real signals on the {@link Subscriber}.
	 * The error {@link Signal} will trigger onError and complete {@link Signal} will trigger
	 * onComplete.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/dematerializeForFlux.svg" alt="">
	 *
	 * @param <X> the dematerialized type
	 *
	 * @return a dematerialized {@link Flux}
	 * @see #materialize()
	 */
	public final <X> Flux<X> dematerialize() {
		@SuppressWarnings("unchecked")
		Flux<Signal<X>> thiz = (Flux<Signal<X>>) this;
		return onAssembly(new FluxDematerialize<>(thiz));
	}

	/**
	 * For each {@link Subscriber}, track elements from this {@link Flux} that have been
	 * seen and filter out duplicates.
	 * <p>
	 * The values themselves are recorded into a {@link HashSet} for distinct detection.
	 * Use {@code distinct(Object::hashcode)} if you want a more lightweight approach that
	 * doesn't retain all the objects, but is more susceptible to falsely considering two
	 * elements as distinct due to a hashcode collision.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/distinct.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that don't match the distinct predicate,
	 * but you should use the version with a cleanup if you need discarding of keys
	 * categorized by the operator as "seen". See {@link #distinct(Function, Supplier, BiPredicate, Consumer)}.
	 *
	 * @return a filtering {@link Flux} only emitting distinct values
	 */
	public final Flux<T> distinct() {
		return distinct(identityFunction());
	}

	/**
	 * For each {@link Subscriber}, track elements from this {@link Flux} that have been
	 * seen and filter out duplicates, as compared by a key extracted through the user
	 * provided {@link Function}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/distinctWithKey.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that don't match the distinct predicate,
	 * but you should use the version with a cleanup if you need discarding of keys
	 * categorized by the operator as "seen". See {@link #distinct(Function, Supplier, BiPredicate, Consumer)}.
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @param <V> the type of the key extracted from each value in this sequence
	 *
	 * @return a filtering {@link Flux} only emitting values with distinct keys
	 */
	public final <V> Flux<T> distinct(Function<? super T, ? extends V> keySelector) {
		return distinct(keySelector, hashSetSupplier());
	}

	/**
	 * For each {@link Subscriber}, track elements from this {@link Flux} that have been
	 * seen and filter out duplicates, as compared by a key extracted through the user
	 * provided {@link Function} and by the {@link Collection#add(Object) add method}
	 * of the {@link Collection} supplied (typically a {@link Set}).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/distinctWithKey.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that don't match the distinct predicate,
	 * but you should use the version with a cleanup if you need discarding of keys
	 * categorized by the operator as "seen". See {@link #distinct(Function, Supplier, BiPredicate, Consumer)}.
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @param distinctCollectionSupplier supplier of the {@link Collection} used for distinct
	 * check through {@link Collection#add(Object) add} of the key.
	 *
	 * @param <V> the type of the key extracted from each value in this sequence
	 * @param <C> the type of Collection used for distinct checking of keys
	 *
	 * @return a filtering {@link Flux} only emitting values with distinct keys
	 */
	public final <V, C extends Collection<? super V>> Flux<T> distinct(
			Function<? super T, ? extends V> keySelector,
			Supplier<C> distinctCollectionSupplier) {
		return this.distinct(keySelector, distinctCollectionSupplier, Collection::add, Collection::clear);
	}

	/**
	 * For each {@link Subscriber}, track elements from this {@link Flux} that have been
	 * seen and filter out duplicates, as compared by applying a {@link BiPredicate} on
	 * an arbitrary user-supplied {@code <C>} store and a key extracted through the user
	 * provided {@link Function}. The BiPredicate should typically add the key to the
	 * arbitrary store for further comparison. A cleanup callback is also invoked on the
	 * store upon termination of the sequence.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/distinctWithKey.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that don't match the distinct predicate,
	 * but you should use the {@code cleanup} as well if you need discarding of keys
	 * categorized by the operator as "seen".
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @param distinctStoreSupplier supplier of the arbitrary store object used in distinct
	 * checks along the extracted key.
	 * @param distinctPredicate the {@link BiPredicate} to apply to the arbitrary store +
	 * extracted key to perform a distinct check. Since nothing is assumed of the store,
	 * this predicate should also add the key to the store as necessary.
	 * @param cleanup the cleanup callback to invoke on the store upon termination.
	 *
	 * @param <V> the type of the key extracted from each value in this sequence
	 * @param <C> the type of store backing the {@link BiPredicate}
	 *
	 * @return a filtering {@link Flux} only emitting values with distinct keys
	 */
	public final <V, C> Flux<T> distinct(
			Function<? super T, ? extends V> keySelector,
			Supplier<C> distinctStoreSupplier,
			BiPredicate<C, V> distinctPredicate,
			Consumer<C> cleanup) {
		if (this instanceof Fuseable) {
			return onAssembly(new FluxDistinctFuseable<>(this, keySelector,
					distinctStoreSupplier, distinctPredicate, cleanup));
		}
		return onAssembly(new FluxDistinct<>(this, keySelector, distinctStoreSupplier, distinctPredicate, cleanup));
	}

	/**
	 * Filter out subsequent repetitions of an element (that is, if they arrive right after
	 * one another).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/distinctUntilChanged.svg" alt="">
	 * <p>
	 * The last distinct value seen is retained for further comparison, which is done
	 * on the values themselves using {@link Object#equals(Object) the equals method}.
	 * Use {@code distinctUntilChanged(Object::hashcode)} if you want a more lightweight approach that
	 * doesn't retain all the objects, but is more susceptible to falsely considering two
	 * elements as distinct due to a hashcode collision.
	 *
	 * <p><strong>Discard Support:</strong> Although this operator discards elements that are considered as "already seen",
	 * it is not recommended for cases where discarding is needed as the operator doesn't
	 * discard the "key" (in this context, the distinct instance that was last seen).
	 *
	 * @return a filtering {@link Flux} with only one occurrence in a row of each element
	 * (yet elements can repeat in the overall sequence)
	 */
	public final Flux<T> distinctUntilChanged() {
		return distinctUntilChanged(identityFunction());
	}

	/**
	 * Filter out subsequent repetitions of an element (that is, if they arrive right after
	 * one another), as compared by a key extracted through the user provided {@link Function}
	 * using equality.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/distinctUntilChangedWithKey.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are considered as "already seen".
	 * The keys themselves are not discarded.
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @param <V> the type of the key extracted from each value in this sequence
	 *
	 * @return a filtering {@link Flux} with only one occurrence in a row of each element of
	 * the same key (yet element keys can repeat in the overall sequence)
	 */
	public final <V> Flux<T> distinctUntilChanged(Function<? super T, ? extends V> keySelector) {
		return distinctUntilChanged(keySelector, equalPredicate());
	}

	/**
	 * Filter out subsequent repetitions of an element (that is, if they arrive right
	 * after one another), as compared by a key extracted through the user provided {@link
	 * Function} and then comparing keys with the supplied {@link BiPredicate}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/distinctUntilChangedWithKey.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are considered as "already seen"
	 * (for which the {@code keyComparator} returns {@literal true}). The keys themselves
	 * are not discarded.
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @param keyComparator predicate used to compare keys.
	 * @param <V> the type of the key extracted from each value in this sequence
	 *
	 * @return a filtering {@link Flux} with only one occurrence in a row of each element
	 * of the same key for which the predicate returns true (yet element keys can repeat
	 * in the overall sequence)
	 */
	public final <V> Flux<T> distinctUntilChanged(Function<? super T, ? extends V> keySelector,
			BiPredicate<? super V, ? super V> keyComparator) {
		return onAssembly(new FluxDistinctUntilChanged<>(this,
				keySelector,
				keyComparator));
	}

	/**
	 * Add behavior (side-effect) triggered after the {@link Flux} terminates, either by completing downstream successfully or with an error.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doAfterTerminateForFlux.svg" alt="">
	 * <p>
	 * The relevant signal is propagated downstream, then the {@link Runnable} is executed.
	 *
	 * @param afterTerminate the callback to call after {@link Subscriber#onComplete} or {@link Subscriber#onError}
	 *
	 * @return an observed  {@link Flux}
	 */
	public final Flux<T> doAfterTerminate(Runnable afterTerminate) {
		Objects.requireNonNull(afterTerminate, "afterTerminate");
		return doOnSignal(this, null, null, null, null, afterTerminate, null, null);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} is cancelled.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnCancelForFlux.svg" alt="">
	 * <p>
	 * The handler is executed first, then the cancel signal is propagated upstream
	 * to the source.
	 *
	 * @param onCancel the callback to call on {@link Subscription#cancel}
	 *
	 * @return an observed  {@link Flux}
	 */
	public final Flux<T> doOnCancel(Runnable onCancel) {
		Objects.requireNonNull(onCancel, "onCancel");
		return doOnSignal(this, null, null, null, null, null, null, onCancel);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} completes successfully.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnComplete.svg" alt="">
	 * <p>
	 * The {@link Runnable} is executed first, then the onComplete signal is propagated
	 * downstream.
	 *
	 * @param onComplete the callback to call on {@link Subscriber#onComplete}
	 *
	 * @return an observed  {@link Flux}
	 */
	public final Flux<T> doOnComplete(Runnable onComplete) {
		Objects.requireNonNull(onComplete, "onComplete");
		return doOnSignal(this, null, null, null, onComplete, null, null, null);
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
	 * @return a {@link Flux} that cleans up matching elements that get discarded upstream of it.
	 */
	public final <R> Flux<T> doOnDiscard(final Class<R> type, final Consumer<? super R> discardHook) {
		return contextWriteSkippingContextPropagation(Operators.discardLocalAdapter(type, discardHook));
	}

	/**
	 * Add behavior (side-effects) triggered when the {@link Flux} emits an item, fails with an error
	 * or completes successfully. All these events are represented as a {@link Signal}
	 * that is passed to the side-effect callback. Note that this is an advanced operator,
	 * typically used for monitoring of a Flux. These {@link Signal} have a {@link Context}
	 * associated to them.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnEachForFlux.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the relevant signal is propagated
	 * downstream.
	 *
	 * @param signalConsumer the mandatory callback to call on
	 *   {@link Subscriber#onNext(Object)}, {@link Subscriber#onError(Throwable)} and
	 *   {@link Subscriber#onComplete()}
	 * @return an observed {@link Flux}
	 * @see #doOnNext(Consumer)
	 * @see #doOnError(Consumer)
	 * @see #doOnComplete(Runnable)
	 * @see #materialize()
	 * @see Signal
	 */
	public final Flux<T> doOnEach(Consumer<? super Signal<T>> signalConsumer) {
		if (this instanceof Fuseable) {
			return onAssembly(new FluxDoOnEachFuseable<>(this, signalConsumer));
		}
		return onAssembly(new FluxDoOnEach<>(this, signalConsumer));
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} completes with an error.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnErrorForFlux.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onError signal is propagated
	 * downstream.
	 *
	 * @param onError the callback to call on {@link Subscriber#onError}
	 *
	 * @return an observed  {@link Flux}
	 */
	public final Flux<T> doOnError(Consumer<? super Throwable> onError) {
		Objects.requireNonNull(onError, "onError");
		return doOnSignal(this, null, null, onError, null, null, null, null);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} completes with an error matching the given exception type.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnErrorWithClassPredicateForFlux.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onError signal is propagated
	 * downstream.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param onError the error handler for each error
	 * @param <E> type of the error to handle
	 *
	 * @return an observed  {@link Flux}
	 *
	 */
	public final <E extends Throwable> Flux<T> doOnError(Class<E> exceptionType,
			final Consumer<? super E> onError) {
		Objects.requireNonNull(exceptionType, "type");
		@SuppressWarnings("unchecked")
		Consumer<Throwable> handler = (Consumer<Throwable>)onError;
		return doOnError(exceptionType::isInstance, (handler));
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} completes with an error matching the given exception.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnErrorWithPredicateForFlux.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onError signal is propagated
	 * downstream.
	 *
	 * @param predicate the matcher for exceptions to handle
	 * @param onError the error handler for each error
	 *
	 * @return an observed  {@link Flux}
	 *
	 */
	public final Flux<T> doOnError(Predicate<? super Throwable> predicate,
			final Consumer<? super Throwable> onError) {
		Objects.requireNonNull(predicate, "predicate");
		return doOnError(t -> {
			if (predicate.test(t)) {
				onError.accept(t);
			}
		});
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} emits an item.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnNextForFlux.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onNext signal is propagated
	 * downstream.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * (including when fusion is enabled). Exceptions thrown by the consumer are passed to
	 * the {@link #onErrorContinue(BiConsumer)} error consumer (the value consumer
	 * is not invoked, as the source element will be part of the sequence). The onNext
	 * signal is then propagated as normal.
	 *
	 * @param onNext the callback to call on {@link Subscriber#onNext}
	 *
	 * @return an observed  {@link Flux}
	 */
	public final Flux<T> doOnNext(Consumer<? super T> onNext) {
		Objects.requireNonNull(onNext, "onNext");
		return doOnSignal(this, null, onNext, null, null, null, null, null);
	}

	/**
	 * Add behavior (side-effect) triggering a {@link LongConsumer} when this {@link Flux}
	 * receives any request.
	 * <p>
	 *     Note that non fatal error raised in the callback will not be propagated and
	 *     will simply trigger {@link Operators#onOperatorError(Throwable, Context)}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnRequestForFlux.svg" alt="">
	 * <p>
	 * The {@link LongConsumer} is executed first, then the request signal is propagated
	 * upstream to the parent.
	 *
	 * @param consumer the consumer to invoke on each request
	 *
	 * @return an observed  {@link Flux}
	 */
	public final Flux<T> doOnRequest(LongConsumer consumer) {
		Objects.requireNonNull(consumer, "consumer");
		return doOnSignal(this, null, null, null, null, null, consumer, null);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} is being subscribed,
	 * that is to say when a {@link Subscription} has been produced by the {@link Publisher}
	 * and is being passed to the {@link Subscriber#onSubscribe(Subscription)}.
	 * <p>
	 * This method is <strong>not</strong> intended for capturing the subscription and calling its methods,
	 * but for side effects like monitoring. For instance, the correct way to cancel a subscription is
	 * to call {@link Disposable#dispose()} on the Disposable returned by {@link Flux#subscribe()}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnSubscribe.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the {@link Subscription} is propagated
	 * downstream to the next subscriber in the chain that is being established.
	 *
	 * @param onSubscribe the callback to call on {@link Subscriber#onSubscribe}
	 *
	 * @return an observed  {@link Flux}
	 * @see #doFirst(Runnable)
	 */
	public final Flux<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		Objects.requireNonNull(onSubscribe, "onSubscribe");
		return doOnSignal(this, onSubscribe, null, null, null, null, null, null);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} terminates, either by
	 * completing successfully or failing with an error.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnTerminateForFlux.svg" alt="">
	 * <p>
	 * The {@link Runnable} is executed first, then the onComplete/onError signal is propagated
	 * downstream.
	 *
	 * @param onTerminate the callback to call on {@link Subscriber#onComplete} or {@link Subscriber#onError}
	 *
	 * @return an observed  {@link Flux}
	 */
	public final Flux<T> doOnTerminate(Runnable onTerminate) {
		Objects.requireNonNull(onTerminate, "onTerminate");
		return doOnSignal(this,
				null,
				null,
				e -> onTerminate.run(),
				onTerminate,
				null,
				null,
				null);
	}

	/**
	 * Add behavior (side-effect) triggered <strong>before</strong> the {@link Flux} is
	 * <strong>subscribed to</strong>, which should be the first event after assembly time.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doFirstForFlux.svg" alt="">
	 * <p>
	 * Note that when several {@link #doFirst(Runnable)} operators are used anywhere in a
	 * chain of operators, their order of execution is reversed compared to the declaration
	 * order (as subscribe signal flows backward, from the ultimate subscriber to the source
	 * publisher):
	 * <pre><code>
	 * Flux.just(1, 2)
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
	 * {@link Flux} ({@code this}).
	 * <p>
	 * This side-effect method provides stronger <i>first</i> guarantees compared to
	 * {@link #doOnSubscribe(Consumer)}, which is triggered once the {@link Subscription}
	 * has been set up and passed to the {@link Subscriber}.
	 *
	 * @param onFirst the callback to execute before the {@link Flux} is subscribed to
	 * @return an observed {@link Flux}
	 */
	public final Flux<T> doFirst(Runnable onFirst) {
		Objects.requireNonNull(onFirst, "onFirst");
		if (this instanceof Fuseable) {
			return onAssembly(new FluxDoFirstFuseable<>(this, onFirst));
		}
		return onAssembly(new FluxDoFirst<>(this, onFirst));
	}

	/**
	 * Add behavior (side-effect) triggered <strong>after</strong> the {@link Flux} terminates for any reason,
	 * including cancellation. The terminating event ({@link SignalType#ON_COMPLETE},
	 * {@link SignalType#ON_ERROR} and {@link SignalType#CANCEL}) is passed to the consumer,
	 * which is executed after the signal has been passed downstream.
	 * <p>
	 * Note that the fact that the signal is propagated downstream before the callback is
	 * executed means that several doFinally in a row will be executed in
	 * <strong>reverse order</strong>. If you want to assert the execution of the callback
	 * please keep in mind that the Flux will complete before it is executed, so its
	 * effect might not be visible immediately after eg. a {@link #blockLast()}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doFinallyForFlux.svg" alt="">
	 *
	 * @param onFinally the callback to execute after a terminal signal (complete, error
	 * or cancel)
	 * @return an observed {@link Flux}
	 */
	public final Flux<T> doFinally(Consumer<SignalType> onFinally) {
		Objects.requireNonNull(onFinally, "onFinally");
		return onAssembly(new FluxDoFinally<>(this, onFinally));
	}

	/**
	 * Map this {@link Flux} into {@link reactor.util.function.Tuple2 Tuple2&lt;Long, T&gt;}
	 * of timemillis and source data. The timemillis corresponds to the elapsed time
	 * between each signal as measured by the {@link Schedulers#parallel() parallel} scheduler.
	 * First duration is measured between the subscription and the first element.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/elapsedForFlux.svg" alt="">
	 *
	 * @return a new {@link Flux} that emits a tuple of time elapsed in milliseconds and matching data
	 * @see #timed()
	 * @see Timed#elapsed()
	 */
	public final Flux<Tuple2<Long, T>> elapsed() {
		return elapsed(Schedulers.parallel());
	}

	/**
	 * Map this {@link Flux} into {@link reactor.util.function.Tuple2 Tuple2&lt;Long, T&gt;}
	 * of timemillis and source data. The timemillis corresponds to the elapsed time
	 * between each signal as measured by the provided {@link Scheduler}.
	 * First duration is measured between the subscription and the first element.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/elapsedForFlux.svg" alt="">
	 *
	 * @param scheduler a {@link Scheduler} instance to {@link Scheduler#now(TimeUnit) read time from}
	 *
	 * @return a new {@link Flux} that emits tuples of time elapsed in milliseconds and matching data
	 * @see #timed(Scheduler)
	 * @see Timed#elapsed()
	 */
	public final Flux<Tuple2<Long, T>> elapsed(Scheduler scheduler) {
		Objects.requireNonNull(scheduler, "scheduler");
		return onAssembly(new FluxElapsed<>(this, scheduler));
	}

	/**
	 * Emit only the element at the given index position or {@link IndexOutOfBoundsException}
	 * if the sequence is shorter.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/elementAt.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that appear before the requested index.
	 *
	 * @param index zero-based index of the only item to emit
	 *
	 * @return a {@link Mono} of the item at the specified zero-based index
	 */
	public final Mono<T> elementAt(int index) {
		return Mono.onAssembly(new MonoElementAt<>(this, index));
	}

	/**
	 * Emit only the element at the given index position or fall back to a
	 * default value if the sequence is shorter.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/elementAtWithDefault.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that appear before the requested index.
	 *
	 * @param index zero-based index of the only item to emit
	 * @param defaultValue a default value to emit if the sequence is shorter
	 *
	 * @return a {@link Mono} of the item at the specified zero-based index or a default value
	 */
	public final Mono<T> elementAt(int index, T defaultValue) {
		return Mono.onAssembly(new MonoElementAt<>(this, index, defaultValue));
	}

	/**
	 * Recursively expand elements into a graph and emit all the resulting element,
	 * in a depth-first traversal order.
	 * <p>
	 * That is: emit one value from this {@link Flux}, expand it and emit the first value
	 * at this first level of recursion, and so on... When no more recursion is possible,
	 * backtrack to the previous level and re-apply the strategy.
	 * <p>
	 * For example, given the hierarchical structure
	 * <pre>
	 *  A
	 *   - AA
	 *     - aa1
	 *  B
	 *   - BB
	 *     - bb1
	 * </pre>
	 *
	 * Expands {@code Flux.just(A, B)} into
	 * <pre>
	 *  A
	 *  AA
	 *  aa1
	 *  B
	 *  BB
	 *  bb1
	 * </pre>
	 *
	 * @param expander the {@link Function} applied at each level of recursion to expand
	 * values into a {@link Publisher}, producing a graph.
	 * @param capacityHint a capacity hint to prepare the inner queues to accommodate n
	 * elements per level of recursion.
	 *
	 * @return a {@link Flux} expanded depth-first
	 */
	public final Flux<T> expandDeep(Function<? super T, ? extends Publisher<? extends T>> expander,
			int capacityHint) {
		return onAssembly(new FluxExpand<>(this, expander, false, capacityHint));
	}

	/**
	 * Recursively expand elements into a graph and emit all the resulting element,
	 * in a depth-first traversal order.
	 * <p>
	 * That is: emit one value from this {@link Flux}, expand it and emit the first value
	 * at this first level of recursion, and so on... When no more recursion is possible,
	 * backtrack to the previous level and re-apply the strategy.
	 * <p>
	 * For example, given the hierarchical structure
	 * <pre>
	 *  A
	 *   - AA
	 *     - aa1
	 *  B
	 *   - BB
	 *     - bb1
	 * </pre>
	 *
	 * Expands {@code Flux.just(A, B)} into
	 * <pre>
	 *  A
	 *  AA
	 *  aa1
	 *  B
	 *  BB
	 *  bb1
	 * </pre>
	 *
	 * @param expander the {@link Function} applied at each level of recursion to expand
	 * values into a {@link Publisher}, producing a graph.
	 *
	 * @return a {@link Flux} expanded depth-first
	 */
	public final Flux<T> expandDeep(Function<? super T, ? extends Publisher<? extends T>> expander) {
		return expandDeep(expander, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Recursively expand elements into a graph and emit all the resulting element using
	 * a breadth-first traversal strategy.
	 * <p>
	 * That is: emit the values from this {@link Flux} first, then expand each at a first level of
	 * recursion and emit all of the resulting values, then expand all of these at a second
	 * level and so on.
	 * <p>
	 * For example, given the hierarchical structure
	 * <pre>
	 *  A
	 *   - AA
	 *     - aa1
	 *  B
	 *   - BB
	 *     - bb1
	 * </pre>
	 *
	 * Expands {@code Flux.just(A, B)} into
	 * <pre>
	 *  A
	 *  B
	 *  AA
	 *  BB
	 *  aa1
	 *  bb1
	 * </pre>
	 *
	 * @param expander the {@link Function} applied at each level of recursion to expand
	 * values into a {@link Publisher}, producing a graph.
	 * @param capacityHint a capacity hint to prepare the inner queues to accommodate n
	 * elements per level of recursion.
	 *
	 * @return a breadth-first expanded {@link Flux}
	 */
	public final Flux<T> expand(Function<? super T, ? extends Publisher<? extends T>> expander,
			int capacityHint) {
		return Flux.onAssembly(new FluxExpand<>(this, expander, true, capacityHint));
	}

	/**
	 * Recursively expand elements into a graph and emit all the resulting element using
	 * a breadth-first traversal strategy.
	 * <p>
	 * That is: emit the values from this {@link Flux} first, then expand each at a first level of
	 * recursion and emit all of the resulting values, then expand all of these at a second
	 * level and so on..
	 * <p>
	 * For example, given the hierarchical structure
	 * <pre>
	 *  A
	 *   - AA
	 *     - aa1
	 *  B
	 *   - BB
	 *     - bb1
	 * </pre>
	 *
	 * Expands {@code Flux.just(A, B)} into
	 * <pre>
	 *  A
	 *  B
	 *  AA
	 *  BB
	 *  aa1
	 *  bb1
	 * </pre>
	 *
	 * @param expander the {@link Function} applied at each level of recursion to expand
	 * values into a {@link Publisher}, producing a graph.
	 *
	 * @return a breadth-first expanded {@link Flux}
	 */
	public final Flux<T> expand(Function<? super T, ? extends Publisher<? extends T>> expander) {
		return expand(expander, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Evaluate each source value against the given {@link Predicate}. If the predicate test succeeds, the value is
	 * emitted. If the predicate test fails, the value is ignored and a request of 1 is made upstream.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/filterForFlux.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that do not match the filter. It
	 * also discards elements internally queued for backpressure upon cancellation or error triggered by a data signal.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * (including when fusion is enabled). Exceptions thrown by the predicate are
	 * considered as if the predicate returned false: they cause the source value to be
	 * dropped and a new element ({@code request(1)}) being requested from upstream.
	 *
	 * @param p the {@link Predicate} to test values against
	 *
	 * @return a new {@link Flux} containing only values that pass the predicate test
	 */
	public final Flux<T> filter(Predicate<? super T> p) {
		if (this instanceof Fuseable) {
			return onAssembly(new FluxFilterFuseable<>(this, p));
		}
		return onAssembly(new FluxFilter<>(this, p));
	}

	/**
	 * Test each value emitted by this {@link Flux} asynchronously using a generated
	 * {@code Publisher<Boolean>} test. A value is replayed if the first item emitted
	 * by its corresponding test is {@literal true}. It is dropped if its test is either
	 * empty or its first emitted value is {@literal false}.
	 * <p>
	 * Note that only the first value of the test publisher is considered, and unless it
	 * is a {@link Mono}, test will be cancelled after receiving that first value. Test
	 * publishers are generated and subscribed to in sequence.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/filterWhenForFlux.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that do not match the filter. It
	 * also discards elements internally queued for backpressure upon cancellation or error triggered by a data signal.
	 *
	 * @param asyncPredicate the function generating a {@link Publisher} of {@link Boolean}
	 * for each value, to filter the Flux with
	 *
	 * @return a filtered {@link Flux}
	 */
	public final Flux<T> filterWhen(Function<? super T, ? extends Publisher<Boolean>> asyncPredicate) {
		return filterWhen(asyncPredicate, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Test each value emitted by this {@link Flux} asynchronously using a generated
	 * {@code Publisher<Boolean>} test. A value is replayed if the first item emitted
	 * by its corresponding test is {@literal true}. It is dropped if its test is either
	 * empty or its first emitted value is {@literal false}.
	 * <p>
	 * Note that only the first value of the test publisher is considered, and unless it
	 * is a {@link Mono}, test will be cancelled after receiving that first value. Test
	 * publishers are generated and subscribed to in sequence.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/filterWhenForFlux.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that do not match the filter. It
	 * also discards elements internally queued for backpressure upon cancellation or error triggered by a data signal.
	 *
	 * @param asyncPredicate the function generating a {@link Publisher} of {@link Boolean}
	 * for each value, to filter the Flux with
	 * @param bufferSize the maximum expected number of values to hold pending a result of
	 * their respective asynchronous predicates, rounded to the next power of two. This is
	 * capped depending on the size of the heap and the JVM limits, so be careful with
	 * large values (although eg. {@literal 65536} should still be fine). Also serves as
	 * the initial request size for the source.
	 *
	 * @return a filtered {@link Flux}
	 */
	public final Flux<T> filterWhen(Function<? super T, ? extends Publisher<Boolean>> asyncPredicate,
			int bufferSize) {
		return onAssembly(new FluxFilterWhen<>(this, asyncPredicate, bufferSize));
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux} through merging,
	 * which allow them to interleave.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMapSequential(Function) flatMapSequential} and {@link #concatMap(Function) concatMap}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator does not necessarily preserve
	 *     original ordering, as inner element are flattened as they arrive.</li>
	 *     <li><b>Interleaving</b>: this operator lets values from different inners interleave
	 *     (similar to merging the inner sequences).</li>
	 * </ul>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapForFlux.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements internally queued for backpressure upon cancellation or error triggered by a data signal.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * in the mapper {@link Function}. Exceptions thrown by the mapper then behave as if
	 * it had mapped the value to an empty publisher. If the mapper does map to a scalar
	 * publisher (an optimization in which the value can be resolved immediately without
	 * subscribing to the publisher, e.g. a {@link Mono#fromCallable(Callable)}) but said
	 * publisher throws, this can be resumed from in the same manner.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param <R> the merged output sequence type
	 *
	 * @return a new {@link Flux}
	 */
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return flatMap(mapper, Queues.SMALL_BUFFER_SIZE, Queues
				.XS_BUFFER_SIZE);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux} through merging,
	 * which allow them to interleave.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMapSequential(Function) flatMapSequential} and {@link #concatMap(Function) concatMap}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator does not necessarily preserve
	 *     original ordering, as inner element are flattened as they arrive.</li>
	 *     <li><b>Interleaving</b>: this operator lets values from different inners interleave
	 *     (similar to merging the inner sequences).</li>
	 * </ul>
	 * The concurrency argument allows to control how many {@link Publisher} can be
	 * subscribed to and merged in parallel. In turn, that argument shows the size of
	 * the first {@link Subscription#request} to the upstream.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapWithConcurrency.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements internally queued for backpressure upon cancellation or error triggered by a data signal.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * in the mapper {@link Function}. Exceptions thrown by the mapper then behave as if
	 * it had mapped the value to an empty publisher. If the mapper does map to a scalar
	 * publisher (an optimization in which the value can be resolved immediately without
	 * subscribing to the publisher, e.g. a {@link Mono#fromCallable(Callable)}) but said
	 * publisher throws, this can be resumed from in the same manner.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param concurrency the maximum number of in-flight inner sequences
	 *
	 * @param <V> the merged output sequence type
	 *
	 * @return a new {@link Flux}
	 */
	public final <V> Flux<V> flatMap(Function<? super T, ? extends Publisher<? extends V>> mapper, int
			concurrency) {
		return flatMap(mapper, concurrency, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux} through merging,
	 * which allow them to interleave.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMapSequential(Function) flatMapSequential} and {@link #concatMap(Function) concatMap}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator does not necessarily preserve
	 *     original ordering, as inner element are flattened as they arrive.</li>
	 *     <li><b>Interleaving</b>: this operator lets values from different inners interleave
	 *     (similar to merging the inner sequences).</li>
	 * </ul>
	 * The concurrency argument allows to control how many {@link Publisher} can be
	 * subscribed to and merged in parallel. In turn, that argument shows the size of
	 * the first {@link Subscription#request} to the upstream.
	 * The prefetch argument allows to give an arbitrary prefetch size to the merged
	 * {@link Publisher} (in other words prefetch size means the size of the first
	 * {@link Subscription#request} to the merged {@link Publisher}).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapWithConcurrencyAndPrefetch.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements internally queued for backpressure upon cancellation or error triggered by a data signal.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * in the mapper {@link Function}. Exceptions thrown by the mapper then behave as if
	 * it had mapped the value to an empty publisher. If the mapper does map to a scalar
	 * publisher (an optimization in which the value can be resolved immediately without
	 * subscribing to the publisher, e.g. a {@link Mono#fromCallable(Callable)}) but said
	 * publisher throws, this can be resumed from in the same manner.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param concurrency the maximum number of in-flight inner sequences
	 * @param prefetch the maximum in-flight elements from each inner {@link Publisher} sequence
	 *
	 * @param <V> the merged output sequence type
	 *
	 * @return a merged {@link Flux}
	 */
	public final <V> Flux<V> flatMap(Function<? super T, ? extends Publisher<? extends V>> mapper, int
			concurrency, int prefetch) {
		return flatMap(mapper, false, concurrency, prefetch);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux} through merging,
	 * which allow them to interleave.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMapSequential(Function) flatMapSequential} and {@link #concatMap(Function) concatMap}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator does not necessarily preserve
	 *     original ordering, as inner element are flattened as they arrive.</li>
	 *     <li><b>Interleaving</b>: this operator lets values from different inners interleave
	 *     (similar to merging the inner sequences).</li>
	 * </ul>
	 * The concurrency argument allows to control how many {@link Publisher} can be
	 * subscribed to and merged in parallel. The prefetch argument allows to give an
	 * arbitrary prefetch size to the merged {@link Publisher}. This variant will delay
	 * any error until after the rest of the flatMap backlog has been processed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapWithConcurrencyAndPrefetch.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements internally queued for backpressure upon cancellation or error triggered by a data signal.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * in the mapper {@link Function}. Exceptions thrown by the mapper then behave as if
	 * it had mapped the value to an empty publisher. If the mapper does map to a scalar
	 * publisher (an optimization in which the value can be resolved immediately without
	 * subscribing to the publisher, e.g. a {@link Mono#fromCallable(Callable)}) but said
	 * publisher throws, this can be resumed from in the same manner.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param concurrency the maximum number of in-flight inner sequences
	 * @param prefetch the maximum in-flight elements from each inner {@link Publisher} sequence
	 *
	 * @param <V> the merged output sequence type
	 *
	 * @return a merged {@link Flux}
	 */
	public final <V> Flux<V> flatMapDelayError(Function<? super T, ? extends Publisher<? extends V>> mapper,
			int concurrency, int prefetch) {
		return flatMap(mapper, true, concurrency, prefetch);
	}

	/**
	 * Transform the signals emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux} through merging,
	 * which allow them to interleave. Note that at least one of the signal mappers must
	 * be provided, and all provided mappers must produce a publisher.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMapSequential(Function) flatMapSequential} and {@link #concatMap(Function) concatMap}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator does not necessarily preserve
	 *     original ordering, as inner element are flattened as they arrive.</li>
	 *     <li><b>Interleaving</b>: this operator lets values from different inners interleave
	 *     (similar to merging the inner sequences).</li>
	 * </ul>
	 * <p>
	 * OnError will be transformed into completion signal after its mapping callback has been applied.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapWithMappersOnTerminalEventsForFlux.svg" alt="">
	 *
	 * @param mapperOnNext the {@link Function} to call on next data and returning a sequence to merge.
	 * Use {@literal null} to ignore (provided at least one other mapper is specified).
	 * @param mapperOnError the {@link Function} to call on error signal and returning a sequence to merge.
	 * Use {@literal null} to ignore (provided at least one other mapper is specified).
	 * @param mapperOnComplete the {@link Function} to call on complete signal and returning a sequence to merge.
	 * Use {@literal null} to ignore (provided at least one other mapper is specified).
	 * @param <R> the output {@link Publisher} type target
	 *
	 * @return a new {@link Flux}
	 */
	public final <R> Flux<R> flatMap(
			@Nullable Function<? super T, ? extends Publisher<? extends R>> mapperOnNext,
			@Nullable Function<? super Throwable, ? extends Publisher<? extends R>> mapperOnError,
			@Nullable Supplier<? extends Publisher<? extends R>> mapperOnComplete) {
		return onAssembly(new FluxFlatMap<>(
				new FluxMapSignal<>(this, mapperOnNext, mapperOnError, mapperOnComplete),
				identityFunction(),
				false, Queues.XS_BUFFER_SIZE,
				Queues.xs(), Queues.XS_BUFFER_SIZE,
				Queues.xs()
		));
	}

	/**
	 * Transform the items emitted by this {@link Flux} into {@link Iterable}, then flatten the elements from those by
	 * merging them into a single {@link Flux}. For each iterable, {@link Iterable#iterator()} will be called at least
	 * once and at most twice.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapIterableForFlux.svg" alt="">
	 * <p>
	 * This operator inspects each {@link Iterable}'s {@link Spliterator} to assess if the iteration
	 * can be guaranteed to be finite (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 * Since the default Spliterator wraps the Iterator we can have two {@link Iterable#iterator()}
	 * calls per iterable. This second invocation is skipped on a {@link Collection} however, a type which is
	 * assumed to be always finite.
	 * <p>
	 * Note that unlike {@link #flatMap(Function)} and {@link #concatMap(Function)}, with Iterable there is
	 * no notion of eager vs lazy inner subscription. The content of the Iterables are all played sequentially.
	 * Thus {@code flatMapIterable} and {@code concatMapIterable} are equivalent offered as a discoverability
	 * improvement for users that explore the API with the concat vs flatMap expectation.
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator discards {@code T} elements it prefetched and, in
	 * some cases, attempts to discard remainder of the currently processed {@link Iterable} (if it can
	 * safely ensure the iterator is finite). Note that this means each {@link Iterable}'s {@link Iterable#iterator()}
	 * method could be invoked twice.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * (including when fusion is enabled). Exceptions thrown by the consumer are passed to
	 * the {@link #onErrorContinue(BiConsumer)} error consumer (the value consumer
	 * is not invoked, as the source element will be part of the sequence). The onNext
	 * signal is then propagated as normal.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N {@link Iterable}
	 *
	 * @param <R> the merged output sequence type
	 *
	 * @return a concatenation of the values from the Iterables obtained from each element in this {@link Flux}
	 */
	public final <R> Flux<R> flatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper) {
		return flatMapIterable(mapper, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Transform the items emitted by this {@link Flux} into {@link Iterable}, then flatten the emissions from those by
	 * merging them into a single {@link Flux}. The prefetch argument allows to give an
	 * arbitrary prefetch size to the upstream source.
	 * For each iterable, {@link Iterable#iterator()} will be called at least once and at most twice.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapIterableForFlux.svg" alt="">
	 * <p>
	 * This operator inspects each {@link Iterable}'s {@link Spliterator} to assess if the iteration
	 * can be guaranteed to be finite (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 * Since the default Spliterator wraps the Iterator we can have two {@link Iterable#iterator()}
	 * calls per iterable. This second invocation is skipped on a {@link Collection} however, a type which is
	 * assumed to be always finite.
	 * <p>
	 * Note that unlike {@link #flatMap(Function)} and {@link #concatMap(Function)}, with Iterable there is
	 * no notion of eager vs lazy inner subscription. The content of the Iterables are all played sequentially.
	 * Thus {@code flatMapIterable} and {@code concatMapIterable} are equivalent offered as a discoverability
	 * improvement for users that explore the API with the concat vs flatMap expectation.
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator discards {@code T} elements it prefetched and, in
	 * some cases, attempts to discard remainder of the currently processed {@link Iterable} (if it can
	 * safely ensure the iterator is finite).
	 * Note that this means each {@link Iterable}'s {@link Iterable#iterator()} method could be invoked twice.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * (including when fusion is enabled). Exceptions thrown by the consumer are passed to
	 * the {@link #onErrorContinue(BiConsumer)} error consumer (the value consumer
	 * is not invoked, as the source element will be part of the sequence). The onNext
	 * signal is then propagated as normal.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N {@link Iterable}
	 * @param prefetch the number of values to request from the source upon subscription, to be transformed to {@link Iterable}
	 *
	 * @param <R> the merged output sequence type
	 *
	 * @return a concatenation of the values from the Iterables obtained from each element in this {@link Flux}
	 */
	public final <R> Flux<R> flatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch) {
		return onAssembly(new FluxFlattenIterable<>(this, mapper, prefetch,
				Queues.get(prefetch)));
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, but merge them in
	 * the order of their source element.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMap(Function) flatMap} and {@link #concatMap(Function) concatMap}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners (like flatMap).</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator queues elements from
	 *     late inners until all elements from earlier inners have been emitted, thus emitting
	 *     inner sequences as a whole, in an order that matches their source's order.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (similar looking result to concatMap, but due to queueing of values
	 *     that would have been interleaved otherwise).</li>
	 * </ul>
	 *
	 * <p>
	 * That is to say, whenever a source element is emitted it is transformed to an inner
	 * {@link Publisher}. However, if such an early inner takes more time to complete than
	 * subsequent faster inners, the data from these faster inners will be queued until
	 * the earlier inner completes, so as to maintain source ordering.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapSequential.svg" alt="">
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public final <R> Flux<R> flatMapSequential(Function<? super T, ? extends
			Publisher<? extends R>> mapper) {
		return flatMapSequential(mapper, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, but merge them in
	 * the order of their source element.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMap(Function) flatMap} and {@link #concatMap(Function) concatMap}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners (like flatMap).</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator queues elements from
	 *     late inners until all elements from earlier inners have been emitted, thus emitting
	 *     inner sequences as a whole, in an order that matches their source's order.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (similar looking result to concatMap, but due to queueing of values
	 *     that would have been interleaved otherwise).</li>
	 * </ul>
	 *
	 * <p>
	 * That is to say, whenever a source element is emitted it is transformed to an inner
	 * {@link Publisher}. However, if such an early inner takes more time to complete than
	 * subsequent faster inners, the data from these faster inners will be queued until
	 * the earlier inner completes, so as to maintain source ordering.
	 *
	 * <p>
	 * The concurrency argument allows to control how many merged {@link Publisher} can happen in parallel.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapSequentialWithConcurrency.svg" alt="">
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param maxConcurrency the maximum number of in-flight inner sequences
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public final <R> Flux<R> flatMapSequential(Function<? super T, ? extends
			Publisher<? extends R>> mapper, int maxConcurrency) {
		return flatMapSequential(mapper, maxConcurrency, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, but merge them in
	 * the order of their source element.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMap(Function) flatMap} and {@link #concatMap(Function) concatMap}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners (like flatMap).</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator queues elements from
	 *     late inners until all elements from earlier inners have been emitted, thus emitting
	 *     inner sequences as a whole, in an order that matches their source's order.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (similar looking result to concatMap, but due to queueing of values
	 *     that would have been interleaved otherwise).</li>
	 * </ul>
	 *
	 * <p>
	 * That is to say, whenever a source element is emitted it is transformed to an inner
	 * {@link Publisher}. However, if such an early inner takes more time to complete than
	 * subsequent faster inners, the data from these faster inners will be queued until
	 * the earlier inner completes, so as to maintain source ordering.
	 *
	 * <p>
	 * The concurrency argument allows to control how many merged {@link Publisher}
	 * can happen in parallel. The prefetch argument allows to give an arbitrary prefetch
	 * size to the merged {@link Publisher}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapSequentialWithConcurrencyAndPrefetch.svg" alt="">
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param maxConcurrency the maximum number of in-flight inner sequences
	 * @param prefetch the maximum in-flight elements from each inner {@link Publisher} sequence
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public final <R> Flux<R> flatMapSequential(Function<? super T, ? extends
			Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
		return flatMapSequential(mapper, false, maxConcurrency, prefetch);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, but merge them in
	 * the order of their source element.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #flatMap(Function) flatMap} and {@link #concatMap(Function) concatMap}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners (like flatMap).</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator queues elements from
	 *     late inners until all elements from earlier inners have been emitted, thus emitting
	 *     inner sequences as a whole, in an order that matches their source's order.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (similar looking result to concatMap, but due to queueing of values
	 *     that would have been interleaved otherwise).</li>
	 * </ul>
	 *
	 * <p>
	 * That is to say, whenever a source element is emitted it is transformed to an inner
	 * {@link Publisher}. However, if such an early inner takes more time to complete than
	 * subsequent faster inners, the data from these faster inners will be queued until
	 * the earlier inner completes, so as to maintain source ordering.
	 *
	 * <p>
	 * The concurrency argument allows to control how many merged {@link Publisher}
	 * can happen in parallel. The prefetch argument allows to give an arbitrary prefetch
	 * size to the merged {@link Publisher}. This variant will delay any error until after the
	 * rest of the flatMap backlog has been processed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapSequentialWithConcurrencyAndPrefetch.svg" alt="">
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param maxConcurrency the maximum number of in-flight inner sequences
	 * @param prefetch the maximum in-flight elements from each inner {@link Publisher} sequence
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public final <R> Flux<R> flatMapSequentialDelayError(Function<? super T, ? extends
			Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
		return flatMapSequential(mapper, true, maxConcurrency, prefetch);
	}

	/**
	 * The prefetch configuration of the {@link Flux}
	 * @return the prefetch configuration of the {@link Flux}, -1 if unspecified
	 */
	public int getPrefetch() {
		return -1;
	}

	/**
	 * Divide this sequence into dynamically created {@link Flux} (or groups) for each
	 * unique key, as produced by the provided keyMapper {@link Function}. Note that
	 * groupBy works best with a low cardinality of groups, so chose your keyMapper
	 * function accordingly.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/groupByWithKeyMapper.svg" alt="">
	 *
	 * <p>
	 * The groups need to be drained and consumed downstream for groupBy to work correctly.
	 * Notably when the criteria produces a large amount of groups, it can lead to hanging
	 * if the groups are not suitably consumed downstream (eg. due to a {@code flatMap}
	 * with a {@code maxConcurrency} parameter that is set too low).
	 *
	 * <p>
  	 * To avoid deadlock, the concurrency of the subscriber to groupBy should be 
	 * greater than or equal to the number of groups created. In that case every group
	 * has its own subscriber and progress can be made, even when the data publish pattern
	 * is arbitrary. Otherwise, when the number of groups exceeds downstream concurrency,
	 * the subscribers should be designed with caution, because if the consumption
	 * pattern doesn't match what can be accommodated in its producer buffer,
	 * the process may enter deadlock due to backpressure.
	 *
	 * <p>
	 * Note that groups are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a specific group more than once: groups are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * @param keyMapper the key mapping {@link Function} that evaluates an incoming data and returns a key.
	 * @param <K> the key type extracted from each value of this sequence
	 *
	 * @return a {@link Flux} of {@link GroupedFlux} grouped sequences
	 */
	public final <K> Flux<GroupedFlux<K, T>> groupBy(Function<? super T, ? extends K> keyMapper) {
		return groupBy(keyMapper, identityFunction());
	}

	/**
	 * Divide this sequence into dynamically created {@link Flux} (or groups) for each
	 * unique key, as produced by the provided keyMapper {@link Function}. Note that
	 * groupBy works best with a low cardinality of groups, so chose your keyMapper
	 * function accordingly.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/groupByWithKeyMapper.svg" alt="">
	 *
	 * <p>
	 * The groups need to be drained and consumed downstream for groupBy to work correctly.
	 * Notably when the criteria produces a large amount of groups, it can lead to hanging
	 * if the groups are not suitably consumed downstream (eg. due to a {@code flatMap}
	 * with a {@code maxConcurrency} parameter that is set too low).
 	 *
	 * <p>
	 * To avoid deadlock, the concurrency of the subscriber to groupBy should be
	 * greater than or equal to the number of groups created. In that case every group
	 * has its own subscriber and progress can be made, even when the data publish pattern
	 * is arbitrary. Otherwise, when the number of groups exceeds downstream concurrency,
	 * the subscribers should be designed with caution, because if the consumption
	 * pattern doesn't match what can be accommodated in its producer buffer,
	 * the process may enter deadlock due to backpressure.
	 *
	 * <p>
	 * Note that groups are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a specific group more than once: groups are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * @param keyMapper the key mapping {@link Function} that evaluates an incoming data and returns a key.
	 * @param prefetch the number of values to prefetch from the source
	 * @param <K> the key type extracted from each value of this sequence
	 *
	 * @return a {@link Flux} of {@link GroupedFlux} grouped sequences
	 */
	public final <K> Flux<GroupedFlux<K, T>> groupBy(Function<? super T, ? extends K> keyMapper, int prefetch) {
		return groupBy(keyMapper, identityFunction(), prefetch);
	}

	/**
	 * Divide this sequence into dynamically created {@link Flux} (or groups) for each
	 * unique key, as produced by the provided keyMapper {@link Function}. Source elements
	 * are also mapped to a different value using the {@code valueMapper}. Note that
	 * groupBy works best with a low cardinality of groups, so chose your keyMapper
	 * function accordingly.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/groupByWithKeyMapperAndValueMapper.svg" alt="">
	 *
	 * <p>
	 * The groups need to be drained and consumed downstream for groupBy to work correctly.
	 * Notably when the criteria produces a large amount of groups, it can lead to hanging
	 * if the groups are not suitably consumed downstream (eg. due to a {@code flatMap}
	 * with a {@code maxConcurrency} parameter that is set too low).
  	 *
	 * <p>
	 * To avoid deadlock, the concurrency of the subscriber to groupBy should be
	 * greater than or equal to the number of groups created. In that case every group
	 * has its own subscriber and progress can be made, even when the data publish pattern
	 * is arbitrary. Otherwise, when the number of groups exceeds downstream concurrency,
	 * the subscribers should be designed with caution, because if the consumption
	 * pattern doesn't match what can be accommodated in its producer buffer,
	 * the process may enter deadlock due to backpressure.
	 *
	 * <p>
	 * Note that groups are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a specific group more than once: groups are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * @param keyMapper the key mapping function that evaluates an incoming data and returns a key.
	 * @param valueMapper the value mapping function that evaluates which data to extract for re-routing.
	 * @param <K> the key type extracted from each value of this sequence
	 * @param <V> the value type extracted from each value of this sequence
	 *
	 * @return a {@link Flux} of {@link GroupedFlux} grouped sequences
	 *
	 */
	public final <K, V> Flux<GroupedFlux<K, V>> groupBy(Function<? super T, ? extends K> keyMapper,
			Function<? super T, ? extends V> valueMapper) {
		return groupBy(keyMapper, valueMapper, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Divide this sequence into dynamically created {@link Flux} (or groups) for each
	 * unique key, as produced by the provided keyMapper {@link Function}. Source elements
	 * are also mapped to a different value using the {@code valueMapper}. Note that
	 * groupBy works best with a low cardinality of groups, so chose your keyMapper
	 * function accordingly.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/groupByWithKeyMapperAndValueMapper.svg" alt="">
	 *
	 * <p>
	 * The groups need to be drained and consumed downstream for groupBy to work correctly.
	 * Notably when the criteria produces a large amount of groups, it can lead to hanging
	 * if the groups are not suitably consumed downstream (eg. due to a {@code flatMap}
	 * with a {@code maxConcurrency} parameter that is set too low).
	 *
	 * <p>
	 * To avoid deadlock, the concurrency of the subscriber to groupBy should be
	 * greater than or equal to the number of groups created. In that case every group
	 * has its own subscriber and progress can be made, even when the data publish pattern
	 * is arbitrary. Otherwise, when the number of groups exceeds downstream concurrency,
	 * the subscribers should be designed with caution, because if the consumption
	 * pattern doesn't match what can be accommodated in its producer buffer,
	 * the process may enter deadlock due to backpressure.
	 *
	 * <p>
	 * Note that groups are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a specific group more than once: groups are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * @param keyMapper the key mapping function that evaluates an incoming data and returns a key.
	 * @param valueMapper the value mapping function that evaluates which data to extract for re-routing.
	 * @param prefetch the number of values to prefetch from the source
	 *
	 * @param <K> the key type extracted from each value of this sequence
	 * @param <V> the value type extracted from each value of this sequence
	 *
	 * @return a {@link Flux} of {@link GroupedFlux} grouped sequences
	 *
	 */
	public final <K, V> Flux<GroupedFlux<K, V>> groupBy(Function<? super T, ? extends K> keyMapper,
			Function<? super T, ? extends V> valueMapper, int prefetch) {
		return onAssembly(new FluxGroupBy<>(this, keyMapper, valueMapper,
				Queues.unbounded(prefetch),
				Queues.unbounded(prefetch), prefetch));
	}

	/**
	 * Map values from two Publishers into time windows and emit combination of values
	 * in case their windows overlap. The emitted elements are obtained by passing the
	 * value from this {@link Flux} and a {@link Flux} emitting the value from the other
	 * {@link Publisher} to a {@link BiFunction}.
	 * <p>
	 * There are no guarantees in what order the items get combined when multiple items from
	 * one or both source Publishers overlap.
	 * <p>
	 * Unlike {@link Flux#join}, items from the second {@link Publisher} will be provided
	 * as a {@link Flux} to the {@code resultSelector}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/groupJoin.svg" alt="">
	 *
	 * @param other the other {@link Publisher} to correlate items with
	 * @param leftEnd a function that returns a Publisher whose emissions indicate the
	 * time window for the source value to be considered
	 * @param rightEnd a function that returns a Publisher whose emissions indicate the
	 * time window for the {@code right} Publisher value to be considered
	 * @param resultSelector a function that takes an item emitted by this {@link Flux} and
	 * a {@link Flux} representation of the overlapping item from the other {@link Publisher}
	 * and returns the value to be emitted by the resulting {@link Flux}
	 * @param <TRight> the type of the elements from the right {@link Publisher}
	 * @param <TLeftEnd> the type for this {@link Flux} window signals
	 * @param <TRightEnd> the type for the right {@link Publisher} window signals
	 * @param <R> the combined result type
	 *
	 * @return a joining {@link Flux}
	 * @see #join(Publisher, Function, Function, BiFunction)
	 */
	public final <TRight, TLeftEnd, TRightEnd, R> Flux<R> groupJoin(
			Publisher<? extends TRight> other,
			Function<? super T, ? extends Publisher<TLeftEnd>> leftEnd,
			Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd,
			BiFunction<? super T, ? super Flux<TRight>, ? extends R> resultSelector
	) {
		return onAssembly(new FluxGroupJoin<T, TRight, TLeftEnd, TRightEnd, R>(
				this, other, leftEnd, rightEnd, resultSelector,
				Queues.unbounded(Queues.XS_BUFFER_SIZE),
				Queues.unbounded(Queues.XS_BUFFER_SIZE)));
	}

	/**
	 * Handle the items emitted by this {@link Flux} by calling a biconsumer with the
	 * output sink for each onNext. At most one {@link SynchronousSink#next(Object)}
	 * call must be performed and/or 0 or 1 {@link SynchronousSink#error(Throwable)} or
	 * {@link SynchronousSink#complete()}.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors} (including when
	 * fusion is enabled) when the {@link BiConsumer} throws an exception or if an error is signaled explicitly via
	 * {@link SynchronousSink#error(Throwable)}.
	 * <p>
	 * When the <a href="https://github.com/micrometer-metrics/context-propagation">context-propagation library</a>
	 * is available at runtime and the downstream {@link ContextView} is not empty, this operator implicitly uses the
	 * library to restore thread locals around the handler {@link BiConsumer}. Typically, this would be done in conjunction
	 * with the use of {@link #contextCapture()} operator down the chain.
	 *
	 * @param handler the handling {@link BiConsumer}
	 * @param <R> the transformed type
	 * @return a transformed {@link Flux}
	 */
	public final <R> Flux<R> handle(BiConsumer<? super T, SynchronousSink<R>> handler) {
		if (this instanceof Fuseable) {
			return onAssembly(new FluxHandleFuseable<>(this, handler));
		}
		return onAssembly(new FluxHandle<>(this, handler));
	}

	/**
	 * Emit a single boolean true if any of the elements of this {@link Flux} sequence is
	 * equal to the provided value.
	 * <p>
	 * The implementation uses short-circuit logic and completes with true if
	 * an element matches the value.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/hasElementForFlux.svg" alt="">
	 *
	 * @param value constant compared to incoming signals
	 *
	 * @return a new {@link Flux} with <code>true</code> if any element is equal to a given value and <code>false</code>
	 * otherwise
	 *
	 */
	public final Mono<Boolean> hasElement(T value) {
		Objects.requireNonNull(value, "value");
		return any(t -> Objects.equals(value, t));
	}

	/**
	 * Emit a single boolean true if this {@link Flux} sequence has at least one element.
	 * <p>
	 * The implementation uses short-circuit logic and completes with true on onNext.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/hasElements.svg" alt="">
	 *
	 * @return a new {@link Mono} with <code>true</code> if any value is emitted and <code>false</code>
	 * otherwise
	 */
	public final Mono<Boolean> hasElements() {
		return Mono.onAssembly(new MonoHasElements<>(this));
	}

	/**
	 * Hides the identities of this {@link Flux} instance.
	 * <p>The main purpose of this operator is to prevent certain identity-based
	 * optimizations from happening, mostly for diagnostic purposes.
	 *
	 * @return a new {@link Flux} preventing {@link Publisher} / {@link Subscription} based Reactor optimizations
	 */
	public Flux<T> hide() {
		return new FluxHide<>(this);
	}

	/**
	 * Keep information about the order in which source values were received by
	 * indexing them with a 0-based incrementing long, returning a {@link Flux}
	 * of {@link Tuple2 Tuple2<(index, value)>}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/index.svg" alt="">
	 *
	 * @return an indexed {@link Flux} with each source value combined with its 0-based index.
	 */
	public final Flux<Tuple2<Long, T>> index() {
		return index(tuple2Function());
	}

	/**
	 * Keep information about the order in which source values were received by
	 * indexing them internally with a 0-based incrementing long then combining this
	 * information with the source value into a {@code I} using the provided {@link BiFunction},
	 * returning a {@link Flux Flux&lt;I&gt;}.
	 * <p>
	 * Typical usage would be to produce a {@link Tuple2} similar to {@link #index()}, but
	 * 1-based instead of 0-based:
	 * <p>
	 * {@code index((i, v) -> Tuples.of(i+1, v))}
	 * <p>
	 * <img class="marble" src="doc-files/marbles/indexWithMapper.svg" alt="">
	 *
	 * @param indexMapper the {@link BiFunction} to use to combine elements and their index.
	 * @return an indexed {@link Flux} with each source value combined with its computed index.
	 */
	public final <I> Flux<I> index(BiFunction<? super Long, ? super T, ? extends I> indexMapper) {
		if (this instanceof Fuseable) {
			return onAssembly(new FluxIndexFuseable<>(this, indexMapper));
		}
		return onAssembly(new FluxIndex<>(this, indexMapper));
	}

	/**
	 * Ignores onNext signals (dropping them) and only propagate termination events.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/ignoreElementsForFlux.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the upstream's elements.
	 *
	 * @return a new empty {@link Mono} representing the completion of this {@link Flux}.
	 */
	public final Mono<T> ignoreElements() {
		return Mono.onAssembly(new MonoIgnoreElements<>(this));
	}

	/**
	 * Combine values from two Publishers in case their windows overlap. Each incoming
	 * value triggers a creation of a new Publisher via the given {@link Function}. If the
	 * Publisher signals its first value or completes, the time windows for the original
	 * element is immediately closed. The emitted elements are obtained by passing the
	 * values from this {@link Flux} and the other {@link Publisher} to a {@link BiFunction}.
	 * <p>
	 * There are no guarantees in what order the items get combined when multiple items from
	 * one or both source Publishers overlap.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/join.svg" alt="">
	 *
	 *
	 * @param other the other {@link Publisher} to correlate items with
	 * @param leftEnd a function that returns a Publisher whose emissions indicate the
	 * time window for the source value to be considered
	 * @param rightEnd a function that returns a Publisher whose emissions indicate the
	 * time window for the {@code right} Publisher value to be considered
	 * @param resultSelector a function that takes an item emitted by each Publisher and returns the
	 * value to be emitted by the resulting {@link Flux}
	 * @param <TRight> the type of the elements from the right {@link Publisher}
	 * @param <TLeftEnd> the type for this {@link Flux} window signals
	 * @param <TRightEnd> the type for the right {@link Publisher} window signals
	 * @param <R> the combined result type
	 *
	 * @return a joining {@link Flux}
	 * @see #groupJoin(Publisher, Function, Function, BiFunction)
	 */
	public final <TRight, TLeftEnd, TRightEnd, R> Flux<R> join(
			Publisher<? extends TRight> other,
			Function<? super T, ? extends Publisher<TLeftEnd>> leftEnd,
			Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd,
			BiFunction<? super T, ? super TRight, ? extends R> resultSelector
	) {
		return onAssembly(new FluxJoin<T, TRight, TLeftEnd, TRightEnd, R>(
				this, other, leftEnd, rightEnd, resultSelector));
	}

	/**
	 * Emit the last element observed before complete signal as a {@link Mono}, or emit
	 * {@link NoSuchElementException} error if the source was empty.
	 * For a passive version use {@link #takeLast(int)}
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/last.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements before the last.
	 *
	 * @return a {@link Mono} with the last value in this {@link Flux}
	 */
    public final Mono<T> last() {
	    if (this instanceof Callable) {
		    @SuppressWarnings("unchecked")
		    Callable<T> thiz = (Callable<T>) this;
		    Mono<T> callableMono = wrapToMono(thiz);
		    if (callableMono == Mono.empty()) {
			    return Mono.onAssembly(new MonoError<>(new NoSuchElementException("Flux#last() didn't observe any onNext signal from Callable flux")));
		    }
	        return Mono.onAssembly(callableMono);
	    }
		return Mono.onAssembly(new MonoTakeLastOne<>(this));
	}

	/**
	 * Emit the last element observed before complete signal as a {@link Mono}, or emit
	 * the {@code defaultValue} if the source was empty.
	 * For a passive version use {@link #takeLast(int)}
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/lastWithDefault.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements before the last.
	 *
	 * @param defaultValue  a single fallback item if this {@link Flux} is empty
	 *
	 * @return a {@link Mono} with the last value in this {@link Flux}
	 */
    public final Mono<T> last(T defaultValue) {
	    if (this instanceof Callable) {
		    @SuppressWarnings("unchecked")
		    Callable<T> thiz = (Callable<T>)this;
		    if(thiz instanceof Fuseable.ScalarCallable){
			    Fuseable.ScalarCallable<T> c = (Fuseable.ScalarCallable<T>)thiz;
			    T v;
			    try {
				    v = c.call();
			    }
			    catch (Exception e) {
				    return Mono.error(Exceptions.unwrap(e));
			    }
			    if(v == null){
			    	return Mono.just(defaultValue);
			    }
			    return Mono.just(v);
		    }
		    Mono.onAssembly(new MonoCallable<>(thiz));
	    }
		return Mono.onAssembly(new MonoTakeLastOne<>(this, defaultValue));
	}

	/**
	 * Ensure that backpressure signals from downstream subscribers are split into batches
	 * capped at the provided {@code prefetchRate} when propagated upstream, effectively
	 * rate limiting the upstream {@link Publisher}.
	 * <p>
	 * Note that this is an upper bound, and that this operator uses a prefetch-and-replenish
	 * strategy, requesting a replenishing amount when 75% of the prefetch amount has been
	 * emitted.
	 * <p>
	 * Typically used for scenarios where consumer(s) request a large amount of data
	 * (eg. {@code Long.MAX_VALUE}) but the data source behaves better or can be optimized
	 * with smaller requests (eg. database paging, etc...). All data is still processed,
	 * unlike with {@link #take(long)} which will cap the grand total request
	 * amount.
	 * <p>
	 * Equivalent to {@code flux.publishOn(Schedulers.immediate(), prefetchRate).subscribe() }.
	 * Note that the {@code prefetchRate} is an upper bound, and that this operator uses a
	 * prefetch-and-replenish strategy, requesting a replenishing amount when 75% of the
	 * prefetch amount has been emitted.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/limitRate.svg" alt="">
	 *
	 * @param prefetchRate the limit to apply to downstream's backpressure
	 *
	 * @return a {@link Flux} limiting downstream's backpressure
	 * @see #publishOn(Scheduler, int)
	 * @see #take(long)
	 */
	public final Flux<T> limitRate(int prefetchRate) {
		return onAssembly(this.publishOn(Schedulers.immediate(), prefetchRate));
	}

	/**
	 * Ensure that backpressure signals from downstream subscribers are split into batches
	 * capped at the provided {@code highTide} first, then replenishing at the provided
	 * {@code lowTide}, effectively rate limiting the upstream {@link Publisher}.
	 * <p>
	 * Note that this is an upper bound, and that this operator uses a prefetch-and-replenish
	 * strategy, requesting a replenishing amount when 75% of the prefetch amount has been
	 * emitted.
	 * <p>
	 * Typically used for scenarios where consumer(s) request a large amount of data
	 * (eg. {@code Long.MAX_VALUE}) but the data source behaves better or can be optimized
	 * with smaller requests (eg. database paging, etc...). All data is still processed,
	 * unlike with {@link #take(long)} which will cap the grand total request
	 * amount.
	 * <p>
	 * Similar to {@code flux.publishOn(Schedulers.immediate(), prefetchRate).subscribe() },
	 * except with a customized "low tide" instead of the default 75%.
	 * Note that the smaller the lowTide is, the higher the potential for concurrency
	 * between request and data production. And thus the more extraneous replenishment
	 * requests this operator could make. For example, for a global downstream
	 * request of 14, with a highTide of 10 and a lowTide of 2, the operator would perform
	 * low tide requests ({@code request(2)}) seven times in a row, whereas with the default
	 * lowTide of 8 it would only perform one low tide request ({@code request(8)}).
	 * Using a {@code lowTide} equal to {@code highTide} reverts to the default 75% strategy,
	 * while using a {@code lowTide} of {@literal 0} disables the lowTide, resulting in
	 * all requests strictly adhering to the highTide.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/limitRateWithHighAndLowTide.svg" alt="">
	 *
	 * @param highTide the initial request amount
	 * @param lowTide the subsequent (or replenishing) request amount, {@literal 0} to
	 * disable early replenishing, {@literal highTide} to revert to a 75% replenish strategy.
	 *
	 * @return a {@link Flux} limiting downstream's backpressure and customizing the
	 * replenishment request amount
	 * @see #publishOn(Scheduler, int)
	 * @see #take(long)
	 */
	public final Flux<T> limitRate(int highTide, int lowTide) {
		return onAssembly(this.publishOn(Schedulers.immediate(), true, highTide, lowTide));
	}

	/**
	 * Take only the first N values from this {@link Flux}, if available.
	 * Furthermore, ensure that the total amount requested upstream is capped at {@code n}.
	 * If n is zero, the source isn't even subscribed to and the operator completes immediately
	 * upon subscription.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeLimitRequestTrue.svg" alt="">
	 * <p>
	 * Backpressure signals from downstream subscribers are smaller than the cap are
	 * propagated as is, but if they would cause the total requested amount to go over the
	 * cap, they are reduced to the minimum value that doesn't go over.
	 * <p>
	 * As a result, this operator never let the upstream produce more elements than the
	 * cap.
	 * Typically useful for cases where a race between request and cancellation can lead the upstream to
	 * producing a lot of extraneous data, and such a production is undesirable (e.g.
	 * a source that would send the extraneous data over the network).
	 *
	 * @param n the number of elements to emit from this flux, which is also the backpressure
	 * cap for all of downstream's request
	 *
	 * @return a {@link Flux} of {@code n} elements from the source, that requests AT MOST {@code n} from upstream in total.
	 * @see #take(long)
	 * @see #take(long, boolean)
	 * @deprecated replace with {@link #take(long, boolean) take(n, true)} in 3.4.x, then {@link #take(long)} in 3.5.0.
	 * To be removed in 3.6.0 at the earliest. See https://github.com/reactor/reactor-core/issues/2339
	 */
	@Deprecated
	public final Flux<T> limitRequest(long n) {
		return take(n, true);
	}

	/**
	 * Observe all Reactive Streams signals and trace them using {@link Logger} support.
	 * Default will use {@link Level#INFO} and {@code java.util.logging}.
	 * If SLF4J is available, it will be used instead.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 * <p>
	 * The default log category will be "reactor.Flux.", followed by a suffix generated from
	 * the source operator, e.g. "reactor.Flux.Map".
	 *
	 * @return a new {@link Flux} that logs signals
	 */
	public final Flux<T> log() {
		return log(null, Level.INFO);
	}

	/**
	 * Observe all Reactive Streams signals and trace them using {@link Logger} support.
	 * Default will use {@link Level#INFO} and {@code java.util.logging}.
	 * If SLF4J is available, it will be used instead.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator
	 * suffix will be added, e.g. "reactor.Flux.Map".
	 *
	 * @return a new {@link Flux} that logs signals
	 */
	public final Flux<T> log(String category) {
		return log(category, Level.INFO);
	}

	/**
	 * Observe Reactive Streams signals matching the passed filter {@code options} and
	 * trace them using {@link Logger} support. Default will use {@link Level#INFO} and
	 * {@code java.util.logging}. If SLF4J is available, it will be used instead.
	 * <p>
	 * Options allow fine grained filtering of the traced signal, for instance to only
	 * capture onNext and onError:
	 * <pre>
	 *     flux.log("category", Level.INFO, SignalType.ON_NEXT, SignalType.ON_ERROR)
	 * </pre>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator
	 * suffix will be added, e.g. "reactor.Flux.Map".
	 * @param level the {@link Level} to enforce for this tracing Flux (only FINEST, FINE,
	 * INFO, WARNING and SEVERE are taken into account)
	 * @param options a vararg {@link SignalType} option to filter log messages
	 *
	 * @return a new {@link Flux} that logs signals
	 */
	public final Flux<T> log(@Nullable String category, Level level, SignalType... options) {
		return log(category, level, false, options);
	}

	/**
	 * Observe Reactive Streams signals matching the passed filter {@code options} and
	 * trace them using {@link Logger} support. Default will use {@link Level#INFO} and
	 * {@code java.util.logging}. If SLF4J is available, it will be used instead.
	 * <p>
	 * Options allow fine grained filtering of the traced signal, for instance to only
	 * capture onNext and onError:
	 * <pre>
	 *     flux.log("category", Level.INFO, SignalType.ON_NEXT, SignalType.ON_ERROR)
	 * </pre>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator
	 * suffix will be added, e.g. "reactor.Flux.Map".
	 * @param level the {@link Level} to enforce for this tracing Flux (only FINEST, FINE,
	 * INFO, WARNING and SEVERE are taken into account)
	 * @param showOperatorLine capture the current stack to display operator class/line number.
	 * @param options a vararg {@link SignalType} option to filter log messages
	 *
	 * @return a new {@link Flux} that logs signals
	 */
	public final Flux<T> log(@Nullable String category,
			Level level,
			boolean showOperatorLine,
			SignalType... options) {
		SignalLogger<T> log = new SignalLogger<>(this, category, level,
				showOperatorLine, options);

		if (this instanceof Fuseable) {
			return onAssembly(new FluxLogFuseable<>(this, log));
		}
		return onAssembly(new FluxLog<>(this, log));
	}

	/**
	 * Observe Reactive Streams signals matching the passed filter {@code options} and
	 * trace them using a specific user-provided {@link Logger}, at {@link Level#INFO} level.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param logger the {@link Logger} to use, instead of resolving one through a category.
	 *
	 * @return a new {@link Flux} that logs signals
	 */
	public final Flux<T> log(Logger logger) {
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
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param logger the {@link Logger} to use, instead of resolving one through a category.
	 * @param level the {@link Level} to enforce for this tracing Flux (only FINEST, FINE,
	 * INFO, WARNING and SEVERE are taken into account)
	 * @param showOperatorLine capture the current stack to display operator class/line number (default in overload is false).
	 * @param options a vararg {@link SignalType} option to filter log messages
	 *
	 * @return a new {@link Flux} that logs signals
	 */
	public final Flux<T> log(Logger logger,
			Level level,
			boolean showOperatorLine,
			SignalType... options) {
		SignalLogger<T> log = new SignalLogger<>(this, "IGNORED", level,
				showOperatorLine,
				s -> logger,
				options);

		if (this instanceof Fuseable) {
			return onAssembly(new FluxLogFuseable<>(this, log));
		}
		return onAssembly(new FluxLog<>(this, log));
	}

	/**
	 * Transform the items emitted by this {@link Flux} by applying a synchronous function
	 * to each item.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mapForFlux.svg" alt="">
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * (including when fusion is enabled). Exceptions thrown by the mapper then cause the
	 * source value to be dropped and a new element ({@code request(1)}) being requested
	 * from upstream.
	 *
	 * @param mapper the synchronous transforming {@link Function}
	 *
	 * @param <V> the transformed type
	 *
	 * @return a transformed {@link Flux}
	 */
	public final <V> Flux<V> map(Function<? super T, ? extends V> mapper) {
		if (this instanceof Fuseable) {
			return onAssembly(new FluxMapFuseable<>(this, mapper));
		}
		return onAssembly(new FluxMap<>(this, mapper));
	}

	/**
	 * Transform the items emitted by this {@link Flux} by applying a synchronous function
	 * to each item, which may produce {@code null} values. In that case, no value is emitted.
	 * This operator effectively behaves like {@link #map(Function)} followed by {@link #filter(Predicate)}
	 * although {@code null} is not a supported value, so it can't be filtered out.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mapNotNullForFlux.svg" alt="">
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link #onErrorContinue(BiConsumer) resuming on errors}
	 * (including when fusion is enabled). Exceptions thrown by the mapper then cause the
	 * source value to be dropped and a new element ({@code request(1)}) being requested
	 * from upstream.
	 *
	 * @param mapper the synchronous transforming {@link Function}
	 *
	 * @param <V> the transformed type
	 *
	 * @return a transformed {@link Flux}
	 */
	public final <V> Flux<V> mapNotNull(Function <? super T, ? extends V> mapper) {
		return this.handle((t, sink) -> {
			V v = mapper.apply(t);
			if (v != null) {
				sink.next(v);
			}
		});
	}

	/**
	 * Transform incoming onNext, onError and onComplete signals into {@link Signal} instances,
	 * materializing these signals.
	 * Since the error is materialized as a {@code Signal}, the propagation will be stopped and onComplete will be
	 * emitted. Complete signal will first emit a {@code Signal.complete()} and then effectively complete the flux.
	 * All these {@link Signal} have a {@link Context} associated to them.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/materializeForFlux.svg" alt="">
	 *
	 * @return a {@link Flux} of materialized {@link Signal}
	 * @see #dematerialize()
	 */
	public final Flux<Signal<T>> materialize() {
		return onAssembly(new FluxMaterialize<>(this));
	}

	/**
	 * Merge data from this {@link Flux} and a {@link Publisher} into a reordered merge
	 * sequence, by picking the smallest value from each sequence as defined by a provided
	 * {@link Comparator}. Note that subsequent calls are combined, and their comparators are
	 * in lexicographic order as defined by {@link Comparator#thenComparing(Comparator)}.
	 * <p>
	 * The combination step is avoided if the two {@link Comparator Comparators} are
	 * {@link Comparator#equals(Object) equal} (which can easily be achieved by using the
	 * same reference, and is also always true of {@link Comparator#naturalOrder()}).
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 * <p>
	 * Note that it is delaying errors until all data is consumed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparingWith.svg" alt="">
	 *
	 * @param other the {@link Publisher} to merge with
	 * @param otherComparator the {@link Comparator} to use for merging
	 *
	 * @return a new {@link Flux} that compares latest values from the given publisher
	 * and this flux, using the smallest value and replenishing the source that produced it
	 * @deprecated Use {@link #mergeComparingWith(Publisher, Comparator)} instead
	 * (with the caveat that it defaults to NOT delaying errors, unlike this operator).
	 * To be removed in 3.6.0 at the earliest.
	 */
	@Deprecated
	public final Flux<T> mergeOrderedWith(Publisher<? extends T> other,
			Comparator<? super T> otherComparator) {
		if (this instanceof FluxMergeComparing) {
			FluxMergeComparing<T> fluxMerge = (FluxMergeComparing<T>) this;
			return fluxMerge.mergeAdditionalSource(other, otherComparator);
		}
		return mergeOrdered(otherComparator, this, other);
	}

	/**
	 * Merge data from this {@link Flux} and a {@link Publisher} into a reordered merge
	 * sequence, by picking the smallest value from each sequence as defined by a provided
	 * {@link Comparator}. Note that subsequent calls are combined, and their comparators are
	 * in lexicographic order as defined by {@link Comparator#thenComparing(Comparator)}.
	 * <p>
	 * The combination step is avoided if the two {@link Comparator Comparators} are
	 * {@link Comparator#equals(Object) equal} (which can easily be achieved by using the
	 * same reference, and is also always true of {@link Comparator#naturalOrder()}).
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparingWith.svg" alt="">
	 * <p>
	 * mergeComparingWith doesn't delay errors by default, but it will inherit the delayError
	 * behavior of a mergeComparingDelayError directly above it.
	 *
	 * @param other the {@link Publisher} to merge with
	 * @param otherComparator the {@link Comparator} to use for merging
	 *
	 * @return a new {@link Flux} that compares latest values from the given publisher
	 * and this flux, using the smallest value and replenishing the source that produced it
	 */
	public final Flux<T> mergeComparingWith(Publisher<? extends T> other,
			Comparator<? super T> otherComparator) {
		if (this instanceof FluxMergeComparing) {
			FluxMergeComparing<T> fluxMerge = (FluxMergeComparing<T>) this;
			return fluxMerge.mergeAdditionalSource(other, otherComparator);
		}
		return mergeComparing(otherComparator, this, other);
	}

	/**
	 * Merge data from this {@link Flux} and a {@link Publisher} into an interleaved merged
	 * sequence. Unlike {@link #concatWith(Publisher) concat}, inner sources are subscribed
	 * to eagerly.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeWithForFlux.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param other the {@link Publisher} to merge with
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> mergeWith(Publisher<? extends T> other) {
		if (this instanceof FluxMerge) {
			FluxMerge<T> fluxMerge = (FluxMerge<T>) this;
			return fluxMerge.mergeAdditionalSource(other, Queues::get);
		}
		return merge(this, other);
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
	 *
	 * @return an instrumented {@link Flux}
	 *
	 * @see #name(String)
	 * @see #tag(String, String)
	 * @deprecated Prefer using the {@link #tap(SignalListenerFactory)} with the {@link SignalListenerFactory} provided by
	 * the new reactor-core-micrometer module. To be removed in 3.6.0 at the earliest.
	 */
	@Deprecated
	public final Flux<T> metrics() {
		if (!Metrics.isInstrumentationAvailable()) {
			return this;
		}

		if (this instanceof Fuseable) {
			return onAssembly(new FluxMetricsFuseable<>(this));
		}
		return onAssembly(new FluxMetrics<>(this));
	}

	/**
	 * Give a name to this sequence, which can be retrieved using {@link Scannable#name()}
	 * as long as this is the first reachable {@link Scannable#parents()}.
	 * <p>
	 * The name is typically visible at assembly time by the {@link #tap(SignalListenerFactory)} operator,
	 * which could for example be configured with a metrics listener using the name as a prefix for meters' id.
	 *
	 * @param name a name for the sequence
	 *
	 * @return the same sequence, but bearing a name
	 *
	 *@see #metrics()
	 *@see #tag(String, String)
	 */
	public final Flux<T> name(String name) {
		return FluxName.createOrAppend(this, name);
	}

	/**
	 * Emit only the first item emitted by this {@link Flux}, into a new {@link Mono}.
	 * If called on an empty {@link Flux}, emits an empty {@link Mono}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/next.svg" alt="">
	 *
	 * @return a new {@link Mono} emitting the first value in this {@link Flux}
	 */
	public final Mono<T> next() {
		if(this instanceof Callable){
			@SuppressWarnings("unchecked")
			Callable<T> m = (Callable<T>)this;
			return Mono.onAssembly(wrapToMono(m));
		}
		return Mono.onAssembly(new MonoNext<>(this));
	}

	/**
	 * Evaluate each accepted value against the given {@link Class} type. If the
	 * value matches the type, it is passed into the resulting {@link Flux}. Otherwise
	 * the value is ignored and a request of 1 is emitted.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/ofTypeForFlux.svg" alt="">
	 *
	 * @param clazz the {@link Class} type to test values against
	 *
	 * @return a new {@link Flux} filtered on items of the requested type
	 */
	public final <U> Flux<U> ofType(final Class<U> clazz) {
			Objects.requireNonNull(clazz, "clazz");
			return filter(o -> clazz.isAssignableFrom(o.getClass())).cast(clazz);
	}

	/**
	 * Request an unbounded demand and push to the returned {@link Flux}, or park the
	 * observed elements if not enough demand is requested downstream. Errors will be
	 * delayed until the buffer gets consumed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onBackpressureBuffer.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the buffered overflow elements upon cancellation or error triggered by a data signal.
	 *
	 * @return a backpressured {@link Flux} that buffers with unbounded capacity
	 *
	 */
	public final Flux<T> onBackpressureBuffer() {
		return onAssembly(new FluxOnBackpressureBuffer<>(this, Queues
				.SMALL_BUFFER_SIZE, true, null));
	}

	/**
	 * Request an unbounded demand and push to the returned {@link Flux}, or park up to
	 * {@code maxSize} elements when not enough demand is requested downstream.
	 * The first element past this buffer to arrive out of sync with the downstream
	 * subscriber's demand (the "overflowing" element) immediately triggers an overflow
	 * error and cancels the source.
	 * The {@link Flux} is going to terminate with an overflow error, but this error is
	 * delayed, which lets the subscriber make more requests for the content of the buffer.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onBackpressureBufferWithMaxSize.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the buffered overflow elements upon cancellation or error triggered by a data signal,
	 * as well as elements that are rejected by the buffer due to {@code maxSize}.
	 *
	 * @param maxSize maximum number of elements overflowing request before the source is cancelled
	 *
	 * @return a backpressured {@link Flux} that buffers with bounded capacity
	 *
	 */
	public final Flux<T> onBackpressureBuffer(int maxSize) {
		return onAssembly(new FluxOnBackpressureBuffer<>(this, maxSize, false, null));
	}

	/**
	 * Request an unbounded demand and push to the returned {@link Flux}, or park up to
	 * {@code maxSize} elements when not enough demand is requested downstream.
	 * The first element past this buffer to arrive out of sync with the downstream
	 * subscriber's demand (the "overflowing" element) is immediately passed to a
	 * {@link Consumer} and the source is cancelled.
	 * The {@link Flux} is going to terminate with an overflow error, but this error is
	 * delayed, which lets the subscriber make more requests for the content of the buffer.
	 * <p>
	 * Note that should the cancelled source produce further overflowing elements, these
	 * would be passed to the {@link Hooks#onNextDropped(Consumer) onNextDropped hook}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onBackpressureBufferWithMaxSizeConsumer.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the buffered overflow elements upon cancellation or error triggered by a data signal,
	 * as well as elements that are rejected by the buffer due to {@code maxSize} (even though
	 * they are passed to the {@code onOverflow} {@link Consumer} first).
	 *
	 * @param maxSize maximum number of elements overflowing request before callback is called and source is cancelled
	 * @param onOverflow callback to invoke on overflow
	 *
	 * @return a backpressured {@link Flux} that buffers with a bounded capacity
	 *
	 */
	public final Flux<T> onBackpressureBuffer(int maxSize, Consumer<? super T> onOverflow) {
		Objects.requireNonNull(onOverflow, "onOverflow");
		return onAssembly(new FluxOnBackpressureBuffer<>(this, maxSize, false, onOverflow));
	}

	/**
	 * Request an unbounded demand and push to the returned {@link Flux}, or park the observed
	 * elements if not enough demand is requested downstream, within a {@code maxSize}
	 * limit. Over that limit, the overflow strategy is applied (see {@link BufferOverflowStrategy}).
	 * <p>
	 * Note that for the {@link BufferOverflowStrategy#ERROR ERROR} strategy, the overflow
	 * error will be delayed after the current backlog is consumed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onBackpressureBufferWithMaxSizeStrategyDropOldest.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the buffered overflow elements upon cancellation or error triggered by a data signal,
	 * as well as elements that are rejected by the buffer due to {@code maxSize} (even though
	 * they are passed to the {@code bufferOverflowStrategy} first).
	 *
	 *
	 * @param maxSize maximum buffer backlog size before overflow strategy is applied
	 * @param bufferOverflowStrategy strategy to apply to overflowing elements
	 *
	 * @return a backpressured {@link Flux} that buffers up to a capacity then applies an
	 * overflow strategy
	 */
	public final Flux<T> onBackpressureBuffer(int maxSize, BufferOverflowStrategy bufferOverflowStrategy) {
		Objects.requireNonNull(bufferOverflowStrategy, "bufferOverflowStrategy");
		return onAssembly(new FluxOnBackpressureBufferStrategy<>(this, maxSize,
				null, bufferOverflowStrategy));
	}

	/**
	 * Request an unbounded demand and push to the returned {@link Flux}, or park the observed
	 * elements if not enough demand is requested downstream, within a {@code maxSize}
	 * limit. Over that limit, the overflow strategy is applied (see {@link BufferOverflowStrategy}).
	 * <p>
	 * A {@link Consumer} is immediately invoked when there is an overflow, receiving the
	 * value that was discarded because of the overflow (which can be different from the
	 * latest element emitted by the source in case of a
	 * {@link BufferOverflowStrategy#DROP_LATEST DROP_LATEST} strategy).
	 *
	 * <p>
	 * Note that for the {@link BufferOverflowStrategy#ERROR ERROR} strategy, the overflow
	 * error will be delayed after the current backlog is consumed. The consumer is still
	 * invoked immediately.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onBackpressureBufferWithMaxSizeStrategyDropOldest.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the buffered overflow elements upon cancellation or error triggered by a data signal,
	 * as well as elements that are rejected by the buffer due to {@code maxSize} (even though
	 * they are passed to the {@code onOverflow} {@link Consumer} AND the {@code bufferOverflowStrategy} first).
	 *
	 * @param maxSize maximum buffer backlog size before overflow callback is called
	 * @param onBufferOverflow callback to invoke on overflow
	 * @param bufferOverflowStrategy strategy to apply to overflowing elements
	 *
	 * @return a backpressured {@link Flux} that buffers up to a capacity then applies an
	 * overflow strategy
	 */
	public final Flux<T> onBackpressureBuffer(int maxSize, Consumer<? super T> onBufferOverflow,
			BufferOverflowStrategy bufferOverflowStrategy) {
		Objects.requireNonNull(onBufferOverflow, "onBufferOverflow");
		Objects.requireNonNull(bufferOverflowStrategy, "bufferOverflowStrategy");
		return onAssembly(new FluxOnBackpressureBufferStrategy<>(this, maxSize,
				onBufferOverflow, bufferOverflowStrategy));
	}

	/**
	 * Request an unbounded demand and push to the returned {@link Flux}, or park the observed
	 * elements if not enough demand is requested downstream, within a {@code maxSize}
	 * limit and for a maximum {@link Duration} of {@code ttl} (as measured on the
	 * {@link Schedulers#parallel() parallel Scheduler}). Over that limit, oldest
	 * elements from the source are dropped.
	 * <p>
	 * Elements evicted based on the TTL are passed to a cleanup {@link Consumer}, which
	 * is also immediately invoked when there is an overflow.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onBackpressureBufferWithDurationAndMaxSize.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards its internal buffer of elements that overflow,
	 * after having applied the {@code onBufferEviction} handler.
	 *
	 * @param ttl maximum {@link Duration} for which an element is kept in the backlog
	 * @param maxSize maximum buffer backlog size before overflow callback is called
	 * @param onBufferEviction callback to invoke once TTL is reached or on overflow
	 *
	 * @return a backpressured {@link Flux} that buffers with a TTL and up to a capacity then applies an
	 * overflow strategy
	 */
	public final Flux<T> onBackpressureBuffer(Duration ttl, int maxSize, Consumer<? super T> onBufferEviction) {
		return onBackpressureBuffer(ttl, maxSize, onBufferEviction, Schedulers.parallel());
	}

	/**
	 * Request an unbounded demand and push to the returned {@link Flux}, or park the observed
	 * elements if not enough demand is requested downstream, within a {@code maxSize}
	 * limit and for a maximum {@link Duration} of {@code ttl} (as measured on the provided
	 * {@link Scheduler}). Over that limit, oldest elements from the source are dropped.
	 * <p>
	 * Elements evicted based on the TTL are passed to a cleanup {@link Consumer}, which
	 * is also immediately invoked when there is an overflow.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onBackpressureBufferWithDurationAndMaxSize.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards its internal buffer of elements that overflow,
	 * after having applied the {@code onBufferEviction} handler.
	 *
	 * @param ttl maximum {@link Duration} for which an element is kept in the backlog
	 * @param maxSize maximum buffer backlog size before overflow callback is called
	 * @param onBufferEviction callback to invoke once TTL is reached or on overflow
	 * @param scheduler the scheduler on which to run the timeout check
	 *
	 * @return a backpressured {@link Flux} that buffers with a TTL and up to a capacity then applies an
	 * overflow strategy
	 */
	public final Flux<T> onBackpressureBuffer(Duration ttl, int maxSize, Consumer<? super T> onBufferEviction, Scheduler scheduler) {
		Objects.requireNonNull(ttl, "ttl");
		Objects.requireNonNull(onBufferEviction, "onBufferEviction");
		return onAssembly(new FluxOnBackpressureBufferTimeout<>(this, ttl, scheduler, maxSize, onBufferEviction));
	}

	/**
	 * Request an unbounded demand and push to the returned {@link Flux}, or drop
	 * the observed elements if not enough demand is requested downstream.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onBackpressureDrop.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that it drops.
	 *
	 * @return a backpressured {@link Flux} that drops overflowing elements
	 */
	public final Flux<T> onBackpressureDrop() {
		return onAssembly(new FluxOnBackpressureDrop<>(this));
	}

	/**
	 * Request an unbounded demand and push to the returned {@link Flux}, or drop and
	 * notify dropping {@link Consumer} with the observed elements if not enough demand
	 * is requested downstream.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onBackpressureDropWithConsumer.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that it drops after having passed
	 * them to the provided {@code onDropped} handler.
	 *
	 * @param onDropped the Consumer called when a value gets dropped due to lack of downstream requests
	 * @return a backpressured {@link Flux} that drops overflowing elements
	 */
	public final Flux<T> onBackpressureDrop(Consumer<? super T> onDropped) {
		return onAssembly(new FluxOnBackpressureDrop<>(this, onDropped));
	}

	/**
	 * Request an unbounded demand and push to the returned {@link Flux}, or emit onError
	 * fom {@link Exceptions#failWithOverflow} if not enough demand is requested
	 * downstream.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onBackpressureError.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that it drops, after having propagated
	 * the error.
	 *
	 * @return a backpressured {@link Flux} that errors on overflowing elements
	 */
	public final Flux<T> onBackpressureError() {
		return onBackpressureDrop(t -> { throw Exceptions.failWithOverflow();});
	}

	/**
	 * Request an unbounded demand and push to the returned {@link Flux}, or only keep
	 * the most recent observed item if not enough demand is requested downstream.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onBackpressureLatest.svg" alt="">
	 * <p>
	 * <p><strong>Discard Support:</strong> Each time a new element comes in (the new "latest"), this operator
	 * discards the previously retained element.
	 *
	 * @return a backpressured {@link Flux} that will only keep a reference to the last observed item
	 */
	public final Flux<T> onBackpressureLatest() {
		return onAssembly(new FluxOnBackpressureLatest<>(this));
	}

	/**
	 * Simply complete the sequence by replacing an {@link Subscriber#onError(Throwable) onError signal}
	 * with an {@link Subscriber#onComplete() onComplete signal}. All other signals are propagated as-is.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorCompleteForFlux.svg" alt="">
	 *
	 * @return a new {@link Flux} falling back on completion when an onError occurs
	 * @see #onErrorReturn(Object)
	 */
	public final Flux<T> onErrorComplete() {
		return onAssembly(new FluxOnErrorReturn<>(this, null, null));
	}

	/**
	 * Simply complete the sequence by replacing an {@link Subscriber#onError(Throwable) onError signal}
	 * with an {@link Subscriber#onComplete() onComplete signal} if the error matches the given
	 * {@link Class}. All other signals, including non-matching onError, are propagated as-is.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorCompleteForFlux.svg" alt="">
	 *
	 * @return a new {@link Flux} falling back on completion when a matching error occurs
	 * @see #onErrorReturn(Class, Object)
	 */
	public final Flux<T> onErrorComplete(Class<? extends Throwable> type) {
		Objects.requireNonNull(type, "type must not be null");
		return onErrorComplete(type::isInstance);
	}

	/**
	 * Simply complete the sequence by replacing an {@link Subscriber#onError(Throwable) onError signal}
	 * with an {@link Subscriber#onComplete() onComplete signal} if the error matches the given
	 * {@link Predicate}. All other signals, including non-matching onError, are propagated as-is.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorCompleteForFlux.svg" alt="">
	 *
	 * @return a new {@link Flux} falling back on completion when a matching error occurs
	 * @see #onErrorReturn(Predicate, Object)
	 */
	public final Flux<T> onErrorComplete(Predicate<? super Throwable> predicate) {
		Objects.requireNonNull(predicate, "predicate must not be null");
		return onAssembly(new FluxOnErrorReturn<>(this, predicate, null));
	}

	/**
	 * Let compatible operators <strong>upstream</strong> recover from errors by dropping the
	 * incriminating element from the sequence and continuing with subsequent elements.
	 * The recovered error and associated value are notified via the provided {@link BiConsumer}.
	 * Alternatively, throwing from that biconsumer will propagate the thrown exception downstream
	 * in place of the original error, which is added as a suppressed exception to the new one.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorContinue.svg" alt="">
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
	 * @param errorConsumer a {@link BiConsumer} fed with errors matching the predicate and the value
	 * that triggered the error.
	 * @return a {@link Flux} that attempts to continue processing on errors.
	 */
	public final Flux<T> onErrorContinue(BiConsumer<Throwable, Object> errorConsumer) {
		BiConsumer<Throwable, Object> genericConsumer = errorConsumer;
		return contextWriteSkippingContextPropagation(Context.of(
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
	 * <img class="marble" src="doc-files/marbles/onErrorContinueWithClassPredicate.svg" alt="">
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
	 * @return a {@link Flux} that attempts to continue processing on some errors.
	 */
	public final <E extends Throwable> Flux<T> onErrorContinue(Class<E> type, BiConsumer<Throwable, Object> errorConsumer) {
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
	 * <img class="marble" src="doc-files/marbles/onErrorContinueWithPredicate.svg" alt="">
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
	 * @return a {@link Flux} that attempts to continue processing on some errors.
	 */
	public final <E extends Throwable> Flux<T> onErrorContinue(Predicate<E> errorPredicate,
			BiConsumer<Throwable, Object> errorConsumer) {
		//this cast is ok as only T values will be propagated in this sequence
		@SuppressWarnings("unchecked")
		Predicate<Throwable> genericPredicate = (Predicate<Throwable>) errorPredicate;
		BiConsumer<Throwable, Object> genericErrorConsumer = errorConsumer;
		return contextWriteSkippingContextPropagation(Context.of(
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
	 * @return a {@link Flux} that terminates on errors, even if {@link #onErrorContinue(BiConsumer)}
	 * was used downstream
	 */
	public final Flux<T> onErrorStop() {
		return contextWriteSkippingContextPropagation(Context.of(
				OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY,
				OnNextFailureStrategy.stop()));
	}

	/**
	 * Transform any error emitted by this {@link Flux} by synchronously applying a function to it.

	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorMapForFlux.svg" alt="">
	 *
	 * @param mapper the error transforming {@link Function}
	 *
	 * @return a {@link Flux} that transforms source errors to other errors
	 */
	public final Flux<T> onErrorMap(Function<? super Throwable, ? extends Throwable> mapper) {
		return onErrorResume(e -> Mono.error(mapper.apply(e)));
	}

	/**
	 * Transform an error emitted by this {@link Flux} by synchronously applying a function
	 * to it if the error matches the given type. Otherwise let the error pass through.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorMapWithClassPredicateForFlux.svg" alt="">
	 *
	 * @param type the class of the exception type to react to
	 * @param mapper the error transforming {@link Function}
	 * @param <E> the error type
	 *
	 * @return a {@link Flux} that transforms some source errors to other errors
	 */
	public final <E extends Throwable> Flux<T> onErrorMap(Class<E> type,
			Function<? super E, ? extends Throwable> mapper) {
		@SuppressWarnings("unchecked")
		Function<Throwable, Throwable> handler = (Function<Throwable, Throwable>)mapper;
		return onErrorMap(type::isInstance, handler);
	}

	/**
	 * Transform an error emitted by this {@link Flux} by synchronously applying a function
	 * to it if the error matches the given predicate. Otherwise let the error pass through.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorMapWithPredicateForFlux.svg" alt="">
	 *
	 * @param predicate the error predicate
	 * @param mapper the error transforming {@link Function}
	 *
	 * @return a {@link Flux} that transforms some source errors to other errors
	 */
	public final Flux<T> onErrorMap(Predicate<? super Throwable> predicate,
			Function<? super Throwable, ? extends Throwable> mapper) {
		return onErrorResume(predicate, e -> Mono.error(mapper.apply(e)));
	}

	/**
	 * Subscribe to a returned fallback publisher when any error occurs, using a function to
	 * choose the fallback depending on the error.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorResumeForFlux.svg" alt="">
	 *
	 * @param fallback the function to choose the fallback to an alternative {@link Publisher}
	 *
	 * @return a {@link Flux} falling back upon source onError
	 */
	public final Flux<T> onErrorResume(Function<? super Throwable, ? extends Publisher<? extends T>> fallback) {
		return onAssembly(new FluxOnErrorResume<>(this, fallback));
	}

	/**
	 * Subscribe to a fallback publisher when an error matching the given type
	 * occurs, using a function to choose the fallback depending on the error.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorResumeForFlux.svg" alt="">
	 *
	 * @param type the error type to match
	 * @param fallback the function to choose the fallback to an alternative {@link Publisher}
	 * @param <E> the error type
	 *
	 * @return a {@link Flux} falling back upon source onError
	 */
	public final <E extends Throwable> Flux<T> onErrorResume(Class<E> type,
			Function<? super E, ? extends Publisher<? extends T>> fallback) {
		Objects.requireNonNull(type, "type");
		@SuppressWarnings("unchecked")
		Function<? super Throwable, Publisher<? extends T>> handler = (Function<?
				super Throwable, Publisher<? extends T>>)fallback;
		return onErrorResume(type::isInstance, handler);
	}

	/**
	 * Subscribe to a fallback publisher when an error matching a given predicate
	 * occurs.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorResumeForFlux.svg" alt="">
	 *
	 * @param predicate the error predicate to match
	 * @param fallback the function to choose the fallback to an alternative {@link Publisher}
	 *
	 * @return a {@link Flux} falling back upon source onError
	 */
	public final Flux<T> onErrorResume(Predicate<? super Throwable> predicate,
			Function<? super Throwable, ? extends Publisher<? extends T>> fallback) {
		Objects.requireNonNull(predicate, "predicate");
		return onErrorResume(e -> predicate.test(e) ? fallback.apply(e) : error(e));
	}

	/**
	 * Simply emit a captured fallback value when any error is observed on this {@link Flux}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorReturnForFlux.svg" alt="">
	 *
	 * @param fallbackValue the value to emit if an error occurs
	 *
	 * @return a new falling back {@link Flux}
	 * @see #onErrorComplete()
	 */
	public final Flux<T> onErrorReturn(T fallbackValue) {
		Objects.requireNonNull(fallbackValue, "fallbackValue must not be null");
		return onAssembly(new FluxOnErrorReturn<>(this, null, fallbackValue));
	}

	/**
	 * Simply emit a captured fallback value when an error of the specified type is
	 * observed on this {@link Flux}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorReturnForFlux.svg" alt="">
	 *
	 * @param type the error type to match
	 * @param fallbackValue the value to emit if an error occurs that matches the type
	 * @param <E> the error type
	 *
	 * @return a new falling back {@link Flux}
	 * @see #onErrorComplete(Class)
	 */
	public final <E extends Throwable> Flux<T> onErrorReturn(Class<E> type, T fallbackValue) {
		Objects.requireNonNull(type, "type must not be null");
		return onErrorReturn(type::isInstance, fallbackValue);
	}

	/**
	 * Simply emit a captured fallback value when an error matching the given predicate is
	 * observed on this {@link Flux}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorReturnForFlux.svg" alt="">
	 *
	 * @param predicate the error predicate to match
	 * @param fallbackValue the value to emit if an error occurs that matches the predicate
	 *
	 * @return a new falling back {@link Flux}
	 * @see #onErrorComplete(Predicate)
	 */
	public final Flux<T> onErrorReturn(Predicate<? super Throwable> predicate, T fallbackValue) {
		Objects.requireNonNull(predicate, "predicate must not be null");
		Objects.requireNonNull(fallbackValue, "fallbackValue must not be null");
		return onAssembly(new FluxOnErrorReturn<>(this, predicate, fallbackValue));
	}

	/**
	 * Detaches both the child {@link Subscriber} and the {@link Subscription} on
	 * termination or cancellation.
	 * <p>This is an advanced interoperability operator that should help with odd
	 * retention scenarios when running with non-reactor {@link Subscriber}.
	 *
	 * @return a detachable {@link Flux}
	 */
	public final Flux<T> onTerminateDetach() {
		return new FluxDetach<>(this);
	}


	/**
	 * Pick the first {@link Publisher} between this {@link Flux} and another publisher
	 * to emit any signal (onNext/onError/onComplete) and replay all signals from that
	 * {@link Publisher}, effectively behaving like the fastest of these competing sources.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/orForFlux.svg" alt="">
	 *
	 * @param other the {@link Publisher} to race with
	 *
	 * @return the fastest sequence
	 * @see #firstWithSignal
	 */
	public final Flux<T> or(Publisher<? extends T> other) {
		if (this instanceof FluxFirstWithSignal) {
			FluxFirstWithSignal<T> orPublisher = (FluxFirstWithSignal<T>) this;

			FluxFirstWithSignal<T> result = orPublisher.orAdditionalSource(other);
			if (result != null) {
				return result;
			}
		}
		return firstWithSignal(this, other);
	}

	/**
	 * Prepare this {@link Flux} by dividing data on a number of 'rails' matching the
	 * number of CPU cores, in a round-robin fashion. Note that to actually perform the
	 * work in parallel, you should call {@link ParallelFlux#runOn(Scheduler)} afterward.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/parallel.svg" alt="">
	 *
	 * @return a new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> parallel() {
		return parallel(Schedulers.DEFAULT_POOL_SIZE);
	}

	/**
	 * Prepare this {@link Flux} by dividing data on a number of 'rails' matching the
	 * provided {@code parallelism} parameter, in a round-robin fashion. Note that to
	 * actually perform the work in parallel, you should call {@link ParallelFlux#runOn(Scheduler)}
	 * afterward.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/parallel.svg" alt="">
	 *
	 * @param parallelism the number of parallel rails
	 *
	 * @return a new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> parallel(int parallelism) {
		return parallel(parallelism, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Prepare this {@link Flux} by dividing data on a number of 'rails' matching the
	 * provided {@code parallelism} parameter, in a round-robin fashion and using a
	 * custom prefetch amount and queue for dealing with the source {@link Flux}'s values.
	 * Note that to actually perform the work in parallel, you should call
	 * {@link ParallelFlux#runOn(Scheduler)} afterward.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/parallel.svg" alt="">
	 *
	 * @param parallelism the number of parallel rails
	 * @param prefetch the number of values to prefetch from the source
	 *
	 * @return a new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> parallel(int parallelism, int prefetch) {
		return ParallelFlux.from(this,
				parallelism,
				prefetch,
				Queues.get(prefetch));
	}

	/**
	 * Prepare a {@link ConnectableFlux} which shares this {@link Flux} sequence and
	 * dispatches values to subscribers in a backpressure-aware manner. Prefetch will
	 * default to {@link Queues#SMALL_BUFFER_SIZE}. This will effectively turn
	 * any type of sequence into a hot sequence.
	 * <p>
	 * Backpressure will be coordinated on {@link Subscription#request} and if any
	 * {@link Subscriber} is missing demand (requested = 0), multicast will pause
	 * pushing/pulling.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/publish.svg" alt="">
	 *
	 * @return a new {@link ConnectableFlux}
	 */
	public final ConnectableFlux<T> publish() {
		return publish(Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Prepare a {@link ConnectableFlux} which shares this {@link Flux} sequence and
	 * dispatches values to subscribers in a backpressure-aware manner. This will
	 * effectively turn any type of sequence into a hot sequence.
	 * <p>
	 * Backpressure will be coordinated on {@link Subscription#request} and if any
	 * {@link Subscriber} is missing demand (requested = 0), multicast will pause
	 * pushing/pulling.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/publish.svg" alt="">
	 *
	 * @param prefetch bounded requested demand
	 *
	 * @return a new {@link ConnectableFlux}
	 */
	public final ConnectableFlux<T> publish(int prefetch) {
		return onAssembly(new FluxPublish<>(this, prefetch, Queues
				.get(prefetch), true));
	}

	/**
	 * Shares a sequence for the duration of a function that may transform it and
	 * consume it as many times as necessary without causing multiple subscriptions
	 * to the upstream.
	 *
	 * @param transform the transformation function
	 * @param <R> the output value type
	 *
	 * @return a new {@link Flux}
	 */
	public final <R> Flux<R> publish(Function<? super Flux<T>, ? extends Publisher<?
			extends R>> transform) {
		return publish(transform, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Shares a sequence for the duration of a function that may transform it and
	 * consume it as many times as necessary without causing multiple subscriptions
	 * to the upstream.
	 *
	 * @param transform the transformation function
	 * @param prefetch the request size
	 * @param <R> the output value type
	 *
	 * @return a new {@link Flux}
	 */
	public final <R> Flux<R> publish(Function<? super Flux<T>, ? extends Publisher<?
			extends R>> transform, int prefetch) {
		return onAssembly(new FluxPublishMulticast<>(this, transform, prefetch, Queues
				.get(prefetch)));
	}

	/**
	 * Prepare a {@link Mono} which shares this {@link Flux} sequence and dispatches the
	 * first observed item to subscribers in a backpressure-aware manner.
	 * This will effectively turn any type of sequence into a hot sequence when the first
	 * {@link Subscriber} subscribes.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/publishNext.svg" alt="">
	 *
	 * @return a new {@link Mono}
	 * @deprecated use {@link #shareNext()} instead, or use `publish().next()` if you need
	 * to `{@link ConnectableFlux#connect() connect()}. To be removed in 3.5.0
	 */
	@Deprecated
	public final Mono<T> publishNext() {
		//Should add a ConnectableMono to align with #publish()
		return shareNext();
	}

	/**
	 * Run onNext, onComplete and onError on a supplied {@link Scheduler}
	 * {@link Worker Worker}.
	 * <p>
	 * This operator influences the threading context where the rest of the operators in
	 * the chain below it will execute, up to a new occurrence of {@code publishOn}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/publishOnForFlux.svg" alt="">
	 * <p>
	 * Typically used for fast publisher, slow consumer(s) scenarios.
	 * <blockquote><pre>
	 * {@code flux.publishOn(Schedulers.single()).subscribe() }
	 * </pre></blockquote>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation or error triggered by a data signal.
	 *
	 * @param scheduler a {@link Scheduler} providing the {@link Worker} where to publish
	 *
	 * @return a {@link Flux} producing asynchronously on a given {@link Scheduler}
	 */
	public final Flux<T> publishOn(Scheduler scheduler) {
		return publishOn(scheduler, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Run onNext, onComplete and onError on a supplied {@link Scheduler}
	 * {@link Worker}.
	 * <p>
	 * This operator influences the threading context where the rest of the operators in
	 * the chain below it will execute, up to a new occurrence of {@code publishOn}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/publishOnForFlux.svg" alt="">
	 * <p>
	 * Typically used for fast publisher, slow consumer(s) scenarios.
	 * <blockquote><pre>
	 * {@code flux.publishOn(Schedulers.single()).subscribe() }
	 * </pre></blockquote>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation or error triggered by a data signal.
	 *
	 * @param scheduler a {@link Scheduler} providing the {@link Worker} where to publish
	 * @param prefetch the asynchronous boundary capacity
	 *
	 * @return a {@link Flux} producing asynchronously
	 */
	public final Flux<T> publishOn(Scheduler scheduler, int prefetch) {
		return publishOn(scheduler, true, prefetch);
	}

	/**
	 * Run onNext, onComplete and onError on a supplied {@link Scheduler}
	 * {@link Worker}.
	 * <p>
	 * This operator influences the threading context where the rest of the operators in
	 * the chain below it will execute, up to a new occurrence of {@code publishOn}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/publishOnForFlux.svg" alt="">
	 * <p>
	 * Typically used for fast publisher, slow consumer(s) scenarios.
	 * <blockquote><pre>
	 * {@code flux.publishOn(Schedulers.single()).subscribe() }
	 * </pre></blockquote>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation or error triggered by a data signal.
	 *
	 * @param scheduler a {@link Scheduler} providing the {@link Worker} where to publish
	 * @param delayError should the buffer be consumed before forwarding any error
	 * @param prefetch the asynchronous boundary capacity
	 *
	 * @return a {@link Flux} producing asynchronously
	 */
	public final Flux<T> publishOn(Scheduler scheduler, boolean delayError, int prefetch) {
		return publishOn(scheduler, delayError, prefetch, prefetch);
	}

	final Flux<T> publishOn(Scheduler scheduler, boolean delayError, int prefetch, int lowTide) {
		if (this instanceof Callable) {
			if (this instanceof Fuseable.ScalarCallable) {
				@SuppressWarnings("unchecked")
				Fuseable.ScalarCallable<T> s = (Fuseable.ScalarCallable<T>) this;
				try {
					return onAssembly(new FluxSubscribeOnValue<>(s.call(), scheduler));
				}
				catch (Exception e) {
					//leave FluxSubscribeOnCallable defer exception call
				}
			}
			@SuppressWarnings("unchecked")
			Callable<T> c = (Callable<T>)this;
			return onAssembly(new FluxSubscribeOnCallable<>(c, scheduler));
		}

		return onAssembly(new FluxPublishOn<>(this, scheduler, delayError, prefetch, lowTide, Queues.get(prefetch)));
	}

	/**
	 * Reduce the values from this {@link Flux} sequence into a single object of the same
	 * type than the emitted items. Reduction is performed using a {@link BiFunction} that
	 * takes the intermediate result of the reduction and the current value and returns
	 * the next intermediate value of the reduction. Note, {@link BiFunction} will not
	 * be invoked for a sequence with 0 or 1 elements. In case of one element's
	 * sequence, the result will be directly sent to the subscriber.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/reduceWithSameReturnType.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the internally accumulated value upon cancellation or error.
	 *
	 * @param aggregator the reducing {@link BiFunction}
	 *
	 * @return a reduced {@link Flux}
	 */
	public final Mono<T> reduce(BiFunction<T, T, T> aggregator) {
		if (this instanceof Callable){
			@SuppressWarnings("unchecked")
			Callable<T> thiz = (Callable<T>)this;
		    return Mono.onAssembly(wrapToMono(thiz));
		}
	    return Mono.onAssembly(new MonoReduce<>(this, aggregator));
	}

	/**
	 * Reduce the values from this {@link Flux} sequence into a single object matching the
	 * type of a seed value. Reduction is performed using a {@link BiFunction} that
	 * takes the intermediate result of the reduction and the current value and returns
	 * the next intermediate value of the reduction. First element is paired with the seed
	 * value, {@literal initial}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/reduce.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the internally accumulated value upon cancellation or error.
	 *
	 * @param accumulator the reducing {@link BiFunction}
	 * @param initial the seed, the initial leftmost argument to pass to the reducing {@link BiFunction}
	 * @param <A> the type of the seed and the reduced object
	 *
	 * @return a reduced {@link Flux}
	 *
	 */
	public final <A> Mono<A> reduce(A initial, BiFunction<A, ? super T, A> accumulator) {
		return reduceWith(() -> initial, accumulator);
	}

	/**
	 * Reduce the values from this {@link Flux} sequence into a single object matching the
	 * type of a lazily supplied seed value. Reduction is performed using a
	 * {@link BiFunction} that takes the intermediate result of the reduction and the
	 * current value and returns the next intermediate value of the reduction. First
	 * element is paired with the seed value, supplied via {@literal initial}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/reduceWith.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the internally accumulated value upon cancellation or error.
	 *
	 * @param accumulator the reducing {@link BiFunction}
	 * @param initial a {@link Supplier} of the seed, called on subscription and passed to the the reducing {@link BiFunction}
	 * @param <A> the type of the seed and the reduced object
	 *
	 * @return a reduced {@link Flux}
	 *
	 */
	public final <A> Mono<A> reduceWith(Supplier<A> initial, BiFunction<A, ? super T, A> accumulator) {
		return Mono.onAssembly(new MonoReduceSeed<>(this, initial, accumulator));
	}

	/**
	 * Repeatedly and indefinitely subscribe to the source upon completion of the
	 * previous subscription.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatForFlux.svg" alt="">
	 *
	 * @return an indefinitely repeated {@link Flux} on onComplete
	 */
	public final Flux<T> repeat() {
		return repeat(ALWAYS_BOOLEAN_SUPPLIER);
	}

	/**
	 * Repeatedly subscribe to the source if the predicate returns true after completion of the previous subscription.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatWithPredicateForFlux.svg" alt="">
	 *
	 * @param predicate the boolean to evaluate on onComplete.
	 *
	 * @return a {@link Flux} that repeats on onComplete while the predicate matches
	 */
	public final Flux<T> repeat(BooleanSupplier predicate) {
		return onAssembly(new FluxRepeatPredicate<>(this, predicate));
	}

	/**
	 * Repeatedly subscribe to the source {@code numRepeat} times. This results in
	 * {@code numRepeat + 1} total subscriptions to the original source. As a consequence,
	 * using 0 plays the original sequence once.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatWithAttemptsForFlux.svg" alt="">
	 *
	 * @param numRepeat the number of times to re-subscribe on onComplete (positive, or 0 for original sequence only)
	 *
	 * @return a {@link Flux} that repeats on onComplete, up to the specified number of repetitions
	 */
	public final Flux<T> repeat(long numRepeat) {
		if(numRepeat == 0L){
			return this;
		}
		return onAssembly(new FluxRepeat<>(this, numRepeat));
	}

	/**
	 * Repeatedly subscribe to the source if the predicate returns true after completion of the previous
	 * subscription. A specified maximum of repeat will limit the number of re-subscribe.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatWithAttemptsAndPredicateForFlux.svg" alt="">
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
			return this;
		}
		return defer( () -> repeat(countingBooleanSupplier(predicate, numRepeat)));
	}

	/**
	 * Repeatedly subscribe to this {@link Flux} when a companion sequence emits elements in
	 * response to the flux completion signal. Any terminal signal from the companion
	 * sequence will terminate the resulting {@link Flux} with the same signal immediately.
	 * <p>If the companion sequence signals when this {@link Flux} is active, the repeat
	 * attempt is suppressed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/repeatWhenForFlux.svg" alt="">
	 * <p>
	 * Note that if the companion {@link Publisher} created by the {@code repeatFactory}
	 * emits {@link Context} as trigger objects, these {@link Context} will be merged with
	 * the previous Context:
	 * <pre><code>
	 * .repeatWhen(emittedEachAttempt -> emittedEachAttempt.handle((lastEmitted, sink) -> {
	 * 	    Context ctx = sink.currentContext();
	 * 	    int rl = ctx.getOrDefault("repeatsLeft", 0);
	 * 	    if (rl > 0) {
	 *		    sink.next(Context.of(
	 *		        "repeatsLeft", rl - 1,
	 *		        "emitted", lastEmitted
	 *		    ));
	 * 	    } else {
	 * 	        sink.error(new IllegalStateException("repeats exhausted"));
	 * 	    }
	 * }))
	 * </code></pre>
	 *
	 * @param repeatFactory the {@link Function} that returns the associated {@link Publisher}
	 * companion, given a {@link Flux} that signals each onComplete as a {@link Long}
	 * representing the number of source elements emitted in the latest attempt.
	 *
	 * @return a {@link Flux} that repeats on onComplete when the companion {@link Publisher} produces an
	 * onNext signal
	 */
	public final Flux<T> repeatWhen(Function<Flux<Long>, ? extends Publisher<?>> repeatFactory) {
		return onAssembly(new FluxRepeatWhen<>(this, repeatFactory));
	}

	/**
	 * Turn this {@link Flux} into a hot source and cache last emitted signals for further {@link Subscriber}. Will
	 * retain an unbounded amount of onNext signals. Completion and Error will also be
	 * replayed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/replay.svg" alt="">
	 *
	 * @return a replaying {@link ConnectableFlux}
	 */
	public final ConnectableFlux<T> replay() {
		return replay(Integer.MAX_VALUE);
	}

	/**
	 * Turn this {@link Flux} into a connectable hot source and cache last emitted
	 * signals for further {@link Subscriber}.
	 * Will retain up to the given history size onNext signals. Completion and Error will also be
	 * replayed.
	 * <p>
	 *     Note that {@code replay(0)} will only cache the terminal signal without
	 *     expiration.
	 *
	 * <p>
	 *     Re-connects are not supported.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/replayWithHistory.svg" alt="">
	 *
	 * @param history number of events retained in history excluding complete and
	 * error
	 *
	 * @return a replaying {@link ConnectableFlux}
	 *
	 */
	public final ConnectableFlux<T> replay(int history) {
		if (history == 0) {
			return onAssembly(new FluxPublish<>(this, Queues.SMALL_BUFFER_SIZE,
					Queues.get(Queues.SMALL_BUFFER_SIZE), false));
		}
		return onAssembly(new FluxReplay<>(this, history, 0L, null));
	}

	/**
	 * Turn this {@link Flux} into a connectable hot source and cache last emitted signals
	 * for further {@link Subscriber}. Will retain each onNext up to the given per-item
	 * expiry timeout.
	 * <p>
	 *   Completion and Error will also be replayed until {@code ttl} triggers in which case
	 *   the next {@link Subscriber} will start over a new subscription
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/replayWithTtl.svg" alt="">
	 *
	 * @param ttl Per-item and post termination timeout duration
	 *
	 * @return a replaying {@link ConnectableFlux}
	 */
	public final ConnectableFlux<T> replay(Duration ttl) {
		return replay(Integer.MAX_VALUE, ttl);
	}

	/**
	 * Turn this {@link Flux} into a connectable hot source and cache last emitted signals
	 * for further {@link Subscriber}. Will retain up to the given history size onNext
	 * signals with a per-item ttl.
	 * <p>
	 *   Completion and Error will also be replayed until {@code ttl} triggers in which case
	 *   the next {@link Subscriber} will start over a new subscription
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/replayWithHistoryAndTtl.svg" alt="">
	 *
	 * @param history number of events retained in history excluding complete and error
	 * @param ttl Per-item and post termination timeout duration
	 *
	 * @return a replaying {@link ConnectableFlux}
	 */
	public final ConnectableFlux<T> replay(int history, Duration ttl) {
		return replay(history, ttl, Schedulers.parallel());
	}

	/**
	 * Turn this {@link Flux} into a connectable hot source and cache last emitted signals
	 * for further {@link Subscriber}. Will retain onNext signal for up to the given
	 * {@link Duration} with a per-item ttl.
	 * <p>
	 *   Completion and Error will also be replayed until {@code ttl} triggers in which case
	 *   the next {@link Subscriber} will start over a new subscription
	 * <p>
	 * <img class="marble" src="doc-files/marbles/replayWithTtl.svg" alt="">
	 *
	 * @param ttl Per-item and post termination timeout duration
	 * @param timer a time-capable {@link Scheduler} instance to read current time from
	 *
	 * @return a replaying {@link ConnectableFlux}
	 */
	public final ConnectableFlux<T> replay(Duration ttl, Scheduler timer) {
		return replay(Integer.MAX_VALUE, ttl, timer);
	}

	/**
	 * Turn this {@link Flux} into a connectable hot source and cache last emitted signals
	 * for further {@link Subscriber}. Will retain up to the given history size onNext
	 * signals with a per-item ttl.
	 * <p>
	 *   Completion and Error will also be replayed until {@code ttl} triggers in which case
	 *   the next {@link Subscriber} will start over a new subscription
	 * <p>
	 * <img class="marble" src="doc-files/marbles/replayWithHistoryAndTtl.svg" alt="">
	 *
	 * @param history number of events retained in history excluding complete and error
	 * @param ttl Per-item and post termination timeout duration
	 * @param timer a {@link Scheduler} instance to read current time from
	 *
	 * @return a replaying {@link ConnectableFlux}
	 */
	public final ConnectableFlux<T> replay(int history, Duration ttl, Scheduler timer) {
		Objects.requireNonNull(timer, "timer");
		if (history == 0) {
			return onAssembly(new FluxPublish<>(this, Queues.SMALL_BUFFER_SIZE,
					Queues.get(Queues.SMALL_BUFFER_SIZE), true));
		}
		return onAssembly(new FluxReplay<>(this, history, ttl.toNanos(), timer));
	}

	/**
	 * Re-subscribes to this {@link Flux} sequence if it signals any error, indefinitely.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/retryForFlux.svg" alt="">
	 *
	 * @return a {@link Flux} that retries on onError
	 */
	public final Flux<T> retry() {
		return retry(Long.MAX_VALUE);
	}

	/**
	 * Re-subscribes to this {@link Flux} sequence if it signals any error, for a fixed
	 * number of times.
	 * <p>
	 * Note that passing {@literal Long.MAX_VALUE} is treated as infinite retry.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/retryWithAttemptsForFlux.svg" alt="">
	 *
	 * @param numRetries the number of times to tolerate an error
	 *
	 * @return a {@link Flux} that retries on onError up to the specified number of retry attempts.
	 *
	 */
	public final Flux<T> retry(long numRetries) {
		return onAssembly(new FluxRetry<>(this, numRetries));
	}

	/**
	 * Retries this {@link Flux} in response to signals emitted by a companion {@link Publisher}.
	 * The companion is generated by the provided {@link Retry} instance, see {@link Retry#max(long)}, {@link Retry#maxInARow(long)}
	 * and {@link Retry#backoff(long, Duration)} for readily available strategy builders.
	 * <p>
	 * The operator generates a base for the companion, a {@link Flux} of {@link reactor.util.retry.Retry.RetrySignal}
	 * which each give metadata about each retryable failure whenever this {@link Flux} signals an error. The final companion
	 * should be derived from that base companion and emit data in response to incoming onNext (although it can emit less
	 * elements, or delay the emissions).
	 * <p>
	 * Terminal signals in the companion terminate the sequence with the same signal, so emitting an {@link Subscriber#onError(Throwable)}
	 * will fail the resulting {@link Flux} with that same error.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/retryWhenSpecForFlux.svg" alt="">
	 * <p>
	 * Note that the {@link reactor.util.retry.Retry.RetrySignal} state can be
	 * transient and change between each source
	 * {@link org.reactivestreams.Subscriber#onError(Throwable) onError} or
	 * {@link org.reactivestreams.Subscriber#onNext(Object) onNext}. If processed with a delay,
	 * this could lead to the represented state being out of sync with the state at which the retry
	 * was evaluated. Map it to {@link reactor.util.retry.Retry.RetrySignal#copy()}
	 * right away to mediate this.
	 * <p>
	 * Note that if the companion {@link Publisher} created by the {@code whenFactory}
	 * emits {@link Context} as trigger objects, these {@link Context} will be merged with
	 * the previous Context:
	 * <blockquote><pre>
	 * {@code
	 * Retry customStrategy = Retry.from(companion -> companion.handle((retrySignal, sink) -> {
	 * 	    ContextView ctx = sink.contextView();
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
	 * Flux<T> retried = originalFlux.retryWhen(customStrategy);
	 * }</pre>
	 * </blockquote>
	 *
	 * @param retrySpec the {@link Retry} strategy that will generate the companion {@link Publisher},
	 * given a {@link Flux} that signals each onError as a {@link reactor.util.retry.Retry.RetrySignal}.
	 *
	 * @return a {@link Flux} that retries on onError when a companion {@link Publisher} produces an onNext signal
	 * @see reactor.util.retry.Retry#max(long)
	 * @see reactor.util.retry.Retry#maxInARow(long)
	 * @see reactor.util.retry.Retry#backoff(long, Duration)
	 */
	public final Flux<T> retryWhen(Retry retrySpec) {
		return onAssembly(new FluxRetryWhen<>(this, retrySpec));
	}

	/**
	 * Sample this {@link Flux} by periodically emitting an item corresponding to that
	 * {@link Flux} latest emitted value within the periodical time window.
	 * Note that if some elements are emitted quicker than the timespan just before source
	 * completion, the last of these elements will be emitted along with the onComplete
	 * signal.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sampleAtRegularInterval.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are not part of the sampling.
	 *
	 * @param timespan the duration of the window after which to emit the latest observed item
	 *
	 * @return a {@link Flux} sampled to the last item seen over each periodic window
	 */
	public final Flux<T> sample(Duration timespan) {
		return sample(interval(timespan));
	}

	/**
	 * Sample this {@link Flux} by emitting an item corresponding to that {@link Flux}
	 * latest emitted value whenever a companion sampler {@link Publisher} signals a value.
	 * <p>
	 * Termination of either {@link Publisher} will result in termination for the {@link Subscriber}
	 * as well.
	 * Note that if some elements are emitted just before source completion and before a
	 * last sampler can trigger, the last of these elements will be emitted along with the
	 * onComplete signal.
	 * <p>
	 * Both {@link Publisher} will run in unbounded mode because the backpressure
	 * would interfere with the sampling precision.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sampleWithSampler.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are not part of the sampling.
	 *
	 * @param sampler the sampler companion {@link Publisher}
	 *
	 * @param <U> the type of the sampler sequence
	 *
	 * @return a {@link Flux} sampled to the last item observed each time the sampler {@link Publisher} signals
	 */
	public final <U> Flux<T> sample(Publisher<U> sampler) {
		return onAssembly(new FluxSample<>(this, sampler));
	}

	/**
	 * Repeatedly take a value from this {@link Flux} then skip the values that follow
	 * within a given duration.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sampleFirstAtRegularInterval.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are not part of the sampling.
	 *
	 * @param timespan the duration during which to skip values after each sample
	 *
	 * @return a {@link Flux} sampled to the first item of each duration-based window
	 */
	public final Flux<T> sampleFirst(Duration timespan) {
		return sampleFirst(t -> Mono.delay(timespan));
	}

	/**
	 * Repeatedly take a value from this {@link Flux} then skip the values that follow
	 * before the next signal from a companion sampler {@link Publisher}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sampleFirstWithSamplerFactory.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are not part of the sampling.
	 *
	 * @param samplerFactory supply a companion sampler {@link Publisher} which signals the end of the skip window
	 * @param <U> the companion reified type
	 *
	 * @return a {@link Flux} sampled to the first item observed in each window closed by the sampler signals
	 */
	public final <U> Flux<T> sampleFirst(Function<? super T, ? extends Publisher<U>> samplerFactory) {
		return onAssembly(new FluxSampleFirst<>(this, samplerFactory));
	}

	/**
	 * Emit the latest value from this {@link Flux} only if there were no new values emitted
	 * during the window defined by a companion {@link Publisher} derived from that particular
	 * value.
	 * <p>
	 * Note that this means that the last value in the sequence is always emitted.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sampleTimeoutWithThrottlerFactory.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are not part of the sampling.
	 *
	 * @param throttlerFactory supply a companion sampler {@link Publisher} which signals
	 * the end of the window during which no new emission should occur. If it is the case,
	 * the original value triggering the window is emitted.
	 * @param <U> the companion reified type
	 *
	 * @return a {@link Flux} sampled to items not followed by any other item within a window
	 * defined by a companion {@link Publisher}
	 */
	public final <U> Flux<T> sampleTimeout(Function<? super T, ? extends Publisher<U>> throttlerFactory) {
		return sampleTimeout(throttlerFactory, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Emit the latest value from this {@link Flux} only if there were no new values emitted
	 * during the window defined by a companion {@link Publisher} derived from that particular
	 * value.
	 * <p>The provided {@literal maxConcurrency} will keep a bounded maximum of concurrent timeouts and drop any new
	 * items until at least one timeout terminates.
	 * <p>
	 * Note that this means that the last value in the sequence is always emitted.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sampleTimeoutWithThrottlerFactoryAndMaxConcurrency.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are not part of the sampling.
	 *
	 * @param throttlerFactory supply a companion sampler {@link Publisher} which signals
	 * the end of the window during which no new emission should occur. If it is the case,
	 * the original value triggering the window is emitted.
	 * @param maxConcurrency the maximum number of concurrent timeouts
	 * @param <U> the throttling type
	 *
	 * @return a {@link Flux} sampled to items not followed by any other item within a window
	 * defined by a companion {@link Publisher}
	 */
	//FIXME re-evaluate the wording on maxConcurrency, seems more related to request/buffering. If so redo the marble
	public final <U> Flux<T> sampleTimeout(Function<? super T, ? extends Publisher<U>>
			throttlerFactory, int maxConcurrency) {
		return onAssembly(new FluxSampleTimeout<>(this, throttlerFactory,
				Queues.get(maxConcurrency)));
	}

	/**
	 * Reduce this {@link Flux} values with an accumulator {@link BiFunction} and
	 * also emit the intermediate results of this function.
	 * <p>
	 * Unlike {@link #scan(Object, BiFunction)}, this operator doesn't take an initial value
	 * but treats the first {@link Flux} value as initial value.
	 * <br>
	 * The accumulation works as follows:
	 * <pre><code>
	 * result[0] = source[0]
	 * result[1] = accumulator(result[0], source[1])
	 * result[2] = accumulator(result[1], source[2])
	 * result[3] = accumulator(result[2], source[3])
	 * ...
	 * </code></pre>
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/scanWithSameReturnType.svg" alt="">
	 *
	 * @param accumulator the accumulating {@link BiFunction}
	 *
	 * @return an accumulating {@link Flux}
	 */
	public final Flux<T> scan(BiFunction<T, T, T> accumulator) {
		return onAssembly(new FluxScan<>(this, accumulator));
	}

	/**
	 * Reduce this {@link Flux} values with an accumulator {@link BiFunction} and
	 * also emit the intermediate results of this function.
	 * <p>
	 * The accumulation works as follows:
	 * <pre><code>
	 * result[0] = initialValue;
	 * result[1] = accumulator(result[0], source[0])
	 * result[2] = accumulator(result[1], source[1])
	 * result[3] = accumulator(result[2], source[2])
	 * ...
	 * </code></pre>
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/scan.svg" alt="">
	 *
	 * @param initial the initial leftmost argument to pass to the reduce function
	 * @param accumulator the accumulating {@link BiFunction}
	 * @param <A> the accumulated type
	 *
	 * @return an accumulating {@link Flux} starting with initial state
	 *
	 */
	public final <A> Flux<A> scan(A initial, BiFunction<A, ? super T, A> accumulator) {
		Objects.requireNonNull(initial, "seed");
		return scanWith(() -> initial, accumulator);
	}

	/**
	 * Reduce this {@link Flux} values with the help of an accumulator {@link BiFunction}
	 * and also emits the intermediate results. A seed value is lazily provided by a
	 * {@link Supplier} invoked for each {@link Subscriber}.
	 * <p>
	 * The accumulation works as follows:
	 * <pre><code>
	 * result[0] = initialValue;
	 * result[1] = accumulator(result[0], source[0])
	 * result[2] = accumulator(result[1], source[1])
	 * result[3] = accumulator(result[2], source[2])
	 * ...
	 * </code></pre>
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/scanWith.svg" alt="">
	 *
	 * @param initial the supplier providing the seed, the leftmost parameter initially
	 * passed to the reduce function
	 * @param accumulator the accumulating {@link BiFunction}
	 * @param <A> the accumulated type
	 *
	 * @return an accumulating {@link Flux} starting with initial state
	 *
	 */
	public final <A> Flux<A> scanWith(Supplier<A> initial, BiFunction<A, ? super T, A>
			accumulator) {
		return onAssembly(new FluxScanSeed<>(this, initial, accumulator));
	}

	/**
	 * Returns a new {@link Flux} that multicasts (shares) the original {@link Flux}.
	 * As long as there is at least one {@link Subscriber} this {@link Flux} will be subscribed and
	 * emitting data.
	 * When all subscribers have cancelled it will cancel the source
	 * {@link Flux}.
	 * <p>This is an alias for {@link #publish()}.{@link ConnectableFlux#refCount()}.
	 *
	 * @return a {@link Flux} that upon first subscribe causes the source {@link Flux}
	 * to subscribe once, late subscribers might therefore miss items.
	 */
	public final Flux<T> share() {
		return onAssembly(
				new FluxRefCount<>(new FluxPublish<>(
						this, Queues.SMALL_BUFFER_SIZE, Queues.small(), true
				), 1)
		);
	}

	/**
	 * Prepare a {@link Mono} which shares this {@link Flux} sequence and dispatches the
	 * first observed item to subscribers.
	 * This will effectively turn any type of sequence into a hot sequence when the first
	 * {@link Subscriber} subscribes.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/shareNext.svg" alt="">
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> shareNext() {
		final NextProcessor<T> nextProcessor = new NextProcessor<>(this);
		return Mono.onAssembly(nextProcessor);
	}

	/**
	 * Expect and emit a single item from this {@link Flux} source or signal
	 * {@link java.util.NoSuchElementException} for an empty source, or
	 * {@link IndexOutOfBoundsException} for a source with more than one element.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/singleForFlux.svg" alt="">
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
		return Mono.onAssembly(new MonoSingle<>(this));
	}

	/**
	 * Expect and emit a single item from this {@link Flux} source and emit a default
	 * value for an empty source, but signal an {@link IndexOutOfBoundsException} for a
	 * source with more than one element.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/singleWithDefault.svg" alt="">
	 *
	 * @param defaultValue  a single fallback item if this {@link Flux} is empty
	 *
	 * @return a {@link Mono} with the expected single item, the supplied default value or
	 * an error signal
	 */
	public final Mono<T> single(T defaultValue) {
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
					return Mono.just(defaultValue);
				}
				return Mono.just(v);
			}
			@SuppressWarnings("unchecked")
			Callable<T> thiz = (Callable<T>)this;
			return Mono.onAssembly(new MonoSingleCallable<>(thiz, defaultValue));
		}
		return Mono.onAssembly(new MonoSingle<>(this, defaultValue, false));
	}

	/**
	 * Expect and emit a single item from this {@link Flux} source, and accept an empty
	 * source but signal an {@link IndexOutOfBoundsException} for a source with more than
	 * one element.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/singleOrEmpty.svg" alt="">
	 *
	 * @return a {@link Mono} with the expected single item, no item or an error
	 */
    public final Mono<T> singleOrEmpty() {
	    if (this instanceof Callable) {
		    @SuppressWarnings("unchecked")
		    Callable<T> thiz = (Callable<T>)this;
	        return Mono.onAssembly(wrapToMono(thiz));
	    }
		return Mono.onAssembly(new MonoSingle<>(this, null, true));
	}

	/**
	 * Skip the specified number of elements from the beginning of this {@link Flux} then
	 * emit the remaining elements.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/skip.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are skipped.
	 *
	 * @param skipped the number of elements to drop
	 *
	 * @return a dropping {@link Flux} with the specified number of elements skipped at
	 * the beginning
	 */
	public final Flux<T> skip(long skipped) {
		if (skipped == 0L) {
			return this;
		}
		else {
			return onAssembly(new FluxSkip<>(this, skipped));
		}
	}

	/**
	 * Skip elements from this {@link Flux} emitted within the specified initial duration.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/skipWithTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are skipped.
	 *
	 * @param timespan the initial time window during which to drop elements
	 *
	 * @return a {@link Flux} dropping at the beginning until the end of the given duration
	 */
	public final Flux<T> skip(Duration timespan) {
		return skip(timespan, Schedulers.parallel());
	}

	/**
	 * Skip elements from this {@link Flux} emitted within the specified initial duration,
	 * as measured on the provided {@link Scheduler}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/skipWithTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are skipped.
	 *
	 * @param timespan the initial time window during which to drop elements
	 * @param timer a time-capable {@link Scheduler} instance to measure the time window on
	 *
	 * @return a {@link Flux} dropping at the beginning for the given duration
	 */
	public final Flux<T> skip(Duration timespan, Scheduler timer) {
		if(!timespan.isZero()) {
			return skipUntilOther(Mono.delay(timespan, timer));
		}
		else{
			return this;
		}
	}

	/**
	 * Skip a specified number of elements at the end of this {@link Flux} sequence.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/skipLast.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are skipped.
	 *
	 * @param n the number of elements to drop before completion
	 *
	 * @return a {@link Flux} dropping the specified number of elements at the end of the
	 * sequence
	 *
	 */
	public final Flux<T> skipLast(int n) {
		if (n == 0) {
			return this;
		}
		return onAssembly(new FluxSkipLast<>(this, n));
	}

	/**
	 * Skips values from this {@link Flux} until a {@link Predicate} returns true for the
	 * value. The resulting {@link Flux} will include and emit the matching value.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/skipUntil.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are skipped.
	 *
	 * @param untilPredicate the {@link Predicate} evaluated to stop skipping.
	 *
	 * @return a {@link Flux} dropping until the {@link Predicate} matches
	 */
	public final Flux<T> skipUntil(Predicate<? super T> untilPredicate) {
		return onAssembly(new FluxSkipUntil<>(this, untilPredicate));
	}

	/**
	 * Skip values from this {@link Flux} until a specified {@link Publisher} signals
	 * an onNext or onComplete.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/skipUntilOther.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are skipped.
	 *
	 * @param other the companion {@link Publisher} to coordinate with to stop skipping
	 *
	 * @return a {@link Flux} dropping until the other {@link Publisher} emits
	 *
	 */
	public final Flux<T> skipUntilOther(Publisher<?> other) {
		return onAssembly(new FluxSkipUntilOther<>(this, other));
	}

	/**
	 * Skips values from this {@link Flux} while a {@link Predicate} returns true for the value.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/skipWhile.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements that are skipped.
	 *
	 * @param skipPredicate the {@link Predicate} that causes skipping while evaluating to true.
	 *
	 * @return a {@link Flux} dropping while the {@link Predicate} matches
	 */
	public final Flux<T> skipWhile(Predicate<? super T> skipPredicate) {
		return onAssembly(new FluxSkipWhile<>(this, skipPredicate));
	}

	/**
	 * Sort elements from this {@link Flux} by collecting and sorting them in the background
	 * then emitting the sorted sequence once this sequence completes.
	 * Each item emitted by the {@link Flux} must implement {@link Comparable} with
	 * respect to all other items in the sequence.
	 *
	 * <p>Note that calling {@code sort} with long, non-terminating or infinite sources
	 * might cause {@link OutOfMemoryError}. Use sequence splitting like {@link #window} to sort batches in that case.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sort.svg" alt="">
	 *
	 * @throws ClassCastException if any item emitted by the {@link Flux} does not implement
	 * {@link Comparable} with respect to all other items emitted by the {@link Flux}
	 * @return a sorted {@link Flux}
	 */
	public final Flux<T> sort(){
		return collectSortedList().flatMapIterable(identityFunction());
	}

	/**
	 * Sort elements from this {@link Flux} using a {@link Comparator} function, by
	 * collecting and sorting elements in the background then emitting the sorted sequence
	 * once this sequence completes.
	 *
	 * <p>Note that calling {@code sort} with long, non-terminating or infinite sources
	 * might cause {@link OutOfMemoryError}
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sort.svg" alt="">
	 *
	 * @param sortFunction a function that compares two items emitted by this {@link Flux}
	 * to indicate their sort order
	 * @return a sorted {@link Flux}
	 */
	public final Flux<T> sort(Comparator<? super T> sortFunction) {
		return collectSortedList(sortFunction).flatMapIterable(identityFunction());
	}

	/**
	 * Prepend the given {@link Iterable} before this {@link Flux} sequence.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/startWithIterable.svg" alt="">
	 *
	 * @param iterable the sequence of values to start the resulting {@link Flux} with
	 *
	 * @return a new {@link Flux} prefixed with elements from an {@link Iterable}
	 */
	public final Flux<T> startWith(Iterable<? extends T> iterable) {
		return startWith(fromIterable(iterable));
	}

	/**
	 * Prepend the given values before this {@link Flux} sequence.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/startWithValues.svg" alt="">
	 *
	 * @param values the array of values to start the resulting {@link Flux} with
	 *
	 * @return a new {@link Flux} prefixed with the given elements
	 */
	@SafeVarargs
	public final Flux<T> startWith(T... values) {
		return startWith(just(values));
	}

	/**
	 * Prepend the given {@link Publisher} sequence to this {@link Flux} sequence.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/startWithPublisher.svg" alt="">
	 *
	 * @param publisher the Publisher whose values to prepend
	 *
	 * @return a new {@link Flux} prefixed with the given {@link Publisher} sequence
	 */
	public final Flux<T> startWith(Publisher<? extends T> publisher) {
		if (this instanceof FluxConcatArray) {
			FluxConcatArray<T> fluxConcatArray = (FluxConcatArray<T>) this;
			return fluxConcatArray.concatAdditionalSourceFirst(publisher);
		}
		return concat(publisher, this);
	}

	/**
	 * Subscribe to this {@link Flux} and request unbounded demand.
	 * <p>
	 * This version doesn't specify any consumption behavior for the events from the
	 * chain, especially no error handling, so other variants should usually be preferred.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeIgoringAllSignalsForFlux.svg" alt="">
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 */
	public final Disposable subscribe() {
		return subscribe(null, null, null);
	}

	/**
	 * Subscribe a {@link Consumer} to this {@link Flux} that will consume all the
	 * elements in the  sequence. It will request an unbounded demand ({@code Long.MAX_VALUE}).
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnNext(java.util.function.Consumer)}.
	 * <p>
	 * For a version that gives you more control over backpressure and the request, see
	 * {@link #subscribe(Subscriber)} with a {@link BaseSubscriber}.
	 * <p>
	 * Keep in mind that since the sequence can be asynchronous, this will immediately
	 * return control to the calling thread. This can give the impression the consumer is
	 * not invoked when executing in a main thread or a unit test for instance.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeWithOnNextForFlux.svg" alt="">
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
	 * Subscribe to this {@link Flux} with a {@link Consumer} that will consume all the
	 * elements in the sequence, as well as a {@link Consumer} that will handle errors.
	 * The subscription will request an unbounded demand ({@code Long.MAX_VALUE}).
	 * <p>
	 * For a passive version that observe and forward incoming data see
	 * {@link #doOnNext(java.util.function.Consumer)} and {@link #doOnError(java.util.function.Consumer)}.
	 * <p>For a version that gives you more control over backpressure and the request, see
	 * {@link #subscribe(Subscriber)} with a {@link BaseSubscriber}.
	 * <p>
	 * Keep in mind that since the sequence can be asynchronous, this will immediately
	 * return control to the calling thread. This can give the impression the consumers are
	 * not invoked when executing in a main thread or a unit test for instance.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeWithOnNextAndOnErrorForFlux.svg" alt="">
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
	 * Subscribe {@link Consumer} to this {@link Flux} that will respectively consume all the
	 * elements in the sequence, handle errors and react to completion. The subscription
	 * will request unbounded demand ({@code Long.MAX_VALUE}).
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnNext(java.util.function.Consumer)},
	 * {@link #doOnError(java.util.function.Consumer)} and {@link #doOnComplete(Runnable)}.
	 * <p>For a version that gives you more control over backpressure and the request, see
	 * {@link #subscribe(Subscriber)} with a {@link BaseSubscriber}.
	 * <p>
	 * Keep in mind that since the sequence can be asynchronous, this will immediately
	 * return control to the calling thread. This can give the impression the consumer is
	 * not invoked when executing in a main thread or a unit test for instance.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeWithOnNextAndOnErrorAndOnCompleteForFlux.svg" alt="">
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
	 * Subscribe {@link Consumer} to this {@link Flux} that will respectively consume all the
	 * elements in the sequence, handle errors, react to completion, and request upon subscription.
	 * It will let the provided {@link Subscription subscriptionConsumer}
	 * request the adequate amount of data, or request unbounded demand
	 * {@code Long.MAX_VALUE} if no such consumer is provided.
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnNext(java.util.function.Consumer)},
	 * {@link #doOnError(java.util.function.Consumer)}, {@link #doOnComplete(Runnable)}
	 * and {@link #doOnSubscribe(Consumer)}.
	 * <p>For a version that gives you more control over backpressure and the request, see
	 * {@link #subscribe(Subscriber)} with a {@link BaseSubscriber}.
	 * <p>
	 * Keep in mind that since the sequence can be asynchronous, this will immediately
	 * return control to the calling thread. This can give the impression the consumer is
	 * not invoked when executing in a main thread or a unit test for instance.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeForFlux.svg" alt="">
	 *
	 * @param consumer the consumer to invoke on each value
	 * @param errorConsumer the consumer to invoke on error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 * @param subscriptionConsumer the consumer to invoke on subscribe signal, to be used
	 * for the initial {@link Subscription#request(long) request}, or null for max request
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 * @deprecated Because users tend to forget to {@link Subscription#request(long) request} the subsciption. If
	 * the behavior is really needed, consider using {@link #subscribeWith(Subscriber)}. To be removed in 3.5.
	 */
	@Deprecated
	public final Disposable subscribe(
			@Nullable Consumer<? super T> consumer,
			@Nullable Consumer<? super Throwable> errorConsumer,
			@Nullable Runnable completeConsumer,
			@Nullable Consumer<? super Subscription> subscriptionConsumer) {
		return subscribeWith(new LambdaSubscriber<>(consumer, errorConsumer,
				completeConsumer,
				subscriptionConsumer,
				null));
	}

	/**
	 * Subscribe {@link Consumer} to this {@link Flux} that will respectively consume all the
	 * elements in the sequence, handle errors and react to completion. Additionally, a {@link Context}
	 * is tied to the subscription. At subscription, an unbounded request is implicitly made.
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnNext(java.util.function.Consumer)},
	 * {@link #doOnError(java.util.function.Consumer)}, {@link #doOnComplete(Runnable)}
	 * and {@link #doOnSubscribe(Consumer)}.
	 * <p>For a version that gives you more control over backpressure and the request, see
	 * {@link #subscribe(Subscriber)} with a {@link BaseSubscriber}.
	 * <p>
	 * Keep in mind that since the sequence can be asynchronous, this will immediately
	 * return control to the calling thread. This can give the impression the consumer is
	 * not invoked when executing in a main thread or a unit test for instance.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeForFlux.svg" alt="">
	 *
	 * @param consumer the consumer to invoke on each value
	 * @param errorConsumer the consumer to invoke on error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 * @param initialContext the base {@link Context} tied to the subscription that will
	 * be visible to operators upstream
	 *
	 * @return a new {@link Disposable} that can be used to cancel the underlying {@link Subscription}
	 */
	public final Disposable subscribe(
			@Nullable Consumer<? super T> consumer,
			@Nullable Consumer<? super Throwable> errorConsumer,
			@Nullable Runnable completeConsumer,
			@Nullable Context initialContext) {
		return subscribeWith(new LambdaSubscriber<>(consumer, errorConsumer,
				completeConsumer,
				null,
				initialContext));
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void subscribe(Subscriber<? super T> actual) {
		CorePublisher publisher = Operators.onLastAssembly(this);
		CoreSubscriber subscriber = Operators.toCoreSubscriber(actual);

		if (subscriber instanceof Fuseable.QueueSubscription && this != publisher && this instanceof Fuseable && !(publisher instanceof Fuseable)) {
			subscriber = new FluxHide.SuppressFuseableSubscriber<>(subscriber);
		}

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

			subscriber = Operators.restoreContextOnSubscriberIfPublisherNonInternal(publisher, subscriber);
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
	 * @see Flux#subscribe(Subscriber)
	 */
	public abstract void subscribe(CoreSubscriber<? super T> actual);

	/**
	 * Run subscribe, onSubscribe and request on a specified {@link Scheduler}'s {@link Worker}.
	 * As such, placing this operator anywhere in the chain will also impact the execution
	 * context of onNext/onError/onComplete signals from the beginning of the chain up to
	 * the next occurrence of a {@link #publishOn(Scheduler) publishOn}.
	 * <p>
	 * Note that if you are using an eager or blocking
	 * {@link #create(Consumer, FluxSink.OverflowStrategy)}
	 * as the source, it can lead to deadlocks due to requests piling up behind the emitter.
	 * In such case, you should call {@link #subscribeOn(Scheduler, boolean) subscribeOn(scheduler, false)}
	 * instead.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeOnForFlux.svg" alt="">
	 * <p>
	 * Typically used for slow publisher e.g., blocking IO, fast consumer(s) scenarios.
	 *
	 * <blockquote><pre>
	 * {@code flux.subscribeOn(Schedulers.single()).subscribe() }
	 * </pre></blockquote>
	 *
	 * <p>
	 *     Note that {@link Worker#schedule(Runnable)} raising
	 *     {@link java.util.concurrent.RejectedExecutionException} on late
	 *     {@link Subscription#request(long)} will be propagated to the request caller.
	 *
	 * @param scheduler a {@link Scheduler} providing the {@link Worker} where to subscribe
	 *
	 * @return a {@link Flux} requesting asynchronously
	 * @see #publishOn(Scheduler)
	 * @see #subscribeOn(Scheduler, boolean)
	 */
	public final Flux<T> subscribeOn(Scheduler scheduler) {
		return subscribeOn(scheduler, true);
	}

	/**
	 * Run subscribe and onSubscribe on a specified {@link Scheduler}'s {@link Worker}.
	 * Request will be run on that worker too depending on the {@code requestOnSeparateThread}
	 * parameter (which defaults to true in the {@link #subscribeOn(Scheduler)} version).
	 * As such, placing this operator anywhere in the chain will also impact the execution
	 * context of onNext/onError/onComplete signals from the beginning of the chain up to
	 * the next occurrence of a {@link #publishOn(Scheduler) publishOn}.
	 * <p>
	 * Note that if you are using an eager or blocking
	 * {@link Flux#create(Consumer, FluxSink.OverflowStrategy)}
	 * as the source, it can lead to deadlocks due to requests piling up behind the emitter.
	 * Thus this operator has a {@code requestOnSeparateThread} parameter, which should be
	 * set to {@code false} in this case.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/subscribeOnForFlux.svg" alt="">
	 * <p>
	 * Typically used for slow publisher e.g., blocking IO, fast consumer(s) scenarios.
	 *
	 * <blockquote><pre>
	 * {@code flux.subscribeOn(Schedulers.single()).subscribe() }
	 * </pre></blockquote>
	 *
	 * <p>
	 *     Note that {@link Worker#schedule(Runnable)} raising
	 *     {@link java.util.concurrent.RejectedExecutionException} on late
	 *     {@link Subscription#request(long)} will be propagated to the request caller.
	 *
	 * @param scheduler a {@link Scheduler} providing the {@link Worker} where to subscribe
	 * @param requestOnSeparateThread whether or not to also perform requests on the worker.
	 * {@code true} to behave like {@link #subscribeOn(Scheduler)}
	 *
	 * @return a {@link Flux} requesting asynchronously
	 * @see #publishOn(Scheduler)
	 * @see #subscribeOn(Scheduler)
	 */
	public final Flux<T> subscribeOn(Scheduler scheduler, boolean requestOnSeparateThread) {
		if (this instanceof Callable) {
			if (this instanceof Fuseable.ScalarCallable) {
				try {
					@SuppressWarnings("unchecked") T value = ((Fuseable.ScalarCallable<T>) this).call();
					return onAssembly(new FluxSubscribeOnValue<>(value, scheduler));
				}
				catch (Exception e) {
					//leave FluxSubscribeOnCallable defer error
				}
			}
			@SuppressWarnings("unchecked")
			Callable<T> c = (Callable<T>)this;
			return onAssembly(new FluxSubscribeOnCallable<>(c, scheduler));
		}
		return onAssembly(new FluxSubscribeOn<>(this, scheduler, requestOnSeparateThread));
	}

	/**
	 * Subscribe a provided instance of a subclass of {@link Subscriber} to this {@link Flux}
	 * and return said instance for further chaining calls. This is similar to {@link #as(Function)},
	 * except a subscription is explicitly performed by this method.
	 * <p>
	 * If you need more control over backpressure and the request, use a {@link BaseSubscriber}.
	 *
	 * @param subscriber the {@link Subscriber} to subscribe with and return
	 * @param <E> the reified type from the input/output subscriber
	 *
	 * @return the passed {@link Subscriber}
	 */
	public final <E extends Subscriber<? super T>> E subscribeWith(E subscriber) {
		subscribe(subscriber);
		return subscriber;
	}

	/**
	 * Transform the current {@link Flux} once it emits its first element, making a
	 * conditional transformation possible. This operator first requests one element
	 * from the source then applies a transformation derived from the first {@link Signal}
	 * and the source. The whole source (including the first signal) is passed as second
	 * argument to the {@link BiFunction} and it is very strongly advised to always build
	 * upon with operators (see below).
	 * <p>
	 * Note that the source might complete or error immediately instead of emitting,
	 * in which case the {@link Signal} would be onComplete or onError. It is NOT
	 * necessarily an onNext Signal, and must be checked accordingly.
     * <p>
	 * For example, this operator could be used to define a dynamic transformation that depends
	 * on the first element (which could contain routing metadata for instance):
	 *
	 * <blockquote><pre>
	 * {@code
	 *  fluxOfIntegers.switchOnFirst((signal, flux) -> {
	 *      if (signal.hasValue()) {
	 *          ColoredShape firstColor = signal.get();
	 *          return flux.filter(v -> !v.hasSameColorAs(firstColor))
	 *      }
	 *      return flux; //either early complete or error, this forwards the termination in any case
	 *      //`return flux.onErrorResume(t -> Mono.empty());` instead would suppress an early error
	 *      //`return Flux.just(1,2,3);` instead would suppress an early error and return 1, 2, 3.
	 *      //It would also only cancel the original `flux` at the completion of `just`.
	 *  })
	 * }
	 * </pre></blockquote>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/switchOnFirst.svg" alt="">
	 * <p>
	 * It is advised to return a {@link Publisher} derived from the original {@link Flux}
	 * in all cases, as not doing so would keep the original {@link Publisher} open and
	 * hanging with a single request until the inner {@link Publisher} terminates or
	 * the whole {@link Flux} is cancelled.
	 *
	 * @param transformer A {@link BiFunction} executed once the first signal is
	 * available and used to transform the source conditionally. The whole source (including
	 * first signal) is passed as second argument to the BiFunction.
	 * @param <V> the item type in the returned {@link Flux}
     *
	 * @return a new {@link Flux} that transform the upstream once a signal is available
	 */
	public final <V> Flux<V> switchOnFirst(BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends V>> transformer) {
		return switchOnFirst(transformer, true);
	}

	/**
	 * Transform the current {@link Flux} once it emits its first element, making a
	 * conditional transformation possible. This operator first requests one element
	 * from the source then applies a transformation derived from the first {@link Signal}
	 * and the source. The whole source (including the first signal) is passed as second
	 * argument to the {@link BiFunction} and it is very strongly advised to always build
	 * upon with operators (see below).
	 * <p>
	 * Note that the source might complete or error immediately instead of emitting,
	 * in which case the {@link Signal} would be onComplete or onError. It is NOT
	 * necessarily an onNext Signal, and must be checked accordingly.
	 * <p>
	 * For example, this operator could be used to define a dynamic transformation that depends
	 * on the first element (which could contain routing metadata for instance):
	 *
	 * <blockquote><pre>
	 * {@code
	 *  fluxOfIntegers.switchOnFirst((signal, flux) -> {
	 *      if (signal.hasValue()) {
	 *          ColoredShape firstColor = signal.get();
	 *          return flux.filter(v -> !v.hasSameColorAs(firstColor))
	 *      }
	 *      return flux; //either early complete or error, this forwards the termination in any case
	 *      //`return flux.onErrorResume(t -> Mono.empty());` instead would suppress an early error
	 *      //`return Flux.just(1,2,3);` instead would suppress an early error and return 1, 2, 3.
	 *      //It would also only cancel the original `flux` at the completion of `just`.
	 *  })
	 * }
	 * </pre></blockquote>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/switchOnFirst.svg" alt="">
	 * <p>
	 * It is advised to return a {@link Publisher} derived from the original {@link Flux}
	 * in all cases, as not doing so would keep the original {@link Publisher} open and
	 * hanging with a single request. In case the value of the {@code cancelSourceOnComplete} parameter is {@code true} the original publisher until the inner {@link Publisher} terminates or
	 * the whole {@link Flux} is cancelled. Otherwise the original publisher will hang forever.
	 *
	 * @param transformer A {@link BiFunction} executed once the first signal is
	 * available and used to transform the source conditionally. The whole source (including
	 * first signal) is passed as second argument to the BiFunction.
	 * @param <V> the item type in the returned {@link Flux}
	 * @param cancelSourceOnComplete specify whether original publisher should be cancelled on {@code onComplete} from the derived one
	 *
	 * @return a new {@link Flux} that transform the upstream once a signal is available
	 */
	public final <V> Flux<V> switchOnFirst(BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends V>> transformer, boolean cancelSourceOnComplete) {
		return onAssembly(new FluxSwitchOnFirst<>(this, transformer, cancelSourceOnComplete));
	}

	/**
	 * Switch to an alternative {@link Publisher} if this sequence is completed without any data.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/switchIfEmptyForFlux.svg" alt="">
	 *
	 * @param alternate the alternative {@link Publisher} if this sequence is empty
	 *
	 * @return a new {@link Flux} that falls back on a {@link Publisher} if source is empty
	 */
	public final Flux<T> switchIfEmpty(Publisher<? extends T> alternate) {
		return onAssembly(new FluxSwitchIfEmpty<>(this, alternate));
	}

	/**
	 * Switch to a new {@link Publisher} generated via a {@link Function} whenever
	 * this {@link Flux} produces an item. As such, the elements from each generated
	 * Publisher are emitted in the resulting {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/switchMap.svg" alt="">
	 * <p>
	 * This operator requests the source for an unbounded amount, but doesn't
	 * request each generated {@link Publisher} unless the downstream has made
	 * a corresponding request (no prefetch of inner publishers).
	 *
	 * @param fn the {@link Function} to generate a {@link Publisher} for each source value
	 * @param <V> the type of the return value of the transformation function
	 *
	 * @return a new {@link Flux} that emits values from an alternative {@link Publisher}
	 * for each source onNext
	 */
	public final <V> Flux<V> switchMap(Function<? super T, Publisher<? extends V>> fn) {
		return onAssembly(new FluxSwitchMapNoPrefetch<>(this, fn));
	}

	/**
	 * Switch to a new {@link Publisher} generated via a {@link Function} whenever
	 * this {@link Flux} produces an item. As such, the elements from each generated
	 * Publisher are emitted in the resulting {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/switchMap.svg" alt="">
	 *
	 * @param fn the {@link Function} to generate a {@link Publisher} for each source value
	 * @param prefetch the produced demand for inner sources
	 *
	 * @param <V> the type of the return value of the transformation function
	 *
	 * @return a new {@link Flux} that emits values from an alternative {@link Publisher}
	 * for each source onNext
	 *
	 * @deprecated to be removed in 3.6.0 at the earliest. In 3.5.0, you should replace
	 * calls with prefetch=0 with calls to switchMap(fn), as the default behavior of the
	 * single-parameter variant will then change to prefetch=0.
	 */
	@Deprecated
	public final <V> Flux<V> switchMap(Function<? super T, Publisher<? extends V>> fn, int prefetch) {
		if (prefetch == 0) {
			return onAssembly(new FluxSwitchMapNoPrefetch<>(this, fn));
		}
		return onAssembly(new FluxSwitchMap<>(this, fn, Queues.unbounded(prefetch), prefetch));
	}

	/**
	 * Tag this flux with a key/value pair. These can be retrieved as a {@link Set} of
	 * all tags throughout the publisher chain by using {@link Scannable#tags()} (as
	 * traversed by {@link Scannable#parents()}).
	 * <p>
	 * The name is typically visible at assembly time by the {@link #tap(SignalListenerFactory)} operator,
	 * which could for example be configured with a metrics listener applying the tag(s) to its meters.
	 *
	 * @param key a tag key
	 * @param value a tag value
	 *
	 * @return the same sequence, but bearing tags
	 *
	 *@see #name(String)
	 *@see #metrics()
	 */
	public final Flux<T> tag(String key, String value) {
		return FluxName.createOrAppend(this, key, value);
	}

	/**
	 * Take only the first N values from this {@link Flux}, if available.
	 * If n is zero, the source isn't even subscribed to and the operator completes immediately upon subscription.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeLimitRequestTrue.svg" alt="">
	 * <p>
	 * This ensures that the total amount requested upstream is capped at {@code n}, although smaller
	 * requests can be made if the downstream makes requests &lt; n. In any case, this operator never lets
	 * the upstream produce more elements than the cap, and it can be used to more strictly adhere to backpressure.
	 * <p>
	 * This mode is typically useful for cases where a race between request and cancellation can lead
	 * the upstream to producing a lot of extraneous data, and such a production is undesirable (e.g.
	 * a source that would send the extraneous data over the network).
	 * It is equivalent to {@link #take(long, boolean)} with {@code limitRequest == true},
	 * If there is a requirement for unbounded upstream request (eg. for performance reasons),
	 * use {@link #take(long, boolean)} with {@code limitRequest=false} instead.
	 *
	 * @param n the maximum number of items to request from upstream and emit from this {@link Flux}
	 *
	 * @return a {@link Flux} limited to size N
	 * @see #take(long, boolean)
	 */
	public final Flux<T> take(long n) {
		return take(n, true);
	}

	/**
	 * Take only the first N values from this {@link Flux}, if available.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeLimitRequestTrue.svg" alt="">
	 * <p>
	 * If {@code limitRequest == true}, ensure that the total amount requested upstream is capped
	 * at {@code n}. In that configuration, this operator never let the upstream produce more elements
	 * than the cap, and it can be used to more strictly adhere to backpressure.
	 * If n is zero, the source isn't even subscribed to and the operator completes immediately
	 * upon subscription (the behavior inherited from {@link #take(long)}).
	 * <p>
	 * This mode is typically useful for cases where a race between request and cancellation can lead
	 * the upstream to producing a lot of extraneous data, and such a production is undesirable (e.g.
	 * a source that would send the extraneous data over the network).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeLimitRequestFalse.svg" alt="takeLimitRequestFalse">
	 * <p>
	 * If {@code limitRequest == false} this operator doesn't propagate the backpressure requested amount.
	 * Rather, it makes an unbounded request and cancels once N elements have been emitted.
	 * If n is zero, the source is subscribed to but immediately cancelled, then the operator completes.
	 * <p>
	 * In this mode, the source could produce a lot of extraneous elements despite cancellation.
	 * If that behavior is undesirable and you do not own the request from downstream
	 * (e.g. prefetching operators), consider using {@code limitRequest = true} instead.
	 *
	 * @param n the number of items to emit from this {@link Flux}
	 * @param limitRequest {@code true} to follow the downstream request more closely and limit the upstream request
	 * to {@code n}. {@code false} to request an unbounded amount from upstream.
	 *
	 * @return a {@link Flux} limited to size N
	 */
	public final Flux<T> take(long n, boolean limitRequest) {
		if (limitRequest) {
			return onAssembly(new FluxLimitRequest<>(this, n));
		}
		if (this instanceof Fuseable) {
			return onAssembly(new FluxTakeFuseable<>(this, n));
		}
		return onAssembly(new FluxTake<>(this, n));
	}

	/**
	 * Relay values from this {@link Flux} until the specified {@link Duration} elapses.
	 * <p>
	 * If the duration is zero, the resulting {@link Flux} completes as soon as this {@link Flux}
	 * signals its first value (which is not relayed, though).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeWithTimespanForFlux.svg" alt="">
	 *
	 * @param timespan the {@link Duration} of the time window during which to emit elements
	 * from this {@link Flux}
	 *
	 * @return a {@link Flux} limited to elements emitted within a specific duration
	 */
	public final Flux<T> take(Duration timespan) {
		return take(timespan, Schedulers.parallel());
	}

	/**
	 * Relay values from this {@link Flux} until the specified {@link Duration} elapses,
	 * as measured on the specified {@link Scheduler}.
	 * <p>
	 * If the duration is zero, the resulting {@link Flux} completes as soon as this {@link Flux}
	 * signals its first value (which is not relayed, though).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeWithTimespanForFlux.svg" alt="">
	 *
	 * @param timespan the {@link Duration} of the time window during which to emit elements
	 * from this {@link Flux}
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a {@link Flux} limited to elements emitted within a specific duration
	 */
	public final Flux<T> take(Duration timespan, Scheduler timer) {
		if (!timespan.isZero()) {
			return takeUntilOther(Mono.delay(timespan, timer));
		}
		else {
			return take(0, false);
		}
	}

	/**
	 * Emit the last N values this {@link Flux} emitted before its completion.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeLast.svg" alt="">
	 *
	 * @param n the number of items from this {@link Flux} to retain and emit on onComplete
	 *
	 * @return a terminating {@link Flux} sub-sequence
	 *
	 */
	public final Flux<T> takeLast(int n) {
		if(n == 1){
			return onAssembly(new FluxTakeLastOne<>(this));
		}
		return onAssembly(new FluxTakeLast<>(this, n));
	}

	/**
	 * Relay values from this {@link Flux} until the given {@link Predicate} matches.
	 * This includes the matching data (unlike {@link #takeWhile}).
	 * The predicate is tested before the element is emitted,
	 * so if the element is modified by the consumer, this won't affect the predicate.
	 * In case of an error during the predicate test, the current element is emitted before the error.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeUntil.svg" alt="">
	 *
	 * @param predicate the {@link Predicate} that stops the taking of values from this {@link Flux}
	 * when returning {@literal true}.
	 *
	 * @return a new {@link Flux} limited by the predicate
	 *
	 */
	public final Flux<T> takeUntil(Predicate<? super T> predicate) {
		return onAssembly(new FluxTakeUntil<>(this, predicate));
	}

	/**
	 * Relay values from this {@link Flux} until the given {@link Publisher} emits.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeUntilOtherForFlux.svg" alt="">
	 *
	 * @param other the companion {@link Publisher} that signals when to stop taking values from this {@link Flux}
	 *
	 * @return a new {@link Flux} limited by a companion {@link Publisher}
	 *
	 */
	public final Flux<T> takeUntilOther(Publisher<?> other) {
		return onAssembly(new FluxTakeUntilOther<>(this, other));
	}

	/**
	 * Relay values from this {@link Flux} while a predicate returns {@literal TRUE}
	 * for the values (checked before each value is delivered).
	 * This only includes the matching data (unlike {@link #takeUntil}).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/takeWhile.svg" alt="">
	 *
	 * @param continuePredicate the {@link Predicate} invoked each onNext returning {@literal TRUE}
	 * to relay a value or {@literal FALSE} to terminate
	 *
	 * @return a new {@link Flux} taking values from the source while the predicate matches
	 */
	public final Flux<T> takeWhile(Predicate<? super T> continuePredicate) {
		return onAssembly(new FluxTakeWhile<>(this, continuePredicate));
	}

	/**
	 * Tap into Reactive Streams signals emitted or received by this {@link Flux} and notify a stateful per-{@link Subscriber}
	 * {@link SignalListener}.
	 * <p>
	 * Any exception thrown by the {@link SignalListener} methods causes the subscription to be cancelled
	 * and the subscriber to be terminated with an {@link Subscriber#onError(Throwable) onError signal} of that
	 * exception. Note that {@link SignalListener#doFinally(SignalType)}, {@link SignalListener#doAfterComplete()} and
	 * {@link SignalListener#doAfterError(Throwable)} instead just {@link Operators#onErrorDropped(Throwable, Context) drop}
	 * the exception.
	 * <p>
	 * This simplified variant assumes the state is purely initialized within the {@link Supplier},
	 * as it is called for each incoming {@link Subscriber} without additional context.
	 * <p>
	 * When the <a href="https://github.com/micrometer-metrics/context-propagation">context-propagation library</a>
	 * is available at runtime and the downstream {@link ContextView} is not empty, this operator implicitly uses the library
	 * to restore thread locals around all invocations of {@link SignalListener} methods. Typically, this would be done
	 * in conjunction with the use of {@link #contextCapture()} operator down the chain.
	 *
	 * @param simpleListenerGenerator the {@link Supplier} to create a new {@link SignalListener} on each subscription
	 * @return a new {@link Flux} with side effects defined by generated {@link SignalListener}
	 * @see #tap(Function)
	 * @see #tap(SignalListenerFactory)
	 */
	public final Flux<T> tap(Supplier<SignalListener<T>> simpleListenerGenerator) {
		return tap(new SignalListenerFactory<T, Void>() {
			@Override
			public Void initializePublisherState(Publisher<? extends T> ignored) {
				return null;
			}

			@Override
			public SignalListener<T> createListener(Publisher<? extends T> ignored1, ContextView ignored2, Void ignored3) {
				return simpleListenerGenerator.get();
			}
		});
	}

	/**
	 * Tap into Reactive Streams signals emitted or received by this {@link Flux} and notify a stateful per-{@link Subscriber}
	 * {@link SignalListener}.
	 * <p>
	 * Any exception thrown by the {@link SignalListener} methods causes the subscription to be cancelled
	 * and the subscriber to be terminated with an {@link Subscriber#onError(Throwable) onError signal} of that
	 * exception. Note that {@link SignalListener#doFinally(SignalType)}, {@link SignalListener#doAfterComplete()} and
	 * {@link SignalListener#doAfterError(Throwable)} instead just {@link Operators#onErrorDropped(Throwable, Context) drop}
	 * the exception.
	 * <p>
	 * This simplified variant allows the {@link SignalListener} to be constructed for each subscription
	 * with access to the incoming {@link Subscriber}'s {@link ContextView}.
	 * <p>
	 * When the <a href="https://github.com/micrometer-metrics/context-propagation">context-propagation library</a>
	 * is available at runtime and the {@link ContextView} is not empty, this operator implicitly uses the library
	 * to restore thread locals around all invocations of {@link SignalListener} methods. Typically, this would be done
	 * in conjunction with the use of {@link #contextCapture()} operator down the chain.
	 *
	 * @param listenerGenerator the {@link Function} to create a new {@link SignalListener} on each subscription
	 * @return a new {@link Flux} with side effects defined by generated {@link SignalListener}
	 * @see #tap(Supplier)
	 * @see #tap(SignalListenerFactory)
	 */
	public final Flux<T> tap(Function<ContextView, SignalListener<T>> listenerGenerator) {
		return tap(new SignalListenerFactory<T, Void>() {
			@Override
			public Void initializePublisherState(Publisher<? extends T> ignored) {
				return null;
			}

			@Override
			public SignalListener<T> createListener(Publisher<? extends T> ignored1, ContextView listenerContext, Void ignored2) {
				return listenerGenerator.apply(listenerContext);
			}
		});
	}

	/**
	 * Tap into Reactive Streams signals emitted or received by this {@link Flux} and notify a stateful per-{@link Subscriber}
	 * {@link SignalListener} created by the provided {@link SignalListenerFactory}.
	 * <p>
	 * The factory will initialize a {@link SignalListenerFactory#initializePublisherState(Publisher) state object} for
	 * each {@link Flux} or {@link Mono} instance it is used with, and that state will be cached and exposed for each
	 * incoming {@link Subscriber} in order to generate the associated {@link SignalListenerFactory#createListener(Publisher, ContextView, Object) listener}.
	 * <p>
	 * Any exception thrown by the {@link SignalListener} methods causes the subscription to be cancelled
	 * and the subscriber to be terminated with an {@link Subscriber#onError(Throwable) onError signal} of that
	 * exception. Note that {@link SignalListener#doFinally(SignalType)}, {@link SignalListener#doAfterComplete()} and
	 * {@link SignalListener#doAfterError(Throwable)} instead just {@link Operators#onErrorDropped(Throwable, Context) drop}
	 * the exception.
	 * <p>
	 * When the <a href="https://github.com/micrometer-metrics/context-propagation">context-propagation library</a>
	 * is available at runtime and the downstream {@link ContextView} is not empty, this operator implicitly uses the library
	 * to restore thread locals around all invocations of {@link SignalListener} methods. Typically, this would be done
	 * in conjunction with the use of {@link #contextCapture()} operator down the chain.
	 *
	 * @param listenerFactory the {@link SignalListenerFactory} to create a new {@link SignalListener} on each subscription
	 * @return a new {@link Flux} with side effects defined by generated {@link SignalListener}
	 * @see #tap(Supplier)
	 * @see #tap(Function)
	 */
	public final Flux<T> tap(SignalListenerFactory<T, ?> listenerFactory) {
		if (ContextPropagationSupport.shouldPropagateContextToThreadLocals()) {
			return onAssembly(new FluxTapRestoringThreadLocals<>(this, listenerFactory));
		}
		if (this instanceof Fuseable) {
			return onAssembly(new FluxTapFuseable<>(this, listenerFactory));
		}
		return onAssembly(new FluxTap<>(this, listenerFactory));
	}

	/**
	 * Return a {@code Mono<Void>} that completes when this {@link Flux} completes.
	 * This will actively ignore the sequence and only replay completion or error signals.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/thenForFlux.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements from the source.
	 *
	 * @return a new {@link Mono} representing the termination of this {@link Flux}
	 */
	public final Mono<Void> then() {
		@SuppressWarnings("unchecked")
		Mono<Void> then = (Mono<Void>) new MonoIgnoreElements<>(this);
		return Mono.onAssembly(then);
	}

	/**
	 * Let this {@link Flux} complete then play signals from a provided {@link Mono}.
	 * <p>
	 * In other words ignore element from this {@link Flux} and transform its completion signal into the
	 * emission and completion signal of a provided {@code Mono<V>}. Error signal is
	 * replayed in the resulting {@code Mono<V>}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/thenWithMonoForFlux.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements from the source.
	 *
	 * @param other a {@link Mono} to emit from after termination
	 * @param <V> the element type of the supplied Mono
	 *
	 * @return a new {@link Flux} that wait for source completion then emits from the supplied {@link Mono}
	 */
	public final <V> Mono<V> then(Mono<V> other) {
		return Mono.onAssembly(new MonoIgnoreThen<>(new Publisher[] { this }, other));
	}

	/**
	 * Return a {@code Mono<Void>} that waits for this {@link Flux} to complete then
	 * for a supplied {@link Publisher Publisher&lt;Void&gt;} to also complete. The
	 * second completion signal is replayed, or any error signal that occurs instead.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/thenEmptyForFlux.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements from the source.
	 *
	 * @param other a {@link Publisher} to wait for after this Flux's termination
	 * @return a new {@link Mono} completing when both publishers have completed in
	 * sequence
	 */
	public final Mono<Void> thenEmpty(Publisher<Void> other) {
		return then(Mono.fromDirect(other));
	}

	/**
	 * Let this {@link Flux} complete then play another {@link Publisher}.
	 * <p>
	 * In other words ignore element from this flux and transform the completion signal into a
	 * {@code Publisher<V>} that will emit elements from the provided {@link Publisher}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/thenManyForFlux.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements from the source.
	 *
	 * @param other a {@link Publisher} to emit from after termination
	 * @param <V> the element type of the supplied Publisher
	 *
	 * @return a new {@link Flux} that emits from the supplied {@link Publisher} after
	 * this Flux completes.
	 */
	public final <V> Flux<V> thenMany(Publisher<V> other) {
		@SuppressWarnings("unchecked")
		Flux<V> concat = (Flux<V>)concat(ignoreElements(), other);
		return concat;
	}

	/**
	 * Times {@link Subscriber#onNext(Object)} events, encapsulated into a {@link Timed} object
	 * that lets downstream consumer look at various time information gathered with nanosecond
	 * resolution using the default clock ({@link Schedulers#parallel()}):
	 * <ul>
	 *     <li>{@link Timed#elapsed()}: the time in nanoseconds since last event, as a {@link Duration}.
	 *     For the first onNext, "last event" is the subscription. Otherwise it is the previous onNext.
	 *     This is functionally equivalent to {@link #elapsed()}, with a more expressive and precise
	 *     representation than a {@link Tuple2} with a long.</li>
	 *     <li>{@link Timed#timestamp()}: the timestamp of this onNext, as an {@link java.time.Instant}
	 *     (with nanoseconds part). This is functionally equivalent to {@link #timestamp()}, with a more
	 *     expressive and precise representation than a {@link Tuple2} with a long.</li>
	 *     <li>{@link Timed#elapsedSinceSubscription()}: the time in nanoseconds since subscription,
	 *     as a {@link Duration}.</li>
	 * </ul>
	 * <p>
	 * The {@link Timed} object instances are safe to store and use later, as they are created as an
	 * immutable wrapper around the {@code <T>} value and immediately passed downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timedForFlux.svg" alt="">
	 *
	 * @return a timed {@link Flux}
	 * @see #elapsed()
	 * @see #timestamp()
	 */
	public final Flux<Timed<T>> timed() {
		return this.timed(Schedulers.parallel());
	}

	/**
	 * Times {@link Subscriber#onNext(Object)} events, encapsulated into a {@link Timed} object
	 * that lets downstream consumer look at various time information gathered with nanosecond
	 * resolution using the provided {@link Scheduler} as a clock:
	 * <ul>
	 *     <li>{@link Timed#elapsed()}: the time in nanoseconds since last event, as a {@link Duration}.
	 *     For the first onNext, "last event" is the subscription. Otherwise it is the previous onNext.
	 *     This is functionally equivalent to {@link #elapsed()}, with a more expressive and precise
	 *     representation than a {@link Tuple2} with a long.</li>
	 *     <li>{@link Timed#timestamp()}: the timestamp of this onNext, as an {@link java.time.Instant}
	 *     (with nanoseconds part). This is functionally equivalent to {@link #timestamp()}, with a more
	 *     expressive and precise representation than a {@link Tuple2} with a long.</li>
	 *     <li>{@link Timed#elapsedSinceSubscription()}: the time in nanoseconds since subscription,
	 *     as a {@link Duration}.</li>
	 * </ul>
	 * <p>
	 * The {@link Timed} object instances are safe to store and use later, as they are created as an
	 * immutable wrapper around the {@code <T>} value and immediately passed downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timedForFlux.svg" alt="">
	 *
	 * @return a timed {@link Flux}
	 * @see #elapsed(Scheduler)
	 * @see #timestamp(Scheduler)
	 */
	public final Flux<Timed<T>> timed(Scheduler clock) {
		return onAssembly(new FluxTimed<>(this, clock));
	}

	/**
	 * Propagate a {@link TimeoutException} as soon as no item is emitted within the
	 * given {@link Duration} from the previous emission (or the subscription for the first item).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutForFlux.svg" alt="">
	 *
	 * @param timeout the timeout between two signals from this {@link Flux}
	 *
	 * @return a {@link Flux} that can time out on a per-item basis
	 */
	public final Flux<T> timeout(Duration timeout) {
		return timeout(timeout, null, Schedulers.parallel());
	}

	/**
	 * Switch to a fallback {@link Flux} as soon as no item is emitted within the
	 * given {@link Duration} from the previous emission (or the subscription for the first item).
	 * <p>
	 * If the given {@link Publisher} is null, signal a {@link TimeoutException} instead.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutFallbackForFlux.svg" alt="">
	 *
	 * @param timeout the timeout between two signals from this {@link Flux}
	 * @param fallback the fallback {@link Publisher} to subscribe when a timeout occurs
	 *
	 * @return a {@link Flux} that will fallback to a different {@link Publisher} in case of a per-item timeout
	 */
	public final Flux<T> timeout(Duration timeout, @Nullable Publisher<? extends T> fallback) {
		return timeout(timeout, fallback, Schedulers.parallel());
	}

	/**
	 * Propagate a {@link TimeoutException} as soon as no item is emitted within the
	 * given {@link Duration} from the previous emission (or the subscription for the first
	 * item), as measured by the specified {@link Scheduler}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutForFlux.svg" alt="">
	 *
	 * @param timeout the timeout {@link Duration} between two signals from this {@link Flux}
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a {@link Flux} that can time out on a per-item basis
	 */
	public final Flux<T> timeout(Duration timeout, Scheduler timer) {
		return timeout(timeout, null, timer);
	}

	/**
	 * Switch to a fallback {@link Flux} as soon as no item is emitted within the
	 * given {@link Duration} from the previous emission (or the subscription for the
	 * first item), as measured on the specified {@link Scheduler}.
	 * <p>
	 * If the given {@link Publisher} is null, signal a {@link TimeoutException} instead.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutFallbackForFlux.svg" alt="">
	 *
	 * @param timeout the timeout {@link Duration} between two signals from this {@link Flux}
	 * @param fallback the fallback {@link Publisher} to subscribe when a timeout occurs
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a {@link Flux} that will fallback to a different {@link Publisher} in case of a per-item timeout
	 */
	public final Flux<T> timeout(Duration timeout,
			@Nullable Publisher<? extends T> fallback,
			Scheduler timer) {
		final Mono<Long> _timer = Mono.delay(timeout, timer).onErrorReturn(0L);
		final Function<T, Publisher<Long>> rest = o -> _timer;

		if(fallback == null) {
			return timeout(_timer, rest, timeout.toMillis() + "ms");
		}
		return timeout(_timer, rest, fallback);
	}

	/**
	 * Signal a {@link TimeoutException} in case the first item from this {@link Flux} has
	 * not been emitted before the given {@link Publisher} emits.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutPublisher.svg" alt="">
	 *
	 * @param firstTimeout the companion {@link Publisher} that will trigger a timeout if
	 * emitting before the first signal from this {@link Flux}
	 *
	 * @param <U> the type of the timeout Publisher
	 *
	 * @return a {@link Flux} that can time out if the first item does not come before
	 * a signal from a companion {@link Publisher}
	 *
	 */
	public final <U> Flux<T> timeout(Publisher<U> firstTimeout) {
		return timeout(firstTimeout, t -> never());
	}

	/**
	 * Signal a {@link TimeoutException} in case the first item from this {@link Flux} has
	 * not been emitted before the {@code firstTimeout} {@link Publisher} emits, and whenever
	 * each subsequent elements is not emitted before a {@link Publisher} generated from
	 * the latest element signals.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutPublisherFunctionForFlux.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards an element if it comes right after the timeout.
	 *
	 * @param firstTimeout the timeout {@link Publisher} that must not emit before the first signal from this {@link Flux}
	 * @param nextTimeoutFactory the timeout {@link Publisher} factory for each next item
	 * @param <U> the type of the elements of the first timeout Publisher
	 * @param <V> the type of the elements of the subsequent timeout Publishers
	 *
	 * @return a {@link Flux} that can time out if each element does not come before
	 * a signal from a per-item companion {@link Publisher}
	 */
	public final <U, V> Flux<T> timeout(Publisher<U> firstTimeout,
			Function<? super T, ? extends Publisher<V>> nextTimeoutFactory) {
		return timeout(firstTimeout, nextTimeoutFactory, "first signal from a Publisher");
	}

	private <U, V> Flux<T> timeout(Publisher<U> firstTimeout,
			Function<? super T, ? extends Publisher<V>> nextTimeoutFactory,
			String timeoutDescription) {
			return onAssembly(new FluxTimeout<>(this, firstTimeout, nextTimeoutFactory, timeoutDescription));
	}

	/**
	 * Switch to a fallback {@link Publisher} in case the first item from this {@link Flux} has
	 * not been emitted before the {@code firstTimeout} {@link Publisher} emits, and whenever
	 * each subsequent elements is not emitted before a {@link Publisher} generated from
	 * the latest element signals.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timeoutPublisherFunctionAndFallbackForFlux.svg" alt="">
	 *
	 * @param firstTimeout the timeout {@link Publisher} that must not emit before the first signal from this {@link Flux}
	 * @param nextTimeoutFactory the timeout {@link Publisher} factory for each next item
	 * @param fallback the fallback {@link Publisher} to subscribe when a timeout occurs
	 *
	 * @param <U> the type of the elements of the first timeout Publisher
	 * @param <V> the type of the elements of the subsequent timeout Publishers
	 *
	 * @return a {@link Flux} that can time out if each element does not come before
	 * a signal from a per-item companion {@link Publisher}
	 */
	public final <U, V> Flux<T> timeout(Publisher<U> firstTimeout,
			Function<? super T, ? extends Publisher<V>> nextTimeoutFactory, Publisher<? extends T>
			fallback) {
		return onAssembly(new FluxTimeout<>(this, firstTimeout, nextTimeoutFactory,
				fallback));
	}

	/**
	 * Emit a {@link reactor.util.function.Tuple2} pair of T1 the current clock time in
	 * millis (as a {@link Long} measured by the {@link Schedulers#parallel() parallel}
	 * Scheduler) and T2 the emitted data (as a {@code T}), for each item from this {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/timestampForFlux.svg" alt="">
	 *
	 * @return a timestamped {@link Flux}
	 * @see #timed()
	 * @see Timed#timestamp()
	 */
	public final Flux<Tuple2<Long, T>> timestamp() {
		return timestamp(Schedulers.parallel());
	}

	/**
	 * Emit a {@link reactor.util.function.Tuple2} pair of T1 the current clock time in
	 * millis (as a {@link Long} measured by the provided {@link Scheduler}) and T2
	 * the emitted data (as a {@code T}), for each item from this {@link Flux}.
	 *
	 * <p>The provider {@link Scheduler} will be asked to {@link Scheduler#now(TimeUnit) provide time}
	 * with a granularity of {@link TimeUnit#MILLISECONDS}. In order for this operator to work as advertised, the
	 * provided Scheduler should thus return results that can be interpreted as unix timestamps.</p>
	 * <p>
	 *
	 * <img class="marble" src="doc-files/marbles/timestampForFlux.svg" alt="">
	 *
	 * @param scheduler the {@link Scheduler} to read time from
	 * @return a timestamped {@link Flux}
	 * @see Scheduler#now(TimeUnit)
	 * @see #timed(Scheduler)
	 * @see Timed#timestamp()
	 */
	public final Flux<Tuple2<Long, T>> timestamp(Scheduler scheduler) {
		Objects.requireNonNull(scheduler, "scheduler");
		return map(d -> Tuples.of(scheduler.now(TimeUnit.MILLISECONDS), d));
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Iterable} blocking on
	 * {@link Iterator#next()} calls.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/toIterable.svg" alt="">
	 * <p>
	 * Note that iterating from within threads marked as "non-blocking only" is illegal and will
	 * cause an {@link IllegalStateException} to be thrown, but obtaining the {@link Iterable}
	 * itself within these threads is ok.
	 *
	 * @return a blocking {@link Iterable}
	 */
	public final Iterable<T> toIterable() {
		return toIterable(Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Iterable} blocking on
	 * {@link Iterator#next()} calls.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/toIterableWithBatchSize.svg" alt="">
	 * <p>
	 * Note that iterating from within threads marked as "non-blocking only" is illegal and will
	 * cause an {@link IllegalStateException} to be thrown, but obtaining the {@link Iterable}
	 * itself within these threads is ok.
	 *
	 * @param batchSize the bounded capacity to prefetch from this {@link Flux} or
	 * {@code Integer.MAX_VALUE} for unbounded demand
	 * @return a blocking {@link Iterable}
	 */
	public final Iterable<T> toIterable(int batchSize) {
		return toIterable(batchSize, null);
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Iterable} blocking on
	 * {@link Iterator#next()} calls.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/toIterableWithBatchSize.svg" alt="">
	 * <p>
	 * Note that iterating from within threads marked as "non-blocking only" is illegal and will
	 * cause an {@link IllegalStateException} to be thrown, but obtaining the {@link Iterable}
	 * itself within these threads is ok.
	 *
	 * @param batchSize the bounded capacity to prefetch from this {@link Flux} or
	 * {@code Integer.MAX_VALUE} for unbounded demand
	 * @param queueProvider the supplier of the queue implementation to be used for storing
	 * elements emitted faster than the iteration
	 *
	 * @return a blocking {@link Iterable}
	 */
	public final Iterable<T> toIterable(int batchSize, @Nullable Supplier<Queue<T>>
			queueProvider) {
		final Supplier<Queue<T>> provider;
		if(queueProvider == null){
			provider = Queues.get(batchSize);
		}
		else{
			provider = () -> Hooks.wrapQueue(queueProvider.get());
		}
		Supplier<Context> contextSupplier =
				ContextPropagationSupport.shouldPropagateContextToThreadLocals() ?
						ContextPropagation::contextCaptureToEmpty : Context::empty;
		return new BlockingIterable<>(this, batchSize, provider, contextSupplier);
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Stream} blocking for each source
	 * {@link Subscriber#onNext(Object) onNext} call.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/toStream.svg" alt="">
	 * <p>
	 * Note that iterating from within threads marked as "non-blocking only" is illegal and will
	 * cause an {@link IllegalStateException} to be thrown, but obtaining the {@link Stream}
	 * itself or applying lazy intermediate operation on the stream within these threads is ok.
	 *
	 * @return a {@link Stream} of unknown size with onClose attached to {@link Subscription#cancel()}
	 */
	public final Stream<T> toStream() {
		return toStream(Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Stream} blocking for each source
	 * {@link Subscriber#onNext(Object) onNext} call.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/toStreamWithBatchSize.svg" alt="">
	 * <p>
	 * Note that iterating from within threads marked as "non-blocking only" is illegal and will
	 * cause an {@link IllegalStateException} to be thrown, but obtaining the {@link Stream}
	 * itself or applying lazy intermediate operation on the stream within these threads is ok.
	 *
	 *
	 * @param batchSize the bounded capacity to prefetch from this {@link Flux} or
	 * {@code Integer.MAX_VALUE} for unbounded demand
	 * @return a {@link Stream} of unknown size with onClose attached to {@link Subscription#cancel()}
	 */
	public final Stream<T> toStream(int batchSize) {
		final Supplier<Queue<T>> provider;
		provider = Queues.get(batchSize);
		Supplier<Context> contextSupplier =
				ContextPropagationSupport.shouldPropagateContextToThreadLocals() ?
						ContextPropagation::contextCaptureToEmpty : Context::empty;
		return new BlockingIterable<>(this, batchSize, provider, contextSupplier).stream();
	}

	/**
	 * Transform this {@link Flux} in order to generate a target {@link Flux}. Unlike {@link #transformDeferred(Function)}, the
	 * provided function is executed as part of assembly.
	 * <blockquote><pre>
	 * Function<Flux, Flux> applySchedulers = flux -> flux.subscribeOn(Schedulers.boundedElastic())
	 *                                                    .publishOn(Schedulers.parallel());
	 * flux.transform(applySchedulers).map(v -> v * v).subscribe();
	 * </pre></blockquote>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/transformForFlux.svg" alt="">
	 *
	 * @param transformer the {@link Function} to immediately map this {@link Flux} into a target {@link Flux}
	 * instance.
	 * @param <V> the item type in the returned {@link Flux}
	 *
	 * @return a new {@link Flux}
	 * @see #transformDeferred(Function) transformDeferred(Function) for deferred composition of Flux for each @link Subscriber
	 * @see #as(Function) as(Function) for a loose conversion to an arbitrary type
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public final <V> Flux<V> transform(Function<? super Flux<T>, ? extends Publisher<V>> transformer) {
		if (Hooks.DETECT_CONTEXT_LOSS) {
			transformer = new ContextTrackingFunctionWrapper(transformer);
		}
		return onAssembly(from(transformer.apply(this)));
	}

	/**
	 * Defer the transformation of this {@link Flux} in order to generate a target {@link Flux} type.
	 * A transformation will occur for each {@link Subscriber}. For instance:
	 * <blockquote><pre>
	 * flux.transformDeferred(original -> original.log());
	 * </pre></blockquote>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/transformDeferredForFlux.svg" alt="">
	 *
	 * @param transformer the {@link Function} to lazily map this {@link Flux} into a target {@link Publisher}
	 * instance for each new subscriber
	 * @param <V> the item type in the returned {@link Publisher}
	 *
	 * @return a new {@link Flux}
	 * @see #transform(Function) transform(Function) for immmediate transformation of Flux
	 * @see #transformDeferredContextual(BiFunction) transformDeferredContextual(BiFunction) for a similarly deferred transformation of Flux reading the ContextView
	 * @see #as(Function)  as(Function) for a loose conversion to an arbitrary type
	 */
	public final <V> Flux<V> transformDeferred(Function<? super Flux<T>, ? extends Publisher<V>> transformer) {
		return defer(() -> {
			if (Hooks.DETECT_CONTEXT_LOSS) {
				@SuppressWarnings({"rawtypes", "unchecked"})
				ContextTrackingFunctionWrapper<T, V> wrapper = new ContextTrackingFunctionWrapper<T, V>((Function) transformer);
				return wrapper.apply(this);
			}
			return transformer.apply(this);
		});
	}

	/**
	 * Defer the given transformation to this {@link Flux} in order to generate a
	 * target {@link Flux} type. A transformation will occur for each
	 * {@link Subscriber}. In addition, the transforming {@link BiFunction} exposes
	 * the {@link ContextView} of each {@link Subscriber}. For instance:
	 *
	 * <blockquote><pre>
	 * Flux&lt;T> fluxLogged = flux.transformDeferredContextual((original, ctx) -> original.log("for RequestID" + ctx.get("RequestID"))
	 * //...later subscribe. Each subscriber has its Context with a RequestID entry
	 * fluxLogged.contextWrite(Context.of("RequestID", "requestA").subscribe();
	 * fluxLogged.contextWrite(Context.of("RequestID", "requestB").subscribe();
	 * </pre></blockquote>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/transformDeferredForFlux.svg" alt="">
	 *
	 * @param transformer the {@link BiFunction} to lazily map this {@link Flux} into a target {@link Flux}
	 * instance upon subscription, with access to {@link ContextView}
	 * @param <V> the item type in the returned {@link Publisher}
	 * @return a new {@link Flux}
	 * @see #transform(Function) transform(Function) for immmediate transformation of Flux
	 * @see #transformDeferred(Function) transformDeferred(Function) for a similarly deferred transformation of Flux without the ContextView
	 * @see #as(Function)  as(Function) for a loose conversion to an arbitrary type
	 */
	public final <V> Flux<V> transformDeferredContextual(BiFunction<? super Flux<T>, ? super ContextView, ? extends Publisher<V>> transformer) {
		return deferContextual(ctxView -> {
			if (Hooks.DETECT_CONTEXT_LOSS) {
				ContextTrackingFunctionWrapper<T, V> wrapper = new ContextTrackingFunctionWrapper<>(
						publisher -> transformer.apply(wrap(publisher), ctxView),
						transformer.toString()
				);

				return wrapper.apply(this);
			}
			return transformer.apply(this, ctxView);
		});
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows containing
	 * {@code maxSize} elements (or less for the final window) and starting from the first item.
	 * Each {@link Flux} window will onComplete after {@code maxSize} items have been routed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithMaxSize.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the maximum number of items to emit in the window before closing it
	 *
	 * @return a {@link Flux} of {@link Flux} windows based on element count
	 */
	public final Flux<Flux<T>> window(int maxSize) {
		return onAssembly(new FluxWindow<>(this, maxSize, Queues.get(maxSize)));
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows of size
	 * {@code maxSize}, that each open every {@code skip} elements in the source.
	 *
	 * <p>
	 * When maxSize < skip : dropping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithMaxSizeLessThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize > skip : overlapping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithMaxSizeGreaterThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize == skip : exact windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithMaxSizeEqualsSkipSize.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> The overlapping variant DOES NOT discard elements, as they might be part of another still valid window.
	 * The exact window and dropping window variants bot discard elements they internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. The dropping window variant also discards elements in between windows.
	 *
	 * @param maxSize the maximum number of items to emit in the window before closing it
	 * @param skip the number of items to count before opening and emitting a new window
	 *
	 * @return a {@link Flux} of {@link Flux} windows based on element count and opened every skipCount
	 */
	public final Flux<Flux<T>> window(int maxSize, int skip) {
		return onAssembly(new FluxWindow<>(this,
				maxSize,
				skip,
				Queues.unbounded(Queues.XS_BUFFER_SIZE),
				Queues.unbounded(Queues.XS_BUFFER_SIZE)));
	}

	/**
	 * Split this {@link Flux} sequence into continuous, non-overlapping windows
	 * where the window boundary is signalled by another {@link Publisher}
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithBoundary.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors and those emitted by the {@code boundary} delivered to the window
	 * {@link Flux} are wrapped in {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param boundary a {@link Publisher} to emit any item for a split signal and complete to terminate
	 *
	 * @return a {@link Flux} of {@link Flux} windows delimited by a given {@link Publisher}
	 */
	public final Flux<Flux<T>> window(Publisher<?> boundary) {
		return onAssembly(new FluxWindowBoundary<>(this,
				boundary, Queues.unbounded(Queues.XS_BUFFER_SIZE)));
	}

	/**
	 * Split this {@link Flux} sequence into continuous, non-overlapping windows that open
	 * for a {@code windowingTimespan} {@link Duration} (as measured on the {@link Schedulers#parallel() parallel}
	 * Scheduler).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespan.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param windowingTimespan the {@link Duration} to delimit {@link Flux} windows
	 *
	 * @return a {@link Flux} of {@link Flux} windows continuously opened for a given {@link Duration}
	 */
	public final Flux<Flux<T>> window(Duration windowingTimespan) {
		return window(windowingTimespan, Schedulers.parallel());
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows that open
	 * for a given {@code windowingTimespan} {@link Duration}, after which it closes with onComplete.
	 * Each window is opened at a regular {@code timeShift} interval, starting from the
	 * first item.
	 * Both durations are measured on the {@link Schedulers#parallel() parallel} Scheduler.
	 *
	 * <p>
	 * When windowingTimespan < openWindowEvery : dropping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanLessThanOpenWindowEvery.svg" alt="">
	 * <p>
	 * When windowingTimespan > openWindowEvery : overlapping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanGreaterThanOpenWindowEvery.svg" alt="">
	 * <p>
	 * When windowingTimespan == openWindowEvery : exact windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanEqualsOpenWindowEvery.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> The overlapping variant DOES NOT discard elements, as they might be part of another still valid window.
	 * The exact window and dropping window variants bot discard elements they internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. The dropping window variant also discards elements in between windows.
	 *
	 * @param windowingTimespan the maximum {@link Flux} window {@link Duration}
	 * @param openWindowEvery the period of time at which to create new {@link Flux} windows
	 *
	 * @return a {@link Flux} of {@link Flux} windows opened at regular intervals and
	 * closed after a {@link Duration}
	 *
	 */
	public final Flux<Flux<T>> window(Duration windowingTimespan, Duration openWindowEvery) {
		return window(windowingTimespan, openWindowEvery, Schedulers.parallel());
	}

	/**
	 * Split this {@link Flux} sequence into continuous, non-overlapping windows that open
	 * for a {@code windowingTimespan} {@link Duration} (as measured on the provided {@link Scheduler}).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespan.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param windowingTimespan the {@link Duration} to delimit {@link Flux} windows
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a {@link Flux} of {@link Flux} windows continuously opened for a given {@link Duration}
	 */
	public final Flux<Flux<T>> window(Duration windowingTimespan, Scheduler timer) {
		return window(interval(windowingTimespan, timer));
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows that open
	 * for a given {@code windowingTimespan} {@link Duration}, after which it closes with onComplete.
	 * Each window is opened at a regular {@code timeShift} interval, starting from the
	 * first item.
	 * Both durations are measured on the provided {@link Scheduler}.
	 *
	 * <p>
	 * When windowingTimespan < openWindowEvery : dropping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanLessThanOpenWindowEvery.svg" alt="">
	 * <p>
	 * When windowingTimespan > openWindowEvery : overlapping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanGreaterThanOpenWindowEvery.svg" alt="">
	 * <p>
	 * When openWindowEvery == openWindowEvery : exact windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanEqualsOpenWindowEvery.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> The overlapping variant DOES NOT discard elements, as they might be part of another still valid window.
	 * The exact window and dropping window variants bot discard elements they internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. The dropping window variant also discards elements in between windows.
	 *
	 * @param windowingTimespan the maximum {@link Flux} window {@link Duration}
	 * @param openWindowEvery the period of time at which to create new {@link Flux} windows
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a {@link Flux} of {@link Flux} windows opened at regular intervals and
	 * closed after a {@link Duration}
	 */
	public final Flux<Flux<T>> window(Duration windowingTimespan, Duration openWindowEvery, Scheduler timer) {
		if (openWindowEvery.equals(windowingTimespan)) {
			return window(windowingTimespan);
		}
		return windowWhen(interval(Duration.ZERO, openWindowEvery, timer), aLong -> Mono.delay(windowingTimespan, timer));
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows containing
	 * {@code maxSize} elements (or less for the final window) and starting from the first item.
	 * Each {@link Flux} window will onComplete once it contains {@code maxSize} elements
	 * OR it has been open for the given {@link Duration} (as measured on the {@link Schedulers#parallel() parallel}
	 * Scheduler).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowTimeout.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the maximum number of items to emit in the window before closing it
	 * @param maxTime the maximum {@link Duration} since the window was opened before closing it
	 *
	 * @return a {@link Flux} of {@link Flux} windows based on element count and duration
	 */
	public final Flux<Flux<T>> windowTimeout(int maxSize, Duration maxTime) {
		return windowTimeout(maxSize, maxTime , Schedulers.parallel());
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows containing
	 * {@code maxSize} elements (or less for the final window) and starting from the first item.
	 * Each {@link Flux} window will onComplete once it contains {@code maxSize} elements
	 * OR it has been open for the given {@link Duration} (as measured on the {@link Schedulers#parallel() parallel}
	 * Scheduler).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowTimeout.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the maximum number of items to emit in the window before closing it
	 * @param maxTime the maximum {@link Duration} since the window was opened before closing it
	 * @param fairBackpressure define whether operator request unbounded demand or
	 *                            prefetch by maxSize
	 *
	 * @return a {@link Flux} of {@link Flux} windows based on element count and duration
	 */
	public final Flux<Flux<T>> windowTimeout(int maxSize, Duration maxTime, boolean fairBackpressure) {
		return windowTimeout(maxSize, maxTime , Schedulers.parallel(), fairBackpressure);
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows containing
	 * {@code maxSize} elements (or less for the final window) and starting from the first item.
	 * Each {@link Flux} window will onComplete once it contains {@code maxSize} elements
	 * OR it has been open for the given {@link Duration} (as measured on the provided
	 * {@link Scheduler}).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowTimeout.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the maximum number of items to emit in the window before closing it
	 * @param maxTime the maximum {@link Duration} since the window was opened before closing it
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a {@link Flux} of {@link Flux} windows based on element count and duration
	 */
	public final Flux<Flux<T>> windowTimeout(int maxSize, Duration maxTime, Scheduler timer) {
		return windowTimeout(maxSize, maxTime, timer, false);
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows containing
	 * {@code maxSize} elements (or less for the final window) and starting from the first item.
	 * Each {@link Flux} window will onComplete once it contains {@code maxSize} elements
	 * OR it has been open for the given {@link Duration} (as measured on the provided
	 * {@link Scheduler}).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowTimeout.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the maximum number of items to emit in the window before closing it
	 * @param maxTime the maximum {@link Duration} since the window was opened before closing it
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 * @param fairBackpressure define whether operator request unbounded demand or
	 *                            prefetch by maxSize
	 *
	 * @return a {@link Flux} of {@link Flux} windows based on element count and duration
	 */
	public final Flux<Flux<T>> windowTimeout(int maxSize, Duration maxTime, Scheduler timer, boolean fairBackpressure) {
		return onAssembly(new FluxWindowTimeout<>(this, maxSize, maxTime.toNanos(), TimeUnit.NANOSECONDS, timer, fairBackpressure));
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows delimited by the
	 * given predicate. A new window is opened each time the predicate returns true, at which
	 * point the previous window will receive the triggering element then onComplete.
	 * <p>
	 * Windows are lazily made available downstream at the point where they receive their
	 * first event (an element is pushed, the window errors). This variant shouldn't
	 * expose empty windows, as the separators are emitted into
	 * the windows they close.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntil.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @param boundaryTrigger a predicate that triggers the next window when it becomes true.
	 * @return a {@link Flux} of {@link Flux} windows, bounded depending
	 * on the predicate.
	 */
	public final Flux<Flux<T>> windowUntil(Predicate<T> boundaryTrigger) {
		return windowUntil(boundaryTrigger, false);
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows delimited by the
	 * given predicate. A new window is opened each time the predicate returns true.
	 * <p>
	 * Windows are lazily made available downstream at the point where they receive their
	 * first event (an element is pushed, the window completes or errors).
	 * <p>
	 * If {@code cutBefore} is true, the old window will onComplete and the triggering
	 * element will be emitted in the new window, which becomes immediately available.
	 * This variant can emit an empty window if the sequence starts with a separator.
	 * <p>
	 * Otherwise, the triggering element will be emitted in the old window before it does
	 * onComplete, similar to {@link #windowUntil(Predicate)}. This variant shouldn't
	 * expose empty windows, as the separators are emitted into the windows they close.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntilWithCutBefore.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @param boundaryTrigger a predicate that triggers the next window when it becomes true.
	 * @param cutBefore set to true to include the triggering element in the new window rather than the old.
	 * @return a {@link Flux} of {@link Flux} windows, bounded depending
	 * on the predicate.
	 */
	public final Flux<Flux<T>> windowUntil(Predicate<T> boundaryTrigger, boolean cutBefore) {
		return windowUntil(boundaryTrigger, cutBefore, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows delimited by the given
	 * predicate and using a prefetch. A new window is opened each time the predicate
	 * returns true.
	 * <p>
	 * Windows are lazily made available downstream at the point where they receive their
	 * first event (an element is pushed, the window completes or errors).
	 * <p>
	 * If {@code cutBefore} is true, the old window will onComplete and the triggering
	 * element will be emitted in the new window. This variant can emit an empty window
	 * if the sequence starts with a separator.
	 * <p>
	 * Otherwise, the triggering element will be emitted in the old window before it does
	 * onComplete, similar to {@link #windowUntil(Predicate)}. This variant shouldn't
	 * expose empty windows, as the separators are emitted into the windows they close.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntilWithCutBefore.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @param boundaryTrigger a predicate that triggers the next window when it becomes true.
	 * @param cutBefore set to true to include the triggering element in the new window rather than the old.
	 * @param prefetch the request size to use for this {@link Flux}.
	 * @return a {@link Flux} of {@link Flux} windows, bounded depending
	 * on the predicate.
	 */
	public final Flux<Flux<T>> windowUntil(Predicate<T> boundaryTrigger, boolean cutBefore, int prefetch) {
		return onAssembly(new FluxWindowPredicate<>(this,
				Queues.unbounded(prefetch),
				Queues.unbounded(prefetch),
				prefetch,
				boundaryTrigger,
				cutBefore ? FluxBufferPredicate.Mode.UNTIL_CUT_BEFORE : FluxBufferPredicate.Mode.UNTIL));
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another) into multiple {@link Flux} windows.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntilChanged.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @return a microbatched {@link Flux} of {@link Flux} windows.
	 */
	public final Flux<Flux<T>> windowUntilChanged() {
		return windowUntilChanged(identityFunction());
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another), as compared by a key extracted through the user provided {@link
	 * Function}, into multiple {@link Flux} windows.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntilChangedWithKeySelector.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @return a microbatched {@link Flux} of {@link Flux} windows.
	 */
	public final <V> Flux<Flux<T>> windowUntilChanged(Function<? super T, ? super V> keySelector) {
		return windowUntilChanged(keySelector, equalPredicate());
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another), as compared by a key extracted through the user provided {@link
	 * Function} and compared using a supplied {@link BiPredicate}, into multiple
	 * {@link Flux} windows.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntilChangedWithKeySelector.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @param keyComparator predicate used to compare keys
	 * @return a microbatched {@link Flux} of {@link Flux} windows.
	 */
	public final <V> Flux<Flux<T>> windowUntilChanged(Function<? super T, ? extends V> keySelector,
			BiPredicate<? super V, ? super V> keyComparator) {
		return Flux.defer(() -> windowUntil(new FluxBufferPredicate.ChangedPredicate<T, V>
				(keySelector, keyComparator), true));
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows that stay open
	 * while a given predicate matches the source elements. Once the predicate returns
	 * false, the window closes with an onComplete and the triggering element is discarded.
	 * <p>
	 * Windows are lazily made available downstream at the point where they receive their
	 * first event (an element is pushed, the window completes or errors). Empty windows
	 * can happen when a sequence starts with a separator or contains multiple separators,
	 * but a sequence that finishes with a separator won't cause a remainder empty window
	 * to be emitted.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWhile.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal, as well as the triggering element(s) (that doesn't match
	 * the predicate). Upon cancellation of the current window, it also discards the remaining elements
	 * that were bound for it until the main sequence completes or creation of a new window is triggered.
	 *
	 * @param inclusionPredicate a predicate that triggers the next window when it becomes false.
	 * @return a {@link Flux} of {@link Flux} windows, each containing
	 * subsequent elements that all passed a predicate.
	 */
	public final Flux<Flux<T>> windowWhile(Predicate<T> inclusionPredicate) {
		return windowWhile(inclusionPredicate, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows that stay open
	 * while a given predicate matches the source elements. Once the predicate returns
	 * false, the window closes with an onComplete and the triggering element is discarded.
	 * <p>
	 * Windows are lazily made available downstream at the point where they receive their
	 * first event (an element is pushed, the window completes or errors). Empty windows
	 * can happen when a sequence starts with a separator or contains multiple separators,
	 * but a sequence that finishes with a separator won't cause a remainder empty window
	 * to be emitted.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWhile.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal, as well as the triggering element(s) (that doesn't match
	 * the predicate). Upon cancellation of the current window, it also discards the remaining elements
	 * that were bound for it until the main sequence completes or creation of a new window is triggered.
	 *
	 * @param inclusionPredicate a predicate that triggers the next window when it becomes false.
	 * @param prefetch the request size to use for this {@link Flux}.
	 * @return a {@link Flux} of {@link Flux} windows, each containing
	 * subsequent elements that all passed a predicate.
	 */
	public final Flux<Flux<T>> windowWhile(Predicate<T> inclusionPredicate, int prefetch) {
		return onAssembly(new FluxWindowPredicate<>(this,
				Queues.unbounded(prefetch),
				Queues.unbounded(prefetch),
				prefetch,
				inclusionPredicate,
				FluxBufferPredicate.Mode.WHILE));
	}

	/**
	 * Split this {@link Flux} sequence into potentially overlapping windows controlled by items of a
	 * start {@link Publisher} and end {@link Publisher} derived from the start values.
	 *
	 * <p>
	 * When Open signal is strictly not overlapping Close signal : dropping windows
	 * <p>
	 * When Open signal is strictly more frequent than Close signal : overlapping windows
	 * <p>
	 * When Open signal is exactly coordinated with Close signal : exact windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWhen.svg" alt="">
	 *
	 * <p>
	 * Note that windows are a live view of part of the underlying source publisher,
	 * and as such their lifecycle is tied to that source. As a result, it is not possible
	 * to subscribe to a window more than once: they are unicast.
	 * This is most noticeable when trying to {@link #retry()} or {@link #repeat()} a window,
	 * as these operators are based on re-subscription.
	 *
	 * <p>
	 * To distinguish errors emitted by the processing of individual windows, source
	 * sequence errors delivered to the window {@link Flux} are wrapped in
	 * {@link reactor.core.Exceptions.SourceException}.
	 *
	 * <p><strong>Discard Support:</strong> This operator DOES NOT discard elements.
	 *
	 * @param bucketOpening a {@link Publisher} that opens a new window when it emits any item
	 * @param closeSelector a {@link Function} given an opening signal and returning a {@link Publisher} that
	 * will close the window when emitting
	 *
	 * @param <U> the type of the sequence opening windows
	 * @param <V> the type of the sequence closing windows opened by the bucketOpening Publisher's elements
	 *
	 * @return a {@link Flux} of {@link Flux} windows opened by signals from a first
	 * {@link Publisher} and lasting until a selected second {@link Publisher} emits
	 */
	public final <U, V> Flux<Flux<T>> windowWhen(Publisher<U> bucketOpening,
			final Function<? super U, ? extends Publisher<V>> closeSelector) {
		return onAssembly(new FluxWindowWhen<>(this,
				bucketOpening,
				closeSelector,
				Queues.unbounded(Queues.XS_BUFFER_SIZE)));
	}

	/**
	 * Combine the most recently emitted values from both this {@link Flux} and another
	 * {@link Publisher} through a {@link BiFunction} and emits the result.
	 * <p>
	 * The operator will drop values from this {@link Flux} until the other
	 * {@link Publisher} produces any value.
	 * <p>
	 * If the other {@link Publisher} completes without any value, the sequence is completed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/withLatestFrom.svg" alt="">
	 *
	 * @param other the {@link Publisher} to combine with
	 * @param resultSelector the bi-function called with each pair of source and other
	 * elements that should return a single value to be emitted
	 *
	 * @param <U> the other {@link Publisher} sequence type
	 * @param <R> the result type
	 *
	 * @return a combined {@link Flux} gated by another {@link Publisher}
	 */
	public final <U, R> Flux<R> withLatestFrom(Publisher<? extends U> other, BiFunction<? super T, ? super U, ?
			extends R > resultSelector){
		return onAssembly(new FluxWithLatestFrom<>(this, other, resultSelector));
	}

	/**
	 * Zip this {@link Flux} with another {@link Publisher} source, that is to say wait
	 * for both to emit one element and combine these elements once into a {@link Tuple2}.
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipWithOtherForFlux.svg" alt="">
	 *
	 * @param source2 The second source {@link Publisher} to zip with this {@link Flux}.
	 * @param <T2> type of the value from source2
	 *
	 * @return a zipped {@link Flux}
	 *
	 */
	public final <T2> Flux<Tuple2<T, T2>> zipWith(Publisher<? extends T2> source2) {
		return zipWith(source2, tuple2Function());
	}

	/**
	 * Zip this {@link Flux} with another {@link Publisher} source, that is to say wait
	 * for both to emit one element and combine these elements using a {@code combinator}
	 * {@link BiFunction}
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipWithOtherUsingZipperForFlux.svg" alt="">
	 *
	 * @param source2 The second source {@link Publisher} to zip with this {@link Flux}.
	 * @param combinator The aggregate function that will receive a unique value from each
	 * source and return the value to signal downstream
	 * @param <T2> type of the value from source2
	 * @param <V> The produced output after transformation by the combinator
	 *
	 * @return a zipped {@link Flux}
	 */
	public final <T2, V> Flux<V> zipWith(Publisher<? extends T2> source2,
			final BiFunction<? super T, ? super T2, ? extends V> combinator) {
		if (this instanceof FluxZip) {
			@SuppressWarnings("unchecked")
			FluxZip<T, V> o = (FluxZip<T, V>) this;
			Flux<V> result = o.zipAdditionalSource(source2, combinator);
			if (result != null) {
				return result;
			}
		}
		return zip(this, source2, combinator);
	}

	/**
	 * Zip this {@link Flux} with another {@link Publisher} source, that is to say wait
	 * for both to emit one element and combine these elements using a {@code combinator}
	 * {@link BiFunction}
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipWithOtherUsingZipperForFlux.svg" alt="">
	 *
	 * @param source2 The second source {@link Publisher} to zip with this {@link Flux}.
	 * @param prefetch the request size to use for this {@link Flux} and the other {@link Publisher}
	 * @param combinator The aggregate function that will receive a unique value from each
	 * source and return the value to signal downstream
	 * @param <T2> type of the value from source2
	 * @param <V> The produced output after transformation by the combinator
	 *
	 * @return a zipped {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	public final <T2, V> Flux<V> zipWith(Publisher<? extends T2> source2,
			int prefetch,
			BiFunction<? super T, ? super T2, ? extends V> combinator) {
		return zip(objects -> combinator.apply((T) objects[0], (T2) objects[1]),
				prefetch,
				this,
				source2);
	}

	/**
	 * Zip this {@link Flux} with another {@link Publisher} source, that is to say wait
	 * for both to emit one element and combine these elements once into a {@link Tuple2}.
	 * The operator will continue doing so until any of the sources completes.
	 * Errors will immediately be forwarded.
	 * This "Step-Merge" processing is especially useful in Scatter-Gather scenarios.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipWithOtherForFlux.svg" alt="">
	 *
	 * @param source2 The second source {@link Publisher} to zip with this {@link Flux}.
	 * @param prefetch the request size to use for this {@link Flux} and the other {@link Publisher}
	 * @param <T2> type of the value from source2
	 *
	 * @return a zipped {@link Flux}
	 */
	public final <T2> Flux<Tuple2<T, T2>> zipWith(Publisher<? extends T2> source2, int prefetch) {
		return zipWith(source2, prefetch, tuple2Function());
	}

	/**
	 * Zip elements from this {@link Flux} with the content of an {@link Iterable}, that is
	 * to say combine one element from each, pairwise, into a {@link Tuple2}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipWithIterableForFlux.svg" alt="">
	 *
	 * @param iterable the {@link Iterable} to zip with
	 * @param <T2> the value type of the other iterable sequence
	 *
	 * @return a zipped {@link Flux}
	 *
	 */
	public final <T2> Flux<Tuple2<T, T2>> zipWithIterable(Iterable<? extends T2> iterable) {
		return zipWithIterable(iterable, tuple2Function());
	}

	/**
	 * Zip elements from this {@link Flux} with the content of an {@link Iterable}, that is
	 * to say combine one element from each, pairwise, using the given zipper {@link BiFunction}.
	 *
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/zipWithIterableUsingZipperForFlux.svg" alt="">
	 *
	 * @param iterable the {@link Iterable} to zip with
	 * @param zipper the {@link BiFunction} pair combinator
	 *
	 * @param <T2> the value type of the other iterable sequence
	 * @param <V> the result type
	 *
	 * @return a zipped {@link Flux}
	 *
	 */
	public final <T2, V> Flux<V> zipWithIterable(Iterable<? extends T2> iterable,
			BiFunction<? super T, ? super T2, ? extends V> zipper) {
		return onAssembly(new FluxZipIterable<>(this, iterable, zipper));
	}

	/**
	 * To be used by custom operators: invokes assembly {@link Hooks} pointcut given a
	 * {@link Flux}, potentially returning a new {@link Flux}. This is for example useful
	 * to activate cross-cutting concerns at assembly time, eg. a generalized
	 * {@link #checkpoint()}.
	 *
	 * @param <T> the value type
	 * @param source the source to apply assembly hooks onto
	 *
	 * @return the source, potentially wrapped with assembly time cross-cutting behavior
	 */
	@SuppressWarnings("unchecked")
	protected static <T> Flux<T> onAssembly(Flux<T> source) {
		Function<Publisher, Publisher> hook = Hooks.onEachOperatorHook;
		if(hook != null) {
			source = (Flux<T>) hook.apply(source);
		}
		if (Hooks.GLOBAL_TRACE) {
			AssemblySnapshot stacktrace = new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get());
			source = (Flux<T>) Hooks.addAssemblyInfo(source, stacktrace);
		}
		return source;
	}

	/**
	 * To be used by custom operators: invokes assembly {@link Hooks} pointcut given a
	 * {@link ConnectableFlux}, potentially returning a new {@link ConnectableFlux}. This
	 * is for example useful to activate cross-cutting concerns at assembly time, eg. a
	 * generalized {@link #checkpoint()}.
	 *
	 * @param <T> the value type
	 * @param source the source to apply assembly hooks onto
	 *
	 * @return the source, potentially wrapped with assembly time cross-cutting behavior
	 */
	@SuppressWarnings("unchecked")
	protected static <T> ConnectableFlux<T> onAssembly(ConnectableFlux<T> source) {
		Function<Publisher, Publisher> hook = Hooks.onEachOperatorHook;
		if(hook != null) {
			source = (ConnectableFlux<T>) hook.apply(source);
		}
		if (Hooks.GLOBAL_TRACE) {
			AssemblySnapshot stacktrace = new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get());
			source = (ConnectableFlux<T>) Hooks.addAssemblyInfo(source, stacktrace);
		}
		return source;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}


	final <V> Flux<V> flatMap(Function<? super T, ? extends Publisher<? extends
			V>> mapper, boolean delayError, int concurrency, int prefetch) {
		return onAssembly(new FluxFlatMap<>(
				this,
				mapper,
				delayError,
				concurrency,
				Queues.get(concurrency),
				prefetch,
				Queues.get(prefetch)
		));
	}

	final <R> Flux<R> flatMapSequential(Function<? super T, ? extends
			Publisher<? extends R>> mapper, boolean delayError, int maxConcurrency,
			int prefetch) {
		return onAssembly(new FluxMergeSequential<>(this, mapper, maxConcurrency,
				prefetch, delayError ? FluxConcatMap.ErrorMode.END :
				FluxConcatMap.ErrorMode.IMMEDIATE));
	}

	static <T> Flux<T> doOnSignal(Flux<T> source,
			@Nullable Consumer<? super Subscription> onSubscribe,
			@Nullable Consumer<? super T> onNext,
			@Nullable Consumer<? super Throwable> onError,
			@Nullable Runnable onComplete,
			@Nullable Runnable onAfterTerminate,
			@Nullable LongConsumer onRequest,
			@Nullable Runnable onCancel) {
		if (source instanceof Fuseable) {
			return onAssembly(new FluxPeekFuseable<>(source,
					onSubscribe,
					onNext,
					onError,
					onComplete,
					onAfterTerminate,
					onRequest,
					onCancel));
		}
		return onAssembly(new FluxPeek<>(source,
				onSubscribe,
				onNext,
				onError,
				onComplete,
				onAfterTerminate,
				onRequest,
				onCancel));
	}

	/**
	 * Returns the appropriate Mono instance for a known Supplier Flux, WITHOUT applying hooks
	 * (see {@link #wrap(Publisher)}).
	 *
	 * @param supplier the supplier Flux
	 *
	 * @return the mono representing that Flux
	 */
	static <T> Mono<T> wrapToMono(Callable<T> supplier) {
		if (supplier instanceof Fuseable.ScalarCallable) {
			Fuseable.ScalarCallable<T> scalarCallable = (Fuseable.ScalarCallable<T>) supplier;

			T v;
			try {
				v = scalarCallable.call();
			}
			catch (Exception e) {
				return new MonoError<>(Exceptions.unwrap(e));
			}
			if (v == null) {
				return MonoEmpty.instance();
			}
			return new MonoJust<>(v);
		}
		return new MonoCallable<>(supplier);
	}

	@SafeVarargs
	static <I> Flux<I> merge(int prefetch, boolean delayError, Publisher<? extends I>... sources) {
		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from(sources[0]);
		}
		return onAssembly(new FluxMerge<>(sources,
				delayError,
				sources.length,
				Queues.get(sources.length),
				prefetch,
				Queues.get(prefetch)));
	}

	@SafeVarargs
	static <I> Flux<I> mergeSequential(int prefetch, boolean delayError,
			Publisher<? extends I>... sources) {
		if (sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from(sources[0]);
		}
		return onAssembly(new FluxMergeSequential<>(new FluxArray<>(sources),
				identityFunction(), sources.length, prefetch,
				delayError ? FluxConcatMap.ErrorMode.END : FluxConcatMap.ErrorMode.IMMEDIATE));
	}

	static <T> Flux<T> mergeSequential(Publisher<? extends Publisher<? extends T>> sources,
			boolean delayError, int maxConcurrency, int prefetch) {
		return onAssembly(new FluxMergeSequential<>(from(sources),
				identityFunction(),
				maxConcurrency, prefetch, delayError ? FluxConcatMap.ErrorMode.END :
				FluxConcatMap.ErrorMode.IMMEDIATE));
	}

	static <I> Flux<I> mergeSequential(Iterable<? extends Publisher<? extends I>> sources,
			boolean delayError, int maxConcurrency, int prefetch) {
		return onAssembly(new FluxMergeSequential<>(new FluxIterable<>(sources),
				identityFunction(), maxConcurrency, prefetch,
				delayError ? FluxConcatMap.ErrorMode.END : FluxConcatMap.ErrorMode.IMMEDIATE));
	}


	static BooleanSupplier countingBooleanSupplier(BooleanSupplier predicate, long max) {
		if (max <= 0) {
			return predicate;
		}
		return new BooleanSupplier() {
			long n;

			@Override
			public boolean getAsBoolean() {
				return n++ < max && predicate.getAsBoolean();
			}
		};
	}

	static <O> Predicate<O> countingPredicate(Predicate<O> predicate, long max) {
		if (max == 0) {
			return predicate;
		}
		return new Predicate<O>() {
			long n;

			@Override
			public boolean test(O o) {
				return n++ < max && predicate.test(o);
			}
		};
	}

	@SuppressWarnings("unchecked")
	static <O> Supplier<Set<O>> hashSetSupplier() {
		return SET_SUPPLIER;
	}

	@SuppressWarnings("unchecked")
	static <O> Supplier<List<O>> listSupplier() {
		return LIST_SUPPLIER;
	}

	@SuppressWarnings("unchecked")
	static <U, V> BiPredicate<U, V> equalPredicate() {
		return OBJECT_EQUAL;
	}

	@SuppressWarnings("unchecked")
	static <T> Function<T, T> identityFunction(){
		return IDENTITY_FUNCTION;
	}

	@SuppressWarnings("unchecked")
	static <A, B> BiFunction<A, B, Tuple2<A, B>> tuple2Function() {
		return TUPLE2_BIFUNCTION;
	}

	/**
	 * Unchecked wrap of {@link Publisher} as {@link Flux}, supporting {@link Fuseable} sources.
	 * Note that this bypasses {@link Hooks#onEachOperator(String, Function) assembly hooks}.
	 *
	 * @param source the {@link Publisher} to wrap
	 * @param <I> input upstream type
	 * @return a wrapped {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	static <I> Flux<I> wrap(Publisher<? extends I> source) {
		boolean shouldWrap = ContextPropagationSupport.shouldWrapPublisher(source);
		if (source instanceof Flux) {
			if (!shouldWrap) {
				return (Flux<I>) source;
			}
			return ContextPropagation.fluxRestoreThreadLocals(
					(Flux<? extends I>) source, source instanceof Fuseable);
		}

		//for scalars we'll instantiate the operators directly to avoid onAssembly
		if (source instanceof Fuseable.ScalarCallable) {
			try {
				@SuppressWarnings("unchecked") I t =
						((Fuseable.ScalarCallable<I>) source).call();
				if (t != null) {
					return new FluxJust<>(t);
				}
				return FluxEmpty.instance();
			}
			catch (Exception e) {
				return new FluxError<>(Exceptions.unwrap(e));
			}
		}

		Flux<I> target;
		boolean fuseable = source instanceof Fuseable;
		if (source instanceof Mono) {
			if (fuseable) {
				target = new FluxSourceMonoFuseable<>((Mono<I>) source);
			} else {
				target = new FluxSourceMono<>((Mono<I>) source);
			}
		} else if (fuseable) {
			target = new FluxSourceFuseable<>(source);
		} else {
			target = new FluxSource<>(source);
		}
		if (shouldWrap) {
			return ContextPropagation.fluxRestoreThreadLocals(target, fuseable);
		}
		return target;
	}

	@SuppressWarnings("rawtypes")
	static final BiFunction      TUPLE2_BIFUNCTION       = Tuples::of;
	@SuppressWarnings("rawtypes")
	static final Supplier        LIST_SUPPLIER           = ArrayList::new;
	@SuppressWarnings("rawtypes")
	static final Supplier        SET_SUPPLIER            = HashSet::new;
	static final BooleanSupplier ALWAYS_BOOLEAN_SUPPLIER = () -> true;
	@SuppressWarnings("rawtypes")
	static final BiPredicate     OBJECT_EQUAL            = Object::equals;
	@SuppressWarnings("rawtypes")
	static final Function        IDENTITY_FUNCTION       = Function.identity();

}
