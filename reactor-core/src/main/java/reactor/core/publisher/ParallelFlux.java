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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.FluxConcatMap.ErrorMode;
import reactor.core.publisher.FluxOnAssembly.CheckpointHeavySnapshot;
import reactor.core.publisher.FluxOnAssembly.CheckpointLightSnapshot;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshot;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * A ParallelFlux publishes to an array of Subscribers, in parallel 'rails' (or
 * {@link #groups() 'groups'}).
 * <p>
 * Use {@link #from} to start processing a regular Publisher in 'rails', which each
 * cover a subset of the original Publisher's data. {@link Flux#parallel()} is a
 * convenient shortcut to achieve that on a {@link Flux}.
 * <p>
 * Use {@link #runOn} to introduce where each 'rail' should run on thread-wise.
 * <p>
 * Use {@link #sequential} to merge the sources back into a single {@link Flux}.
 * <p>
 * Use {@link #then} to listen for all rails termination in the produced {@link Mono}
 * <p>
 * {@link #subscribe(Subscriber)} if you simply want to subscribe to the merged sequence.
 * Note that other variants like {@link #subscribe(Consumer)} instead do multiple
 * subscribes, one on each rail (which means that the lambdas should be as stateless and
 * side-effect free as possible).
 *
 *
 * @param <T> the value type
 */
public abstract class ParallelFlux<T> implements CorePublisher<T> {

	/**
	 * Take a Publisher and prepare to consume it on multiple 'rails' (one per CPU core)
	 * in a round-robin fashion. Equivalent to {@link Flux#parallel}.
	 *
	 * @param <T> the value type
	 * @param source the source Publisher
	 *
	 * @return the {@link ParallelFlux} instance
	 */
	public static <T> ParallelFlux<T> from(Publisher<? extends T> source) {
		return from(source, Schedulers.DEFAULT_POOL_SIZE, Queues.SMALL_BUFFER_SIZE,
				Queues.small());
	}

	/**
	 * Take a Publisher and prepare to consume it on {@code parallelism} number of 'rails',
	 * possibly ordered and in a round-robin fashion.
	 *
	 * @param <T> the value type
	 * @param source the source Publisher
	 * @param parallelism the number of parallel rails
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public static <T> ParallelFlux<T> from(Publisher<? extends T> source,
			int parallelism) {
		return from(source,
				parallelism, Queues.SMALL_BUFFER_SIZE,
				Queues.small());
	}

	/**
	 * Take a Publisher and prepare to consume it on {@code parallelism} number of 'rails'
	 * and in a round-robin fashion and use custom prefetch amount and queue
	 * for dealing with the source Publisher's values.
	 *
	 * @param <T> the value type
	 * @param source the source Publisher
	 * @param parallelism the number of parallel rails
	 * @param prefetch the number of values to prefetch from the source
	 * @param queueSupplier the queue structure supplier to hold the prefetched values
	 * from the source until there is a rail ready to process it.
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public static <T> ParallelFlux<T> from(Publisher<? extends T> source,
			int parallelism,
			int prefetch,
			Supplier<Queue<T>> queueSupplier) {
		Objects.requireNonNull(queueSupplier, "queueSupplier");
		Objects.requireNonNull(source, "source");

		return onAssembly(new ParallelSource<>(source,
				parallelism,
				prefetch, queueSupplier));
	}

	/**
	 * Wraps multiple Publishers into a {@link ParallelFlux} which runs them in parallel and
	 * unordered.
	 *
	 * @param <T> the value type
	 * @param publishers the array of publishers
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	@SafeVarargs
	public static <T> ParallelFlux<T> from(Publisher<T>... publishers) {
		return onAssembly(new ParallelArraySource<>(publishers));
	}

	/**
	 * Perform a fluent transformation to a value via a converter function which receives
	 * this ParallelFlux.
	 *
	 * @param <U> the output value type
	 * @param converter the converter function from {@link ParallelFlux} to some type
	 *
	 * @return the value returned by the converter function
	 */
	public final <U> U as(Function<? super ParallelFlux<T>, U> converter) {
		return converter.apply(this);
	}

	/**
	 * Activate traceback (full assembly tracing) for this particular {@link ParallelFlux}, in case of an
	 * error upstream of the checkpoint. Tracing incurs the cost of an exception stack trace
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
	 * @return the assembly tracing {@link ParallelFlux}
	 */
	public final ParallelFlux<T> checkpoint() {
		AssemblySnapshot stacktrace = new CheckpointHeavySnapshot(null, Traces.callSiteSupplierFactory.get());
		return new ParallelFluxOnAssembly<>(this, stacktrace);
	}

	/**
	 * Activate traceback (assembly marker) for this particular {@link ParallelFlux} by giving it a description that
	 * will be reflected in the assembly traceback in case of an error upstream of the
	 * checkpoint. Note that unlike {@link #checkpoint()}, this doesn't create a
	 * filled stack trace, avoiding the main cost of the operator.
	 * However, as a trade-off the description must be unique enough for the user to find
	 * out where this ParallelFlux was assembled. If you only want a generic description, and
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
	 * @return the assembly marked {@link ParallelFlux}
	 */
	public final ParallelFlux<T> checkpoint(String description) {
		return new ParallelFluxOnAssembly<>(this, new CheckpointLightSnapshot(description));
	}

	/**
	 * Activate traceback (full assembly tracing or the lighter assembly marking depending on the
	 * {@code forceStackTrace} option).
	 * <p>
	 * By setting the {@code forceStackTrace} parameter to {@literal true}, activate assembly
	 * tracing for this particular {@link ParallelFlux} and give it a description that
	 * will be reflected in the assembly traceback in case of an error upstream of the
	 * checkpoint. Note that unlike {@link #checkpoint(String)}, this will incur
	 * the cost of an exception stack trace creation. The description could for
	 * example be a meaningful name for the assembled ParallelFlux or a wider correlation ID,
	 * since the stack trace will always provide enough information to locate where this
	 * ParallelFlux was assembled.
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
	 * @return the assembly marked {@link ParallelFlux}.
	 */
	public final ParallelFlux<T> checkpoint(String description, boolean forceStackTrace) {
		final AssemblySnapshot stacktrace;
		if (!forceStackTrace) {
			stacktrace = new CheckpointLightSnapshot(description);
		}
		else {
			stacktrace = new CheckpointHeavySnapshot(description, Traces.callSiteSupplierFactory.get());
		}

		return new ParallelFluxOnAssembly<>(this, stacktrace);
	}

	/**
	 * Collect the elements in each rail into a collection supplied via a
	 * collectionSupplier and collected into with a collector action, emitting the
	 * collection at the end.
	 *
	 * @param <C> the collection type
	 * @param collectionSupplier the supplier of the collection in each rail
	 * @param collector the collector, taking the per-rail collection and the current
	 * item
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final <C> ParallelFlux<C> collect(Supplier<? extends C> collectionSupplier,
			BiConsumer<? super C, ? super T> collector) {
		return onAssembly(new ParallelCollect<>(this, collectionSupplier, collector));
	}

	/**
	 * Sorts the 'rails' according to the comparator and returns a full sorted list as a
	 * Publisher.
	 * <p>
	 * This operator requires a finite source ParallelFlux.
	 *
	 * @param comparator the comparator to compare elements
	 *
	 * @return the new Flux instance
	 */
	public final Mono<List<T>> collectSortedList(Comparator<? super T> comparator) {
		return collectSortedList(comparator, 16);
	}

	/**
	 * Sorts the 'rails' according to the comparator and returns a full sorted list as a
	 * Publisher.
	 * <p>
	 * This operator requires a finite source ParallelFlux.
	 *
	 * @param comparator the comparator to compare elements
	 * @param capacityHint the expected number of total elements
	 *
	 * @return the new Mono instance
	 */
	public final Mono<List<T>> collectSortedList(Comparator<? super T> comparator,
			int capacityHint) {
		int ch = capacityHint / parallelism() + 1;
		ParallelFlux<List<T>> railReduced =
				reduce(() -> new ArrayList<>(ch), (a, b) -> {
					a.add(b);
					return a;
				});
		ParallelFlux<List<T>> railSorted = railReduced.map(list -> {
			list.sort(comparator);
			return list;
		});

		Mono<List<T>> merged = railSorted.reduce((a, b) -> sortedMerger(a, b, comparator));

		return merged;
	}

	/**
	 * Generates and concatenates Publishers on each 'rail', signalling errors immediately
	 * and generating 2 publishers upfront.
	 *
	 * @param <R> the result type
	 * @param mapper the function to map each rail's value into a Publisher source and the
	 * inner Publishers (immediate, boundary, end)
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final <R> ParallelFlux<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return concatMap(mapper, 2, ErrorMode.IMMEDIATE);
	}

	/**
	 * Generates and concatenates Publishers on each 'rail', signalling errors immediately
	 * and using the given prefetch amount for generating Publishers upfront.
	 *
	 * @param <R> the result type
	 * @param mapper the function to map each rail's value into a Publisher
	 * @param prefetch the number of items to prefetch from each inner Publisher source
	 * and the inner Publishers (immediate, boundary, end)
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final <R> ParallelFlux<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper,
			int prefetch) {
		return concatMap(mapper, prefetch, ErrorMode.IMMEDIATE);
	}

	/**
	 * Generates and concatenates Publishers on each 'rail',  delaying errors
	 * and generating 2 publishers upfront.
	 *
	 * @param <R> the result type
	 * @param mapper the function to map each rail's value into a Publisher
	 * source and the inner Publishers (immediate, boundary, end)
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final <R> ParallelFlux<R> concatMapDelayError(Function<? super T, ? extends
			Publisher<? extends R>> mapper) {
		return concatMap(mapper, 2, ErrorMode.END);
	}

	/**
	 * Run the specified runnable when a 'rail' completes or signals an error.
	 *
	 * @param afterTerminate the callback
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> doAfterTerminate(Runnable afterTerminate) {
		Objects.requireNonNull(afterTerminate, "afterTerminate");
		return doOnSignal(this, null, null, null, null, afterTerminate, null, null, null);
	}

	/**
	 * Run the specified runnable when a 'rail' receives a cancellation.
	 *
	 * @param onCancel the callback
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> doOnCancel(Runnable onCancel) {
		Objects.requireNonNull(onCancel, "onCancel");
		return doOnSignal(this, null, null, null, null, null, null, null, onCancel);
	}

	/**
	 * Run the specified runnable when a 'rail' completes.
	 *
	 * @param onComplete the callback
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> doOnComplete(Runnable onComplete) {
		Objects.requireNonNull(onComplete, "onComplete");
		return doOnSignal(this, null, null, null, onComplete, null, null, null, null);
	}

	/**
	 * Triggers side-effects when the {@link ParallelFlux} emits an item, fails with an error
	 * or completes successfully. All these events are represented as a {@link Signal}
	 * that is passed to the side-effect callback. Note that with {@link ParallelFlux} and
	 * the {@link #subscribe(Consumer) lambda-based subscribes} or the
	 * {@link #subscribe(CoreSubscriber[]) array-based one}, onError and onComplete will be
	 * invoked as many times as there are rails, resulting in as many corresponding
	 * {@link Signal} being seen in the callback.
	 * <p>
	 * Use of {@link #subscribe(Subscriber)}, which calls {@link #sequential()}, might
	 * cancel some rails, resulting in less signals being observed. This is an advanced
	 * operator, typically used for monitoring of a ParallelFlux.
	 *
	 * @param signalConsumer the mandatory callback to call on
	 *   {@link Subscriber#onNext(Object)}, {@link Subscriber#onError(Throwable)} and
	 *   {@link Subscriber#onComplete()}
	 * @return an observed {@link ParallelFlux}
	 * @see #doOnNext(Consumer)
	 * @see #doOnError(Consumer)
	 * @see #doOnComplete(Runnable)
	 * @see #subscribe(CoreSubscriber[])
	 * @see Signal
	 */
	public final ParallelFlux<T> doOnEach(Consumer<? super Signal<T>> signalConsumer) {
		Objects.requireNonNull(signalConsumer, "signalConsumer");
		return onAssembly(new ParallelDoOnEach<>(
				this,
				(ctx, v) -> signalConsumer.accept(Signal.next(v, ctx)),
				(ctx, e) -> signalConsumer.accept(Signal.error(e, ctx)),
				ctx -> signalConsumer.accept(Signal.complete(ctx))
		));
	}

	/**
	 * Call the specified consumer with the exception passing through any 'rail'.
	 *
	 * @param onError the callback
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> doOnError(Consumer<? super Throwable> onError) {
		Objects.requireNonNull(onError, "onError");
		return doOnSignal(this, null, null, onError, null, null, null, null, null);
	}

	/**
	 * Call the specified callback when a 'rail' receives a Subscription from its
	 * upstream.
	 * <p>
	 * This method is <strong>not</strong> intended for capturing the subscription and calling its methods,
	 * but for side effects like monitoring. For instance, the correct way to cancel a subscription is
	 * to call {@link Disposable#dispose()} on the Disposable returned by {@link ParallelFlux#subscribe()}.
	 *
	 * @param onSubscribe the callback
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		Objects.requireNonNull(onSubscribe, "onSubscribe");
		return doOnSignal(this, null, null, null, null, null, onSubscribe, null, null);
	}

	/**
	 * Call the specified consumer with the current element passing through any 'rail'.
	 *
	 * @param onNext the callback
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> doOnNext(Consumer<? super T> onNext) {
		Objects.requireNonNull(onNext, "onNext");
		return doOnSignal(this, onNext, null, null, null, null, null, null, null);
	}

	/**
	 * Call the specified consumer with the request amount if any rail receives a
	 * request.
	 *
	 * @param onRequest the callback
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> doOnRequest(LongConsumer onRequest) {
		Objects.requireNonNull(onRequest, "onRequest");
		return doOnSignal(this, null, null, null, null, null, null, onRequest, null);
	}

	/**
	 * Triggered when the {@link ParallelFlux} terminates, either by completing successfully or with an error.
	 * @param onTerminate the callback to call on {@link Subscriber#onComplete} or {@link Subscriber#onError}
	 *
	 * @return an observed  {@link ParallelFlux}
	 */
	public final ParallelFlux<T> doOnTerminate(Runnable onTerminate) {
		Objects.requireNonNull(onTerminate, "onTerminate");
		return doOnSignal(this,
				null,
				null,
				e -> onTerminate.run(),
				onTerminate,
				null,
				null,
				null,
				null);
	}

	/**
	 * Filters the source values on each 'rail'.
	 * <p>
	 * Note that the same predicate may be called from multiple threads concurrently.
	 *
	 * @param predicate the function returning true to keep a value or false to drop a
	 * value
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> filter(Predicate<? super T> predicate) {
		Objects.requireNonNull(predicate, "predicate");
		return onAssembly(new ParallelFilter<>(this, predicate));
	}

	/**
	 * Generates and flattens Publishers on each 'rail'.
	 * <p>
	 * Errors are not delayed and uses unbounded concurrency along with default inner
	 * prefetch.
	 *
	 * @param <R> the result type
	 * @param mapper the function to map each rail's value into a Publisher
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final <R> ParallelFlux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return flatMap(mapper,
				false,
				Integer.MAX_VALUE, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Generates and flattens Publishers on each 'rail', optionally delaying errors.
	 * <p>
	 * It uses unbounded concurrency along with default inner prefetch.
	 *
	 * @param <R> the result type
	 * @param mapper the function to map each rail's value into a Publisher
	 * @param delayError should the errors from the main and the inner sources delayed
	 * till everybody terminates?
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final <R> ParallelFlux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper,
			boolean delayError) {
		return flatMap(mapper,
				delayError,
				Integer.MAX_VALUE, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Generates and flattens Publishers on each 'rail', optionally delaying errors and
	 * having a total number of simultaneous subscriptions to the inner Publishers.
	 * <p>
	 * It uses a default inner prefetch.
	 *
	 * @param <R> the result type
	 * @param mapper the function to map each rail's value into a Publisher
	 * @param delayError should the errors from the main and the inner sources delayed
	 * till everybody terminates?
	 * @param maxConcurrency the maximum number of simultaneous subscriptions to the
	 * generated inner Publishers
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final <R> ParallelFlux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper,
			boolean delayError,
			int maxConcurrency) {
		return flatMap(mapper,
				delayError,
				maxConcurrency, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Generates and flattens Publishers on each 'rail', optionally delaying errors,
	 * having a total number of simultaneous subscriptions to the inner Publishers and
	 * using the given prefetch amount for the inner Publishers.
	 *
	 * @param <R> the result type
	 * @param mapper the function to map each rail's value into a Publisher
	 * @param delayError should the errors from the main and the inner sources delayed
	 * till everybody terminates?
	 * @param maxConcurrency the maximum number of simultaneous subscriptions to the
	 * generated inner Publishers
	 * @param prefetch the number of items to prefetch from each inner Publisher
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final <R> ParallelFlux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper,
			boolean delayError,
			int maxConcurrency,
			int prefetch) {
		return onAssembly(new ParallelFlatMap<>(this,
				mapper,
				delayError,
				maxConcurrency,
				Queues.get(maxConcurrency),
				prefetch, Queues.get(prefetch)));
	}

	/**
	 * Exposes the 'rails' as individual GroupedFlux instances, keyed by the rail
	 * index (zero based).
	 * <p>
	 * Each group can be consumed only once; requests and cancellation compose through.
	 * Note that cancelling only one rail may result in undefined behavior.
	 *
	 * @return the new Flux instance
	 */
	public final Flux<GroupedFlux<Integer, T>> groups() {
		return Flux.onAssembly(new ParallelGroup<>(this));
	}

	/**
	 * Hides the identities of this {@link ParallelFlux} and its {@link Subscription}
	 * as well.
	 *
	 * @return a new {@link ParallelFlux} defeating any {@link Publisher} / {@link Subscription} feature-detection
	 */
	public final ParallelFlux<T> hide() {
		return new ParallelFluxHide<>(this);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace
	 * implementation. Default will use {@link Level#INFO} and java.util.logging. If SLF4J
	 * is available, it will be used instead.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 * <p>
	 * The default log category will be "reactor.*", a generated operator suffix will
	 * complete, e.g. "reactor.Parallel.Map".
	 *
	 * @return a new unaltered {@link ParallelFlux}
	 */
	public final ParallelFlux<T> log() {
		return log(null, Level.INFO);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace
	 * implementation. Default will use {@link Level#INFO} and java.util.logging. If SLF4J
	 * is available, it will be used instead.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 * <p>
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator suffix
	 * will complete, e.g. "reactor.Parallel.Map".
	 *
	 * @return a new unaltered {@link ParallelFlux}
	 */
	public final ParallelFlux<T> log(@Nullable String category) {
		return log(category, Level.INFO);
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
	 *     ParallelFlux.log("category", Level.INFO, SignalType.ON_NEXT,
	 * SignalType.ON_ERROR)
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator
	 * suffix will complete, e.g. "reactor.Parallel.Map".
	 * @param level the {@link Level} to enforce for this tracing ParallelFlux (only
	 * FINEST, FINE, INFO, WARNING and SEVERE are taken into account)
	 * @param options a vararg {@link SignalType} option to filter log messages
	 *
	 * @return a new unaltered {@link ParallelFlux}
	 */
	public final ParallelFlux<T> log(@Nullable String category,
			Level level,
			SignalType... options) {
		return log(category, level, false, options);
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
	 *     ParallelFlux.log("category", Level.INFO, SignalType.ON_NEXT,
	 * SignalType.ON_ERROR)
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator
	 * suffix will complete, e.g. "reactor.ParallelFlux.Map".
	 * @param level the {@link Level} to enforce for this tracing ParallelFlux (only
	 * FINEST, FINE, INFO, WARNING and SEVERE are taken into account)
	 * @param showOperatorLine capture the current stack to display operator
	 * class/line number.
	 * @param options a vararg {@link SignalType} option to filter log messages
	 *
	 * @return a new unaltered {@link ParallelFlux}
	 */
	public final ParallelFlux<T> log(@Nullable String category,
			Level level,
			boolean showOperatorLine,
			SignalType... options) {
		return onAssembly(new ParallelLog<>(this, new SignalLogger<>(this, category, level, showOperatorLine, options)));
	}

	/**
	 * Maps the source values on each 'rail' to another value.
	 * <p>
	 * Note that the same mapper function may be called from multiple threads
	 * concurrently.
	 *
	 * @param <U> the output value type
	 * @param mapper the mapper function turning Ts into Us.
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final <U> ParallelFlux<U> map(Function<? super T, ? extends U> mapper) {
		Objects.requireNonNull(mapper, "mapper");
		return onAssembly(new ParallelMap<>(this, mapper));
	}

	/**
	 * Give a name to this sequence, which can be retrieved using {@link Scannable#name()}
	 * as long as this is the first reachable {@link Scannable#parents()}.
	 *
	 * @param name a name for the sequence
	 * @return the same sequence, but bearing a name
	 */
	public final ParallelFlux<T> name(String name) {
		return ParallelFluxName.createOrAppend(this, name);
	}

	/**
	 * Merges the values from each 'rail', but choose which one to merge by way of a
	 * provided {@link Comparator}, picking the smallest of all rails. The result is
	 * exposed back as a {@link Flux}.
	 * <p>
	 * This version uses a default prefetch of {@link Queues#SMALL_BUFFER_SIZE}.
	 *
	 * @param comparator the comparator to choose the smallest value available from all rails
	 * @return the new Flux instance
	 *
	 * @see ParallelFlux#ordered(Comparator, int)
	 */
	public final Flux<T> ordered(Comparator<? super T> comparator) {
		return ordered(comparator, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Merges the values from each 'rail', but choose which one to merge by way of a
	 * provided {@link Comparator}, picking the smallest of all rails. The result is
	 * exposed back as a {@link Flux}.
	 *
	 * @param comparator the comparator to choose the smallest value available from all rails
	 * @param prefetch the prefetch to use
	 * @return the new Flux instance
	 *
	 * @see ParallelFlux#ordered(Comparator)
	 */
	public final Flux<T> ordered(Comparator<? super T> comparator, int prefetch) {
		return new ParallelMergeOrdered<>(this, prefetch, comparator);
	}

	/**
	 * Returns the number of expected parallel Subscribers.
	 *
	 * @return the number of expected parallel Subscribers
	 */
	public abstract int parallelism();

	/**
	 * Reduces all values within a 'rail' and across 'rails' with a reducer function into
	 * a single sequential value.
	 * <p>
	 * Note that the same reducer function may be called from multiple threads
	 * concurrently.
	 *
	 * @param reducer the function to reduce two values into one.
	 *
	 * @return the new Mono instance emitting the reduced value or empty if the
	 * {@link ParallelFlux} was empty
	 */
	public final Mono<T> reduce(BiFunction<T, T, T> reducer) {
		Objects.requireNonNull(reducer, "reducer");
		return Mono.onAssembly(new ParallelMergeReduce<>(this, reducer));
	}

	/**
	 * Reduces all values within a 'rail' to a single value (with a possibly different
	 * type) via a reducer function that is initialized on each rail from an
	 * initialSupplier value.
	 * <p>
	 * Note that the same mapper function may be called from multiple threads
	 * concurrently.
	 *
	 * @param <R> the reduced output type
	 * @param initialSupplier the supplier for the initial value
	 * @param reducer the function to reduce a previous output of reduce (or the initial
	 * value supplied) with a current source value.
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final <R> ParallelFlux<R> reduce(Supplier<R> initialSupplier,
			BiFunction<R, ? super T, R> reducer) {
		Objects.requireNonNull(initialSupplier, "initialSupplier");
		Objects.requireNonNull(reducer, "reducer");
		return onAssembly(new ParallelReduceSeed<>(this, initialSupplier, reducer));
	}

	/**
	 * Specifies where each 'rail' will observe its incoming values with possible
	 * work-stealing and default prefetch amount.
	 * <p>
	 * This operator uses the default prefetch size returned by {@code
	 * Queues.SMALL_BUFFER_SIZE}.
	 * <p>
	 * The operator will call {@code Scheduler.createWorker()} as many times as this
	 * ParallelFlux's parallelism level is.
	 * <p>
	 * No assumptions are made about the Scheduler's parallelism level, if the Scheduler's
	 * parallelism level is lower than the ParallelFlux's, some rails may end up on
	 * the same thread/worker.
	 * <p>
	 * This operator doesn't require the Scheduler to be trampolining as it does its own
	 * built-in trampolining logic.
	 *
	 * @param scheduler the scheduler to use
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> runOn(Scheduler scheduler) {
		return runOn(scheduler, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Specifies where each 'rail' will observe its incoming values with possible
	 * work-stealing and a given prefetch amount.
	 * <p>
	 * This operator uses the default prefetch size returned by {@code
	 * Queues.SMALL_BUFFER_SIZE}.
	 * <p>
	 * The operator will call {@code Scheduler.createWorker()} as many times as this
	 * ParallelFlux's parallelism level is.
	 * <p>
	 * No assumptions are made about the Scheduler's parallelism level, if the Scheduler's
	 * parallelism level is lower than the ParallelFlux's, some rails may end up on
	 * the same thread/worker.
	 * <p>
	 * This operator doesn't require the Scheduler to be trampolining as it does its own
	 * built-in trampolining logic.
	 *
	 * @param scheduler the scheduler to use that rail's worker has run out of work.
	 * @param prefetch the number of values to request on each 'rail' from the source
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	public final ParallelFlux<T> runOn(Scheduler scheduler, int prefetch) {
		Objects.requireNonNull(scheduler, "scheduler");
		return onAssembly(new ParallelRunOn<>(this,
				scheduler,
				prefetch,
				Queues.get(prefetch)));
	}

	/**
	 * Merges the values from each 'rail' in a round-robin or same-order fashion and
	 * exposes it as a regular Publisher sequence, running with a default prefetch value
	 * for the rails.
	 * <p>
	 * This operator uses the default prefetch size returned by {@code
	 * Queues.SMALL_BUFFER_SIZE}.
	 *
	 * @return the new Flux instance
	 *
	 * @see ParallelFlux#sequential(int)
	 */
	public final Flux<T> sequential() {
		return sequential(Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Merges the values from each 'rail' in a round-robin or same-order fashion and
	 * exposes it as a regular Publisher sequence, running with a give prefetch value for
	 * the rails.
	 *
	 * @param prefetch the prefetch amount to use for each rail
	 *
	 * @return the new Flux instance
	 */
	public final Flux<T> sequential(int prefetch) {
		return Flux.onAssembly(new ParallelMergeSequential<>(this,
				prefetch,
				Queues.get(prefetch)));
	}

	/**
	 * Sorts the 'rails' of this {@link ParallelFlux} and returns a Publisher that
	 * sequentially picks the smallest next value from the rails.
	 * <p>
	 * This operator requires a finite source ParallelFlux.
	 *
	 * @param comparator the comparator to use
	 *
	 * @return the new Flux instance
	 */
	public final Flux<T> sorted(Comparator<? super T> comparator) {
		return sorted(comparator, 16);
	}

	/**
	 * Sorts the 'rails' of this {@link ParallelFlux} and returns a Publisher that
	 * sequentially picks the smallest next value from the rails.
	 * <p>
	 * This operator requires a finite source ParallelFlux.
	 *
	 * @param comparator the comparator to use
	 * @param capacityHint the expected number of total elements
	 *
	 * @return the new Flux instance
	 */
	public final Flux<T> sorted(Comparator<? super T> comparator, int capacityHint) {
		int ch = capacityHint / parallelism() + 1;
		ParallelFlux<List<T>> railReduced = reduce(() -> new ArrayList<>(ch), (a, b) -> {
					a.add(b);
					return a;
				});
		ParallelFlux<List<T>> railSorted = railReduced.map(list -> {
			list.sort(comparator);
			return list;
		});

		return Flux.onAssembly(new ParallelMergeSort<>(railSorted, comparator));
	}

	/**
	 * Subscribes an array of Subscribers to this {@link ParallelFlux} and triggers the
	 * execution chain for all 'rails'.
	 *
	 * @param subscribers the subscribers array to run in parallel, the number of items
	 * must be equal to the parallelism level of this ParallelFlux
	 */
	public abstract void subscribe(CoreSubscriber<? super T>[] subscribers);

	/**
	 * Subscribes to this {@link ParallelFlux} and triggers the execution chain for all
	 * 'rails'.
	 */
	public final Disposable subscribe(){
		return subscribe(null, null, null);
	}

	/**
	 * Subscribes to this {@link ParallelFlux} by providing an onNext callback and
	 * triggers the execution chain for all 'rails'.
	 *
	 * @param onNext consumer of onNext signals
	 */
	public final Disposable subscribe(Consumer<? super T> onNext){
		return subscribe(onNext, null, null);
	}

	/**
	 * Subscribes to this {@link ParallelFlux} by providing an onNext and onError callback
	 * and triggers the execution chain for all 'rails'.
	 *
	 * @param onNext consumer of onNext signals
	 * @param onError consumer of error signal
	 */
	public final Disposable subscribe(@Nullable Consumer<? super T> onNext, Consumer<? super Throwable>
			onError){
		return subscribe(onNext, onError, null);
	}

	/**
	 * Subscribes to this {@link ParallelFlux} by providing an onNext, onError and
	 * onComplete callback and triggers the execution chain for all 'rails'.
	 *
	 * @param onNext consumer of onNext signals
	 * @param onError consumer of error signal
	 * @param onComplete callback on completion signal
	 */
	public final Disposable subscribe(
			@Nullable Consumer<? super T> onNext,
			@Nullable Consumer<? super Throwable> onError,
			@Nullable Runnable onComplete) {
		return this.subscribe(onNext, onError, onComplete, null, (Context) null);
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void subscribe(CoreSubscriber<? super T> s) {
		FluxHide.SuppressFuseableSubscriber<T> subscriber =
				new FluxHide.SuppressFuseableSubscriber<>(Operators.toCoreSubscriber(s));

		sequential().subscribe(Operators.toCoreSubscriber(subscriber));
	}

	/**
	 * Subscribes to this {@link ParallelFlux} by providing an onNext, onError,
	 * onComplete and onSubscribe callback and triggers the execution chain for all
	 * 'rails'.
	 *
	 * @param onNext consumer of onNext signals
	 * @param onError consumer of error signal
	 * @param onComplete callback on completion signal
	 * @param onSubscribe consumer of the subscription signal
	 */ //TODO maybe deprecate in 3.4, provided there is at least an alternative for tests
	public final Disposable subscribe(
			@Nullable Consumer<? super T> onNext,
			@Nullable Consumer<? super Throwable> onError,
			@Nullable Runnable onComplete,
			@Nullable Consumer<? super Subscription> onSubscribe) {
		return this.subscribe(onNext, onError, onComplete, onSubscribe, null);
	}

	/**
	 * Subscribes to this {@link ParallelFlux} by providing an onNext, onError and
	 * onComplete callback as well as an initial {@link Context}, then trigger the execution chain for all
	 * 'rails'.
	 *
	 * @param onNext consumer of onNext signals
	 * @param onError consumer of error signal
	 * @param onComplete callback on completion signal
	 * @param initialContext {@link Context} for the rails
	 */
	public final Disposable subscribe(
			@Nullable Consumer<? super T> onNext,
			@Nullable Consumer<? super Throwable> onError,
			@Nullable Runnable onComplete,
			@Nullable Context initialContext) {
		return this.subscribe(onNext, onError, onComplete, null, initialContext);
	}

	final Disposable subscribe(
			@Nullable Consumer<? super T> onNext,
			@Nullable Consumer<? super Throwable> onError,
			@Nullable Runnable onComplete,
			@Nullable Consumer<? super Subscription> onSubscribe,
			@Nullable Context initialContext) {
		CorePublisher<T> publisher = Operators.onLastAssembly(this);
		if (publisher instanceof ParallelFlux) {
			@SuppressWarnings("unchecked")
			LambdaSubscriber<? super T>[] subscribers = new LambdaSubscriber[parallelism()];

			int i = 0;
			while(i < subscribers.length){
				subscribers[i++] =
						new LambdaSubscriber<>(onNext, onError, onComplete, onSubscribe, initialContext);
			}

			((ParallelFlux<T>) publisher).subscribe(subscribers);

			return Disposables.composite(subscribers);
		}
		else {
			LambdaSubscriber<? super T> subscriber =
					new LambdaSubscriber<>(onNext, onError, onComplete, onSubscribe, initialContext);

			publisher.subscribe(Operators.toCoreSubscriber(new FluxHide.SuppressFuseableSubscriber<>(subscriber)));

			return subscriber;
		}
	}

	/**
	 * Merge the rails into a {@link #sequential()} Flux and
	 * {@link Flux#subscribe(Subscriber) subscribe} to said Flux.
	 *
	 * @param s the subscriber to use on {@link #sequential()} Flux
	 */
	@Override
	@SuppressWarnings("unchecked")
	public final void subscribe(Subscriber<? super T> s) {
		FluxHide.SuppressFuseableSubscriber<T> subscriber =
				new FluxHide.SuppressFuseableSubscriber<>(Operators.toCoreSubscriber(s));

		Operators.onLastAssembly(sequential()).subscribe(Operators.toCoreSubscriber(subscriber));
	}

	/**
	 * Tag this ParallelFlux with a key/value pair. These can be retrieved as a
	 * {@link Set} of
	 * all tags throughout the publisher chain by using {@link Scannable#tags()} (as
	 * traversed
	 * by {@link Scannable#parents()}).
	 *
	 * @param key a tag key
	 * @param value a tag value
	 * @return the same sequence, but bearing tags
	 */
	public final ParallelFlux<T> tag(String key, String value) {
		return ParallelFluxName.createOrAppend(this, key, value);
	}



	/**
	 * Emit an onComplete or onError signal once all values across 'rails' have been observed.
	 *
	 * @return the new Mono instance emitting the reduced value or empty if the
	 * {@link ParallelFlux} was empty
	 */
	public final Mono<Void> then() {
		return Mono.onAssembly(new ParallelThen(this));
	}

	/**
	 * Allows composing operators, in assembly time, on top of this {@link ParallelFlux}
	 * and returns another {@link ParallelFlux} with composed features.
	 *
	 * @param <U> the output value type
	 * @param composer the composer function from {@link ParallelFlux} (this) to another
	 * ParallelFlux
	 *
	 * @return the {@link ParallelFlux} returned by the function
	 */
	public final <U> ParallelFlux<U> transform(Function<? super ParallelFlux<T>, ParallelFlux<U>> composer) {
		return onAssembly(as(composer));
	}

	/**
	 * Allows composing operators off the groups (or 'rails'), as individual {@link GroupedFlux}
	 * instances keyed by the zero based rail's index. The transformed groups are
	 * {@link Flux#parallel parallelized} back once the transformation has been applied.
	 * Since groups are generated anew per each subscription, this is all done in a "lazy"
	 * fashion where each subscription trigger distinct applications of the {@link Function}.
	 * <p>
	 * Note that like in {@link #groups()}, requests and cancellation compose through, and
	 * cancelling only one rail may result in undefined behavior.
	 *
	 * @param composer the composition function to apply on each {@link GroupedFlux rail}
	 * @param <U> the type of the resulting parallelized flux
	 * @return a {@link ParallelFlux} of the composed groups
	 */
	public final <U> ParallelFlux<U> transformGroups(Function<? super GroupedFlux<Integer, T>,
			? extends Publisher<? extends U>> composer) {
		if (getPrefetch() > -1) {
			return from(groups().flatMap(composer::apply),
					parallelism(), getPrefetch(),
					Queues.small());
		}
		else {
			return from(groups().flatMap(composer::apply), parallelism());
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}

	/**
	 * Validates the number of subscribers and returns true if their number matches the
	 * parallelism level of this ParallelFlux.
	 *
	 * @param subscribers the array of Subscribers
	 *
	 * @return true if the number of subscribers equals to the parallelism level
	 */
	protected final boolean validate(Subscriber<?>[] subscribers) {
		int p = parallelism();
		if (subscribers.length != p) {
			IllegalArgumentException iae = new IllegalArgumentException("parallelism = " +
					"" + p + ", subscribers = " + subscribers.length);
			for (Subscriber<?> s : subscribers) {
				Operators.error(s, iae);
			}
			return false;
		}
		return true;
	}

	/**
	 * Generates and concatenates Publishers on each 'rail', optionally delaying errors
	 * and using the given prefetch amount for generating Publishers upfront.
	 *
	 * @param <R> the result type
	 * @param mapper the function to map each rail's value into a Publisher
	 * @param prefetch the number of items to prefetch from each inner Publisher
	 * @param errorMode the error handling, i.e., when to report errors from the main
	 * source and the inner Publishers (immediate, boundary, end)
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	final <R> ParallelFlux<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper,
			int prefetch,
			ErrorMode errorMode) {
		return onAssembly(new ParallelConcatMap<>(this,
				mapper,
				Queues.get(prefetch),
				prefetch,
				errorMode));
	}

	/**
	 * Generates and concatenates Publishers on each 'rail', delaying errors
	 * and using the given prefetch amount for generating Publishers upfront.
	 *
	 * @param <R> the result type
	 * @param mapper the function to map each rail's value into a Publisher
	 * @param delayUntilEnd true if delayed until all sources are concatenated
	 * @param prefetch the number of items to prefetch from each inner Publisher
	 * source and the inner Publishers (immediate, boundary, end)
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	final <R> ParallelFlux<R> concatMapDelayError(Function<? super T, ? extends
			Publisher<? extends R>> mapper,
			boolean delayUntilEnd,
			int prefetch) {
		return concatMap(mapper, prefetch, delayUntilEnd ? ErrorMode.END: ErrorMode.BOUNDARY);
	}

	/**
	 * Generates and concatenates Publishers on each 'rail', delaying errors
	 * and using the given prefetch amount for generating Publishers upfront.
	 *
	 * @param <R> the result type
	 * @param mapper the function to map each rail's value into a Publisher
	 * @param prefetch the number of items to prefetch from each inner Publisher
	 * source and the inner Publishers (immediate, boundary, end)
	 *
	 * @return the new {@link ParallelFlux} instance
	 */
	final <R> ParallelFlux<R> concatMapDelayError(Function<? super T, ? extends
			Publisher<? extends R>> mapper, int prefetch) {
		return concatMap(mapper, prefetch, ErrorMode.END);
	}

	/**
	 * The prefetch configuration of the component
	 *
	 * @return the prefetch configuration of the component
	 */
	public int getPrefetch() {
		return -1;
	}


	/**
	 * Invoke {@link Hooks} pointcut given a {@link ParallelFlux} and returning an
	 * eventually new {@link ParallelFlux}
	 *
	 * @param <T> the value type
	 * @param source the source to wrap
	 *
	 * @return the potentially wrapped source
	 */
	@SuppressWarnings("unchecked")
	protected static <T> ParallelFlux<T> onAssembly(ParallelFlux<T> source) {
		Function<Publisher, Publisher> hook = Hooks.onEachOperatorHook;
		if(hook != null) {
			source = (ParallelFlux<T>) hook.apply(source);
		}
		if (Hooks.GLOBAL_TRACE) {
			AssemblySnapshot stacktrace = new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get());
			source = (ParallelFlux<T>) Hooks.addAssemblyInfo(source, stacktrace);
		}
		return source;
	}

	@SuppressWarnings("unchecked")
	static <T> ParallelFlux<T> doOnSignal(ParallelFlux<T> source,
			@Nullable Consumer<? super T> onNext,
			@Nullable Consumer<? super T> onAfterNext,
			@Nullable Consumer<? super Throwable> onError,
			@Nullable Runnable onComplete,
			@Nullable Runnable onAfterTerminate,
			@Nullable Consumer<? super Subscription> onSubscribe,
			@Nullable LongConsumer onRequest,
			@Nullable Runnable onCancel) {
		return onAssembly(new ParallelPeek<>(source,
				onNext,
				onAfterNext,
				onError,
				onComplete,
				onAfterTerminate,
				onSubscribe,
				onRequest,
				onCancel));
	}

	static final <T> List<T> sortedMerger(List<T> a, List<T> b, Comparator<? super T> comparator) {
		int n = a.size() + b.size();
		if (n == 0) {
			return new ArrayList<>();
		}
		List<T> both = new ArrayList<>(n);

		Iterator<T> at = a.iterator();
		Iterator<T> bt = b.iterator();

		T s1 = at.hasNext() ? at.next() : null;
		T s2 = bt.hasNext() ? bt.next() : null;

		while (s1 != null && s2 != null) {
			if (comparator.compare(s1, s2) < 0) { // s1 comes before s2
				both.add(s1);
				s1 = at.hasNext() ? at.next() : null;
			}
			else {
				both.add(s2);
				s2 = bt.hasNext() ? bt.next() : null;
			}
		}

		if (s1 != null) {
			both.add(s1);
			while (at.hasNext()) {
				both.add(at.next());
			}
		}
		else if (s2 != null) {
			both.add(s2);
			while (bt.hasNext()) {
				both.add(bt.next());
			}
		}

		return both;
	}

}
