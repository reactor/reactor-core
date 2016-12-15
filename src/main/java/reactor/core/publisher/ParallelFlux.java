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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
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
import reactor.core.publisher.FluxConcatMap.ErrorMode;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.concurrent.QueueSupplier;

/**
 * Abstract base class for Parallel publishers that take an array of Subscribers.
 * <p>
 * Use {@code from()} to start processing a regular Publisher in 'rails'. Use {@code
 * runOn()} to introduce where each 'rail' should run on thread-vise.
 * Use {@code sequential()} to merge the sources back into a single {@link Flux} or if
 * you simply want to subscribe to the merged sequence use {@link #subscribe(Subscriber)}.
 *
 * @param <T> the value type
 */
public abstract class ParallelFlux<T> implements Publisher<T> {

	/**
	 * Take a Publisher and prepare to consume it on multiple 'rails' (number of CPUs) in
	 * a round-robin fashion.
	 *
	 * @param <T> the value type
	 * @param source the source Publisher
	 *
	 * @return the {@link ParallelFlux} instance
	 */
	public static <T> ParallelFlux<T> from(Publisher<? extends T> source) {
		return from(source,
				Runtime.getRuntime()
				       .availableProcessors(), QueueSupplier.SMALL_BUFFER_SIZE,
				QueueSupplier.small());
	}

	/**
	 * Take a Publisher and prepare to consume it on parallelism number of 'rails' ,
	 * possibly ordered and round-robin fashion.
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
				parallelism, QueueSupplier.SMALL_BUFFER_SIZE,
				QueueSupplier.small());
	}

	/**
	 * Take a Publisher and prepare to consume it on parallelism number of 'rails'
	 * and round-robin fashion and use custom prefetch amount and queue
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
		if (parallelism <= 0) {
			throw new IllegalArgumentException("parallelism > 0 required but it was " + parallelism);
		}
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}

		Objects.requireNonNull(queueSupplier, "queueSupplier");
		Objects.requireNonNull(source, "queueSupplier");

		return onAssembly(new ParallelUnorderedSource<>(source,
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
		if (publishers.length == 0) {
			throw new IllegalArgumentException("Zero publishers not supported");
		}
		return onAssembly(new ParallelUnorderedFrom<>(publishers));
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
	public final <C> ParallelFlux<C> collect(Supplier<C> collectionSupplier,
			BiConsumer<C, T> collector) {
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

		Mono<List<T>> merged = railSorted.reduce((a, b) -> {
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
		});

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
		return onAssembly(new ParallelUnorderedFilter<>(this, predicate));
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
				Integer.MAX_VALUE, QueueSupplier.SMALL_BUFFER_SIZE);
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
				Integer.MAX_VALUE, QueueSupplier.SMALL_BUFFER_SIZE);
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
				maxConcurrency, QueueSupplier.SMALL_BUFFER_SIZE);
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
				QueueSupplier.get(maxConcurrency),
				prefetch, QueueSupplier.get(prefetch)));
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
	 * Returns true if the parallel sequence has to be ordered when joining back.
	 *
	 * @return true if the parallel sequence has to be ordered when joining back
	 */
	public abstract boolean isOrdered();

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace
	 * implementation. Default will use {@link Level#INFO} and java.util.logging. If SLF4J
	 * is available, it will be used instead.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png"
	 * alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png"
	 * alt="">
	 * <p>
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator suffix
	 * will complete, e.g. "reactor.Parallel.Map".
	 *
	 * @return a new unaltered {@link ParallelFlux}
	 */
	public final ParallelFlux<T> log(String category) {
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png"
	 * alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator
	 * suffix will complete, e.g. "reactor.Parallel.Map".
	 * @param level the level to enforce for this tracing ParallelFlux
	 * @param options a vararg {@link SignalType} option to filter log messages
	 *
	 * @return a new unaltered {@link ParallelFlux}
	 */
	public final ParallelFlux<T> log(String category,
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png"
	 * alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework
	 * .reactor). If category ends with "." like "reactor.", a generated operator
	 * suffix will complete, e.g. "reactor.ParallelFlux.Map".
	 * @param level the level to enforce for this tracing ParallelFlux
	 * @param showOperatorLine capture the current stack to display operator
	 * class/line number.
	 * @param options a vararg {@link SignalType} option to filter log messages
	 *
	 * @return a new unaltered {@link ParallelFlux}
	 */
	public final ParallelFlux<T> log(String category,
			Level level,
			boolean showOperatorLine,
			SignalType... options) {

		SignalLogger<T> log =
				new SignalLogger<>(this, category, level, showOperatorLine, options);

		return doOnSignal(this,
				log.onNextCall(),
				log.onAfterNextCall(),
				log.onErrorCall(),
				log.onCompleteCall(),
				log.onAfterTerminateCall(),
				log.onSubscribeCall(),
				log.onRequestCall(),
				log.onCancelCall());
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
		return onAssembly(new ParallelUnorderedMap<>(this, mapper));
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
		return Mono.onAssembly(new ParallelReduceFull<>(this, reducer));
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
			BiFunction<R, T, R> reducer) {
		Objects.requireNonNull(initialSupplier, "initialSupplier");
		Objects.requireNonNull(reducer, "reducer");
		return onAssembly(new ParallelReduce<>(this, initialSupplier, reducer));
	}

	/**
	 * Specifies where each 'rail' will observe its incoming values with no work-stealing
	 * and default prefetch amount.
	 * <p>
	 * This operator uses the default prefetch size returned by {@code
	 * QueueSupplier.SMALL_BUFFER_SIZE}.
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
		return runOn(scheduler, QueueSupplier.SMALL_BUFFER_SIZE);
	}

	/**
	 * Specifies where each 'rail' will observe its incoming values with possibly
	 * work-stealing and a given prefetch amount.
	 * <p>
	 * This operator uses the default prefetch size returned by {@code
	 * QueueSupplier.SMALL_BUFFER_SIZE}.
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
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		Objects.requireNonNull(scheduler, "scheduler");
		return onAssembly(new ParallelUnorderedRunOn<>(this,
				scheduler,
				prefetch,
				QueueSupplier.get(prefetch)));
	}

	/**
	 * Merges the values from each 'rail' in a round-robin or same-order fashion and
	 * exposes it as a regular Publisher sequence, running with a default prefetch value
	 * for the rails.
	 * <p>
	 * This operator uses the default prefetch size returned by {@code
	 * QueueSupplier.SMALL_BUFFER_SIZE}.
	 *
	 * @return the new Flux instance
	 *
	 * @see ParallelFlux#sequential(int)
	 */
	public final Flux<T> sequential() {
		return sequential(QueueSupplier.SMALL_BUFFER_SIZE);
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
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}

		return Flux.onAssembly(new ParallelUnorderedJoin<>(this,
				prefetch,
				QueueSupplier.get(prefetch)));
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

		return Flux.onAssembly(new ParallelSortedJoin<>(railSorted, comparator));
	}

	/**
	 * Subscribes an array of Subscribers to this {@link ParallelFlux} and triggers the
	 * execution chain for all 'rails'.
	 *
	 * @param subscribers the subscribers array to run in parallel, the number of items
	 * must be equal to the parallelism level of this ParallelFlux
	 */
	public abstract void subscribe(Subscriber<? super T>[] subscribers);

	/**
	 * Subscribes to this {@link ParallelFlux} by providing an onNext callback and
	 * triggers the execution chain for all 'rails'.
	 *
	 * @param onNext consumer of onNext signals
	 */
	public void subscribe(Consumer<? super T> onNext){
		subscribe(onNext, null, null);
	}

	/**
	 * Subscribes to this {@link ParallelFlux} by providing an onNext and onError callback
	 * and triggers the execution chain for all 'rails'.
	 *
	 * @param onNext consumer of onNext signals
	 * @param onError consumer of error signal
	 */
	public void subscribe(Consumer<? super T> onNext, Consumer<? super Throwable>
			onError){
		subscribe(onNext, onError, null);
	}

	/**
	 * Subscribes to this {@link ParallelFlux} by providing an onNext, onError and
	 * onComplete callback and triggers the execution chain for all 'rails'.
	 *
	 * @param onNext consumer of onNext signals
	 * @param onError consumer of error signal
	 * @param onComplete callback on completion signal
	 */
	public void subscribe(Consumer<? super T> onNext, Consumer<? super Throwable>
			onError, Runnable onComplete) {
		subscribe(onNext, onError, onComplete, null);
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
	 */
	public void subscribe(Consumer<? super T> onNext, Consumer<? super Throwable>
			onError, Runnable onComplete, Consumer<? super Subscription> onSubscribe){

		@SuppressWarnings("unchecked")
		Subscriber<T>[] subscribers = new Subscriber[parallelism()];

		int i = 0;
		while(i < subscribers.length){
			subscribers[i++] = new LambdaSubscriber<>(onNext, onError, onComplete, onSubscribe);
		}

		subscribe(subscribers);
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
		return as(composer);
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
			for (Subscriber<?> s : subscribers) {
				Operators.error(s,
						new IllegalArgumentException("parallelism = " + p + ", subscribers = " + subscribers.length));
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
		return onAssembly(new ParallelUnorderedConcatMap<>(this,
				mapper,
				QueueSupplier.get(prefetch),
				prefetch,
				errorMode));
	}

	/**
	 * Generates and concatenates Publishers on each 'rail', delaying errors
	 * and using the given prefetch amount for generating Publishers upfront.
	 *
	 * @param <R> the result type
	 * @param mapper the function to map each rail's value into a Publisher
	 * @param delayUntilEnd true if delayed until all sources are concated
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
	public long getPrefetch() {
		return -1L;
	}

	@Override
	public final void subscribe(Subscriber<? super T> s) {
		sequential().subscribe(s);
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
		Hooks.OnOperatorCreate hook = Hooks.onOperatorCreate;
		if (hook == null) {
			return source;
		}
		return (ParallelFlux<T>) hook.apply(source);
	}

	@SuppressWarnings("unchecked")
	static <T> ParallelFlux<T> doOnSignal(ParallelFlux<T> source,
			Consumer<? super T> onNext,
			Consumer<? super T> onAfterNext,
			Consumer<? super Throwable> onError,
			Runnable onComplete,
			Runnable onAfterTerminate,
			Consumer<? super Subscription> onSubscribe,
			LongConsumer onRequest,
			Runnable onCancel) {
		return onAssembly(new ParallelUnorderedPeek<>(source,
				onNext,
				onAfterNext,
				onError,
				onComplete,
				onAfterTerminate,
				onSubscribe,
				onRequest,
				onCancel));
	}
}
