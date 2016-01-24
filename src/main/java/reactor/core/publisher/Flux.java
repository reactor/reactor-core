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

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.queue.QueueSupplier;
import reactor.core.subscriber.BlockingIterable;
import reactor.core.subscriber.ReactiveSession;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.subscriber.Subscribers;
import reactor.core.timer.Timer;
import reactor.core.timer.Timers;
import reactor.core.trait.Backpressurable;
import reactor.core.trait.Connectable;
import reactor.core.trait.Introspectable;
import reactor.core.trait.Publishable;
import reactor.core.util.Assert;
import reactor.core.util.Logger;
import reactor.core.util.PlatformDependent;
import reactor.core.util.ReactiveStateUtils;
import reactor.fn.BiConsumer;
import reactor.fn.BiFunction;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.fn.tuple.Tuple3;
import reactor.fn.tuple.Tuple4;
import reactor.fn.tuple.Tuple5;
import reactor.fn.tuple.Tuple6;
import reactor.fn.tuple.Tuple7;
import reactor.fn.tuple.Tuple8;

/**
 * A Reactive Streams {@link Publisher} with basic rx operators that emits 0 to N elements, and then completes
 * (successfully or with an error).
 *
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flux.png" alt="">
 * <p>
 *
 * <p>It is intended to be used in implementations and return types. Input parameters should keep using raw
 * {@link Publisher} as much as possible.
 *
 * <p>If it is known that the underlying {@link Publisher} will emit 0 or 1 element, {@link Mono} should be used
 * instead.
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @see Mono
 * @since 2.5
 */
public abstract class Flux<T> implements Publisher<T>, Introspectable {

//	 ==============================================================================================================
//	 Static Generators
//	 ==============================================================================================================

	static final IdentityFunction IDENTITY_FUNCTION = new IdentityFunction();
	static final Flux<?>          EMPTY             = Mono.empty()
	                                                      .flux();

	/**
	 * Select the fastest source who won the "ambiguous" race and emitted first onNext or onComplete or onError
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/amb.png" alt="">
	 * <p>
	 *
	 * @param sources The competing source publishers
	 * @param <I> The source type of the data sequence
	 *
	 * @return a new Flux eventually subscribed to one of the sources or empty
	 */
	@SuppressWarnings({"unchecked", "varargs"})
	@SafeVarargs
	public static <I> Flux<I> amb(Publisher<? extends I>... sources) {
		return new FluxAmb<>(sources);
	}

	/**
	 * Select the fastest source who won the "ambiguous" race and emitted first onNext or onComplete or onError
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/amb.png" alt="">
	 * <p>
	 *
	 * @param sources The competing source publishers
	 * @param <I> The source type of the data sequence
	 *
	 * @return a new Flux eventually subscribed to one of the sources or empty
	 */
	@SuppressWarnings("unchecked")
	public static <I> Flux<I> amb(Iterable<? extends Publisher<? extends I>> sources) {
		if (sources == null) {
			return empty();
		}

		return new FluxAmb<>(sources);
	}

	/**
	 * Concat all sources emitted as an onNext signal from a parent {@link Publisher}.
	 * A complete signal from each source will delimit the individual sequences and will be eventually
	 * passed to the returned {@link Publisher} which will stop listening if the main sequence has also completed.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concatinner.png" alt="">
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concat
	 * @param <I> The source type of the data sequence
	 *
	 * @return a new Flux concatenating all inner sources sequences until complete or error
	 */
	public static <I> Flux<I> concat(Publisher<? extends Publisher<? extends I>> sources) {
		return new FluxFlatMap<>(sources, 1, 32);
	}

	/**
	 * Concat all sources pulled from the supplied
	 * {@link Iterator} on {@link Publisher#subscribe} from the passed {@link Iterable} until {@link Iterator#hasNext}
	 * returns false. A complete signal from each source will delimit the individual sequences and will be eventually
	 * passed to the returned Publisher.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concat.png" alt="">
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concat
	 * @param <I> The source type of the data sequence
	 *
	 * @return a new Flux concatenating all source sequences
	 */
	public static <I> Flux<I> concat(Iterable<? extends Publisher<? extends I>> sources) {
		return concat(fromIterable(sources));
	}

	/**
	 * Concat all sources pulled from the given {@link Publisher[]}.
	 * A complete signal from each source will delimit the individual sequences and will be eventually
	 * passed to the returned Publisher.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concat.png" alt="">
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concat
	 * @param <I> The source type of the data sequence
	 *
	 * @return a new Flux concatenating all source sequences
	 */
	@SafeVarargs
	@SuppressWarnings({"unchecked", "varargs"})
	public static <I> Flux<I> concat(Publisher<? extends I>... sources) {
		if (sources == null || sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from(sources[0]);
		}
		return concat(fromArray(sources));
	}

	/**
	 * Create a {@link Flux} reacting on each available {@link Subscriber} read derived with the passed {@link
	 * Consumer}. If a previous request is still running, avoid recursion and extend the previous request iterations.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/generateforeach.png" alt="">
	 *
	 * @param requestConsumer A {@link Consumer} invoked when available read with the target subscriber
	 * @param <T> The type of the data sequence
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> create(Consumer<SubscriberWithContext<T, Void>> requestConsumer) {
		return create(requestConsumer, null, null);
	}

	/**
	 * Create a {@link Flux} reacting on each available {@link Subscriber} read derived with the passed {@link
	 * Consumer}. If a previous request is still running, avoid recursion and extend the previous request iterations.
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every
	 * request calls.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/generateforeach.png" alt="">
	 *
	 * @param requestConsumer A {@link Consumer} invoked when available read with the target subscriber
	 * @param contextFactory A {@link Function} called for every new subscriber returning an immutable context (IO
	 * connection...)
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 *
	 * @return a new {@link Flux}
	 */
	public static <T, C> Flux<T> create(Consumer<SubscriberWithContext<T, C>> requestConsumer,
			Function<Subscriber<? super T>, C> contextFactory) {
		return create(requestConsumer, contextFactory, null);
	}

	/**
	 * Create a {@link Flux} reacting on each available {@link Subscriber} read derived with the passed {@link
	 * Consumer}. If a previous request is still running, avoid recursion and extend the previous request iterations.
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every
	 * request calls. The argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel,
	 * onComplete, onError).
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/generateforeach.png" alt="">
	 *
	 * @param requestConsumer A {@link Consumer} invoked when available read with the target subscriber
	 * @param contextFactory A {@link Function} called once for every new subscriber returning an immutable context (IO
	 * connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(),
	 * onError()
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 *
	 * @return a new {@link Flux}
	 */
	public static <T, C> Flux<T> create(final Consumer<SubscriberWithContext<T, C>> requestConsumer,
			Function<Subscriber<? super T>, C> contextFactory,
			Consumer<C> shutdownConsumer) {
		Assert.notNull(requestConsumer, "A data producer must be provided");
		return new FluxGenerate.FluxForEach<>(requestConsumer, contextFactory, shutdownConsumer);
	}

	/**
	 * Create a {@link Flux} that completes without emitting any item.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/empty.png" alt="">
	 *
	 * @param <T> the reified type of the target {@link Subscriber}
	 *
	 * @return an empty {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> empty() {
		return (Flux<T>) EMPTY;
	}

	/**
	 * Create a {@link Flux} that completes with the specified error.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/error.png" alt="">
	 *
	 * @param error the error to signal to each {@link Subscriber}
	 * @param <T> the reified type of the target {@link Subscriber}
	 *
	 * @return a new failed {@link Flux}
	 */
	public static <T> Flux<T> error(Throwable error) {
		return Mono.<T>error(error).flux();
	}

	/**
	 * Consume the passed 
	 * {@link Publisher} source and transform its sequence of T into a N sequences of V via the given {@link Function}.
	 * The produced sequences {@link Publisher} will be merged back in the returned {@link Flux}.
	 * The backpressure will apply using the provided bufferSize which will actively consume each sequence (and the 
	 * main one) and replenish its request cycle on a threshold free capacity.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmap.png" alt="">
	 *
	 * @param source the source to flatten
	 * @param mapper the function to transform the upstream sequence into N sub-sequences
	 * @param concurrency the maximum alive transformations at a given time
	 * @param bufferSize the bounded capacity for each individual merged sequence
	 * @param <T> the source type
	 * @param <V> the produced merged type
	 *
	 * @return a new merged {@link Flux}
	 */
	public static <T, V> Flux<V> flatMap(Publisher<? extends T> source,
			Function<? super T, ? extends Publisher<? extends V>> mapper,
			int concurrency,
			int bufferSize) {
		return new FluxFlatMap<>(source, mapper, concurrency, bufferSize);
	}

	/**
	 * Expose the specified {@link Publisher} with the {@link Flux} API.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/from.png" alt="">
	 *
	 * @param source the source to decorate
	 * @param <T> the source sequence type
	 *
	 * @return a new {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> from(Publisher<? extends T> source) {
		if (source instanceof Flux) {
			return (Flux<T>) source;
		}

		if (source instanceof Supplier) {
			T t = ((Supplier<T>) source).get();
			if (t != null) {
				return just(t);
			}
		}
		return new FluxBarrier<>(source);
	}

	/**
	 * Create a {@link Flux} that emits the items contained in the provided {@link Iterable}.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromarray.png" alt="">
	 *
	 * @param array the {@link T[]} array to read data from
	 * @param <T> the {@link Publisher} type to stream
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> fromArray(T[] array) {
		if (array == null || array.length == 0) {
			return empty();
		}
		if (array.length == 1) {
			return just(array[0]);
		}
		return new FluxArray<>(array);
	}

	/**
	 * Create a {@link Flux} that emits the items contained in the provided {@link Iterable}.
	 * A new iterator will be created for each subscriber.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromiterable.png" alt="">
	 *
	 * @param it the {@link Iterable} to read data from
	 * @param <T> the {@link Iterable} type to stream
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> fromIterable(Iterable<? extends T> it) {
		FluxGenerate.IterableSequencer<T> iterablePublisher = new FluxGenerate.IterableSequencer<>(it);
		return create(iterablePublisher, iterablePublisher);
	}


	/**
	 * Create a {@link Flux} that emits the items contained in the provided {@link Tuple}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromtuple.png" alt="">
	 * <p>
	 *
	 * @param tuple the {@link Tuple} to read data from
	 *
	 * @return a new {@link Flux}
	 */
	public static Flux<Object> fromTuple(Tuple tuple) {
		return fromArray(tuple.toArray());
	}



	/**
	 * Create a {@link Publisher} reacting on requests with the passed {@link BiConsumer}. The argument {@code
	 * contextFactory} is executed once by new subscriber to generate a context shared by every request calls. The
	 * argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel, onComplete,
	 * onError).
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/generate.png" alt="">
	 *
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory A {@link Function} called once for every new subscriber returning an immutable context (IO
	 * connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(),
	 * onError()
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 *
	 * @return a fresh Reactive Flux publisher ready to be subscribed
	 */
	public static <T, C> Flux<T> generate(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
			Function<Subscriber<? super T>, C> contextFactory,
			Consumer<C> shutdownConsumer) {

		return new FluxGenerate<>(new FluxGenerate.RecursiveConsumer<>(requestConsumer),
				contextFactory,
				shutdownConsumer);
	}

	/**
	 * Create a new {@link Flux} that emits an ever incrementing long starting with 0 every N seconds on
	 * the given timer. If demand is not produced in time, an onError will be signalled. The {@link Flux} will never
	 * complete.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/interval.png" alt="">
	 *
	 * @param seconds The number of seconds to wait before the next increment
	 *
	 * @return a new timed {@link Flux}
	 */
	public static Flux<Long> interval(long seconds) {
		return interval(seconds, TimeUnit.SECONDS);
	}

	/**
	 * Create a new {@link Flux} that emits an ever incrementing long starting with 0 every N period of time unit on
	 * the global timer. If demand is not produced in time, an onError will be signalled. The {@link Flux} will never
	 * complete.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/interval.png" alt="">
	 *
	 * @param period The the time relative to given unit to wait before the next increment
	 * @param unit The unit of time
	 *
	 * @return a new timed {@link Flux}
	 */
	public static Flux<Long> interval(long period, TimeUnit unit) {
		return interval(period, unit, Timers.global());
	}

	/**
	 * Create a new {@link Flux} that emits an ever incrementing long starting with 0 every N period of time unit on
	 * the given timer. If demand is not produced in time, an onError will be signalled. The {@link Flux} will never
	 * complete.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/interval.png" alt="">
	 *
	 * @param period The the time relative to given unit to wait before the next increment
	 * @param unit The unit of time
	 * @param timer a {@link Timer} instance
	 *
	 * @return a new timed {@link Flux}
	 */
	public static Flux<Long> interval(long period, TimeUnit unit, Timer timer) {
		long timespan = TimeUnit.MILLISECONDS.convert(period, unit);
		Assert.isTrue(timespan >= timer.period(), "The delay " + period + "ms cannot be less than the timer resolution" +
				"" + timer.period() + "ms");

		return new FluxInterval(timer, period, unit, period);
	}


	/**
	 * Create a new {@link Flux} that emits the specified items and then complete.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromarray.png" alt="">
	 *
	 * @param data the consecutive data objects to emit
	 * @param <T> the emitted data type
	 *
	 * @return a new {@link Flux}
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <T> Flux<T> just(T... data) {
		return fromArray(data);
	}

	/**
	 * Create a new {@link Flux} that will only emit the passed data then onComplete.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/just.png" alt="">
	 *
	 * @param data the unique data to emit
	 * @param <T> the emitted data type
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> just(T data) {
		return new FluxJust<>(data);
	}

	/**
	 * Create a {@link Flux} that will fallback to the produced {@link Publisher} given an onError signal.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png" alt="">
	 *
	 * @param source
	 * @param category
	 * @param level
	 * @param options
	 * @param <T> the {@link Subscriber} type target
	 *
	 * @return a logged {@link Flux}
	 */
	public static <T> Flux<T> log(Publisher<T> source, String category, Level level, int options) {
		return new FluxLog<>(source, category, level, options);
	}

	/**
	 * Create a {@link Flux} that will transform all signals into a target type.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/mapsignal.png" alt="">
	 *
	 * @param source
	 * @param nextMapper
	 * @param errorMapper
	 * @param completeMapper
	 * @param <T> the input publisher type
	 * @param <V> the output {@link Publisher} type target
	 *
	 * @return a new {@link Flux}
	 */
	public static <T, V> Flux<V> mapSignal(Publisher<T> source,
			Function<? super T, ? extends V> nextMapper,
			Function<Throwable, ? extends V> errorMapper,
			Supplier<? extends V> completeMapper) {
		return new FluxMapSignal<>(source, nextMapper, errorMapper, completeMapper);
	}

	/**
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/merge.png" alt="">
	 *
	 * @param source
	 * @param <T>
	 *
	 * @return
	 */
	public static <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source) {
		return new FluxFlatMap<>(source, PlatformDependent.SMALL_BUFFER_SIZE, 32);
	}

	/**
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/merge.png" alt="">
	 *
	 * @param sources
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive Flux publisher ready to be subscribed
	 */
	public static <I> Flux<I> merge(Iterable<? extends Publisher<? extends I>> sources) {
		return merge(fromIterable(sources));
	}

	/**
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/merge.png" alt="">
	 *
	 * @param sources
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive Flux publisher ready to be subscribed
	 */
	@SafeVarargs
	@SuppressWarnings({"unchecked", "varargs"})
	public static <I> Flux<I> merge(Publisher<? extends I>... sources) {
		if (sources == null || sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from(sources[0]);
		}
		return merge(fromArray(sources));
	}

	/**
	 * Create a {@link Flux} that never completes.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/never.png" alt="">
	 *
	 * @param <T> the {@link Subscriber} type target
	 *
	 * @return a never completing {@link Flux}
	 */
	public static <T> Flux<T> never() {
		return FluxNever.instance();
	}

	/**
	 * Create a {@link Flux} that will fallback to the produced {@link Publisher} given an onError signal.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/onerrorresumewith.png" alt="">
	 *
	 * @param <T> the {@link Subscriber} type target
	 *
	 * @return a resilient {@link Flux}
	 */
	public static <T> Flux<T> onErrorResumeWith(
			Publisher<? extends T> source,
			Function<Throwable, ? extends Publisher<? extends T>> fallback) {
		return new FluxResume<>(source, fallback);
	}

	/**
	 * Create a {@link Flux} reacting on subscribe with the passed {@link Consumer}. The argument {@code
	 * sessionConsumer} is executed once by new subscriber to generate a {@link ReactiveSession} context ready to accept
	 * signals.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/yield.png" alt="">
	 *
	 * @param sessionConsumer A {@link Consumer} called once everytime a subscriber subscribes
	 * @param <T> The type of the data sequence
	 *
	 * @return a fresh Reactive Flux publisher ready to be subscribed
	 */
	public static <T> Flux<T> yield(Consumer<? super ReactiveSession<T>> sessionConsumer) {
		return new FluxYieldingSession<>(sessionConsumer);
	}

	/**
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source1
	 * @param source2
	 * @param <T1>
	 * @param <T2>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2> Flux<Tuple2<T1, T2>> zip(Publisher<? extends T1> source1, Publisher<? extends T2> source2) {

		return new FluxZip<>(new Publisher[]{source1, source2},
				(Function<Tuple2<T1, T2>, Tuple2<T1, T2>>) IDENTITY_FUNCTION, PlatformDependent.XS_BUFFER_SIZE);
	}

	/**
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source1
	 * @param source2
	 * @param combinator
	 * @param <O>
	 * @param <T1>
	 * @param <T2>
	 *
	 * @return
	 */
	public static <T1, T2, O> Flux<O> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			final BiFunction<? super T1, ? super T2, ? extends O> combinator) {

		return new FluxZip<>(new Publisher[]{source1, source2}, new Function<Tuple2<T1, T2>, O>() {
			@Override
			public O apply(Tuple2<T1, T2> tuple) {
				return combinator.apply(tuple.getT1(), tuple.getT2());
			}
		}, PlatformDependent.XS_BUFFER_SIZE);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the value
	 * to signal downstream
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <V> The produced output after transformation by {@param combinator}
	 *
	 * @return a {@link Flux} based on the produced value
	 */
	public static <T1, T2, T3, V> Flux<V> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Function<Tuple3<T1, T2, T3>, ? extends V> combinator) {
		return new FluxZip<>(new Publisher[]{source1, source2, source3}, combinator, PlatformDependent.XS_BUFFER_SIZE);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Flux<Tuple3<T1, T2, T3>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3) {
		return zip(IDENTITY_FUNCTION, source1, source2, source3);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the value
	 * to signal downstream
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <V> The produced output after transformation by {@param combinator}
	 *
	 * @return a {@link Flux} based on the produced value
	 */
	public static <T1, T2, T3, T4, V> Flux<V> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Function<Tuple4<T1, T2, T3, T4>, V> combinator) {
		return new FluxZip<>(new Publisher[]{source1, source2, source3, source4},
				combinator,
				PlatformDependent.XS_BUFFER_SIZE);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4> Flux<Tuple4<T1, T2, T3, T4>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4) {
		return zip(IDENTITY_FUNCTION, source1, source2, source3, source4);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the value
	 * to signal downstream
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 * @param <V> The produced output after transformation by {@param combinator}
	 *
	 * @return a {@link Flux} based on the produced value
	 */
	public static <T1, T2, T3, T4, T5, V> Flux<V> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Function<Tuple5<T1, T2, T3, T4, T5>, V> combinator) {
		return new FluxZip<>(new Publisher[]{source1, source2, source3, source4, source5}, combinator, PlatformDependent
				.XS_BUFFER_SIZE);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5> Flux<Tuple5<T1, T2, T3, T4, T5>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5) {
		return zip(IDENTITY_FUNCTION, source1, source2, source3, source4, source5);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the value
	 * to signal downstream
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 * @param <T6> type of the value from source6
	 * @param <V> The produced output after transformation by {@param combinator}
	 *
	 * @return a {@link Flux} based on the produced value
	 */
	public static <T1, T2, T3, T4, T5, T6, V> Flux<V> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Publisher<? extends T6> source6,
			Function<Tuple6<T1, T2, T3, T4, T5, T6>, V> combinator) {
		return new FluxZip<>(new Publisher[]{source1, source2, source3, source4, source5, source6}, combinator,
				PlatformDependent
				.XS_BUFFER_SIZE);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
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
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6> Flux<Tuple6<T1, T2, T3, T4, T5, T6>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Publisher<? extends T6> source6) {
		return zip(IDENTITY_FUNCTION, source1, source2, source3, source4, source5, source6);
	}


	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
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
	 * @param <V> combined type
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, V> Flux<V> zip(Publisher<? extends T1>
			source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Publisher<? extends T6> source6,
			Publisher<? extends T7> source7,
			Function<Tuple7<T1, T2, T3, T4, T5, T6, T7>, V> combinator) {
		return new FluxZip<>(new Publisher[]{source1, source2, source3, source4, source5, source6, source7}, combinator,
				PlatformDependent
						.XS_BUFFER_SIZE);
	}


	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link Publisher} to subscribe to.
	 * @param source7 The seventh upstream {@link Publisher} to subscribe to.
	 * @param source8 The eigth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 * @param <T6> type of the value from source6
	 * @param <T7> type of the value from source7
	 * @param <T8> type of the value from source8
	 * @param <V> combined type
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8, V> Flux<V> zip(Publisher<? extends
			T1>
			source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Publisher<? extends T6> source6,
			Publisher<? extends T7> source7,
			Publisher<? extends T8> source8,
			Function<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>, V> combinator) {
		return new FluxZip<>(new Publisher[]{source1, source2, source3, source4, source5, source6, source7, source8},
				combinator, PlatformDependent
						.XS_BUFFER_SIZE);
	}

	/** <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param sources
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Flux<Tuple> zip(Iterable<? extends Publisher<?>> sources) {
		return zip(sources, IDENTITY_FUNCTION);
	}

	/**
	 * @param sources
	 * @param combinator
	 * @param <O>
	 *
	 * @return
	 */
	public static <O> Flux<O> zip(Iterable<? extends Publisher<?>> sources,
			final Function<Tuple, ? extends O> combinator) {

		if (sources == null) {
			return empty();
		}

		return new FluxZip<>(sources, combinator, PlatformDependent.XS_BUFFER_SIZE);
	}

	/**
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param sources
	 * @param combinator
	 * @param <O>
	 *
	 * @return
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <I, O> Flux<O> zip(
			final Function<? super Tuple, ? extends O> combinator, Publisher<? extends I>... sources) {

		if (sources == null) {
			return empty();
		}

		return new FluxZip<>(sources, combinator, PlatformDependent.XS_BUFFER_SIZE);
	}

//	 ==============================================================================================================
//	 Instance Operators
//	 ==============================================================================================================

	protected Flux() {
	}

	/**
	 * Immediately apply the given transformation to this Flux in order to generate a target {@link Publisher} type.
	 *
	 * {@code flux.as(Mono::from).subscribe(Subscribers.unbounded()) }
	 *
	 * @param transformer
	 * @param <P>
	 *
	 * @return a new {@link Flux}
	 */
	public final <V, P extends Publisher<V>> P as(Function<? super Flux<T>, P> transformer) {
		return transformer.apply(this);
	}

	/**
	 * Return a {@code Mono<Void>} that completes when this {@link Flux} completes.
	 * This will actively ignore the sequence and only replay completion or error signals.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/after.png" alt="">
	 *
	 * @return a new {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public final Mono<Void> after() {
		return (Mono<Void>)new MonoIgnoreElements<>(this);
	}

	/**
	 * Emit from the fastest first sequence between this publisher and the given publisher
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/amb.png" alt="">
	 *
	 * @return the fastest sequence
	 */
	public final Flux<T> ambWith(Publisher<? extends T> other) {
		return amb(this, other);
	}

	/**
	 * Hint {@link Subscriber} to this {@link Flux} a preferred available capacity should be used.
	 * {@link #toIterable()} can for instance use introspect this value to supply an appropriate queueing strategy.
	 *
	 * @param capacity the maximum capacity (in flight onNext) the return {@link Publisher} should expose
	 *
	 * @return a bounded {@link Flux}
	 */
	public final Flux<T> capacity(long capacity) {
		return new FluxBounded<>(this, capacity);
	}

	/**
	 * Like {@link #flatMap(Function)}, but concatenate emissions instead of merging (no interleave).
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concatmap.png" alt="">
	 *
	 * @param mapper the function to transform this sequence of T into concated sequences of R
	 * @param <R> the produced concated type
	 *
	 * @return a new {@link Flux}
	 */
	public final <R> Flux<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return new FluxFlatMap<>(this, mapper, 1, PlatformDependent.XS_BUFFER_SIZE);
	}

	/**
	 * Concatenate emissions of this {@link Flux} with the provided {@link Publisher} (no interleave).
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concat.png" alt="">
	 *
	 * @param source
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> concatWith(Publisher<? extends T> source) {
		return concat(this, source);
	}

	/**
	 * Introspect this Flux graph
	 *
	 * @return {@link ReactiveStateUtils.Graph} representation of a publisher graph
	 */
	public final ReactiveStateUtils.Graph debug() {
		return ReactiveStateUtils.scan(this);
	}

	/**
	 * Provide a default unique value if this sequence is completed without any data
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/defaultifempty.png" alt="">
	 *
	 * @param defaultV the alternate value if this sequence is empty
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> defaultIfEmpty(T defaultV) {
		return new FluxSwitchIfEmpty<>(this, just(defaultV));
	}

	/**
	 * Run onSubscribe, request, cancel, onNext, onComplete and onError on a supplied
	 * {@link ProcessorGroup#dispatchOn} reference {@link org.reactivestreams.Processor}.
	 *
	 * <p>
	 * Typically used for fast publisher, slow consumer(s) scenarios.
	 * It naturally combines with {@link Processors#singleGroup} and {@link Processors#asyncGroup} which implement
	 * fast async event loops.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/dispatchon.png" alt="">
	 *
	 * {@code flux.dispatchOn(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param group a {@link ProcessorGroup} pool
	 *
	 * @return a {@link Flux} consuming asynchronously
	 */
	@SuppressWarnings("unchecked")
	public final Flux<T> dispatchOn(ProcessorGroup group) {
		return new FluxProcessorGroup<>(this, false, ((ProcessorGroup<T>) group));
	}


	/**
	 * Triggered after the {@link Flux} terminates, either by completing downstream successfully or with an error.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doafterterminate.png" alt="">
	 *
	 * @param afterTerminate
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> doAfterTerminate(Runnable afterTerminate) {
		return new FluxPeek<>(this, null, null, null, afterTerminate, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} is cancelled.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/dooncancel.png" alt="">
	 *
	 * @param onCancel
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> doOnCancel(Runnable onCancel) {
		return new FluxPeek<>(this, null, null, null, null, null, null, onCancel);
	}

	/**
	 * Triggered when the {@link Flux} completes successfully.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/dooncomplete.png" alt="">
	 *
	 * @param onComplete
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> doOnComplete(Runnable onComplete) {
		return new FluxPeek<>(this, null, null, null, onComplete, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} completes with an error.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonerror.png" alt="">
	 *
	 * @param onError
	 *
	 * @return
	 */
	public final Flux<T> doOnError(Consumer<? super Throwable> onError) {
		return new FluxPeek<>(this, null, null, onError, null, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} emits an item.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonnext.png" alt="">
	 *
	 * @param onNext
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> doOnNext(Consumer<? super T> onNext) {
		return new FluxPeek<>(this, null, onNext, null, null, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} is subscribed.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonsubscribe.png" alt="">
	 *
	 * @param onSubscribe
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		return new FluxPeek<>(this, onSubscribe, null, null, null, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} terminates, either by completing successfully or with an error.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonterminate.png" alt="">
	 *
	 * @param onTerminate
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> doOnTerminate(Runnable onTerminate) {
		return new FluxPeek<>(this, null, null, null, null, onTerminate, null, null);
	}

	/**
	 * Transform the items emitted by this {@link Flux} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmap.png" alt="">
	 *
	 * @param mapper
	 * @param <R>
	 *
	 * @return a new {@link Flux}
	 */
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return new FluxFlatMap<>(this, mapper, PlatformDependent.SMALL_BUFFER_SIZE, 32);
	}

	/**
	 * Transform the signals emitted by this {@link Flux} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmaps.png" alt="">
	 *
	 * @param mapperOnNext
	 * @param mapperOnError
	 * @param mapperOnComplete
	 * @param <R>
	 *
	 * @return a new {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapperOnNext,
			Function<Throwable, ? extends Publisher<? extends R>> mapperOnError,
			Supplier<? extends Publisher<? extends R>> mapperOnComplete) {
		return new FluxFlatMap<>(
				new FluxMapSignal<>(this, mapperOnNext, mapperOnError, mapperOnComplete),
				IDENTITY_FUNCTION, PlatformDependent.SMALL_BUFFER_SIZE, 32);
	}

	@Override
	public String getName() {
		return getClass().getName()
		                 .replace(Flux.class.getSimpleName(), "");
	}

	@Override
	public int getMode() {
		return FACTORY;
	}

	/**
	 * Create a {@link Flux} intercepting all source signals with the returned Subscriber that might choose to pass them
	 * alone to the provided Subscriber (given to the returned {@code subscribe(Subscriber)}.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/lift.png" alt="">
	 *
	 * @param operator
	 * @param <R>
	 *
	 * @return a new {@link Flux}
	 */
	public final <R> Flux<R> lift(Function<Subscriber<? super R>, Subscriber<? super T>> operator) {
		return new FluxLift<>(this, operator);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace implementation. Default will
	 * use {@link Level#INFO} and java.util.logging. If SLF4J is available, it will be used instead.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png" alt="">
	 *
	 * The default log category will be "reactor.core.publisher.FluxLog".
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> log() {
		return log(null, Level.INFO, Logger.ALL);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace implementation. Default will
	 * use {@link Level#INFO} and java.util.logging. If SLF4J is available, it will be used instead.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png" alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework.reactor).
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> log(String category) {
		return log(category, Level.INFO, Logger.ALL);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace implementation. Default will
	 * use the passed {@link Level} and java.util.logging. If SLF4J is available, it will be used instead.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png" alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework.reactor).
	 * @param level the level to enforce for this tracing Flux
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> log(String category, Level level) {
		return log(category, level, Logger.ALL);
	}

	/**
	 * Observe Reactive Streams signals matching the passed flags {@code options} and use {@link Logger} support to
	 * handle trace
	 * implementation. Default will
	 * use the passed {@link Level} and java.util.logging. If SLF4J is available, it will be used instead.
	 *
	 * Options allow fine grained filtering of the traced signal, for instance to only capture onNext and onError:
	 * <pre>
	 *     flux.log("category", Level.INFO, Logger.ON_NEXT | LOGGER.ON_ERROR)
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png" alt="">
	 *
	 * @param category to be mapped into logger configuration (e.g. org.springframework.reactor).
	 * @param level the level to enforce for this tracing Flux
	 * @param options a flag option that can be mapped with {@link Logger#ON_NEXT} etc.
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> log(String category, Level level, int options) {
		return new FluxLog<>(this, category, level, options);
	}

	/**
	 * Transform the items emitted by this {@link Flux} by applying a function to each item.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/map.png" alt="">
	 *
	 * @param mapper
	 * @param <R>
	 *
	 * @return a new {@link Flux}
	 */
	public final <R> Flux<R> map(Function<? super T, ? extends R> mapper) {
		return new FluxMap<>(this, mapper);
	}

	/**
	 * Merge emissions of this {@link Flux} with the provided {@link Publisher}, so that they may interleave.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/merge.png" alt="">
	 *
	 * @param source
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> mergeWith(Publisher<? extends T> source) {
		return merge(just(this, source));
	}

	/**
	 * Emit only the first item emitted by this {@link Flux}.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/next.png" alt="">
	 *
	 * If the sequence emits more than 1 data, emit {@link ArrayIndexOutOfBoundsException}.
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> next() {
		return new MonoNext<>(this);
	}

	/**
	 * Subscribe to a returned fallback publisher when any error occurs.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/onerrorresumewith.png" alt="">
	 *
	 * @param fallback
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> onErrorResumeWith(Function<Throwable, ? extends Publisher<? extends T>> fallback) {
		return new FluxResume<>(this, fallback);
	}

	/**
	 * Fallback to the given value if an error is observed on this {@link Flux}
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/onerrorreturn.png" alt="">
	 *
	 * @param fallbackValue alternate value on fallback
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> onErrorReturn(final T fallbackValue) {
		return switchOnError(just(fallbackValue));
	}

	/**
	 *
	 * A chaining {@link Publisher#subscribe(Subscriber)} alternative to inline composition type conversion to a hot
	 * emitter (e.g. reactor FluxProcessor Broadcaster and Promise or rxjava Subject).
	 *
	 * {@code flux.subscribeWith(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param subscriber
	 * @param <E>
	 *
	 * @return the passed {@link Subscriber}
	 */
	public final <E extends Subscriber<? super T>> E subscribeWith(E subscriber) {
		subscribe(subscriber);
		return subscriber;
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Iterable} blocking on next calls.
	 *
	 * @return a blocking {@link Iterable}
	 */
	public final Iterable<T> toIterable() {
		return toIterable(this instanceof Backpressurable ? ((Backpressurable) this).getCapacity() : Long.MAX_VALUE
		);
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Iterable} blocking on next calls.
	 *
	 * @return a blocking {@link Iterable}
	 */
	public final Iterable<T> toIterable(long batchSize) {
		return toIterable(batchSize, null);
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Iterable} blocking on next calls.
	 *
	 * @return a blocking {@link Iterable}
	 */
	public final Iterable<T> toIterable(final long batchSize, Supplier<Queue<T>> queueProvider) {
		final Supplier<Queue<T>> provider;
		if(queueProvider == null){
			provider = QueueSupplier.get(batchSize);
		}
		else{
			provider = queueProvider;
		}
		return new BlockingIterable<>(this, batchSize, provider);
	}

	/**
	 * Run subscribe, onSubscribe and request on a supplied
	 * {@link ProcessorGroup#publishOn} reference {@link org.reactivestreams.Processor}.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/publishon.png" alt="">
	 *
	 * <p>
	 * Typically used for slow publisher e.g., blocking IO, fast consumer(s) scenarios.
	 * It naturally combines with {@link Processors#ioGroup} which implements work-queue thread dispatching.
	 *
	 * <p>
	 * {@code flux.publishOn(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param group a {@link ProcessorGroup} pool
	 *
	 * @return a {@link Flux} publishing asynchronously
	 */
	@SuppressWarnings("unchecked")
	public final Flux<T> publishOn(ProcessorGroup group) {
		return new FluxProcessorGroup<>(this, true, ((ProcessorGroup<T>) group));
	}

	/**
	 * Subscribe to the given fallback {@link Publisher} if an error is observed on this {@link Flux}
	 *
	 * @param fallback the alternate {@link Publisher}
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/switchonerror.png" alt="">
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> switchOnError(final Publisher<? extends T> fallback) {
		return onErrorResumeWith(FluxResume.create(fallback));
	}

	/**
	 * Provide an alternative if this sequence is completed without any data
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/switchifempty.png" alt="">
	 *
	 * @param alternate the alternate publisher if this sequence is empty
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> switchIfEmpty(Publisher<? extends T> alternate) {
		return new FluxSwitchIfEmpty<>(this, alternate);
	}

	/**
	 * Start the chain and request unbounded demand.
	 */
	public final void subscribe() {
		subscribe(Subscribers.unbounded());
	}

	/**
	 * Combine the emissions of multiple Publishers together and emit single {@link Tuple2} for each
	 * combination.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source2
	 * @param <R>
	 *
	 * @return a new {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	public final <R> Flux<Tuple2<T, R>> zipWith(Publisher<? extends R> source2) {
		return new FluxZip<>(new Publisher[]{this, source2}, IDENTITY_FUNCTION, PlatformDependent.XS_BUFFER_SIZE);
	}

	/**
	 * Combine the emissions of multiple Publishers together via a specified function and emit single items for each
	 * combination based on the results of this function.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param source2
	 * @param zipper
	 * @param <R>
	 * @param <V>
	 *
	 * @return a new {@link Flux}
	 */
	public final <R, V> Flux<V> zipWith(Publisher<? extends R> source2,
			final BiFunction<? super T, ? super R, ? extends V> zipper) {

		return new FluxZip<>(new Publisher[]{this, source2}, new Function<Tuple2<T, R>, V>() {
			@Override
			public V apply(Tuple2<T, R> tuple) {
				return zipper.apply(tuple.getT1(), tuple.getT2());
			}
		}, PlatformDependent.XS_BUFFER_SIZE);

	}

//	 ==============================================================================================================
//	 Containers
//	 ==============================================================================================================

	/**
	 * A marker interface for components responsible for augmenting subscribers with features like {@link #lift}
	 *
	 * @param <I>
	 * @param <O>
	 */
	public interface Operator<I, O> extends Function<Subscriber<? super O>, Subscriber<? super I>> {

	}

	/**
	 * A connecting Flux Publisher (right-to-left from a composition chain perspective)
	 *
	 * @param <I>
	 * @param <O>
	 */
	public static class FluxBarrier<I, O> extends Flux<O> implements Backpressurable, Publishable {

		protected final Publisher<? extends I> source;

		public FluxBarrier(Publisher<? extends I> source) {
			this.source = source;
		}

		@Override
		public long getCapacity() {
			return Backpressurable.class.isAssignableFrom(source.getClass()) ?
					((Backpressurable) source).getCapacity() :
					Long.MAX_VALUE;
		}

		@Override
		public long getPending() {
			return -1L;
		}

		/**
		 * Default is delegating and decorating with Flux API
		 *
		 * @param s
		 */
		@Override
		@SuppressWarnings("unchecked")
		public void subscribe(Subscriber<? super O> s) {
			source.subscribe((Subscriber<? super I>) s);
		}

		@Override
		public String toString() {
			return "{" +
					" operator : \"" + getName() + "\" " +
					'}';
		}

		@Override
		public final Publisher<? extends I> upstream() {
			return source;
		}
	}

	/**
	 * Decorate a Flux with a capacity for downstream accessors
	 *
	 * @param <I>
	 */
	final static class FluxBounded<I> extends FluxBarrier<I, I> {

		final private long capacity;

		public FluxBounded(Publisher<I> source, long capacity) {
			super(source);
			this.capacity = capacity;
		}

		@Override
		public long getCapacity() {
			return capacity;
		}

		@Override
		public String getName() {
			return "Bounded";
		}

		@Override
		public void subscribe(Subscriber<? super I> s) {
			source.subscribe(s);
		}
	}

	static final class FluxProcessorGroup<I> extends FluxBarrier<I, I> implements Connectable {

		private final ProcessorGroup<I> processor;
		private final boolean publishOn;

		public FluxProcessorGroup(Publisher<? extends I> source, boolean publishOn, ProcessorGroup<I> processor) {
			super(source);
			this.processor = processor;
			this.publishOn = publishOn;
		}

		@Override
		public void subscribe(Subscriber<? super I> s) {
			if(publishOn) {
				processor.publishOn(source)
				         .subscribe(s);
			}
			else{
				processor.dispatchOn(source)
				         .subscribe(s);
			}
		}

		@Override
		public Object connectedInput() {
			return processor;
		}

		@Override
		public Object connectedOutput() {
			return processor;
		}
	}

	/**
	 * i -> i
	 */
	static final class IdentityFunction implements Function {

		@Override
		public Object apply(Object o) {
			return o;
		}
	}

}
