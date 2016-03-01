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

import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Stream;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.queue.QueueSupplier;
import reactor.core.state.Backpressurable;
import reactor.core.state.Introspectable;
import reactor.core.subscriber.BlockingIterable;
import reactor.core.subscriber.ConsumerSubscriber;
import reactor.core.subscriber.SignalEmitter;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.timer.Timer;
import reactor.core.tuple.Tuple;
import reactor.core.tuple.Tuple2;
import reactor.core.tuple.Tuple3;
import reactor.core.tuple.Tuple4;
import reactor.core.tuple.Tuple5;
import reactor.core.tuple.Tuple6;
import reactor.core.util.Assert;
import reactor.core.util.Logger;
import reactor.core.util.PlatformDependent;
import reactor.core.util.ReactiveStateUtils;

/**
 * A Reactive Streams {@link Publisher} with rx operators that emits 0 to N elements, and then completes
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
 *
 * @see Mono
 * @since 2.5
 */
public abstract class Flux<T> implements Publisher<T>, Introspectable, Backpressurable{

//	 ==============================================================================================================
//	 Static Generators
//	 ==============================================================================================================

	static final Flux<?>          EMPTY                  = from(Mono.empty());

	/**
	 * Select the fastest source who won the "ambiguous" race and emitted first onNext or onComplete or onError
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/amb.png" alt="">
	 * <p> <p>
	 *
	 * @param sources The competing source publishers
	 * @param <I> The source type of the data sequence
	 *
	 * @return a new {@link Flux} eventually subscribed to one of the sources or empty
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
	 * <p> <p>
	 *
	 * @param sources The competing source publishers
	 * @param <I> The source type of the data sequence
	 *
	 * @return a new {@link Flux} eventually subscribed to one of the sources or empty
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
	 * <p>
	 * @param sources The {@link Publisher} of {@link Publisher} to concat
	 * @param <I> The source type of the data sequence
	 *
	 * @return a new {@link Flux} concatenating all inner sources sequences until complete or error
	 */
	@SuppressWarnings("unchecked")
	public static <I> Flux<I> concat(Publisher<? extends Publisher<? extends I>> sources) {
		return new FluxFlatMap<>(
				sources,
				Function.identity(),
				false,
				1,
				QueueSupplier.<I>one(),
				PlatformDependent.XS_BUFFER_SIZE,
				QueueSupplier.<I>xs()
		);
	}

	/**
	 * Concat all sources pulled from the supplied
	 * {@link Iterator} on {@link Publisher#subscribe} from the passed {@link Iterable} until {@link Iterator#hasNext}
	 * returns false. A complete signal from each source will delimit the individual sequences and will be eventually
	 * passed to the returned Publisher.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concat.png" alt="">
	 * <p>
	 * @param sources The {@link Publisher} of {@link Publisher} to concat
	 * @param <I> The source type of the data sequence
	 *
	 * @return a new {@link Flux} concatenating all source sequences
	 */
	public static <I> Flux<I> concat(Iterable<? extends Publisher<? extends I>> sources) {
		return concat(fromIterable(sources));
	}

	/**
	 * Concat all sources pulled from the given {@link Publisher} array.
	 * A complete signal from each source will delimit the individual sequences and will be eventually
	 * passed to the returned Publisher.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concat.png" alt="">
	 * <p>
	 * @param sources The {@link Publisher} of {@link Publisher} to concat
	 * @param <I> The source type of the data sequence
	 *
	 * @return a new {@link Flux} concatenating all source sequences
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
	 * <p>
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
	 * <p>
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
	 * <p>
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
	 * Run onNext, onComplete and onError on a supplied
	 * {@link Consumer} {@link Runnable} scheduler e.g. {@link SchedulerGroup#call}.
	 *
	 * <p>
	 * Typically used for fast publisher, slow consumer(s) scenarios.
	 * It naturally combines with {@link SchedulerGroup#single} and {@link SchedulerGroup#async} which implement
	 * fast async event loops.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/dispatchon.png" alt="">
	 * <p>
	 * {@code flux.dispatchOn(WorkQueueProcessor.create()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param source the {@link Publisher} to dispatch asynchronously
	 * @param scheduler a checked factory for {@link Consumer} of {@link Runnable}
	 * @param delayError true if errors should be delayed after consuming any available backlog
	 * @param prefetch the maximum in flight data to produce from the passed source {@link Publisher}
	 *
	 * @return a {@link Flux} consuming asynchronously
	 */
	public static <T> Flux<T> dispatchOn(Publisher<T> source,
			Callable<? extends Consumer<Runnable>> scheduler,
			boolean delayError,
			int prefetch,
			Supplier<? extends Queue<T>> queueProvider) {
		if (source instanceof Fuseable.ScalarSupplier) {
			@SuppressWarnings("unchecked")
			T value = ((Fuseable.ScalarSupplier<T>)source).get();
			return new FluxPublishOnValue<>(value, scheduler, true);
		}

		return new FluxDispatchOn<>(source, scheduler, delayError, prefetch, queueProvider);
	}

	/**
	 * Create a {@link Flux} that completes without emitting any item.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/empty.png" alt="">
	 * <p>
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
	 * <p>
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
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmapc.png" alt="">
	 *
	 * @param source the source to flatten
	 * @param mapper the function to transform the upstream sequence into N sub-sequences
	 * @param concurrency the maximum alive transformations at a given time
	 * @param bufferSize the bounded capacity for each individual merged sequence
	 * @param delayError Consume all pending sequence backlogs before replaying any captured error
	 * @param <T> the source type
	 * @param <V> the produced merged type
	 *
	 * @return a new merged {@link Flux}
	 */
	public static <T, V> Flux<V> flatMap(
			Publisher<? extends T> source,
			Function<? super T, ? extends Publisher<? extends V>> mapper,
			int concurrency,
			int bufferSize,
			boolean delayError) {

		return new FluxFlatMap<>(
				source,
				mapper,
				delayError,
				concurrency,
				QueueSupplier.<V>get(concurrency),
				bufferSize,
				QueueSupplier.<V>get(bufferSize)
		);
	}

	/**
	 * Expose the specified {@link Publisher} with the {@link Flux} API.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/from.png" alt="">
	 * <p>
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
		return FluxSource.wrap(source);
	}

	/**
	 * Create a {@link Flux} that emits the items contained in the provided {@link Iterable}.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromarray.png" alt="">
	 * <p>
	 * @param array the array to read data from
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
	 * <p>
	 * @param it the {@link Iterable} to read data from
	 * @param <T> the {@link Iterable} type to stream
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> fromIterable(Iterable<? extends T> it) {
		return new FluxIterable<>(it);
	}


	/**
	 * Create a {@link Flux} that emits the items contained in the provided {@link Tuple}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromtuple.png" alt="">
	 * <p> <p>
	 *
	 * @param tuple the {@link Tuple} to read data from
	 *
	 * @return a new {@link Flux}
	 */
	public static Flux<Object> fromTuple(Tuple tuple) {
		return fromArray(tuple.toArray());
	}



	/**
	 * Create a {@link Flux} reacting on requests with the passed {@link BiConsumer}. The argument {@code
	 * contextFactory} is executed once by new subscriber to generate a context shared by every request calls. The
	 * argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel, onComplete,
	 * onError).
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/generate.png" alt="">
	 * <p>
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory A {@link Function} called once for every new subscriber returning an immutable context (IO
	 * connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(),
	 * onError()
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 *
	 * @return a fresh Reactive {@link Flux} publisher ready to be subscribed
	 */
	public static <T, C> Flux<T> generate(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
			Function<Subscriber<? super T>, C> contextFactory,
			Consumer<C> shutdownConsumer) {

		return new FluxGenerate<>(new FluxGenerate.RecursiveConsumer<>(requestConsumer),
				contextFactory,
				shutdownConsumer);
	}

	/**
	 * Create a new {@link Flux} that emits an ever incrementing long starting with 0 every N milliseconds on
	 * the given timer. If demand is not produced in time, an onError will be signalled. The {@link Flux} will never
	 * complete.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/interval.png" alt="">
	 * <p>
	 * @param period The number of milliseconds to wait before the next increment
	 *
	 * @return a new timed {@link Flux}
	 */
	public static Flux<Long> interval(long period) {
		return interval(period, Timer.global());
	}

	/**
	 * Create a new {@link Flux} that emits an ever incrementing long starting with 0 every period on
	 * the global timer. If demand is not produced in time, an onError will be signalled. The {@link Flux} will never
	 * complete.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/interval.png" alt="">
	 * <p>
	 * @param period The duration to wait before the next increment
	 * @return a new timed {@link Flux}
	 */
	public static Flux<Long> interval(Duration period) {
		return interval(period.toMillis());
	}

	/**
	 * Create a new {@link Flux} that emits an ever incrementing long starting with 0 every N milliseconds on
	 * the given timer. If demand is not produced in time, an onError will be signalled. The {@link Flux} will never
	 * complete.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/interval.png" alt="">
	 * <p>
	 * @param period The duration in milliseconds to wait before the next increment
	 * @param timer a {@link Timer} instance
	 *
	 * @return a new timed {@link Flux}
	 */
	public static Flux<Long> interval(long period, Timer timer) {
		Assert.isTrue(period >= timer.period(), "The period " + period + " cannot be less than the timer resolution " +
				"" + timer.period());

		return new FluxInterval(timer, period, period);
	}

	/**
	 * Create a new {@link Flux} that emits an ever incrementing long starting with 0 every period on
	 * the given timer. If demand is not produced in time, an onError will be signalled. The {@link Flux} will never
	 * complete.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/interval.png" alt="">
	 * <p>
	 * @param period The duration to wait before the next increment
	 * @param timer a {@link Timer} instance
	 *
	 * @return a new timed {@link Flux}
	 */
	public static Flux<Long> interval(Duration period, Timer timer) {
		return interval(period.toMillis(), timer);
	}


	/**
	 * Create a new {@link Flux} that emits the specified items and then complete.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/justn.png" alt="">
	 * <p>
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
	 * <p>
	 * @param data the unique data to emit
	 * @param <T> the emitted data type
	 *
	 * @return a new {@link Flux}
	 */
	public static <T> Flux<T> just(T data) {
		return new FluxJust<>(data);
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
	 * <p>
	 * @param source the source {@link Publisher} to log
	 * @param category to be mapped into logger configuration (e.g. org.springframework.reactor).
	 * @param level the level to enforce for this tracing Flux
	 * @param options a flag option that can be mapped with {@link Logger#ON_NEXT} etc.
	 *
	 * @param <T> the {@link Subscriber} type target
	 *
	 * @return a logged {@link Flux}
	 */
	public static <T> Flux<T> log(Publisher<T> source, String category, Level level, int options) {
		return new FluxLog<>(source, category, level, options);
	}

	/**
	 * Create a {@link Flux} that will transform all signals into a target type. OnError will be transformed into
	 * completion signal after its mapping callback has been applied.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/mapsignal.png" alt="">
	 * <p>
	 * @param source the source {@link Publisher} to map
	 * @param mapperOnNext the {@link Function} to call on next data and returning the target transformed data
	 * @param mapperOnError the {@link Function} to call on error signal and returning the target transformed data
	 * @param mapperOnComplete the {@link Function} to call on complete signal and returning the target transformed data
	 * @param <T> the input publisher type
	 * @param <V> the output {@link Publisher} type target
	 *
	 * @return a new {@link Flux}
	 */
	public static <T, V> Flux<V> mapSignal(Publisher<T> source,
			Function<? super T, ? extends V> mapperOnNext,
			Function<Throwable, ? extends V> mapperOnError,
			Supplier<? extends V> mapperOnComplete) {
		return new FluxMapSignal<>(source, mapperOnNext, mapperOnError, mapperOnComplete);
	}

	/**
	 * Merge emitted {@link Publisher} sequences by the passed {@link Publisher} into an interleaved merged sequence.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/mergeinner.png" alt="">
	 * <p>
	 * @param source a {@link Publisher} of {@link Publisher} sequence to merge
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source) {
		return new FluxFlatMap<>(
				source,
				Function.identity(),
				false,
				PlatformDependent.SMALL_BUFFER_SIZE,
				QueueSupplier.<T>small(),
				PlatformDependent.XS_BUFFER_SIZE,
				QueueSupplier.<T>xs()
		);
	}

	/**
	 * Merge emitted {@link Publisher} sequences from the passed {@link Iterable} into an interleaved merged sequence.
	 * {@link Iterable#iterator()} will be called for each {@link Publisher#subscribe}.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/merge.png" alt="">
	 * <p>
	 * @param sources the {@link Iterable} to lazily iterate on {@link Publisher#subscribe(Subscriber)}
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive {@link Flux} publisher ready to be subscribed
	 */
	public static <I> Flux<I> merge(Iterable<? extends Publisher<? extends I>> sources) {
		return merge(fromIterable(sources));
	}

	/**
	 * Merge emitted {@link Publisher} sequences from the passed {@link Publisher} array into an interleaved merged
	 * sequence.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/merge.png" alt="">
	 * <p>
	 * @param sources the {@link Publisher} array to iterate on {@link Publisher#subscribe(Subscriber)}
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive {@link Flux} publisher ready to be subscribed
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
	 * Create a {@link Flux} that will never signal any data, error or completion signal.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/never.png" alt="">
	 * <p>
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
	 * <p>
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
	 * Run subscribe, onSubscribe and request on a supplied
	 * {@link Consumer} {@link Runnable} scheduler like {@link SchedulerGroup}.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/publishon.png" alt="">
	 * <p>
	 * <p>
	 * Typically used for slow publisher e.g., blocking IO, fast consumer(s) scenarios.
	 * It naturally combines with {@link SchedulerGroup#io} which implements work-queue thread dispatching.
	 *
	 * <p>
	 * {@code flux.publishOn(WorkQueueProcessor.create()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param source a {@link Publisher} source to publish from the given scheduler
	 * @param schedulerFactory a checked factory for {@link Consumer} of {@link Runnable}
	 *
	 * @return a {@link Flux} publishing asynchronously
	 */
	public static <T> Flux<T> publishOn(Publisher<? extends T> source,
			Callable<? extends Consumer<Runnable>> schedulerFactory) {
		return new FluxPublishOn<>(source, schedulerFactory);
	}

	/**
	 * Build a {@link Flux} that will only emit a sequence of incrementing integer from {@code start} to {@code
	 * start + count} then complete.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/range.png" alt="">
	 *
	 * @param start the first integer to be emit
	 * @param count   the number ot times to emit an increment including the first value
	 * @return a ranged {@link Flux}
	 */
	public static Flux<Integer> range(int start, int count) {
		if(count == 1){
			return just(start);
		}
		if(count == 0){
			return empty();
		}
		return new FluxRange(start, count);
	}

	/**
	 * Create a {@link Flux} reacting on subscribe with the passed {@link Consumer}. The argument {@code
	 * sessionConsumer} is executed once by new subscriber to generate a {@link SignalEmitter} context ready to accept
	 * signals.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/yield.png" alt="">
	 * <p>
	 * @param sessionConsumer A {@link Consumer} called once everytime a subscriber subscribes
	 * @param <T> The type of the data sequence
	 *
	 * @return a fresh Reactive {@link Flux} publisher ready to be subscribed
	 */
	public static <T> Flux<T> yield(Consumer<? super SignalEmitter<T>> sessionConsumer) {
		return new FluxYieldingEmitter<>(sessionConsumer);
	}

	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations
	 * produced by the passed combinator function of the
	 * most recent items emitted by each source until any of them completes. Errors will immediately be forwarded.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 * <p>
	 *
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
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

		return zip(new Function<Object[], O>() {
			@Override
			@SuppressWarnings("unchecked")
			public O apply(Object[] tuple) {
				return combinator.apply((T1)tuple[0], (T2)tuple[1]);
			}
		}, source1, source2);
	}

	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations of the
	 * most recent items emitted by each source until any of them completes. Errors will immediately be forwarded.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zipt.png" alt="">
	 * <p>
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 *
	 * @return a zipped {@link Flux}
	 */
	public static <T1, T2> Flux<Tuple2<T1, T2>> zip(Publisher<? extends T1> source1, Publisher<? extends T2> source2) {
		return zip(Tuple.<T1, T2>fn2(), source1, source2);
	}

	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations of the
	 * most recent items emitted by each source until any of them completes. Errors will immediately be forwarded.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zipt.png" alt="">
	 * <p>
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 *
	 * @return a zipped {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Flux<Tuple3<T1, T2, T3>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3) {
		return zip(Tuple.<T1, T2, T3>fn3(), source1, source2, source3);
	}

	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations of the
	 * most recent items emitted by each source until any of them completes. Errors will immediately be forwarded.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zipt.png" alt="">
	 * <p>
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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4> Flux<Tuple4<T1, T2, T3, T4>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4) {
		return zip(Tuple.<T1, T2, T3, T4>fn4(), source1, source2, source3, source4);
	}

	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations of the
	 * most recent items emitted by each source until any of them completes. Errors will immediately be forwarded.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zipt.png" alt="">
	 * <p>
	 * @param source1 The first upstream {@link Publisher} to subscribe to.
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param source3 The third upstream {@link Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 *
	 * @return a zipped {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5> Flux<Tuple5<T1, T2, T3, T4, T5>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5) {
		return zip(Tuple.<T1, T2, T3, T4, T5>fn5(), source1, source2, source3, source4, source5);
	}

	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations of the
	 * most recent items emitted by each source until any of them completes. Errors will immediately be forwarded.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zipt.png" alt="">
	 * <p>
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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6> Flux<Tuple6<T1, T2, T3, T4, T5, T6>> zip(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Publisher<? extends T6> source6) {
		return zip(Tuple.<T1, T2, T3, T4, T5, T6>fn6(), source1, source2, source3, source4, source5, source6);
	}

	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations
	 * of the most recent items emitted by each source until any of them completes. Errors will immediately be
	 * forwarded.
	 * The {@link Iterable#iterator()} will be called on each {@link Publisher#subscribe(Subscriber)}.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zipt.png" alt="">
	 * <p>
	 * @param sources the {@link Iterable} to iterate on {@link Publisher#subscribe(Subscriber)}
	 *
	 * @return a zipped {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	public static Flux<Tuple> zip(Iterable<? extends Publisher<?>> sources) {
		return zip(sources, Tuple.fnAny());
	}

	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations
	 * produced by the passed combinator function of the
	 * most recent items emitted by each source until any of them completes. Errors will immediately be forwarded.
	 *
	 * The {@link Iterable#iterator()} will be called on each {@link Publisher#subscribe(Subscriber)}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 *
	 * @param sources the {@link Iterable} to iterate on {@link Publisher#subscribe(Subscriber)}
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the value
	 * to signal downstream
	 * @param <O> the combined produced type
	 *
	 * @return a zipped {@link Flux}
	 */
	public static <O> Flux<O> zip(Iterable<? extends Publisher<?>> sources,
			final Function<? super Object[], ? extends O> combinator) {

		return zip(sources, PlatformDependent.XS_BUFFER_SIZE, combinator);
	}

	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations
	 * produced by the passed combinator function of the
	 * most recent items emitted by each source until any of them completes. Errors will immediately be forwarded.
	 *
	 * The {@link Iterable#iterator()} will be called on each {@link Publisher#subscribe(Subscriber)}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zipp.png" alt="">
	 *
	 * @param sources the {@link Iterable} to iterate on {@link Publisher#subscribe(Subscriber)}
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

		if (sources == null) {
			return empty();
		}

		return new FluxZip<>(sources, combinator, QueueSupplier.get(prefetch), prefetch);
	}

	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations
	 * produced by the passed combinator function of the
	 * most recent items emitted by each source until any of them completes. Errors will immediately be forwarded.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 * <p>
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 * value to signal downstream
	 * @param sources the {@link Publisher} array to iterate on {@link Publisher#subscribe(Subscriber)}
	 * @param <O> the combined produced type
	 *
	 * @return a zipped {@link Flux}
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <I, O> Flux<O> zip(
			final Function<? super Object[], ? extends O> combinator, Publisher<? extends I>... sources) {
		return zip(combinator, PlatformDependent.XS_BUFFER_SIZE, sources);
	}
	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations
	 * produced by the passed combinator function of the
	 * most recent items emitted by each source until any of them completes. Errors will immediately be forwarded.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zipp.png" alt="">
	 * <p>
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 * value to signal downstream
	 * @param prefetch individual source request size
	 * @param sources the {@link Publisher} array to iterate on {@link Publisher#subscribe(Subscriber)}
	 * @param <O> the combined produced type
	 *
	 * @return a zipped {@link Flux}
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <I, O> Flux<O> zip(
			final Function<? super Object[], ? extends O> combinator, int prefetch, Publisher<? extends I>... sources) {

		if (sources == null) {
			return empty();
		}

		return new FluxZip<>(sources, combinator, QueueSupplier.get(prefetch), prefetch);
	}

//	 ==============================================================================================================
//	 Instance Operators
//	 ==============================================================================================================

	protected Flux() {
	}

	/**
	 * Immediately apply the given transformation to this {@link Flux} in order to generate a target {@link Publisher} type.
	 *
	 * {@code flux.as(Mono::from).subscribe(Subscribers.unbounded()) }
	 *
	 * @param transformer the {@link Function} to immediately map this {@link Flux} into a target {@link Publisher}
	 * instance.
	 * @param <P> the returned {@link Publisher} sequence type
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
	 * <p>
	 * @return a new {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public final Mono<Void> after() {
		return (Mono<Void>)new MonoIgnoreElements<>(this);
	}

	/**
	 * Emit from the fastest first sequence between this publisher and the given publisher
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/amb.png" alt="">
	 * <p>
	 * @param other the {@link Publisher} to race with
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
	public final Flux<T> useCapacity(long capacity) {
		return new FluxBounded<>(this, capacity);
	}

	/**
	 * Like {@link #flatMap(Function)}, but concatenate emissions instead of merging (no interleave).
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concatmap.png" alt="">
	 * <p>
	 * @param mapper the function to transform this sequence of T into concated sequences of R
	 * @param <R> the produced concated type
	 *
	 * @return a new {@link Flux}
	 */
	public final <R> Flux<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return new FluxFlatMap<>(
				this,
				mapper,
				false,
				1,
				QueueSupplier.<R>one(),
				PlatformDependent.XS_BUFFER_SIZE,
				QueueSupplier.<R>xs()
		);
	}

	/**
	 * Concatenate emissions of this {@link Flux} with the provided {@link Publisher} (no interleave).
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concat.png" alt="">
	 * <p>
	 * @param other the {@link Publisher} sequence to concat after this {@link Flux}
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> concatWith(Publisher<? extends T> other) {
		return concat(this, other);
	}

	/**
	 * Introspect this {@link Flux} graph
	 *
	 * @return {@link ReactiveStateUtils} {@literal Graph} representation of the operational flow
	 */
	public final ReactiveStateUtils.Graph debug() {
		return ReactiveStateUtils.scan(this);
	}

	/**
	 * Provide a default unique value if this sequence is completed without any data
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/defaultifempty.png" alt="">
	 * <p>
	 * @param defaultV the alternate value if this sequence is empty
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> defaultIfEmpty(T defaultV) {
		return new FluxSwitchIfEmpty<>(this, just(defaultV));
	}

	/**
	 * Run onNext, onComplete and onError on a supplied
	 * {@link Consumer} {@link Runnable} scheduler factory like {@link SchedulerGroup}.
	 *
	 * <p>
	 * Typically used for fast publisher, slow consumer(s) scenarios.
	 * It naturally combines with {@link SchedulerGroup#single} and {@link SchedulerGroup#async} which implement
	 * fast async event loops.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/dispatchon.png" alt="">
	 * <p>
	 * {@code flux.dispatchOn(WorkQueueProcessor.create()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param scheduler a checked factory for {@link Consumer} of {@link Runnable}
	 *
	 * @return a {@link Flux} consuming asynchronously
	 */
	public final Flux<T> dispatchOn(Callable<? extends Consumer<Runnable>> scheduler) {
		return dispatchOn(this, scheduler, true, PlatformDependent.XS_BUFFER_SIZE, QueueSupplier.<T>xs());
	}

	/**
	 * Triggered after the {@link Flux} terminates, either by completing downstream successfully or with an error.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doafterterminate.png" alt="">
	 * <p>
	 * @param afterTerminate the callback to call after {@link Subscriber#onComplete} or {@link Subscriber#onError}
	 *
	 * @return a new unaltered {@link Flux}
	 */
	public final Flux<T> doAfterTerminate(Runnable afterTerminate) {
		return new FluxPeek<>(this, null, null, null, afterTerminate, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} is cancelled.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/dooncancel.png" alt="">
	 * <p>
	 * @param onCancel the callback to call on {@link Subscription#cancel}
	 *
	 * @return a new unaltered {@link Flux}
	 */
	public final Flux<T> doOnCancel(Runnable onCancel) {
		return new FluxPeek<>(this, null, null, null, null, null, null, onCancel);
	}

	/**
	 * Triggered when the {@link Flux} completes successfully.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/dooncomplete.png" alt="">
	 * <p>
	 * @param onComplete the callback to call on {@link Subscriber#onComplete}
	 *
	 * @return a new unaltered {@link Flux}
	 */
	public final Flux<T> doOnComplete(Runnable onComplete) {
		return new FluxPeek<>(this, null, null, null, onComplete, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} completes with an error.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonerror.png" alt="">
	 * <p>
	 * @param onError the callback to call on {@link Subscriber#onError}
	 *
	 * @return a new unaltered {@link Flux}
	 */
	public final Flux<T> doOnError(Consumer<? super Throwable> onError) {
		return new FluxPeek<>(this, null, null, onError, null, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} emits an item.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonnext.png" alt="">
	 * <p>
	 * @param onNext the callback to call on {@link Subscriber#onNext}
	 *
	 * @return a new unaltered {@link Flux}
	 */
	public final Flux<T> doOnNext(Consumer<? super T> onNext) {
		return new FluxPeek<>(this, null, onNext, null, null, null, null, null);
	}

	/**
	 * Attach a {@link LongConsumer} to this {@link Flux} that will observe any request to this {@link Flux}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonrequest.png" alt="">
	 *
	 * @param consumer the consumer to invoke on each request
	 *
	 * @return an observed  {@link Flux}
	 */
	public final Flux<T> doOnRequest(final LongConsumer consumer) {
		return new FluxPeek<>(this, null, null, null, null, null, consumer, null);
	}

	/**
	 * Triggered when the {@link Flux} is subscribed.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonsubscribe.png" alt="">
	 * <p>
	 * @param onSubscribe the callback to call on {@link Subscriber#onSubscribe}
	 *
	 * @return a new unaltered {@link Flux}
	 */
	public final Flux<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		return new FluxPeek<>(this, onSubscribe, null, null, null, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} terminates, either by completing successfully or with an error.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonterminate.png" alt="">
	 * <p>
	 * @param onTerminate the callback to call on {@link Subscriber#onComplete} or {@link Subscriber#onError}
	 *
	 * @return a new unaltered {@link Flux}
	 */
	public final Flux<T> doOnTerminate(Runnable onTerminate) {
		return new FluxPeek<>(this, null, null, null, null, onTerminate, null, null);
	}

	/**
	 * Transform the items emitted by this {@link Flux} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmap.png" alt="">
	 * <p>
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param <R> the merged output sequence type
	 *
	 * @return a new {@link Flux}
	 */
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return new FluxFlatMap<>(
				this,
				mapper,
				false,
				PlatformDependent.SMALL_BUFFER_SIZE,
				QueueSupplier.<R>small(),
				PlatformDependent.XS_BUFFER_SIZE,
				QueueSupplier.<R>xs()
		);
	}

	/**
	 * Transform the signals emitted by this {@link Flux} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 * OnError will be transformed into completion signal after its mapping callback has been applied.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmaps.png" alt="">
	 * <p>
	 * @param mapperOnNext the {@link Function} to call on next data and returning a sequence to merge
	 * @param mapperOnError the {@link Function} to call on error signal and returning a sequence to merge
	 * @param mapperOnComplete the {@link Function} to call on complete signal and returning a sequence to merge
	 * @param <R> the output {@link Publisher} type target
	 *
	 * @return a new {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapperOnNext,
			Function<Throwable, ? extends Publisher<? extends R>> mapperOnError,
			Supplier<? extends Publisher<? extends R>> mapperOnComplete) {
		return new FluxFlatMap<>(
				new FluxMapSignal<>(this, mapperOnNext, mapperOnError, mapperOnComplete),
				Function.identity(),
				false,
				PlatformDependent.SMALL_BUFFER_SIZE,
				QueueSupplier.<R>small(),
				PlatformDependent.XS_BUFFER_SIZE,
				QueueSupplier.<R>xs()
		);
	}

	/**
	 * Get the current timer available if any or try returning the shared Environment one (which may cause an error if
	 * no Environment has been globally initialized)
	 *
	 * @return any available timer
	 */
	public Timer getTimer() {
		return Timer.globalOrNull();
	}


	@Override
	public int getMode() {
		return FACTORY;
	}

	@Override
	public String getName() {
		return getClass().getSimpleName()
		                 .replace(Flux.class.getSimpleName(), "");
	}


	/**
	 * Re-route this sequence into dynamically created {@link Flux} for each unique key evaluated by the given
	 * key mapper.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/groupby.png" alt="">
	 *
	 * @param keyMapper the key mapping {@link Function} that evaluates an incoming data and returns a key.
	 *
	 * @return a {@link Flux} of {@link GroupedFlux} grouped sequences
	 *
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <K> Flux<GroupedFlux<K, T>> groupBy(final Function<? super T, ? extends K> keyMapper) {
		return groupBy(keyMapper, Function.identity());
	}

	/**
	 * Re-route this sequence into dynamically created {@link Flux} for each unique key evaluated by the given
	 * key mapper. It will use the given value mapper to extract the element to route.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/groupby.png" alt="">
	 *
	 * @param keyMapper the key mapping function that evaluates an incoming data and returns a key.
	 * @param valueMapper the value mapping function that evaluates which data to extract for re-routing.
	 *
	 * @return a {@link Flux} of {@link GroupedFlux} grouped sequences
	 *
	 * @since 2.5
	 */
	public final <K, V> Flux<GroupedFlux<K, V>> groupBy(Function<? super T, ? extends K> keyMapper,
			Function<? super T, ? extends V> valueMapper) {
		return new FluxGroupBy<>(this, keyMapper, valueMapper,
				QueueSupplier.<GroupedFlux<K, V>>small(),
				QueueSupplier.<V>unbounded(),
				PlatformDependent.SMALL_BUFFER_SIZE);
	}
	
	/**
	 * Hides the identities of this {@link Flux} and its {@link Subscription}
	 * as well.
	 *
	 * @return a new {@link Flux} defeating any {@link Publisher} / {@link Subscription} feature-detection
	 */
	public final Flux<T> hide() {
		return new FluxHide<>(this);
	}
	
	/**
	 * Create a {@link Flux} intercepting all source signals with the returned Subscriber that might choose to pass them
	 * alone to the provided Subscriber (given to the returned {@code subscribe(Subscriber)}.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/lift.png" alt="">
	 * <p>
	 * @param lifter the function accepting the target {@link Subscriber} and returning the {@link Subscriber}
	 * exposed this sequence
	 * @param <R> the output operator type
	 *
	 * @return a new {@link Flux}
	 */
	public final <R> Flux<R> lift(Function<Subscriber<? super R>, Subscriber<? super T>> lifter) {
		return new FluxLift<>(this, lifter);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace implementation. Default will
	 * use {@link Level#INFO} and java.util.logging. If SLF4J is available, it will be used instead.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png" alt="">
	 * <p>
	 * The default log category will be "reactor.core.publisher.FluxLog".
	 *
	 * @return a new unaltered {@link Flux}
	 */
	public final Flux<T> log() {
		return log(null, Level.INFO, Logger.ALL);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace implementation. Default will
	 * use {@link Level#INFO} and java.util.logging. If SLF4J is available, it will be used instead.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png" alt="">
	 * <p>
	 * @param category to be mapped into logger configuration (e.g. org.springframework.reactor).
	 *
	 * @return a new unaltered {@link Flux}
	 */
	public final Flux<T> log(String category) {
		return log(category, Level.INFO, Logger.ALL);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace implementation. Default will
	 * use the passed {@link Level} and java.util.logging. If SLF4J is available, it will be used instead.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log.png" alt="">
	 * <p>
	 * @param category to be mapped into logger configuration (e.g. org.springframework.reactor).
	 * @param level the level to enforce for this tracing Flux
	 *
	 * @return a new unaltered {@link Flux}
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
	 * <p>
	 * @param category to be mapped into logger configuration (e.g. org.springframework.reactor).
	 * @param level the level to enforce for this tracing Flux
	 * @param options a flag option that can be mapped with {@link Logger#ON_NEXT} etc.
	 *
	 * @return a new unaltered {@link Flux}
	 */
	public final Flux<T> log(String category, Level level, int options) {
		return new FluxLog<>(this, category, level, options);
	}

	/**
	 * Transform the items emitted by this {@link Flux} by applying a function to each item.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/map.png" alt="">
	 * <p>
	 * @param mapper the transforming {@link Function}
	 * @param <R> the transformed type
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
	 * <p>
	 * @param other the {@link Publisher} to merge with
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> mergeWith(Publisher<? extends T> other) {
		return merge(just(this, other));
	}

	/**
	 * Prepare a
	 * {@link ConnectableFlux} which subscribes this {@link Flux} sequence to the given {@link Processor}.
	 * The {@link Processor} will be itself subscribed by child {@link Subscriber} when {@link ConnectableFlux#connect()}
	 *  is invoked manually or automatically via {@link ConnectableFlux#autoConnect} and {@link ConnectableFlux#refCount}.
	 *  Note that some {@link Processor} do not support multi-subscribe, multicast is non opinionated in fact and
	 *  focuses on subscribe lifecycle.
	 *
	 * This will effectively turn any type of sequence into a hot sequence by sharing a single {@link Subscription}.
	 * <p> The {@link Processor} will not be specifically reusable and multi-connect might not work as expected
	 * depending on the {@link Processor}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/multicastp.png" alt="">
	 *
	 * @param processor the {@link Processor} reference to subscribe to this {@link Flux} and share.
	 *
	 * @return a new {@link ConnectableFlux} whose values are broadcasted to supported subscribers once connected via {@link Processor}
	 *
	 * @since 2.5
	 */
	public final ConnectableFlux<T> multicast(final Processor<? super T, ? extends T> processor) {
		return multicast(() -> processor);
	}

	/**
	 * Prepare a
	 * {@link ConnectableFlux} which subscribes this {@link Flux} sequence to a supplied {@link Processor}
	 * when
	 * {@link ConnectableFlux#connect()} is invoked manually or automatically via {@link ConnectableFlux#autoConnect} and {@link ConnectableFlux#refCount}.
	 * The {@link Processor} will be itself subscribed by child {@link Subscriber}.
	 *  Note that some {@link Processor} do not support multi-subscribe, multicast is non opinionated in fact and
	 *  focuses on subscribe lifecycle.
	 *
	 * This will effectively turn any type of sequence into a hot sequence by sharing a single {@link Subscription}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/multicastp.png" alt="">
	 *
	 * @param processorSupplier the {@link Processor} {@link Supplier} to call, subscribe to this {@link Flux} and
	 * share.
	 *
	 * @return a new {@link ConnectableFlux} whose values are broadcasted to supported subscribers once connected via {@link Processor}
	 *
	 * @since 2.5
	 */
	public final ConnectableFlux<T> multicast(
			Supplier<? extends Processor<? super T, ? extends T>> processorSupplier) {
		return multicast(processorSupplier, Function.identity());
	}

	/**
	 * Prepare a
	 * {@link ConnectableFlux} which subscribes this {@link Flux} sequence to the given {@link Processor}.
	 * The {@link Processor} will be itself subscribed by child {@link Subscriber} when {@link ConnectableFlux#connect()}
	 *  is invoked manually or automatically via {@link ConnectableFlux#autoConnect} and {@link ConnectableFlux#refCount}.
	 *  Note that some {@link Processor} do not support multi-subscribe, multicast is non opinionated in fact and
	 *  focuses on subscribe lifecycle.
	 *
	 * This will effectively turn any type of sequence into a hot sequence by sharing a single {@link Subscription}.
	 * <p> The {@link Processor} will not be specifically reusable and multi-connect might not work as expected
	 * depending on the {@link Processor}.
	 *
	 * <p> The selector will be applied once per {@link Subscriber} and can be used to blackbox pre-processing.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/multicastp.png" alt="">
	 *
	 * @param processor the {@link Processor} reference to subscribe to this {@link Flux} and share.
	 * @param selector a {@link Function} receiving a {@link Flux} derived from the supplied {@link Processor} and
	 * returning the end {@link Publisher} subscribed by a unique {@link Subscriber}
	 * @param <U> produced type from the given selector
	 *
	 * @return a new {@link ConnectableFlux} whose values are broadcasted to supported subscribers once connected via {@link Processor}
	 *
	 * @since 2.5
	 */
	public final <U> ConnectableFlux<U> multicast(final Processor<? super T, ? extends T>
			processor, Function<Flux<T>, ? extends Publisher<? extends U>> selector) {
		return multicast(() -> processor, selector);
	}

	 /**
	 * Prepare a
	 * {@link ConnectableFlux} which subscribes this {@link Flux} sequence to a supplied {@link Processor}
	 * when
	 * {@link ConnectableFlux#connect()} is invoked manually or automatically via {@link ConnectableFlux#autoConnect} and {@link ConnectableFlux#refCount}.
	 * The {@link Processor} will be itself subscribed by child {@link Subscriber}.
	 *  Note that some {@link Processor} do not support multi-subscribe, multicast is non opinionated in fact and
	 *  focuses on subscribe lifecycle.
	 *
	 * This will effectively turn any type of sequence into a hot sequence by sharing a single {@link Subscription}.
	 * <p> The selector will be applied once per {@link Subscriber} and can be used to blackbox pre-processing.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/multicastp.png" alt="">
	 *
	 * @param processorSupplier the {@link Processor} {@link Supplier} to call, subscribe to this {@link Flux} and
	 * share.
	 * @param selector a {@link Function} receiving a {@link Flux} derived from the supplied {@link Processor} and
	 * returning the end {@link Publisher} subscribed by a unique {@link Subscriber}
	 * @param <U> produced type from the given selector
	 *
	 * @return a new {@link ConnectableFlux} whose values are broadcasted to supported subscribers once connected via {@link Processor}
	 *
	 * @since 2.5
	 */
	public final <U> ConnectableFlux<U> multicast(Supplier<? extends Processor<? super T, ? extends T>>
			processorSupplier, Function<Flux<T>, ? extends Publisher<? extends U>> selector) {
		return new FluxMulticast<>(this, processorSupplier, selector);
	}
	
	/**
	 * Emit only the first item emitted by this {@link Flux}.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/next.png" alt="">
	 * <p>
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
	 * <p>
	 * @param fallback the {@link Function} mapping the error to a new {@link Publisher} sequence
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
	 * <p>
	 * @param fallbackValue alternate value on fallback
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> onErrorReturn(final T fallbackValue) {
		return switchOnError(just(fallbackValue));
	}



	/**
	 * Prepare a {@link ConnectableFlux} which shares this {@link Flux} sequence and dispatches values to
	 * subscribers in a backpressure-aware manner. Prefetch will default to {@link PlatformDependent#SMALL_BUFFER_SIZE}.
	 * This will effectively turn any type of sequence into a hot sequence.
	 * <p>
	 * Backpressure will be coordinated on {@link Subscription#request} and if any {@link Subscriber} is missing
	 * demand (requested = 0), multicast will pause pushing/pulling.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/publish.png" alt="">
	 *
	 * @return a new {@link ConnectableFlux}
	 */
	public final ConnectableFlux<T> publish() {
		return publish(PlatformDependent.SMALL_BUFFER_SIZE);
	}

	/**
	 * Prepare a {@link ConnectableFlux} which shares this {@link Flux} sequence and dispatches values to
	 * subscribers in a backpressure-aware manner. This will effectively turn any type of sequence into a hot sequence.
	 * <p>
	 * Backpressure will be coordinated on {@link Subscription#request} and if any {@link Subscriber} is missing
	 * demand (requested = 0), multicast will pause pushing/pulling.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/publish.png" alt="">
	 *
	 * @param prefetch bounded requested demand
	 *
	 * @return a new {@link ConnectableFlux}
	 */
	public final ConnectableFlux<T> publish(int prefetch) {
		return new FluxPublish<>(this, prefetch, QueueSupplier.<T>get(prefetch));
	}


	/**
	 * Run subscribe, onSubscribe and request on a supplied
	 * {@link Consumer} {@link Runnable} factory like {@link SchedulerGroup}.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/publishon.png" alt="">
	 * <p>
	 * <p>
	 * Typically used for slow publisher e.g., blocking IO, fast consumer(s) scenarios.
	 * It naturally combines with {@link SchedulerGroup#io} which implements work-queue thread dispatching.
	 *
	 * <p>
	 * {@code flux.publishOn(WorkQueueProcessor.create()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param schedulerFactory a checked factory for {@link Consumer} of {@link Runnable}
	 *
	 * @return a {@link Flux} publishing asynchronously
	 */
	public final Flux<T> publishOn(Callable<? extends Consumer<Runnable>> schedulerFactory) {
		return publishOn(this, schedulerFactory);
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Stream} blocking on next calls.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/tostream.png" alt="">
	 *
	 * @return a {@link Stream} of unknown size with onClose attached to {@link Subscription#cancel()}
	 */
	public Stream<T> stream() {
		return stream(getCapacity() == -1L ? Long.MAX_VALUE : getCapacity());
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Stream} blocking on next calls.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/tostream.png" alt="">
	 *
	 * @return a {@link Stream} of unknown size with onClose attached to {@link Subscription#cancel()}
	 */
	public Stream<T> stream(long batchSize) {
		final Supplier<Queue<T>> provider;
		provider = QueueSupplier.get(batchSize);
		return new BlockingIterable<>(this, batchSize, provider).stream();
	}

	/**
	 * Start the chain and request unbounded demand.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/unbounded.png" alt="">
	 * <p>
	 *
	 * @return a {@link Runnable} task to execute to dispose and cancel the underlying {@link Subscription}
	 */
	public final Runnable subscribe() {
		ConsumerSubscriber<T> s = new ConsumerSubscriber<>();
		subscribe(s);
		return s;
	}

	/**
	 *
	 * A chaining {@link Publisher#subscribe(Subscriber)} alternative to inline composition type conversion to a hot
	 * emitter (e.g. reactor FluxProcessor Broadcaster and Promise or rxjava Subject).
	 *
	 * {@code flux.subscribeWith(WorkQueueProcessor.create()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param subscriber the {@link Subscriber} to subscribe and return
	 * @param <E> the reified type from the input/output subscriber
	 *
	 * @return the passed {@link Subscriber}
	 */
	public final <E extends Subscriber<? super T>> E subscribeWith(E subscriber) {
		subscribe(subscriber);
		return subscriber;
	}

	/**
	 * Subscribe to the given fallback {@link Publisher} if an error is observed on this {@link Flux}
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/switchonerror.png" alt="">
	 * <p>
	 *
	 * @param fallback the alternate {@link Publisher}
	 *
	 * @return an alternating {@link Flux} on source onError
	 */
	public final Flux<T> switchOnError(final Publisher<? extends T> fallback) {
		return onErrorResumeWith(FluxResume.create(fallback));
	}

	/**
	 * Provide an alternative if this sequence is completed without any data
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/switchifempty.png" alt="">
	 * <p>
	 * @param alternate the alternate publisher if this sequence is empty
	 *
	 * @return an alternating {@link Flux} on source onComplete without elements
	 */
	public final Flux<T> switchIfEmpty(Publisher<? extends T> alternate) {
		return new FluxSwitchIfEmpty<>(this, alternate);
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Iterable} blocking on next calls.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/toiterable.png" alt="">
	 * <p>
	 *
	 * @return a blocking {@link Iterable}
	 */
	public final Iterable<T> toIterable() {
		return toIterable(getCapacity() == -1L ? Long.MAX_VALUE : getCapacity());
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Iterable} blocking on next calls.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/toiterablen.png" alt="">
	 * <p>
	 *
	 * @return a blocking {@link Iterable}
	 */
	public final Iterable<T> toIterable(long batchSize) {
		return toIterable(batchSize, null);
	}

	/**
	 * Transform this {@link Flux} into a lazy {@link Iterable} blocking on next calls.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/toiterablen.png" alt="">
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
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations of the
	 * most recent items emitted by each source until any of them completes. Errors will immediately be forwarded.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zipt.png" alt="">
	 * <p>
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param <R> type of the value from source2
	 *
	 * @return a zipped {@link Flux}
	 */
	public final <R> Flux<Tuple2<T, R>> zipWith(Publisher<? extends R> source2) {
		return zip(this, source2);
	}

	/**
	 * "Step-Merge" especially useful in Scatter-Gather scenarios. The operator will forward all combinations
	 * produced by the passed combinator from the most recent items emitted by each source until any of them
	 * completes. Errors will immediately be forwarded.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png" alt="">
	 * <p>
	 * @param source2 The second upstream {@link Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the value
	 * to signal downstream
	 * @param <R> type of the value from source2
	 * @param <V> The produced output after transformation by the combinator
	 *
	 * @return a zipped {@link Flux}
	 */
	public final <R, V> Flux<V> zipWith(Publisher<? extends R> source2,
			final BiFunction<? super T, ? super R, ? extends V> combinator) {
		return zip(this, source2, combinator);

	}

//	 ==============================================================================================================
//	 Containers
//	 ==============================================================================================================
	/**
	 * Decorate a {@link Flux} with a capacity for downstream accessors
	 *
	 * @param <I>
	 */
	final static class FluxBounded<I> extends FluxSource<I, I> {

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
}
