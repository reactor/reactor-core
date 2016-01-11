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

package reactor;

import java.util.Iterator;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.processor.ProcessorGroup;
import reactor.core.publisher.FluxAmb;
import reactor.core.publisher.FluxArray;
import reactor.core.publisher.FluxDefaultIfEmpty;
import reactor.core.publisher.FluxFactory;
import reactor.core.publisher.FluxFlatMap;
import reactor.core.publisher.FluxJust;
import reactor.core.publisher.FluxLift;
import reactor.core.publisher.FluxLog;
import reactor.core.publisher.FluxMap;
import reactor.core.publisher.FluxMapSignal;
import reactor.core.publisher.FluxNever;
import reactor.core.publisher.FluxPeek;
import reactor.core.publisher.FluxResume;
import reactor.core.publisher.FluxSession;
import reactor.core.publisher.FluxZip;
import reactor.core.publisher.ForEachSequencer;
import reactor.core.publisher.MonoIgnoreElements;
import reactor.core.publisher.MonoNext;
import reactor.core.publisher.convert.DependencyUtils;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
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

/**
 * A Reactive Streams {@link Publisher} with basic rx operators that emits 0 to N elements, and then completes
 * (successfully or with an error).
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
public abstract class Flux<T> implements Publisher<T> {

	/**
	 * ==============================================================================================================
	 * ==============================================================================================================
	 * <p>
	 * Static Generators
	 * <p>
	 * ==============================================================================================================
	 * ==============================================================================================================
	 */

	static final IdentityFunction IDENTITY_FUNCTION = new IdentityFunction();
	static final Flux<?>          EMPTY             = Mono.empty()
	                                                      .flux();

	/**
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive Fluxs publisher ready to be subscribed
	 */
	@SuppressWarnings({"unchecked", "varargs"})
	@SafeVarargs
	public static <I> Flux<I> amb(Publisher<? extends I>... sources) {
		return new FluxAmb<>(sources);
	}

	/**
	 * @param sources
	 * @param <I>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <I> Flux<I> amb(Iterable<? extends Publisher<? extends I>> sources) {
		if (sources == null) {
			return empty();
		}

		return new FluxAmb<>(sources);
	}

	/**
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive Fluxs publisher ready to be subscribed
	 */
	public static <I> Flux<I> concat(Publisher<? extends Publisher<? extends I>> source) {
		return new FluxFlatMap<>(source, 1, 32);
	}

	/**
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive Fluxs publisher ready to be subscribed
	 */
	public static <I> Flux<I> concat(Iterable<? extends Publisher<? extends I>> source) {
		return concat(fromIterable(source));
	}

	/**
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive Fluxs publisher ready to be subscribed
	 */
	@SafeVarargs
	@SuppressWarnings({"unchecked", "varargs"})
	public static <I> Flux<I> concat(Publisher<? extends I>... sources) {
		if (sources == null || sources.length == 0) {
			return empty();
		}
		if (sources.length == 1) {
			return from((Publisher<I>) sources[0]);
		}
		return concat(fromArray(sources));
	}

	/**
	 * @param source
	 * @param <IN>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <IN> Flux<IN> convert(Object source) {

		if (Publisher.class.isAssignableFrom(source.getClass())) {
			return from((Publisher<IN>) source);
		}
		else if (Iterable.class.isAssignableFrom(source.getClass())) {
			return fromIterable((Iterable<IN>) source);
		}
		else if (Iterator.class.isAssignableFrom(source.getClass())) {
			return fromIterator((Iterator<IN>) source);
		}
		else {
			return (Flux<IN>) from(DependencyUtils.convertToPublisher(source));
		}
	}

	/**
	 * Create a {@link Publisher} reacting on each available {@link Subscriber} read derived with the passed {@link
	 * Consumer}. If a previous request is still running, avoid recursion and extend the previous request iterations.
	 *
	 * @param requestConsumer A {@link Consumer} invoked when available read with the target subscriber
	 * @param <T> The type of the data sequence
	 *
	 * @return a fresh Reactive Fluxs publisher ready to be subscribed
	 */
	public static <T> Flux<T> create(Consumer<SubscriberWithContext<T, Void>> requestConsumer) {
		return create(requestConsumer, null, null);
	}

	/**
	 * Create a {@link Publisher} reacting on each available {@link Subscriber} read derived with the passed {@link
	 * Consumer}. If a previous request is still running, avoid recursion and extend the previous request iterations.
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every
	 * request calls.
	 *
	 * @param requestConsumer A {@link Consumer} invoked when available read with the target subscriber
	 * @param contextFactory A {@link Function} called for every new subscriber returning an immutable context (IO
	 * connection...)
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 *
	 * @return a fresh Reactive Fluxs publisher ready to be subscribed
	 */
	public static <T, C> Flux<T> create(Consumer<SubscriberWithContext<T, C>> requestConsumer,
			Function<Subscriber<? super T>, C> contextFactory) {
		return create(requestConsumer, contextFactory, null);
	}

	/**
	 * Create a {@link Publisher} reacting on each available {@link Subscriber} read derived with the passed {@link
	 * Consumer}. If a previous request is still running, avoid recursion and extend the previous request iterations.
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every
	 * request calls. The argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel,
	 * onComplete, onError).
	 *
	 * @param requestConsumer A {@link Consumer} invoked when available read with the target subscriber
	 * @param contextFactory A {@link Function} called once for every new subscriber returning an immutable context (IO
	 * connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(),
	 * onError()
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 *
	 * @return a fresh Reactive Fluxs publisher ready to be subscribed
	 */
	public static <T, C> Flux<T> create(final Consumer<SubscriberWithContext<T, C>> requestConsumer,
			Function<Subscriber<? super T>, C> contextFactory,
			Consumer<C> shutdownConsumer) {
		return FluxFactory.create(requestConsumer, contextFactory, shutdownConsumer);
	}

	/**
	 * Create a {@link Flux} that completes without emitting any item.
	 *
	 * @param <T>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> empty() {
		return (Flux<T>) EMPTY;
	}

	/**
	 * Create a {@link Flux} that completes with the specified error.
	 *
	 * @param error
	 * @param <T>
	 *
	 * @return
	 */
	public static <T> Flux<T> error(Throwable error) {
		return Mono.<T>error(error).flux();
	}


	/**
	 * Expose the specified {@link Publisher} with the {@link Flux} API.
	 *
	 * @param source
	 * @param <T>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> from(Publisher<? extends T> source) {
		if (Flux.class.isAssignableFrom(source.getClass())) {
			return (Flux<T>) source;
		}

		if (Supplier.class.isAssignableFrom(source.getClass())) {
			T t = ((Supplier<T>) source).get();
			if (t != null) {
				return just(t);
			}
		}
		return new FluxBarrier<>(source);
	}

	/**
	 * Create a {@link Flux} that emits the items contained in the provided {@link Iterable}.
	 *
	 * @param array
	 * @param <T>
	 *
	 * @return
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
	 *
	 * @param it
	 * @param <T>
	 *
	 * @return
	 */
	public static <T> Flux<T> fromIterable(Iterable<? extends T> it) {
		ForEachSequencer.IterableSequencer<T> iterablePublisher = new ForEachSequencer.IterableSequencer<>(it);
		return create(iterablePublisher, iterablePublisher);
	}

	/**
	 * @param defaultValues
	 * @param <T>
	 *
	 * @return
	 */
	public static <T> Flux<T> fromIterator(final Iterator<? extends T> defaultValues) {
		if (defaultValues == null || !defaultValues.hasNext()) {
			return empty();
		}
		ForEachSequencer.IteratorSequencer<T> iteratorPublisher =
				new ForEachSequencer.IteratorSequencer<>(defaultValues);
		return create(iteratorPublisher, iteratorPublisher);
	}

	/**
	 * Create a {@link Publisher} reacting on requests with the passed {@link BiConsumer}. The argument {@code
	 * contextFactory} is executed once by new subscriber to generate a context shared by every request calls. The
	 * argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel, onComplete,
	 * onError).
	 *
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory A {@link Function} called once for every new subscriber returning an immutable context (IO
	 * connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(),
	 * onError()
	 * @param <T> The type of the data sequence
	 * @param <C> The type of contextual information to be read by the requestConsumer
	 *
	 * @return a fresh Reactive Fluxs publisher ready to be subscribed
	 */
	public static <T, C> Flux<T> generate(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
			Function<Subscriber<? super T>, C> contextFactory,
			Consumer<C> shutdownConsumer) {

		return FluxFactory.generate(requestConsumer, contextFactory, shutdownConsumer);
	}

	/**
	 * Create a new {@link Flux} that emits the specified item.
	 *
	 * @param data
	 * @param <T>
	 *
	 * @return
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <T> Flux<T> just(T... data) {
		return fromArray(data);
	}

	/**
	 * @param data
	 * @param <T>
	 *
	 * @return
	 */
	public static <T> Flux<T> just(T data) {
		return new FluxJust<>(data);
	}

	/**
	 * @param source
	 * @param <T>
	 *
	 * @return
	 */
	public static <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source) {
		return new FluxFlatMap<>(source, ReactiveState.SMALL_BUFFER_SIZE, 32);
	}

	/**
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive Fluxs publisher ready to be subscribed
	 */
	public static <I> Flux<I> merge(Iterable<? extends Publisher<? extends I>> sources) {
		return merge(fromIterable(sources));
	}

	/**
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive Fluxs publisher ready to be subscribed
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
	 *
	 * @param <T>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> never() {
		return FluxNever.instance();
	}

	/**
	 * Create a {@link Flux} reacting on subscribe with the passed {@link Consumer}. The argument {@code
	 * sessionConsumer} is executed once by new subscriber to generate a {@link ReactiveSession} context ready to accept
	 * signals.
	 *
	 * @param sessionConsumer A {@link Consumer} called once everytime a subscriber subscribes
	 * @param <T> The type of the data sequence
	 *
	 * @return a fresh Reactive Fluxs publisher ready to be subscribed
	 */
	public static <T> Flux<T> yield(Consumer<? super ReactiveSession<T>> sessionConsumer) {
		return new FluxSession<>(sessionConsumer);
	}

	/**
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
				(Function<Tuple2<T1, T2>, Tuple2<T1, T2>>) IDENTITY_FUNCTION,
				ReactiveState.XS_BUFFER_SIZE);
	}

	/**
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
		}, ReactiveState.XS_BUFFER_SIZE);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
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
		return new FluxZip<>(new Publisher[]{source1, source2, source3}, combinator, ReactiveState.XS_BUFFER_SIZE);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
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
		return new FluxZip<>(new Publisher[]{source1, source2, source3, source4}, combinator, ReactiveState.XS_BUFFER_SIZE);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
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
		return new FluxZip<>(new Publisher[]{source1, source2, source3, source4, source5}, combinator, ReactiveState
				.XS_BUFFER_SIZE);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
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
				ReactiveState
				.XS_BUFFER_SIZE);
	}

	/**
	 * Build a {@literal Flux} whose data are generated by the passed publishers.
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
			final Function<Tuple, O> combinator) {

		if (sources == null) {
			return empty();
		}

		return new FluxZip<>(sources, combinator, ReactiveState.XS_BUFFER_SIZE);
	}

	/**
	 * @param sources
	 * @param combinator
	 * @param <O>
	 *
	 * @return
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <I, O> Flux<O> zip(
			final Function<Tuple, O> combinator, Publisher<? extends I>... sources) {

		if (sources == null) {
			return empty();
		}

		return new FluxZip<>(sources, combinator, ReactiveState.XS_BUFFER_SIZE);
	}

	/**
	 * ==============================================================================================================
	 * ==============================================================================================================
	 * <p>
	 * Instance Operators
	 * <p>
	 * ==============================================================================================================
	 * ==============================================================================================================
	 */
	protected Flux() {
	}

	/**
	 *
	 * {@code flux.to(Mono::from).subscribe(Subscribers.unbounded()) }
	 *
	 * @param transfomer
	 * @param <P>
	 *
	 * @return
	 */
	public final <V, P extends Publisher<V>> P as(Function<? super Flux<T>, P> transfomer) {
		return transfomer.apply(this);
	}

	/**
	 * Return a {@code Mono<Void>} that completes when this {@link Flux} completes.
	 *
	 * @return
	 */
	public final Mono<Void> after() {
		return new MonoIgnoreElements<>(this);
	}

	/**
	 * Emit from the fastest first sequence between this publisher and the given publisher
	 *
	 * @return the fastest sequence
	 */
	public final Flux<T> ambWith(Publisher<? extends T> other) {
		return amb(this, other);
	}

	/**
	 * @param capacity
	 *
	 * @return
	 */
	public final Flux<T> capacity(long capacity) {
		return new FluxBounded<>(this, capacity);
	}

	/**
	 * Like {@link #flatMap(Function)}, but concatenate emissions instead of merging (no interleave).
	 *
	 * @param mapper
	 * @param <R>
	 *
	 * @return
	 */
	public final <R> Flux<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return new FluxFlatMap<>(this, mapper, 1, 32);
	}

	/**
	 * Concatenate emissions of this {@link Flux} with the provided {@link Publisher} (no interleave). TODO Varargs ?
	 *
	 * @param source
	 *
	 * @return
	 */
	public final Flux<T> concatWith(Publisher<? extends T> source) {
		return concat(this, source);
	}

	/**
	 * @param to
	 * @param <TO>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <TO> TO convert(Class<TO> to) {
		if (Publisher.class.isAssignableFrom(to.getClass())) {
			return (TO) this;
		}
		else {
			return DependencyUtils.convertFromPublisher(this, to);
		}
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
	 *
	 * @param defaultV the alternate value if this sequence is empty
	 *
	 * @return a new {@link Flux}
	 */
	public final Flux<T> defaultIfEmpty(T defaultV) {
		return new FluxDefaultIfEmpty<>(this, defaultV);
	}

	/**
	 * {@code flux.dispatchOn(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param group
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final Flux<T> dispatchOn(ProcessorGroup group) {
		return new FluxProcessorGroup<>(this, false, ((ProcessorGroup<T>) group));
	}

	/**
	 * Triggered when the {@link Flux} is cancelled.
	 *
	 * @param onCancel
	 *
	 * @return
	 */
	public final Flux<T> doOnCancel(Runnable onCancel) {
		return new FluxPeek<>(this, null, null, null, null, null, null, onCancel);
	}

	/**
	 * Triggered when the {@link Flux} completes successfully.
	 *
	 * @param onComplete
	 *
	 * @return
	 */
	public final Flux<T> doOnComplete(Runnable onComplete) {
		return new FluxPeek<>(this, null, null, null, onComplete, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} completes with an error.
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
	 *
	 * @param onNext
	 *
	 * @return
	 */
	public final Flux<T> doOnNext(Consumer<? super T> onNext) {
		return new FluxPeek<>(this, null, onNext, null, null, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} is subscribed.
	 *
	 * @param onSubscribe
	 *
	 * @return
	 */
	public final Flux<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		return new FluxPeek<>(this, onSubscribe, null, null, null, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} terminates, either by completing successfully or with an error.
	 *
	 * @param onTerminate
	 *
	 * @return
	 */
	public final Flux<T> doOnTerminate(Runnable onTerminate) {
		return new FluxPeek<>(this, null, null, null, null, onTerminate, null, null);
	}

	/**
	 * Transform the items emitted by this {@link Flux} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 *
	 * @param mapper
	 * @param <R>
	 *
	 * @return
	 */
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return new FluxFlatMap<>(this, mapper, ReactiveState.SMALL_BUFFER_SIZE, 32);
	}

	/**
	 * Transform the signals emitted by this {@link Flux} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 *
	 * @param mapperOnNext
	 * @param mapperOnError
	 * @param mapperOnComplete
	 * @param <R>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapperOnNext,
			Function<Throwable, ? extends Publisher<? extends R>> mapperOnError,
			Supplier<? extends Publisher<? extends R>> mapperOnComplete) {
		return new FluxFlatMap<>(
				new FluxMapSignal<>(this, mapperOnNext, mapperOnError, mapperOnComplete),
				IDENTITY_FUNCTION,
				ReactiveState.SMALL_BUFFER_SIZE, 32);
	}

	/**
	 * Create a {@link Flux} intercepting all source signals with the returned Subscriber that might choose to pass them
	 * alone to the provided Subscriber (given to the returned {@code subscribe(Subscriber)}.
	 *
	 * @param operator
	 * @param <R>
	 *
	 * @return
	 */
	public final <R> Flux<R> lift(Function<Subscriber<? super R>, Subscriber<? super T>> operator) {
		return new FluxLift<>(this, operator);
	}

	/**
	 * @return
	 */
	public final Flux<T> log() {
		return log(null, Level.INFO, FluxLog.ALL);
	}

	/**
	 * @param category
	 *
	 * @return
	 */
	public final Flux<T> log(String category) {
		return log(category, Level.INFO, FluxLog.ALL);
	}

	/**
	 * @param category
	 * @param level
	 *
	 * @return
	 */
	public final Flux<T> log(String category, Level level) {
		return log(category, level, FluxLog.ALL);
	}

	/**
	 * @param category
	 * @param level
	 * @param options
	 *
	 * @return
	 */
	public final Flux<T> log(String category, Level level, int options) {
		return new FluxLog<>(this, category, level, options);
	}

	/**
	 * Transform the items emitted by this {@link Flux} by applying a function to each item.
	 *
	 * @param mapper
	 * @param <R>
	 *
	 * @return
	 */
	public final <R> Flux<R> map(Function<? super T, ? extends R> mapper) {
		return new FluxMap<>(this, mapper);
	}

	/**
	 * Merge emissions of this {@link Flux} with the provided {@link Publisher}, so that they may interleave.
	 *
	 * @param source
	 *
	 * @return
	 */
	public final Flux<T> mergeWith(Publisher<? extends T> source) {
		return merge(just(this, source));
	}

	/**
	 * Emit only the first item emitted by this {@link Flux}.
	 *
	 * If the sequence emits more than 1 data, emit {@link ArrayIndexOutOfBoundsException}.
	 *
	 * @return
	 */
	public final Mono<T> next() {
		return new MonoNext<>(this);
	}

	/**
	 * Subscribe to a returned fallback publisher when any error occurs.
	 *
	 * @param fallback
	 *
	 * @return
	 */
	public final Flux<T> onErrorResumeWith(Function<Throwable, ? extends Publisher<? extends T>> fallback) {
		return new FluxResume<>(this, fallback);
	}

	/**
	 * @param fallbackValue
	 *
	 * @return
	 */
	public final Flux<T> onErrorReturn(final T fallbackValue) {
		return switchOnError(just(fallbackValue));
	}

	/**
	 *
	 * {@code flux.to(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param subscriber
	 * @param <E>
	 *
	 * @return
	 */
	public final <E extends Subscriber<? super T>> E to(E subscriber) {
		subscribe(subscriber);
		return subscriber;
	}

	/**
	 * <p>
	 * {@code flux.publishOn(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param group
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final Flux<T> publishOn(ProcessorGroup group) {
		return new FluxProcessorGroup<>(this, true, ((ProcessorGroup<T>) group));
	}

	/**
	 * @param fallback
	 *
	 * @return
	 */
	public final Flux<T> switchOnError(final Publisher<? extends T> fallback) {
		return onErrorResumeWith(new Function<Throwable, Publisher<? extends T>>() {
			@Override
			public Publisher<? extends T> apply(Throwable throwable) {
				return fallback;
			}
		});
	}

	/**
	 * Start the chain and request unbounded demand.
	 */
	public final void subscribe() {
		subscribe(Subscribers.unbounded());
	}

	/**
	 * Combine the emissions of multiple Publishers together via a specified function and emit single items for each
	 * combination based on the results of this function.
	 *
	 * @param source2
	 * @param zipper
	 * @param <R>
	 * @param <V>
	 *
	 * @return
	 */
	public final <R, V> Flux<V> zipWith(Publisher<? extends R> source2,
			final BiFunction<? super T, ? super R, ? extends V> zipper) {

		return new FluxZip<>(new Publisher[]{this, source2}, new Function<Tuple2<T, R>, V>() {
			@Override
			public V apply(Tuple2<T, R> tuple) {
				return zipper.apply(tuple.getT1(), tuple.getT2());
			}
		}, ReactiveState.XS_BUFFER_SIZE);

	}

	/**
	 * ==============================================================================================================
	 * ==============================================================================================================
	 *
	 * Containers
	 *
	 * ==============================================================================================================
	 * ==============================================================================================================
	 */

	/**
	 * A marker interface for components responsible for augmenting subscribers with features like {@link #lift}
	 *
	 * @param <I>
	 * @param <O>
	 */
	public interface Operator<I, O>
			extends Function<Subscriber<? super O>, Subscriber<? super I>>, ReactiveState.Factory {

	}

	/**
	 * A connecting Flux Publisher (right-to-left from a composition chain perspective)
	 *
	 * @param <I>
	 * @param <O>
	 */
	public static class FluxBarrier<I, O> extends Flux<O>
			implements ReactiveState.Factory, ReactiveState.Bounded, ReactiveState.Named, ReactiveState.Upstream {

		protected final Publisher<? extends I> source;

		public FluxBarrier(Publisher<? extends I> source) {
			this.source = source;
		}

		@Override
		public long getCapacity() {
			return Bounded.class.isAssignableFrom(source.getClass()) ? ((Bounded) source).getCapacity() :
					Long.MAX_VALUE;
		}

		@Override
		public String getName() {
			return getClass().getSimpleName().replaceAll("Flux|Operator", "");
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

	static final class FluxProcessorGroup<I> extends FluxBarrier<I, I> implements ReactiveState.FeedbackLoop{

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
		public Object delegateInput() {
			return processor;
		}

		@Override
		public Object delegateOutput() {
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
