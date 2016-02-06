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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.queue.QueueSupplier;
import reactor.core.state.Backpressurable;
import reactor.core.state.Introspectable;
import reactor.core.subscriber.ConsumerSubscriber;
import reactor.core.timer.Timer;
import reactor.core.util.Assert;
import reactor.core.util.Logger;
import reactor.core.util.PlatformDependent;
import reactor.core.util.ReactiveStateUtils;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.fn.tuple.Tuple3;
import reactor.fn.tuple.Tuple4;
import reactor.fn.tuple.Tuple5;
import reactor.fn.tuple.Tuple6;

/**
 * A Reactive Streams {@link Publisher} with basic rx operators that completes successfully by emitting an element, or
 * with an error.
 *
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/mono.png" alt="">
 * <p>
 *
 * <p>The rx operators will offer aliases for input {@link Mono} type to preserve the "at most one"
 * property of the resulting {@link Mono}. For instance {@link Mono#flatMap flatMap} returns a {@link Flux} with 
 * possibly
 * more than 1 emission. Its alternative enforcing {@link Mono} input is {@link Mono#then then}.
 *
 * <p>{@code Mono<Void>} should be used for {@link Publisher} that just completes without any value.
 *
 * <p>It is intended to be used in implementations and return types, input parameters should keep using raw {@link
 * Publisher} as much as possible.
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @see Flux
 * @since 2.5
 */
public abstract class Mono<T> implements Publisher<T>, Backpressurable, Introspectable {

	static final Mono<?> NEVER = MonoSource.from(FluxNever.instance());

//	 ==============================================================================================================
//	 Static Generators
//	 ==============================================================================================================

	/**
	 * Pick the first result coming from any of the given monos and populate a new {@literal Mono}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/any.png" alt="">
	 * <p>
	 * @param monos The deferred monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <T> Mono<T> any(Mono<? extends T>... monos) {
		return MonoSource.wrap(new FluxAmb<>(monos));
	}

	/**
	 * Pick the first result coming from any of the given monos and populate a new {@literal Mono}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/any.png" alt="">
	 * <p>
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	public static <T> Mono<T> any(Iterable<? extends Mono<? extends T>> monos) {
		return MonoSource.wrap(new FluxAmb<>(monos));
	}

	/**
	 * Create a {@link Mono} provider that will {@link Supplier#get supply} a target {@link Mono} to subscribe to for
	 * each {@link Subscriber} downstream.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/defer1.png" alt="">
	 * <p>
	 * @param supplier a {@link Mono} factory
	 *
	 * @return a new {@link Mono} factory
	 */
	public static <T> Mono<T> defer(Supplier<? extends Mono<? extends T>> supplier) {
		return new MonoDefer<>(supplier);
	}

	/**
	 * Create a Mono which delays an onNext signal of {@code duration} seconds and complete.
	 * If the demand cannot be produced in time, an onError will be signalled instead.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/delay.png" alt="">
	 * <p>
	 * @param duration in seconds
	 *
	 * @return a new {@link Mono}
	 */
	public static Mono<Long> delay(long duration) {
		return delay(duration, TimeUnit.SECONDS);
	}

	/**
	 * Create a Mono which delays an onNext signal of {@code duration} of given unit and complete on the global timer.
	 * If the demand cannot be produced in time, an onError will be signalled instead.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/delay.png" alt="">
	 * <p>
	 * @param duration in unit of time
	 * @param unit the time unit
	 *
	 * @return a new {@link Mono}
	 */
	public static Mono<Long> delay(long duration, TimeUnit unit) {
		return delay(duration, unit, Timer.global());
	}

	/**
	 * Create a Mono which delays an onNext signal of {@code duration} seconds and complete.
	 * If the demand cannot be produced in time, an onError will be signalled instead.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/delay.png" alt="">
	 * <p>
	 * @param duration in unit of time
	 * @param unit the time unit
	 * @param timer the timer
	 *
	 * @return a new {@link Mono}
	 */
	public static Mono<Long> delay(long duration, TimeUnit unit, Timer timer) {
		long timespan = TimeUnit.MILLISECONDS.convert(duration, unit);
		Assert.isTrue(timespan >= timer.period(), "The delay " + duration + "ms cannot be less than the timer resolution" +
				"" + timer.period() + "ms");
		return new MonoTimer(timer, duration, unit);
	}

	/**
	 * Create a {@link Mono} that completes without emitting any item.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/empty.png" alt="">
	 * <p>
	 * @param <T> the reified {@link Subscriber} type
	 *
	 * @return a completed {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<T> empty() {
		return (Mono<T>) MonoEmpty.instance();
	}

	/**
	 * Create a new {@link Mono} that ignores onNext (dropping them) and only react on Completion signal.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/after.png" alt="">
	 * <p>
	 * @param source the {@link Publisher to ignore}
	 * @param <T> the reified {@link Publisher} type
	 *
	 * @return a new completable {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<Void> empty(Publisher<T> source) {
		return (Mono<Void>)new MonoIgnoreElements<>(source);
	}

	/**
	 * Create a {@link Mono} that completes with the specified error immediately after onSubscribe.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/error.png" alt="">
	 * <p>
	 * @param error the onError signal
	 * @param <T> the reified {@link Subscriber} type
	 *
	 * @return a failed {@link Mono}
	 */
	public static <T> Mono<T> error(Throwable error) {
		return new MonoError<>(error);
	}

	/**
	 * Expose the specified {@link Publisher} with the {@link Mono} API, and ensure it will emit 0 or 1 item.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/from1.png" alt="">
	 * <p>
	 * @param source the {@link Publisher} source
	 * @param <T> the source type
	 *
	 * @return the next item emitted as a {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<T> from(Publisher<T> source) {
		if (source == null) {
			return empty();
		}
		if (source instanceof Mono) {
			return (Mono<T>) source;
		}
		return new MonoNext<>(source);
	}

	/**
	 * Create a {@link Mono} producing the value for the {@link Mono} using the given supplier.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromcallable.png" alt="">
	 * <p>
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T> type of the expected value
	 *
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> fromCallable(Callable<? extends T> supplier) {
		return new MonoCallable<>(supplier);
	}

	/**
	 * Create a {@link Mono} only producing a completion signal after using the given
	 * runnable.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromrunnable.png" alt="">
	 * <p>
	 * @param runnable {@link Runnable} that will callback the completion signal
	 *
	 * @return A {@link Mono}.
	 */
	public static Mono<Void> fromRunnable(Runnable runnable) {
		return MonoSource.wrap(new FluxPeek<>(MonoEmpty.<Void>instance(), null, null, null, runnable, null, null,
				null));
	}

	/**
	 * Create a new {@link Mono} that ignores onNext (dropping them) and only react on Completion signal.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/ignoreelements.png" alt="">
	 * <p>
	 * @param source the {@link Publisher to ignore}
	 * @param <T> the source type of the ignored data
	 *
	 * @return a new completable {@link Mono}.
	 */
	public static <T> Mono<T> ignoreElements(Publisher<T> source) {
		return new MonoIgnoreElements<>(source);
	}


	/**
	 * Create a new {@link Mono} that emits the specified item.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/just.png" alt="">
	 * <p>
	 * @param data the only item to onNext
	 * @param <T> the type of the produced item
	 *
	 * @return a {@link Mono}.
	 */
	public static <T> Mono<T> just(T data) {
		return new MonoJust<>(data);
	}

	/**
	 * Return a {@link Mono} that will never signal any data, error or completion signal.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/never.png" alt="">
	 * <p>
	 * @param <T> the {@link Subscriber} type target
	 *
	 * @return a never completing {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<T> never() {
		return (Mono<T>)NEVER;
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 *
	 * @return a {@link Mono}.
	 */
	public static <T1, T2> Mono<Tuple2<T1, T2>> when(Mono<? extends T1> p1, Mono<? extends T2> p2) {
		return when(Tuple.<T1, T2>fn2(), p1, p2);
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	public static <T1, T2, T3> Mono<Tuple3<T1, T2, T3>> when(Mono<? extends T1> p1, Mono<? extends T2> p2, Mono<? extends T3> p3) {
		return when(Tuple.<T1, T2, T3>fn3(), p1, p2, p3);
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	public static <T1, T2, T3, T4> Mono<Tuple4<T1, T2, T3, T4>> when(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4) {
		return when(Tuple.<T1, T2, T3, T4>fn4(), p1, p2, p3, p4);
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	public static <T1, T2, T3, T4, T5> Mono<Tuple5<T1, T2, T3, T4, T5>> when(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5) {
		return when(Tuple.<T1, T2, T3, T4, T5>fn5(), p1, p2, p3, p4, p5);
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	public static <T1, T2, T3, T4, T5, T6> Mono<Tuple6<T1, T2, T3, T4, T5, T6>> when(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5,
			Mono<? extends T6> p6) {
		return when(Tuple.<T1, T2, T3, T4, T5, T6> fn6(), p1, p2, p3, p4, p5, p6);
	}

	/**
	 * Aggregate given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono
	 * Monos} have been fulfilled.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
	 * <p>
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SafeVarargs
	@SuppressWarnings({"unchecked","varargs"})
	private static <T> Mono<T[]> when(Mono<? extends T>... monos) {
		return when(Flux.IDENTITY_FUNCTION, monos);
	}

	/**
	 * Aggregate given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono
	 * Monos} have been fulfilled.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/when.png" alt="">
	 * <p>
	 * @param combinator the combinator {@link Function}
	 * @param monos The monos to use.
	 * @param <T> The super incoming type
	 * @param <V> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SafeVarargs
	@SuppressWarnings({"varargs", "unchecked"})
	private static <T, V> Mono<V> when(Function<? super Object[], ? extends V> combinator, Mono<? extends T>... monos) {
		return MonoSource.wrap(new FluxZip<>(monos, combinator, QueueSupplier.<T>one(), 1));
	}

	/**
	 * Aggregate given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono
	 * Monos} have been fulfilled.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
	 * <p>
	 *
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<T[]> when(final Iterable<? extends Mono<? extends T>> monos) {
		return when(Flux.IDENTITY_FUNCTION, monos);
	}

	/**
	 * Aggregate given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono
	 * Monos} have been fulfilled.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/when.png" alt="">
	 * <p>
	 *
	 * @param combinator the combinator {@link Function}
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T, V> Mono<V> when(final Function<? super Object[], ? extends V> combinator, final Iterable<?
			extends Mono<? extends T>> monos) {
		return MonoSource.wrap(new FluxZip<>(monos, combinator, QueueSupplier.<T>one(), 1));
	}

//	 ==============================================================================================================
//	 Operators
//	 ==============================================================================================================
	protected Mono() {

	}

	/**
	 * Transform this {@link Mono} into a target {@link Publisher}
	 *
	 * {@code mono.as(Flux::from).subscribe(Subscribers.unbounded()) }
	 *
	 * @param transformer the {@link Function} applying this {@link Mono}
	 * @param <P> the returned {@link Publisher} output
	 *
	 * @return the transformed {@link Mono}
	 */
	public final <V, P extends Publisher<V>> P as(Function<? super Mono<T>, P> transformer) {
		return transformer.apply(this);
	}

	/**
	 * Combine the result from this mono and another into a {@link Tuple2}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/and.png" alt="">
	 * <p>
	 * @param other the {@link Mono} to combine with
	 *
	 * @return a new combined Mono
	 * @see #when
	 */
	public final <T2> Mono<Tuple2<T, T2>> and(Mono<? extends T2> other) {
		return when(this, other);
	}

	/**
	 * Return a {@code Mono<Void>} which only listens for complete and error signals from this {@link Mono} completes.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/after1.png" alt="">
	 * <p>
	 * @return a {@link Mono} igoring its payload (actively dropping)
	 */
	public final Mono<Void> after() {
		return empty(this);
	}

	/**
	 * Transform the terminal signal (error or completion) into {@code Mono<V>} will emit at most one result in the
	 * returned {@link Mono}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/afters1.png" alt="">
	 * <p>
	 * @return a new {@link Mono}
	 */
	public final <V> Mono<V> after(final Supplier<? extends Mono<V>> sourceSupplier) {
		return MonoSource.wrap(after().flatMap(null, new Function<Throwable, Publisher<? extends V>>() {
			@Override
			public Publisher<? extends V> apply(Throwable throwable) {
				return Flux.concat(sourceSupplier.get(), Mono.<V>error(throwable));
			}
		}, sourceSupplier));
	}

	/**
	 * Introspect this Mono graph
	 *
	 * @return {@literal ReactiveStateUtils.Graph} representation of a publisher graph
	 */
	public final ReactiveStateUtils.Graph debug() {
		return ReactiveStateUtils.scan(this);
	}


	/**
	 * Provide a default unique value if this mono is completed without any data
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/defaultifempty.png" alt="">
	 * <p>
	 * @param defaultV the alternate value if this sequence is empty
	 *
	 * @return a new {@link Mono}
	 *
	 * @see Flux#defaultIfEmpty(Object)
	 */
	public final Mono<T> defaultIfEmpty(T defaultV) {
		return MonoSource.wrap(new FluxSwitchIfEmpty<>(this, just(defaultV)));
	}

	/**
	 * Run onNext, onComplete and onError on a supplied {@link Function} worker like {@link SchedulerGroup}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/dispatchon1.png" alt="">
	 * <p> <p>
	 * Typically used for fast publisher, slow consumer(s) scenarios.
	 * It naturally combines with {@link SchedulerGroup#single} and {@link SchedulerGroup#async} which implement
	 * fast async event loops.
	 *
	 * {@code mono.dispatchOn(WorkQueueProcessor.create()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param schedulers a checked factory for {@link Consumer} of {@link Runnable}
	 *
	 * @return an asynchronous {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public final Mono<T> dispatchOn(Callable<? extends Consumer<Runnable>> schedulers) {
		return MonoSource.wrap(new FluxDispatchOn(this, schedulers, false, 1, QueueSupplier.<T>one()));
	}


	/**
	 * Triggered after the {@link Mono} terminates, either by completing downstream successfully or with an error.
	 * The arguments will be null depending on success, success with data and error:
	 * <ul>
	 *     <li>null, null : completed without data</li>
	 *     <li>T, null : completed with data</li>
	 *     <li>null, Throwable : failed with/without data</li>
	 * </ul>
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doafterterminate1.png" alt="">
	 * <p>
	 * @param afterTerminate the callback to call after {@link Subscriber#onNext}, {@link Subscriber#onComplete} without preceding {@link Subscriber#onNext} or {@link Subscriber#onError}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doAfterTerminate(BiConsumer<? super T, Throwable> afterTerminate) {
		return new MonoSuccess<>(this, null, null, afterTerminate);
	}

	/**
	 * Triggered when the {@link Mono} is cancelled.
	 *
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/dooncancel.png" alt="">
	 * <p>
	 * @param onCancel the callback to call on {@link Subscription#cancel()}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnCancel(Runnable onCancel) {
		return MonoSource.wrap(new FluxPeek<>(this, null, null, null, null, null, null, onCancel));
	}

	/**
	 * Triggered when the {@link Mono} completes successfully.
	 *
	 * <ul>
	 *     <li>null : completed without data</li>
	 *     <li>T: completed with data</li>
	 * </ul>
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonsuccess.png" alt="">
	 * <p>
	 * @param onSuccess the callback to call on
	 * {@link Subscriber#onNext} or {@link Subscriber#onComplete} without preceding {@link Subscriber#onNext}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnSuccess(Consumer<? super T> onSuccess) {
		return new MonoSuccess<>(this, onSuccess, null, null);
	}

	/**
	 * Triggered when the {@link Mono} completes with an error.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonerror1.png" alt="">
	 * <p>
	 * @param onError the error callback to call on {@link Subscriber#onError(Throwable)}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnError(Consumer<? super Throwable> onError) {
		return MonoSource.wrap(new FluxPeek<>(this, null, null, onError, null, null, null, null));
	}

	/**
	 * Triggered when the {@link Mono} is subscribed.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonsubscribe.png" alt="">
	 * <p>
	 * @param onSubscribe the callback to call on {@link Subscriber#onSubscribe(Subscription)}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		return MonoSource.wrap(new FluxPeek<>(this, onSubscribe, null, null, null, null, null, null));
	}

	/**
	 * Triggered when the {@link Mono} terminates, either by completing successfully or with an error.
	 *
	 * <ul>
	 *     <li>null, null : completing without data</li>
	 *     <li>T, null : completing with data</li>
	 *     <li>null, Throwable : failing with/without data</li>
	 * </ul>
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonterminate1.png" alt="">
	 * <p>
	 * @param onTerminate the callback to call {@link Subscriber#onNext}, {@link Subscriber#onComplete} without preceding {@link Subscriber#onNext} or {@link Subscriber#onError}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnTerminate(BiConsumer<? super T, Throwable> onTerminate) {
		return new MonoSuccess<>(this, null, onTerminate, null);
	}

	/**
	 * Transform the items emitted by a {@link Publisher} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmap1.png" alt="">
	 * <p>
	 * @param mapper the
	 * {@link Function} to produce a sequence of R from the the eventual passed {@link Subscriber#onNext}
	 * @param <R> the merged sequence type
	 *
	 * @return a new {@link Flux} as the sequence is not guaranteed to be single at most
	 */
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return new FluxFlatMap<>(
				this,
				mapper,
				false,
				Integer.MAX_VALUE,
				QueueSupplier.<R>small(),
				PlatformDependent.SMALL_BUFFER_SIZE,
				QueueSupplier.<R>xs()
		);
	}

	/**
	 * Transform the signals emitted by this {@link Flux} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmaps1.png" alt="">
	 * <p>
	 * @param mapperOnNext the {@link Function} to call on next data and returning a sequence to merge
	 * @param mapperOnError the {@link Function} to call on error signal and returning a sequence to merge
	 * @param mapperOnComplete the {@link Function} to call on complete signal and returning a sequence to merge
	 * @param <R> the type of the produced merged sequence
	 *
	 * @return a new {@link Flux} as the sequence is not guaranteed to be single at most
	 *
	 * @see Flux#flatMap(Function, Function, Supplier)
	 */
	@SuppressWarnings("unchecked")
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapperOnNext,
			Function<Throwable, ? extends Publisher<? extends R>> mapperOnError,
			Supplier<? extends Publisher<? extends R>> mapperOnComplete) {

		return new FluxFlatMap<>(
				new FluxMapSignal<>(this, mapperOnNext, mapperOnError, mapperOnComplete),
				Flux.IDENTITY_FUNCTION,
				false,
				PlatformDependent.SMALL_BUFFER_SIZE,
				QueueSupplier.<R>small(),
				PlatformDependent.XS_BUFFER_SIZE,
				QueueSupplier.<R>xs()
		);
	}

	/**
	 * Convert this {@link Mono} to a {@link Flux}
	 *
	 * @return a {@link Flux} variant of this {@link Mono}
	 */
	public final Flux<T> flux() {
		return FluxSource.wrap(this);
	}

	/**
	 * Block until a next signal is received, will return null if onComplete, T if onNext, throw a
	 * {@literal Exceptions.DownstreamException} if checked error or origin RuntimeException if unchecked.
	 * If the default timeout {@literal PlatformDependent#DEFAULT_TIMEOUT} has elapsed, a CancelException will be thrown.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/get.png" alt="">
	 * <p>
	 *
	 * @return T the result
	 */
	public T get() {
		return get(PlatformDependent.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block until a next signal is received, will return null if onComplete, T if onNext, throw a
	 * {@literal Exceptions.DownstreamException} if checked error or origin RuntimeException if unchecked.
	 * If the default timeout {@literal PlatformDependent#DEFAULT_TIMEOUT} has elapsed, a CancelException will be thrown.
	 *
	 * Note that each get() will subscribe a new single (MonoResult) subscriber, in other words, the result might
	 * miss signal from hot publishers.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/get.png" alt="">
	 * <p>
	 *
	 * @param timeout maximum time period to wait for before raising a {@literal reactor.core.util.Exceptions.CancelException}
	 * @param unit unit of time
	 *
	 * @return T the result
	 */
	public T get(long timeout, TimeUnit unit) {
		MonoResult<T> result = new MonoResult<>();
		subscribe(result);
		return result.await(timeout, unit);
	}

	@Override
	public final long getCapacity() {
		return 1L;
	}

	@Override
	public int getMode() {
		return FACTORY;
	}

	@Override
	public String getName() {
		return getClass().getSimpleName()
		                 .replace(Mono.class.getSimpleName(), "");
	}

	@Override
	public long getPending() {
		return -1L;
	}

	/**
	 * Create a {@link Mono} intercepting all source signals with the returned Subscriber that might choose to pass them
	 * alone to the provided Subscriber (given to the returned {@code subscribe(Subscriber)}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/lift1.png" alt="">
	 * <p>
	 * @param lifter the function accepting the target {@link Subscriber} and returning the {@link Subscriber}
	 * exposed this sequence
	 * @param <V> the output type
	 * @return a new lifted {@link Mono}
	 *
	 * @see Flux#lift
	 */
	public final <V> Mono<V> lift(Function<Subscriber<? super V>, Subscriber<? super T>> lifter) {
		return new FluxLift.MonoLift<>(this, lifter);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace implementation. Default will
	 * use {@link Level#INFO} and java.util.logging. If SLF4J is available, it will be used instead.
	 *
	 * Options allow fine grained filtering of the traced signal, for instance to only capture onNext and onError:
	 * <pre>
	 *     mono.log("category", Level.INFO, Logger.ON_NEXT | LOGGER.ON_ERROR)
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log1.png" alt="">
	 * <p>
	 *
	 * @return a new {@link Mono}
	 *
	 * @see Flux#log()
	 */
	public final Mono<T> log() {
		return log(null, Level.INFO, Logger.ALL);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace implementation. Default will
	 * use {@link Level#INFO} and java.util.logging. If SLF4J is available, it will be used instead.
	 *
	 * Options allow fine grained filtering of the traced signal, for instance to only capture onNext and onError:
	 * <pre>
	 *     mono.log("category", Level.INFO, Logger.ON_NEXT | LOGGER.ON_ERROR)
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log1.png" alt="">
	 * <p>
	 * @param category to be mapped into logger configuration (e.g. org.springframework.reactor).
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> log(String category) {
		return log(category, Level.INFO, Logger.ALL);
	}

	/**
	 * Observe all Reactive Streams signals and use {@link Logger} support to handle trace implementation. Default will
	 * use the passed {@link Level} and java.util.logging. If SLF4J is available, it will be used instead.
	 *
	 * Options allow fine grained filtering of the traced signal, for instance to only capture onNext and onError:
	 * <pre>
	 *     mono.log("category", Level.INFO, Logger.ON_NEXT | LOGGER.ON_ERROR)
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log1.png" alt="">
	 * <p>
	 * @param category to be mapped into logger configuration (e.g. org.springframework.reactor).
	 * @param level the level to enforce for this tracing Flux
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> log(String category, Level level) {
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
	 *     mono.log("category", Level.INFO, Logger.ON_NEXT | LOGGER.ON_ERROR)
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log1.png" alt="">
	 * <p>
	 * @param category to be mapped into logger configuration (e.g. org.springframework.reactor).
	 * @param level the level to enforce for this tracing Flux
	 * @param options a flag option that can be mapped with {@link Logger#ON_NEXT} etc.
	 *
	 * @return a new {@link Mono}
	 *
	 */
	public final Mono<T> log(String category, Level level, int options) {
		return MonoSource.wrap(new FluxLog<>(this, category, level, options));
	}

	/**
	 * Transform the item emitted by this {@link Mono} by applying a function to item emitted.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/map1.png" alt="">
	 * <p>
	 * @param mapper the transforming function
	 * @param <R> the transformed type
	 *
	 * @return a new {@link Mono}
	 */
	public final <R> Mono<R> map(Function<? super T, ? extends R> mapper) {
		return MonoSource.wrap(new FluxMap<>(this, mapper));
	}

	/**
	 * Merge emissions of this {@link Mono} with the provided {@link Publisher}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/merge1.png" alt="">
	 * <p>
	 * @param other the other {@link Publisher} to merge with
	 *
	 * @return a new {@link Flux} as the sequence is not guaranteed to be at most 1
	 */
	@SuppressWarnings("unchecked")
	public final Flux<T> mergeWith(Publisher<? extends T> other) {
		return Flux.merge(this, other);
	}

	/**
	 * Emit the any of the result from this mono or from the given mono
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/or.png" alt="">
	 * <p>
	 * @param other the racing other {@link Mono} to compete with for the result
	 *
	 * @return a new Mono
	 * @see #any
	 */
	public final Mono<T> or(Mono<? extends T> other) {
		return any(this, other);
	}

	/**
	 * Subscribe to a returned fallback publisher when any error occurs.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/otherwise.png" alt="">
	 * <p>
	 * @param fallback the function to map an alternative {@link Mono}
	 *
	 * @return a new {@link Mono}
	 *
	 * @see Flux#onErrorResumeWith
	 */
	public final Mono<T> otherwise(Function<Throwable, ? extends Mono<? extends T>> fallback) {
		return MonoSource.wrap(new FluxResume<>(this, fallback));
	}

	/**
	 * Provide an alternative {@link Mono} if this mono is completed without data
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/otherwiseempty.png" alt="">
	 * <p>
	 * @param alternate the alternate mono if this mono is empty
	 *
	 * @return a new {@link Mono}
	 * @see Flux#switchIfEmpty
	 */
	public final Mono<T> otherwiseIfEmpty(Mono<? extends T> alternate) {
		return MonoSource.wrap(new FluxSwitchIfEmpty<>(this, alternate));
	}

	/**
	 * Subscribe to a returned fallback value when any error occurs.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/otherwisejust.png" alt="">
	 * <p>
	 * @param fallback the value to emit if an error occurs
	 *
	 * @return a new {@link Mono}
	 *
	 * @see Flux#onErrorReturn
	 */
	public final Mono<T> otherwiseJust(final T fallback) {
		return otherwise(new Function<Throwable, Mono<? extends T>>() {
			@Override
			public Mono<? extends T> apply(Throwable throwable) {
				return just(fallback);
			}
		});
	}

	/**
	 * Run the requests to this Publisher {@link Mono} on a given scheduler thread like {@link SchedulerGroup}.
	 * <p>
	 * {@code mono.publishOn(SchedulerGroup.io()).subscribe(Subscribers.unbounded()) }
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/publishon1.png" alt="">
	 * <p>
	 * @param schedulers a checked factory for {@link Consumer} of {@link Runnable}
	 *
	 * @return a new asynchronous {@link Mono}
	 */
	public final Mono<T> publishOn(Callable<? extends Consumer<Runnable>> schedulers) {
		return MonoSource.wrap(Flux.publishOn(this, schedulers));
	}

	/**
	 * Start the chain and request unbounded demand.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/unbounded1.png" alt="">
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
	 * Convert the value of {@link Mono} to another {@link Mono} possibly with another value type.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/then.png" alt="">
	 * <p>
	 * @param transformer the function to dynamically bind a new {@link Mono}
	 * @param <R> the result type bound
	 *
	 * @return a new {@link Mono} containing the merged values
	 */
	public final <R> Mono<R> then(Function<? super T, ? extends Mono<? extends R>> transformer) {
		return MonoSource.wrap(flatMap(transformer));
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into n {@code Mono<? extends T1>} and pass
	 * the result as a combined {@code Tuple}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/thenn.png" alt="">
	 * <p>
	 * @param fn1 the transformation function
	 * @param fn2 the transformation function
	 * @param <T1> the type of the return value of the transformation function
	 * @param <T2> the type of the return value of the transformation function
	 *
	 * @return a new {@link Mono} containing the combined values
	 */
	public final <T1, T2> Mono<Tuple2<T1, T2>> then(
			final Function<? super T, ? extends Mono<? extends T1>> fn1,
			final Function<? super T, ? extends Mono<? extends T2>> fn2) {
		return then(new Function<T, Mono<? extends Tuple2<T1, T2>>>() {
			@Override
			public Mono<? extends Tuple2<T1, T2>> apply(T o) {
				return when(fn1.apply(o), fn2.apply(o));
			}
		});
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into n {@code Mono<? extends T1>} and pass
	 * the result as a combined {@code Tuple}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/thenn.png" alt="">
	 * <p>
	 * @param fn1 the transformation function
	 * @param fn2 the transformation function
	 * @param fn3 the transformation function
	 * @param <T1> the type of the return value of the transformation function
	 * @param <T2> the type of the return value of the transformation function
	 * @param <T3> the type of the return value of the transformation function
	 *
	 * @return a new {@link Mono} containing the combined values
	 */
	public final <T1, T2, T3> Mono<Tuple3<T1, T2, T3>> then(
			final Function<? super T, ? extends Mono<? extends T1>> fn1,
			final Function<? super T, ? extends Mono<? extends T2>> fn2,
			final Function<? super T, ? extends Mono<? extends T3>> fn3) {
		return then(new Function<T, Mono<? extends Tuple3<T1, T2, T3>>>() {
			@Override
			public Mono<? extends Tuple3<T1, T2, T3>> apply(T o) {
				return when(fn1.apply(o), fn2.apply(o), fn3.apply(o));
			}
		});
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into n {@code Mono<? extends T1>} and pass
	 * the result as a combined {@code Tuple}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/thenn.png" alt="">
	 * <p>
	 * @param fn1 the transformation function
	 * @param fn2 the transformation function
	 * @param fn3 the transformation function
	 * @param fn4 the transformation function
	 * @param <T1> the type of the return value of the transformation function
	 * @param <T2> the type of the return value of the transformation function
	 * @param <T3> the type of the return value of the transformation function
	 * @param <T4> the type of the return value of the transformation function
	 *
	 * @return a new {@link Mono} containing the combined values
	 *
	 * @since 2.5
	 */
	public final <T1, T2, T3, T4> Mono<Tuple4<T1, T2, T3, T4>> then(
			final Function<? super T, ? extends Mono<? extends T1>> fn1,
			final Function<? super T, ? extends Mono<? extends T2>> fn2,
			final Function<? super T, ? extends Mono<? extends T3>> fn3,
			final Function<? super T, ? extends Mono<? extends T4>> fn4) {
		return then(new Function<T, Mono<? extends Tuple4<T1, T2, T3, T4>>>() {
			@Override
			public Mono<? extends Tuple4<T1, T2, T3, T4>> apply(T o) {
				return when(fn1.apply(o), fn2.apply(o), fn3.apply(o), fn4.apply(o));
			}
		});
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into n {@code Mono<? extends T1>} and pass
	 * the result as a combined {@code Tuple}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/thenn.png" alt="">
	 * <p>
	 * @param fn1 the transformation function
	 * @param fn2 the transformation function
	 * @param fn3 the transformation function
	 * @param fn4 the transformation function
	 * @param fn5 the transformation function
	 * @param <T1> the type of the return value of the transformation function
	 * @param <T2> the type of the return value of the transformation function
	 * @param <T3> the type of the return value of the transformation function
	 * @param <T4> the type of the return value of the transformation function
	 * @param <T5> the type of the return value of the transformation function
	 *
	 * @return a new {@link Mono} containing the combined values
	 *
	 */
	public final <T1, T2, T3, T4, T5> Mono<Tuple5<T1, T2, T3, T4, T5>> then(
			final Function<? super T, ? extends Mono<? extends T1>> fn1,
			final Function<? super T, ? extends Mono<? extends T2>> fn2,
			final Function<? super T, ? extends Mono<? extends T3>> fn3,
			final Function<? super T, ? extends Mono<? extends T4>> fn4,
			final Function<? super T, ? extends Mono<? extends T5>> fn5) {
		return then(new Function<T, Mono<? extends Tuple5<T1, T2, T3, T4, T5>>>() {
			@Override
			public Mono<? extends Tuple5<T1, T2, T3, T4, T5>> apply(T o) {
				return when(fn1.apply(o), fn2.apply(o), fn3.apply(o), fn4.apply(o), fn5.apply(o));
			}
		});
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into n {@code Mono<? extends T1>} and pass
	 * the result as a combined {@code Tuple}.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/thenn.png" alt="">
	 * <p>
	 * @param fn1 the transformation function
	 * @param fn2 the transformation function
	 * @param fn3 the transformation function
	 * @param fn4 the transformation function
	 * @param fn5 the transformation function
	 * @param fn6 the transformation function
	 * @param <T1> the type of the return value of the transformation function
	 * @param <T2> the type of the return value of the transformation function
	 * @param <T3> the type of the return value of the transformation function
	 * @param <T4> the type of the return value of the transformation function
	 * @param <T5> the type of the return value of the transformation function
	 * @param <T6> the type of the return value of the transformation function
	 *
	 * @return a new {@link Mono} containing the combined values
	 *
	 */
	public final <T1, T2, T3, T4, T5, T6> Mono<Tuple6<T1, T2, T3, T4, T5, T6>> then(
			final Function<? super T, ? extends Mono<? extends T1>> fn1,
			final Function<? super T, ? extends Mono<? extends T2>> fn2,
			final Function<? super T, ? extends Mono<? extends T3>> fn3,
			final Function<? super T, ? extends Mono<? extends T4>> fn4,
			final Function<? super T, ? extends Mono<? extends T5>> fn5,
			final Function<? super T, ? extends Mono<? extends T6>> fn6) {
		return then(new Function<T, Mono<? extends Tuple6<T1, T2, T3, T4, T5, T6>>>() {
			@Override
			public Mono<? extends Tuple6<T1, T2, T3, T4, T5, T6>> apply(T o) {
				return when(fn1.apply(o), fn2.apply(o), fn3.apply(o), fn4.apply(o), fn5.apply(o), fn6.apply(o));
			}
		});
	}

	/**
	 * Subscribe the {@link Mono} with the givne {@link Subscriber} and return it.
	 *
	 * @param subscriber the {@link Subscriber} to subscribe
	 * @param <E> the reified type of the {@link Subscriber} for chaining
	 *
	 * @return the passed {@link Subscriber} after subscribing it to this {@link Mono}
	 */
	public final <E extends Subscriber<? super T>> E subscribeWith(E subscriber) {
		subscribe(subscriber);
		return subscriber;
	}

	/**
	 * Test the result if any of this {@link Mono} and replay it if predicate returns true.
	 * Otherwise complete without value.
	 *
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/where.png" alt="">
	 * <p>
	 * @param tester the predicate to evaluate
	 *
	 * @return a filtered {@link Mono}
	 */
	public final Mono<T> where(final Predicate<? super T> tester) {
		return then(new WhereFunction<>(tester));
	}

//	 ==============================================================================================================
//	 Containers
//	 ==============================================================================================================


	static final class WhereFunction<T> implements Function<T, Mono<T>> {

		private final Predicate<? super T> test;

		public WhereFunction(Predicate<? super T> test) {
			this.test = Objects.requireNonNull(test, "Where predicate is null");
		}

		@Override
		public Mono<T> apply(T t) {
			if(test.test(t)) {
				return just(t);
			}
			else{
				return empty();
			}
		}
	}
}
