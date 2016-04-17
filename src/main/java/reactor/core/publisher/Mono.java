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
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
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
import reactor.core.flow.Cancellation;
import reactor.core.flow.Fuseable;
import reactor.core.queue.QueueSupplier;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.TimedScheduler;
import reactor.core.scheduler.Timer;
import reactor.core.state.Backpressurable;
import reactor.core.state.Completable;
import reactor.core.state.Introspectable;
import reactor.core.subscriber.LambdaSubscriber;
import reactor.core.tuple.Tuple;
import reactor.core.tuple.Tuple2;
import reactor.core.tuple.Tuple3;
import reactor.core.tuple.Tuple4;
import reactor.core.tuple.Tuple5;
import reactor.core.tuple.Tuple6;
import reactor.core.util.Logger;
import reactor.core.util.PlatformDependent;
import reactor.core.util.ReactiveStateUtils;

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
 * @param <T> the type of the single value of this class
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @see Flux
 * @since 2.5
 */
public abstract class Mono<T> implements Publisher<T>, Backpressurable, Introspectable,
                                         Completable {

	static final Mono<?> NEVER = MonoSource.from(FluxNever.instance());

//	 ==============================================================================================================
//	 Static Generators
//	 ==============================================================================================================

	/**
	 * Pick the first result coming from any of the given monos and populate a new {@literal Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/any.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/any.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/defer1.png" alt="">
	 * <p>
	 * @param supplier a {@link Mono} factory
	 *
	 * @param <T> the element type of the returned Mono instance
	 *
	 * @return a new {@link Mono} factory
	 */
	public static <T> Mono<T> defer(Supplier<? extends Mono<? extends T>> supplier) {
		return new MonoDefer<>(supplier);
	}

	/**
	 * Create a Mono which delays an onNext signal of {@code duration} milliseconds and complete.
	 * If the demand cannot be produced in time, an onError will be signalled instead.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/delay.png" alt="">
	 * <p>
	 * @param duration the duration in milliseconds of the delay
	 *
	 * @return a new {@link Mono}
	 */
	public static Mono<Long> delay(long duration) {
		return delay(duration, Timer.global());
	}

	/**
	 * Create a Mono which delays an onNext signal of {@code duration} of given unit and complete on the global timer.
	 * If the demand cannot be produced in time, an onError will be signalled instead.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/delay.png" alt="">
	 * <p>
	 * @param duration the duration of the delay
	 *
	 * @return a new {@link Mono}
	 */
	public static Mono<Long> delay(Duration duration) {
		return delay(duration.toMillis());
	}

	/**
	 * Create a Mono which delays an onNext signal of {@code duration} milliseconds and complete.
	 * If the demand cannot be produced in time, an onError will be signalled instead.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/delay.png" alt="">
	 * <p>
	 * @param duration the duration in milliseconds of the delay
	 * @param timer the timer
	 *
	 * @return a new {@link Mono}
	 */
	public static Mono<Long> delay(long duration, TimedScheduler timer) {
		return new MonoTimer(duration, TimeUnit.MILLISECONDS, timer);
	}

	/**
	 * Create a Mono which delays an onNext signal of {@code duration} and complete.
	 * If the demand cannot be produced in time, an onError will be signalled instead.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/delay.png" alt="">
	 * <p>
	 * @param duration the duration of the delay
	 * @param timer the timer
	 *
	 * @return a new {@link Mono}
	 */
	public static Mono<Long> delay(Duration duration, TimedScheduler timer) {
		return delay(duration.toMillis(), timer);
	}

	/**
	 * Create a {@link Mono} that completes without emitting any item.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/empty.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/after.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/error.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/from1.png" alt="">
	 * <p>
	 * @param source the {@link Publisher} source
	 * @param <T> the source type
	 *
	 * @return the next item emitted as a {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<T> from(Publisher<? extends T> source) {
		if (source == null) {
			return empty();
		}
		if (source instanceof Mono) {
			return (Mono<T>) source;
		}
		if (source instanceof Fuseable.ScalarSupplier) {
			T t = ((Fuseable.ScalarSupplier<T>) source).get();
			if (t != null) {
				return just(t);
			}
			return empty();
		}
		return new MonoNext<>(source);
	}

	/**
	 * Create a {@link Mono} producing the value for the {@link Mono} using the given supplier.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromcallable.png" alt="">
	 * <p>
	 * @param supplier {@link Callable} that will produce the value
	 * @param <T> type of the expected value
	 *
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> fromCallable(Callable<? extends T> supplier) {
		return new MonoCallable<>(supplier);
	}


	/**
	 * Create a {@link Mono} producing the value for the {@link Mono} using the given {@link CompletableFuture}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromcompletablefuture.png" alt="">
	 * <p>
	 * @param completableFuture {@link CompletableFuture} that will produce the value
	 * @param <T> type of the expected value
	 *
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> fromCompletableFuture(CompletableFuture<? extends T> completableFuture) {
		return new MonoCompletableFuture<>(completableFuture);
	}


	/**
	 * Build a {@link Mono} that will only emit the result of the future and then complete.
	 * The future will be polled for an unbounded amount of time on {@code subscribe()}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromfuture.png" alt="">
	 *
	 * @param future the future to poll value from
	 * @param <T> the value type of the Future and the retuned Mono instance
	 * @return a new {@link Mono}
	 * @deprecated Try to avoid using this method because it invokes {@link Future#get()} blocking method. Try to use {@link #fromCompletableFuture(CompletableFuture)} or {@link MonoProcessor#create()} + callback when possible.
	 */
	public static <T> Mono<T> fromFuture(Future<? extends T> future) {
		return new MonoFuture<>(future);
	}

	/**
	 * Build a {@link Mono} that will only emit the result of the future and then complete.
	 * The future will be polled for a given amount of time on request() then onError if failed to return.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromfuture.png" alt="">
	 *
	 * @param <T> the value type of the Future and the retuned Mono instance
	 * @param future the future to poll value from
	 * @param timeout the timeout in milliseconds
	 * @return a new {@link Mono}
	 */
	public static <T> Mono<T> fromFuture(Future<? extends T> future, long timeout) {
		return new MonoFuture<>(future, timeout);
	}

	/**
	 * Build a {@link Mono} that will only emit the result of the future and then complete.
	 * The future will be polled for a given amount of time on request() then onError if failed to return.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromfuture.png" alt="">
	 *
	 * @param future the future to poll value from
	 * @param duration the duration to wait for the result of the future before failing with onError
	 * @param <T> the value type of the Future and the retuned Mono instance
	 * @return a new {@link Mono}
	 */
	public static <T> Mono<T> fromFuture(Future<? extends T> future, Duration duration) {
		return fromFuture(future, duration.toMillis());
	}

	/**
	 * Create a {@link Mono} only producing a completion signal after using the given
	 * runnable.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromrunnable.png" alt="">
	 * <p>
	 * @param runnable {@link Runnable} that will callback the completion signal
	 *
	 * @return A {@link Mono}.
	 */
	public static Mono<Void> fromRunnable(Runnable runnable) {
		return new MonoRunnable(runnable);
	}

	/**
	 * Create a {@link Mono} producing the value for the {@link Mono} using the given supplier.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/fromsupplier.png" alt="">
	 * <p>
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T> type of the expected value
	 *
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> fromSupplier(Supplier<? extends T> supplier) {
		return new MonoSupplier<>(supplier);
	}


	/**
	 * Create a new {@link Mono} that ignores onNext (dropping them) and only react on Completion signal.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/ignoreelements.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/just.png" alt="">
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
	 * Create a new {@link Mono} that emits the specified item if {@link Optional#isPresent()} otherwise only emits
	 * onComplete.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/justorempty.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/justorempty.png" alt="">
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
	 * Return a {@link Mono} that will never signal any data, error or completion signal.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/never.png" alt="">
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
	 * have been fulfilled. An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2> Mono<Tuple2<T1, T2>> when(Mono<? extends T1> p1, Mono<? extends T2> p2) {
		return new MonoWhen<>(false, p1, p2).map(a -> (Tuple2)Tuple.of(a));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled. An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Mono<Tuple3<T1, T2, T3>> when(Mono<? extends T1> p1, Mono<? extends T2> p2, Mono<? extends T3> p3) {
		return new MonoWhen<>(false, p1, p2, p3).map(a -> (Tuple3)Tuple.of(a));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled. An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4> Mono<Tuple4<T1, T2, T3, T4>> when(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4) {
		return new MonoWhen<>(false, p1, p2, p3, p4).map(a -> (Tuple4)Tuple.of(a));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled. An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5> Mono<Tuple5<T1, T2, T3, T4, T5>> when(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5) {
		return new MonoWhen<>(false, p1, p2, p3, p4, p5).map(a -> (Tuple5)Tuple.of(a));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled. An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6> Mono<Tuple6<T1, T2, T3, T4, T5, T6>> when(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5,
			Mono<? extends T6> p6) {
        return new MonoWhen<>(false, p1, p2, p3, p4, p5, p6).map(a -> (Tuple6)Tuple.of(a));
	}

	/**
	 * Aggregate given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono
	 * Monos} have been fulfilled. An error will cause pending results to be cancelled and immediate error emission to the
	 * returned {@link Flux}.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
	 * <p>
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SafeVarargs
	@SuppressWarnings({"unchecked","varargs"})
	public static <T> Mono<T[]> when(Mono<? extends T>... monos) {
		return new MonoWhen<>(false, monos);
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
	 * <p>
	 * @param p1 The first upstream {@link Publisher} to subscribe to.
	 * @param p2 The second upstream {@link Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2> Mono<Tuple2<T1, T2>> whenDelayError(Mono<? extends T1> p1, Mono<? extends T2> p2) {
		return new MonoWhen<>(true, p1, p2).map(a -> (Tuple2)Tuple.of(a));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Mono<Tuple3<T1, T2, T3>> whenDelayError(Mono<? extends T1> p1, Mono<? extends T2> p2, Mono<? extends T3> p3) {
		return new MonoWhen<>(true, p1, p2, p3).map(a -> (Tuple3)Tuple.of(a));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4> Mono<Tuple4<T1, T2, T3, T4>> whenDelayError(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4) {
		return new MonoWhen<>(true, p1, p2, p3, p4).map(a -> (Tuple4)Tuple.of(a));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5> Mono<Tuple5<T1, T2, T3, T4, T5>> whenDelayError(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5) {
		return new MonoWhen<>(true, p1, p2, p3, p4, p5).map(a -> (Tuple5)Tuple.of(a));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6> Mono<Tuple6<T1, T2, T3, T4, T5, T6>> whenDelayError(Mono<? extends T1> p1,
			Mono<? extends T2> p2,
			Mono<? extends T3> p3,
			Mono<? extends T4> p4,
			Mono<? extends T5> p5,
			Mono<? extends T6> p6) {
		return new MonoWhen<>(true, p1, p2, p3, p4, p5, p6).map(a -> (Tuple6)Tuple.of(a));
	}

	/**
	 * Aggregate given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono
	 * Monos} have been fulfilled. If any Mono terminates without value, the returned sequence will be terminated immediately and pending results cancelled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
	 * <p>
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SafeVarargs
	@SuppressWarnings({"unchecked","varargs"})
	public static <T> Mono<T[]> whenDelayError(Mono<? extends T>... monos) {
		return new MonoWhen<>(true, monos);
	}

	/**
	 * Aggregate given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono
	 * Monos} have been fulfilled. If any Mono terminates without value, the returned sequence will be terminated immediately and pending results cancelled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/when.png" alt="">
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
	private static <T, V> Mono<V> zip(Function<? super Object[], ? extends V> combinator, Mono<? extends T>... monos) {
		return MonoSource.wrap(new FluxZip<>(monos, combinator, QueueSupplier.one(), 1));
	}

	/**
	 * Aggregate given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono
	 * Monos} have been fulfilled. If any Mono terminates without value, the returned sequence will be terminated immediately and pending results cancelled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/whent.png" alt="">
	 * <p>
	 *
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<T[]> when(final Iterable<? extends Mono<? extends T>> monos) {
		return new MonoWhen<>(false, monos);
	}

	/**
	 * Aggregate given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono
	 * Monos} have been fulfilled. If any Mono terminates without value, the returned sequence will be terminated immediately and pending results cancelled.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip1.png" alt="">
	 * <p>
	 *
	 * @param combinator the combinator {@link Function}
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 * @param <V> The result type
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T, V> Mono<V> zip(final Function<? super Object[], ? extends V> combinator, final Iterable<?
			extends Mono<? extends T>> monos) {
		return MonoSource.wrap(new FluxZip<>(monos, combinator, QueueSupplier.one(), 1));
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
	 * @param <V> the element type of the returned Publisher
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/and.png" alt="">
	 * <p>
	 * @param other the {@link Mono} to combine with
	 * @param <T2> the element type of the other Mono instance
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/after1.png" alt="">
	 * <p>
	 * @return a {@link Mono} igoring its payload (actively dropping)
	 */
	public final Mono<Void> after() {
		return empty(this);
	}

	/**
	 * Transform the terminal signal (error or completion) into {@code Mono<V>} that will emit at most one result in the
	 * returned {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/afters1.png" alt="">
	 *
	 * @param other a {@link Mono} to emit from after termination
	 * @param <V> the element type of the supplied Mono
	 *
	 * @return a new {@link Mono} that emits from the supplied {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public final <V> Mono<V> after(Mono<V> other) {
		return (Mono<V>)MonoSource.wrap(new FluxConcatArray<>(false, ignoreElement(), other));
	}

	/**
	 * Transform the terminal signal (error or completion) into {@code Mono<V>} that will emit at most one result in the
	 * returned {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/afters1.png" alt="">
	 *
	 * @param sourceSupplier a {@link Supplier} of {@link Mono} to emit from after termination
	 * @param <V> the element type of the supplied Mono
	 *
	 * @return a new {@link Mono} that emits from the supplied {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public final <V> Mono<V> after(final Supplier<? extends Mono<V>> sourceSupplier) {
		return (Mono<V>)MonoSource.wrap(new FluxConcatArray<>(false, ignoreElement(), defer(sourceSupplier)));
	}

	/**
	 * Transform the terminal signal (error or completion) into {@code Mono<V>} that will emit at most one result in the
	 * returned {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/afters1.png" alt="">
	 *
	 * @param <V> the element type of the supplied Mono
	 * @param sourceSupplier a {@link Supplier} of {@link Mono} to emit from after termination
	 *
	 * @return a new {@link Mono} that emits from the supplied {@link Mono}
	 */
	public final <V> Mono<V> afterSuccessOrError(final Supplier<? extends Mono<V>> sourceSupplier) {
		return (Mono<V>)MonoSource.wrap(new FluxConcatArray<>(true, ignoreElement(), defer(sourceSupplier)));
	}

	/**
	 * Cast the current {@link Mono} produced type into a target produced type.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/cast1.png" alt="">
	 *
	 * @param <E> the {@link Mono} output type
	 * @param stream the target type to cast to
	 *
	 * @return a casted {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public final <E> Mono<E> cast(Class<E> stream) {
		return (Mono<E>) this;
	}

	/**
	 * Turn this {@link Mono} into a hot source and cache last emitted signals for further {@link Subscriber}.
	 * Completion and Error will also be replayed.
	 * <p>
	 * <img width="500" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/cache1.png"
	 * alt="">
	 *
	 * @return a replaying {@link Mono}
	 */
	public final Mono<T> cache() {
		return new MonoProcessor<>(this);
	}

	/**
	 * Subscribe a {@link Consumer} to this {@link Mono} that will consume all the
	 * sequence.
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnSuccess(Consumer)} and
	 * {@link #doOnError(java.util.function.Consumer)}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/consume1.png" alt="">
	 *
	 * @param consumer the consumer to invoke on each value
	 *
	 * @return a new {@link Runnable} to dispose the {@link Subscription}
	 */
	public final Cancellation consume(Consumer<? super T> consumer) {
		return consume(consumer, null, null);
	}

	/**
	 * Subscribe {@link Consumer} to this {@link Mono} that will consume all the
	 * sequence.
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnSuccess(Consumer)} and
	 * {@link #doOnError(java.util.function.Consumer)}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/consumeerror1.png" alt="">
	 *
	 * @param consumer the consumer to invoke on each next signal
	 * @param errorConsumer the consumer to invoke on error signal
	 *
	 * @return a new {@link Runnable} to dispose the {@link Subscription}
	 */
	public final Cancellation consume(Consumer<? super T> consumer, Consumer<? super Throwable> errorConsumer) {
		return consume(consumer, errorConsumer, null);
	}

	/**
	 * Subscribe {@link Consumer} to this {@link Mono} that will consume all the
	 * sequence.
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link #doOnSuccess(Consumer)} and
	 * {@link #doOnError(java.util.function.Consumer)}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/consumecomplete1.png" alt="">
	 *
	 * @param consumer the consumer to invoke on each value
	 * @param errorConsumer the consumer to invoke on error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 *
	 * @return a new {@link Cancellation} to dispose the {@link Subscription}
	 */
	public final Cancellation consume(Consumer<? super T> consumer,
			Consumer<? super Throwable> errorConsumer,
			Runnable completeConsumer) {
		return subscribeWith(new LambdaSubscriber<>(consumer, errorConsumer, completeConsumer));
	}

	/**
	 * Concatenate emissions of this {@link Mono} with the provided {@link Publisher} (no interleave).
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concat.png" alt="">
	 *
	 * @param other the {@link Publisher} sequence to concat after this {@link Flux}
	 *
	 * @return a concatenated {@link Flux}
	 */
	@SuppressWarnings("unchecked")
	public final Flux<T> concatWith(Publisher<? extends T> other) {
		return Flux.concat(this, other);
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/defaultifempty.png" alt="">
	 * <p>
	 * @param defaultV the alternate value if this sequence is empty
	 *
	 * @return a new {@link Mono}
	 *
	 * @see Flux#defaultIfEmpty(Object)
	 */
	public final Mono<T> defaultIfEmpty(T defaultV) {
	    if (this instanceof Fuseable.ScalarSupplier) {
            T v = get();
	        if (v == null) {
	            return Mono.just(defaultV);
	        }
	        return this;
	    }
		return new MonoDefaultIfEmpty<>(this, defaultV);
	}


	/**
	 * Delay the {@link Mono#subscribe(Subscriber) subscription} to this {@link Mono} source until the given
	 * period elapses.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/delaysubscription1.png" alt="">
	 *
	 * @param delay period in seconds before subscribing this {@link Mono}
	 *
	 * @return a delayed {@link Mono}
	 *
	 */
	public final Mono<T> delaySubscription(long delay) {
		return delaySubscription(Duration.ofSeconds(delay));
	}

	/**
	 * Delay the {@link Mono#subscribe(Subscriber) subscription} to this {@link Mono} source until the given
	 * period elapses.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/delaysubscription1.png" alt="">
	 *
	 * @param delay duration before subscribing this {@link Mono}
	 *
	 * @return a delayed {@link Mono}
	 *
	 */
	public final Mono<T> delaySubscription(Duration delay) {
		return delaySubscription(Mono.delay(delay));
	}

	/**
	 * Delay the subscription to this {@link Mono} until another {@link Publisher}
	 * signals a value or completes.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/delaysubscriptionp1.png" alt="">
	 *
	 * @param subscriptionDelay a
	 * {@link Publisher} to signal by next or complete this {@link Mono#subscribe(Subscriber)}
	 * @param <U> the other source type
	 *
	 * @return a delayed {@link Mono}
	 *
	 */
	public final <U> Mono<T> delaySubscription(Publisher<U> subscriptionDelay) {
		return MonoSource.wrap(new FluxDelaySubscription<>(this, subscriptionDelay));
	}

	/**
	 * A "phantom-operator" working only if this
	 * {@link Mono} is a emits onNext, onError or onComplete {@link Signal}. The relative {@link Subscriber}
	 * callback will be invoked, error {@link Signal} will trigger onError and complete {@link Signal} will trigger
	 * onComplete.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/dematerialize1.png" alt="">
	 * @param <X> the dematerialized type
	 *
	 * @return a dematerialized {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public final <X> Mono<X> dematerialize() {
		Mono<Signal<X>> thiz = (Mono<Signal<X>>) this;
		return MonoSource.wrap(new FluxDematerialize<>(thiz));
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doafterterminate1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/dooncancel.png" alt="">
	 * <p>
	 * @param onCancel the callback to call on {@link Subscription#cancel()}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnCancel(Runnable onCancel) {
		if (this instanceof Fuseable) {
			return MonoSource.wrap(new FluxPeekFuseable<>(this, null, null, null, null, null, null, onCancel));
		}
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonsuccess.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonerror1.png" alt="">
	 * <p>
	 * @param onError the error callback to call on {@link Subscriber#onError(Throwable)}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnError(Consumer<? super Throwable> onError) {
		if (this instanceof Fuseable) {
			return MonoSource.wrap(new FluxPeekFuseable<>(this, null, null, onError, null, null, null, null));
		}
		return MonoSource.wrap(new FluxPeek<>(this, null, null, onError, null, null, null, null));
	}

	/**
	 * Triggered when the {@link Mono} completes with an error and the exception type of
	 * that error matches the exceptionType parameter
	 *
	 * @param exceptionType type to match the error
	 * @param onError error callback to be executed on {@link Subscriber#onError(Throwable)}
	 * @param <E> type of exception
	 * @return a new {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public final  <E extends Throwable>  Mono<T> doOnError(Class<? extends Throwable> exceptionType, Consumer<E> onError) {
		return doOnError(throwable -> {
			if (exceptionType.isAssignableFrom(throwable.getClass())) {
				onError.accept((E) throwable);
			}
		});
	}

	/**
	 * Attach a {@link LongConsumer} to this {@link Mono} that will observe any request to this {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonrequest1.png" alt="">
	 *
	 * @param consumer the consumer to invoke on each request
	 *
	 * @return an observed  {@link Mono}
	 */
	public final Mono<T> doOnRequest(final LongConsumer consumer) {
		if (this instanceof Fuseable) {
			return MonoSource.wrap(new FluxPeekFuseable<>(this, null, null, null, null, null, consumer, null));
		}
		return MonoSource.wrap(new FluxPeek<>(this, null, null, null, null, null, consumer, null));
	}

	/**
	 * Triggered when the {@link Mono} is subscribed.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonsubscribe.png" alt="">
	 * <p>
	 * @param onSubscribe the callback to call on {@link Subscriber#onSubscribe(Subscription)}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		if (this instanceof Fuseable) {
			return MonoSource.wrap(new FluxPeekFuseable<>(this, onSubscribe, null, null, null, null, null, null));
		}
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/doonterminate1.png" alt="">
	 * <p>
	 * @param onTerminate the callback to call {@link Subscriber#onNext}, {@link Subscriber#onComplete} without preceding {@link Subscriber#onNext} or {@link Subscriber#onError}
	 *
	 * @return a new {@link Mono}
	 */
	public final Mono<T> doOnTerminate(BiConsumer<? super T, Throwable> onTerminate) {
		return new MonoSuccess<>(this, null, onTerminate, null);
	}

	/**
	 * Map this {@link Mono} sequence into {@link reactor.core.tuple.Tuple2} of T1 {@link Long} timemillis and T2
	 * {@link T} associated data. The timemillis corresponds to the elapsed time between the subscribe and the first
	 * next signal.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/elapsed1.png" alt="">
	 *
	 * @return a transforming {@link Mono} that emits a tuple of time elapsed in milliseconds and matching data
	 */
	public final Mono<Tuple2<Long, T>> elapsed() {
		return MonoSource.wrap(new FluxElapsed<>(this));
	}

	/**
	 * Transform the items emitted by a {@link Publisher} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmap1.png" alt="">
	 * <p>
	 * @param mapper the
	 * {@link Function} to produce a sequence of R from the the eventual passed {@link Subscriber#onNext}
	 * @param <R> the merged sequence type
	 *
	 * @return a new {@link Flux} as the sequence is not guaranteed to be single at most
	 */
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return flatMap(mapper, PlatformDependent.SMALL_BUFFER_SIZE);
	}

	/**
	 * Transform the items emitted by a {@link Publisher} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmap1.png" alt="">
	 * <p>
	 * @param mapper the
	 * {@link Function} to produce a sequence of R from the the eventual passed {@link Subscriber#onNext}
	 * @param prefetch the inner source request size
	 * @param <R> the merged sequence type
	 *
	 * @return a new {@link Flux} as the sequence is not guaranteed to be single at most
	 */
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper, int prefetch) {
		return new FluxFlatMap<>(
				this,
				mapper,
				false,
				Integer.MAX_VALUE,
				QueueSupplier.one(),
				prefetch,
				QueueSupplier.get(prefetch)
		);
	}

	/**
	 * Transform the signals emitted by this {@link Flux} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmaps1.png" alt="">
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
				Flux.identityFunction(),
				false,
				Integer.MAX_VALUE,
				QueueSupplier.xs(),
				PlatformDependent.XS_BUFFER_SIZE,
				QueueSupplier.xs()
		);
	}


	/**
	 * Transform the items emitted by this {@link Mono} into {@link Iterable}, then flatten the elements from those by
	 * merging them into a single {@link Flux}. The prefetch argument allows to give an
	 * arbitrary prefetch size to the merged {@link Iterable}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmap.png" alt="">
	 *
	 * @param mapper the {@link Function} to transform input item into a sequence {@link Iterable}
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}
	 *
	 */
	public final <R> Flux<R> flatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper) {
		return flatMapIterable(mapper, PlatformDependent.SMALL_BUFFER_SIZE);
	}

	/**
	 * Transform the items emitted by this {@link Mono} into {@link Iterable}, then flatten the emissions from those by
	 * merging them into a single {@link Flux}. The prefetch argument allows to give an
	 * arbitrary prefetch size to the merged {@link Iterable}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmapc.png" alt="">
	 *
	 * @param mapper the {@link Function} to transform input item into a sequence {@link Iterable}
	 * @param prefetch the maximum in-flight elements from the inner {@link Iterable} sequence
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}
	 *
	 */
	public final <R> Flux<R> flatMapIterable(Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch) {
		return new FluxFlattenIterable<>(this, mapper, prefetch, QueueSupplier.get(prefetch));
	}

	/**
	 * Convert this {@link Mono} to a {@link Flux}
	 *
	 * @return a {@link Flux} variant of this {@link Mono}
	 */
	@SuppressWarnings("unchecked")
    public final Flux<T> flux() {
	    if (this instanceof Supplier) {
	        if (this instanceof Fuseable.ScalarSupplier) {
	            T v = get();
	            if (v == null) {
	                return Flux.empty();
	            }
	            return Flux.just(v);
	        }
	        return new FluxSupplier<>((Supplier<T>)this);
	    }
		return FluxSource.wrap(this);
	}

	/**
	 * Block until a next signal is received, will return null if onComplete, T if onNext, throw a
	 * {@literal Exceptions.DownstreamException} if checked error or origin RuntimeException if unchecked.
	 * If the default timeout {@literal PlatformDependent#DEFAULT_TIMEOUT} has elapsed, a CancelException will be thrown.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/get.png" alt="">
	 * <p>
	 *
	 * @return T the result
	 */
	public T get() {
		return get(PlatformDependent.DEFAULT_TIMEOUT);
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/get.png" alt="">
	 * <p>
	 *
	 * @param timeout maximum time period to wait for in milliseconds before raising a {@literal reactor.core.util.Exceptions.CancelException}
	 *
	 * @return T the result
	 */
	public T get(long timeout) {
		if(this instanceof Supplier){
			return get();
		}
		return subscribe().get(timeout);
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/get.png" alt="">
	 * <p>
	 *
	 * @param timeout maximum time period to wait for before raising a {@literal reactor.core.util.Exceptions.CancelException}
	 *
	 * @return T the result
	 */
	public T get(Duration timeout) {
		return get(timeout.toMillis());
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

	/**
	 * Emit a single boolean true if this {@link Mono} has an element.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/haselement.png" alt="">
	 *
	 * @return a new {@link Mono} with <code>true</code> if a value is emitted and <code>false</code>
	 * otherwise
	 */
	public final Mono<Boolean> hasElement() {
		return new MonoHasElements<>(this);
	}

	/**
	 * Hides the identity of this {@link Mono} instance.
	 *
	 * <p>The main purpose of this operator is to prevent certain identity-based
	 * optimizations from happening, mostly for diagnostic purposes.
	 *
	 * @return a new {@link Mono} instance
	 */
	public final Mono<T> hide() {
	    return new MonoHide<>(this);
	}

	/**
	 * Ignores onNext signal (dropping it) and only reacts on termination.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/ignoreelement.png" alt="">
	 * <p>
	 *
	 * @return a new completable {@link Mono}.
	 */
	public final Mono<T> ignoreElement() {
		return ignoreElements(this);
	}

	/**
	 * Create a {@link Mono} intercepting all source signals with the returned Subscriber that might choose to pass them
	 * alone to the provided Subscriber (given to the returned {@code subscribe(Subscriber)}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/lift1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/log1.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/map1.png" alt="">
	 * <p>
	 * @param mapper the transforming function
	 * @param <R> the transformed type
	 *
	 * @return a new {@link Mono}
	 */
	public final <R> Mono<R> map(Function<? super T, ? extends R> mapper) {
		if (this instanceof Fuseable) {
			return new MonoMapFuseable<>(this, mapper);
		}
		return new MonoMap<>(this, mapper);
	}

	/**
	 * Transform the incoming onNext, onError and onComplete signals into {@link Signal}.
	 * Since the error is materialized as a {@code Signal}, the propagation will be stopped and onComplete will be
	 * emitted. Complete signal will first emit a {@code Signal.complete()} and then effectively complete the flux.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/materialize1.png" alt="">
	 *
	 * @return a {@link Mono} of materialized {@link Signal}
	 */
	public final Mono<Signal<T>> materialize() {
		return new FluxMaterialize<>(this).next();
	}

	/**
	 * Merge emissions of this {@link Mono} with the provided {@link Publisher}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/merge1.png" alt="">
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
	 * Emit the current instance of the {@link Mono}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/nest1.png" alt="">
	 *
	 * @return a new {@link Mono} whose value will be the current {@link Mono}
	 */
	public final Mono<Mono<T>> nest() {
		return just(this);
	}

	/**
	 * Emit the any of the result from this mono or from the given mono
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/or.png" alt="">
	 * <p>
	 * @param other the racing other {@link Mono} to compete with for the result
	 *
	 * @return a new {@link Mono}
	 * @see #any
	 */
	public final Mono<T> or(Mono<? extends T> other) {
		return any(this, other);
	}

	/**
	 * Subscribe to a returned fallback publisher when any error occurs.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/otherwise.png" alt="">
	 * <p>
	 * @param fallback the function to map an alternative {@link Mono}
	 *
	 * @return an alternating {@link Mono} on source onError
	 *
	 * @see Flux#onErrorResumeWith
	 */
	public final Mono<T> otherwise(Function<Throwable, ? extends Mono<? extends T>> fallback) {
		return MonoSource.wrap(new FluxResume<>(this, fallback));
	}

	/**
	 *  Subscribe to a fallback mono that respond with a supplier if the given
	 *  exceptionType is matched with the incoming error
	 *
	 * @param exceptionType expected type of exception
	 * @param fallback mono to return if the exception match the exceptionType
	 * @return an alternating {@link Mono} supplyed by you when the exceptions match, otherwise a wrapped error
	 *
	 * @see this#otherwise(Function)
	 */
	public final Mono<T> otherwise(Class<? extends Throwable> exceptionType, Mono<? extends T> fallback) {
		return otherwise(exceptionType, throwable -> fallback);
	}

	/**
	 * Subscribe to a
	 * @param ex
	 * @param fallback
	 * @return
	 */
	public final Mono<T> otherwise(Class<? extends Throwable> ex,
			Function<Throwable, Mono<? extends T>> fallback) {
		return otherwise(throwable -> {
			if (ex.isAssignableFrom(throwable.getClass())) {
				return otherwise(fallback);
			}
			else {
				return Mono.error(throwable);
			}
		});
	}

	/**
	 * Provide an alternative {@link Mono} if this mono is completed without data
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/otherwiseempty.png" alt="">
	 * <p>
	 * @param alternate the alternate mono if this mono is empty
	 *
	 * @return an alternating {@link Mono} on source onComplete without elements
	 * @see Flux#switchIfEmpty
	 */
	public final Mono<T> otherwiseIfEmpty(Mono<? extends T> alternate) {
		return MonoSource.wrap(new FluxSwitchIfEmpty<>(this, alternate));
	}

	/**
	 * Subscribe to a returned fallback value when any error occurs.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/otherwisejust.png" alt="">
	 * <p>
	 * @param fallback the value to emit if an error occurs
	 *
	 * @return a new {@link Mono}
	 *
	 * @see Flux#onErrorReturn
	 */
	public final Mono<T> otherwiseJust(final T fallback) {
		return otherwise(throwable -> just(fallback));
	}

	/**
	 * Run onNext, onComplete and onError on a supplied {@link Function} worker like {@link SchedulerGroup}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/publishon1.png" alt="">
	 * <p> <p>
	 * Typically used for fast publisher, slow consumer(s) scenarios.
	 * It naturally combines with {@link SchedulerGroup#single} and {@link SchedulerGroup#async} which implement
	 * fast async event loops.
	 *
	 * {@code mono.publishOn(WorkQueueProcessor.create()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param scheduler a checked {@link reactor.core.scheduler.Scheduler.Worker} factory
	 *
	 * @return an asynchronously producing {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public final Mono<T> publishOn(Scheduler scheduler) {
		if (this instanceof Fuseable.ScalarSupplier) {
			T value = get();
			return  new MonoSubscribeOnValue<>(value, scheduler);
		}
		return new MonoPublishOn<>(this, scheduler);
	}

	/**
	 * Run onNext, onComplete and onError on a supplied {@link ExecutorService}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/publishon.png" alt="">
	 * <p>
	 * {@code mono.publishOn(ForkJoinPool.commonPool()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param executorService an {@link ExecutorService}
	 *
	 * @return a {@link Mono} producing asynchronously
	 */
	public final Mono<T> publishOn(ExecutorService executorService) {
		return publishOn(new ExecutorServiceScheduler(executorService));
	}

	/**
	 * Repeatedly subscribe to this {@link Mono} until there is an onNext signal when a companion sequence signals a
	 * number of emitted elements.
	 * <p>If the companion sequence signals when this {@link Mono} is active, the repeat
	 * attempt is suppressed and any terminal signal will terminate this {@link Flux} with the same signal immediately.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/repeatwhenempty.png" alt="">
	 *
	 * @param repeatFactory the
	 * {@link Function} providing a {@link Flux} signalling the current number of repeat on onComplete and returning a {@link Publisher} companion.
	 *
	 * @return an eventually repeated {@link Mono} on onComplete when the companion {@link Publisher} produces an
	 * onNext signal
	 *
	 */
	public final Mono<T> repeatWhenEmpty(Function<Flux<Long>, ? extends Publisher<?>> repeatFactory) {
		return repeatWhenEmpty(Integer.MAX_VALUE, repeatFactory);
	}


	/**
	 * Repeatedly subscribe to this {@link Mono} until there is an onNext signal when a companion sequence signals a
	 * number of emitted elements.
	 * <p>If the companion sequence signals when this {@link Mono} is active, the repeat
	 * attempt is suppressed and any terminal signal will terminate this {@link Flux} with the same signal immediately.
	 * <p>Emits an {@link IllegalStateException} if the max repeat is exceeded and different from {@code Integer.MAX_VALUE}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/repeatwhen1.png" alt="">
	 *
	 * @param maxRepeat the maximum repeat number of time (infinite if {@code Integer.MAX_VALUE})
	 * @param repeatFactory the
	 * {@link Function} providing a {@link Flux} signalling the current repeat index from 0 on onComplete and returning a {@link Publisher} companion.
	 *
	 * @return an eventually repeated {@link Mono} on onComplete when the companion {@link Publisher} produces an
	 * onNext signal
	 *
	 */
	public final Mono<T> repeatWhenEmpty(int maxRepeat, Function<Flux<Long>, ? extends Publisher<?>> repeatFactory) {
		return Mono.defer(() -> {
			Flux<Long> iterations;

			if(maxRepeat == Integer.MAX_VALUE) {
                AtomicLong counter = new AtomicLong();
				iterations = Flux
					.generate((range, subscriber) -> LongStream
						.range(0, range)
						.forEach(l -> subscriber.onNext(counter.getAndIncrement())));
			} else {
				iterations = Flux
					.range(0, maxRepeat)
					.map(Integer::longValue)
					.concatWith(Flux.error(new IllegalStateException("Exceeded maximum number of repeats"), true));
			}

			AtomicBoolean nonEmpty = new AtomicBoolean();

			return Flux
				.from(this
					.doOnSuccess(e -> nonEmpty.lazySet(e != null)))
				.repeatWhen(o -> repeatFactory
					.apply(o
						.takeWhile(e -> !nonEmpty.get())
						.zipWith(iterations, 1, (c, i) -> i)))
				.single();
		});
	}


	/**
	 * Re-subscribes to this {@link Mono} sequence if it signals any error
	 * either indefinitely.
	 * <p>
	 * The times == Long.MAX_VALUE is treated as infinite retry.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/retry1.png" alt="">
	 *
	 * @return a re-subscribing {@link Mono} on onError
	 */
	public final Mono<T> retry() {
		return retry(Long.MAX_VALUE);
	}

	/**
	 * Re-subscribes to this {@link Mono} sequence if it signals any error
	 * either indefinitely or a fixed number of times.
	 * <p>
	 * The times == Long.MAX_VALUE is treated as infinite retry.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/retryn1.png" alt="">
	 *
	 * @param numRetries the number of times to tolerate an error
	 *
	 * @return a re-subscribing {@link Mono} on onError up to the specified number of retries.
	 *
	 */
	public final Mono<T> retry(long numRetries) {
		return MonoSource.wrap(new FluxRetry<>(this, numRetries));
	}

	/**
	 * Re-subscribes to this {@link Mono} sequence if it signals any error
	 * and the given {@link Predicate} matches otherwise push the error downstream.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/retryb1.png" alt="">
	 *
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 *
	 * @return a re-subscribing {@link Mono} on onError if the predicates matches.
	 */
	public final Mono<T> retry(Predicate<Throwable> retryMatcher) {
		return retryWhen(v -> v.filter(retryMatcher));
	}

	/**
	 * Re-subscribes to this {@link Mono} sequence up to the specified number of retries if it signals any
	 * error and the given {@link Predicate} matches otherwise push the error downstream.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/retrynb1.png" alt="">
	 *
	 * @param numRetries the number of times to tolerate an error
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 *
	 * @return a re-subscribing {@link Mono} on onError up to the specified number of retries and if the predicate
	 * matches.
	 *
	 */
	public final Mono<T> retry(long numRetries, Predicate<Throwable> retryMatcher) {
		return retry(Flux.countingPredicate(retryMatcher, numRetries));
	}

	/**
	 * Retries this {@link Mono} when a companion sequence signals
	 * an item in response to this {@link Mono} error signal
	 * <p>If the companion sequence signals when the {@link Mono} is active, the retry
	 * attempt is suppressed and any terminal signal will terminate the {@link Mono} source with the same signal
	 * immediately.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/retrywhen1.png" alt="">
	 *
	 * @param whenFactory the {@link Function} providing a {@link Flux} signalling any error from the source sequence and returning a {@link Publisher} companion.
	 *
	 * @return a re-subscribing {@link Mono} on onError when the companion {@link Publisher} produces an
	 * onNext signal
	 */
	public final Mono<T> retryWhen(Function<Flux<Throwable>, ? extends Publisher<?>> whenFactory) {
		return MonoSource.wrap(new FluxRetryWhen<>(this, whenFactory));
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

	/**
	 * Run the requests to this Publisher {@link Mono} on a given worker assigned by the supplied {@link Scheduler}.
	 * <p>
	 * {@code mono.subscribeOn(SchedulerGroup.io()).subscribe(Subscribers.unbounded()) }
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/subscribeon1.png" alt="">
	 * <p>
	 * @param scheduler a checked {@link reactor.core.scheduler.Scheduler.Worker} factory
	 *
	 * @return an asynchronously requesting {@link Mono}
	 */
	public final Mono<T> subscribeOn(Scheduler scheduler) {
		if (this instanceof Fuseable.ScalarSupplier) {
			T value = get();
			return new MonoSubscribeOnValue<>(value, scheduler);
		}
		return new MonoSubscribeOn<>(this, scheduler);
	}

	/**
	 * Run the requests to this Publisher {@link Mono} on supplied {@link ExecutorService}.
	 * <p>
	 * {@code mono.subscribeOn(ForkJoinPool.commonPool()).subscribe(Subscribers.unbounded()) }
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/subscribeon1.png" alt="">
	 * <p>
	 * @param executorService an {@link ExecutorService}
	 *
	 * @return an asynchronously requesting {@link Mono}
	 */
	public final Mono<T> subscribeOn(ExecutorService executorService) {
		return subscribeOn(new ExecutorServiceScheduler(executorService));
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
	 * Convert the value of {@link Mono} to another {@link Mono} possibly with another value type.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/then.png" alt="">
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/thenn.png" alt="">
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
		return then(o -> when(fn1.apply(o), fn2.apply(o)));
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into n {@code Mono<? extends T1>} and pass
	 * the result as a combined {@code Tuple}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/thenn.png" alt="">
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
		return then(o -> when(fn1.apply(o), fn2.apply(o), fn3.apply(o)));
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into n {@code Mono<? extends T1>} and pass
	 * the result as a combined {@code Tuple}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/thenn.png" alt="">
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
		return then(o -> when(fn1.apply(o), fn2.apply(o), fn3.apply(o), fn4.apply(o)));
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into n {@code Mono<? extends T1>} and pass
	 * the result as a combined {@code Tuple}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/thenn.png" alt="">
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
		return then(o -> when(fn1.apply(o), fn2.apply(o), fn3.apply(o), fn4.apply(o), fn5.apply(o)));
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into n {@code Mono<? extends T1>} and pass
	 * the result as a combined {@code Tuple}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/thenn.png" alt="">
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
		return then(o -> when(fn1.apply(o), fn2.apply(o), fn3.apply(o), fn4.apply(o), fn5.apply(o), fn6.apply(o)));
	}

	/**
	 * Signal a {@link java.util.concurrent.TimeoutException} error in case an item doesn't arrive before the given period.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/timeouttime1.png" alt="">
	 *
	 * @param timeout the timeout before the onNext signal from this {@link Mono}
	 *
	 * @return an expirable {@link Mono}
	 */
	public final Mono<T> timeout(long timeout) {
		return timeout(Duration.ofMillis(timeout), null);
	}

	/**
	 * Signal a {@link java.util.concurrent.TimeoutException} in case an item doesn't arrive before the given period.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/timeouttime1.png" alt="">
	 *
	 * @param timeout the timeout before the onNext signal from this {@link Mono}
	 *
	 * @return an expirable {@link Mono}
	 */
	public final Mono<T> timeout(Duration timeout) {
		return timeout(timeout, null);
	}

	/**
	 * Switch to a fallback {@link Mono} in case an item doesn't arrive before the given period.
	 *
	 * <p> If the given {@link Publisher} is null, signal a {@link java.util.concurrent.TimeoutException}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/timeouttimefallback1.png" alt="">
	 *
	 * @param timeout the timeout before the onNext signal from this {@link Mono}
	 * @param fallback the fallback {@link Mono} to subscribe when a timeout occurs
	 *
	 * @return an expirable {@link Mono} with a fallback {@link Mono}
	 */
	public final Mono<T> timeout(Duration timeout, Mono<? extends T> fallback) {
		final Mono<Long> _timer = Mono.delay(timeout).otherwiseJust(0L);
		final Function<T, Publisher<Long>> rest = o -> never();

		if(fallback == null) {
			return MonoSource.wrap(new FluxTimeout<>(this, _timer, rest));
		}
		return MonoSource.wrap(new FluxTimeout<>(this, _timer, rest, fallback));
	}

	/**
	 * Signal a {@link java.util.concurrent.TimeoutException} in case the item from this {@link Mono} has
	 * not been emitted before the given {@link Publisher} emits.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/timeoutp1.png" alt="">
	 *
	 * @param firstTimeout the timeout {@link Publisher} that must not emit before the first signal from this {@link Flux}
	 * @param <U> the element type of the timeout Publisher
	 *
	 * @return an expirable {@link Mono} if the first item does not come before a {@link Publisher} signal
	 *
	 */
	public final <U> Mono<T> timeout(Publisher<U> firstTimeout) {
		return MonoSource.wrap(new FluxTimeout<>(this, firstTimeout, t -> never()));
	}

	/**
	 * Switch to a fallback {@link Publisher} in case the  item from this {@link Mono} has
	 * not been emitted before the given {@link Publisher} emits. The following items will be individually timed via
	 * the factory provided {@link Publisher}.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/timeoutfallbackp1.png" alt="">
	 *
	 * @param firstTimeout the timeout
	 * {@link Publisher} that must not emit before the first signal from this {@link Mono}
	 * @param fallback the fallback {@link Publisher} to subscribe when a timeout occurs
	 * @param <U> the element type of the timeout Publisher
	 *
	 * @return a first then per-item expirable {@link Mono} with a fallback {@link Publisher}
	 *
	 */
	public final <U> Mono<T> timeout(Publisher<U> firstTimeout, Mono<? extends T> fallback) {
		return MonoSource.wrap(new FluxTimeout<>(this, firstTimeout, t -> never(), fallback));
	}


	/**
	 * Emit a {@link reactor.core.tuple.Tuple2} pair of T1 {@link Long} current system time in
	 * millis and T2 {@link T} associated data for the eventual item from this {@link Mono}
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/timestamp1.png" alt="">
	 *
	 * @return a timestamped {@link Mono}
	 */
	@SuppressWarnings("unchecked")
	public final Mono<Tuple2<Long, T>> timestamp() {
		return map(Flux.TIMESTAMP_OPERATOR);
	}

	/**
	 * Transform this {@link Mono} into a {@link CompletableFuture} completing on onNext or onComplete and failing on
	 * onError.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/tocompletablefuture.png" alt="">
	 * <p>
	 *
	 * @return a {@link CompletableFuture}
	 */
	public final CompletableFuture<T> toCompletableFuture() {
		return subscribeWith(new MonoToCompletableFuture<>());
	}

	/**
	 * Test the result if any of this {@link Mono} and replay it if predicate returns true.
	 * Otherwise complete without value.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/where.png" alt="">
	 * <p>
	 * @param tester the predicate to evaluate
	 *
	 * @return a filtered {@link Mono}
	 */
	public final Mono<T> where(final Predicate<? super T> tester) {
		if (this instanceof Fuseable) {
			return new MonoFilterFuseable<>(this, tester);
		}
		return new MonoFilter<>(this, tester);
	}

}
