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
package reactor.core.subscriber;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

//
/**
 * A Reactive Streams {@link Subscriber} factory which callbacks on start, onNext, onError and shutdown
 * <p>
 * The Publisher will directly forward all the signals passed to the subscribers and complete when onComplete is called.
 * <p>
 * Create such publisher with the provided factory, E.g.:
 * <pre>
 * {@code
 * Flux.just("hello")
 *     .subscribe( Subscribers.unbounded(System.out::println) );
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
public enum Subscribers{
	;

	/**
	 *
	 * Create a bounded {@link LambdaSubscriber} that will keep a maximum in-flight items running.
	 * The prefetch strategy works with a first request N, then when 25% of N is left to be received on
	 * onNext, request N x 0.75.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/consume.png" alt="">
	 *
	 * @param prefetch the in-flight capacity used to request source {@link Publisher}
	 * @param callback an onNext {@link Consumer} callback
	 * @param <T> the consumed sequence type
	 * @return a bounded {@link LambdaSubscriber}
	 */
	public static <T> LambdaSubscriber<T> bounded(int prefetch, Consumer<? super T> callback){
		return bounded(prefetch, callback, null, null);
	}

	/**
	 * Create a bounded {@link LambdaSubscriber} that will keep a maximum in-flight items running.
	 * The prefetch strategy works with a first request N, then when 25% of N is left to be received on
	 * onNext, request N x 0.75.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/consume.png" alt="">
	 *
	 * @param prefetch the in-flight capacity used to request source {@link Publisher}
	 * @param callback an onNext {@link Consumer} callback
	 * @param errorCallback an onError {@link Consumer} callback
	 * @param completeCallback an onComplete {@link Consumer} callback
	 * @param <T> the consumed sequence type
	 * @return a bounded {@link LambdaSubscriber}
	 */
	public static <T> LambdaSubscriber<T> bounded(int prefetch, Consumer<? super T> callback,
			Consumer<? super Throwable> errorCallback, Runnable completeCallback){
		return new BoundedSubscriber<>(prefetch, callback, errorCallback, completeCallback);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 *
	 * @param dataConsumer A {@link Consumer} with argument onNext data
	 * @param <T>          The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> consumer(Consumer<? super T> dataConsumer) {
		return consumer(dataConsumer, null, null);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext and onError. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 *
	 * @param dataConsumer  A {@link Consumer} with argument onNext data
	 * @param errorConsumer A {@link Consumer} called onError
	 * @param <T>           The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> consumer(Consumer<? super T> dataConsumer,
			Consumer<? super Throwable> errorConsumer) {
		return consumer(dataConsumer, errorConsumer, null);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext, onError and onComplete. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 * <p>
	 *
	 * @param dataConsumer     A {@link Consumer} with argument onNext data
	 * @param errorConsumer    A {@link Consumer} called onError
	 * @param completeConsumer A {@link Runnable} called onComplete with the actual context if any
	 * @param <T>              The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> consumer(Consumer<? super T> dataConsumer,
			final Consumer<? super Throwable> errorConsumer,
			Runnable completeConsumer) {
		return new LambdaSubscriber<>(
				dataConsumer,
				errorConsumer,
				completeConsumer
		);
	}

	/**
	 * Create a {@link Subscriber} reacting onSubscribe with the passed {@link Consumer}
	 *
	 * @param subscriptionHandler A {@link Consumer} called once for every new subscription returning
	 *                            (IO connection...)
	 * @param <T>                 The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> create(final Consumer<? super Subscription> subscriptionHandler) {
		return create(subscriptionHandler, null, null, null);
	}

	/**
	 * Create a {@link Subscriber} reacting onSubscribe and onNext
	 *
	 * @param subscriptionHandler A {@link Consumer} called once for every new
	 * subscription
	 *                            context
	 * @param dataConsumer        A {@link BiConsumer} with left argument onNext data and right argument upstream
	 *                            subscription
	 * @param <T>                 The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> create(Consumer<? super Subscription> subscriptionHandler,
			BiConsumer<? super T, ? super Subscription> dataConsumer) {
		return create(subscriptionHandler, dataConsumer, null, null);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext, onError.
	 * <p>
	 *
	 * @param subscriptionHandler A {@link Consumer} called once for every new
	 * subscription
	 *                            context
	 * @param dataConsumer        A {@link BiConsumer} with left argument onNext data and right argument upstream
	 *                            subscription
	 * @param errorConsumer       A {@link Consumer} called onError
	 * @param <T>                 The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> create(Consumer<? super Subscription> subscriptionHandler,
			BiConsumer<? super T, ? super Subscription> dataConsumer,
			Consumer<? super Throwable> errorConsumer) {
		return create(subscriptionHandler, dataConsumer, errorConsumer, null);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext, onSubscribe, onError, onComplete with the passed {@link
	 * BiConsumer}.
	 * The argument {@code subscriptionHandler} is executed once by new subscriber to generate a context shared by
	 * every
	 * request calls.
	 * The argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel, onComplete,
	 * onError).
	 *
	 * @param dataConsumer        A {@link BiConsumer} with left argument onNext data and right argument upstream
	 *                            subscription
	 * @param subscriptionHandler A {@link Function} called once for every new subscription returning an immutable
	 *                            context
	 *                            (IO connection...)
	 * @param errorConsumer       A {@link BiConsumer} called onError with the actual error
	 * @param completeConsumer    A {@link Runnable} called onComplete with the actual
	 * @param <T>                 The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> create(Consumer<? super Subscription> subscriptionHandler,
			BiConsumer<? super T, ? super Subscription> dataConsumer,
			Consumer<? super Throwable> errorConsumer,
			Runnable completeConsumer) {

		return new OpeningSubscriber<>(dataConsumer, subscriptionHandler, errorConsumer, completeConsumer);
	}

	/**
	 * Safely gate a {@link Subscriber} by a serializing {@link Subscriber}.
	 * Serialization uses thread-stealing and a potentially unbounded queue that might starve a calling thread if
	 * races are too important and
	 * {@link Subscriber} is slower.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/serialize.png" alt="">
	 *
	 * @param <T> the relayed type
	 * @param subscriber the subscriber to wrap
	 * @return a serializing {@link Subscriber}
	 */
	public static <T> Subscriber<T> serialize(Subscriber<? super T> subscriber) {
		return new SerializedSubscriber<>(subscriber);
	}

	/**
	 * Create a {@link Subscriber} that will automatically request Long.MAX_VALUE onSubscribe.
	 *
	 * @param <T> The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> unbounded() {
		return new LambdaSubscriber<>(
				null,
				null,
				null
		);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 *
	 * @param dataConsumer A {@link BiConsumer} with left argument onNext data and right argument upstream subscription
	 * @param <T>          The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> unbounded(BiConsumer<? super T, ? super Subscription> dataConsumer) {
		return unbounded(dataConsumer, null, null);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext and onError. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 *
	 * @param dataConsumer  A {@link BiConsumer} with left argument onNext data and right argument upstream
	 *                         subscription
	 * @param errorConsumer A {@link Consumer} called onError
	 * @param <T>           The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> unbounded(BiConsumer<? super T, ? super Subscription> dataConsumer,
			Consumer<? super Throwable> errorConsumer) {
		return unbounded(dataConsumer, errorConsumer, null);
	}

	/**
	 * Create a {@link Subscriber} reacting onNext, onError and onComplete. The subscriber will automatically
	 * request Long.MAX_VALUE onSubscribe.
	 * <p>
	 * The argument {@code subscriptionHandler} is executed once by new subscriber to generate a context shared by
	 * every
	 * request calls.
	 *
	 * @param dataConsumer     A {@link BiConsumer} with left argument onNext data and right argument upstream
	 *                         subscription
	 * @param errorConsumer    A {@link Consumer} called onError
	 * @param completeConsumer A {@link Runnable} called onComplete
	 * @param <T>              The type of the data sequence
	 * @return a fresh Reactive Streams subscriber ready to be subscribed
	 */
	public static <T> Subscriber<T> unbounded(BiConsumer<? super T, ? super Subscription> dataConsumer,
			final Consumer<? super Throwable> errorConsumer,
			Runnable completeConsumer) {
		return create(UNBOUNDED_REQUEST,
				dataConsumer, errorConsumer,
				completeConsumer
		);
	}

	private static final Consumer<Subscription> UNBOUNDED_REQUEST =
			subscription -> subscription.request(Long.MAX_VALUE);

}
