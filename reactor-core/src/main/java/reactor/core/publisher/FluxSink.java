/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.function.Function;
import java.util.function.LongConsumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.util.context.Context;

/**
 * Wrapper API around a downstream Subscriber for emitting any number of
 * next signals followed by zero or one onError/onComplete.
 * <p>
 * @param <T> the value type
 */
public interface FluxSink<T> {

	/**
	 * Emit a non-null element, generating an {@link Subscriber#onNext(Object) onNext} signal.
	 * <p>
	 * Might throw an unchecked exception in case of a fatal error downstream which cannot
	 * be propagated to any asynchronous handler (aka a bubbling exception).
	 *
	 * @param t the value to emit, not null
	 * @return this sink for chaining further signals
	 **/
	FluxSink<T> next(T t);

	/**
	 * Terminate the sequence successfully, generating an {@link Subscriber#onComplete() onComplete}
	 * signal.
	 *
	 * @see Subscriber#onComplete()
	 */
	void complete();

	/**
	 * Fail the sequence, generating an {@link Subscriber#onError(Throwable) onError}
	 * signal.
	 *
	 * @param e the exception to signal, not null
	 * @see Subscriber#onError(Throwable)
	 */
	void error(Throwable e);

	/**
	 * Return the current subscriber {@link Context}.
	 * <p>
	 *   {@link Context} can be enriched via {@link Flux#contextWrite(Function)}
	 *   operator or directly by a child subscriber overriding
	 *   {@link CoreSubscriber#currentContext()}
	 *
	 * @return the current subscriber {@link Context}.
	 */
	Context currentContext();


	/**
	 * The current outstanding request amount.
	 * @return the current outstanding request amount
	 */
	long requestedFromDownstream();

	/**
	 * Returns true if the downstream cancelled the sequence.
	 * @return true if the downstream cancelled the sequence
	 */
	boolean isCancelled();

	/**
	 * Attaches a {@link LongConsumer} to this {@link FluxSink} that will be notified of
	 * any request to this sink.
	 * <p>
	 * For push/pull sinks created using {@link Flux#create(java.util.function.Consumer)}
	 * or {@link Flux#create(java.util.function.Consumer, FluxSink.OverflowStrategy)},
	 * the consumer
	 * is invoked for every request to enable a hybrid backpressure-enabled push/pull model.
	 * When bridging with asynchronous listener-based APIs, the {@code onRequest} callback
	 * may be used to request more data from source if required and to manage backpressure
	 * by delivering data to sink only when requests are pending.
	 * <p>
	 * For push-only sinks created using {@link Flux#push(java.util.function.Consumer)}
	 * or {@link Flux#push(java.util.function.Consumer, FluxSink.OverflowStrategy)},
	 * the consumer is invoked with an initial request of {@code Long.MAX_VALUE} when this method
	 * is invoked.
	 *
	 * @param consumer the consumer to invoke on each request
	 * @return {@link FluxSink} with a consumer that is notified of requests
	 */
	FluxSink<T> onRequest(LongConsumer consumer);

	/**
	 * Attach a {@link Disposable} as a callback for when this {@link FluxSink} is
	 * cancelled. At most one callback can be registered, and subsequent calls to this method
	 * will result in the immediate disposal of the extraneous {@link Disposable}.
	 * <p>
	 * The callback is only relevant when the downstream {@link Subscription} is {@link Subscription#cancel() cancelled}.
	 *
	 * @param d the {@link Disposable} to use as a callback
	 * @return the {@link FluxSink} with a cancellation callback
	 * @see #onCancel(Disposable) onDispose(Disposable) for a callback that covers cancellation AND terminal signals
	 */
	FluxSink<T> onCancel(Disposable d);

	/**
	 * Attach a {@link Disposable} as a callback for when this {@link FluxSink} is effectively
	 * disposed, that is it cannot be used anymore. This includes both having played terminal
	 * signals (onComplete, onError) and having been cancelled (see {@link #onCancel(Disposable)}).
	 * At most one callback can be registered, and subsequent calls to this method will result in
	 * the immediate disposal of the extraneous {@link Disposable}.
	 * <p>
	 * Note that the "dispose" term is used from the perspective of the sink. Not to
	 * be confused with {@link Flux#subscribe()}'s {@link Disposable#dispose()} method, which
	 * maps to disposing the {@link Subscription} (effectively, a {@link Subscription#cancel()}
	 * signal).
	 *
	 * @param d the {@link Disposable} to use as a callback
	 * @return the {@link FluxSink} with a callback invoked on any terminal signal or on cancellation
	 * @see #onCancel(Disposable) onCancel(Disposable) for a cancellation-only callback
	 */
	FluxSink<T> onDispose(Disposable d);

	/**
	 * Enumeration for backpressure handling.
	 */
	enum OverflowStrategy {
		/**
		 * Completely ignore downstream backpressure requests.
		 * <p>
		 * This may yield {@link IllegalStateException} when queues get full downstream.
		 */
		IGNORE,
		/**
		 * Signal an {@link IllegalStateException} when the downstream can't keep up
		 */
		ERROR,
		/**
		 * Drop the incoming signal if the downstream is not ready to receive it.
		 */
		DROP,
		/**
		 * Downstream will get only the latest signals from upstream.
		 */
		LATEST,
		/**
		 * Buffer all signals if the downstream can't keep up.
		 * <p>
		 * Warning! This does unbounded buffering and may lead to {@link OutOfMemoryError}.
		 */
		BUFFER
	}
}
