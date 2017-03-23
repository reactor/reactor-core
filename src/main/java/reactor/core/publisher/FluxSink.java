/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Subscriber;
import reactor.core.Cancellation;
import reactor.core.Disposable;


/**
 * Wrapper API around a downstream Subscriber for emitting any number of
 * next signals followed by zero or one onError/onComplete.
 * <p>
 * @param <T> the value type
 */
public interface FluxSink<T> {

	/**
     * @see Subscriber#onComplete()
     */
    void complete();

    /**
     * @see Subscriber#onError(Throwable)
     * @param e the exception to signal, not null
     */
    void error(Throwable e);

    /**
     * Try emitting, might throw an unchecked exception.
     * @see Subscriber#onNext(Object)
     * @param t the value to emit, not null
     * @return this sink
     */
    FluxSink<T> next(T t);

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
	 * Ensures that calls to next, error and complete are properly serialized.
	 * @return a new serialized {@link FluxSink}
	 */
	FluxSink<T> serialize();

	/**
	 * Associates a disposable resource with this FluxSink
	 * that will be disposed in case the downstream cancels the sequence
	 * via {@link org.reactivestreams.Subscription#cancel()}.
	 * @param d the disposable callback to use
	 * @return the {@link FluxSink} with resource to be disposed on cancel signal
	 */
	FluxSink<T> onCancel(Disposable d);

	/**
	 * Associates a disposable resource with this FluxSink
	 * that will be disposed on the first terminate signal which may be
	 * a cancel, complete or error signal.
	 * @param d the disposable callback to use
	 * @return the {@link FluxSink} with resource to be disposed on first terminate signal
	 */
	FluxSink<T> onDispose(Disposable d);


    /**
     * Associate a cancellation-based resource with this FluxSink
     * that will be disposed in case the downstream cancels the sequence
     * via {@link org.reactivestreams.Subscription#cancel()}.
     * @param c the cancellation callback to use
     * @return this sink
     */
	@Deprecated
    FluxSink<T> setCancellation(Cancellation c);

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
