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

import reactor.core.flow.Cancellation;
import reactor.core.subscriber.SignalEmitter;

/**
 * Wrapper API around a downstream Subscriber for emitting any number of
 * next signals followed by zero or one onError/onComplete.
 * <p>
 * @param <T> the value type
 */
public interface FluxEmitter<T> extends SignalEmitter<T>  {
    
    /**
     * Enumeration for backpressure handling.
     */
    enum BackpressureHandling {
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
    
    /**
     * Associate a cancellation-based resource with this FluxEmitter
     * that will be disposed in case the downstream cancels the sequence
     * via {@link org.reactivestreams.Subscription#cancel()}.
     * @param c
     */
    void setCancellation(Cancellation c);
}
