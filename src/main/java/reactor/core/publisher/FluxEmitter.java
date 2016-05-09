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

/**
 * Wrapper API around a downstream Subscriber for emittin any number of
 * next signals followed by zero or one onError/onComplete.
 * <p>
 * One should call {@link FluxEmitter#setBackpressureHandling(BackpressureHandling)}
 * before emitting any signal.
 * @param <T> the value type
 */
public interface FluxEmitter<T> {
    
    enum BackpressureHandling {
        IGNORE,
        ERROR,
        DROP,
        LATEST,
        BUFFER
    }
    
    void setBackpressureHandling(BackpressureHandling mode);
    
    void next(T value);
    
    void fail(Throwable error);
    
    void complete();
    
    void stop();
    
    void setCancellation(Cancellation c);
}
