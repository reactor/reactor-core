/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.subscriber;

import reactor.core.flow.Cancellation;

/**
 * Wrapper API around an actual downstream Subscriber
 * for emitting nothing, a single value or an error (mutually exclusive).
 * @param <T> the value type emitted
 */
public interface SingleEmitter<T> {
    /**
     * Complete without any value.
     */
    void complete();
    
    /**
     * Complete with the given value.
     * @param value
     */
    void complete(T value);
    
    /**
     * Terminate with the give exception
     * @param e
     */
    void error(Throwable e);
    
    /**
     * Sets a cancellation callback triggered by
     * downstreams cancel().
     * @param c
     */
    void setCancellation(Cancellation c);
}
