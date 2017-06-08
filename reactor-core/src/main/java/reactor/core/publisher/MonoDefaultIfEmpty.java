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

import java.util.Objects;

import org.reactivestreams.Subscriber;
import reactor.util.context.Context;

/**
 * Emits a default value if the wrapped Mono is empty.
 * @param <T> the value type
 */
final class MonoDefaultIfEmpty<T> extends MonoOperator<T, T> {
    final T defaultValue;

    MonoDefaultIfEmpty(Mono<? extends T> source, T defaultValue) {
        super(source);
        this.defaultValue = Objects.requireNonNull(defaultValue, "defaultValue");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s, Context ctx) {
        source.subscribe(new FluxDefaultIfEmpty.DefaultIfEmptySubscriber<>(s, defaultValue),
                ctx);
    }
}
