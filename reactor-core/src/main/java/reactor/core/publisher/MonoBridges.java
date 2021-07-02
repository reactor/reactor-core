/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.function.Function;

import org.reactivestreams.Publisher;

/**
 * Utilities to avoid vararg array copying overhead when relaying vararg parameters
 * to underlying Java methods from their corresponding Kotlin functions.
 * <p>
 * When <a href="https://youtrack.jetbrains.com/issue/KT-17043">this</a> issue is
 * resolved, uses of these bridge methods can be removed.
 *
 * @author DoHyung Kim
 * @since 3.1
 */
final class MonoBridges {

    static <R> Mono<R> zip(Function<? super Object[], ? extends R> combinator, Mono<?>[] monos) {
        return Mono.zip(combinator, monos);
    }

    static Mono<Void> when(Publisher<?>[] sources) {
        return Mono.when(sources);
    }
}
