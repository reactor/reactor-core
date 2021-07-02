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

import java.util.Objects;

import reactor.core.CoreSubscriber;

/**
 * Emits a default value if the wrapped Mono is empty.
 * @param <T> the value type
 */
final class MonoDefaultIfEmpty<T> extends InternalMonoOperator<T, T> {
    final T defaultValue;

    MonoDefaultIfEmpty(Mono<? extends T> source, T defaultValue) {
        super(source);
        this.defaultValue = Objects.requireNonNull(defaultValue, "defaultValue");
    }

    @Override
    public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
        return new FluxDefaultIfEmpty.DefaultIfEmptySubscriber<>(actual, defaultValue);
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
        return super.scanUnsafe(key);
    }
}
