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

import java.util.Objects;

import org.reactivestreams.Subscriber;
import reactor.core.subscription.ScalarSubscription;
import reactor.core.support.ReactiveState;
import reactor.fn.Supplier;

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 *
 * @since 2.5
 */
public final class MonoJust<T> extends reactor.Mono<T>
        implements Supplier<T>, ReactiveState.Factory, ReactiveState.Upstream {

    final T value;

    public MonoJust(T value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    @Override
    public T get() {
        return value;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        s.onSubscribe(new ScalarSubscription<>(s, value));
    }

    @Override
    public Object upstream() {
        return value;
    }
}