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
import java.util.concurrent.CompletableFuture;

import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Receiver;

/**
 * Emits the value or error produced by the wrapped CompletableFuture.
 * <p>
 * Note that if Subscribers cancel their subscriptions, the CompletableFuture
 * is not cancelled.
 *
 * @param <T> the value type
 */
final class MonoCompletableFuture<T>
extends Mono<T>
        implements Receiver, Fuseable {

    final CompletableFuture<? extends T> future;

    public MonoCompletableFuture(CompletableFuture<? extends T> future) {
        this.future = Objects.requireNonNull(future, "future");
    }

    @Override
    public Object upstream() {
        return future;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        Operators.DeferredScalarSubscriber<T, T>
                sds = new Operators.DeferredScalarSubscriber<>(s);

        s.onSubscribe(sds);

        if (sds.isCancelled()) {
            return;
        }

        future.whenComplete((v, e) -> {
            if (e != null) {
                s.onError(e);
            } else if (v != null) {
                sds.complete(v);
            } else {
                s.onComplete();
            }
        });
    }
}
