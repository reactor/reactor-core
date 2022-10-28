/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.context.Context;

/**
 * Emits the value or error produced by the wrapped CompletionStage.
 * <p>
 * Note that if Subscribers cancel their subscriptions, the CompletionStage
 * is not cancelled.
 *
 * @param <T> the value type
 */
final class MonoCompletionStage<T> extends Mono<T>
        implements Fuseable, Scannable {

    final CompletionStage<? extends T> future;
    final boolean suppressCancellation;

    MonoCompletionStage(CompletionStage<? extends T> future, boolean suppressCancellation) {
        this.future = Objects.requireNonNull(future, "future");
        this.suppressCancellation = suppressCancellation;
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        Operators.MonoSubscriber<T, T>
                sds = suppressCancellation
                ? new Operators.MonoSubscriber<>(actual)
                : new Operators.MonoSubscriber<T, T>(actual) {
                    @Override
                    public void cancel() {
                        super.cancel();
                        if (future instanceof Future) {
                            ((Future<?>) future).cancel(true);
                        }
                    }
                };

        actual.onSubscribe(sds);

        if (sds.isCancelled()) {
            return;
        }

        future.handle((v, e) -> {
            if (sds.isCancelled()) {
                //nobody is interested in the Mono anymore, don't risk dropping errors
                Context ctx = sds.currentContext();
                if (e == null || e instanceof CancellationException) {
                    //we discard any potential value and ignore Future cancellations
                    Operators.onDiscard(v, ctx);
                }
                else {
                    //we make sure we keep _some_ track of a Future failure AFTER the Mono cancellation
                    Operators.onErrorDropped(e, ctx);
                    //and we discard any potential value just in case both e and v are not null
                    Operators.onDiscard(v, ctx);
                }

                return null;
            }
            try {
                if (e instanceof CompletionException) {
                    actual.onError(e.getCause());
                }
                else if (e != null) {
                    actual.onError(e);
                }
                else if (v != null) {
                    sds.complete(v);
                }
                else {
                    actual.onComplete();
                }
            }
            catch (Throwable e1) {
                Operators.onErrorDropped(e1, actual.currentContext());
                throw Exceptions.bubble(e1);
            }
            return null;
        });
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;
        return null;
    }
}
