/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Emits the value or error produced by the wrapped CompletableFuture.
 * <p>
 * Note that if Subscribers cancel their subscriptions, the CompletableFuture
 * is cancelled if specified.
 *
 * @param <T> the value type
 */
final class MonoCompletableFuture<T> extends Mono<T>
        implements Fuseable, Scannable {

    final CompletableFuture<? extends T> future;
    final boolean suppressCancellation;

    MonoCompletableFuture(CompletableFuture<? extends T> future, boolean suppressCancellation) {
        this.future = Objects.requireNonNull(future, "future");
        this.suppressCancellation = suppressCancellation;
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;
        return null;
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        MonoCompletableFutureSubscription<T> s = suppressCancellation
                ? new MonoCompletableFutureSubscription<>(actual)
                : new CancellingMonoCompletableFutureSubscription<>(actual, future);

        actual.onSubscribe(s);

        if (s.isCancelled()) {
            return;
        }

        future.handle(s);
    }

    static class MonoCompletableFutureSubscription<T> extends Operators.MonoSubscriber<T, T>
            implements BiFunction<T, Throwable, Void> {

        public MonoCompletableFutureSubscription(CoreSubscriber<? super T> actual) {
            super(actual);
        }

        @Override
        public final Void apply(@Nullable T v, @Nullable Throwable e) {
            if (this.isCancelled()) {
                //nobody is interested in the Mono anymore, don't risk dropping errors
                Context ctx = this.currentContext();
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
                    this.complete(v);
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
        }

    }

    static final class CancellingMonoCompletableFutureSubscription<T>
            extends MonoCompletableFutureSubscription<T> {
        final CompletableFuture<? extends T> future;

        CancellingMonoCompletableFutureSubscription(CoreSubscriber<? super T> actual, CompletableFuture<? extends T> future) {
            super(actual);
            this.future = future;
        }

        @Override
        public void cancel() {
            super.cancel();
            try {
                this.future.cancel(true);
            } catch (Throwable t) {
                Operators.onErrorDropped(t, this.currentContext());
            }
        }
    }
}
