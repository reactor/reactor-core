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
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Emits the value or error produced by the wrapped CompletionStage.
 *
 * @param <T> the value type
 */
final class MonoCompletionStage<T> extends Mono<T>
        implements Fuseable, Scannable {

    final CompletionStage<? extends T> stage;
    @Nullable
    final Runnable cancellationHandler;

    MonoCompletionStage(CompletionStage<? extends T> stage, @Nullable Runnable cancellationHandler) {
        this.stage = Objects.requireNonNull(stage, "future");
        this.cancellationHandler = cancellationHandler;
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        MonoCompletionStageSubscription<T> s =
                new MonoCompletionStageSubscription<>(actual, cancellationHandler);

        actual.onSubscribe(s);

        if (s.isCancelled()) {
            return;
        }

        stage.handle(s);
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;
        return null;
    }


    static final class MonoCompletionStageSubscription<T> extends Operators.MonoSubscriber<T, T>
            implements BiFunction<T, Throwable, Void> {

        @Nullable
        final Runnable handler;

        public MonoCompletionStageSubscription(CoreSubscriber<? super T> actual, @Nullable Runnable handler) {
            super(actual);
            this.handler = handler;
        }

        @Override
        public Void apply(@Nullable T v, @Nullable Throwable e) {
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

        @Override
        public void cancel() {
            super.cancel();
            if (handler != null) {
                try {
                    this.handler.run();
                }
                catch (Throwable t) {
                    Operators.onErrorDropped(t, this.currentContext());
                }
            }
        }
    }
}
