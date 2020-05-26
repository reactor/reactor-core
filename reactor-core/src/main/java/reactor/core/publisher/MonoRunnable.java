/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * Executes the runnable whenever a Subscriber subscribes to this Mono.
 */
final class MonoRunnable<T> extends Mono<T> implements Callable<Void>, SourceProducer<T>  {

    final Runnable run;
    
    MonoRunnable(Runnable run) {
        this.run = Objects.requireNonNull(run, "run");
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        MonoRunnableEagerSubscription s = new MonoRunnableEagerSubscription();
        actual.onSubscribe(s);
        if (s.isCancelled()) {
            return;
        }

        try {
            run.run();
            actual.onComplete();
        } catch (Throwable ex) {
            actual.onError(Operators.onOperatorError(ex, actual.currentContext()));
        }
    }
    
    @Override
    @Nullable
    public T block(Duration m) {
        run.run();
        return null;
    }

    @Override
    @Nullable
    public T block() {
        run.run();
        return null;
    }

    @Override
    @Nullable
    public Void call() throws Exception {
        run.run();
        return null;
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
        return null;
    }

    static final class MonoRunnableEagerSubscription extends AtomicBoolean implements Subscription {

        @Override
        public void request(long n) {
            //NO-OP
        }

        @Override
        public void cancel() {
            set(true);
        }

        public boolean isCancelled() {
            return get();
        }

    }
}
