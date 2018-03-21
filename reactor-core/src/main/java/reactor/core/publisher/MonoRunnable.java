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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Executes the runnable whenever a Subscriber subscribes to this Mono.
 */
final class MonoRunnable<T> extends Mono<T>
        implements Callable<Void>, Scannable {

    final Runnable run;
    
    MonoRunnable(Runnable run) {
        this.run = Objects.requireNonNull(run, "run");
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        try {
            run.run();
        } catch (Throwable ex) {
            Operators.error(actual, Operators.onOperatorError(ex, actual.currentContext()));
            return;
        }
        Operators.complete(actual);
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
        return null; //no particular key to be represented, still useful in hooks
    }
}
