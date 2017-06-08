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

import org.reactivestreams.Subscriber;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Executes the runnable whenever a Subscriber subscribes to this Mono.
 */
final class MonoRunnable extends Mono<Void> implements Callable<Void> {

    final Runnable run;
    
    MonoRunnable(Runnable run) {
        this.run = Objects.requireNonNull(run, "run");
    }

    @Override
    public void subscribe(Subscriber<? super Void> s, Context context) {
        try {
            run.run();
        } catch (Throwable ex) {
            Operators.error(s, Operators.onOperatorError(ex));
            return;
        }
        Operators.complete(s);
    }
    
    @Override
    @Nullable
    public Void block(Duration m) {
        run.run();
        return null;
    }

    @Override
    @Nullable
    public Void block() {
        run.run();
        return null;
    }

    @Override
    @Nullable
    public Void call() throws Exception {
        run.run();
        return null;
    }
}
