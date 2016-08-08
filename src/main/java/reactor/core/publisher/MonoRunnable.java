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
import reactor.core.Exceptions;

/**
 * Executes the runnable whenever a Subscriber subscribes to this Mono.
 */
final class MonoRunnable extends Mono<Void> {

    final Runnable run;
    
    public MonoRunnable(Runnable run) {
        this.run = Objects.requireNonNull(run, "run");
    }

    @Override
    public void subscribe(Subscriber<? super Void> s) {
        try {
            run.run();
        } catch (Throwable ex) {
            Operators.error(s, Exceptions.onOperatorError(ex));
            return;
        }
        Operators.complete(s);
    }
    
    @Override
    public Void block() {
        run.run();
        return null;
    }

}
