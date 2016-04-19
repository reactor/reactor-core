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

import java.util.concurrent.Callable;

import org.reactivestreams.Subscriber;
import reactor.core.subscriber.DeferredScalarSubscriber;
import reactor.core.util.Exceptions;

/**
 * For each subscriber, a Supplier is invoked and the returned value emitted.
 * @param <T> the value type;
 */
final class FluxCallable<T> extends Flux<T> implements Callable<T> {

    final Callable<T> callable;
    
    FluxCallable(Callable<T> callable) {
        this.callable = callable;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        DeferredScalarSubscriber<T, T> wrapper = new DeferredScalarSubscriber<>(s);
        s.onSubscribe(wrapper);
        
        T v;
        try {
            v = callable.call();
        } catch (Throwable ex) {
            Exceptions.throwIfFatal(ex);
            s.onError(ex);
            return;
        }
        
        wrapper.complete(v);
    }

    @Override
    public T call() throws Exception {
        return callable.call();
    }
}
