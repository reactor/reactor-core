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

import java.util.Collection;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;

/**
 * Buffers all values from the source Publisher and emits it as a single Collection.
 *
 * @param <T> the source value type
 * @param <C> the collection type that takes any supertype of T
 */
final class MonoBufferAll<T, C extends Collection<? super T>> extends MonoSource<T, C>
        implements Fuseable {

    final Supplier<C> collectionSupplier;
    
    protected MonoBufferAll(Publisher<? extends T> source, Supplier<C> collectionSupplier) {
        super(source);
        this.collectionSupplier = collectionSupplier;
    }

    @Override
    public void subscribe(Subscriber<? super C> s) {
        C collection;
        
        try {
            collection = collectionSupplier.get();
        } catch (Throwable ex) {
            Operators.error(s, Exceptions.mapOperatorError(null, ex));
            return;
        }
        
        if (collection == null) {
            Operators.error(s, Exceptions.mapOperatorError(null, new NullPointerException
                    ("The " +
                    "collectionSupplier " +
                    "returned a null collection")));
            return;
        }
        
        source.subscribe(new MonoBufferAllSubscriber<>(s, collection));
    }
    
    static final class MonoBufferAllSubscriber<T, C extends Collection<? super T>>
            extends Operators.DeferredScalarSubscriber<T, C>
    implements Subscriber<T>, Subscription {
        
        final Subscriber<? super C> actual;
        
        C collection;
        
        Subscription s;

        public MonoBufferAllSubscriber(Subscriber<? super C> actual, C collection) {
            super(actual);
            this.actual = actual;
            this.collection = collection;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
                
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(T t) {
            collection.add(t);
        }
        
        @Override
        public void onError(Throwable t) {
            collection = null;
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            C c = collection;
            collection = null;
            
            complete(c);
        }
        
        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }
    }
}
