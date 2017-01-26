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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import reactor.core.Fuseable;
import reactor.core.MultiReceiver;

/**
 * Concatenates a several Mono sources with a final Mono source by
 * ignoring values from the first set of sources and emitting the value
 * the last Mono source generates.
 *
 * @param <T> the final value type
 */
final class MonoThenIgnore<T> extends Mono<T> implements Fuseable, MultiReceiver {

    final Mono<?>[] ignore;
    
    final Mono<T> last;
    
    public MonoThenIgnore(Mono<?>[] ignore, Mono<T> last) {
        this.ignore = Objects.requireNonNull(ignore, "ignore");
        this.last = Objects.requireNonNull(last, "last");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        MonoThenIgnoreMain<T> manager = new MonoThenIgnoreMain<>(s, ignore, last);
        s.onSubscribe(manager);
        
        manager.drain();
    }

    @Override
    public Iterator<?> upstreams() {
        List<Mono<?>> r = Arrays.asList(ignore);
        r.add(last);
        return r.iterator();
    }

    @Override
    public long upstreamCount() {
        return ignore.length + 1;
    }
    
    /**
     * Shifts the current last Mono into the ignore array and sets up a new last Mono instance.
     * @param <U> the new last value type
     * @param newLast the new last Mono instance
     * @return the new operator set up
     */
    public <U> MonoThenIgnore<U> shift(Mono<U> newLast) {
        Objects.requireNonNull(newLast, "newLast");
        Mono<?>[] a = ignore;
        int n = a.length;
        Mono<?>[] b = new Mono[n + 1];
        System.arraycopy(a, 0, b, 0, n);
        b[n] = last;
        
        return new MonoThenIgnore<>(b, newLast);
    }
    
    static final class MonoThenIgnoreMain<T>
            extends Operators.MonoSubscriber<T, T> {
        final MonoThenIgnoreSubscriber ignore;
        
        final MonoThenAcceptSubscriber<T> accept;
        
        final Mono<?>[] ignoreMonos;
        
        final Mono<T> lastMono;

        int index;
        
        volatile boolean active;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<MonoThenIgnoreMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(MonoThenIgnoreMain.class, "wip");
        
        public MonoThenIgnoreMain(Subscriber<? super T> subscriber, Mono<?>[] ignoreMonos, Mono<T> lastMono) {
            super(subscriber);
            this.ignoreMonos = ignoreMonos;
            this.lastMono = lastMono;
            this.ignore = new MonoThenIgnoreSubscriber(this);
            this.accept = new MonoThenAcceptSubscriber<>(this);
        }

        @SuppressWarnings("unchecked")
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            for (;;) {
                if (isCancelled()) {
                    return;
                }
                
                if (!active) {
                    
                    Mono<?>[] a = ignoreMonos;
                    int i = index;
                    if (i == a.length) {
                        ignore.clear();
                        Mono<T> m = lastMono;
                        if (m instanceof Callable) {
                            T v;
                            try {
                                v = ((Callable<T>)m).call();
                            }
                            catch (Throwable ex) {
	                            actual.onError(Operators.onOperatorError(ex));
	                            return;
                            }
                            
                            if (v == null) {
                                actual.onComplete();
                            }
                            else {
                                complete(v);
                            }
                            return;
                        }
                        
                        active = true;
                        m.subscribe(accept);
                    } else {
                        Mono<?> m = a[i];
                        index = i + 1;
                        
                        if (m instanceof Callable) {
                            try {
                                ((Callable<?>)m).call();
                            }
                            catch (Throwable ex) {
	                            actual.onError(Operators.onOperatorError(ex));
	                            return;
                            }
                            
                            continue;
                        }
                        
                        active = true;
                        m.subscribe(ignore);
                    }
                }
                if (WIP.decrementAndGet(this) == 0) {
                    break;
                }
            }
        }
        
        @Override
        public void cancel() {
            super.cancel();
            ignore.cancel();
            accept.cancel();
        }
        
        void ignoreDone() {
            active = false;
            drain();
        }
    }
    
    static final class MonoThenIgnoreSubscriber implements Subscriber<Object> {
        final MonoThenIgnoreMain<?> parent;
        
        volatile Subscription s;
        static final AtomicReferenceFieldUpdater<MonoThenIgnoreSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(MonoThenIgnoreSubscriber.class, Subscription.class, "s");
        
        public MonoThenIgnoreSubscriber(MonoThenIgnoreMain<?> parent) {
            this.parent = parent;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.replace(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(Object t) {
            // ignored
        }
        
        @Override
        public void onError(Throwable t) {
            this.parent.onError(t);
        }
        
        @Override
        public void onComplete() {
            this.parent.ignoreDone();
        }
        
        void cancel() {
            Operators.terminate(S, this);
        }
        
        void clear() {
            S.lazySet(this, null);
        }
    }
    
    static final class MonoThenAcceptSubscriber<T> implements Subscriber<T> {
        final MonoThenIgnoreMain<T> parent;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<MonoThenAcceptSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(MonoThenAcceptSubscriber.class, Subscription.class, "s");

        boolean done;
        
        public MonoThenAcceptSubscriber(MonoThenIgnoreMain<T> parent) {
            this.parent = parent;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(T t) {
            if (done) {
                Operators.onNextDropped(t);
                return;
            }
            done = true;
            this.parent.complete(t);
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                Operators.onErrorDropped(t);
                return;
            }
            done = true;
            this.parent.onError(t);
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            this.parent.onComplete();
        }
        
        void cancel() {
            Operators.terminate(S, this);
        }
    }
}
