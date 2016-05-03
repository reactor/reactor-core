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
import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import reactor.core.flow.Fuseable;
import reactor.core.subscriber.DeferredScalarSubscriber;
import reactor.core.tuple.*;
import reactor.core.util.*;

/**
 * Waits for all Mono sources to produce a value or terminate, and if
 * all of them produced a value, emit a Tuple of those values; otherwise
 * terminate.
 *
 * @param <T> the source value types
 */
final class MonoWhen<T> extends Mono<T[]> implements Fuseable {

    final boolean delayError;
    
    final Mono<? extends T>[] sources;

    final Iterable<? extends Mono<? extends T>> sourcesIterable;

    @SafeVarargs
    public MonoWhen(boolean delayError, Mono<? extends T>... sources) {
        this.delayError = delayError;
        this.sources = Objects.requireNonNull(sources, "sources");
        this.sourcesIterable = null;
    }

    public MonoWhen(boolean delayError, Iterable<? extends Mono<? extends T>> sourcesIterable) {
        this.delayError = delayError;
        this.sources = null;
        this.sourcesIterable = Objects.requireNonNull(sourcesIterable, "sourcesIterable");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void subscribe(Subscriber<? super T[]> s) {
        Mono<? extends T>[] a;
        int n = 0;
        if (sources != null) {
            a = sources;
            n = a.length;
        } else {
            a = new Mono[8];
            for (Mono<? extends T> m : sourcesIterable) {
                if (n == a.length) {
                    Mono<? extends T>[] b = new Mono[n + (n >> 2)];
                    System.arraycopy(a, 0, b, 0, n);
                    a = b;
                }
                a[n++] = m;
            }
        }
        
        if (n == 0) {
            EmptySubscription.complete(s);
            return;
        }
        
        MonoWhenCoordinator<T> parent = new MonoWhenCoordinator<>(s, n, delayError);
        s.onSubscribe(parent);
        parent.subscribe(a);
    }
    
    static final class MonoWhenCoordinator<T> 
    extends DeferredScalarSubscriber<T, T[]>
    implements Subscription {
        final MonoWhenSubscriber<T>[] subscribers;
        
        final boolean delayError;
        
        volatile int done;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<MonoWhenCoordinator> DONE =
                AtomicIntegerFieldUpdater.newUpdater(MonoWhenCoordinator.class, "done");

        @SuppressWarnings("unchecked")
        public MonoWhenCoordinator(Subscriber<? super T[]> subscriber, int n, boolean delayError) {
            super(subscriber);
            this.delayError = delayError;
            subscribers = new MonoWhenSubscriber[n];
            for (int i = 0; i < n; i++) {
                subscribers[i] = new MonoWhenSubscriber<>(this);
            }
        }
        
        void subscribe(Mono<? extends T>[] sources) {
            MonoWhenSubscriber<T>[] a = subscribers;
            for (int i = 0; i < a.length; i++) {
                sources[i].subscribe(a[i]);
            }
        }
        
        void signalError(Throwable t) {
            if (delayError) {
                signal();
            } else {
                int n = subscribers.length;
                if (DONE.getAndSet(this, n) != n) {
                    cancel();
                    subscriber.onError(t);
                }
            }
        }
        
        @SuppressWarnings("unchecked")
        void signal() {
            MonoWhenSubscriber<T>[] a = subscribers;
            int n = a.length;
            if (DONE.incrementAndGet(this) != n) {
                return;
            }
            
            Object[] o = new Object[n];
            Throwable error = null;
            Throwable compositeError = null;
            boolean hasEmpty = false;
            
            for (int i = 0; i < a.length; i++) {
                MonoWhenSubscriber<T> m = a[i];
                T v = m.value;
                if (v != null) {
                    o[i] = v;
                } else {
                    Throwable e = m.error;
                    if (e != null) {
                        if (compositeError != null) {
                            compositeError.addSuppressed(e);
                        } else
                        if (error != null) {
                            compositeError = new Throwable("Multiple errors");
                            compositeError.addSuppressed(error);
                            compositeError.addSuppressed(e);
                        } else {
                            error = e;
                        }
                    } else {
                        hasEmpty = true;
                    }
                }
            }
            
            if (compositeError != null) {
                subscriber.onError(compositeError);
            } else
            if (error != null) {
                subscriber.onError(error);
            } else
            if (hasEmpty) {
                subscriber.onComplete();
            } else {
                complete((T[])o);
            }
        }
        
        @Override
        public void cancel() {
            if (!isCancelled()) {
                super.cancel();
                for (MonoWhenSubscriber<T> ms : subscribers) {
                    ms.cancel();
                }
            }
        }
    }
    
    static final class MonoWhenSubscriber<T> implements Subscriber<T> {
        
        final MonoWhenCoordinator<T> parent;

        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<MonoWhenSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(MonoWhenSubscriber.class, Subscription.class, "s");
        
        T value;
        Throwable error;
        
        public MonoWhenSubscriber(MonoWhenCoordinator<T> parent) {
            this.parent = parent;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (BackpressureUtils.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            } else {
                s.cancel();
            }
        }
        
        @Override
        public void onNext(T t) {
            if (value == null) {
                value = t;
                parent.signal();
            }
        }
        
        @Override
        public void onError(Throwable t) {
            error = t;
            parent.signalError(t);
        }
        
        @Override
        public void onComplete() {
            if (value == null) {
                parent.signal();
            }
        }
        
        void cancel() {
            BackpressureUtils.terminate(S, this);
        }
    }
}
