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

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Scannable;


/**
 * Concatenates a several Mono sources with a final Mono source by
 * ignoring values from the first set of sources and emitting the value
 * the last Mono source generates.
 *
 * @param <T> the final value type
 */
final class MonoThenIgnore<T> extends Mono<T> implements Fuseable {

    final Mono<?>[] ignore;
    
    final Mono<T> last;
    
    MonoThenIgnore(Mono<?>[] ignore, Mono<T> last) {
        this.ignore = Objects.requireNonNull(ignore, "ignore");
        this.last = Objects.requireNonNull(last, "last");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        ThenIgnoreMain<T> manager = new ThenIgnoreMain<>(s, ignore, last);
        s.onSubscribe(manager);
        
        manager.drain();
    }
    
    /**
     * Shifts the current last Mono into the ignore array and sets up a new last Mono instance.
     * @param <U> the new last value type
     * @param newLast the new last Mono instance
     * @return the new operator set up
     */
    <U> MonoThenIgnore<U> shift(Mono<U> newLast) {
        Objects.requireNonNull(newLast, "newLast");
        Mono<?>[] a = ignore;
        int n = a.length;
        Mono<?>[] b = new Mono[n + 1];
        System.arraycopy(a, 0, b, 0, n);
        b[n] = last;
        
        return new MonoThenIgnore<>(b, newLast);
    }
    
    static final class ThenIgnoreMain<T>
            extends Operators.MonoSubscriber<T, T> {
        final ThenIgnoreInner ignore;
        
        final ThenAcceptInner<T> accept;
        
        final Mono<?>[] ignoreMonos;

        final Mono<T> lastMono;

        int index;
        
        volatile boolean active;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ThenIgnoreMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(ThenIgnoreMain.class, "wip");
        
        ThenIgnoreMain(Subscriber<? super T> subscriber,
		        Mono<?>[] ignoreMonos, Mono<T> lastMono) {
            super(subscriber);
            this.ignoreMonos = ignoreMonos;
            this.lastMono = lastMono;
            this.ignore = new ThenIgnoreInner(this);
            this.accept = new ThenAcceptInner<>(this);

        }

	    @Override
	    public Stream<? extends Scannable> inners() {
		    return Stream.of(ignore, accept);
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
    
    static final class ThenIgnoreInner implements InnerConsumer<Object> {
        final ThenIgnoreMain<?> parent;
        
        volatile Subscription s;
        static final AtomicReferenceFieldUpdater<ThenIgnoreInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(ThenIgnoreInner.class, Subscription.class, "s");
        
        ThenIgnoreInner(ThenIgnoreMain<?> parent) {
            this.parent = parent;
        }

	    @Override
	    public Object scan(Attr key) {
		    switch (key){
			    case PARENT:
				    return s;
			    case ACTUAL:
				    return parent;
			    case CANCELLED:
				    return s == Operators.cancelledSubscription();
		    }
		    return null;
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
    
    static final class ThenAcceptInner<T> implements InnerConsumer<T> {
        final ThenIgnoreMain<T> parent;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<ThenAcceptInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(ThenAcceptInner.class, Subscription.class, "s");

        boolean done;
        
        ThenAcceptInner(ThenIgnoreMain<T> parent) {
            this.parent = parent;
        }

        @Override
        public Object scan(Attr key) {
            switch (key){
                case PARENT:
                    return s;
                case ACTUAL:
                    return parent;
	            case TERMINATED:
                    return done;
	            case CANCELLED:
	            	return s == Operators.cancelledSubscription();
            }
            return null;
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
