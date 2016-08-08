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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import reactor.core.Cancellation;
import reactor.core.Fuseable;
import reactor.core.Fuseable.QueueSubscription;
import reactor.core.Producer;
import reactor.core.Trackable;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.Exceptions;
import reactor.util.concurrent.QueueSupplier;

/**
 * Provides a multi-valued emitter API for a callback that is called for
 * each individual Subscriber.
 *
 * @param <T> the value type
 */
final class FluxCreate<T> extends Flux<T> {
    
    final Consumer<? super FluxSink<T>> emitter;
    
    final OverflowStrategy backpressure;
    
    public FluxCreate(Consumer<? super FluxSink<T>> emitter, FluxSink.OverflowStrategy backpressure) {
        this.emitter = Objects.requireNonNull(emitter, "emitter");
        this.backpressure = Objects.requireNonNull(backpressure, "backpressure");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        DefaultFluxSink<T> dfe = new DefaultFluxSink<>(s, backpressure);
        s.onSubscribe(dfe);
        
        try {
            emitter.accept(dfe);
        } catch (Throwable ex) {
            dfe.error(Exceptions.onOperatorError(ex));
        }
    }
    
    static final class DefaultFluxSink<T>
            implements FluxSink<T>, QueueSubscription<T>, Producer, Trackable {

        final Subscriber<? super T> actual; 
        
        final OverflowStrategy handling;
        
        Queue<T> queue;

        volatile T latest;
        
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<DefaultFluxSink, Object> LATEST =
        AtomicReferenceFieldUpdater.newUpdater(DefaultFluxSink.class, Object.class, "latest");
        
        volatile boolean done;
        Throwable error;
        
        volatile Cancellation cancel;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<DefaultFluxSink, Cancellation> CANCEL =
                AtomicReferenceFieldUpdater.newUpdater(DefaultFluxSink.class, Cancellation.class, "cancel");
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<DefaultFluxSink> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(DefaultFluxSink.class, "requested");
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<DefaultFluxSink> WIP =
                AtomicIntegerFieldUpdater.newUpdater(DefaultFluxSink.class, "wip");
        
        static final Cancellation CANCELLED = () -> { };
        
        public DefaultFluxSink(Subscriber<? super T> actual, OverflowStrategy handling) {
            this.actual = actual;
            this.queue = QueueSupplier.<T>unbounded().get();
            this.handling = handling;
        }
        
        @Override
        public void next(T value) {
            if (value == null) {
                error(new NullPointerException("value is null"));
                return;
            }
            if (isCancelled() || done) {
                Exceptions.onNextDropped(value);
                return;
            }
            switch (this.handling) {
            case IGNORE: {
                actual.onNext(value);
                break;
            }
            case ERROR: {
                if (requested != 0L) {
                    actual.onNext(value);
                    if (requested != Long.MAX_VALUE) {
                        REQUESTED.decrementAndGet(this);
                    }
                } else {
                    error(new IllegalStateException("Could not emit value due to lack of request"));
                }
                break;
            }
            case BUFFER: {
                queue.offer(value);
                drain();
                break;
            }
            case LATEST: {
                LATEST.lazySet(this, value);
                drainLatest();
                break;
            }
            case DROP: {
                if (requested != 0L) {
                    actual.onNext(value);
                    if (requested != Long.MAX_VALUE) {
                        REQUESTED.decrementAndGet(this);
                    }
                }
                break;
            }
            }
        }
        
        @Override
        public void error(Throwable error) {
            if (error == null) {
                error = new NullPointerException("error is null");
            }
            if (isCancelled() || done) {
                Exceptions.onErrorDropped(error);
                return;
            }
            done = true;
            switch (this.handling) {
            case IGNORE:
            case ERROR:
            case DROP:
                cancel();
                actual.onError(error);
                break;
            case BUFFER:
                this.error = error;
                done = true;
                drain();
                break;
            case LATEST:
                this.error = error;
                done = true;
                drainLatest();
                break;
            }
        }

        @Override
        public boolean isCancelled() {
            return cancel == CANCELLED;
        }
        
        @Override
        public void complete() {
            if (isCancelled() || done) {
                return;
            }
            done = true;
            
            switch (this.handling) {
            case IGNORE:
            case ERROR:
            case DROP:
                cancel();
                actual.onComplete();
                break;
            case BUFFER:
                drain();
                break;
            case LATEST:
                drainLatest();
                break;
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            int missed = 1;
            final Queue<T> q = queue;
            final Subscriber<? super T> a = actual;
            
            for (;;) {
                
                long r = requested;
                long e = 0L;
                
                while (e != r) {
                    if (isCancelled()) {
                        q.clear();
                        return;
                    }
                    
                    boolean d = done;
                    T v = q.poll();
                    boolean empty = v == null;
                    
                    if (d && empty) {
                        cancelResource();
                        q.clear();
                        Throwable ex = error;
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }
                    
                    if (empty) {
                        break;
                    }
                    
                    a.onNext(v);
                    
                    e++;
                }
                
                if (e == r) {
                    if (isCancelled()) {
                        q.clear();
                        return;
                    }
                    
                    if (done && q.isEmpty()) {
                        cancelResource();
                        q.clear();
                        Throwable ex = error;
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }
                }
                
                if (e != 0L) {
                    if (r != Long.MAX_VALUE) {
                        REQUESTED.addAndGet(this, -e);
                    }
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    return;
                }
            }
        }
        
        void drainLatest() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            int missed = 1;
            final Subscriber<? super T> a = actual;
            
            for (;;) {
                
                long r = requested;
                long e = 0L;
                
                while (e != r) {
                    if (isCancelled()) {
                        LATEST.lazySet(this, null);
                        return;
                    }
                    
                    boolean d = done;
                    @SuppressWarnings("unchecked")
                    T v = (T)LATEST.getAndSet(this, null);
                    boolean empty = v == null;
                    
                    if (d && empty) {
                        cancelResource();
                        Throwable ex = error;
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }
                    
                    if (empty) {
                        break;
                    }
                    
                    a.onNext(v);
                    
                    e++;
                }
                
                if (e == r) {
                    if (isCancelled()) {
                        LATEST.lazySet(this, null);
                        return;
                    }
                    
                    if (done && latest == null) {
                        cancelResource();
                        Throwable ex = error;
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }
                }
                
                if (e != 0L) {
                    if (r != Long.MAX_VALUE) {
                        REQUESTED.addAndGet(this, -e);
                    }
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    return;
                }
            }
        }
        @Override
        public void setCancellation(Cancellation c) {
            if (!CANCEL.compareAndSet(this, null, c)) {
                if (cancel != CANCELLED && c != null) {
                    c.dispose();
                }
            }
        }
        
        @Override
        public int requestFusion(int requestedMode) {
// FIXME enable
//            if ((requestedMode & Fuseable.ASYNC) != 0) {
//                return Fuseable.ASYNC;
//            }
            return Fuseable.NONE;
        }
        
        @Override
        public T poll() {
            return null;
        }
        
        @Override
        public boolean isEmpty() {
            return false;
        }
        
        @Override
        public int size() {
            return 0;
        }
        
        @Override
        public void clear() {
        }
        
        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                Operators.getAndAddCap(REQUESTED, this, n);
                if (handling == OverflowStrategy.BUFFER) {
                    drain();
                } else
                if (handling == OverflowStrategy.LATEST) {
                    drainLatest();
                }
            }
        }
        
        void cancelResource() {
            Cancellation c = cancel;
            if (c != CANCELLED) {
                c = CANCEL.getAndSet(this, CANCELLED);
                if (c != null && c != CANCELLED) {
                    c.dispose();
                }
            }
        }
        
        @Override
        public void cancel() {
            cancelResource();
            
            if (WIP.getAndIncrement(this) == 0) {
                Queue<T> q = queue;
                if (q != null) {
                    q.clear();
                }
            }
        }

        @Override
        public long requestedFromDownstream() {
            return requested;
        }

        @Override
        public long getCapacity() {
            return Long.MAX_VALUE;
        }

        @Override
        public long getPending() {
            return queue != null ? queue.size() : (latest != null ? 1 : 0);
        }

        @Override
        public Throwable getError() {
            return error;
        }

        @Override
        public Object downstream() {
            return actual;
        }
    }
}
