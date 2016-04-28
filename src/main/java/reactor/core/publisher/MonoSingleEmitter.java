/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

import org.reactivestreams.*;

import reactor.core.flow.Cancellation;
import reactor.core.util.BackpressureUtils;
import rx.exceptions.Exceptions;

/**
 * Wraps a the downstream Subscriber into a single emission object
 * and calls the given callback to produce a signal (a)synchronously.
 * @param <T> the value type
 */
final class MonoSingleEmitter<T> extends Mono<T> {

    final Consumer<SingleEmitter<T>> callback;

    public MonoSingleEmitter(Consumer<SingleEmitter<T>> callback) {
        this.callback = callback;
    }

    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        DefaultSingleEmitter<T> emitter = new DefaultSingleEmitter<>(s);
        
        s.onSubscribe(emitter);
        
        try {
            callback.accept(emitter);
        } catch (Throwable ex) {
            Exceptions.throwIfFatal(ex);
            emitter.error(ex);
        }
    }

    static final class DefaultSingleEmitter<T> implements SingleEmitter<T>, Subscription {
        final Subscriber<? super T> actual;
        
        volatile Cancellation cancellation;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<DefaultSingleEmitter, Cancellation> CANCELLATION =
                AtomicReferenceFieldUpdater.newUpdater(DefaultSingleEmitter.class, Cancellation.class, "cancellation");
        
        volatile int state;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<DefaultSingleEmitter> STATE =
                AtomicIntegerFieldUpdater.newUpdater(DefaultSingleEmitter.class, "state");

        T value;
        
        static final Cancellation CANCELLED = () -> { };
        
        static final int NO_REQUEST_NO_VALUE = 0;
        static final int NO_REQUEST_HAS_VALUE = 1;
        static final int HAS_REQUEST_NO_VALUE = 2;
        static final int HAS_REQUEST_HAS_VALUE = 3;
        
        public DefaultSingleEmitter(Subscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void complete() {
            if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
                cancellation = CANCELLED;
                actual.onComplete();
            }
        }

        @Override
        public void complete(T value) {
            for (;;) {
                int s = state;
                if (s == HAS_REQUEST_HAS_VALUE || s == NO_REQUEST_HAS_VALUE) {
                    return;
                }
                if (s == HAS_REQUEST_NO_VALUE) {
                    if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
                        cancellation = CANCELLED;
                        actual.onNext(value);
                        actual.onComplete();
                    }
                    return;
                }
                this.value = value;
                if (STATE.compareAndSet(this, s, NO_REQUEST_HAS_VALUE)) {
                    return;
                }
            }
        }

        @Override
        public void error(Throwable e) {
            if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
                cancellation = CANCELLED;
                actual.onError(e);
            }
        }

        @Override
        public void setCancellation(Cancellation c) {
            if (!CANCELLATION.compareAndSet(this, null, c)) {
                if (cancellation != CANCELLED && c != null) {
                    c.dispose();
                }
            }
        }

        @Override
        public void request(long n) {
            if (BackpressureUtils.validate(n)) {
                for (;;) {
                    int s = state;
                    if (s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE) {
                        return;
                    }
                    if (s == NO_REQUEST_HAS_VALUE) {
                        if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
                            cancellation = CANCELLED;
                            actual.onNext(value);
                            actual.onComplete();
                        }
                        return;
                    }
                    if (STATE.compareAndSet(this, s, HAS_REQUEST_NO_VALUE)) {
                        return;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
                value = null;
            }
            Cancellation c = cancellation;
            if (c != CANCELLED) {
                c = CANCELLATION.getAndSet(this, CANCELLED);
                if (c != null && c != CANCELLED) {
                    c.dispose();
                }
            }
        }
        
        
    }
}
