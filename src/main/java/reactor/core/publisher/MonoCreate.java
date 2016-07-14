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

import reactor.core.Cancellation;
import reactor.core.subscriber.SubscriptionHelper;
import reactor.util.Exceptions;

/**
 * Wraps a the downstream Subscriber into a single emission object
 * and calls the given callback to produce a signal (a)synchronously.
 * @param <T> the value type
 */
final class MonoCreate<T> extends Mono<T> {

    final Consumer<MonoEmitter<T>> callback;

    public MonoCreate(Consumer<MonoEmitter<T>> callback) {
        this.callback = callback;
    }

    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        DefaultMonoEmitter<T> emitter = new DefaultMonoEmitter<>(s);
        
        s.onSubscribe(emitter);
        
        try {
            callback.accept(emitter);
        } catch (Throwable ex) {
            Exceptions.throwIfFatal(ex);
            emitter.fail(ex);
        }
    }

    static final class DefaultMonoEmitter<T> implements MonoEmitter<T>, Subscription {
        final Subscriber<? super T> actual;
        
        volatile Cancellation cancellation;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<DefaultMonoEmitter, Cancellation> CANCELLATION =
                AtomicReferenceFieldUpdater.newUpdater(DefaultMonoEmitter.class, Cancellation.class, "cancellation");
        
        volatile int state;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<DefaultMonoEmitter> STATE =
                AtomicIntegerFieldUpdater.newUpdater(DefaultMonoEmitter.class, "state");

        T value;
        
        static final Cancellation CANCELLED = () -> { };
        
        static final int NO_REQUEST_NO_VALUE = 0;
        static final int NO_REQUEST_HAS_VALUE = 1;
        static final int HAS_REQUEST_NO_VALUE = 2;
        static final int HAS_REQUEST_HAS_VALUE = 3;
        
        public DefaultMonoEmitter(Subscriber<? super T> actual) {
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
        public void fail(Throwable e) {
            if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
                cancellation = CANCELLED;
                actual.onError(e);
            } else {
                Exceptions.onErrorDropped(e);
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
            if (SubscriptionHelper.validate(n)) {
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
