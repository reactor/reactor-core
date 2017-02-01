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
import reactor.core.Disposable;

/**
 * Wraps a the downstream Subscriber into a single emission object
 * and calls the given callback to produce a signal (a)synchronously.
 * @param <T> the value type
 */
final class MonoCreate<T> extends Mono<T> {

    final Consumer<MonoSink<T>> callback;

    public MonoCreate(Consumer<MonoSink<T>> callback) {
        this.callback = callback;
    }

    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        DefaultMonoSink<T> emitter = new DefaultMonoSink<>(s);
        
        s.onSubscribe(emitter);
        
        try {
            callback.accept(emitter);
        } catch (Throwable ex) {
            emitter.error(Operators.onOperatorError(ex));
        }
    }

    static final class DefaultMonoSink<T> implements MonoSink<T>, Subscription {
        final Subscriber<? super T> actual;
        
        volatile Cancellation disposable;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<DefaultMonoSink, Cancellation>
                CANCELLATION =
                AtomicReferenceFieldUpdater.newUpdater(DefaultMonoSink.class, Cancellation.class, "disposable");
        
        volatile int state;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<DefaultMonoSink> STATE =
                AtomicIntegerFieldUpdater.newUpdater(DefaultMonoSink.class, "state");

        T value;
        
        static final int NO_REQUEST_HAS_VALUE = 1;
        static final int HAS_REQUEST_NO_VALUE = 2;
        static final int HAS_REQUEST_HAS_VALUE = 3;
        
        DefaultMonoSink(Subscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void success() {
            if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
	            disposable = Flux.CANCELLED;
                actual.onComplete();
            }
        }

        @Override
        public void success(T value) {
            if (value == null) {
                success();
                return;
            }
            for (;;) {
                int s = state;
                if (s == HAS_REQUEST_HAS_VALUE || s == NO_REQUEST_HAS_VALUE) {
                    return;
                }
                if (s == HAS_REQUEST_NO_VALUE) {
                    if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
	                    disposable = Flux.CANCELLED;
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
	            disposable = Flux.CANCELLED;
                actual.onError(e);
            } else {
                Operators.onErrorDropped(e);
            }
        }

        @Override
        public void setCancellation(Cancellation c) {
            if (!CANCELLATION.compareAndSet(this, null, c)) {
                if (disposable != Flux.CANCELLED && c != null) {
                    c.dispose();
                }
            }
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                for (;;) {
                    int s = state;
                    if (s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE) {
                        return;
                    }
                    if (s == NO_REQUEST_HAS_VALUE) {
                        if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
	                        disposable = Flux.CANCELLED;
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
	        Cancellation c = disposable;
            if (c != Flux.CANCELLED) {
                c = CANCELLATION.getAndSet(this, Flux.CANCELLED);
                if (c != null && c != Flux.CANCELLED) {
                    c.dispose();
                }
            }
        }
        
        
    }
}
