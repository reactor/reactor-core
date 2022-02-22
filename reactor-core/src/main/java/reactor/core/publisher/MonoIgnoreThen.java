/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Concatenates a several Mono sources with a final Mono source by
 * ignoring values from the first set of sources and emitting the value
 * the last Mono source generates.
 *
 * @param <T> the final value type
 */
final class MonoIgnoreThen<T> extends Mono<T> implements Scannable {

    final Publisher<?>[] ignore;
    
    final Mono<T> last;
    
    MonoIgnoreThen(Publisher<?>[] ignore, Mono<T> last) {
        this.ignore = Objects.requireNonNull(ignore, "ignore");
        this.last = Objects.requireNonNull(last, "last");
    }
    
    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        ThenIgnoreMain<T> manager = new ThenIgnoreMain<>(actual, this.ignore, this.last);
        actual.onSubscribe(manager);
        manager.subscribeNext();
    }
    
    /**
     * Shifts the current last Mono into the ignore array and sets up a new last Mono instance.
     * @param <U> the new last value type
     * @param newLast the new last Mono instance
     * @return the new operator set up
     */
    <U> MonoIgnoreThen<U> shift(Mono<U> newLast) {
        Objects.requireNonNull(newLast, "newLast");
        Publisher<?>[] a = this.ignore;
        int n = a.length;
        Publisher<?>[] b = new Publisher[n + 1];
        System.arraycopy(a, 0, b, 0, n);
        b[n] = this.last;
        
        return new MonoIgnoreThen<>(b, newLast);
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
        return null;
    }
    
    static final class ThenIgnoreMain<T> implements InnerOperator<T, T> {
        
        final Publisher<?>[] ignoreMonos;
        final Mono<T> lastMono;
        final CoreSubscriber<? super T> actual;

        T value;

        int          index;
        Subscription activeSubscription;
        boolean      done;

        volatile int state;
        @SuppressWarnings("rawtypes")
        private static final AtomicIntegerFieldUpdater<ThenIgnoreMain> STATE =
                AtomicIntegerFieldUpdater.newUpdater(ThenIgnoreMain.class, "state");
        // The following are to be used as bit masks, not as values per se.
        static final int HAS_REQUEST      = 0b00000010;
        static final int HAS_SUBSCRIPTION = 0b00000100;
        static final int HAS_VALUE        = 0b00001000;
        static final int HAS_COMPLETION   = 0b00010000;
        // The following are to be used as value (ie using == or !=).
        static final int CANCELLED        = 0b10000000;

        ThenIgnoreMain(CoreSubscriber<? super T> subscriber,
		        Publisher<?>[] ignoreMonos, Mono<T> lastMono) {
            this.actual = subscriber;
            this.ignoreMonos = ignoreMonos;
            this.lastMono = lastMono;
        }

        @Override
        @Nullable
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PARENT) return this.activeSubscription;
            if (key == Attr.CANCELLED) return isCancelled(this.state);
            if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

            return InnerOperator.super.scanUnsafe(key);
        }

        @Override
        public CoreSubscriber<? super T> actual() {
            return this.actual;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.activeSubscription, s)) {
                this.activeSubscription = s;

                final int previousState = markHasSubscription();
                if (isCancelled(previousState)) {
                    s.cancel();
                    return;
                }

                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void cancel() {
            int previousState = markCancelled();

            if (hasSubscription(previousState)) {
                this.activeSubscription.cancel();
            }
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                for (; ; ) {
                    final int state = this.state;
                    if (isCancelled(state)) {
                        return;
                    }
                    if (hasRequest(state)) {
                        return;
                    }
                    if (STATE.compareAndSet(this, state, state | HAS_REQUEST)) {
                        if (hasValue(state)) {
                            final CoreSubscriber<? super T> actual = this.actual;
                            final T v = this.value;

                            actual.onNext(v);
                            actual.onComplete();
                        }
                        return;
                    }
                }
            }
        }

        @Override
        public void onNext(T t) {
            if (this.done) {
                Operators.onDiscard(t, currentContext());
                return;
            }

            if (this.index != this.ignoreMonos.length) {
                // ignored
                Operators.onDiscard(t, currentContext());
                return;
            }

            this.done = true;

            complete(t);
        }

        @Override
        public void onComplete() {
            if (this.done) {
                return;
            }

            if (this.index != this.ignoreMonos.length) {
                final int previousState = markUnsubscribed();
                if (isCancelled(previousState)) {
                    return;
                }
                this.activeSubscription = null;
                this.index++;
                subscribeNext();
                return;
            }

            this.done = true;

            this.actual.onComplete();
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        void subscribeNext() {
            final Publisher<?>[] a = this.ignoreMonos;

            for (;;) {
                final int i = this.index;

                if (i == a.length) {
                    Mono<T> m = this.lastMono;
                    if (m instanceof Callable) {
                        if (isCancelled(this.state)) {
                            //NB: in the non-callable case, this is handled by activeSubscription.cancel()
                            return;
                        }
                        T v;
                        try {
                            v = ((Callable<T>)m).call();
                        }
                        catch (Throwable ex) {
                            onError(Operators.onOperatorError(ex, currentContext()));
                            return;
                        }

                        if (v != null) {
                            onNext(v);
                        }
                        onComplete();
                    } else {
                        m.subscribe(this);
                    }
                    return;
                } else {
                    final Publisher<?> m = a[i];

                    if (m instanceof Callable) {
                        if (isCancelled(this.state)) {
                            //NB: in the non-callable case, this is handled by activeSubscription.cancel()
                            return;
                        }
                        try {
                            Operators.onDiscard(((Callable<?>) m).call(), currentContext());
                        }
                        catch (Throwable ex) {
                            onError(Operators.onOperatorError(ex, currentContext()));
                            return;
                        }

                        this.index = i + 1;
                        continue;
                    }

                    m.subscribe((CoreSubscriber) this);
                    return;
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            if (this.done) {
                Operators.onErrorDropped(t, actual().currentContext());
                return;
            }

            this.done = true;

            this.actual.onError(t);
        }

        final void complete(T value) {
            for (; ; ) {
                int s = this.state;
                if (isCancelled(s)) {
                    Operators.onDiscard(value, this.actual.currentContext());
                    return;
                }

                if (hasRequest(s) && STATE.compareAndSet(this, s, s | (HAS_VALUE | HAS_COMPLETION))) {
                    final CoreSubscriber<? super T> actual = this.actual;

                    actual.onNext(value);
                    actual.onComplete();
                    return;
                }

                this.value = value;
                if (STATE.compareAndSet(this, s, s | (HAS_VALUE | HAS_COMPLETION))) {
                    return;
                }
            }
        }

        final int markHasSubscription() {
            for (;;) {
                final int state = this.state;

                if (state == CANCELLED) {
                    return state;
                }

                if ((state & HAS_SUBSCRIPTION) == HAS_SUBSCRIPTION) {
                    return state;
                }

                if (STATE.compareAndSet(this, state, state | HAS_SUBSCRIPTION)) {
                    return state;
                }
            }
        }

        final int markUnsubscribed() {
            for (;;) {
                final int state = this.state;

                if (isCancelled(state)) {
                    return state;
                }

                if (!hasSubscription(state)) {
                    return state;
                }

                if (STATE.compareAndSet(this, state, state &~ HAS_SUBSCRIPTION)) {
                    return state;
                }
            }
        }

        final int markCancelled() {
            for (;;) {
                final int state = this.state;

                if (state == CANCELLED) {
                    return state;
                }

                if (STATE.compareAndSet(this, state, CANCELLED)) {
                    return state;
                }
            }
        }

        static boolean isCancelled(int s) {
            return s == CANCELLED;
        }

        static boolean hasSubscription(int s) {
            return (s & HAS_SUBSCRIPTION) == HAS_SUBSCRIPTION;
        }

        static boolean hasRequest(int s) {
            return (s & HAS_REQUEST) == HAS_REQUEST;
        }

        static boolean hasValue(int s) {
            return (s & HAS_VALUE) == HAS_VALUE;
        }

    }
}
