/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;

/**
 * Concatenates a several Mono sources with a final Mono source by
 * ignoring values from the first set of sources and emitting the value
 * the last Mono source generates.
 *
 * @param <T> the final value type
 */
final class MonoIgnoreThen<T> extends Mono<T> implements Fuseable, Scannable {

    final Publisher<?>[] ignore;
    
    final Mono<T> last;
    
    MonoIgnoreThen(Publisher<?>[] ignore, Mono<T> last) {
        this.ignore = Objects.requireNonNull(ignore, "ignore");
        this.last = Objects.requireNonNull(last, "last");
    }
    
    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        ThenIgnoreMain<T> manager = new ThenIgnoreMain<>(actual, ignore, last);
        actual.onSubscribe(manager);
    }
    
    /**
     * Shifts the current last Mono into the ignore array and sets up a new last Mono instance.
     * @param <U> the new last value type
     * @param newLast the new last Mono instance
     * @return the new operator set up
     */
    <U> MonoIgnoreThen<U> shift(Mono<U> newLast) {
        Objects.requireNonNull(newLast, "newLast");
        Publisher<?>[] a = ignore;
        int n = a.length;
        Publisher<?>[] b = new Publisher[n + 1];
        System.arraycopy(a, 0, b, 0, n);
        b[n] = last;
        
        return new MonoIgnoreThen<>(b, newLast);
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
        return null;
    }
    
    static final class ThenIgnoreMain<T> implements CoreSubscriber<T>, Subscription {
        
        final Publisher<?>[] ignoreMonos;

        final Mono<T> lastMono;

        final CoreSubscriber<? super T> actual;

        int index;

        volatile Subscription activeSubscription;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<ThenIgnoreMain, Subscription> ACTIVE_SUBSCRIPTION =
                AtomicReferenceFieldUpdater.newUpdater(ThenIgnoreMain.class, Subscription.class, "activeSubscription");
        
        ThenIgnoreMain(CoreSubscriber<? super T> subscriber,
		        Publisher<?>[] ignoreMonos, Mono<T> lastMono) {
            this.actual = subscriber;
            this.ignoreMonos = ignoreMonos;
            this.lastMono = lastMono;

            ACTIVE_SUBSCRIPTION.lazySet(this, Operators.EmptySubscription.INSTANCE);
        }

//	    @Override
//	    public Stream<? extends Scannable> inners() {
//		    return Stream.of(ignore, accept);
//	    }

	    @SuppressWarnings({"unchecked", "rawtypes"})
        void subscribeNext() {
            final Publisher<?>[] a = ignoreMonos;

            for (;;) {
                final int i = this.index;

                if (i == a.length) {
                    Mono<T> m = lastMono;
                    if (m instanceof Callable) {
                        T v;
                        try {
                            v = ((Callable<T>)m).call();
                        }
                        catch (Throwable ex) {
                            onError(Operators.onOperatorError(ex, actual.currentContext()));
                            return;
                        }

                        if (v != null) {
                            onNext(v);
                        }
                        onComplete();
                        return;
                    }

                    m.subscribe(this);
                } else {
                    final Publisher<?> m = a[i];

                    if (m instanceof Callable) {
                        try {
                            ((Callable<?>) m).call();
                        }
                        catch (Throwable ex) {
                            onError(Operators.onOperatorError(ex, actual.currentContext()));
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
        public void cancel() {
            Operators.terminate(ACTIVE_SUBSCRIPTION, this);
        }

        @Override
        public void request(long n) {
            final Subscription current = activeSubscription;
            if (current == Operators.EmptySubscription.INSTANCE && ACTIVE_SUBSCRIPTION.getAndSet(this, null) == Operators.EmptySubscription.INSTANCE) {
                subscribeNext();
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.setOnce(ACTIVE_SUBSCRIPTION, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            if (this.index != ignoreMonos.length) {
                // ignored
                Operators.onDiscard(t, actual.currentContext());
                return;
            }

            Subscription current = this.activeSubscription;
            if (current == Operators.CancelledSubscription.INSTANCE) {
                Operators.onDiscard(t, actual.currentContext());
                return;
            }

            actual.onNext(t);
        }

        @Override
        public void onComplete() {
            if (this.index != ignoreMonos.length) {
                ACTIVE_SUBSCRIPTION.lazySet(this, null);
                this.index++;
                subscribeNext();
                return;
            }

            Subscription current = this.activeSubscription;
            if (current == Operators.CancelledSubscription.INSTANCE || ACTIVE_SUBSCRIPTION.getAndSet(
                    this,
                    Operators.CancelledSubscription.INSTANCE) == Operators.CancelledSubscription.INSTANCE) {
                return;
            }

            actual.onComplete();
        }

        @Override
        public void onError(Throwable t) {
            Subscription current = this.activeSubscription;
            if (current == Operators.CancelledSubscription.INSTANCE || ACTIVE_SUBSCRIPTION.getAndSet(
                    this,
                    Operators.CancelledSubscription.INSTANCE) == Operators.CancelledSubscription.INSTANCE) {
                Operators.onErrorDropped(t, actual.currentContext());
                return;
            }

            actual.onError(t);
        }
    }
//
//    static final class ThenIgnoreInner implements InnerConsumer<Object> {
//        final ThenIgnoreMain<?> parent;
//
//        volatile Subscription s;
//        static final AtomicReferenceFieldUpdater<ThenIgnoreInner, Subscription> S =
//                AtomicReferenceFieldUpdater.newUpdater(ThenIgnoreInner.class, Subscription.class, "s");
//
//        ThenIgnoreInner(ThenIgnoreMain<?> parent) {
//            this.parent = parent;
//        }
//
//	    @Override
//        @Nullable
//	    public Object scanUnsafe(Attr key) {
//            if (key == Attr.PARENT) return s;
//            if (key == Attr.ACTUAL) return parent;
//            if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
//            if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
//
//		    return null;
//	    }
//
//        @Override
//        public void onSubscribe(Subscription s) {
//            if (Operators.replace(S, this, s)) {
//                s.request(Long.MAX_VALUE);
//            }
//        }
//
//	    @Override
//	    public Context currentContext() {
//		    return parent.currentContext();
//	    }
//
//        @Override
//        public void onNext(Object t) {
//            // ignored
//            Operators.onDiscard(t, parent.currentContext()); //FIXME cache Context
//        }
//
//        @Override
//        public void onError(Throwable t) {
//            this.parent.onError(t);
//        }
//
//        @Override
//        public void onComplete() {
//            this.parent.ignoreDone();
//        }
//
//        void cancel() {
//            Operators.terminate(S, this);
//        }
//
//        void clear() {
//            S.lazySet(this, null);
//        }
//    }
//
//    static final class ThenAcceptInner<T> implements InnerConsumer<T> {
//        final ThenIgnoreMain<T> parent;
//
//        volatile Subscription s;
//        @SuppressWarnings("rawtypes")
//        static final AtomicReferenceFieldUpdater<ThenAcceptInner, Subscription> S =
//                AtomicReferenceFieldUpdater.newUpdater(ThenAcceptInner.class, Subscription.class, "s");
//
//        boolean done;
//
//        ThenAcceptInner(ThenIgnoreMain<T> parent) {
//            this.parent = parent;
//        }
//
//        @Override
//        @Nullable
//        public Object scanUnsafe(Attr key) {
//            if (key == Attr.PARENT) return s;
//            if (key == Attr.ACTUAL) return parent;
//            if (key == Attr.TERMINATED) return done;
//            if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
//            if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
//
//            return null;
//        }
//
//        @Override
//        public Context currentContext() {
//            return parent.currentContext();
//        }
//
//        @Override
//        public void onSubscribe(Subscription s) {
//            if (Operators.setOnce(S, this, s)) {
//                s.request(Long.MAX_VALUE);
//            }
//        }
//
//        @Override
//        public void onNext(T t) {
//            if (done) {
//                Operators.onNextDropped(t, parent.currentContext());
//                return;
//            }
//            done = true;
//            this.parent.complete(t);
//        }
//
//        @Override
//        public void onError(Throwable t) {
//            if (done) {
//                Operators.onErrorDropped(t, parent.currentContext());
//                return;
//            }
//            done = true;
//            this.parent.onError(t);
//        }
//
//        @Override
//        public void onComplete() {
//            if (done) {
//                return;
//            }
//            this.parent.onComplete();
//        }
//
//        void cancel() {
//            Operators.terminate(S, this);
//        }
//    }
}
