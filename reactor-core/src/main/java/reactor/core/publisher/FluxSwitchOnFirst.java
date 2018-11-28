/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Oleh Dokuka
 * @param <T>
 * @param <R>
 */
final class FluxSwitchOnFirst<T, R> extends FluxOperator<T, R> {

    final BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer;

    FluxSwitchOnFirst(
            Flux<? extends T> source,
            BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer) {
        super(source);
        this.transformer = Objects.requireNonNull(transformer, "transformer");
    }

    @Override
    public int getPrefetch() {
        return 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void subscribe(CoreSubscriber<? super R> actual) {
        if (actual instanceof Fuseable.ConditionalSubscriber) {
            source.subscribe(new SwitchOnFirstConditionalInner<>((Fuseable.ConditionalSubscriber<? super R>) actual, transformer));
            return;
        }
        source.subscribe(new SwitchOnFirstInner<>(actual, transformer));
    }

    static final class SwitchOnFirstInner<T, R> extends Flux<T>
            implements InnerOperator<T, R> {

        final CoreSubscriber<? super R>                                        outer;
        final BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer;

        Subscription s;
        Throwable    throwable;

        volatile T       first;
        volatile boolean done;
        volatile boolean cancelled;

        volatile CoreSubscriber<? super T> inner;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<SwitchOnFirstInner, CoreSubscriber> INNER =
                AtomicReferenceFieldUpdater.newUpdater(SwitchOnFirstInner.class, CoreSubscriber.class, "inner");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<SwitchOnFirstInner> WIP =
                AtomicIntegerFieldUpdater.newUpdater(SwitchOnFirstInner.class, "wip");

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<SwitchOnFirstInner> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(SwitchOnFirstInner.class, "once");

        SwitchOnFirstInner(
                CoreSubscriber<? super R> outer,
                BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer) {
            this.outer = outer;
            this.transformer = transformer;
        }

        @Override
        @Nullable
        public Object scanUnsafe(Attr key) {
            if (key == Attr.CANCELLED) return cancelled;
            if (key == Attr.TERMINATED) return done || cancelled;

            return InnerOperator.super.scanUnsafe(key);
        }

        @Override
        public CoreSubscriber<? super R> actual() {
            return outer;
        }

        @Override
        public Context currentContext() {
            CoreSubscriber<? super T> actual = inner;

            if (actual != null) {
                return actual.currentContext();
            }

            return outer.currentContext();
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                s.cancel();

                if (WIP.getAndIncrement(this) == 0) {
                    INNER.lazySet(this, null);

                    T f = first;
                    if (f != null) {
                        first = null;
                        Operators.onDiscard(f, currentContext());
                    }
                }
            }
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> actual) {
            if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
                if (first == null && done) {
                    if (throwable != null) {
                        Operators.error(actual, throwable);
                    }
                    else {
                        Operators.complete(actual);
                    }
                    return;
                }
                INNER.lazySet(this, actual);
                actual.onSubscribe(this);
            }
            else {
                Operators.error(actual, new IllegalStateException("FluxSwitchOnFirst allows only one Subscriber"));
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;
                s.request(1);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                Operators.onNextDropped(t, currentContext());
                return;
            }

            CoreSubscriber<? super T> i = inner;

            if (i == null) {
                Publisher<? extends R> result;
                CoreSubscriber<? super R> o = outer;

                try {
                    result = Objects.requireNonNull(
                        transformer.apply(Signal.next(t, o.currentContext()), this),
                        "The transformer returned a null value"
                    );
                }
                catch (Throwable e) {
                    done = true;
                    Operators.error(o, Operators.onOperatorError(s, e, t, o.currentContext()));
                    return;
                }

                first = t;
                result.subscribe(o);
                return;
            }

            i.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                Operators.onErrorDropped(t, currentContext());
                return;
            }

            throwable = t;
            done = true;
            CoreSubscriber<? super T> i = inner;
            T f = first;

            if (f == null && i == null) {
                Publisher<? extends R> result;
                CoreSubscriber<? super R> o = outer;

                try {
                    result = Objects.requireNonNull(
                        transformer.apply(Signal.error(t, o.currentContext()), this),
                        "The transformer returned a null value"
                    );
                }
                catch (Throwable e) {
                    done = true;
                    Operators.error(o, Operators.onOperatorError(s, e, t, o.currentContext()));
                    return;
                }

                result.subscribe(o);
                return;
            }

            if (f == null) {
                drainRegular();
            }
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }

            done = true;
            CoreSubscriber<? super T> i = inner;
            T f = first;

            if (f == null && i == null) {
                Publisher<? extends R> result;
                CoreSubscriber<? super R> o = outer;

                try {
                    result = Objects.requireNonNull(
                        transformer.apply(Signal.complete(o.currentContext()), this),
                        "The transformer returned a null value"
                    );
                }
                catch (Throwable e) {
                    done = true;
                    Operators.error(o, Operators.onOperatorError(s, e, null, o.currentContext()));
                    return;
                }

                result.subscribe(o);
                return;
            }

            if (f == null) {
                drainRegular();
            }
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                if (once == 1 && ONCE.compareAndSet(this, 1, 2)) {
                    if (first != null) {
                        drainRegular();
                    }

                    if (n != Long.MAX_VALUE && --n > 0) {
                        s.request(n);
                        return;
                    }
                }

                s.request(n);
            }
        }

        void drainRegular() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }

            T f = first;
            int m = 1;
            CoreSubscriber<? super T> a = inner;

            for (;;) {
                if (f != null) {
                    first = null;

                    if (cancelled) {
                        Operators.onDiscard(f, a.currentContext());
                        return;
                    }

                    a.onNext(f);
                    f = null;
                }

                if (cancelled) {
                    return;
                }

                if (done) {
                    Throwable t = throwable;
                    if (t != null) {
                        a.onError(t);
                    } else {
                        a.onComplete();
                    }
                    return;
                }

                m = WIP.addAndGet(this, -m);

                if (m == 0) {
                    return;
                }
            }
        }
    }


    static final class SwitchOnFirstConditionalInner<T, R> extends Flux<T>
            implements Fuseable.ConditionalSubscriber<T>, InnerOperator<T, R> {

        final Fuseable.ConditionalSubscriber<? super R>                        outer;
        final BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer;

        Subscription s;
        Throwable    throwable;

        volatile T       first;
        volatile boolean done;
        volatile boolean cancelled;

        volatile Fuseable.ConditionalSubscriber<? super T> inner;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<SwitchOnFirstConditionalInner, Fuseable.ConditionalSubscriber>INNER =
                AtomicReferenceFieldUpdater.newUpdater(SwitchOnFirstConditionalInner.class, Fuseable.ConditionalSubscriber.class, "inner");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<SwitchOnFirstConditionalInner> WIP =
                AtomicIntegerFieldUpdater.newUpdater(SwitchOnFirstConditionalInner.class, "wip");

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<SwitchOnFirstConditionalInner> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(SwitchOnFirstConditionalInner.class, "once");

        SwitchOnFirstConditionalInner(
                Fuseable.ConditionalSubscriber<? super R> outer,
                BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer) {
            this.outer = outer;
            this.transformer = transformer;
        }

        @Override
        @Nullable
        public Object scanUnsafe(Attr key) {
            if (key == Attr.CANCELLED) return cancelled;
            if (key == Attr.TERMINATED) return done || cancelled;

            return InnerOperator.super.scanUnsafe(key);
        }

        @Override
        public Context currentContext() {
            CoreSubscriber<? super T> actual = inner;

            if (actual != null) {
                return actual.currentContext();
            }

            return outer.currentContext();
        }

        @Override
        public CoreSubscriber<? super R> actual() {
            return outer;
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                s.cancel();

                if (WIP.getAndIncrement(this) == 0) {
                    INNER.lazySet(this, null);

                    T f = first;
                    if (f != null) {
                        first = null;
                        Operators.onDiscard(f, currentContext());
                    }
                }
            }
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> actual) {
            if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
                if (first == null && done) {
                    if (throwable != null) {
                        Operators.error(actual, throwable);
                    }
                    else {
                        Operators.complete(actual);
                    }
                    return;
                }
                INNER.lazySet(this, Operators.toConditionalSubscriber(actual));
                actual.onSubscribe(this);
            }
            else {
                Operators.error(actual, new IllegalStateException("FluxSwitchOnFirst allows only one Subscriber"));
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;
                s.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                Operators.onNextDropped(t, currentContext());
                return false;
            }

            Fuseable.ConditionalSubscriber<? super T> i = inner;

            if (i == null) {
                Publisher<? extends R> result;
                CoreSubscriber<? super R> o = outer;

                try {
                    result = Objects.requireNonNull(
                        transformer.apply(Signal.next(t, o.currentContext()), this),
                        "The transformer returned a null value"
                    );
                }
                catch (Throwable e) {
                    done = true;
                    Operators.error(o, Operators.onOperatorError(s, e, t, o.currentContext()));
                    return false;
                }

                first = t;
                result.subscribe(o);
                return true;
            }

            return i.tryOnNext(t);
        }

        @Override
        public void onNext(T t) {
            if (done) {
                Operators.onNextDropped(t, currentContext());
                return;
            }

            CoreSubscriber<? super T> i = inner;

            if (i == null) {
                Publisher<? extends R> result;
                CoreSubscriber<? super R> o = outer;

                try {
                    result = Objects.requireNonNull(
                        transformer.apply(Signal.next(t, o.currentContext()), this),
                        "The transformer returned a null value"
                    );
                }
                catch (Throwable e) {
                    done = true;
                    Operators.error(o, Operators.onOperatorError(s, e, t, o.currentContext()));
                    return;
                }

                first = t;
                result.subscribe(o);
                return;
            }

            i.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                Operators.onErrorDropped(t, currentContext());
                return;
            }

            throwable = t;
            done = true;
            CoreSubscriber<? super T> i = inner;
            T f = first;

            if (f == null && i == null) {
                Publisher<? extends R> result;
                CoreSubscriber<? super R> o = outer;

                try {
                    result = Objects.requireNonNull(
                        transformer.apply(Signal.error(t, o.currentContext()), this),
                        "The transformer returned a null value"
                    );
                }
                catch (Throwable e) {
                    done = true;
                    Operators.error(o, Operators.onOperatorError(s, e, t, o.currentContext()));
                    return;
                }

                result.subscribe(o);
                return;
            }

            if (f == null) {
                drainRegular();
            }
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }

            done = true;
            CoreSubscriber<? super T> i = inner;
            T f = first;

            if (f == null && i == null) {
                Publisher<? extends R> result;
                CoreSubscriber<? super R> o = outer;

                try {
                    result = Objects.requireNonNull(
                        transformer.apply(Signal.complete(o.currentContext()), this),
                        "The transformer returned a null value"
                    );
                }
                catch (Throwable e) {
                    done = true;
                    Operators.error(o, Operators.onOperatorError(s, e, null, o.currentContext()));
                    return;
                }

                result.subscribe(o);
                return;
            }

            if (f == null) {
                drainRegular();
            }
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                if (once == 1 && ONCE.compareAndSet(this, 1, 2)) {
                    boolean sent = false;

                    if (first != null) {
                        sent = drainRegular();
                    }

                    if (sent && n != Long.MAX_VALUE && --n > 0) {
                        s.request(n);
                        return;
                    }
                }

                s.request(n);
            }
        }

        boolean drainRegular() {
            if (WIP.getAndIncrement(this) != 0) {
                return false;
            }

            T f = first;
            int m = 1;
            boolean sent = false;
            Fuseable.ConditionalSubscriber<? super T> a = inner;

            for (; ; ) {
                if (f != null) {
                    first = null;

                    if (cancelled) {
                        Operators.onDiscard(f, a.currentContext());
                        return true;
                    }

                    sent = a.tryOnNext(f);
                    f = null;
                }

                if (cancelled) {
                    return sent;
                }

                if (done) {
                    Throwable t = throwable;
                    if (t != null) {
                        a.onError(t);
                    } else {
                        a.onComplete();
                    }
                    return sent;
                }

                m = WIP.addAndGet(this, -m);

                if (m == 0) {
                    return sent;
                }
            }
        }
    }
}
