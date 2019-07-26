/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
final class FluxSwitchOnFirst<T, R> extends InternalFluxOperator<T, R> {

    static final int STATE_INIT            = 0;
    static final int STATE_SUBSCRIBED_ONCE = 1;

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
    public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
        if (actual instanceof Fuseable.ConditionalSubscriber) {
            source.subscribe(new SwitchOnFirstConditionalInner<>((Fuseable.ConditionalSubscriber<? super R>) actual, transformer));
            return null;
        }
        return new SwitchOnFirstInner<>(actual, transformer);
    }

    static abstract class AbstractSwitchOnFirstInner<T, R> extends Flux<T>
            implements InnerOperator<T, R> {

        final CoreSubscriber<? super R>                                        outer;
        final BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer;

        Subscription s;
        Throwable    throwable;
        T            first;
        boolean      done;

        volatile boolean cancelled;

        volatile CoreSubscriber<? super T> inner;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<AbstractSwitchOnFirstInner, CoreSubscriber> INNER =
                AtomicReferenceFieldUpdater.newUpdater(AbstractSwitchOnFirstInner.class, CoreSubscriber.class, "inner");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<AbstractSwitchOnFirstInner> WIP =
                AtomicIntegerFieldUpdater.newUpdater(AbstractSwitchOnFirstInner.class, "wip");

        volatile int state;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<AbstractSwitchOnFirstInner> STATE =
                AtomicIntegerFieldUpdater.newUpdater(AbstractSwitchOnFirstInner.class, "state");

        @SuppressWarnings("unchecked")
        AbstractSwitchOnFirstInner(
                CoreSubscriber<? super R> outer,
                BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer) {
            this.outer = outer instanceof Fuseable.ConditionalSubscriber
                ? new SwitchOnFirstConditionalInnerSubscriber<>(this, (Fuseable.ConditionalSubscriber<R>) outer)
                : new SwitchOnFirstInnerSubscriber<>(this, outer);
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

            if (f == null && i == null && !cancelled) {
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
                drain();
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

            if (f == null && i == null && !cancelled) {
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
                drain();
            }
        }

        abstract void drain();

    }

    static final class SwitchOnFirstInner<T, R> extends AbstractSwitchOnFirstInner<T, R> {

        SwitchOnFirstInner(
                CoreSubscriber<? super R> outer,
                BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer) {
            super(outer, transformer);
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> actual) {
            if (state == STATE_INIT && STATE.compareAndSet(this, STATE_INIT, STATE_SUBSCRIBED_ONCE)) {
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
        public void request(long n) {
            if (Operators.validate(n)) {
                if (first != null) {
                    drain();

                    if (n != Long.MAX_VALUE) {
                        if (--n > 0) {
                            s.request(n);
                            return;
                        }

                        return;
                    }
                }

                s.request(n);
            }
        }

        @Override
        void drain() {
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


    static final class SwitchOnFirstConditionalInner<T, R> extends AbstractSwitchOnFirstInner<T, R>
            implements Fuseable.ConditionalSubscriber<T> {

        SwitchOnFirstConditionalInner(
                Fuseable.ConditionalSubscriber<? super R> outer,
                BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer) {
            super(outer, transformer);
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> actual) {
            if (state == STATE_INIT && STATE.compareAndSet(this, STATE_INIT, STATE_SUBSCRIBED_ONCE)) {
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
        public boolean tryOnNext(T t) {
            if (done) {
                Operators.onNextDropped(t, currentContext());
                return false;
            }

            @SuppressWarnings("unchecked")
            Fuseable.ConditionalSubscriber<? super T> i =
                    (Fuseable.ConditionalSubscriber<? super T>) inner;

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
        public void request(long n) {
            if (Operators.validate(n)) {
                if (first != null) {
                    if (drainRegular() && n != Long.MAX_VALUE) {
                        if (--n > 0) {
                            s.request(n);
                            return;
                        }

                        return;
                    }
                }

                s.request(n);
            }
        }

        @Override
        void drain() {
            drainRegular();
        }

        boolean drainRegular() {
            if (WIP.getAndIncrement(this) != 0) {
                return false;
            }

            T f = first;
            int m = 1;
            boolean sent = false;
            @SuppressWarnings("unchecked")
            Fuseable.ConditionalSubscriber<? super T> a =
                    (Fuseable.ConditionalSubscriber<? super T>) inner;

            for (;;) {
                if (f != null) {
                    first = null;

                    if (cancelled) {
                        Operators.onDiscard(f, a.currentContext());
                        return false;
                    }

                    sent = a.tryOnNext(f);
                    f = null;
                }

                if (cancelled) {
                    return false;
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

    static final class SwitchOnFirstInnerSubscriber<T> implements InnerConsumer<T> {

        final AbstractSwitchOnFirstInner<?, T> parent;
        final CoreSubscriber<? super T> inner;

        SwitchOnFirstInnerSubscriber(
                AbstractSwitchOnFirstInner<?, T> parent,
                CoreSubscriber<? super T> inner) {
            this.parent = parent;
            this.inner = inner;
        }

        @Override
        public Context currentContext() {
            return inner.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {
            inner.onSubscribe(s);
        }

        @Override
        public void onNext(T t) {
            inner.onNext(t);
        }

        @Override
        public void onError(Throwable throwable) {
            if (!parent.done) {
                parent.cancel();
            }

            inner.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (!parent.done) {
                parent.cancel();
            }

            inner.onComplete();
        }

        @Override
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PARENT) return parent;
            if (key == Attr.ACTUAL) return inner;

            return null;
        }
    }

    static final class SwitchOnFirstConditionalInnerSubscriber<T> implements InnerConsumer<T>,
                                                                             Fuseable.ConditionalSubscriber<T> {

        final AbstractSwitchOnFirstInner<?, ? super T>  parent;
        final Fuseable.ConditionalSubscriber<? super T> inner;

        SwitchOnFirstConditionalInnerSubscriber(
                AbstractSwitchOnFirstInner<?, ? super T> parent,
                Fuseable.ConditionalSubscriber<? super T> inner) {
            this.parent = parent;
            this.inner = inner;
        }

        @Override
        public Context currentContext() {
            return inner.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {
            inner.onSubscribe(s);
        }

        @Override
        public void onNext(T t) {
            inner.onNext(t);
        }

        @Override
        public boolean tryOnNext(T t) {
            return inner.tryOnNext(t);
        }

        @Override
        public void onError(Throwable throwable) {
            if (!parent.done) {
                parent.cancel();
            }

            inner.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (!parent.done) {
                parent.cancel();
            }

            inner.onComplete();
        }

        @Override
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PARENT) return parent;
            if (key == Attr.ACTUAL) return inner;

            return null;
        }
    }
}
