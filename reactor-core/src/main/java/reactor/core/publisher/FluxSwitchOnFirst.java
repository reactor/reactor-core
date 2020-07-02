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
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * @author Oleh Dokuka
 * @param <T>
 * @param <R>
 */
final class FluxSwitchOnFirst<T, R> extends InternalFluxOperator<T, R> {


    final BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer;
    final boolean cancelSourceOnComplete;

    FluxSwitchOnFirst(
            Flux<? extends T> source,
            BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer,
            boolean cancelSourceOnComplete) {
        super(source);
        this.transformer = Objects.requireNonNull(transformer, "transformer");
        this.cancelSourceOnComplete = cancelSourceOnComplete;
    }

    @Override
    public int getPrefetch() {
        return 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
        if (actual instanceof Fuseable.ConditionalSubscriber) {
            return new SwitchOnFirstConditionalMain<>((Fuseable.ConditionalSubscriber<? super R>) actual, transformer, cancelSourceOnComplete);
        }
        return new SwitchOnFirstMain<>(actual, transformer, cancelSourceOnComplete);
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
        return super.scanUnsafe(key);
    }

    static abstract class AbstractSwitchOnFirstMain<T, R> extends Flux<T>
            implements InnerOperator<T, R> {

        final ControlSubscriber<? super R>                                     outer;
        final BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer;

        Subscription s;
        Throwable    throwable;
        T            first;
        boolean      requestedOnce;
        boolean      done;

        volatile CoreSubscriber<? super T> inner;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<AbstractSwitchOnFirstMain, CoreSubscriber> INNER =
                AtomicReferenceFieldUpdater.newUpdater(AbstractSwitchOnFirstMain.class, CoreSubscriber.class, "inner");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<AbstractSwitchOnFirstMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(AbstractSwitchOnFirstMain.class, "wip");

        @SuppressWarnings("unchecked")
        AbstractSwitchOnFirstMain(
                CoreSubscriber<? super R> outer,
                BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer,
                boolean cancelSourceOnComplete) {
            this.outer = outer instanceof Fuseable.ConditionalSubscriber
                ? new SwitchOnFirstConditionalControlSubscriber<>(this, (Fuseable.ConditionalSubscriber<R>) outer, cancelSourceOnComplete)
                : new SwitchOnFirstControlSubscriber<>(this, outer, cancelSourceOnComplete);
            this.transformer = transformer;
        }

        @Override
        @Nullable
        public Object scanUnsafe(Attr key) {
            final boolean isCancelled = this.inner == Operators.EMPTY_SUBSCRIBER;

            if (key == Attr.CANCELLED) return isCancelled && !this.done;
            if (key == Attr.TERMINATED) return this.done || isCancelled;
            if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

            return InnerOperator.super.scanUnsafe(key);
        }

        @Override
        public CoreSubscriber<? super R> actual() {
            return this.outer;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;
                this.outer.sendSubscription();
                if (this.inner != Operators.EMPTY_SUBSCRIBER) {
                    s.request(1);
                }
            }
        }

        @Override
        public void onNext(T t) {
            final CoreSubscriber<? super T> i = this.inner;
            if (this.done || i == Operators.EMPTY_SUBSCRIBER) {
                Operators.onNextDropped(t, currentContext());
                return;
            }

            if (i == null) {
                final Publisher<? extends R> result;
                final CoreSubscriber<? super R> o = this.outer;

                try {
                    result = Objects.requireNonNull(
                        this.transformer.apply(Signal.next(t, o.currentContext()), this),
                        "The transformer returned a null value"
                    );
                }
                catch (Throwable e) {
                    this.done = true;
                    Operators.error(o, Operators.onOperatorError(this.s, e, t, o.currentContext()));
                    return;
                }

                this.first = t;
                result.subscribe(o);
                return;
            }

            i.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            // read of the first should occur before the read of inner since otherwise
            // first may be nulled while the previous read has shown that inner is still
            // null hence double invocation of transformer occurs
            final T f = this.first;
            final CoreSubscriber<? super T> i = this.inner;
            if (this.done || i == Operators.EMPTY_SUBSCRIBER) {
                Operators.onErrorDropped(t, currentContext());
                return;
            }

            this.throwable = t;
            this.done = true;

            if (f == null && i == null) {
                final Publisher<? extends R> result;
                final CoreSubscriber<? super R> o = this.outer;
                try {
                    result = Objects.requireNonNull(
                        this.transformer.apply(Signal.error(t, o.currentContext()), this),
                        "The transformer returned a null value"
                    );
                }
                catch (Throwable e) {
                    this.done = true;
                    Operators.error(o, Operators.onOperatorError(this.s, e, t, o.currentContext()));
                    return;
                }

                result.subscribe(o);
                return;
            }

            drain();
        }

        @Override
        public void onComplete() {
            // read of the first should occur before the read of inner since otherwise
            // first may be nulled while the previous read has shown that inner is still
            // null hence double invocation of transformer occurs
            final T f = this.first;
            final CoreSubscriber<? super T> i = this.inner;
            if (this.done || i == Operators.EMPTY_SUBSCRIBER) {
                return;
            }

            this.done = true;

            if (f == null && i == null) {
                final Publisher<? extends R> result;
                final CoreSubscriber<? super R> o = outer;

                try {
                    result = Objects.requireNonNull(
                            this.transformer.apply(Signal.complete(o.currentContext()), this),
                        "The transformer returned a null value"
                    );
                }
                catch (Throwable e) {
                    this.done = true;
                    Operators.error(o, Operators.onOperatorError(this.s, e, null, o.currentContext()));
                    return;
                }

                result.subscribe(o);
                return;
            }

            drain();
        }

        @Override
        public void cancel() {
            if (INNER.getAndSet(this, Operators.EMPTY_SUBSCRIBER) == Operators.EMPTY_SUBSCRIBER) {
                return;
            }

            this.s.cancel();

            if (WIP.getAndIncrement(this) == 0) {
                final T f = this.first;
                if (f != null) {
                    this.first = null;
                    Operators.onDiscard(f, currentContext());
                }
            }
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                if (this.first != null) {
                    this.requestedOnce = true;
                    if (drain() && n != Long.MAX_VALUE) {
                        if (--n > 0) {
                            this.s.request(n);
                            return;
                        }

                        return;
                    }
                }

                this.s.request(n);
            }
        }

        @SuppressWarnings("unchecked")
        boolean drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return false;
            }

            T f = this.first;
            int m = 1;
            boolean sent = false;
            CoreSubscriber<? super T> a;

            for (;;) {
                a = this.inner;

                // check for the case where upstream terminates before downstream has subscribed at all
                if (a != null) {
                    if (f != null && this.requestedOnce) {
                        this.first = null;

                        if (a == Operators.EMPTY_SUBSCRIBER) {
                            Operators.onDiscard(f, currentContext());
                            return false;
                        }

                        sent = tryOnNext(a, f);
                        f = null;
                        // check if not cancelled if it has just been sent (next + cancel case)
                        a = this.inner;
                    }

                    if (a == Operators.EMPTY_SUBSCRIBER) {
                        return false;
                    }

                    if (this.done && f == null) {
                        Throwable t = this.throwable;
                        if (t != null) {
                            a.onError(t);
                        } else {
                            a.onComplete();
                        }
                        INNER.lazySet(this, Operators.EMPTY_SUBSCRIBER);
                        return sent;
                    }
                }

                m = WIP.addAndGet(this, -m);
                if (m == 0) {
                    return sent;
                }
            }
        }

        abstract boolean tryOnNext(CoreSubscriber<? super T> actual, T value);

    }

    static final class SwitchOnFirstMain<T, R> extends AbstractSwitchOnFirstMain<T, R> {

        SwitchOnFirstMain(
                CoreSubscriber<? super R> outer,
                BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer,
                boolean cancelSourceOnComplete) {
            super(outer, transformer, cancelSourceOnComplete);
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> actual) {
            if (this.inner == null && INNER.compareAndSet(this, null, actual)) {
                if (this.first == null && this.done) {
                    final Throwable t = this.throwable;
                    if (t != null) {
                        Operators.error(actual, t);
                    }
                    else {
                        Operators.complete(actual);
                    }
                    return;
                }
                actual.onSubscribe(this);
            }
            else if (this.inner != Operators.EMPTY_SUBSCRIBER) {
                Operators.error(actual, new IllegalStateException("FluxSwitchOnFirst allows only one Subscriber"));
            } else {
                Operators.error(actual, new CancellationException("FluxSwitchOnFirst has already been cancelled"));
            }
        }

        @Override
        boolean tryOnNext(CoreSubscriber<? super T> actual, T t) {
            actual.onNext(t);
            return true;
        }
    }

    static final class SwitchOnFirstConditionalMain<T, R> extends AbstractSwitchOnFirstMain<T, R>
            implements Fuseable.ConditionalSubscriber<T> {

        SwitchOnFirstConditionalMain(
                Fuseable.ConditionalSubscriber<? super R> outer,
                BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer,
                boolean cancelSourceOnComplete) {
            super(outer, transformer, cancelSourceOnComplete);
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> actual) {
            if (this.inner == null && INNER.compareAndSet(this, null, Operators.toConditionalSubscriber(actual))) {
                if (this.first == null && this.done) {
                    final Throwable t = this.throwable;
                    if (t != null) {
                        Operators.error(actual, t);
                    }
                    else {
                        Operators.complete(actual);
                    }
                    return;
                }
                actual.onSubscribe(this);
            }
            else if (this.inner != Operators.EMPTY_SUBSCRIBER) {
                Operators.error(actual, new IllegalStateException("FluxSwitchOnFirst allows only one Subscriber"));
            } else {
                Operators.error(actual, new CancellationException("FluxSwitchOnFirst has already been cancelled"));
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            @SuppressWarnings("unchecked")
            final Fuseable.ConditionalSubscriber<? super T> i =
                    (Fuseable.ConditionalSubscriber<? super T>) this.inner;
            if (this.done || i == Operators.EMPTY_SUBSCRIBER) {
                Operators.onNextDropped(t, currentContext());
                return false;
            }

            if (i == null) {
                final Publisher<? extends R> result;
                final CoreSubscriber<? super R> o = this.outer;

                try {
                    result = Objects.requireNonNull(
                        this.transformer.apply(Signal.next(t, o.currentContext()), this),
                        "The transformer returned a null value"
                    );
                }
                catch (Throwable e) {
                    this.done = true;
                    Operators.error(o, Operators.onOperatorError(this.s, e, t, o.currentContext()));
                    return false;
                }

                this.first = t;
                result.subscribe(o);
                return true;
            }

            return i.tryOnNext(t);
        }

        @Override
        @SuppressWarnings("unchecked")
        boolean tryOnNext(CoreSubscriber<? super T> actual, T t) {
            return ((Fuseable.ConditionalSubscriber<? super T>) actual).tryOnNext(t);
        }
    }

    static final class SwitchOnFirstControlSubscriber<T> extends Operators.DeferredSubscription implements InnerOperator<T,
            T>, ControlSubscriber<T> {

        final AbstractSwitchOnFirstMain<?, T> parent;
        final CoreSubscriber<? super T> delegate;
        final boolean cancelSourceOnComplete;

        SwitchOnFirstControlSubscriber(
                AbstractSwitchOnFirstMain<?, T> parent,
                CoreSubscriber<? super T> delegate,
                boolean cancelSourceOnComplete) {
            this.parent = parent;
            this.delegate = delegate;
            this.cancelSourceOnComplete = cancelSourceOnComplete;
        }

        @Override
        public void sendSubscription() {
            delegate.onSubscribe(this);
        }

        @Override
        public void onSubscribe(Subscription s) {
            set(s);
        }

        @Override
        public CoreSubscriber<? super T> actual() {
            return this.delegate;
        }

        @Override
        public void onNext(T t) {
            this.delegate.onNext(t);
        }

        @Override
        public void onError(Throwable throwable) {
            if (this.requested == STATE_CANCELLED) {
                Operators.onErrorDropped(throwable, currentContext());
                return;
            }

            final AbstractSwitchOnFirstMain<?, T> parent = this.parent;
            if (!parent.done) {
                parent.cancel();
            }

            this.delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (this.requested == STATE_CANCELLED) {
                return;
            }

            final AbstractSwitchOnFirstMain<?, T> parent = this.parent;
            if (!parent.done && cancelSourceOnComplete) {
                parent.cancel();
            }

            this.delegate.onComplete();
        }

        @Override
        public void cancel() {
            final long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
            if (state == STATE_CANCELLED) {
                return;
            }

            if (state == STATE_SUBSCRIBED) {
                this.s.cancel();
            }

            this.parent.cancel();
        }

        @Override
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PARENT) return parent;
            if (key == Attr.ACTUAL) return delegate;
            if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

            return null;
        }
    }

    static final class SwitchOnFirstConditionalControlSubscriber<T> extends Operators.DeferredSubscription implements InnerOperator<T, T>, ControlSubscriber<T>,
                                                                             Fuseable.ConditionalSubscriber<T> {

        final AbstractSwitchOnFirstMain<?, T> parent;
        final Fuseable.ConditionalSubscriber<? super T> delegate;
        final boolean terminateUpstreamOnComplete;

        SwitchOnFirstConditionalControlSubscriber(
                AbstractSwitchOnFirstMain<?, T> parent,
                Fuseable.ConditionalSubscriber<? super T> delegate,
                boolean terminateUpstreamOnComplete) {
            this.parent = parent;
            this.delegate = delegate;
            this.terminateUpstreamOnComplete = terminateUpstreamOnComplete;
        }

        @Override
        public void sendSubscription() {
            delegate.onSubscribe(this);
        }

        @Override
        public void onSubscribe(Subscription s) {
            set(s);
        }

        @Override
        public CoreSubscriber<? super T> actual() {
            return this.delegate;
        }

        @Override
        public void onNext(T t) {
            this.delegate.onNext(t);
        }

        @Override
        public boolean tryOnNext(T t) {
            return this.delegate.tryOnNext(t);
        }

        @Override
        public void onError(Throwable throwable) {
            if (this.requested == STATE_CANCELLED) {
                Operators.onErrorDropped(throwable, currentContext());
                return;
            }

            final AbstractSwitchOnFirstMain<?, T> parent = this.parent;
            if (!parent.done) {
                parent.cancel();
            }

            this.delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (this.requested == STATE_CANCELLED) {
                return;
            }

            final AbstractSwitchOnFirstMain<?, T> parent = this.parent;
            if (!parent.done && terminateUpstreamOnComplete) {
                parent.cancel();
            }

            this.delegate.onComplete();
        }

        @Override
        public void cancel() {
            final long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
            if (state == STATE_CANCELLED) {
                return;
            }

            if (state == STATE_SUBSCRIBED) { // mean subscription happened so we can just cancel upstream only
                this.s.cancel();
            }

            this.parent.cancel();
        }

        @Override
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PARENT) return parent;
            if (key == Attr.ACTUAL) return delegate;
            if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

            return null;
        }
    }

    interface ControlSubscriber<T> extends CoreSubscriber<T> {

        void sendSubscription();
    }
}
