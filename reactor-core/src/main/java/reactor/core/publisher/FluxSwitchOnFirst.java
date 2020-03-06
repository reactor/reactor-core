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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
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


    static final int STATE_CANCELLED = -2;
    static final int STATE_SUBSCRIBED = -1;
    static final int STATE_INIT            = 0;
    static final int STATE_SUBSCRIBED_ONCE = 1;


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

    static abstract class AbstractSwitchOnFirstMain<T, R> extends Flux<T>
            implements InnerOperator<T, R> {

        final ControlSubscriber<? super R>                                     outer;
        final BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer;

        Subscription s;
        Throwable    throwable;
        T            first;
        boolean      done;

        volatile CoreSubscriber<? super T> inner;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<AbstractSwitchOnFirstMain, CoreSubscriber> INNER =
                AtomicReferenceFieldUpdater.newUpdater(AbstractSwitchOnFirstMain.class, CoreSubscriber.class, "inner");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<AbstractSwitchOnFirstMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(AbstractSwitchOnFirstMain.class, "wip");

        volatile int state;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<AbstractSwitchOnFirstMain> STATE =
                AtomicIntegerFieldUpdater.newUpdater(AbstractSwitchOnFirstMain.class, "state");

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

            return InnerOperator.super.scanUnsafe(key);
        }

        @Override
        public CoreSubscriber<? super R> actual() {
            return this.outer;
        }

        @Override
        public void cancel() {
            if (INNER.getAndSet(this, Operators.EMPTY_SUBSCRIBER) != Operators.EMPTY_SUBSCRIBER) {
                this.s.cancel();

                if (WIP.getAndIncrement(this) == 0) {
                    final T f = this.first;
                    if (f != null) {
                        this.first = null;
                        Operators.onDiscard(f, currentContext());
                    }
                }
            }
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
            final CoreSubscriber<? super T> i = this.inner;
            if (this.done || i == Operators.EMPTY_SUBSCRIBER) {
                Operators.onErrorDropped(t, currentContext());
                return;
            }

            this.throwable = t;
            this.done = true;

            final T f = this.first;
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

            if (f == null) {
                drain();
            }
        }

        @Override
        public void onComplete() {
            final CoreSubscriber<? super T> i = this.inner;
            if (this.done || i == Operators.EMPTY_SUBSCRIBER) {
                return;
            }

            this.done = true;

            final T f = this.first;
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

            if (f == null) {
                drain();
            }
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                if (this.first != null) {
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

        abstract boolean drain();

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
            if (this.state == STATE_INIT && STATE.compareAndSet(this, STATE_INIT, STATE_SUBSCRIBED_ONCE)) {
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
                this.inner = actual;
                actual.onSubscribe(this);
            }
            else {
                Operators.error(actual, new IllegalStateException("FluxSwitchOnFirst allows only one Subscriber"));
            }
        }

        @Override
        boolean drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return false;
            }

            CoreSubscriber<? super T> a = this.inner;
            T f = this.first;
            int m = 1;

            for (;;) {
                if (f != null) {
                    this.first = null;

                    if (a == Operators.EMPTY_SUBSCRIBER) {
                        Operators.onDiscard(f, currentContext());
                        return false;
                    }

                    a.onNext(f);
                    f = null;
                }

                a = this.inner;

                if (a == Operators.EMPTY_SUBSCRIBER) {
                    return false;
                }

                if (this.done) {
                    final Throwable t = this.throwable;
                    if (t != null) {
                        a.onError(t);
                    } else {
                        a.onComplete();
                    }
                    INNER.lazySet(this, Operators.EMPTY_SUBSCRIBER);
                    return true;
                }

                m = WIP.addAndGet(this, -m);

                if (m == 0) {
                    return true;
                }
            }
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
            if (this.state == STATE_INIT && STATE.compareAndSet(this, STATE_INIT, STATE_SUBSCRIBED_ONCE)) {
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
                this.inner = Operators.toConditionalSubscriber(actual);
                actual.onSubscribe(this);
            }
            else {
                Operators.error(actual, new IllegalStateException("FluxSwitchOnFirst allows only one Subscriber"));
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
        boolean drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return false;
            }

            T f = this.first;
            int m = 1;
            boolean sent = false;
            Fuseable.ConditionalSubscriber<? super T> a =
                    (Fuseable.ConditionalSubscriber<? super T>) this.inner;

            for (;;) {
                if (f != null) {
                    this.first = null;

                    if (a == Operators.EMPTY_SUBSCRIBER) {
                        Operators.onDiscard(f, currentContext());
                        return false;
                    }

                    sent = a.tryOnNext(f);
                    f = null;
                }

                a = (Fuseable.ConditionalSubscriber<? super T>) this.inner;

                if (a == Operators.EMPTY_SUBSCRIBER) {
                    return false;
                }

                if (this.done) {
                    Throwable t = this.throwable;
                    if (t != null) {
                        a.onError(t);
                    } else {
                        a.onComplete();
                    }
                    INNER.lazySet(this, Operators.EMPTY_SUBSCRIBER);
                    return sent;
                }

                m = WIP.addAndGet(this, -m);

                if (m == 0) {
                    return sent;
                }
            }
        }
    }

    static final class SwitchOnFirstControlSubscriber<T> implements InnerOperator<T, T>, ControlSubscriber<T> {

        final AbstractSwitchOnFirstMain<?, T> parent;
        final CoreSubscriber<? super T> delegate;
        final boolean cancelSourceOnComplete;

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<SwitchOnFirstControlSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(SwitchOnFirstControlSubscriber.class, "requested");

        Subscription s;

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
            final long state = this.requested;
            if (this.s == null && state != STATE_CANCELLED) {
                this.s = s;

                this.tryRequest();
            }
            else {
                s.cancel();
            }
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
            final AbstractSwitchOnFirstMain<?, T> parent = this.parent;
            if (!parent.done) {
                parent.cancel();
            }

            this.delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            final AbstractSwitchOnFirstMain<?, T> parent = this.parent;
            if (!parent.done && cancelSourceOnComplete) {
                parent.cancel();
            }

            this.delegate.onComplete();
        }

        @Override
        public void request(long n) {
            long r = this.requested; // volatile read beforehand

            if (r > STATE_SUBSCRIBED) { // works only in case onSubscribe has not happened
                long u;
                for (;;) { // normal CAS loop with overflow protection
                    if (r == Long.MAX_VALUE) { // if r == Long.MAX_VALUE then we dont care and we can loose this request just in case of racing
                        return;
                    }
                    u = Operators.addCap(r, n);
                    if (REQUESTED.compareAndSet(this, r, u)) { // Means increment happened before onSubscribe
                        return;
                    }
                    else { // Means increment happened after onSubscribe
                        r = this.requested; // update new state to see what exactly happened (onSubscribe | cancel | requestN)

                        if (r < 0) { // check state (expect -1 | -2 to exit, otherwise repeat)
                            break;
                        }
                    }
                }
            }

            if (r == STATE_CANCELLED) { // if canceled, just exit
                return;
            }

            this.s.request(n); // if onSubscribe -> subscription exists (and we sure of that becuase volatile read after volatile write) so we can execute requestN on the subscription
        }

        void tryRequest() {
            final Subscription s = this.s;
            final long r = REQUESTED.getAndSet(this, STATE_SUBSCRIBED);

            if (r > 0) { // if there is something,
                s.request(r); // then we do a request on the given subscription
            }
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

            return null;
        }
    }

    static final class SwitchOnFirstConditionalControlSubscriber<T> implements InnerOperator<T, T>, ControlSubscriber<T>,
                                                                             Fuseable.ConditionalSubscriber<T> {

        final AbstractSwitchOnFirstMain<?, T> parent;
        final Fuseable.ConditionalSubscriber<? super T> delegate;
        final boolean terminateUpstreamOnComplete;

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<SwitchOnFirstConditionalControlSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(SwitchOnFirstConditionalControlSubscriber.class, "requested");

        Subscription s;

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
            final long state = this.requested;
            if (this.s == null && state != STATE_CANCELLED) {
                this.s = s;

                this.tryRequest();
            }
            else {
                s.cancel();
            }
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
            final AbstractSwitchOnFirstMain<?, T> parent = this.parent;
            if (!parent.done) {
                parent.cancel();
            }

            this.delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            final AbstractSwitchOnFirstMain<?, T> parent = this.parent;
            if (!parent.done && terminateUpstreamOnComplete) {
                parent.cancel();
            }

            this.delegate.onComplete();
        }

        @Override
        public void request(long n) {
            long r = this.requested; // volatile read beforehand

            if (r > STATE_SUBSCRIBED) { // works only in case onSubscribe has not happened
                long u;
                for (;;) { // normal CAS loop with overflow protection
                    if (r == Long.MAX_VALUE) { // if r == Long.MAX_VALUE then we dont care and we can loose this request just in case of racing
                        return;
                    }
                    u = Operators.addCap(r, n);
                    if (REQUESTED.compareAndSet(this, r, u)) { // Means increment happened before onSubscribe
                        return;
                    }
                    else { // Means increment happened after onSubscribe
                        r = this.requested; // update new state to see what exactly happened (onSubscribe | cancel | requestN)

                        if (r < 0) { // check state (expect -1 | -2 to exit, otherwise repeat)
                            break;
                        }
                    }
                }
            }

            if (r == STATE_CANCELLED) { // if canceled, just exit
                return;
            }

            this.s.request(n); // if onSubscribe -> subscription exists (and we sure of that becuase volatile read after volatile write) so we can execute requestN on the subscription
        }

        void tryRequest() {
            final Subscription s = this.s;
            final long r = REQUESTED.getAndSet(this, -1); // read and write

            if (r > 0) { // if there is something,
                s.request(r); // then we do a request on the given subscription
            }
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

            return null;
        }
    }

    interface ControlSubscriber<T> extends CoreSubscriber<T> {

        void sendSubscription();
    }
}
