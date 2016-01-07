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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscriber.SubscriberDeferredSubscription;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;

/**
 * Given a set of source Publishers the values of that Publisher is forwarded to the
 * subscriber which responds first with any signal.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class FluxAmb<T> extends reactor.Flux<T> implements ReactiveState.Factory, ReactiveState.LinkedUpstreams {

    final Publisher<? extends T>[] array;

    final Iterable<? extends Publisher<? extends T>> iterable;

    @SafeVarargs
    public FluxAmb(Publisher<? extends T>... array) {
        this.array = Objects.requireNonNull(array, "array");
        this.iterable = null;
    }

    public FluxAmb(Iterable<? extends Publisher<? extends T>> iterable) {
        this.array = null;
        this.iterable = Objects.requireNonNull(iterable);
    }

    @Override
    public Iterator<?> upstreams() {
        return iterable != null ? iterable.iterator() : Arrays.asList(array)
                                                              .iterator();
    }

    @Override
    public long upstreamsCount() {
        return array != null ? array.length : -1L;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Publisher<? extends T>[] a = array;
        int n;
        if (a == null) {
            n = 0;
            a = new Publisher[8];

            Iterator<? extends Publisher<? extends T>> it;

            try {
                it = iterable.iterator();
            }
            catch (Throwable e) {
                EmptySubscription.error(s, e);
                return;
            }

            if (it == null) {
                EmptySubscription.error(s, new NullPointerException("The iterator returned is null"));
                return;
            }

            for (; ; ) {

                boolean b;

                try {
                    b = it.hasNext();
                }
                catch (Throwable e) {
                    EmptySubscription.error(s, e);
                    return;
                }

                if (!b) {
                    break;
                }

                Publisher<? extends T> p;

                try {
                    p = it.next();
                }
                catch (Throwable e) {
                    EmptySubscription.error(s, e);
                    return;
                }

                if (p == null) {
                    EmptySubscription.error(s,
                            new NullPointerException("The Publisher returned by the iterator is " + "null"));
                    return;
                }

                if (n == a.length) {
                    Publisher<? extends T>[] c = new Publisher[n + (n >> 2)];
                    System.arraycopy(a, 0, c, 0, n);
                    a = c;
                }
                a[n++] = p;
            }

        }
        else {
            n = a.length;
        }

        if (n == 0) {
            EmptySubscription.complete(s);
            return;
        }
        if (n == 1) {
            Publisher<? extends T> p = a[0];

            if (p == null) {
                EmptySubscription.error(s, new NullPointerException("The single source Publisher is null"));
            }
            else {
                p.subscribe(s);
            }
            return;
        }

        FluxAmbCoordinator<T> coordinator = new FluxAmbCoordinator<>(n);

        coordinator.subscribe(a, n, s);
    }

    static final class FluxAmbCoordinator<T> implements Subscription, LinkedUpstreams, ActiveDownstream {

        final FluxAmbSubscriber<T>[] subscribers;

        volatile boolean cancelled;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<FluxAmbCoordinator> WIP =
                AtomicIntegerFieldUpdater.newUpdater(FluxAmbCoordinator.class, "wip");

        @SuppressWarnings("unchecked")
        public FluxAmbCoordinator(int n) {
            subscribers = new FluxAmbSubscriber[n];
            wip = Integer.MIN_VALUE;
        }

        void subscribe(Publisher<? extends T>[] sources, int n, Subscriber<? super T> actual) {
            FluxAmbSubscriber<T>[] a = subscribers;

            for (int i = 0; i < n; i++) {
                a[i] = new FluxAmbSubscriber<>(actual, this, i);
            }

            actual.onSubscribe(this);

            for (int i = 0; i < n; i++) {
                if (cancelled || wip != Integer.MIN_VALUE) {
                    return;
                }

                Publisher<? extends T> p = sources[i];

                if (p == null) {
                    if (WIP.compareAndSet(this, Integer.MIN_VALUE, -1)) {
                        actual.onError(new NullPointerException("The " + i + " th Publisher source is null"));
                    }
                    return;
                }

                p.subscribe(a[i]);
            }

        }

        @Override
        public void request(long n) {
            if (BackpressureUtils.validate(n)) {
                int w = wip;
                if (w >= 0) {
                    subscribers[w].request(n);
                }
                else {
                    for (FluxAmbSubscriber<T> s : subscribers) {
                        s.request(n);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            if (cancelled) {
                return;
            }
            cancelled = true;

            int w = wip;
            if (w >= 0) {
                subscribers[w].cancel();
            }
            else {
                for (FluxAmbSubscriber<T> s : subscribers) {
                    s.cancel();
                }
            }
        }

        boolean tryWin(int index) {
            if (wip == Integer.MIN_VALUE) {
                if (WIP.compareAndSet(this, Integer.MIN_VALUE, index)) {

                    FluxAmbSubscriber<T>[] a = subscribers;
                    int n = a.length;

                    for (int i = 0; i < n; i++) {
                        if (i != index) {
                            a[i].cancel();
                        }
                    }

                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public Iterator<?> upstreams() {
            return Arrays.asList(subscribers)
                         .iterator();
        }

        @Override
        public long upstreamsCount() {
            return subscribers.length;
        }
    }

    static final class FluxAmbSubscriber<T> extends SubscriberDeferredSubscription<T, T> implements Inner {

        final FluxAmbCoordinator<T> parent;

        final int index;

        boolean won;

        public FluxAmbSubscriber(Subscriber<? super T> actual, FluxAmbCoordinator<T> parent, int index) {
            super(actual);
            this.parent = parent;
            this.index = index;
        }

        @Override
        public void onNext(T t) {
            if (won) {
                subscriber.onNext(t);
            }
            else if (parent.tryWin(index)) {
                won = true;
                subscriber.onNext(t);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (won) {
                subscriber.onError(t);
            }
            else if (parent.tryWin(index)) {
                won = true;
                subscriber.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (won) {
                subscriber.onComplete();
            }
            else if (parent.tryWin(index)) {
                won = true;
                subscriber.onComplete();
            }
        }
    }
}