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
package reactor.core.subscriber;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;

/**
 * A subscription implementation that arbitrates request amounts between subsequent Subscriptions, including the
 * duration until the first Subscription is set.
 * <p>
 * The class is thread safe but switching Subscriptions should happen only when the source associated with the current
 * Subscription has finished emitting values. Otherwise, two sources may emit for one request.
 * <p>
 * You should call {@link #produced(long)} or {@link #producedOne()} after each element has been delivered to properly
 * account the outstanding request amount in case a Subscription switch happens.
 *
 * @param <I> the input value type
 * @param <O> the output value type
 */
public abstract class SubscriberMultiSubscription<I, O>
        implements Subscription, Subscriber<I>, ReactiveState.Downstream, ReactiveState.ActiveDownstream,
                   ReactiveState.DownstreamDemand, ReactiveState.Upstream {

    protected final Subscriber<? super O> subscriber;

    /**
     * The current subscription which may null if no Subscriptions have been set.
     */
    Subscription actual;

    /**
     * The current outstanding request amount.
     */
    long requested;

    volatile Subscription missedSubscription;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<SubscriberMultiSubscription, Subscription> MISSED_SUBSCRIPTION =
            AtomicReferenceFieldUpdater.newUpdater(SubscriberMultiSubscription.class,
                    Subscription.class,
                    "missedSubscription");

    volatile long missedRequested;
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<SubscriberMultiSubscription> MISSED_REQUESTED =
            AtomicLongFieldUpdater.newUpdater(SubscriberMultiSubscription.class, "missedRequested");

    volatile long missedProduced;
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<SubscriberMultiSubscription> MISSED_PRODUCED =
            AtomicLongFieldUpdater.newUpdater(SubscriberMultiSubscription.class, "missedProduced");

    volatile int wip;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<SubscriberMultiSubscription> WIP =
            AtomicIntegerFieldUpdater.newUpdater(SubscriberMultiSubscription.class, "wip");

    volatile boolean cancelled;

    public SubscriberMultiSubscription(Subscriber<? super O> subscriber) {
        this.subscriber = subscriber;
    }

    @Override
    public void onSubscribe(Subscription s) {
        set(s);
    }

    @Override
    public void onError(Throwable t) {
        subscriber.onError(t);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
    }

    public final void set(Subscription s) {
        if (cancelled) {
            return;
        }

        Objects.requireNonNull(s);

        Subscription a = MISSED_SUBSCRIPTION.getAndSet(this, s);
        if (a != null) {
            a.cancel();
        }
        drain();
    }

    @Override
    public final void request(long n) {
        if (BackpressureUtils.validate(n)) {

            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                long r = requested;

                if (r != Long.MAX_VALUE) {
                    requested = BackpressureUtils.addCap(r, n);
                }
                Subscription a = actual;
                if (a != null) {
                    a.request(n);
                }

                if (WIP.decrementAndGet(this) == 0) {
                    return;
                }

                drainLoop();

                return;
            }

            BackpressureUtils.addAndGet(MISSED_REQUESTED, this, n);

            drain();
        }
    }

    public final void producedOne() {
        if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
            long r = requested;

            if (r != Long.MAX_VALUE) {
                r--;
                if (r < 0L) {
                    BackpressureUtils.reportMoreProduced();
                    r = 0;
                }
                requested = r;
            }

            if (WIP.decrementAndGet(this) == 0) {
                return;
            }

            drainLoop();

            return;
        }

        BackpressureUtils.addAndGet(MISSED_PRODUCED, this, 1L);

        drain();
    }

    public final void produced(long n) {
        if (BackpressureUtils.validate(n)) {
            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                long r = requested;

                if (r != Long.MAX_VALUE) {
                    long u = r - n;
                    if (u < 0L) {
                        BackpressureUtils.reportMoreProduced();
                        u = 0;
                    }
                    requested = u;
                }

                if (WIP.decrementAndGet(this) == 0) {
                    return;
                }

                drainLoop();

                return;
            }

            BackpressureUtils.addAndGet(MISSED_PRODUCED, this, n);

            drain();
        }
    }

    @Override
    public void cancel() {
        if (!cancelled) {
            cancelled = true;

            drain();
        }
    }

    @Override
    public final boolean isCancelled() {
        return cancelled;
    }

    final void drain() {
        if (WIP.getAndIncrement(this) != 0) {
            return;
        }
        drainLoop();
    }

    final void drainLoop() {
        int missed = 1;

        for (; ; ) {

            Subscription ms = missedSubscription;

            if (ms != null) {
                ms = MISSED_SUBSCRIPTION.getAndSet(this, null);
            }

            long mr = missedRequested;
            if (mr != 0L) {
                mr = MISSED_REQUESTED.getAndSet(this, 0L);
            }

            long mp = missedProduced;
            if (mp != 0L) {
                mp = MISSED_PRODUCED.getAndSet(this, 0L);
            }

            Subscription a = actual;

            if (cancelled) {
                if (a != null) {
                    a.cancel();
                    actual = null;
                }
                if (ms != null) {
                    ms.cancel();
                }
            }
            else {
                long r = requested;
                if (r != Long.MAX_VALUE) {
                    long u = BackpressureUtils.addCap(r, mr);

                    if (u != Long.MAX_VALUE) {
                        long v = u - mp;
                        if (v < 0L) {
                            BackpressureUtils.reportMoreProduced();
                            v = 0;
                        }
                        r = v;
                    }
                    else {
                        r = u;
                    }
                    requested = r;
                }

                if (ms != null) {
                    if (a != null) {
                        a.cancel();
                    }
                    actual = ms;
                    if (r != 0L) {
                        ms.request(r);
                    }
                }
                else if (mr != 0L && a != null) {
                    a.request(mr);
                }
            }

            missed = WIP.addAndGet(this, -missed);
            if (missed == 0) {
                return;
            }
        }
    }

    @Override
    public final Subscriber<? super O> downstream() {
        return subscriber;
    }

    @Override
    public final Subscription upstream() {
        return actual != null ? actual : missedSubscription;
    }

    @Override
    public final long requestedFromDownstream() {
        return requested + missedRequested;
    }
}
