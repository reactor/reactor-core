/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

import static reactor.core.Fuseable.ConditionalSubscriber;

/**
 * @author Oleh Dokuka
 */
public class FluxLimitRate<T> extends InternalFluxOperator<T, T> {

    final int highTide;

    final int lowTide;

    FluxLimitRate(Flux<? extends T> source, int highTide, int lowTide) {
        super(source);
        if (highTide <= 0) {
            throw new IllegalArgumentException("highTide > 0 required but it was " + highTide);
        }
        this.highTide = highTide;
        this.lowTide = lowTide;
    }

    @Override
    public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
        if (actual instanceof ConditionalSubscriber) {
            @SuppressWarnings("unchecked") // javac, give reason to suppress because inference anomalies
            final ConditionalSubscriber<? super T> cs =
                (ConditionalSubscriber<? super T>) actual;
          return new LimitRateConditionalSubscriber<>(cs, highTide, lowTide);
        }
        return new LimitRateSubscriber<>(actual, highTide, lowTide);
    }

    private final static class LimitRateSubscriber<T> implements InnerOperator<T, T> {

        private final int highTide;
        private final int limit;

        volatile long requested; // need sync
        static final AtomicLongFieldUpdater<LimitRateSubscriber> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(LimitRateSubscriber.class, "requested");

        volatile int wip; // need sync
        static final AtomicIntegerFieldUpdater<LimitRateSubscriber> WIP =
            AtomicIntegerFieldUpdater.newUpdater(LimitRateSubscriber.class, "wip");

        volatile int produced = 0;
        static final AtomicIntegerFieldUpdater<LimitRateSubscriber> PRODUCED =
            AtomicIntegerFieldUpdater.newUpdater(LimitRateSubscriber.class, "produced");

        private int pending;
        // need sync since should be checked/zerroed in onNext
        // and increased in request
        private int counter; // no need to sync since increased zerroed only in
        // the request method

        final CoreSubscriber<? super T> actual;

        Subscription s;

        private LimitRateSubscriber(CoreSubscriber<? super T> actual,
            int highTide,
            int lowTide) {
            this.actual = actual;
            this.highTide = highTide;
            this.limit = Operators.unboundedOrLimit(highTide, lowTide);
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                Operators.addCap(REQUESTED, this, n);

                tryRequest();
            }
        }

        void tryRequest() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }


            final Subscription s = this.s;
            final int highTide = this.highTide;

            int missed = 1;
            int pending = this.pending;
            int produced = this.produced;
            long r = this.requested;
            long deliver = 0;

            for (;;) {
                if (r != Long.MAX_VALUE || highTide != Integer.MAX_VALUE) {
                    if (produced != 0) {
                        deliver = Math.min(produced, r);
                    }
                    else if (pending != highTide) {
                        deliver = Math.min(highTide - pending, r);
                    }

                    int w = wip;
                    if (missed == w) {
                        if (deliver > 0) {
                            if (produced != 0) {
                                this.pending = highTide - produced + (int) deliver;
                                PRODUCED.addAndGet(this, -produced);
                            }
                            else {
                                this.pending = pending + (int) deliver;
                            }
                            Operators.addCap(REQUESTED, this, -deliver);

                            missed = WIP.addAndGet(this, -missed);
                            if (missed == 0) {
                                s.request(deliver);
                                break;
                            }
                        } else {
                            if (produced != 0) {
                                this.pending = highTide - produced;
                                PRODUCED.addAndGet(this, -produced);
                            }

                            missed = WIP.addAndGet(this, -missed);
                            if (missed == 0) {
                                break;
                            }
                        }

                        produced = this.produced;
                        r = this.requested;
                    }
                    else {
                        missed = w;
                        r = this.requested;
                    }
                }
                else {
                    this.pending = Integer.MAX_VALUE;
                    s.request(Long.MAX_VALUE);
                    return;
                }
            }
        }

        @Override
        public CoreSubscriber<? super T> actual() {
            return actual;
        }

        public void cancel() {
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);

            if (highTide == Integer.MAX_VALUE) {
                return;
            }

            final int l = this.limit;
            final int p = counter + 1;

            if (p == l) {
                PRODUCED.getAndAdd(this, l);
                tryRequest();
                this.counter = 0;
            } else {
                this.counter = p;
            }
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }

    }

    private final static class LimitRateConditionalSubscriber<T> implements ConditionalSubscriber<T>, InnerOperator<T, T> {

        private final int highTide;
        private final int limit;

        volatile long requested; // need sync
        static final AtomicLongFieldUpdater<LimitRateConditionalSubscriber> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(LimitRateConditionalSubscriber.class, "requested");

        volatile int wip; // need sync
        static final AtomicIntegerFieldUpdater<LimitRateConditionalSubscriber> WIP =
            AtomicIntegerFieldUpdater.newUpdater(LimitRateConditionalSubscriber.class, "wip");

        volatile int produced = 0;
        static final AtomicIntegerFieldUpdater<LimitRateConditionalSubscriber> PRODUCED =
            AtomicIntegerFieldUpdater.newUpdater(LimitRateConditionalSubscriber.class, "produced");

        private int pending;
        // need sync since should be checked/zerroed in onNext
        // and increased in request
        private int counter; // no need to sync since increased zerroed only in
        // the request method

        final ConditionalSubscriber<? super T> actual;

        Subscription s;

        private LimitRateConditionalSubscriber(ConditionalSubscriber<? super T> actual,
            int highTide,
            int lowTide) {
            this.actual = actual;
            this.highTide = highTide;
            this.limit = Operators.unboundedOrLimit(highTide, lowTide);
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                Operators.addCap(REQUESTED, this, n);

                tryRequest();
            }
        }

        void tryRequest() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }


            final Subscription s = this.s;
            final int highTide = this.highTide;

            int missed = 1;
            int pending = this.pending;
            int produced = this.produced;
            long r = this.requested;
            long deliver = 0;

            for (;;) {
                if (r != Long.MAX_VALUE || highTide != Integer.MAX_VALUE) {
                    if (produced != 0) {
                        deliver = Math.min(produced, r);
                    }
                    else if (pending != highTide) {
                        deliver = Math.min(highTide - pending, r);
                    }

                    int w = wip;
                    if (missed == w) {
                        if (deliver > 0) {
                            if (produced != 0) {
                                this.pending = highTide - produced + (int) deliver;
                                PRODUCED.addAndGet(this, -produced);
                            }
                            else {
                                this.pending = pending + (int) deliver;
                            }
                            Operators.addCap(REQUESTED, this, -deliver);

                            missed = WIP.addAndGet(this, -missed);
                            if (missed == 0) {
                                s.request(deliver);
                                break;
                            }
                        } else {
                            if (produced != 0) {
                                this.pending = highTide - produced;
                                PRODUCED.addAndGet(this, -produced);
                            }

                            missed = WIP.addAndGet(this, -missed);
                            if (missed == 0) {
                                break;
                            }
                        }

                        produced = this.produced;
                        r = this.requested;
                    }
                    else {
                        missed = w;
                        r = this.requested;
                    }
                }
                else {
                    this.pending = Integer.MAX_VALUE;
                    s.request(Long.MAX_VALUE);
                    return;
                }
            }
        }

        @Override
        public CoreSubscriber<? super T> actual() {
            return actual;
        }

        public void cancel() {
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);

            if (highTide == Integer.MAX_VALUE) {
                return;
            }

            final int l = this.limit;
            final int p = counter + 1;

            if (p == l) {
                PRODUCED.getAndAdd(this, l);
                tryRequest();
                this.counter = 0;
            } else {
                this.counter = p;
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            if (!actual.tryOnNext(t)) {
                return false;
            }

            if (highTide == Integer.MAX_VALUE) {
                return true;
            }

            final int l = this.limit;
            final int p = counter + 1;

            if (p == l) {
                PRODUCED.getAndAdd(this, l);
                tryRequest();
                this.counter = 0;
            } else {
                this.counter = p;
            }

            return true;
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }
}