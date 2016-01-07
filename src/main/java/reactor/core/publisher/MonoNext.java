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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.support.BackpressureUtils;

/**
 * Emits a single item at most from the source.
 *
 * @param <T> the value type
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
public final class MonoNext<T> extends reactor.Mono.MonoBarrier<T, T> {

    public MonoNext(Publisher<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new MonoNextSubscriber<>(s));
    }

    static final class MonoNextSubscriber<T>
            implements Subscriber<T>, Subscription, Upstream, ActiveUpstream, Bounded, Downstream {

        final Subscriber<? super T> actual;

        Subscription s;

        boolean done;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<MonoNextSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(MonoNextSubscriber.class, "wip");

        public MonoNextSubscriber(Subscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (BackpressureUtils.validate(this.s, s)) {
                this.s = s;
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                Exceptions.onNextDropped(t);
                return;
            }

            s.cancel();
            actual.onNext(t);
            onComplete();
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                Exceptions.onErrorDropped(t);
                return;
            }
            done = true;
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            if (WIP.compareAndSet(this, 0, 1)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void cancel() {
            s.cancel();
        }

        @Override
        public boolean isStarted() {
            return s != null && !done;
        }

        @Override
        public boolean isTerminated() {
            return done;
        }

        @Override
        public long getCapacity() {
            return 1L;
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public Object downstream() {
            return actual;
        }
    }
}