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

import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.core.util.BackpressureUtils;

/**
 * Subscribes to the upstream Mono on the specified Scheduler and makes sure
 * any request from downstream is issued on the same worker where the subscription
 * happened.
 * 
 * @param <T> the value type
 */
final class MonoSubscribeOn<T> extends MonoSource<T, T> {
    
    final Scheduler scheduler;

    public MonoSubscribeOn(Publisher<? extends T> source, Scheduler scheduler) {
        super(source);
        this.scheduler = scheduler;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Scheduler.Worker worker = scheduler.createWorker();
        
        MonoSubscribeOnSubscriber<T> parent = new MonoSubscribeOnSubscriber<>(s, worker);
        s.onSubscribe(parent);
        
        worker.schedule(() -> source.subscribe(parent));
    }
    
    static final class MonoSubscribeOnSubscriber<T> implements Subscriber<T>, Subscription {
        final Subscriber<? super T> actual;
        
        final Scheduler.Worker worker;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<MonoSubscribeOnSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(MonoSubscribeOnSubscriber.class, Subscription.class, "s");

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<MonoSubscribeOnSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(MonoSubscribeOnSubscriber.class, "requested");
        
        public MonoSubscribeOnSubscriber(Subscriber<? super T> actual, Worker worker) {
            this.actual = actual;
            this.worker = worker;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (!BackpressureUtils.setOnce(S, this, s)) {
                s.cancel();
            } else {
                long r = REQUESTED.getAndSet(this, 0L);
                if (r != 0L) {
                    worker.schedule(() -> requestMore(r));
                }
            }
        }
        
        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }
        
        @Override
        public void onError(Throwable t) {
            try {
                actual.onError(t);
            } finally {
                worker.shutdown();
            }
        }
        
        @Override
        public void onComplete() {
            try {
                actual.onComplete();
            } finally {
                worker.shutdown();
            }
        }
        
        @Override
        public void request(long n) {
            if (BackpressureUtils.validate(n)) {
                worker.schedule(() -> requestMore(n));
            }
        }
        
        @Override
        public void cancel() {
            if (BackpressureUtils.terminate(S, this)) {
                worker.shutdown();
            }
        }
        
        void requestMore(long n) {
            Subscription a = s;
            if (a != null) {
                a.request(n);
            } else {
                BackpressureUtils.getAndAddCap(REQUESTED, this, n);
                a = s;
                if (a != null) {
                    long r = REQUESTED.getAndSet(this, 0L);
                    if (r != 0L) {
                        a.request(r);
                    }
                }
            }
        }
    }
}
