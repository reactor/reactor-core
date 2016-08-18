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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.Exceptions;
import reactor.core.Receiver;
import reactor.core.Trackable;

/**
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
abstract class BlockingSingleSubscriber<T> extends CountDownLatch
implements Subscriber<T>, Cancellation, Trackable, Receiver {

    T value;
    Throwable error;
    
    Subscription s;
    
    volatile boolean cancelled;

    public BlockingSingleSubscriber() {
        super(1);
    }

    @Override
    public final void onSubscribe(Subscription s) {
        this.s = s;
        if (!cancelled) {
            s.request(Long.MAX_VALUE);
            if (cancelled) {
                this.s = null;
                s.cancel();
            }
        }
    }
    
    @Override
    public final void onComplete() {
        countDown();
    }
    
    @Override
    public final void dispose() {
        cancelled = true;
        Subscription s = this.s;
        if (s != null) {
            this.s = null;
            s.cancel();
        }
    }
    
    /**
     * Block until the first value arrives and return it, otherwise
     * return null for an empty source and rethrow any exception.
     * @return the first value or null if the source is empty
     */
    public final T blockingGet() {
        if (getCount() != 0) {
            try {
                await();
            } catch (InterruptedException ex) {
                dispose();
                throw Exceptions.propagate(ex);
            }
        }
        
        Throwable e = error;
        if (e != null) {
            throw Exceptions.propagate(e);
        }
        return value;
    }
    
    /**
     * Block until the first value arrives and return it, otherwise
     * return null for an empty source and rethrow any exception.
     * @param timeout the timeout to wait
     * @param unit the time unit
     * @return the first value or null if the source is empty
     */
    public final T blockingGet(long timeout, TimeUnit unit) {
        if (getCount() != 0) {
            try {
                if (!await(timeout, unit)) {
                    dispose();
	                throw new IllegalStateException("Timeout on blocking read");
                }
            } catch (InterruptedException ex) {
                dispose();
                throw Exceptions.propagate(ex);
            }
        }
        
        Throwable e = error;
        if (e != null) {
            throw Exceptions.propagate(e);
        }
        return value;
    }

    @Override
    public Object upstream() {
        return s;
    }

    @Override
    public boolean isTerminated() {
        return getCount() == 0;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isStarted() {
        return s != null;
    }

    @Override
    public Throwable getError() {
        return error;
    }

    @Override
    public long getPending() {
        return getCount();
    }
}
