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

import java.util.*;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;

import org.reactivestreams.*;

import reactor.core.subscriber.SubscriptionHelper;
import reactor.core.util.*;

/**
 * @author Stephane Maldini
 */
final class FluxDematerialize<T> extends FluxSource<Signal<T>, T> {

	public FluxDematerialize(Publisher<Signal<T>> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		source.subscribe(new DematerializeAction<>(subscriber));
	}

	static final class DematerializeAction<T>
	extends AbstractQueue<T>
	implements Subscriber<Signal<T>>, Subscription, BooleanSupplier {

	    final Subscriber<? super T> actual;
	    
	    Subscription s;
	    
	    T value;
	    
	    boolean done;
	    
	    long produced;
	    
	    volatile long requested;
	    @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<DematerializeAction> REQUESTED =
	            AtomicLongFieldUpdater.newUpdater(DematerializeAction.class, "requested");
	    
	    volatile boolean cancelled;
	    
	    Throwable error;
	    
		public DematerializeAction(Subscriber<? super T> subscriber) {
			this.actual = subscriber;
		}

		@Override
		public void onSubscribe(Subscription s) {
		    if (SubscriptionHelper.validate(this.s, s)) {
		        this.s = s;
		        
		        actual.onSubscribe(this);
		        
		        s.request(1);
		    }
		}
		
		@Override
		public void onNext(Signal<T> t) {
		    if (done) {
		        Exceptions.onNextDropped(t);
		        return;
		    }
		    if (t.isOnComplete()) {
		        s.cancel();
		        onComplete();
		    } else
		    if (t.isOnError()) {
                s.cancel();
		        onError(t.getThrowable());
		    } else
		    if (t.isOnNext()) {
		        T v = value;
		        value = t.get();
		        
		        if (v != null) {
		            produced++;
		            actual.onNext(v);
		        }
		    }
		}
		
		@Override
		public void onError(Throwable t) {
		    if (done) {
		        Exceptions.onErrorDropped(t);
		        return;
		    }
		    done = true;
		    error = t;
            long p = produced;
            if (p != 0L) {
                REQUESTED.addAndGet(this, -p);
            }
            DrainUtils.postCompleteDelayError(actual, this, REQUESTED, this, this, error);
		}
		
		@Override
		public void onComplete() {
		    if (done) {
		        return;
		    }
		    done = true;
		    long p = produced;
		    if (p != 0L) {
		        REQUESTED.addAndGet(this, -p);
		    }
		    DrainUtils.postCompleteDelayError(actual, this, REQUESTED, this, this, error);
		}
		
		@Override
		public void request(long n) {
		    if (SubscriptionHelper.validate(n)) {
    		    if (!DrainUtils.postCompleteRequestDelayError(n, actual, this, REQUESTED, this, this, error)) {
    		        s.request(n);
    		    }
		    }
		}
		
		@Override
		public void cancel() {
		    cancelled = true;
		    s.cancel();
		}
		
		@Override
		public boolean getAsBoolean() {
		    return cancelled;
		}
		
		@Override
		public int size() {
		    return value == null ? 0 : 1;
		}
		
		@Override
		public boolean isEmpty() {
		    return value == null;
		}
		
		@Override
		public boolean offer(T e) {
            throw new UnsupportedOperationException();
		}
		
		@Override
		public T peek() {
		    return value;
		}
		
		@Override
		public T poll() {
		    T v = value;
		    if (v != null) {
		        value = null;
		        return v;
		    }
		    return null;
		}
		
		@Override
		public Iterator<T> iterator() {
		    throw new UnsupportedOperationException();
		}
	}
}
