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

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
final class FluxMaterialize<T> extends FluxSource<T, Signal<T>> {

	public FluxMaterialize(Publisher<T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super Signal<T>> subscriber) {
		source.subscribe(new MaterializeAction<>(subscriber));
	}

	final static class MaterializeAction<T>
	extends AbstractQueue<Signal<T>>
	implements Subscriber<T>, Subscription, BooleanSupplier {
	    
	    final Subscriber<? super Signal<T>> actual;

	    Signal<T> value;
	    
	    volatile boolean cancelled;
	    
	    volatile long requested;
	    @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<MaterializeAction> REQUESTED =
	            AtomicLongFieldUpdater.newUpdater(MaterializeAction.class, "requested");
	    
	    long produced;
	    
	    Subscription s;
	    
		public MaterializeAction(Subscriber<? super Signal<T>> subscriber) {
		    this.actual = subscriber;
		}

		@Override
		public void onSubscribe(Subscription s) {
		    if (SubscriptionHelper.validate(this.s, s)) {
		        this.s = s;
		        
		        actual.onSubscribe(this);
		    }
		}
		
		@Override
		public void onNext(T ev) {
		    produced++;
			actual.onNext(Signal.next(ev));
		}

		@Override
		public void onError(Throwable ev) {
			value = Signal.error(ev);
            long p = produced;
            if (p != 0L) {
                REQUESTED.addAndGet(this, -p);
            }
            DrainUtils.postComplete(actual, this, REQUESTED, this, this);
		}

		@Override
		public void onComplete() {
			value = Signal.complete();
            long p = produced;
            if (p != 0L) {
                REQUESTED.addAndGet(this, -p);
            }
            DrainUtils.postComplete(actual, this, REQUESTED, this, this);
		}
		
		@Override
		public void request(long n) {
		    if (SubscriptionHelper.validate(n)) {
		        if (!DrainUtils.postCompleteRequest(n, actual, this, REQUESTED, this, this)) {
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
        public boolean offer(Signal<T> e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Signal<T> poll() {
            Signal<T> v = value;
            if (v != null) {
                value = null;
                return v;
            }
            return null;
        }

        @Override
        public Signal<T> peek() {
            return value;
        }

        @Override
        public Iterator<Signal<T>> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return value == null ? 0 : 1;
        }
	}
}
