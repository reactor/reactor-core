/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
final class FluxMaterialize<T> extends InternalFluxOperator<T, Signal<T>> {

	FluxMaterialize(Flux<T> source) {
		super(source);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Signal<T>> actual) {
		return new MaterializeSubscriber<>(actual);
	}

	final static class MaterializeSubscriber<T>
	extends AbstractQueue<Signal<T>>
			implements InnerOperator<T, Signal<T>>, BooleanSupplier {
	    
	    final CoreSubscriber<? super Signal<T>> actual;
	    final Context                           cachedContext;

	    Signal<T> terminalSignal;
	    
	    volatile boolean cancelled;
	    
	    volatile long requested;
	    @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<MaterializeSubscriber> REQUESTED =
	            AtomicLongFieldUpdater.newUpdater(MaterializeSubscriber.class, "requested");
	    
	    long produced;
	    
	    Subscription s;
	    
		MaterializeSubscriber(CoreSubscriber<? super Signal<T>> subscriber) {
		    this.actual = subscriber;
		    this.cachedContext = actual.currentContext();
		}

		@Override
		public Context currentContext() {
			return cachedContext;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return terminalSignal != null;
			if (key == Attr.ERROR) return terminalSignal != null ? terminalSignal.getThrowable() : null;
			if (key == Attr.CANCELLED) return getAsBoolean();
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.BUFFERED) return size();

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super Signal<T>> actual() {
			return actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
		    if (Operators.validate(this.s, s)) {
		        this.s = s;

		        actual.onSubscribe(this);
		    }
		}

		@Override
		public void onNext(T ev) {
			if(terminalSignal != null){
				Operators.onNextDropped(ev, this.cachedContext);
				return;
			}
		    produced++;
			actual.onNext(Signal.next(ev, this.cachedContext));
		}

		@Override
		public void onError(Throwable ev) {
			if(terminalSignal != null){
				Operators.onErrorDropped(ev, this.cachedContext);
				return;
			}
			terminalSignal = Signal.error(ev, this.cachedContext);
            long p = produced;
            if (p != 0L) {
	            Operators.addCap(REQUESTED, this, -p);
            }
            DrainUtils.postComplete(actual, this, REQUESTED, this, this);
		}

		@Override
		public void onComplete() {
			if(terminalSignal != null){
				return;
			}
			terminalSignal = Signal.complete(this.cachedContext);
            long p = produced;
            if (p != 0L) {
	            Operators.addCap(REQUESTED, this, -p);
            }
            DrainUtils.postComplete(actual, this, REQUESTED, this, this);
		}
		
		@Override
		public void request(long n) {
		    if (Operators.validate(n)) {
		        if (!DrainUtils.postCompleteRequest(n, actual, this, REQUESTED, this, this)) {
		            s.request(n);
		        }
		    }
		}
		
		@Override
		public void cancel() {
			if(cancelled){
				return;
			}
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
        @Nullable
        @SuppressWarnings("unchecked")
        public Signal<T> poll() {
            Signal<T> v = terminalSignal;
            if (v != null && v != empty) {
	            terminalSignal = (Signal<T>)empty;
                return v;
            }
            return null;
        }

        @Override
        @Nullable
        public Signal<T> peek() {
            return empty == terminalSignal ? null : terminalSignal;
        }

        @Override
        public Iterator<Signal<T>> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return terminalSignal == null || terminalSignal == empty ? 0 : 1;
        }

		@Override
		public String toString() {
			return "MaterializeSubscriber";
		}

		static final Signal empty = new ImmutableSignal<>(Context.empty(), SignalType.ON_NEXT, null, null, null);
	}
}
