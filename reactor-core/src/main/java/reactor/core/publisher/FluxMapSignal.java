/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Maps the values of the source publisher one-on-one via a mapper function.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 * @author Stephane Maldini
 */
final class FluxMapSignal<T, R> extends FluxOperator<T, R> {

    final Function<? super T, ? extends R> mapperNext;
    final Function<? super Throwable, ? extends R> mapperError;
    final Supplier<? extends R>            mapperComplete;

    /**
     * Constructs a FluxMapSignal instance with the given source and mappers.
     *
     * @param source the source Publisher instance
     * @param mapperNext the next mapper function
     * @param mapperError the error mapper function
     * @param mapperComplete the complete mapper function
     *
     * @throws NullPointerException if either {@code source} is null or all {@code mapper} are null.
     */
    FluxMapSignal(ContextualPublisher<? extends T> source,
		    @Nullable Function<? super T, ? extends R> mapperNext,
		    @Nullable Function<? super Throwable, ? extends R> mapperError,
		    @Nullable Supplier<? extends R> mapperComplete) {
        super(source);
	    if(mapperNext == null && mapperError == null && mapperComplete == null){
		    throw new IllegalArgumentException("Map Signal needs at least one valid mapper");
	    }

        this.mapperNext = mapperNext;
        this.mapperError = mapperError;
        this.mapperComplete = mapperComplete;
    }

    @Override
    public void subscribe(Subscriber<? super R> s, Context ctx) {
        source.subscribe(new FluxMapSignalSubscriber<>(s, this), ctx);
    }

    static final class FluxMapSignalSubscriber<T, R> 
    extends AbstractQueue<R>
		    implements InnerOperator<T, R>,
		               BooleanSupplier {

        final Subscriber<? super R>            actual;
        final FluxMapSignal<T, R> parent;

        boolean done;

        Subscription s;
        
        R value;
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<FluxMapSignalSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(FluxMapSignalSubscriber.class, "requested");

        volatile boolean cancelled;
        
        long produced;
        
        FluxMapSignalSubscriber(Subscriber<? super R> actual, FluxMapSignal<T, R> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(this);

                if (parent.mapperNext == null) {
		            s.request(Long.MAX_VALUE);
	            }
	        }
        }

        @Override
        public void onNext(T t) {
	        if (done) {
	            Operators.onNextDropped(t);
                return;
            }

	        if (parent.mapperNext == null) {
		        return;
		    }

            R v;

            try {
                v = Objects.requireNonNull(parent.mapperNext.apply(t),
		                "The mapper returned a null value.");
            }
            catch (Throwable e) {
	            done = true;
	            actual.onError(Operators.onOperatorError(s, e, t));
                return;
            }

            produced++;
            actual.onNext(v);
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                Operators.onErrorDropped(t);
                return;
            }

            done = true;

	        if(parent.mapperError == null){
		        actual.onError(t);
		        return;
	        }

	        R v;

	        try {
		        v = Objects.requireNonNull(parent.mapperError.apply(t),
				        "The mapper returned a null value.");
	        }
	        catch (Throwable e) {
		        done = true;
		        actual.onError(Operators.onOperatorError(s, e, t));
		        return;
	        }

	        value = v;
            long p = produced;
            if (p != 0L) {
	            Operators.addAndGet(REQUESTED, this, -p);
            }
	        DrainUtils.postComplete(actual, this, REQUESTED, this, this);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;

	        if(parent.mapperComplete == null){
		        actual.onComplete();
		        return;
	        }

	        R v;

	        try {
		        v = Objects.requireNonNull(parent.mapperComplete.get(),
				        "The mapper returned a null value.");
	        }
	        catch (Throwable e) {
		        done = true;
		        actual.onError(Operators.onOperatorError(s, e));
		        return;
	        }

            value = v;
            long p = produced;
            if (p != 0L) {
	            Operators.addAndGet(REQUESTED, this, -p);
            }
            DrainUtils.postComplete(actual, this, REQUESTED, this, this);
        }

        @Override
        public Subscriber<? super R> actual() {
            return actual;
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
        public boolean offer(R e) {
            throw new UnsupportedOperationException();
        }

        @Override
        @Nullable
        public R poll() {
            R v = value;
            if (v != null) {
                value = null;
                return v;
            }
            return null;
        }

	    @Override
	    @Nullable
	    public Object scanUnsafe(Attr key) {
		    if (key == ScannableAttr.PARENT) return s;
		    if (key == BooleanAttr.TERMINATED) return done;
		    if (key == BooleanAttr.CANCELLED) return getAsBoolean();
		    if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
		    if (key == IntAttr.BUFFERED) return size();

		    return InnerOperator.super.scanUnsafe(key);
	    }

	    @Override
	    @Nullable
        public R peek() {
            return value;
        }

        @Override
        public boolean getAsBoolean() {
            return cancelled;
        }

        @Override
        public void cancel() {
            cancelled = true;
            s.cancel();
        }

        @Override
        public Iterator<R> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return value == null ? 0 : 1;
        }
    }
}