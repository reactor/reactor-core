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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * Maps the values of the source publisher one-on-one via a mapper function.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 * @author Stephane Maldini
 */
final class FluxMapSignal<T, R> extends InternalFluxOperator<T, R> {

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
    FluxMapSignal(Flux<? extends T> source,
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
    public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
        return new FluxMapSignalSubscriber<>(actual,
                mapperNext,
                mapperError,
                mapperComplete);
    }

	@Override
	public Object scanUnsafe(Attr key) {
    	if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class FluxMapSignalSubscriber<T, R>
    extends AbstractQueue<R>
		    implements InnerOperator<T, R>,
		               BooleanSupplier {

	    final CoreSubscriber<? super R>                actual;
	    final Function<? super T, ? extends R>         mapperNext;
	    final Function<? super Throwable, ? extends R> mapperError;
	    final Supplier<? extends R>                    mapperComplete;

        boolean done;

        Subscription s;
        
        R value;
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<FluxMapSignalSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(FluxMapSignalSubscriber.class, "requested");

        volatile boolean cancelled;
        
        long produced;

	    FluxMapSignalSubscriber(CoreSubscriber<? super R> actual,
		        @Nullable Function<? super T, ? extends R> mapperNext,
		        @Nullable Function<? super Throwable, ? extends R> mapperError,
		        @Nullable Supplier<? extends R>            mapperComplete) {
            this.actual = actual;
            this.mapperNext = mapperNext;
            this.mapperError = mapperError;
            this.mapperComplete = mapperComplete;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(this);

                if (mapperNext == null) {
		            s.request(Long.MAX_VALUE);
	            }
	        }
        }

        @Override
        public void onNext(T t) {
	        if (done) {
	            Operators.onNextDropped(t, actual.currentContext());
                return;
            }

	        if (mapperNext == null) {
		        return;
		    }

            R v;

            try {
                v = Objects.requireNonNull(mapperNext.apply(t),
		                "The mapper returned a null value.");
            }
            catch (Throwable e) {
	            done = true;
	            actual.onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
                return;
            }

            produced++;
            actual.onNext(v);
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                Operators.onErrorDropped(t, actual.currentContext());
                return;
            }

            done = true;

	        if(mapperError == null){
		        actual.onError(t);
		        return;
	        }

	        R v;

	        try {
		        v = Objects.requireNonNull(mapperError.apply(t),
				        "The mapper returned a null value.");
	        }
	        catch (Throwable e) {
		        done = true;
		        actual.onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
		        return;
	        }

	        value = v;
            long p = produced;
            if (p != 0L) {
	            Operators.addCap(REQUESTED, this, -p);
            }
	        DrainUtils.postComplete(actual, this, REQUESTED, this, this);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;

	        if(mapperComplete == null){
		        actual.onComplete();
		        return;
	        }

	        R v;

	        try {
		        v = Objects.requireNonNull(mapperComplete.get(),
				        "The mapper returned a null value.");
	        }
	        catch (Throwable e) {
		        done = true;
		        actual.onError(Operators.onOperatorError(s, e, actual.currentContext()));
		        return;
	        }

            value = v;
            long p = produced;
            if (p != 0L) {
	            Operators.addCap(REQUESTED, this, -p);
            }
            DrainUtils.postComplete(actual, this, REQUESTED, this, this);
        }

        @Override
        public CoreSubscriber<? super R> actual() {
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
		    if (key == Attr.PARENT) return s;
		    if (key == Attr.TERMINATED) return done;
		    if (key == Attr.CANCELLED) return getAsBoolean();
		    if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
		    if (key == Attr.BUFFERED) return size();
		    if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

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