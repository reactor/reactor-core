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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;

/**
 * Waits for all Mono sources to produce a value or terminate, and if
 * all of them produced a value, emit a Tuples of those values; otherwise
 * terminate.
 *
 * @param <T> the source value types
 */
final class MonoWhen<T, R> extends Mono<R> {

    final boolean delayError;

	final Publisher<? extends T>[] sources;

	final Iterable<? extends Publisher<? extends T>> sourcesIterable;

	final Function<? super Object[], ? extends R> zipper;

    @SafeVarargs
    public MonoWhen(boolean delayError,
		    Function<? super Object[], ? extends R> zipper,
		    Publisher<? extends T>... sources) {
	    this.delayError = delayError;
	    this.zipper = Objects.requireNonNull(zipper, "zipper");
	    this.sources = Objects.requireNonNull(sources, "sources");
        this.sourcesIterable = null;
    }

	public MonoWhen(boolean delayError,
			Function<? super Object[], ? extends R> zipper,
			Iterable<? extends Publisher<? extends T>> sourcesIterable) {
		this.delayError = delayError;
		this.zipper = Objects.requireNonNull(zipper, "zipper");
		this.sources = null;
		this.sourcesIterable = Objects.requireNonNull(sourcesIterable, "sourcesIterable");
	}

    @SuppressWarnings("unchecked")
    @Override
    public void subscribe(Subscriber<? super R> s) {
	    Publisher<? extends T>[] a;
	    int n = 0;
        if (sources != null) {
            a = sources;
            n = a.length;
        } else {
            a = new Mono[8];
	        for (Publisher<? extends T> m : sourcesIterable) {
		        if (n == a.length) {
	                Publisher<? extends T>[] b = new Publisher[n + (n >> 2)];
			        System.arraycopy(a, 0, b, 0, n);
                    a = b;
                }
                a[n++] = m;
            }
        }
        
        if (n == 0) {
            Operators.complete(s);
            return;
        }

	    MonoWhenCoordinator<T, R> parent =
			    new MonoWhenCoordinator<>(s, n, delayError, zipper);
	    s.onSubscribe(parent);
        parent.subscribe(a);
    }

	static final class MonoWhenCoordinator<T, R>
			extends Operators.DeferredScalarSubscriber<T, R> implements Subscription {

		final MonoWhenSubscriber<T, R>[] subscribers;

		final boolean delayError;

		final Function<? super Object[], ? extends R> zipper;

		volatile int done;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<MonoWhenCoordinator> DONE =
                AtomicIntegerFieldUpdater.newUpdater(MonoWhenCoordinator.class, "done");

        @SuppressWarnings("unchecked")
        public MonoWhenCoordinator(Subscriber<? super R> subscriber,
		        int n,
		        boolean delayError,
		        Function<? super Object[], ? extends R> zipper) {
	        super(subscriber);
            this.delayError = delayError;
	        this.zipper = zipper;
	        subscribers = new MonoWhenSubscriber[n];
            for (int i = 0; i < n; i++) {
                subscribers[i] = new MonoWhenSubscriber<>(this);
            }
        }

		void subscribe(Publisher<? extends T>[] sources) {
			MonoWhenSubscriber<T, R>[] a = subscribers;
			for (int i = 0; i < a.length; i++) {
                sources[i].subscribe(a[i]);
            }
        }
        
        void signalError(Throwable t) {
            if (delayError) {
                signal();
            } else {
                int n = subscribers.length;
                if (DONE.getAndSet(this, n) != n) {
                    cancel();
                    subscriber.onError(t);
                }
            }
        }
        
        @SuppressWarnings("unchecked")
        void signal() {
	        MonoWhenSubscriber<T, R>[] a = subscribers;
	        int n = a.length;
            if (DONE.incrementAndGet(this) != n) {
                return;
            }
            
            Object[] o = new Object[n];
            Throwable error = null;
            Throwable compositeError = null;
            boolean hasEmpty = false;
            
            for (int i = 0; i < a.length; i++) {
	            MonoWhenSubscriber<T, R> m = a[i];
	            T v = m.value;
                if (v != null) {
                    o[i] = v;
                } else {
                    Throwable e = m.error;
                    if (e != null) {
                        if (compositeError != null) {
                            compositeError.addSuppressed(e);
                        } else
                        if (error != null) {
                            compositeError = new Throwable("Multiple errors");
                            compositeError.addSuppressed(error);
                            compositeError.addSuppressed(e);
                        } else {
                            error = e;
                        }
                    } else {
                        hasEmpty = true;
                    }
                }
            }
            
            if (compositeError != null) {
                subscriber.onError(compositeError);
            } else
            if (error != null) {
                subscriber.onError(error);
            } else
            if (hasEmpty || zipper == VOID_FUNCTION) {
                subscriber.onComplete();
            } else {
	            R r;
	            try {
		            r = zipper.apply(o);
	            }
	            catch (Throwable t) {
		            subscriber.onError(Exceptions.onOperatorError(null, t, o));
		            return;
	            }
	            if (r == null) {
		            subscriber.onError(Exceptions.onOperatorError(null,
				            new NullPointerException("zipper produced a null value"),
				            o));
		            return;
	            }
	            complete(r);
            }
        }
        
        @Override
        public void cancel() {
            if (!isCancelled()) {
                super.cancel();
	            for (MonoWhenSubscriber<T, R> ms : subscribers) {
		            ms.cancel();
                }
            }
        }
    }

	static final class MonoWhenSubscriber<T, R> implements Subscriber<T> {

		final MonoWhenCoordinator<T, R> parent;

        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<MonoWhenSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(MonoWhenSubscriber.class, Subscription.class, "s");
        
        T value;
        Throwable error;

		public MonoWhenSubscriber(MonoWhenCoordinator<T, R> parent) {
			this.parent = parent;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            } else {
                s.cancel();
            }
        }
        
        @Override
        public void onNext(T t) {
            if (value == null) {
                value = t;
                parent.signal();
            }
        }
        
        @Override
        public void onError(Throwable t) {
            error = t;
            parent.signalError(t);
        }
        
        @Override
        public void onComplete() {
            if (value == null) {
                parent.signal();
            }
        }
        
        void cancel() {
            Operators.terminate(S, this);
        }
    }
}
