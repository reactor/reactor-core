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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;


/**
 * Waits for all Mono sources to produce a value or terminate, and if
 * all of them produced a value, emit a Tuples of those values; otherwise
 * terminate.
 *
 * @param <R> the source value types
 */
final class MonoWhen<T, R> extends Mono<R> {

    final boolean delayError;

	final Publisher<?>[] sources;

	final Iterable<? extends Publisher<?>> sourcesIterable;

	final Function<? super Object[], ? extends R> zipper;

	@SuppressWarnings("unchecked")
	<U> MonoWhen(boolean delayError,
			Publisher<? extends T> p1,
			Publisher<? extends U> p2,
			BiFunction<? super T, ? super U, ? extends R> zipper2) {
		this(delayError,
				new FluxZip.PairwiseZipper<>(new BiFunction[]{
						Objects.requireNonNull(zipper2, "zipper2")}),
				Objects.requireNonNull(p1, "p1"),
				Objects.requireNonNull(p2, "p2"));
	}

	MonoWhen(boolean delayError,
		    Function<? super Object[], ? extends R> zipper,
		    Publisher<?>... sources) {
	    this.delayError = delayError;
	    this.zipper = Objects.requireNonNull(zipper, "zipper");
	    this.sources = Objects.requireNonNull(sources, "sources");
        this.sourcesIterable = null;
    }

	MonoWhen(boolean delayError,
			Function<? super Object[], ? extends R> zipper,
			Iterable<? extends Publisher<?>> sourcesIterable) {
		this.delayError = delayError;
		this.zipper = Objects.requireNonNull(zipper, "zipper");
		this.sources = null;
		this.sourcesIterable = Objects.requireNonNull(sourcesIterable, "sourcesIterable");
	}

	@SuppressWarnings("unchecked")
	Mono<R> whenAdditionalSource(Publisher source, BiFunction zipper) {
		Publisher[] oldSources = sources;
		if (oldSources != null && this.zipper instanceof FluxZip.PairwiseZipper) {
			int oldLen = oldSources.length;
			Publisher<?>[] newSources = new Publisher[oldLen + 1];
			System.arraycopy(oldSources, 0, newSources, 0, oldLen);
			newSources[oldLen] = source;

			Function<Object[], R> z =
					((FluxZip.PairwiseZipper<R>) this.zipper).then(zipper);

			return new MonoWhen<>(delayError, z, newSources);
		}
		return null;
	}

    @SuppressWarnings("unchecked")
    @Override
    public void subscribe(Subscriber<? super R> s) {
	    Publisher<?>[] a;
	    int n = 0;
        if (sources != null) {
            a = sources;
            n = a.length;
        } else {
            a = new Publisher[8];
	        for (Publisher<?> m : sourcesIterable) {
		        if (n == a.length) {
	                Publisher<?>[] b = new Publisher[n + (n >> 2)];
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

	    WhenCoordinator<R> parent =
			    new WhenCoordinator<>(s, n, delayError, zipper);
	    s.onSubscribe(parent);
        parent.subscribe(a);
    }

	static final class WhenCoordinator<R>
			extends Operators.MonoSubscriber<Object, R> {

		final WhenInner<R>[] subscribers;

		final boolean delayError;

		final Function<? super Object[], ? extends R> zipper;

		volatile int done;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<WhenCoordinator> DONE =
                AtomicIntegerFieldUpdater.newUpdater(WhenCoordinator.class, "done");

        @SuppressWarnings("unchecked")
        WhenCoordinator(Subscriber<? super R> subscriber,
		        int n,
		        boolean delayError,
		        Function<? super Object[], ? extends R> zipper) {
	        super(subscriber);

            this.delayError = delayError;
	        this.zipper = zipper;
	        subscribers = new WhenInner[n];
            for (int i = 0; i < n; i++) {
                subscribers[i] = new WhenInner<>(this);
            }
        }

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.TERMINATED) return done == subscribers.length;
			if (key == IntAttr.BUFFERED) return subscribers.length;
			if (key == BooleanAttr.DELAY_ERROR) return delayError;

			return super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}


		void subscribe(Publisher<?>[] sources) {
			WhenInner<R>[] a = subscribers;
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
	                actual.onError(t);
                }
            }
        }
        
        @SuppressWarnings("unchecked")
        void signal() {
	        WhenInner<R>[] a = subscribers;
	        int n = a.length;
            if (DONE.incrementAndGet(this) != n) {
                return;
            }
            
            Object[] o = new Object[n];
            Throwable error = null;
            Throwable compositeError = null;
            boolean hasEmpty = false;
            
            for (int i = 0; i < a.length; i++) {
	            WhenInner<R> m = a[i];
	            Object v = m.value;
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
	            actual.onError(compositeError);
            } else
            if (error != null) {
	            actual.onError(error);
            } else
            if (hasEmpty || zipper == VOID_FUNCTION) {
	            actual.onComplete();
            } else {
	            R r;
	            try {
		            r = Objects.requireNonNull(zipper.apply(o),
				            "zipper produced a null value");
	            }
	            catch (Throwable t) {
		            actual.onError(Operators.onOperatorError(null, t, o));
		            return;
	            }
	            complete(r);
            }
        }
        
        @Override
        public void cancel() {
            if (!isCancelled()) {
                super.cancel();
	            for (WhenInner<R> ms : subscribers) {
		            ms.cancel();
                }
            }
        }
    }

	static final class WhenInner<R> implements InnerConsumer<Object> {

		final WhenCoordinator<R> parent;

        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<WhenInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(WhenInner.class, Subscription.class, "s");
        
        Object value;
        Throwable error;

		WhenInner(WhenCoordinator<R> parent) {
			this.parent = parent;
        }

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == ScannableAttr.PARENT) return s;
			if (key == ScannableAttr.ACTUAL) return parent;
			if (key == ThrowableAttr.ERROR) return error;

			return null;
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
        public void onNext(Object t) {
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
