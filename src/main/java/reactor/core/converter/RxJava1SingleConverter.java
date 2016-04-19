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

package reactor.core.converter;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.publisher.Mono;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;
import rx.Single;
import rx.SingleSubscriber;

/**
 * Convert a RxJava 1 {@link Single} to/from a Reactive Streams {@link Publisher}.
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public final class RxJava1SingleConverter extends PublisherConverter<Single> {

	static final RxJava1SingleConverter INSTANCE = new RxJava1SingleConverter();

	@SuppressWarnings("unchecked")
	static public <T> Single<T> from(Publisher<T> o) {
		return INSTANCE.fromPublisher(o);
	}

	@SuppressWarnings("unchecked")
	static public <T> Mono<T> from(Single<T> o) {
		return INSTANCE.toPublisher(o);
	}

	@Override
	public Single fromPublisher(Publisher<?> o) {
	    if (o instanceof Fuseable.ScalarCallable) {
            Fuseable.ScalarCallable<?> scalarCallable = (Fuseable.ScalarCallable<?>) o;
		    try {
			    Object v = scalarCallable.call();
			    if (v == null) {
				    return Single.error(new NoSuchElementException("Can't convert an empty Publisher to Single"));
			    }
			    return Single.just(v);
		    }
		    catch(Throwable t){
			    Exceptions.throwIfFatal(t);
			    return Single.error(t);
		    }
	    }
		return Single.create(new PublisherAsSingle<>(o));
	}

	@SuppressWarnings("rawtypes")
    @Override
	public Mono toPublisher(Object o) { 
	    if (o instanceof Single) {
    		Single<?> single = (Single<?>) o;
    		/*if (single instanceof ScalarSynchronousSingle) {
    		    Object v = ((ScalarSynchronousSingle)single).get();
    		    if (v == null) {
    		        return Mono.error(new NullPointerException("The wrapped Single produced a null value"));
    		    }
                return Mono.just(v);
    		}*/
    		return new SingleAsMono<>(single);
	    }
	    return null;
	}

	@Override
	public Class<Single> get() {
		return Single.class;
	}

	/**
	 * Wraps a Publisher and exposes it as a rx.Single, firing NoSuchElementException
	 * for empty Publisher and IndexOutOfBoundsException if it has more than one element.
	 *
	 * @param <T> the value type
	 */
	static final class PublisherAsSingle<T> implements Single.OnSubscribe<T> {
	    final Publisher<? extends T> source;
	    
        public PublisherAsSingle(Publisher<? extends T> source) {
            this.source = source;
        }

        @Override
	    public void call(SingleSubscriber<? super T> t) {
            source.subscribe(new PublisherAsSingleSubscriber<>(t));
	    }
        
        static final class PublisherAsSingleSubscriber<T> 
        implements Subscriber<T>, rx.Subscription {
            final SingleSubscriber<? super T> actual;
            
            Subscription s;
            
            boolean done;
            
            boolean hasValue;
            
            T value;
            
            volatile boolean terminated;
            
            public PublisherAsSingleSubscriber(SingleSubscriber<? super T> actual) {
                this.actual = actual;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                if (BackpressureUtils.validate(this.s, s)) {
                    this.s = s;
                    
                    actual.add(this);
                    
                    s.request(Long.MAX_VALUE);
                }
            }
            
            @Override
            public void onNext(T t) {
                if (done) {
                    Exceptions.onNextDropped(t);
                    return;
                }
                
                if (hasValue) {
                    done = true;
                    value = null;
                    unsubscribe();
                    
                    actual.onError(new IndexOutOfBoundsException("The wrapped Publisher produced more than one value"));
                    
                    return;
                }
                
                hasValue = true;
                value = t;
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
                
                if (hasValue) {
                    T v = value;
                    value = null;
                    
                    actual.onSuccess(v);
                } else {
                    actual.onError(new NoSuchElementException("The wrapped Publisher didn't produce any value"));
                }
            }
            
            @Override
            public void unsubscribe() {
                if (terminated) {
                    return;
                }
                terminated = true;
                s.cancel();
            }
            
            @Override
            public boolean isUnsubscribed() {
                return terminated;
            }
        }
	}
	
	/**
	 * Wraps an rx.Single and exposes it as a Mono&lt;T>, signalling NullPointerException
	 * if the Single produces a null value.
	 *
	 * @param <T> the value type
	 */
	static final class SingleAsMono<T> extends Mono<T> {
	    final Single<? extends T> source;
	    
	    public SingleAsMono(Single<? extends T> source) {
	        this.source = source;
	    }
	    
	    @Override
	    public void subscribe(Subscriber<? super T> s) {
	        SingleAsMonoSubscriber<T> parent = new SingleAsMonoSubscriber<>(s);
	        s.onSubscribe(parent);
            source.subscribe(parent);
	    }
	    
	    static final class SingleAsMonoSubscriber<T> extends SingleSubscriber<T> implements Subscription {
	        final Subscriber<? super T> actual;
	        
	        T value;
	        
	        volatile int state;
	        @SuppressWarnings("rawtypes")
            static final AtomicIntegerFieldUpdater<SingleAsMonoSubscriber> STATE =
	                AtomicIntegerFieldUpdater.newUpdater(SingleAsMonoSubscriber.class, "state");
	        
	        static final int NO_REQUEST_NO_VALUE = 0;
            static final int NO_REQUEST_HAS_VALUE = 1;
            static final int HAS_REQUEST_NO_VALUE = 2;
            static final int HAS_REQUEST_HAS_VALUE = 3;
	        
	        public SingleAsMonoSubscriber(Subscriber<? super T> actual) {
	            this.actual = actual;
	        }
	        
	        @Override
	        public void onSuccess(T value) {
	            if (value == null) {
	                actual.onError(new NullPointerException("The wrapped Single produced a null value"));
	                return;
	            }
	            for (;;) {
	                int s = state;
	                if (s == HAS_REQUEST_HAS_VALUE || s == NO_REQUEST_HAS_VALUE || isUnsubscribed()) {
	                    break;
	                } else
	                if (s == HAS_REQUEST_NO_VALUE) {
	                    if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
	                        actual.onNext(value);
	                        if (!isUnsubscribed()) {
	                            actual.onComplete();
	                        }
	                    }
                        break;
	                } else {
	                    this.value = value;
	                    if (STATE.compareAndSet(this, s, NO_REQUEST_HAS_VALUE)) {
	                        break;
	                    }
	                }
	            }
	        }
	        
	        @Override
	        public void onError(Throwable error) {
	            actual.onError(error);
	        }
	        
	        @Override
	        public void request(long n) {
	            if (BackpressureUtils.validate(n)) {
	                for (;;) {
	                    int s = state;
	                    if (s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE || isUnsubscribed()) {
	                        break;
	                    } else
	                    if (s == NO_REQUEST_HAS_VALUE) {
	                        if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
	                            T v = value;
	                            value = null;
	                            actual.onNext(v);
	                            if (!isUnsubscribed()) {
	                                actual.onComplete();
	                            }
	                        }
                            break;
	                    } else {
	                        if (STATE.compareAndSet(this, s, HAS_REQUEST_NO_VALUE)) {
	                            break;
	                        }
	                    }
	                }
	            }
	        }
	        
	        @Override
	        public void cancel() {
	            unsubscribe();
	        }
	    }
	}
}
