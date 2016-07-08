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

import org.reactivestreams.*;

import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.operators.completable.CompletableFromFlowable;
import io.reactivex.internal.operators.single.SingleFromPublisher;
import reactor.core.flow.Fuseable;
import reactor.core.publisher.*;
import reactor.core.subscriber.DeferredScalarSubscriber;
import reactor.core.util.BackpressureUtils;

/**
 * Conversion methods and fluent-transformers for interoperating with RxJava 2, including
 * operator fusion.
 * 
 * @since 2.5
 * @author akarnokd
 */
public enum RxJava2Interop {
    ;
    
    /**
     * Wraps a Flowable instance into a Flux instance, composing the micro-fusion
     * properties of the Flowable through.
     * @param <T> the value type
     * @param source the source flowable
     * @return the new Flux instance
     */
    public static <T> Flux<T> toFlux(Flowable<T> source) {
        // due to RxJava's own hooks, there is no matching of scalar- and callable types
        // as it would lose tracking information
        return new FlowableAsFlux<>(source);
    }
    
    /**
     * Wraps a Flux instance into a Flowable instance, composing the micro-fusion
     * properties of the Flux through.
     * @param <T> the value type
     * @param source the source flux
     * @return the new Flux instance
     */
    public static <T> Flowable<T> toFlowable(Flux<T> source) {
        return new FluxAsFlowable<>(source);
    }

    /**
     * Wraps a Mono instance into a Flowable instance, composing the micro-fusion
     * properties of the Flux through.
     * @param <T> the value type
     * @param source the source flux
     * @return the new Flux instance
     */
    public static <T> Flowable<T> toFlowable(Mono<T> source) {
        return new FluxAsFlowable<>(source);
    }
    
    /**
     * Wraps a void-Mono instance into a RxJava Completable.
     * @param source the source Mono instance
     * @return the new Completable instance
     */
    public static Completable toCompletable(Mono<?> source) {
        return new CompletableFromFlowable<>(source);
    }
    
    /**
     * Wraps a RxJava Completable into a Mono instance.
     * @param source the source Completable
     * @return the new Mono instance
     */
    public static Mono<Void> toMono(Completable source) {
        return new CompletableAsMono(source);
    }
    
    /**
     * Wraps a Mono instance into a RxJava Single.
     * <p>If the Mono is empty, the single will signal a
     * {@link NoSuchElementException}.
     * @param <T> the value type
     * @param source the source Mono instance
     * @return the new Single instance
     */
    public static <T> Single<T> toSingle(Mono<T> source) {
        return new SingleFromPublisher<>(source);
    }
    
    /**
     * Wraps a RxJava Single into a Mono instance.
     * @param <T> the value type
     * @param source the source Single
     * @return the new Mono instance
     */
    public static <T> Mono<T> toMono(Single<T> source) {
        return new SingleAsMono<>(source);
    }

    static final class FlowableAsFlux<T> extends Flux<T> implements Fuseable {
        
        final Publisher<T> source;
        
        public FlowableAsFlux(Publisher<T> source) {
            this.source = source;
        }
        
        @Override
        public void subscribe(Subscriber<? super T> s) {
            if (s instanceof Fuseable.ConditionalSubscriber) {
                source.subscribe(new FlowableAsFluxConditionalSubscriber<>((Fuseable.ConditionalSubscriber<? super T>)s));
            } else {
                source.subscribe(new FlowableAsFluxSubscriber<>(s));
            }
        }
        
        static final class FlowableAsFluxSubscriber<T> implements Subscriber<T>, Fuseable.QueueSubscription<T> {
            
            final Subscriber<? super T> actual;

            Subscription s;
            
            io.reactivex.internal.fuseable.QueueSubscription<T> qs;
            
            public FlowableAsFluxSubscriber(Subscriber<? super T> actual) {
                this.actual = actual;
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public void onSubscribe(Subscription s) {
                if (BackpressureUtils.validate(this.s, s)) {
                    this.s = s;
                    if (s instanceof io.reactivex.internal.fuseable.QueueSubscription) {
                        this.qs = (io.reactivex.internal.fuseable.QueueSubscription<T>)s;
                    }
                    
                    actual.onSubscribe(this);
                }
            }
            
            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }
            
            @Override
            public void onError(Throwable t) {
                actual.onError(t);
            }
            
            @Override
            public void onComplete() {
                actual.onComplete();
            }
            
            @Override
            public void request(long n) {
                s.request(n);
            }
            
            @Override
            public void cancel() {
                s.cancel();
            }

            @Override
            public T poll() {
                return qs.poll();
            }

            @Override
            public int size() {
                return 0; // not supported
            }

            @Override
            public boolean isEmpty() {
                return qs.isEmpty();
            }

            @Override
            public void clear() {
                qs.clear();
            }

            @Override
            public int requestFusion(int requestedMode) {
                if (qs != null) {
                    return qs.requestFusion(requestedMode);
                }
                return NONE;
            }
        }
        
        static final class FlowableAsFluxConditionalSubscriber<T> implements 
        Fuseable.ConditionalSubscriber<T>, Fuseable.QueueSubscription<T> {
            
            final Fuseable.ConditionalSubscriber<? super T> actual;

            Subscription s;
            
            io.reactivex.internal.fuseable.QueueSubscription<T> qs;
            
            public FlowableAsFluxConditionalSubscriber(Fuseable.ConditionalSubscriber<? super T> actual) {
                this.actual = actual;
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public void onSubscribe(Subscription s) {
                if (BackpressureUtils.validate(this.s, s)) {
                    this.s = s;
                    if (s instanceof io.reactivex.internal.fuseable.QueueSubscription) {
                        this.qs = (io.reactivex.internal.fuseable.QueueSubscription<T>)s;
                    }
                    
                    actual.onSubscribe(this);
                }
            }
            
            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }
            
            @Override
            public boolean tryOnNext(T t) {
                return actual.tryOnNext(t);
            }
            
            @Override
            public void onError(Throwable t) {
                actual.onError(t);
            }
            
            @Override
            public void onComplete() {
                actual.onComplete();
            }
            
            @Override
            public void request(long n) {
                s.request(n);
            }
            
            @Override
            public void cancel() {
                s.cancel();
            }

            @Override
            public T poll() {
                return qs.poll();
            }

            @Override
            public int size() {
                return 0; // not supported
            }

            @Override
            public boolean isEmpty() {
                return qs.isEmpty();
            }

            @Override
            public void clear() {
                qs.clear();
            }

            @Override
            public int requestFusion(int requestedMode) {
                if (qs != null) {
                    return qs.requestFusion(requestedMode);
                }
                return NONE;
            }
        }
    }
    
    static final class FluxAsFlowable<T> extends Flowable<T> {
        
        final Publisher<T> source;
        
        public FluxAsFlowable(Publisher<T> source) {
            this.source = source;
        }
        
        @Override
        public void subscribeActual(Subscriber<? super T> s) {
            if (s instanceof io.reactivex.internal.fuseable.ConditionalSubscriber) {
                source.subscribe(new FluxAsFlowableConditionalSubscriber<>((io.reactivex.internal.fuseable.ConditionalSubscriber<? super T>)s));
            } else {
                source.subscribe(new FluxAsFlowableSubscriber<>(s));
            }
        }
        
        static final class FluxAsFlowableSubscriber<T> implements Subscriber<T>, 
        io.reactivex.internal.fuseable.QueueSubscription<T>, Fuseable.QueueSubscription<T> {
            
            final Subscriber<? super T> actual;

            Subscription s;
            
            Fuseable.QueueSubscription<T> qs;
            
            public FluxAsFlowableSubscriber(Subscriber<? super T> actual) {
                this.actual = actual;
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public void onSubscribe(Subscription s) {
                if (BackpressureUtils.validate(this.s, s)) {
                    this.s = s;
                    if (s instanceof Fuseable.QueueSubscription) {
                        this.qs = (Fuseable.QueueSubscription<T>)s;
                    }
                    
                    actual.onSubscribe(this);
                }
            }
            
            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }
            
            @Override
            public void onError(Throwable t) {
                actual.onError(t);
            }
            
            @Override
            public void onComplete() {
                actual.onComplete();
            }
            
            @Override
            public void request(long n) {
                s.request(n);
            }
            
            @Override
            public void cancel() {
                s.cancel();
            }

            @Override
            public T poll() {
                return qs.poll();
            }

            @Override
            public int size() {
                return 0; // not supported
            }

            @Override
            public boolean isEmpty() {
                return qs.isEmpty();
            }

            @Override
            public void clear() {
                qs.clear();
            }

            @Override
            public int requestFusion(int requestedMode) {
                if (qs != null) {
                    return qs.requestFusion(requestedMode);
                }
                return NONE;
            }
        }
        
        static final class FluxAsFlowableConditionalSubscriber<T> implements 
        Fuseable.ConditionalSubscriber<T>, Fuseable.QueueSubscription<T>, io.reactivex.internal.fuseable.QueueSubscription<T> {
            
            final io.reactivex.internal.fuseable.ConditionalSubscriber<? super T> actual;

            Subscription s;
            
            io.reactivex.internal.fuseable.QueueSubscription<T> qs;
            
            public FluxAsFlowableConditionalSubscriber(io.reactivex.internal.fuseable.ConditionalSubscriber<? super T> actual) {
                this.actual = actual;
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public void onSubscribe(Subscription s) {
                if (BackpressureUtils.validate(this.s, s)) {
                    this.s = s;
                    if (s instanceof io.reactivex.internal.fuseable.QueueSubscription) {
                        this.qs = (io.reactivex.internal.fuseable.QueueSubscription<T>)s;
                    }
                    
                    actual.onSubscribe(this);
                }
            }
            
            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }
            
            @Override
            public boolean tryOnNext(T t) {
                return actual.tryOnNext(t);
            }
            
            @Override
            public void onError(Throwable t) {
                actual.onError(t);
            }
            
            @Override
            public void onComplete() {
                actual.onComplete();
            }
            
            @Override
            public void request(long n) {
                s.request(n);
            }
            
            @Override
            public void cancel() {
                s.cancel();
            }

            @Override
            public T poll() {
                return qs.poll();
            }

            @Override
            public int size() {
                return 0; // not supported
            }

            @Override
            public boolean isEmpty() {
                return qs.isEmpty();
            }

            @Override
            public void clear() {
                qs.clear();
            }

            @Override
            public int requestFusion(int requestedMode) {
                if (qs != null) {
                    return qs.requestFusion(requestedMode);
                }
                return NONE;
            }
        }
    }
    
    static final class CompletableAsMono extends Mono<Void> implements Fuseable {
        
        final Completable source;

        public CompletableAsMono(Completable source) {
            this.source = source;
        }
        
        @Override
        public void subscribe(Subscriber<? super Void> s) {
            source.subscribe(new CompletableAsMonoSubscriber(s));
        }
        
        static final class CompletableAsMonoSubscriber implements CompletableSubscriber, 
        Fuseable.QueueSubscription<Void> {
            
            final Subscriber<? super Void> actual;

            Disposable d;
            
            public CompletableAsMonoSubscriber(Subscriber<? super Void> actual) {
                this.actual = actual;
            }
            
            @Override
            public void onSubscribe(Disposable d) {
                this.d = d;
                actual.onSubscribe(this);
            }
            
            @Override
            public void onError(Throwable e) {
                actual.onError(e);
            }
            
            @Override
            public void onComplete() {
                actual.onComplete();
            }
            
            @Override
            public void request(long n) {
                // no-op as Completable never signals any value
            }
            
            @Override
            public void cancel() {
                d.dispose();
            }
            
            @Override
            public boolean isEmpty() {
                return true;
            }
            
            @Override
            public Void poll() {
                return null; // always empty
            }
            
            @Override
            public int requestFusion(int requestedMode) {
                return requestedMode & Fuseable.ASYNC;
            }
            
            @Override
            public int size() {
                return 0;
            }
            
            @Override
            public void clear() {
                // nothing to clear
            }
        }
    }

    static final class SingleAsMono<T> extends Mono<T> implements Fuseable {
        
        final Single<T> source;

        public SingleAsMono(Single<T> source) {
            this.source = source;
        }
        
        @Override
        public void subscribe(Subscriber<? super T> s) {
            SingleSubscriber<? super T> single = new SingleAsMonoSubscriber<>(s);
            source.subscribe(single);
        }
        
        static final class SingleAsMonoSubscriber<T> extends DeferredScalarSubscriber<T, T>
        implements SingleSubscriber<T> {

            Disposable d;
            
            public SingleAsMonoSubscriber(Subscriber<? super T> subscriber) {
                super(subscriber);
            }

            @Override
            public void onSubscribe(Disposable d) {
                this.d = d;
                subscriber.onSubscribe(this);
            }

            @Override
            public void onSuccess(T value) {
                complete(value);
            }
            
        }
    }
}
