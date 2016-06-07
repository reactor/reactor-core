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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.*;

import reactor.core.flow.Fuseable;
import reactor.core.subscriber.DeferredScalarSubscriber;
import reactor.core.util.*;

/**
 * Given a Mono source, applies a function on its single item and continues
 * with that Mono instance, emitting its final result.
 *
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @since 2.5
 */
final class MonoThenApply<T, R> extends MonoSource<T, R> implements Fuseable {

	final Function<? super T, ? extends Mono<? extends R>> mapper;

	public MonoThenApply(Publisher<? extends T> source, Function<? super T, ? extends
			Mono<? extends R>> mapper) {
		super(source);
		this.mapper = Objects.requireNonNull(mapper, "mapper");
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {

		if (FluxFlatMap.trySubscribeScalarMap(source, s, mapper, true)) {
			return;
		}

		MonoThenApplyManager<T, R> manager = new MonoThenApplyManager<>(s, mapper);
		s.onSubscribe(manager);
		
		source.subscribe(manager);
	}
	
	static final class MonoThenApplyManager<T, R> extends DeferredScalarSubscriber<T, R> {

	    final Function<? super T, ? extends Mono<? extends R>> mapper;

	    final SecondSubscriber<R> second;
	    
	    boolean done;
	    
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<MonoThenApplyManager, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(MonoThenApplyManager.class, Subscription.class, "s");
	    
        public MonoThenApplyManager(Subscriber<? super R> subscriber, Function<? super T, ? extends Mono<? extends R>> mapper) {
            super(subscriber);
            this.mapper = mapper;
            this.second = new SecondSubscriber<>(this);
        }
	    
        @Override
        public void onSubscribe(Subscription s) {
            if (BackpressureUtils.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(T t) {
            if (done) {
                Exceptions.onNextDropped(t);
                return;
            }
            done = true;
            
            Mono<? extends R> m;
            
            try {
                m = mapper.apply(t);
            } catch (Throwable ex) {
                Exceptions.throwIfFatal(ex);
                subscriber.onError(ex);
                return;
            }
            
            if (m == null) {
                subscriber.onError(new NullPointerException("The mapper returned a null Mono"));
                return;
            }
            
            if (m instanceof Callable) {
                @SuppressWarnings("unchecked")
                Callable<R> c = (Callable<R>) m;
                
                R v;
                try {
                    v = c.call();
                } catch (Throwable ex) {
                    Exceptions.throwIfFatal(ex);
                    subscriber.onError(ex);
                    return;
                }
                
                if (v == null) {
                    subscriber.onComplete();
                } else {
                    complete(v);
                }
                return;
            }

            try {
                m.subscribe(second);
            }
            catch (Throwable e){
                Exceptions.throwIfFatal(e);
                subscriber.onError(e);
            }
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                Exceptions.onErrorDropped(t);
                return;
            }
            done = true;
            subscriber.onError(t);
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            subscriber.onComplete();
        }
        
        @Override
        public void cancel() {
            super.cancel();
            BackpressureUtils.terminate(S, this);
            second.cancel();
        }
        
        void secondError(Throwable ex) {
            subscriber.onError(ex);
        }
        
        void secondComplete() {
            subscriber.onComplete();
        }
        
        static final class SecondSubscriber<R> implements Subscriber<R> {

            final MonoThenApplyManager<?, R> parent;
            
            volatile Subscription s;
            @SuppressWarnings("rawtypes")
            static final AtomicReferenceFieldUpdater<SecondSubscriber, Subscription> S =
                    AtomicReferenceFieldUpdater.newUpdater(SecondSubscriber.class, Subscription.class, "s");
            
            boolean done;
            
            public SecondSubscriber(MonoThenApplyManager<?, R> parent) {
                this.parent = parent;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                if (BackpressureUtils.setOnce(S, this, s)) {
                    s.request(Long.MAX_VALUE);
                }
            }

            @Override
            public void onNext(R t) {
                if (done) {
                    Exceptions.onNextDropped(t);
                    return;
                }
                done = true;
                this.parent.complete(t);
            }

            @Override
            public void onError(Throwable t) {
                if (done) {
                    Exceptions.onErrorDropped(t);
                    return;
                }
                done = true;
                this.parent.secondError(t);
            }

            @Override
            public void onComplete() {
                if (done) {
                    return;
                }
                done = true;
                this.parent.secondComplete();
            }
            
            void cancel() {
                BackpressureUtils.terminate(S, this);
            }
            
        }
	}
}
