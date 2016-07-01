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

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.publisher.Mono;
import reactor.core.util.BackpressureUtils;
import rx.*;
import rx.Completable.CompletableSubscriber;

/**
 * Convert a {@link Publisher Publisher} into a Completable
 *
 * @author Joao Pedro Evangelista
 * @since 2.5
 */
public final class RxJava1CompletableConverter extends PublisherConverter<Completable> {

    static final RxJava1CompletableConverter INSTANCE = new RxJava1CompletableConverter();

    static Completable from(Publisher<?> noValue) {
        return INSTANCE.fromPublisher(noValue);
    }

    static Mono<Void> from(Completable completable) {
        return INSTANCE.toPublisher(completable);
    }


    @Override
    public Mono<Void> toPublisher(Object o) {
        if (o instanceof Completable) {
            return new CompletableAsMono((Completable)o);
        } else {
            return null;
        }
    }

    @Override
    public Completable fromPublisher(Publisher<?> source) {
        return Completable.create(new PublisherAsCompletable(source));
    }

    @Override
    public Class<Completable> get() {
        return Completable.class;
    }
    
    /**
     * Wraps an rx.Completable and exposes it as a Mono&lt;Void>.
     */
    static final class CompletableAsMono extends Mono<Void> {
        final Completable source;
        
        public CompletableAsMono(Completable source) {
            this.source = Objects.requireNonNull(source, "source");
        }
        
        @Override
        public void subscribe(Subscriber<? super Void> s) {
            source.subscribe(new CompletableMonoSubscriber(s));
        }
        
        static final class CompletableMonoSubscriber 
        implements Completable.CompletableSubscriber, Subscription {
            final Subscriber<? super Void> actual;

            rx.Subscription d;
            
            public CompletableMonoSubscriber(Subscriber<? super Void> actual) {
                this.actual = actual;
            }
            
            @Override
            public void onSubscribe(rx.Subscription d) {
                Objects.requireNonNull(d, "rx.Subscription cannot be null!");
                if (this.d != null) {
                    d.unsubscribe();
                    return;
                }
                this.d = d;
                
                actual.onSubscribe(this);
            }
            
            @Override
            public void onError(Throwable e) {
                actual.onError(e);
            }
            
            @Override
            public void onCompleted() {
                actual.onComplete();
            }
            
            @Override
            public void request(long n) {
                // ignored, Completable never returns a value
            }
            
            @Override
            public void cancel() {
                d.unsubscribe();
            }
        }
    }
    
    /**
     * Wraps a Publisher and exposes it as a CompletableOnSubscribe and ignores
     * the onNext signals of the Publisher.
     */
    static final class PublisherAsCompletable implements Completable.CompletableOnSubscribe {
        final Publisher<?> source;
        
        public PublisherAsCompletable(Publisher<?> source) {
            this.source = Objects.requireNonNull(source, "source");
        }
        
        @Override
        public void call(CompletableSubscriber t) {
            source.subscribe(new PublisherCompletableSubscriber(t));
        }
        
        static final class PublisherCompletableSubscriber 
        implements Subscriber<Object>, rx.Subscription {
            final CompletableSubscriber actual;

            Subscription s;
            
            volatile boolean unsubscribed;
            
            public PublisherCompletableSubscriber(CompletableSubscriber actual) {
                this.actual = actual;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                if (BackpressureUtils.validate(this.s, s)) {
                    this.s = s;
                    
                    actual.onSubscribe(this);
                    
                    s.request(Long.MAX_VALUE);
                }
            }
            
            @Override
            public void onNext(Object t) {
                // deliberately ignoring events
            }
            
            @Override
            public void onError(Throwable t) {
                actual.onError(t);
            }
            
            @Override
            public void onComplete() {
                actual.onCompleted();
            }
            
            @Override
            public boolean isUnsubscribed() {
                return unsubscribed;
            }
            
            @Override
            public void unsubscribe() {
                if (unsubscribed) {
                    return;
                }
                unsubscribed = true;
                s.cancel();
            }
        }
    }
}
