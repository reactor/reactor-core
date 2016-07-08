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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.flow.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.util.*;
import rx.*;
import rx.internal.util.ScalarSynchronousObservable;

/**
 * Convert a RxJava 1 {@link Observable} to/from a Reactive Streams {@link Publisher}.
 *
 * @author Stephane Maldini
 * @author Sebastien Deleuze
 * @since 2.5
 */
@SuppressWarnings("rawtypes")
public enum RxJava1ObservableConverter {
	;

	@SuppressWarnings("unchecked")
	public static <T> Observable<T> fromPublisher(final Publisher<T> publisher) {
	    if (publisher instanceof Fuseable.ScalarCallable) {
            Fuseable.ScalarCallable<T> scalarCallable = (Fuseable.ScalarCallable<T>) publisher;

            T v = scalarCallable.call();
            if (v == null) {
                return Observable.empty();
            }
            return Observable.just(v);
	    }
		return Observable.create(new PublisherAsObservable<>(publisher));
	}

	@SuppressWarnings("unchecked")
	public static <T> Flux<T> toPublisher(Observable<T> obs) {
		if (obs == Observable.empty()) {
		    return Flux.empty();
		}
		if (obs instanceof ScalarSynchronousObservable) {
			return Flux.just(((ScalarSynchronousObservable<T>) obs).get());
		}
		return new ObservableAsFlux<>(obs);

	}


	private static class ObservableAsFlux<T> extends Flux<T> {
        private final Observable<T> obs;

        private ObservableAsFlux(Observable<T> obs) {
            this.obs = obs;
        }

        @Override
        public void subscribe(final Subscriber<? super T> s) {
        	try {
        		obs.subscribe(new RxSubscriberToRS<>(s));
        	}
        	catch (Throwable t) {
        		EmptySubscription.error(s, t);
        	}
        }
    }

    private static class PublisherAsObservable<T> implements Observable.OnSubscribe<T> {
        private final Publisher<T> pub;

        private PublisherAsObservable(Publisher<T> pub) {
            this.pub = pub;
        }

        @Override
        public void call(final rx.Subscriber<? super T> subscriber) {
        	try {
        		pub.subscribe(new SubscriberToRx<>(subscriber));
        	}
        	catch (Throwable t) {
        		Exceptions.throwIfFatal(t);
        		subscriber.onError(t);
        	}
        }
    }

    private static class SubscriberToRx<T> implements Subscriber<T>, Producer, Subscription, rx.Subscription {

		private final    rx.Subscriber<? super T> subscriber;
		private volatile int                           terminated;
		private volatile Subscription                  subscription;

		private static final AtomicIntegerFieldUpdater<SubscriberToRx> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(SubscriberToRx.class, "terminated");

		public SubscriberToRx(rx.Subscriber<? super T> subscriber) {
			this.subscriber = subscriber;
			terminated = 0;
		}

		@Override
		public void request(long n) {
			if (n == 0 || isUnsubscribed()) {
				return; // ignore in RxJava
			}
			if (n <= 0L) {
				subscriber.onError(Exceptions.nullOrNegativeRequestException(n));
				return;
			}

			Subscription subscription = this.subscription;
			if (subscription != null) {
				subscription.request(n);
			}
		}

		@Override
		public boolean isUnsubscribed() {
			return terminated == 1;
		}

		@Override
		public void unsubscribe() {
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				Subscription subscription = this.subscription;
				if (subscription != null) {
					this.subscription = null;
					subscription.cancel();
				}
			}
		}

		@Override
		public void cancel() {
			unsubscribe();
		}

		@Override
		public void onSubscribe(final Subscription s) {
			if (BackpressureUtils.validate(subscription, s)) {
				this.subscription = s;
				subscriber.add(this);
				subscriber.onStart();
				subscriber.setProducer(this);
			}
		}

		@Override
		public void onNext(T o) {
			subscriber.onNext(o);
		}

		@Override
		public void onError(Throwable t) {
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				subscription = null;
				subscriber.onError(t);
			}
		}

		@Override
		public void onComplete() {
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				subscription = null;
				subscriber.onCompleted();
			}
		}
	}

	private static class RxSubscriberToRS<T> extends rx.Subscriber<T> {

		private final Subscriber<? super T> s;

		public RxSubscriberToRS(Subscriber<? super T> s) {
			this.s = s;
			request(0L);
		}

		void doRequest(long n) {
			request(n);
		}

		@Override
		public void onStart() {
			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					if (BackpressureUtils.checkRequest(n, s)) {
						doRequest(n);
					}
				}

				@Override
				public void cancel() {
					unsubscribe();
				}
			});
		}

		@Override
		public void onCompleted() {
			s.onComplete();
		}

		@Override
		public void onError(Throwable e) {
			if (e == null) {
				throw Exceptions.argumentIsNullException();
			}
			s.onError(e);
		}

		@Override
		public void onNext(T o) {
			if (o == null) {
				throw Exceptions.argumentIsNullException();
			}
			s.onNext(o);
		}
	}
}
