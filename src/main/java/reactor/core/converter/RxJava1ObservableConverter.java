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
 * @since 2.5
 */
public final class RxJava1ObservableConverter extends PublisherConverter<Observable> {

	static final RxJava1ObservableConverter INSTANCE = new RxJava1ObservableConverter();

	@SuppressWarnings("unchecked")
	static public <T> Observable<T> from(Publisher<T> o) {
		return INSTANCE.fromPublisher(o);
	}

	@SuppressWarnings("unchecked")
	static public <T> Flux<T> from(Observable<T> o) {
		return INSTANCE.toPublisher(o);
	}

	@Override
    @SuppressWarnings("rawtypes")
	public Observable fromPublisher(final Publisher<?> pub) {
	    if (pub instanceof Fuseable.ScalarCallable) {
            Fuseable.ScalarCallable scalarCallable = (Fuseable.ScalarCallable) pub;

            Object v = scalarCallable.call();
            if (v == null) {
                return Observable.empty();
            }
            return Observable.just(v);
	    }
		return Observable.create(new PublisherAsObservable(pub));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Flux toPublisher(Object o) {
		if (o instanceof Observable) {
			final Observable<Object> obs = (Observable<Object>) o;
			if (obs == Observable.empty()) {
			    return Flux.empty();
			}
			if (obs instanceof ScalarSynchronousObservable) {
				return Flux.just(((ScalarSynchronousObservable) obs).get());
			}
			return new ObservableAsFlux(obs);
		}
		return null;
	}

	@Override
	public Class<Observable> get() {
		return Observable.class;
	}

	private final class ObservableAsFlux extends Flux<Object> {
        private final Observable<Object> obs;

        private ObservableAsFlux(Observable<Object> obs) {
            this.obs = obs;
        }

        @Override
        public void subscribe(final Subscriber<? super Object> s) {
        	try {
        		obs.subscribe(new RxSubscriberToRS(s));
        	}
        	catch (Throwable t) {
        		EmptySubscription.error(s, t);
        	}
        }
    }

    private final class PublisherAsObservable implements Observable.OnSubscribe<Object> {
        private final Publisher<?> pub;

        private PublisherAsObservable(Publisher<?> pub) {
            this.pub = pub;
        }

        @Override
        public void call(final rx.Subscriber<? super Object> subscriber) {
        	try {
        		pub.subscribe(new SubscriberToRx(subscriber));
        	}
        	catch (Throwable t) {
        		Exceptions.throwIfFatal(t);
        		subscriber.onError(t);
        	}
        }
    }

    private static class SubscriberToRx implements Subscriber<Object>, Producer, Subscription, rx.Subscription {

		private final    rx.Subscriber<? super Object> subscriber;
		private volatile int                           terminated;
		private volatile Subscription                  subscription;

		private static final AtomicIntegerFieldUpdater<SubscriberToRx> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(SubscriberToRx.class, "terminated");

		public SubscriberToRx(rx.Subscriber<? super Object> subscriber) {
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
		public void onNext(Object o) {
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

	private static class RxSubscriberToRS extends rx.Subscriber<Object> {

		private final Subscriber<? super Object> s;

		public RxSubscriberToRS(Subscriber<? super Object> s) {
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
		public void onNext(Object o) {
			if (o == null) {
				throw Exceptions.argumentIsNullException();
			}
			s.onNext(o);
		}
	}
}
