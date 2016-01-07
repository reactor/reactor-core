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

package reactor.core.publisher.convert;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.core.error.Exceptions;
import reactor.core.publisher.FluxJust;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;
import rx.Observable;
import rx.Producer;
import rx.internal.util.ScalarSynchronousObservable;

/**
 * @author Stephane Maldini
 */
public class RxJava1Converter extends PublisherConverter<Observable> {

	static final RxJava1Converter INSTANCE = new RxJava1Converter();

	@SuppressWarnings("unchecked")
	static public <T> Observable<T> from(Publisher<T> o) {
		return INSTANCE.fromPublisher(o);
	}

	@SuppressWarnings("unchecked")
	static public <T> Flux<T> from(Observable<T> o) {
		return INSTANCE.toPublisher(o);
	}

	@Override
	public Observable fromPublisher(final Publisher<?> pub) {
		return Observable.create(new Observable.OnSubscribe<Object>() {
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
		});
	}

	@Override
	@SuppressWarnings("unchecked")
	public Flux toPublisher(Object o) {
		final Observable<Object> obs = (Observable<Object>) o;
		if (ScalarSynchronousObservable.class.isAssignableFrom(obs.getClass())) {
			return new FluxJust<>(((ScalarSynchronousObservable) obs).get());
		}
		return new Flux<Object>() {
			@Override
			public void subscribe(final Subscriber<? super Object> s) {
				try {
					obs.subscribe(new RxSubscriberToRS(s));
				}
				catch (Throwable t) {
					EmptySubscription.error(s, t);
				}
			}
		};
	}

	@Override
	public Class<Observable> get() {
		return Observable.class;
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
			try {
				BackpressureUtils.checkRequest(n);
			}
			catch (Exceptions.Spec309_NullOrNegativeRequest c) {
				subscriber.onError(c);
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
		}

		void doRequest(long n) {
			request(n);
		}

		@Override
		public void onStart() {
			request(0L);
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
				throw Exceptions.spec_2_13_exception();
			}
			s.onError(e);
		}

		@Override
		public void onNext(Object o) {
			if (o == null) {
				throw Exceptions.spec_2_13_exception();
			}
			s.onNext(o);
		}
	}
}
