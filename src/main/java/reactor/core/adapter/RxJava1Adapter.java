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

package reactor.core.adapter;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.subscriber.SubscriptionHelper;
import reactor.core.util.Exceptions;
import rx.Observable;
import rx.Producer;
import rx.internal.util.ScalarSynchronousObservable;

/**
 * Convert a RxJava 1 {@link rx.Observable}. {@link rx.Single} and {@link rx.Completable}
 * to/from a Reactive
 * Streams
 * {@link Publisher}.
 *
 * @author Stephane Maldini
 * @since 2.5
 */
@SuppressWarnings("rawtypes")
public enum RxJava1Adapter {
	;

	/**
	 * @param completable
	 *
	 * @return a new Mono instance
	 */
	public static Mono<Void> completableToMono(rx.Completable completable) {
		return new CompletableAsMono(completable);
	}

	/**
	 *
	 * @param obs
	 * @param <T>
	 * @return a new Flux instance
	 */
	public static <T> Flux<T> observableToFlux(Observable<T> obs) {
		if (obs == Observable.empty()) {
			return Flux.empty();
		}
		if (obs instanceof ScalarSynchronousObservable) {
			return Flux.just(((ScalarSynchronousObservable<T>) obs).get());
		}
		return new ObservableAsFlux<>(obs);

	}

	/**
	 *
	 * @param publisher
	 * @param <T>
	 * @return a new Observable instance
	 */
	@SuppressWarnings("unchecked")
	public static <T> Observable<T> publisherToObservable(final Publisher<T> publisher) {
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

	/**
	 *
	 * @param publisher
	 * @param <T>
	 * @return a new Single instance
	 */
	@SuppressWarnings("unchecked")
	public static <T> rx.Single<T> publisherToSingle(Publisher<T> publisher) {
		if (publisher instanceof Fuseable.ScalarCallable) {
			Fuseable.ScalarCallable<T> scalarCallable =
					(Fuseable.ScalarCallable<T>) publisher;
			T v = scalarCallable.call();
			if (v == null) {
				return rx.Single.error(new NoSuchElementException(
						"Can't convert an empty Publisher to rx.Single"));
			}
			return rx.Single.just(v);
		}
		return rx.Single.create(new PublisherAsSingle<>(publisher));
	}

	/**
	 * @param source
	 *
	 * @return a new Completable instance
	 */
	public static rx.Completable publisherToCompletable(Publisher<?> source) {
		return rx.Completable.create(new PublisherAsCompletable(source));
	}

	/**
	 * @param single
	 * @param <T>
	 *
	 * @return a new Mono instance
	 */
	public static <T> Mono<T> singleToMono(rx.Single<T> single) {
		return new SingleAsMono<>(single);
	}

	/**
	 * Wraps an rx.Completable and exposes it as a Mono&lt;Void>.
	 */
	private static class CompletableAsMono extends Mono<Void> {

		final rx.Completable source;

		public CompletableAsMono(rx.Completable source) {
			this.source = Objects.requireNonNull(source, "source");
		}

		@Override
		public void subscribe(Subscriber<? super Void> s) {
			source.subscribe(new CompletableMonoSubscriber(s));
		}

		static final class CompletableMonoSubscriber
				implements rx.Completable.CompletableSubscriber, Subscription {

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
	 * Wraps a Publisher and exposes it as a CompletableOnSubscribe and ignores the onNext
	 * signals of the Publisher.
	 */
	private static class PublisherAsCompletable
			implements rx.Completable.CompletableOnSubscribe {

		final Publisher<?> source;

		public PublisherAsCompletable(Publisher<?> source) {
			this.source = Objects.requireNonNull(source, "source");
		}

		@Override
		public void call(rx.Completable.CompletableSubscriber t) {
			source.subscribe(new PublisherCompletableSubscriber(t));
		}

		static final class PublisherCompletableSubscriber
				implements Subscriber<Object>, rx.Subscription {

			final rx.Completable.CompletableSubscriber actual;

			Subscription s;

			volatile boolean unsubscribed;

			public PublisherCompletableSubscriber(rx.Completable.CompletableSubscriber actual) {
				this.actual = actual;
			}

			@Override
			public void onSubscribe(Subscription s) {
				if (SubscriptionHelper.validate(this.s, s)) {
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

	/**
	 * Wraps a Publisher and exposes it as a rx.rx.Single, firing NoSuchElementException
	 * for empty Publisher and IndexOutOfBoundsException if it has more than one element.
	 *
	 * @param <T> the value type
	 */
	private static class PublisherAsSingle<T> implements rx.Single.OnSubscribe<T> {

		final Publisher<? extends T> source;

		public PublisherAsSingle(Publisher<? extends T> source) {
			this.source = source;
		}

		@Override
		public void call(rx.SingleSubscriber<? super T> t) {
			source.subscribe(new PublisherAsSingleSubscriber<>(t));
		}

		static final class PublisherAsSingleSubscriber<T>
				implements Subscriber<T>, rx.Subscription {

			final rx.SingleSubscriber<? super T> actual;

			Subscription s;

			boolean done;

			boolean hasValue;

			T value;

			volatile boolean terminated;

			public PublisherAsSingleSubscriber(rx.SingleSubscriber<? super T> actual) {
				this.actual = actual;
			}

			@Override
			public void onSubscribe(Subscription s) {
				if (SubscriptionHelper.validate(this.s, s)) {
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

					actual.onError(new IndexOutOfBoundsException(
							"The wrapped Publisher produced more than one value"));

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
				}
				else {
					actual.onError(new NoSuchElementException(
							"The wrapped Publisher didn't produce any value"));
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
	 * Wraps an rx.rx.Single and exposes it as a Mono&lt;T>, signalling
	 * NullPointerException if the rx.Single produces a null value.
	 *
	 * @param <T> the value type
	 */
	private static class SingleAsMono<T> extends Mono<T> {

		final rx.Single<? extends T> source;

		public SingleAsMono(rx.Single<? extends T> source) {
			this.source = source;
		}

		@Override
		public void subscribe(Subscriber<? super T> s) {
			SingleAsMonoSubscriber<T> parent = new SingleAsMonoSubscriber<>(s);
			s.onSubscribe(parent);
			source.subscribe(parent);
		}

		static final class SingleAsMonoSubscriber<T> extends rx.SingleSubscriber<T>
				implements Subscription {

			final Subscriber<? super T> actual;

			T value;

			volatile int state;
			static final AtomicIntegerFieldUpdater<SingleAsMonoSubscriber> STATE =
					AtomicIntegerFieldUpdater.newUpdater(SingleAsMonoSubscriber.class,
							"state");

			static final int NO_REQUEST_NO_VALUE   = 0;
			static final int NO_REQUEST_HAS_VALUE  = 1;
			static final int HAS_REQUEST_NO_VALUE  = 2;
			static final int HAS_REQUEST_HAS_VALUE = 3;

			public SingleAsMonoSubscriber(Subscriber<? super T> actual) {
				this.actual = actual;
			}

			@Override
			public void onSuccess(T value) {
				if (value == null) {
					actual.onError(new NullPointerException(
							"The wrapped rx.Single produced a null value"));
					return;
				}
				for (; ; ) {
					int s = state;
					if (s == HAS_REQUEST_HAS_VALUE || s == NO_REQUEST_HAS_VALUE || isUnsubscribed()) {
						break;
					}
					else if (s == HAS_REQUEST_NO_VALUE) {
						if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
							actual.onNext(value);
							if (!isUnsubscribed()) {
								actual.onComplete();
							}
						}
						break;
					}
					else {
						this.value = value;
						if (STATE.compareAndSet(this, NO_REQUEST_NO_VALUE, NO_REQUEST_HAS_VALUE)) {
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
				if (SubscriptionHelper.validate(n)) {
					for (; ; ) {
						int s = state;
						if (s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE || isUnsubscribed()) {
							break;
						}
						else if (s == NO_REQUEST_HAS_VALUE) {
							if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
								T v = value;
								value = null;
								actual.onNext(v);
								if (!isUnsubscribed()) {
									actual.onComplete();
								}
							}
							break;
						}
						else {
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
		        SubscriptionHelper.error(s, t);
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
			if (SubscriptionHelper.validate(subscription, s)) {
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
					if (SubscriptionHelper.checkRequest(n, s)) {
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
