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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.subscriber.Subscribers;
import reactor.core.subscriber.SubscriptionHelper;
import reactor.core.util.Exceptions;

/**
 * Combines values from a main Publisher with values from another
 * Publisher through a bi-function and emits the result.
 * <p>
 * <p>
 * The operator will drop values from the main source until the other
 * Publisher produces any value.
 * <p>
 * If the other Publisher completes without any value, the sequence is completed.
 *
 * @param <T> the main source type
 * @param <U> the alternate source type
 * @param <R> the output type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @since 2.5
 */
final class FluxWithLatestFrom<T, U, R> extends FluxSource<T, R> {
	final Publisher<? extends U> other;

	final BiFunction<? super T, ? super U, ? extends R> combiner;

	public FluxWithLatestFrom(Publisher<? extends T> source, Publisher<? extends U> other,
								   BiFunction<? super T, ? super U, ? extends R> combiner) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.combiner = Objects.requireNonNull(combiner, "combiner");
	}

	@Override
	public long getCapacity() {
		return -1L;
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		Subscriber<R> serial = Subscribers.serialize(s);

		WithLatestFromSubscriber<T, U, R> main = new WithLatestFromSubscriber<>(serial, combiner);

		WithLatestFromOtherSubscriber<U> secondary = new WithLatestFromOtherSubscriber<>(main);

		other.subscribe(secondary);

		source.subscribe(main);
	}

	static final class WithLatestFromSubscriber<T, U, R>
	  implements Subscriber<T>, Subscription {
		final Subscriber<? super R> actual;

		final BiFunction<? super T, ? super U, ? extends R> combiner;

		volatile Subscription main;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WithLatestFromSubscriber, Subscription> MAIN =
		  AtomicReferenceFieldUpdater.newUpdater(WithLatestFromSubscriber.class, Subscription.class, "main");

		volatile Subscription other;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<WithLatestFromSubscriber, Subscription> OTHER =
		  AtomicReferenceFieldUpdater.newUpdater(WithLatestFromSubscriber.class, Subscription.class, "other");

		volatile U otherValue;

		public WithLatestFromSubscriber(Subscriber<? super R> actual,
												 BiFunction<? super T, ? super U, ? extends R> combiner) {
			this.actual = actual;
			this.combiner = combiner;
		}

		void setOther(Subscription s) {
			if (!OTHER.compareAndSet(this, null, s)) {
				s.cancel();
				if (other != CancelledSubscription.INSTANCE) {
					SubscriptionHelper.reportSubscriptionSet();
				}
			}
		}

		@Override
		public void request(long n) {
			main.request(n);
		}

		void cancelMain() {
			Subscription s = main;
			if (s != CancelledSubscription.INSTANCE) {
				s = MAIN.getAndSet(this, CancelledSubscription.INSTANCE);
				if (s != null && s != CancelledSubscription.INSTANCE) {
					s.cancel();
				}
			}
		}

		void cancelOther() {
			Subscription s = other;
			if (s != CancelledSubscription.INSTANCE) {
				s = OTHER.getAndSet(this, CancelledSubscription.INSTANCE);
				if (s != null && s != CancelledSubscription.INSTANCE) {
					s.cancel();
				}
			}
		}

		@Override
		public void cancel() {
			cancelMain();
			cancelOther();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!MAIN.compareAndSet(this, null, s)) {
				s.cancel();
				if (main != CancelledSubscription.INSTANCE) {
					SubscriptionHelper.reportSubscriptionSet();
				}
			} else {
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			U u = otherValue;

			if (u != null) {
				R r;

				try {
					r = combiner.apply(t, u);
				} catch (Throwable e) {
					Exceptions.throwIfFatal(e);
					onError(Exceptions.unwrap(e));
					return;
				}

				if (r == null) {
					onError(new NullPointerException("The combiner returned a null value"));
					return;
				}

				actual.onNext(r);
			} else {
				main.request(1);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (main == null) {
				if (MAIN.compareAndSet(this, null, CancelledSubscription.INSTANCE)) {
					cancelOther();

					SubscriptionHelper.error(actual, t);
					return;
				}
			}
			cancelOther();

			otherValue = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			cancelOther();

			otherValue = null;
			actual.onComplete();
		}

		void otherError(Throwable t) {
			if (main == null) {
				if (MAIN.compareAndSet(this, null, CancelledSubscription.INSTANCE)) {
					cancelMain();

					SubscriptionHelper.error(actual, t);
					return;
				}
			}
			cancelMain();
			
			otherValue = null;
			actual.onError(t);
		}
		
		void otherComplete() {
			if (otherValue == null) {
				if (main == null) {
					if (MAIN.compareAndSet(this, null, CancelledSubscription.INSTANCE)) {
						cancelMain();

						EmptySubscription.complete(actual);
						return;
					}
				}
				cancelMain();
				
				actual.onComplete();
			}
		}
	}

	static final class WithLatestFromOtherSubscriber<U> implements Subscriber<U> {
		final WithLatestFromSubscriber<?, U, ?> main;

		public WithLatestFromOtherSubscriber(WithLatestFromSubscriber<?, U, ?> main) {
			this.main = main;
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.setOther(s);

			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(U t) {
			main.otherValue = t;
		}

		@Override
		public void onError(Throwable t) {
			main.otherError(t);
		}

		@Override
		public void onComplete() {
			main.otherComplete();
		}
	}
}
