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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.subscriber.SubscriberState;
import reactor.core.subscriber.SubscriptionHelper;

abstract class Operators {

	/**
	 * Represents a fuseable Subscription that emits a single constant value synchronously
	 * to a Subscriber or consumer.
	 *
	 * @param subscriber the delegate {@link Subscriber} that will be requesting the value
	 * @param value the single value to be emitted
	 * @param <T> the value type
	 * @return a new scalar {@link Subscription}
	 */
	public static <T> Subscription scalarSubscription(Subscriber<? super T> subscriber,
			T value){
		return new ScalarSubscription<>(subscriber, value);
	}

	/**
	 * Base class for Subscribers that will receive their Subscriptions at any time yet
	 * they need to be cancelled or requested at any time.
	 */
	static class DeferredSubscription
			implements Subscription, Receiver, SubscriberState {

		volatile Subscription s;
		static final AtomicReferenceFieldUpdater<DeferredSubscription, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(DeferredSubscription.class, Subscription.class, "s");

		volatile long requested;
		static final AtomicLongFieldUpdater<DeferredSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(DeferredSubscription.class, "requested");

		/**
		 * Atomically sets the single subscription and requests the missed amount from it.
		 *
		 * @param s
		 * @return false if this arbiter is cancelled or there was a subscription already set
		 */
		public final boolean set(Subscription s) {
			Objects.requireNonNull(s, "s");
			Subscription a = this.s;
			if (a == SubscriptionHelper.cancelled()) {
				s.cancel();
				return false;
			}
			if (a != null) {
				s.cancel();
				SubscriptionHelper.reportSubscriptionSet();
				return false;
			}

			if (S.compareAndSet(this, null, s)) {

				long r = REQUESTED.getAndSet(this, 0L);

				if (r != 0L) {
					s.request(r);
				}

				return true;
			}

			a = this.s;

			if (a != SubscriptionHelper.cancelled()) {
				s.cancel();
				return false;
			}

			SubscriptionHelper.reportSubscriptionSet();
			return false;
		}

		@Override
		public void request(long n) {
			Subscription a = s;
			if (a != null) {
				a.request(n);
			} else {
				SubscriptionHelper.addAndGet(REQUESTED, this, n);

				a = s;

				if (a != null) {
					long r = REQUESTED.getAndSet(this, 0L);

					if (r != 0L) {
						a.request(r);
					}
				}
			}
		}

		@Override
		public void cancel() {
			Subscription a = s;
			if (a != SubscriptionHelper.cancelled()) {
				a = S.getAndSet(this, SubscriptionHelper.cancelled());
				if (a != null && a != SubscriptionHelper.cancelled()) {
					a.cancel();
				}
			}
		}

		/**
		 * Returns true if this arbiter has been cancelled.
		 *
		 * @return true if this arbiter has been cancelled
		 */
		@Override
		public final boolean isCancelled() {
			return s == SubscriptionHelper.cancelled();
		}

		@Override
		public final boolean isStarted() {
			return s != null;
		}

		@Override
		public final boolean isTerminated() {
			return isCancelled();
		}

		@Override
		public final long requestedFromDownstream() {
			return requested;
		}

		@Override
		public Subscription upstream() {
			return s;
		}

	}

	/**
	 * A Subscriber/Subscription barrier that holds a single value at most and properly gates asynchronous behaviors
	 * resulting from concurrent request or cancel and onXXX signals.
	 *
	 * @param <I> The upstream sequence type
	 * @param <O> The downstream sequence type
	 */
	static class DeferredScalarSubscriber<I, O> implements Subscriber<I>, Loopback,
	                                                       SubscriberState,
	                                                       Receiver, Producer,
	                                                       Fuseable.QueueSubscription<O> {

		static final int SDS_NO_REQUEST_NO_VALUE   = 0;
		static final int SDS_NO_REQUEST_HAS_VALUE  = 1;
		static final int SDS_HAS_REQUEST_NO_VALUE  = 2;
		static final int SDS_HAS_REQUEST_HAS_VALUE = 3;

		protected final Subscriber<? super O> subscriber;

		protected O value;

		volatile int state;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<DeferredScalarSubscriber> STATE =
				AtomicIntegerFieldUpdater.newUpdater(DeferredScalarSubscriber.class, "state");

		protected byte outputFused;

		static final byte OUTPUT_NO_VALUE = 1;
		static final byte OUTPUT_HAS_VALUE = 2;
		static final byte OUTPUT_COMPLETE = 3;

		public DeferredScalarSubscriber(Subscriber<? super O> subscriber) {
			this.subscriber = subscriber;
		}

		@Override
		public void request(long n) {
			if (SubscriptionHelper.validate(n)) {
				for (; ; ) {
					int s = state;
					if (s == SDS_HAS_REQUEST_NO_VALUE || s == SDS_HAS_REQUEST_HAS_VALUE) {
						return;
					}
					if (s == SDS_NO_REQUEST_HAS_VALUE) {
						if (STATE.compareAndSet(this, SDS_NO_REQUEST_HAS_VALUE, SDS_HAS_REQUEST_HAS_VALUE)) {
							Subscriber<? super O> a = downstream();
							a.onNext(value);
							a.onComplete();
						}
						return;
					}
					if (STATE.compareAndSet(this, SDS_NO_REQUEST_NO_VALUE, SDS_HAS_REQUEST_NO_VALUE)) {
						return;
					}
				}
			}
		}

		@Override
		public void cancel() {
			state = SDS_HAS_REQUEST_HAS_VALUE;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onNext(I t) {
			value = (O) t;
		}

		@Override
		public void onError(Throwable t) {
			subscriber.onError(t);
		}

		@Override
		public void onSubscribe(Subscription s) {
			//if upstream
		}

		@Override
		public void onComplete() {
			subscriber.onComplete();
		}

		@Override
		public final boolean isCancelled() {
			return state == SDS_HAS_REQUEST_HAS_VALUE;
		}

		@Override
		public final Subscriber<? super O> downstream() {
			return subscriber;
		}

		public void setValue(O value) {
			this.value = value;
		}

		/**
		 * Tries to emit the value and complete the underlying subscriber or
		 * stores the value away until there is a request for it.
		 * <p>
		 * Make sure this method is called at most once
		 * @param value the value to emit
		 */
		public final void complete(O value) {
			Objects.requireNonNull(value);
			for (; ; ) {
				int s = state;
				if (s == SDS_NO_REQUEST_HAS_VALUE || s == SDS_HAS_REQUEST_HAS_VALUE) {
					return;
				}
				if (s == SDS_HAS_REQUEST_NO_VALUE) {
					if (outputFused == OUTPUT_NO_VALUE) {
						setValue(value); // make sure poll sees it
						outputFused = OUTPUT_HAS_VALUE;
					}
					Subscriber<? super O> a = downstream();
					a.onNext(value);
					if (state != SDS_HAS_REQUEST_HAS_VALUE) {
						a.onComplete();
					}
					return;
				}
				setValue(value);
				if (STATE.compareAndSet(this, SDS_NO_REQUEST_NO_VALUE, SDS_NO_REQUEST_HAS_VALUE)) {
					return;
				}
			}
		}

		@Override
		public boolean isStarted() {
			return state != SDS_NO_REQUEST_NO_VALUE;
		}

		@Override
		public Object connectedOutput() {
			return value;
		}

		@Override
		public boolean isTerminated() {
			return isCancelled();
		}

		@Override
		public Object upstream() {
			return value;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.ASYNC) != 0) {
				outputFused = OUTPUT_NO_VALUE;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}

		@Override
		public O poll() {
			if (outputFused == OUTPUT_HAS_VALUE) {
				outputFused = OUTPUT_COMPLETE;
				return value;
			}
			return null;
		}

		@Override
		public boolean isEmpty() {
			return outputFused != OUTPUT_HAS_VALUE;
		}

		@Override
		public void clear() {
			outputFused = OUTPUT_COMPLETE;
			value = null;
		}

		@Override
		public int size() {
			return isEmpty() ? 0 : 1;
		}
	}

	/**
	 * Arbitrates the requests and cancellation for a Subscription that may be set onSubscribe once only.
	 * <p>
	 * Note that {@link #request(long)} doesn't validate the amount.
	 *
	 * @param <I> the input value type
	 * @param <O> the output value type
	 */
	static class DeferredSubscriptionSubscriber<I, O>
			extends DeferredSubscription
	implements Subscriber<I>, Producer {

		protected final Subscriber<? super O> subscriber;

		/**
		 * Constructs a SingleSubscriptionArbiter with zero initial request.
		 *
		 * @param subscriber the actual subscriber
		 */
		public DeferredSubscriptionSubscriber(Subscriber<? super O> subscriber) {
			this.subscriber = Objects.requireNonNull(subscriber, "subscriber");
		}

		@Override
		public final Subscriber<? super O> downstream() {
			return subscriber;
		}

		@Override
		public void onSubscribe(Subscription s) {
			set(s);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onNext(I t) {
			subscriber.onNext((O) t);
		}

		@Override
		public void onError(Throwable t) {
			subscriber.onError(t);
		}

		@Override
		public void onComplete() {
			subscriber.onComplete();
		}
	}

	/**
	 * A subscription implementation that arbitrates request amounts between subsequent Subscriptions, including the
	 * duration until the first Subscription is set.
	 * <p>
	 * The class is thread safe but switching Subscriptions should happen only when the source associated with the current
	 * Subscription has finished emitting values. Otherwise, two sources may emit for one request.
	 * <p>
	 * You should call {@link #produced(long)} or {@link #producedOne()} after each element has been delivered to properly
	 * account the outstanding request amount in case a Subscription switch happens.
	 *
	 * @param <I> the input value type
	 * @param <O> the output value type
	 */
	abstract static class MultiSubscriptionSubscriber<I, O>
			implements Subscription, Subscriber<I>, Producer,
			           SubscriberState,
			           Receiver {

		protected final Subscriber<? super O> subscriber;

		/**
		 * The current subscription which may null if no Subscriptions have been set.
		 */
		Subscription actual;

		/**
		 * The current outstanding request amount.
		 */
		long requested;

		volatile Subscription missedSubscription;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MultiSubscriptionSubscriber, Subscription>
				MISSED_SUBSCRIPTION =
		  AtomicReferenceFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class,
			Subscription.class,
			"missedSubscription");

		volatile long missedRequested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<MultiSubscriptionSubscriber>
				MISSED_REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "missedRequested");

		volatile long missedProduced;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<MultiSubscriptionSubscriber> MISSED_PRODUCED =
		  AtomicLongFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "missedProduced");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MultiSubscriptionSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "wip");

		volatile boolean cancelled;

		protected boolean unbounded;

		public MultiSubscriptionSubscriber(Subscriber<? super O> subscriber) {
			this.subscriber = subscriber;
		}

		@Override
		public void onSubscribe(Subscription s) {
			set(s);
		}

		@Override
		public void onError(Throwable t) {
			subscriber.onError(t);
		}

		@Override
		public void onComplete() {
			subscriber.onComplete();
		}

		public final void set(Subscription s) {
			if (cancelled) {
				s.cancel();
				return;
			}

			Objects.requireNonNull(s);

			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				Subscription a = actual;

				if (a != null && shouldCancelCurrent()) {
					a.cancel();
				}

				actual = s;

				long r = requested;
				if (r != 0L) {
					s.request(r);
				}

				if (WIP.decrementAndGet(this) == 0) {
					return;
				}

				drainLoop();

				return;
			}

			Subscription a = MISSED_SUBSCRIPTION.getAndSet(this, s);
			if (a != null) {
				a.cancel();
			}
			drain();
		}

		@Override
		public final void request(long n) {
			if (SubscriptionHelper.validate(n)) {
				if (unbounded) {
					return;
				}
				if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
					long r = requested;

					if (r != Long.MAX_VALUE) {
						r = SubscriptionHelper.addCap(r, n);
						requested = r;
						if (r == Long.MAX_VALUE) {
							unbounded = true;
						}
					}
					Subscription a = actual;
					if (a != null) {
						a.request(n);
					}

					if (WIP.decrementAndGet(this) == 0) {
						return;
					}

					drainLoop();

					return;
				}

				SubscriptionHelper.getAndAddCap(MISSED_REQUESTED, this, n);

				drain();
			}
		}

		public final void producedOne() {
			if (unbounded) {
				return;
			}
			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				long r = requested;

				if (r != Long.MAX_VALUE) {
					r--;
					if (r < 0L) {
						SubscriptionHelper.reportMoreProduced();
						r = 0;
					}
					requested = r;
				} else {
					unbounded = true;
				}

				if (WIP.decrementAndGet(this) == 0) {
					return;
				}

				drainLoop();

				return;
			}

			SubscriptionHelper.getAndAddCap(MISSED_PRODUCED, this, 1L);

			drain();
		}

		public final void produced(long n) {
			if (unbounded) {
				return;
			}
			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				long r = requested;

				if (r != Long.MAX_VALUE) {
					long u = r - n;
					if (u < 0L) {
						SubscriptionHelper.reportMoreProduced();
						u = 0;
					}
					requested = u;
				} else {
					unbounded = true;
				}

				if (WIP.decrementAndGet(this) == 0) {
					return;
				}

				drainLoop();

				return;
			}

			SubscriptionHelper.getAndAddCap(MISSED_PRODUCED, this, n);

			drain();
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				drain();
			}
		}

		@Override
		public final boolean isCancelled() {
			return cancelled;
		}

		final void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			drainLoop();
		}

		final void drainLoop() {
			int missed = 1;

			for (; ; ) {

				Subscription ms = missedSubscription;

				if (ms != null) {
					ms = MISSED_SUBSCRIPTION.getAndSet(this, null);
				}

				long mr = missedRequested;
				if (mr != 0L) {
					mr = MISSED_REQUESTED.getAndSet(this, 0L);
				}

				long mp = missedProduced;
				if (mp != 0L) {
					mp = MISSED_PRODUCED.getAndSet(this, 0L);
				}

				Subscription a = actual;

				if (cancelled) {
					if (a != null) {
						a.cancel();
						actual = null;
					}
					if (ms != null) {
						ms.cancel();
					}
				} else {
					long r = requested;
					if (r != Long.MAX_VALUE) {
						long u = SubscriptionHelper.addCap(r, mr);

						if (u != Long.MAX_VALUE) {
							long v = u - mp;
							if (v < 0L) {
								SubscriptionHelper.reportMoreProduced();
								v = 0;
							}
							r = v;
						} else {
							r = u;
						}
						requested = r;
					}

					if (ms != null) {
						if (a != null && shouldCancelCurrent()) {
							a.cancel();
						}
						actual = ms;
						if (r != 0L) {
							ms.request(r);
						}
					} else if (mr != 0L && a != null) {
						a.request(mr);
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					return;
				}
			}
		}

		@Override
		public final Subscriber<? super O> downstream() {
			return subscriber;
		}

		@Override
		public final Subscription upstream() {
			return actual != null ? actual : missedSubscription;
		}

		@Override
		public final long requestedFromDownstream() {
			return requested + missedRequested;
		}

		@Override
		public boolean isTerminated() {
			return false;
		}

		@Override
		public boolean isStarted() {
			return upstream() != null;
		}

		public final boolean isUnbounded() {
			return unbounded;
		}

		/**
		 * When setting a new subscription via set(), should
		 * the previous subscription be cancelled?
		 * @return true if cancellation is needed
		 */
		protected boolean shouldCancelCurrent() {
			return false;
		}
	}

	Operators(){}

	/**
	 * Represents a fuseable Subscription that emits a single constant value synchronously
	 * to a Subscriber or consumer.
	 *
	 * @param <T> the value type
	 */
	static final class ScalarSubscription<T>
			implements Fuseable.QueueSubscription<T>, Producer, Receiver {

		final Subscriber<? super T> actual;

		final T value;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ScalarSubscription> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(ScalarSubscription.class, "once");

		public ScalarSubscription(Subscriber<? super T> actual, T value) {
			this.value = Objects.requireNonNull(value, "value");
			this.actual = Objects.requireNonNull(actual, "actual");
		}

		@Override
		public final Subscriber<? super T> downstream() {
			return actual;
		}

		@Override
		public void request(long n) {
			if (SubscriptionHelper.validate(n)) {
				if (ONCE.compareAndSet(this, 0, 1)) {
					Subscriber<? super T> a = actual;
					a.onNext(value);
					a.onComplete();
				}
			}
		}

		@Override
		public void cancel() {
			ONCE.lazySet(this, 1);
		}

		@Override
		public Object upstream() {
			return value;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.SYNC) != 0) {
				return Fuseable.SYNC;
			}
			return 0;
		}

		@Override
		public T poll() {
			if (once == 0) {
				ONCE.lazySet(this, 1);
				return value;
			}
			return null;
		}

		@Override
		public boolean isEmpty() {
			return once != 0;
		}

		@Override
		public int size() {
			return isEmpty() ? 0 : 1;
		}

		@Override
		public void clear() {
			ONCE.lazySet(this, 1);
		}
	}
}
