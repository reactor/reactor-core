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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.Reactor;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.util.Exceptions;

/**
 * An helper to support "Operator" writing, handle noop subscriptions, validate request
 * size and
 * to cap concurrent additive operations to Long.MAX_VALUE,
 * which is generic to {@link Subscription#request(long)} handling.
 *
 * Combine utils available to operator implementations, @see http://github.com/reactor/reactive-streams-commons
 *
 */
public abstract class Operators {

	/**
	 * Concurrent addition bound to Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param current current atomic to update
	 * @param toAdd   delta to add
	 * @return Addition result or Long.MAX_VALUE
	 */
	public static long addAndGet(AtomicLong current, long toAdd) {
		long u, r;
		do {
			r = current.get();
			if (r == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			u = addCap(r, toAdd);
		}
		while (!current.compareAndSet(r, u));

		return u;
	}

	/**
	 * Concurrent addition bound to Long.MAX_VALUE. Any concurrent write will "happen"
	 * before this operation.
	 *
	 * @param <T> the parent instance type
	 * @param updater current field updater
	 * @param instance current instance to update
	 * @param n delta to add
	 *
	 * @return Addition result or Long.MAX_VALUE
	 */
	public static <T> long addAndGet(AtomicLongFieldUpdater<T> updater,
			T instance,
			long n) {
		for (; ; ) {
			long r = updater.get(instance);
			if (r == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			long u = addCap(r, n);
			if (updater.compareAndSet(instance, r, u)) {
				return r;
			}
		}
	}

	/**
	 * Cap an addition to Long.MAX_VALUE
	 *
	 * @param a left operand
	 * @param b right operand
	 *
	 * @return Addition result or Long.MAX_VALUE if overflow
	 */
	public static long addCap(long a, long b) {
		long res = a + b;
		if (res < 0L) {
			return Long.MAX_VALUE;
		}
		return res;
	}

	/**
	 * Returns the subscription as QueueSubscription if possible or null.
	 * @param <T> the value type of the QueueSubscription.
	 * @param s the source subscription to try to convert.
	 * @return the QueueSubscription instance or null
	 */
	@SuppressWarnings("unchecked")
	public static <T> Fuseable.QueueSubscription<T> as(Subscription s) {
		if (s instanceof Fuseable.QueueSubscription) {
			return (Fuseable.QueueSubscription<T>) s;
		}
		return null;
	}

	/**
	 * A singleton Subscription that represents a cancelled subscription instance and
	 * should not be leaked to clients as it represents a terminal state. <br> If
	 * algorithms need to hand out a subscription, replace this with a singleton
	 * subscription because
	 * there is
	 * no standard way to tell if a
	 * Subscription is cancelled or not otherwise.
	 *
	 * @return a singleton noop {@link Subscription}
	 */
	public static Subscription cancelledSubscription() {
		return CancelledSubscription.INSTANCE;
	}

	/**
	 * Throws an exception if request is 0 or negative as specified in rule 3.09 of Reactive Streams
	 *
	 * @param n demand to check
	 * @throws IllegalArgumentException
	 */
	public static void checkRequest(long n) throws IllegalArgumentException {
		if (n <= 0L) {
			throw Exceptions.nullOrNegativeRequestException(n);
		}
	}

	/**
	 * Throws an exception if request is 0 or negative as specified in rule 3.09 of Reactive Streams
	 *
	 * @param n          demand to check
	 * @param subscriber Subscriber to onError if non strict positive n
	 *
	 * @return true if valid or false if specification exception occured
	 *
	 * @throws IllegalArgumentException if subscriber is null and demand is negative or 0.
	 */
	public static boolean checkRequest(long n, Subscriber<?> subscriber) {
		if (n <= 0L) {
			if (null != subscriber) {
				subscriber.onError(Exceptions.nullOrNegativeRequestException(n));
			} else {
				throw Exceptions.nullOrNegativeRequestException(n);
			}
			return false;
		}
		return true;
	}

	/**
	 * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onComplete.
	 *
	 * @param s
	 */
	public static void complete(Subscriber<?> s) {
		s.onSubscribe(EmptySubscription.INSTANCE);
		s.onComplete();
	}

	/**
	 * A singleton enumeration that represents a no-op Subscription instance that
	 * can be freely given out to clients.
	 * <p>
	 * The enum also implements Fuseable.QueueSubscription so operators expecting a
	 * QueueSubscription from a Fuseable source don't have to double-check their Subscription
	 * received in onSubscribe.
	 *
	 * @return a singleton noop {@link Subscription}
	 */
	public static Subscription emptySubscription() {
		return EmptySubscription.INSTANCE;
	}

	/**
	 * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onError with the
	 * supplied error.
	 *
	 * @param s target Subscriber to error
	 * @param e the actual error
	 */
	public static void error(Subscriber<?> s, Throwable e) {
		s.onSubscribe(EmptySubscription.INSTANCE);
		s.onError(e);
	}

	/**
	 * Concurrent addition bound to Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
     * @param <T> the parent instance type
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param toAdd    delta to add
	 * @return value before addition or Long.MAX_VALUE
	 */
	public static <T> long getAndAddCap(AtomicLongFieldUpdater<T> updater, T instance, long toAdd) {
		long r, u;
		do {
			r = updater.get(instance);
			if (r == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			u = addCap(r, toAdd);
		} while (!updater.compareAndSet(instance, r, u));

		return r;
	}

	/**
	 * Concurrent substraction bound to 0.
	 * Any concurrent write will "happen" before this operation.
	 *
     * @param <T> the parent instance type
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param toSub    delta to sub
	 * @return value before subscription or zero
	 */
	public static <T> long getAndSub(AtomicLongFieldUpdater<T> updater, T instance, long toSub) {
		long r, u;
		do {
			r = updater.get(instance);
			if (r == 0 || r == Long.MAX_VALUE) {
				return r;
			}
			u = subOrZero(r, toSub);
		} while (!updater.compareAndSet(instance, r, u));

		return r;
	}

	/**
	 * Concurrent substraction bound to 0 and Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param sequence current atomic to update
	 * @param toSub    delta to sub
	 * @return value before subscription, 0 or Long.MAX_VALUE
	 */
	public static long getAndSub(AtomicLong sequence, long toSub) {
		long r, u;
		do {
			r = sequence.get();
			if (r == 0 || r == Long.MAX_VALUE) {
				return r;
			}
			u = subOrZero(r, toSub);
		} while (!sequence.compareAndSet(r, u));

		return r;
	}

	/**
	 * Cap a multiplication to Long.MAX_VALUE
	 *
	 * @param a left operand
	 * @param b right operand
	 *
	 * @return Product result or Long.MAX_VALUE if overflow
	 */
	public static long multiplyCap(long a, long b) {
		long u = a * b;
		if (((a | b) >>> 31) != 0) {
			if (u / a != b) {
				return Long.MAX_VALUE;
			}
		}
		return u;
	}

	/**
	 * A generic utility to atomically replace a subscription or cancel if marked by a
	 * singleton subscription.
	 *
	 * @param field The Atomic container
	 * @param instance the instance reference
	 * @param s the subscription
	 * @param <F> the instance type
	 *
	 * @return true if replaced
	 */
	public static <F> boolean replace(AtomicReferenceFieldUpdater<F, Subscription> field,
			F instance,
			Subscription s) {
		for (; ; ) {
			Subscription a = field.get(instance);
			if (a == CancelledSubscription.INSTANCE) {
				s.cancel();
				return false;
			}
			if (field.compareAndSet(instance, a, s)) {
				return true;
			}
		}
	}

	/**
	 * Throw {@link IllegalArgumentException}
	 *
	 * @param n the demand to evaluate
	 */
	public static void reportBadRequest(long n) {
		throw Exceptions.nullOrNegativeRequestException(n);
	}

	/**
	 * Throw {@code Exceptions.InsufficientCapacityException}
	 */
	public static void reportMoreProduced() {
		throw Exceptions.failWithOverflow();
	}

	/**
	 * Log reportedSubscriptions
	 */
	public static void reportSubscriptionSet() {
		if (log.isDebugEnabled()) {
			log.debug("Duplicate Subscription has been detected",
					Exceptions.duplicateOnSubscribeException());
		}
	}

	/**
	 * A generic utility to atomically replace a subscription or cancel if marked by a
	 * singleton subscription or concurrently set before.
	 *
	 * @param field The Atomic container
	 * @param instance the instance reference
	 * @param s the subscription
	 * @param <F> the instance type
	 *
	 * @return true if replaced
	 */
	public static <F> boolean set(AtomicReferenceFieldUpdater<F, Subscription> field,
			F instance,
			Subscription s) {
		for (; ; ) {
			Subscription a = field.get(instance);
			if (a == CancelledSubscription.INSTANCE) {
				s.cancel();
				return false;
			}
			if (field.compareAndSet(instance, a, s)) {
				if (a != null) {
					a.cancel();
				}
				return true;
			}
		}
	}

	/**
	 * Sets the given subscription once and returns true if successful, false
	 * if the field has a subscription already or has been cancelled.
	 * @param <F> the instance type containing the field
	 * @param field the field accessor
	 * @param instance the parent instance
	 * @param s the subscription to set once
	 * @return true if successful, false if the target was not empty or has been cancelled
	 */
	public static <F> boolean setOnce(AtomicReferenceFieldUpdater<F, Subscription> field, F instance, Subscription s) {
		Subscription a = field.get(instance);
		if (a == CancelledSubscription.INSTANCE) {
			return false;
		}
		if (a != null) {
			reportSubscriptionSet();
			return false;
		}

		if (field.compareAndSet(instance, null, s)) {
			return true;
		}

		a = field.get(instance);

		if (a == CancelledSubscription.INSTANCE) {
			return false;
		}

		reportSubscriptionSet();
		return false;
	}

	/**
	 * Cap a substraction to 0
	 *
	 * @param a left operand
	 * @param b right operand
	 * @return Subscription result or 0 if overflow
	 */
	public static long subOrZero(long a, long b) {
		long res = a - b;
		if (res < 0L) {
			return 0;
		}
		return res;
	}

	public static <F> boolean terminate(AtomicReferenceFieldUpdater<F, Subscription> field,
			F instance) {
		Subscription a = field.get(instance);
		if (a != CancelledSubscription.INSTANCE) {
			a = field.getAndSet(instance, CancelledSubscription.INSTANCE);
			if (a != null && a != CancelledSubscription.INSTANCE) {
				a.cancel();
				return true;
			}
		}
		return false;
	}

	/**
	 * Check Subscription current state and cancel new Subscription if different null, returning true if
	 * ready to subscribe.
	 *
	 * @param current current Subscription, expected to be null
	 * @param next new Subscription
	 * @return true if Subscription can be used
	 */
	public static boolean validate(Subscription current, Subscription next) {
		Objects.requireNonNull(next, "Subscription cannot be null");
		if (current != null) {
			next.cancel();
			//reportSubscriptionSet();
			return false;
		}

		return true;
	}

	/**
	 * Evaluate if a request is strictly positive otherwise {@link #reportBadRequest(long)}
	 * @param n the request value
	 * @return true if valid
	 */
	public static boolean validate(long n) {
		if (n < 0) {
			reportBadRequest(n);
			return false;
		}
		return true;
	}

	//
	enum CancelledSubscription implements Subscription, Trackable {
		INSTANCE;

		@Override
		public void cancel() {
			// deliberately no op
		}

		@Override
		public boolean isCancelled() {
			return true;
		}

		@Override
		public void request(long n) {
			// deliberately no op
		}

	}

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
	 * Safely gate a {@link Subscriber} by a serializing {@link Subscriber}.
	 * Serialization uses thread-stealing and a potentially unbounded queue that might starve a calling thread if
	 * races are too important and
	 * {@link Subscriber} is slower.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/serialize.png" alt="">
	 *
	 * @param <T> the relayed type
	 * @param subscriber the subscriber to wrap
	 * @return a serializing {@link Subscriber}
	 */
	public static <T> Subscriber<T> serialize(Subscriber<? super T> subscriber) {
		return new SerializedSubscriber<>(subscriber);
	}

	enum EmptySubscription implements Fuseable.QueueSubscription<Object> {
		INSTANCE;

		@Override
		public void cancel() {
			// deliberately no op
		}

		@Override
		public void clear() {
			// deliberately no op
		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public Object poll() {
			return null;
		}

		@Override
		public void request(long n) {
			// deliberately no op
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE; // can't enable fusion due to complete/error possibility
		}

		@Override
		public int size() {
			return 0;
		}
	}

	/**
	 * Base class for Subscribers that will receive their Subscriptions at any time yet
	 * they need to be cancelled or requested at any time.
	 */
	static class DeferredSubscription
			implements Subscription, Receiver, Trackable {

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
			if (a == cancelledSubscription()) {
				s.cancel();
				return false;
			}
			if (a != null) {
				s.cancel();
				reportSubscriptionSet();
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

			if (a != cancelledSubscription()) {
				s.cancel();
				return false;
			}

			reportSubscriptionSet();
			return false;
		}

		@Override
		public void request(long n) {
			Subscription a = s;
			if (a != null) {
				a.request(n);
			} else {
				addAndGet(REQUESTED, this, n);

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
			if (a != cancelledSubscription()) {
				a = S.getAndSet(this, cancelledSubscription());
				if (a != null && a != cancelledSubscription()) {
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
			return s == cancelledSubscription();
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
	                                                       Trackable,
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
			if (validate(n)) {
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
			implements Subscription, Subscriber<I>, Producer, Trackable,
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
			if (validate(n)) {
				if (unbounded) {
					return;
				}
				if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
					long r = requested;

					if (r != Long.MAX_VALUE) {
						r = addCap(r, n);
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

				getAndAddCap(MISSED_REQUESTED, this, n);

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
						reportMoreProduced();
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

			getAndAddCap(MISSED_PRODUCED, this, 1L);

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
						reportMoreProduced();
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

			getAndAddCap(MISSED_PRODUCED, this, n);

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
						long u = addCap(r, mr);

						if (u != Long.MAX_VALUE) {
							long v = u - mp;
							if (v < 0L) {
								reportMoreProduced();
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
			if (validate(n)) {
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

	final static Reactor.Logger log = Reactor.getLogger(Operators.class);
}
