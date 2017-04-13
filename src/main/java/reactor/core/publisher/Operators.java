/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.Logger;
import reactor.util.Loggers;


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
				return u;
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
	 * @throws IllegalArgumentException the nullOrNegativeRequestException instance
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
		if(subscriber == null){
			checkRequest(n);
			return true;
		}
		if (n <= 0L) {
			subscriber.onError(Exceptions.nullOrNegativeRequestException(n));
			return false;
		}
		return true;
	}

	/**
	 * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onComplete.
	 *
	 * @param s the target subscriber
	 */
	public static void complete(Subscriber<?> s) {
		s.onSubscribe(EmptySubscription.INSTANCE);
		s.onComplete();
	}

	/**
	 * Return a singleton {@link Subscriber} that does not check for double onSubscribe
	 * and purely request Long.MAX. If an error is received it will raise a
	 * {@link Exceptions#errorCallbackNotImplemented(Throwable)} in the receiving thread.
	 *
	 * @return a new {@link Subscriber} whose sole purpose is to request Long.MAX
	 */
	@SuppressWarnings("unchecked")
	public static <T> Subscriber<T> drainSubscriber() {
		return (Subscriber<T>)DrainSubscriber.INSTANCE;
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
	 * Take an unsignalled exception that is masking anowher one due to callback failure.
	 *
	 * @param e the exception to handle
	 * @param root the optional root cause to suppress
	 */
	public static void onErrorDropped(Throwable e, Throwable root) {
		if(root != null && root != e) {
			e.addSuppressed(root);
		}
		onErrorDropped(e);
	}

	/**
	 * Take an unsignalled exception that is masking another one due to callback failure.
	 *
	 * @param e the exception to handle
	 */
	public static void onErrorDropped(Throwable e) {
		Consumer<? super Throwable> hook = Hooks.onErrorDroppedHook;
		if (hook == null) {
			throw Exceptions.bubble(e);
		}
		hook.accept(e);
	}

	/**
	 * An unexpected event is about to be dropped.
	 *
	 * @param <T> the dropped value type
	 * @param t the dropping data
	 */
	public static <T> void onNextDropped(T t) {
		if(t != null) {
			Consumer<Object> hook = Hooks.onNextDroppedHook;
			if (hook == null) {
				throw Exceptions.failWithCancel();
			}
			hook.accept(t);
		}
	}

	/**
	 * Map an "operator" error. The
	 * result error will be passed via onError to the operator downstream after
	 * checking for fatal error via
	 * {@link Exceptions#throwIfFatal(Throwable)}.
	 *
	 * @param error the callback or operator error
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable onOperatorError(Throwable error) {
		return onOperatorError(null, error, null);
	}

	/**
	 * Map an "operator" error given an operator parent {@link Subscription}. The
	 * result error will be passed via onError to the operator downstream.
	 * {@link Subscription} will be cancelled after checking for fatal error via
	 * {@link Exceptions#throwIfFatal(Throwable)}.
	 *
	 * @param subscription the linked operator parent {@link Subscription}
	 * @param error the callback or operator error
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable onOperatorError(Subscription subscription, Throwable error) {
		return onOperatorError(subscription, error, null);
	}

	/**
	 * Map an "operator" error given an operator parent {@link Subscription}. The
	 * result error will be passed via onError to the operator downstream.
	 * {@link Subscription} will be cancelled after checking for fatal error via
	 * {@link Exceptions#throwIfFatal(Throwable)}.
	 *
	 * @param subscription the linked operator parent {@link Subscription}
	 * @param error the callback or operator error
	 * @param dataSignal the value (onNext or onError) signal processed during failure
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable onOperatorError(Subscription subscription, Throwable
			error, Object dataSignal) {

		Exceptions.throwIfFatal(error);
		if(subscription != null) {
			subscription.cancel();
		}

		Throwable t = Exceptions.unwrap(error);
		BiFunction<? super Throwable, Object, ? extends Throwable> hook =
				Hooks.onOperatorErrorHook;
		if (hook == null) {
			if (dataSignal != null) {
				if (dataSignal != t && dataSignal instanceof Throwable) {
					t.addSuppressed((Throwable) dataSignal);
				}
				//do not wrap original value to avoid strong references
				/*else {
				}*/
			}
			return t;
		}
		return hook.apply(error, dataSignal);
	}

	/**
	 * Return a wrapped {@link RejectedExecutionException} which can be thrown by the
	 * operator. That denotes that an execution was rejected by a
	 * {@link reactor.core.scheduler.Scheduler} due to dispose.
	 * <p>
	 * Wrapping is done by calling both {@link Exceptions#bubble(Throwable)} and
	 * {@link #onOperatorError(Subscription, Throwable, Object)}.
	 *
	 */
	public static RuntimeException onRejectedExecution() {
		return onRejectedExecution(null, null, null);
	}

	/**
	 * Return a wrapped {@link RejectedExecutionException} which can be thrown by the
	 * operator. That denotes that an execution was rejected by a
	 * {@link reactor.core.scheduler.Scheduler} due to dispose.
	 * <p>
	 * Wrapping is done by calling both {@link Exceptions#bubble(Throwable)} and
	 * {@link #onOperatorError(Subscription, Throwable, Object)} (with the passed
	 * {@link Subscription}).
	 *
	 * @param subscription the subscription to pass to onOperatorError.
	 * @param suppressed a Throwable to be suppressed by the {@link RejectedExecutionException} (or null if not relevant)
	 * @param dataSignal a value to be passed to {@link #onOperatorError(Subscription, Throwable, Object)} (or null if not relevant)
	 */
	public static RuntimeException onRejectedExecution(Subscription subscription, Throwable suppressed, Object dataSignal) {
		RejectedExecutionException ree = new RejectedExecutionException("Scheduler unavailable");
		if (suppressed != null) {
			ree.addSuppressed(suppressed);
		}
		if (dataSignal != null) {
			return Exceptions.propagate(Operators.onOperatorError(subscription, ree, dataSignal));
		}
		return Exceptions.propagate(Operators.onOperatorError(subscription, ree));
	}

	/**
	 * Concurrent substraction bound to 0.
	 * Any concurrent write will "happen" before this operation.
	 *
     * @param <T> the parent instance type
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param toSub    delta to sub
	 * @return value after subscription or zero
	 */
	public static <T> long produced(AtomicLongFieldUpdater<T> updater, T instance, long toSub) {
		long r, u;
		do {
			r = updater.get(instance);
			if (r == 0 || r == Long.MAX_VALUE) {
				return r;
			}
			u = subOrZero(r, toSub);
		} while (!updater.compareAndSet(instance, r, u));

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
	 * Throw {@link IllegalStateException}
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/serialize.png" alt="">
	 *
	 * @param <T> the relayed type
	 * @param subscriber the subscriber to wrap
	 * @return a serializing {@link Subscriber}
	 */
	public static <T> Subscriber<T> serialize(Subscriber<? super T> subscriber) {
		return new SerializedSubscriber<>(subscriber);
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
			s.cancel();
			return false;
		}
		if (a != null) {
			s.cancel();
			reportSubscriptionSet();
			return false;
		}

		if (field.compareAndSet(instance, null, s)) {
			return true;
		}

		a = field.get(instance);

		if (a == CancelledSubscription.INSTANCE) {
			s.cancel();
			return false;
		}

		s.cancel();
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

	/**
	 * Atomically terminates the subscription if it is not already a
	 * {@link #cancelledSubscription()}, cancelling the subscription and setting the field
	 * to the singleton {@link #cancelledSubscription()}.
	 *
	 * @param <F> the instance type containing the field
	 * @param field the field accessor
	 * @param instance the parent instance
	 * @return true if terminated, false if the subscription was already terminated
	 */
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
		if (n == 0) {
			return false;
		}
		if (n < 0) {
			reportBadRequest(n); //log instead of failure?
		}
		return true;
	}

	/**
	 * Atomically terminates the subscription if it is not already a
	 * {@link #cancelledSubscription()}, cancelling the subscription and setting the field
	 * to the singleton {@link #cancelledSubscription()}.
	 *
	 * @param <F> the instance type containing the field
	 * @param field the field accessor
	 * @param instance the parent instance
	 * @return true if terminated or null, false if the subscription was already
	 * terminated
	 */
	static <F> boolean setTerminated(AtomicReferenceFieldUpdater<F,
			Subscription> field,
			F instance) {
		Subscription a = field.get(instance);
		if (a != CancelledSubscription.INSTANCE) {
			a = field.getAndSet(instance, CancelledSubscription.INSTANCE);
			if (a == null || a != CancelledSubscription.INSTANCE) {
				return true;
			}
		}
		return false;
	}

	Operators() {
	}

	//
	enum CancelledSubscription implements Subscription, Scannable {
		INSTANCE;

		@Override
		public Object scan(Attr key) {
			switch (key) {
				case CANCELLED:
					return true;
			}
			return null;
		}

		@Override
		public void cancel() {
			// deliberately no op
		}

		@Override
		public void request(long n) {
			// deliberately no op
		}



	}

	enum EmptySubscription implements Fuseable.QueueSubscription<Object>, Scannable {
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
		public Object scan(Attr key) {
			switch (key) {
				case TERMINATED:
					return true;
			}
			return null;
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
	public static class DeferredSubscription
			implements Subscription, Scannable {

		volatile Subscription s;
		volatile long requested;

		protected boolean isCancelled(){
			return s == cancelledSubscription();
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

		@Override
		public Object scan(Attr key) {
			switch (key) {
				case PARENT:
					return s;
				case REQUESTED_FROM_DOWNSTREAM:
					return requested;
				case CANCELLED:
					return isCancelled();
			}
			return null;
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

		/**
		 * Atomically sets the single subscription and requests the missed amount from it.
		 *
		 * @param s the subscription to set
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
				reportSubscriptionSet();
				return false;
			}

			return false;
		}

		static final AtomicReferenceFieldUpdater<DeferredSubscription, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(DeferredSubscription.class, Subscription.class, "s");
		static final AtomicLongFieldUpdater<DeferredSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(DeferredSubscription.class, "requested");

	}

	/**
	 * A Subscriber/Subscription barrier that holds a single value at most and properly gates asynchronous behaviors
	 * resulting from concurrent request or cancel and onXXX signals.
	 * Publisher Operators using this Subscriber can be fused (implement Fuseable).
	 *
	 * @param <I> The upstream sequence type
	 * @param <O> The downstream sequence type
	 */
	public static class MonoSubscriber<I, O>
			implements InnerOperator<I, O>,
			           Fuseable, //for constants only
			           Fuseable.QueueSubscription<O> {

		protected final Subscriber<? super O> actual;

		protected O value;
		volatile int state;
		public MonoSubscriber(Subscriber<? super O> actual) {
			this.actual = actual;
		}

		@Override
		public void cancel() {
			this.state = CANCELLED;
			value = null;
		}

		@Override
		public Object scan(Attr key) {
			switch (key) {
				case CANCELLED:
					return isCancelled();
				case TERMINATED:
					return state == HAS_REQUEST_HAS_VALUE || state == NO_REQUEST_HAS_VALUE;
				case PREFETCH:
					return Integer.MAX_VALUE;
			}
			return InnerOperator.super.scan(key);
		}

		@Override
		public final void clear() {
			STATE.lazySet(this, FUSED_CONSUMED);
			value = null;
		}

		/**
		 * Tries to emit the value and complete the underlying subscriber or
		 * stores the value away until there is a request for it.
		 * <p>
		 * Make sure this method is called at most once
		 * @param v the value to emit
		 */
		public final void complete(O v) {
			int state = this.state;
			for (; ; ) {
				if (state == FUSED_EMPTY) {
					setValue(v);
					STATE.lazySet(this, FUSED_READY);

					Subscriber<? super O> a = actual;
					a.onNext(v);
					if (this.state != CANCELLED) {
						a.onComplete();
					}
					return;
				}

				// if state is >= HAS_CANCELLED or bit zero is set (*_HAS_VALUE) case, return
				if ((state & ~HAS_REQUEST_NO_VALUE) != 0) {
					return;
				}

				if (state == HAS_REQUEST_NO_VALUE) {
					STATE.lazySet(this, HAS_REQUEST_HAS_VALUE);
					Subscriber<? super O> a = actual;
					a.onNext(v);
					if (this.state != CANCELLED) {
						a.onComplete();
					}
					return;
				}
				setValue(v);
				if (STATE.compareAndSet(this, NO_REQUEST_NO_VALUE, NO_REQUEST_HAS_VALUE)) {
					return;
				}
				state = this.state;
				if (state == CANCELLED) {
					value = null;
					return;
				}
			}
		}

		@Override
		public final Subscriber<? super O> actual() {
			return actual;
		}

		/**
		 * Returns true if this Subscription has been cancelled.
		 * @return true if this Subscription has been cancelled
		 */
		public final boolean isCancelled() {
			return state == CANCELLED;
		}

		@Override
		public final boolean isEmpty() {
			return this.state != FUSED_READY;
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onNext(I t) {
			setValue((O) t);
		}

		@Override
		public void onSubscribe(Subscription s) {
			//if upstream
		}

		@Override
		public final O poll() {
			if (STATE.get(this) == FUSED_READY) {
				STATE.lazySet(this, FUSED_CONSUMED);
				O v = value;
				value = null;
				return v;
			}
			return null;
		}

		@Override
		public void request(long n) {
			if (validate(n)) {
				for (; ; ) {
					int s = state;
					// if the any bits 1-31 are set, we are either in fusion mode (FUSED_*)
					// or request has been called (HAS_REQUEST_*)
					if ((s & ~NO_REQUEST_HAS_VALUE) != 0) {
						return;
					}
					if (s == NO_REQUEST_HAS_VALUE) {
						if (STATE.compareAndSet(this, NO_REQUEST_HAS_VALUE, HAS_REQUEST_HAS_VALUE)) {
							O v = value;
							if (v != null) {
								value = null;
								Subscriber<? super O> a = actual;
								a.onNext(v);
								if (state != CANCELLED) {
									a.onComplete();
								}
							}
						}
						return;
					}
					if (STATE.compareAndSet(this, NO_REQUEST_NO_VALUE, HAS_REQUEST_NO_VALUE)) {
						return;
					}
				}
			}
		}

		@Override
		public int requestFusion(int mode) {
			if ((mode & ASYNC) != 0) {
				STATE.lazySet(this, FUSED_EMPTY);
				return ASYNC;
			}
			return NONE;
		}

		public void setValue(O value) {
			this.value = value;
		}

		@Override
		public int size() {
			return isEmpty() ? 0 : 1;
		}

		/**
		 * Indicates this Subscription has no value and not requested yet.
		 */
		static final int NO_REQUEST_NO_VALUE   = 0;
		/**
		 * Indicates this Subscription has a value but not requested yet.
		 */
		static final int NO_REQUEST_HAS_VALUE  = 1;
		/**
		 * Indicates this Subscription has been requested but there is no value yet.
		 */
		static final int HAS_REQUEST_NO_VALUE  = 2;
		/**
		 * Indicates this Subscription has both request and value.
		 */
		static final int HAS_REQUEST_HAS_VALUE = 3;
		/**
		 * Indicates the Subscription has been cancelled.
		 */
		static final int CANCELLED = 4;
		/**
		 * Indicates this Subscription is in fusion mode and is currently empty.
		 */
		static final int FUSED_EMPTY    = 8;
		/**
		 * Indicates this Subscription is in fusion mode and has a value.
		 */
		static final int FUSED_READY    = 16;
		/**
		 * Indicates this Subscription is in fusion mode and its value has been consumed.
		 */
		static final int FUSED_CONSUMED = 32;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MonoSubscriber> STATE =
				AtomicIntegerFieldUpdater.newUpdater(MonoSubscriber.class, "state");
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
			implements InnerOperator<I, O> {

		final Subscriber<? super O> actual;

		protected boolean unbounded;
		/**
		 * The current subscription which may null if no Subscriptions have been set.
		 */
		Subscription subscription;
		/**
		 * The current outstanding request amount.
		 */
		long         requested;
		volatile Subscription missedSubscription;
		volatile long missedRequested;
		volatile long missedProduced;
		volatile int wip;
		volatile boolean cancelled;

		public MultiSubscriptionSubscriber(Subscriber<? super O> actual) {
			this.actual = actual;
		}

		@Override
		public Subscriber<? super O> actual() {
			return actual;
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				drain();
			}
		}

		@Override
		public Object scan(Attr key) {
			switch (key) {
				case PARENT:
					return missedSubscription != null ? missedSubscription : subscription;
				case CANCELLED:
					return isCancelled();
				case REQUESTED_FROM_DOWNSTREAM:
					return Operators.addCap(requested, missedRequested);
			}
			return InnerOperator.super.scan(key);
		}

		public final boolean isUnbounded() {
			return unbounded;
		}

		final boolean isCancelled() {
			return cancelled;
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onSubscribe(Subscription s) {
			set(s);
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

		final void producedOne() {
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
		            Subscription a = subscription;

	                if (WIP.decrementAndGet(this) != 0) {
	                    drainLoop();
	                }

	                if (a != null) {
	                    a.request(n);
	                }

	                return;
	            }

	            getAndAddCap(MISSED_REQUESTED, this, n);

	            drain();
	        }
		}

		public final void set(Subscription s) {
		    if (cancelled) {
	            s.cancel();
	            return;
	        }

	        Objects.requireNonNull(s);

	        if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
		        Subscription a = subscription;

	            if (a != null && shouldCancelCurrent()) {
	                a.cancel();
	            }

		        subscription = s;

	            long r = requested;

	            if (WIP.decrementAndGet(this) != 0) {
	                drainLoop();
	            }

	            if (r != 0L) {
	                s.request(r);
	            }

	            return;
	        }

	        Subscription a = MISSED_SUBSCRIPTION.getAndSet(this, s);
	        if (a != null && shouldCancelCurrent()) {
	            a.cancel();
	        }
	        drain();
		}

		/**
		 * When setting a new subscription via set(), should
		 * the previous subscription be cancelled?
		 * @return true if cancellation is needed
		 */
		protected boolean shouldCancelCurrent() {
			return false;
		}

		final void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			drainLoop();
		}

		final void drainLoop() {
		    int missed = 1;

	        long requestAmount = 0L;
	        Subscription requestTarget = null;

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

		        Subscription a = subscription;

	            if (cancelled) {
	                if (a != null) {
	                    a.cancel();
		                subscription = null;
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
		                subscription = ms;
		                if (r != 0L) {
			                requestAmount = addCap(requestAmount, r);
	                        requestTarget = ms;
	                    }
	                } else if (mr != 0L && a != null) {
	                    requestAmount = addCap(requestAmount, mr);
	                    requestTarget = a;
	                }
	            }

	            missed = WIP.addAndGet(this, -missed);
	            if (missed == 0) {
	                if (requestAmount != 0L) {
	                    requestTarget.request(requestAmount);
	                }
	                return;
	            }
	        }
		}
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MultiSubscriptionSubscriber, Subscription>
				MISSED_SUBSCRIPTION =
		  AtomicReferenceFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class,
			Subscription.class,
			"missedSubscription");
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<MultiSubscriptionSubscriber>
				MISSED_REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "missedRequested");
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<MultiSubscriptionSubscriber> MISSED_PRODUCED =
		  AtomicLongFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "missedProduced");
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MultiSubscriptionSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "wip");
	}

	/**
	 * Represents a fuseable Subscription that emits a single constant value synchronously
	 * to a Subscriber or consumer.
	 *
	 * @param <T> the value type
	 */
	static final class ScalarSubscription<T>
			implements Fuseable.SynchronousSubscription<T>, InnerProducer<T> {

		final Subscriber<? super T> actual;

		final T value;

		volatile int once;
		ScalarSubscription(Subscriber<? super T> actual, T value) {
			this.value = Objects.requireNonNull(value, "value");
			this.actual = Objects.requireNonNull(actual, "actual");
		}

		@Override
		public void cancel() {
			ONCE.lazySet(this, 2);
		}

		@Override
		public void clear() {
			ONCE.lazySet(this, 1);
		}

		@Override
		public boolean isEmpty() {
			return once != 0;
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
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
		public Object scan(Attr key) {
			switch (key) {
				case TERMINATED:
				case CANCELLED:
					return once == 1;
			}
			return InnerProducer.super.scan(key);
		}

		@Override
		public void request(long n) {
			if (validate(n)) {
				if (ONCE.compareAndSet(this, 0, 1)) {
					Subscriber<? super T> a = actual;
					a.onNext(value);
					if(once != 2) {
						a.onComplete();
					}
				}
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.SYNC) != 0) {
				return Fuseable.SYNC;
			}
			return 0;
		}

		@Override
		public int size() {
			return isEmpty() ? 0 : 1;
		}

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ScalarSubscription> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(ScalarSubscription.class, "once");
	}

	final static class DrainSubscriber<T> implements Subscriber<T> {

		static final DrainSubscriber INSTANCE = new DrainSubscriber();

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Object o) {

		}

		@Override
		public void onError(Throwable t) {
			Operators.onErrorDropped(Exceptions.errorCallbackNotImplemented(t));
		}

		@Override
		public void onComplete() {

		}
	}

	final static Logger log = Loggers.getLogger(Operators.class);
}
