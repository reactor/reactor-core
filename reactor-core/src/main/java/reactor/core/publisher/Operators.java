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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

/**
 * An helper to support "Operator" writing, handle noop subscriptions, validate request
 * size and to cap concurrent additive operations to Long.MAX_VALUE,
 * which is generic to {@link Subscription#request(long)} handling.
 *
 * Combine utils available to operator implementations, @see http://github.com/reactor/reactive-streams-commons
 *
 */
public abstract class Operators {

	/**
	 * Concurrent addition bound to Long.MAX_VALUE. Any concurrent write will "happen
	 * before" this operation.
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
	@Nullable
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
	 * subscription because there is no standard way to tell if a Subscription is cancelled
	 * or not otherwise.
	 *
	 * @return a singleton noop {@link Subscription} to be used as an inner representation
	 * of the cancelled state
	 */
	public static Subscription cancelledSubscription() {
		return CancelledSubscription.INSTANCE;
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
	public static <T> CoreSubscriber<T> drainSubscriber() {
		return (CoreSubscriber<T>)DrainSubscriber.INSTANCE;
	}

	/**
	 * A {@link Subscriber} that is expected to be used as a placeholder and
	 * never actually be called. All methods log an error.
	 *
	 * @param <T> the type of data (ignored)
	 * @return a placeholder subscriber
	 */
	@SuppressWarnings("unchecked")
	public static <T> CoreSubscriber<T> emptySubscriber() {
		return (CoreSubscriber<T>) EMPTY_SUBSCRIBER;
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
	 * Any concurrent write will "happen before" this operation.
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
	 * Create a function that can be used to support a custom operator via
	 * {@link CoreSubscriber} decoration. The function is compatible with
	 * {@link Flux#transform(Function)}, {@link Mono#transform(Function)},
	 * {@link Hooks#onEachOperator(Function)} and {@link Hooks#onLastOperator(Function)}
	 *
	 * @param lifter the bifunction taking {@link Scannable} from the enclosing
	 * publisher and consuming {@link CoreSubscriber}. It must return a receiving
	 * {@link CoreSubscriber} that will immediately subscribe to the applied
	 * {@link Publisher}.
	 *
	 * @param <I> the input type
	 * @param <O> the output type
	 *
	 * @return a new {@link Function}
	 */
	public static <I, O> Function<? super Publisher<I>, ? extends Publisher<O>> lift(BiFunction<Scannable, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>> lifter) {
		return new LiftFunction<>(null, lifter);
	}

	/**
	 * Create a function that can be used to support a custom operator via
	 * {@link CoreSubscriber} decoration. The function is compatible with
	 * {@link Flux#transform(Function)}, {@link Mono#transform(Function)},
	 * {@link Hooks#onEachOperator(Function)} and {@link Hooks#onLastOperator(Function)}
	 *
	 * <p>
	 *     The function will be invoked only if the passed {@link Predicate} matches.
	 *     Therefore the transformed type O must be the same than the input type since
	 *     unmatched predicate will return the applied {@link Publisher}.
	 *
	 * @param filter the predicate to match taking {@link Scannable} from the applied
	 * publisher to operate on
	 * @param lifter the bifunction taking {@link Scannable} from the enclosing
	 * publisher and consuming {@link CoreSubscriber}. It must return a receiving
	 * {@link CoreSubscriber} that will immediately subscribe to the applied
	 * {@link Publisher}.
	 *
	 * @param <O> the input and output type
	 *
	 * @return a new {@link Function}
	 */
	public static <O> Function<? super Publisher<O>, ? extends Publisher<O>> lift(
			Predicate<Scannable> filter,
			BiFunction<Scannable, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super O>> lifter) {
		return new LiftFunction<>(filter, lifter);
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
	 * An unexpected exception is about to be dropped.
	 *
	 * @param e the dropped exception
	 * @see #onErrorDropped(Throwable, Context)
	 */
	public static void onErrorDropped(Throwable e) {
		onErrorDropped(e, Context.empty());
	}

	/**
	 * An unexpected exception is about to be dropped.
	 *
	 * @param e the dropped exception
	 * @param context a context that might hold a local error consumer
	 */
	public static void onErrorDropped(Throwable e, Context context) {
		Consumer<? super Throwable> hook = Hooks.onErrorDroppedHook;
		if (hook == null) {
			throw Exceptions.bubble(e);
		}
		hook.accept(e);
	}

	/**
	 * An unexpected event is about to be dropped.
	 * <p>
	 * If no hook is registered for {@link Hooks#onNextDropped(Consumer)}, the dropped
	 * element is just logged at DEBUG level.
	 *
	 * @param <T> the dropped value type
	 * @param t the dropped data
	 * @see #onNextDropped(Object, Context)
	 */
	public static <T> void onNextDropped(T t) {
		onNextDropped(t, Context.empty());
	}

	/**
	 * An unexpected event is about to be dropped.
	 * <p>
	 * If no hook is registered for {@link Hooks#onNextDropped(Consumer)}, the dropped
	 * element is just logged at DEBUG level.
	 *
	 * @param <T> the dropped value type
	 * @param t the dropped data
	 * @param context a context that might hold a local error consumer
	 */
	public static <T> void onNextDropped(T t, Context context) {
		Objects.requireNonNull(t, "onNext");
		Objects.requireNonNull(context, "context");
		Consumer<Object> hook = Hooks.onNextDroppedHook;
		if (hook != null) {
			hook.accept(t);
		}
		else if (log.isDebugEnabled()) {
			log.debug("onNextDropped: " + t);
		}
	}

	/**
	 * Map an "operator" error. The
	 * result error will be passed via onError to the operator downstream after
	 * checking for fatal error via
	 * {@link Exceptions#throwIfFatal(Throwable)}.
	 *
	 * @param error the callback or operator error
	 * @param context a context that might hold a local error consumer
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable onOperatorError(Throwable error, Context context) {
		return onOperatorError(null, error, context);
	}

	/**
	 * Map an "operator" error given an operator parent {@link Subscription}. The
	 * result error will be passed via onError to the operator downstream.
	 * {@link Subscription} will be cancelled after checking for fatal error via
	 * {@link Exceptions#throwIfFatal(Throwable)}.
	 *
	 * @param subscription the linked operator parent {@link Subscription}
	 * @param error the callback or operator error
	 * @param context a context that might hold a local error consumer
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable onOperatorError(@Nullable Subscription subscription,
			Throwable error,
			Context context) {
		return onOperatorError(subscription, error, null, context);
	}

	/**
	 * Map an "operator" error given an operator parent {@link Subscription}. The
	 * result error will be passed via onError to the operator downstream.
	 * {@link Subscription} will be cancelled after checking for fatal error via
	 * {@link Exceptions#throwIfFatal(Throwable)}. Takes an additional signal, which
	 * can be added as a suppressed exception if it is a {@link Throwable} and the
	 * default {@link Hooks#onOperatorError(BiFunction) hook} is in place.
	 *
	 * @param subscription the linked operator parent {@link Subscription}
	 * @param error the callback or operator error
	 * @param dataSignal the value (onNext or onError) signal processed during failure
	 * @param context a context that might hold a local error consumer
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable onOperatorError(@Nullable Subscription subscription,
			Throwable error,
			@Nullable Object dataSignal, Context context) {

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
	 * operator. This exception denotes that an execution was rejected by a
	 * {@link reactor.core.scheduler.Scheduler}, notably when it was already disposed.
	 * <p>
	 * Wrapping is done by calling both {@link Exceptions#bubble(Throwable)} and
	 * {@link #onOperatorError(Subscription, Throwable, Object, Context)}.
	 *
	 * @param original the original execution error
	 * @param context a context that might hold a local error consumer
	 *
	 */
	public static RuntimeException onRejectedExecution(Throwable original, Context context) {
		return onRejectedExecution(original, null, null, null, context);
	}

	/**
	 * Return a wrapped {@link RejectedExecutionException} which can be thrown by the
	 * operator. This exception denotes that an execution was rejected by a
	 * {@link reactor.core.scheduler.Scheduler}, notably when it was already disposed.
	 * <p>
	 * Wrapping is done by calling both {@link Exceptions#bubble(Throwable)} and
	 * {@link #onOperatorError(Subscription, Throwable, Object, Context)} (with the passed
	 * {@link Subscription}).
	 *
	 * @param original the original execution error
	 * @param subscription the subscription to pass to onOperatorError.
	 * @param suppressed a Throwable to be suppressed by the {@link RejectedExecutionException} (or null if not relevant)
	 * @param dataSignal a value to be passed to {@link #onOperatorError(Subscription, Throwable, Object, Context)} (or null if not relevant)
	 * @param context a context that might hold a local error consumer
	 */
	public static RuntimeException onRejectedExecution(Throwable original,
			@Nullable Subscription subscription,
			@Nullable Throwable suppressed,
			@Nullable Object dataSignal, Context context) {
		//FIXME only create REE if original is REE singleton OR there's suppressed OR there's Throwable dataSignal
		RejectedExecutionException ree = Exceptions.failWithRejected(original);
		if (suppressed != null) {
			ree.addSuppressed(suppressed);
		}
		if (dataSignal != null) {
			return Exceptions.propagate(Operators.onOperatorError(subscription, ree, dataSignal,
					Context.empty()));
		}
		return Exceptions.propagate(Operators.onOperatorError(subscription, ree,
				Context.empty()));
	}

	/**
	 * Concurrent subtraction bound to 0, mostly used to decrement a request tracker by
	 * the amount produced by the operator. Any concurrent write will "happen before"
	 * this operation.
	 *
     * @param <T> the parent instance type
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param toSub    delta to subtract
	 * @return value after subtraction or zero
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
	 * A generic utility to atomically replace a subscription or cancel the replacement
	 * if the current subscription is marked as already cancelled (as in
	 * {@link #cancelledSubscription()}).
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
	 * Log an {@link IllegalArgumentException} if the request is null or negative.
	 *
	 * @param n the failing demand
	 *
	 * @see Exceptions#nullOrNegativeRequestException(long)
	 */
	public static void reportBadRequest(long n) {
		if (log.isDebugEnabled()) {
			log.debug("Negative request",
					Exceptions.nullOrNegativeRequestException(n));
		}
	}

	/**
	 * Log an {@link IllegalStateException} that indicates more than the requested
	 * amount was produced.
	 *
	 * @see Exceptions#failWithOverflow()
	 */
	public static void reportMoreProduced() {
		if (log.isDebugEnabled()) {
			log.debug("More data produced than requested",
					Exceptions.failWithOverflow());
		}
	}

	/**
	 * Log a {@link Exceptions#duplicateOnSubscribeException() duplicate subscription} error.
	 *
	 * @see Exceptions#duplicateOnSubscribeException()
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
	public static <T> Subscription scalarSubscription(CoreSubscriber<? super T> subscriber,
			T value){
		return new ScalarSubscription<>(subscriber, value);
	}

	/**
	 * Safely gate a {@link Subscriber} by making sure onNext signals are delivered
	 * sequentially (serialized).
	 * Serialization uses thread-stealing and a potentially unbounded queue that might
	 * starve a calling thread if races are too important and {@link Subscriber} is slower.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.0.M3/src/docs/marble/serialize.png" alt="">
	 *
	 * @param <T> the relayed type
	 * @param subscriber the subscriber to serialize
	 * @return a serializing {@link Subscriber}
	 */
	public static <T> CoreSubscriber<T> serialize(CoreSubscriber<? super T> subscriber) {
		return new SerializedSubscriber<>(subscriber);
	}

	/**
	 * A generic utility to atomically replace a subscription or cancel the replacement
	 * if current subscription is marked as cancelled (as in {@link #cancelledSubscription()})
	 * or was concurrently updated before.
	 * <p>
	 * The replaced subscription is itself cancelled.
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
	 * <p>
	 * If the field already has a subscription, it is cancelled and the duplicate
	 * subscription is reported (see {@link #reportSubscriptionSet()}).
	 *
	 * @param <F> the instance type containing the field
	 * @param field the field accessor
	 * @param instance the parent instance
	 * @param s the subscription to push once
	 * @return true if successful, false if the target was not empty or has been cancelled
	 */
	public static <F> boolean setOnce(AtomicReferenceFieldUpdater<F, Subscription> field, F instance, Subscription s) {
		Objects.requireNonNull(s, "subscription");
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
	 * Cap a subtraction to 0
	 *
	 * @param a left operand
	 * @param b right operand
	 * @return Subtraction result or 0 if overflow
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
	 * Check Subscription current state and cancel new Subscription if current is push,
	 * or return true if ready to subscribe.
	 *
	 * @param current current Subscription, expected to be null
	 * @param next new Subscription
	 * @return true if Subscription can be used
	 */
	public static boolean validate(@Nullable Subscription current, Subscription next) {
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
		if (n <= 0) {
			reportBadRequest(n);
			return false;
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
	static <F> boolean setTerminated(AtomicReferenceFieldUpdater<F, Subscription> field,
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

	/**
	 * If the actual {@link Subscriber} is not a {@link CoreSubscriber}, it will apply
	 * safe strict wrapping to apply all reactive streams rules including the ones
	 * relaxed by internal operators based on {@link CoreSubscriber}.
	 *
	 * @param <T> passed subscriber type
	 *
	 * @param actual the {@link Subscriber} to apply hook on
	 * @return an eventually transformed {@link Subscriber}
	 */
	@SuppressWarnings("unchecked")
	public static <T> CoreSubscriber<? super T> toCoreSubscriber(Subscriber<? super T> actual) {

		Objects.requireNonNull(actual, "actual");

		CoreSubscriber<? super T> _actual;

		if (actual instanceof CoreSubscriber){
			_actual = (CoreSubscriber<? super T>) actual;
		}
		else {
			_actual = new StrictSubscriber<>(actual);
		}

		return _actual;
	}

	Operators() {
	}

	static final CoreSubscriber<?> EMPTY_SUBSCRIBER = new CoreSubscriber<Object>() {
		@Override
		public void onSubscribe(Subscription s) {
			Throwable e = new IllegalStateException("onSubscribe should not be used");
			log.error("Unexpected call to Operators.emptySubscriber()", e);
		}

		@Override
		public void onNext(Object o) {
			Throwable e = new IllegalStateException("onNext should not be used, got " + o);
			log.error("Unexpected call to Operators.emptySubscriber()", e);
		}

		@Override
		public void onError(Throwable t) {
			Throwable e = new IllegalStateException("onError should not be used", t);
			log.error("Unexpected call to Operators.emptySubscriber()", e);
		}

		@Override
		public void onComplete() {
			Throwable e = new IllegalStateException("onComplete should not be used");
			log.error("Unexpected call to Operators.emptySubscriber()", e);
		}
	};

	//
	enum CancelledSubscription implements Subscription, Scannable {
		INSTANCE;

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) {
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
		@Nullable
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return true;
			return null;
		}

		@Override
		public int size() {
			return 0;
		}

	}

	/**
	 * Base class for Subscribers that will receive their Subscriptions at any time, yet
	 * they might also need to be cancelled or requested at any time.
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.CANCELLED) return isCancelled();

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
		 * @param s the subscription to push
		 * @return false if this arbiter is cancelled or there was a subscription already push
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

		protected final CoreSubscriber<? super O> actual;

		protected O value;
		volatile int state;
		public MonoSubscriber(CoreSubscriber<? super O> actual) {
			this.actual = actual;
		}

		@Override
		public void cancel() {
			this.state = CANCELLED;
			value = null;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return isCancelled();
			if (key == Attr.TERMINATED) return state == HAS_REQUEST_HAS_VALUE || state == NO_REQUEST_HAS_VALUE;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;

			return InnerOperator.super.scanUnsafe(key);
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

				// if state is >= HAS_CANCELLED or bit zero is push (*_HAS_VALUE) case, return
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
		public final CoreSubscriber<? super O> actual() {
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
		@Nullable
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
					// if the any bits 1-31 are push, we are either in fusion mode (FUSED_*)
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

		/**
		 * Set the value internally, without impacting request tracking state.
		 *
		 * @param value the new value.
		 * @see #complete(Object)
		 */
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
	 * duration until the first Subscription is push.
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

		final CoreSubscriber<? super O> actual;

		protected boolean unbounded;
		/**
		 * The current subscription which may null if no Subscriptions have been push.
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

		public MultiSubscriptionSubscriber(CoreSubscriber<? super O> actual) {
			this.actual = actual;
		}

		@Override
		public CoreSubscriber<? super O> actual() {
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT)
				return missedSubscription != null ? missedSubscription : subscription;
			if (key == Attr.CANCELLED) return isCancelled();
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM)
				return Operators.addCap(requested, missedRequested);

			return InnerOperator.super.scanUnsafe(key);
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
		 * When setting a new subscription via push(), should
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

		final CoreSubscriber<? super T> actual;

		final T value;

		volatile int once;
		ScalarSubscription(CoreSubscriber<? super T> actual, T value) {
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
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public T poll() {
			if (once == 0) {
				ONCE.lazySet(this, 1);
				return value;
			}
			return null;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED)
				return once == 1;

			return InnerProducer.super.scanUnsafe(key);
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

	final static class DrainSubscriber<T> implements CoreSubscriber<T> {

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

	final static class LiftFunction<I, O>
			implements Function<Publisher<I>, Publisher<O>> {

		final Predicate<Scannable> filter;

		final BiFunction<Scannable, ? super CoreSubscriber<? super O>,
				? extends CoreSubscriber<? super I>> lifter;

		LiftFunction(@Nullable Predicate<Scannable> filter,
				BiFunction<Scannable, ? super CoreSubscriber<? super O>,
				? extends CoreSubscriber<? super I>> lifter) {
			this.filter = filter;
			this.lifter = Objects.requireNonNull(lifter, "lifter");
		}

		@Override
		@SuppressWarnings("unchecked")
		public Publisher<O> apply(Publisher<I> publisher) {
			if (filter != null && !filter.test(Scannable.from(publisher))) {
				return (Publisher<O>)publisher;
			}
			if (publisher instanceof Mono) {
				return new MonoLift<>(publisher, lifter);
			}
			if (publisher instanceof ParallelFlux) {
				return new ParallelLift<>((ParallelFlux<I>)publisher, lifter);
			}

			return new FluxLift<>(publisher, lifter);
		}
	}


	final static Logger log = Loggers.getLogger(Operators.class);
}
