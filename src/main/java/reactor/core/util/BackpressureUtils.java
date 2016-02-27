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

package reactor.core.util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A generic utility to check subscription, request size and to cap concurrent additive operations to Long
 * .MAX_VALUE_LONG,
 * which is generic to {@link Subscription#request(long)} handling.
 *
 * Combine utils available to operator implementations, @see http://github.com/reactor/reactive-streams-commons
 *
 * @author Stephane Maldini
 *
 * @since 2.5
 */
public enum BackpressureUtils {
	;

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
	 * Cap a multiplication to Long.MAX_VALUE
	 *
	 * @param a left operand
	 * @param b right operand
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
	 * Cap an addition to Long.MAX_VALUE
	 *
	 * @param a left operand
	 * @param b right operand
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
	 * Cap a substraction to 0
	 *
	 * @param a left operand
	 * @param b right operand
	 * @return Subscription result or 0 if overflow
	 */
	public static int subOrZero(int a, int b) {
		int res = a - b;
		if (res < 0) {
			return 0;
		}
		return res;
	}

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
		} while (!current.compareAndSet(r, u));

		return u;
	}

	/**
	 * Concurrent addition bound to Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param n    delta to add
	 * @return Addition result or Long.MAX_VALUE
	 */
	public static <T> long addAndGet(AtomicLongFieldUpdater<T> updater, T instance, long n) {
		for (;;) {
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
	 * Concurrent addition bound to Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param toAdd    delta to add
	 * @return value before addition or Long.MAX_VALUE
	 */
	public static <T> long getAndAdd(AtomicLongFieldUpdater<T> updater, T instance, long toAdd) {
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
	 * Concurrent addition bound to Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param sequence current sequence to update
	 * @param toAdd    delta to add
	 * @return value before addition or Long.MAX_VALUE
	 */
	public static long getAndAdd(Sequence sequence, long toAdd) {
		long u, r;
		do {
			r = sequence.getAsLong();
			if (r == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			u = addCap(r, toAdd);
		} while (!sequence.compareAndSet(r, u));
		return r;
	}

	/**
	 * Concurrent substraction bound to 0.
	 * Any concurrent write will "happen" before this operation.
	 *
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
	 * Concurrent substraction bound to 0.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param toSub    delta to sub
	 * @return value before subscription or zero
	 */
	public static <T> long getAndSub(AtomicIntegerFieldUpdater<T> updater, T instance, int toSub) {
		int r, u;
		do {
			r = updater.get(instance);
			if (r == 0) {
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
	 * @param sequence current sequence to update
	 * @param toSub    delta to sub
	 * @return value before subscription, 0 or Long.MAX_VALUE
	 */
	public static long getAndSub(Sequence sequence, long toSub) {
		long r, u;
		do {
			r = sequence.getAsLong();
			if (r == 0 || r == Long.MAX_VALUE) {
				return r;
			}
			u = subOrZero(r, toSub);
		} while (!sequence.compareAndSet(r, u));

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

	public static <F> boolean terminate(AtomicReferenceFieldUpdater<F, Subscription> field, F instance) {
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
	 * A generic utility to atomically replace a subscription or cancel if marked by a {@link CancelledSubscription}
	 * or concurrently set before.
	 *
	 * @param field The Atomic container
	 * @param instance the instance reference
	 * @param s the subscription
	 * @param <F> the instance type
	 * @return true if replaced
	 */
	public static <F> boolean set(AtomicReferenceFieldUpdater<F, Subscription> field, F instance, Subscription s) {
		for (;;) {
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
	 * A generic utility to atomically replace a subscription or cancel if marked by a {@link CancelledSubscription}.
	 *
	 * @param field The Atomic container
	 * @param instance the instance reference
	 * @param s the subscription
	 * @param <F> the instance type
	 * @return true if replaced
	 */
	public static <F> boolean replace(AtomicReferenceFieldUpdater<F, Subscription> field, F instance, Subscription s) {
		for (;;) {
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
	 * Log or Throw {@link reactor.core.util.Exceptions.DuplicateOnSubscribeException}
	 */
	public static void reportSubscriptionSet() {
		if(!PlatformDependent.TRACE_CANCEL) {
			Logger.getLogger(BackpressureUtils.class)
			      .trace("Duplicate Subscription has been detected");
		}
		else {
			throw Exceptions.duplicateOnSubscribeException();
		}
	}

	/**
	 * Throw {@link reactor.core.util.Exceptions.NullOrNegativeRequestException}
	 * @param n the demand to evaluate
	 */
	public static void reportBadRequest(long n) {
		throw Exceptions.nullOrNegativeRequestException(n);
	}

	/**
	 * Throw {@link reactor.core.util.Exceptions.InsufficientCapacityException}
	 */
	public static void reportMoreProduced() {
		Exceptions.failWithOverflow();
	}
}
