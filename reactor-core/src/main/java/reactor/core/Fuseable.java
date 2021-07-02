/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Callable;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.util.annotation.Nullable;

/**
 * A micro API for stream fusion, in particular marks producers that support a {@link QueueSubscription}.
 */
public interface Fuseable {

	/** Indicates the QueueSubscription can't support the requested mode. */
	int NONE = 0;
	/** Indicates the QueueSubscription can perform sync-fusion. */
	int SYNC = 1;
	/** Indicates the QueueSubscription can perform only async-fusion. */
	int ASYNC = 2;
	/** Indicates the QueueSubscription should decide what fusion it performs (input only). */
	int ANY = 3;
	/**
	 * Indicates that the queue will be drained from another thread
	 * thus any queue-exit computation may be invalid at that point.
	 * <p>
	 * For example, an {@code asyncSource.map().publishOn().subscribe()} sequence where {@code asyncSource}
	 * is async-fuseable: publishOn may fuse the whole sequence into a single Queue. That in turn
	 * could invoke the mapper function from its {@code poll()} method from another thread,
	 * whereas the unfused sequence would have invoked the mapper on the previous thread.
	 * If such mapper invocation is costly, it would escape its thread boundary this way.
	 */
	int THREAD_BARRIER = 0b100; //4

	/**
	 * Attempt to convert a fusion mode int code into a human-readable representation.
	 * Note that this can include the {@link #THREAD_BARRIER} flag, as an appended {@code +THREAD_BARRIER}.
	 * <p>
	 * This method accepts {@code -1} as a special code, mainly for the benefit of supporting testing scenarios where
	 * fusion can be entirely deactivated (returns {@code Disabled}).
	 * Other negative values and unknown positive codes on the other hand return {@code Unknown(x)}.
	 * <p>
	 * Note that as this is a human-facing representation, the different values could evolve in the future.
	 * As such, never compare the returned string to a constant, but always use this method on both sides of
	 * a comparison.
	 *
	 * @param mode the fusion mode int code
	 * @return a human-readable {@link String} representation of the code
	 */
	static String fusionModeName(int mode) {
		return fusionModeName(mode, false);
	}

	/**
	 * Attempt to convert a fusion mode int code into a human-readable representation.
	 * Note that this can include the {@link #THREAD_BARRIER} flag, as an appended {@code +THREAD_BARRIER},
	 * unless the {@code ignoreThreadBarrier} parameter is set to {@literal true}.
	 * <p>
	 * This method accepts {@code -1} as a special code, mainly for the benefit of supporting testing scenarios where
	 * fusion can be entirely deactivated (returns {@code Disabled}).
	 * Other negative values and unknown positive codes on the other hand return {@code Unknown(x)}.
	 * <p>
	 * Note that as this is a human-facing representation, the different values could evolve in the future.
	 * As such, never compare the returned string to a constant, but always use this method on both sides of
	 * a comparison.
	 *
	 * @param mode the fusion mode int code
	 * @param ignoreThreadBarrier whether or not to ignore the {@link #THREAD_BARRIER} flag in the representation
	 * @return a human-readable {@link String} representation of the code
	 */
	static String fusionModeName(int mode, boolean ignoreThreadBarrier) {
		int evaluated = mode;
		String threadBarrierSuffix = "";
		if (mode >= 0) {
			evaluated = mode & ~THREAD_BARRIER; //erase the THREAD_BARRIER bit;
			if (!ignoreThreadBarrier && (mode & THREAD_BARRIER) == THREAD_BARRIER) {
				threadBarrierSuffix = "+THREAD_BARRIER";
			}
		}

		switch (evaluated) {
			case -1:
				return "Disabled"; //this is more specific for tests or things that can entirely skip the fusion negotiation
			case Fuseable.NONE:
				return "NONE" + threadBarrierSuffix;
			case Fuseable.SYNC:
				return "SYNC" + threadBarrierSuffix;
			case Fuseable.ASYNC:
				return "ASYNC" + threadBarrierSuffix;
			default:
				return "Unknown(" + evaluated + ")" + threadBarrierSuffix;
		}
	}

	/**
	 * A subscriber variant that can immediately tell if it consumed
	 * the value or not, directly allowing a new value to be sent if
	 * it didn't. This avoids the usual request(1) round-trip for dropped
	 * values.
	 *
	 * @param <T> the value type
	 */
	interface ConditionalSubscriber<T> extends CoreSubscriber<T> {
		/**
		 * Try consuming the value and return true if successful.
		 * @param t the value to consume, not null
		 * @return true if consumed, false if dropped and a new value can be immediately sent
		 */
		boolean tryOnNext(T t);
	}

	/**
	 * Support contract for queue-fusion based optimizations on subscriptions.
	 *
	 * <ul>
	 *  <li>
	 *  Synchronous sources which have fixed size and can
	 *  emit their items in a pull fashion, thus avoiding the request-accounting
	 *  overhead in many cases.
	 *  </li>
	 *  <li>
	 *  Asynchronous sources which can act as a queue and subscription at
	 *  the same time, saving on allocating another queue most of the time.
	 * </li>
	 * </ul>
	 *
	 * <p>
	 *
	 * @param <T> the value type emitted
	 */
	interface QueueSubscription<T> extends Queue<T>, Subscription {
		
		String NOT_SUPPORTED_MESSAGE = "Although QueueSubscription extends Queue it is purely internal" +
				" and only guarantees support for poll/clear/size/isEmpty." +
				" Instances shouldn't be used/exposed as Queue outside of Reactor operators.";

		/**
		 * Request a specific fusion mode from this QueueSubscription.
		 * <p>
		 * One should request either SYNC, ASYNC or ANY modes (never NONE)
		 * and the implementor should return NONE, SYNC or ASYNC (never ANY).
		 * <p>
		 * For example, if a source supports only ASYNC fusion but
		 * the intermediate operator supports only SYNC fuseable sources,
		 * the operator may request SYNC fusion and the source can reject it via
		 * NONE, thus the operator can return NONE as well to downstream and the
		 * fusion doesn't happen.
		 *
		 * @param requestedMode the mode requested by the intermediate operator
		 * @return the actual fusion mode activated
		 */
		int requestFusion(int requestedMode);

		
		@Override
		@Nullable
		default T peek() {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default boolean add(@Nullable T t) {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default boolean offer(@Nullable T t) {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default T remove() {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default T element() {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default boolean contains(@Nullable Object o) {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default Iterator<T> iterator() {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default Object[] toArray() {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default <T1> T1[] toArray(T1[] a) {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default boolean remove(@Nullable Object o) {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default boolean containsAll(Collection<?> c) {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default boolean addAll(Collection<? extends T> c) {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default boolean removeAll(Collection<?> c) {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

		@Override
		default boolean retainAll(Collection<?> c) {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}
	}

	/**
	 * Base class for synchronous sources which have fixed size and can
	 * emit their items in a pull fashion, thus avoiding the request-accounting
	 * overhead in many cases.
	 *
	 * @param <T> the content value type
	 */
	interface SynchronousSubscription<T> extends QueueSubscription<T> {

		@Override
		default int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.SYNC) != 0) {
				return Fuseable.SYNC;
			}
			return NONE;
		}

	}

	/**
	 * Marker interface indicating that the target can return a value or null,
	 * otherwise fail immediately and thus a viable target for assembly-time
	 * optimizations.
	 *
	 * @param <T> the value type returned
	 */
	interface ScalarCallable<T> extends Callable<T> { }
}