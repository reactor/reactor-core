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
package reactor.core.flow;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.fn.Supplier;

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
	 * For example, an {@code asyncSource.map().dispatchOn().subscribe()} sequence where {@code asyncSource}
	 * is async-fuseable, dispatchOn may fuse the whole sequence into a single Queue which invokes the mapper
	 * function on its {@code poll()} method that is on another thread that whereas the unfused sequence
	 * would have invoked the mapper on the previous thread. If such mapper is costly, it would escape its
	 * thread bound this way.
	 */
	int THREAD_BARRIER = 4;

	/**
	 * A subscriber variant that can immediately tell if it consumed
	 * the value or not, avoiding the usual request(1) for dropped
	 * values.
	 *
	 * @param <T> the value type
	 */
	interface ConditionalSubscriber<T> extends Subscriber<T> {
		/**
		 * Try consuming the value and return true if successful.
		 * @param t the value to consume
		 * @return true if consumed, false if dropped and a new value can be immediately sent
		 */
		boolean tryOnNext(T t);
	}

	/**
	 * Contract queue-fusion based optimizations for supporting subscriptions.
	 *
	 * <ul>
	 *  <li>
	 *  Synchronous sources which have fixed size and can
	 *  emit its items in a pull fashion, thus avoiding the request-accounting
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

		/**
		 * Request a specific fusion mode from this QueueSubscription.
		 * <p>
		 * One should request either SYNC, ASYNC or ANY modes (never NONE)
		 * and the implementor should return NONE, SYNC or ASYNC (never ANY).
		 * <p>
		 * For example, if a source supports only ASYNC fusion but
		 * the intermediate operator supports only SYNC fuseable sources,
		 * the operator may request SYNC fusion and the source can reject it via
		 * NONE, thus the operator can return NONE as well to dowstream and the
		 * fusion doesn't happen.
		 *
		 * @param requestedMode the mode to request
		 * @return the fusion mode activated
		 */
		int requestFusion(int requestedMode);

		/**
		 * Requests the upstream to drop the current value.
		 * <p>
		 * This is allows fused intermediate operators to avoid peek/poll pairs.
		 */
		void drop();
	}

	/**
	 * Base class for synchronous sources which have fixed size and can
	 * emit its items in a pull fashion, thus avoiding the request-accounting
	 * overhead in many cases.
	 *
	 * Implementor note: This can be simplified using Java  8 interface features but this source maintains a
	 * JDK7 comp
	 *
	 * @param <T> the content value type
	 */
	abstract class SynchronousSubscription<T> implements QueueSubscription<T>, Queue<T> {
		@Override
		public final boolean offer(T e) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final boolean contains(Object o) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final Iterator<T> iterator() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final Object[] toArray() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final <U> U[] toArray(U[] a) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final boolean remove(Object o) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final boolean containsAll(Collection<?> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final boolean addAll(Collection<? extends T> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final boolean removeAll(Collection<?> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final boolean retainAll(Collection<?> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.SYNC;
		}

		@Override
		public final boolean add(T e) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final T remove() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final T element() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}
	}

	/**
	 * Marker interface indicating that the target can return a value or null
	 * immediately and thus a viable target for assembly-time optimizations.
	 *
	 * @param <T> the value type returned
	 */
	interface ScalarSupplier<T> extends Supplier<T> {

	}
}