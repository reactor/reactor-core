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

/**
 * A micro API for stream merge, in particular marks producers that support a {@link MergeSubscription}.
 */
public interface Mergeable {

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
	 * Contract queue-merging based optimizations for supporting subscriptions.
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
	interface MergeSubscription<T> extends Subscription {

		/**
		 * An asynchronously producing {@link MergeSubscription} will wait for this signal to switch to a fused-mode.
		 * Its consumer must no longer run its own drain loop and will receive onNext(null) signals to
		 * indicate there is one or maany item(s) available in this {@link #queue queue-view}. This will
		 * evaluate to false result.
		 * <p>
		 * A synchronously producing {@link MergeSubscription} will usually consider this method no-op and
		 * return true to signal its consumer its immediate availability.
		 * <p>
		 * On the receiving side, the method has to be called while the parent is in onSubscribe and before any
		 * other interaction with the Subscription.
		 *
		 * @return {@code false} if asynchronous or {@code true} if immediately ready
		 */
		boolean requestSyncMerge();

		/**
		 * @return the {@link Queue} view of the produced sequence, it's behavior is driven by the type of merge:
		 * sync or async
		 *
		 * @see #requestSyncMerge()
		 */
		Queue<T> queue();

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
	 * Implementor note: This can be simplified using Java 8 interface features but this
	 * source maintains a Java 7+ compatibility.
	 *
	 * @param <T> the content value type
	 */
	abstract class SynchronousSubscription<T> implements MergeSubscription<T>, Queue<T> {
		@Override
		public final boolean offer(T e) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public final int size() {
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
		public boolean requestSyncMerge() {
			return true;
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

		@Override
		public final Queue<T> queue() {
			return this;
		}
	}
}
