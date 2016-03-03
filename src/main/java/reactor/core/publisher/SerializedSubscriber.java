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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.state.Completable;
import reactor.core.state.Introspectable;
import reactor.core.util.BackpressureUtils;

/**
 * Subscriber that makes sure signals are delivered sequentially in case the onNext, onError or onComplete methods are
 * called concurrently.
 * <p>
 * <p>
 * The implementation uses {@code synchronized (this)} to ensure mutual exclusion.
 * <p>
 * <p>
 * Note that the class implements Subscription to save on allocation.
 *
 * @param <T> the value type
 */
final class SerializedSubscriber<T>
		implements Subscriber<T>, Subscription, Receiver,
		           Completable, Producer, Cancellable, Introspectable, Backpressurable {

	final Subscriber<? super T> actual;

	boolean emitting;

	boolean missed;

	volatile boolean done;

	volatile boolean cancelled;

	LinkedArrayNode<T> head;

	LinkedArrayNode<T> tail;

	Throwable error;

	Subscription s;

	/**
	 * Safely gate a subscriber subject concurrent Reactive Stream signals, thus serializing as a single sequence. Note
	 * that serialization uses Thread Stealing and is vulnerable to cpu starving issues.
	 *
	 * @param actual the subscriber to gate
	 * @param <T>
	 *
	 * @return a safe subscriber
	 */
	public static <T> Subscriber<T> create(Subscriber<T> actual) {
		return new SerializedSubscriber<>(actual);
	}

	public SerializedSubscriber(Subscriber<? super T> actual) {
		this.actual = actual;
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (BackpressureUtils.validate(this.s, s)) {
			this.s = s;

			actual.onSubscribe(this);
		}
	}

	@Override
	public void onNext(T t) {
		serOnNext(t);
	}

	@Override
	public void onError(Throwable t) {
		serOnError(t);
	}

	@Override
	public void onComplete() {
		serOnComplete();
	}

	@Override
	public void request(long n) {
		s.request(n);
	}

	@Override
	public void cancel() {
		cancelled = true;
		s.cancel();
	}

	void serAdd(T value) {
		LinkedArrayNode<T> t = serGetTail();

		if (t == null) {
			t = new LinkedArrayNode<>(value);

			serSetHead(t);
			serSetTail(t);
		}
		else {
			if (t.count == LinkedArrayNode.DEFAULT_CAPACITY) {
				LinkedArrayNode<T> n = new LinkedArrayNode<>(value);

				t.next = n;
				serSetTail(n);
			}
			else {
				t.array[t.count++] = value;
			}
		}
	}

	void serDrainLoop(Subscriber<? super T> actual) {
		for (; ; ) {

			if (isCancelled()) {
				return;
			}

			boolean d;
			Throwable e;
			LinkedArrayNode<T> n;

			synchronized (serGuard()) {
				if (isCancelled()) {
					return;
				}

				if (!serIsMissed()) {
					serSetEmitting(false);
					return;
				}

				serSetMissed(false);

				d = isTerminated();
				e = getError();
				n = serGetHead();

				serSetHead(null);
				serSetTail(null);
			}

			while (n != null) {

				T[] arr = n.array;
				int c = n.count;

				for (int i = 0; i < c; i++) {

					if (isCancelled()) {
						return;
					}

					actual.onNext(arr[i]);
				}

				n = n.next;
			}

			if (isCancelled()) {
				return;
			}

			if (e != null) {
				actual.onError(e);
				return;
			}
			else if (d) {
				actual.onComplete();
				return;
			}
		}
	}

	@Override
	public Subscriber<? super T> downstream() {
		return actual;
	}

	Object serGuard() {
		return this;
	}

	boolean serIsEmitting() {
		return emitting;
	}

	void serOnComplete() {
		if (isCancelled() || isTerminated()) {
			return;
		}

		synchronized (this) {
			if (isCancelled() || isTerminated()) {
				return;
			}

			serSetDone(true);

			if (serIsEmitting()) {
				serSetMissed(true);
				return;
			}
		}

		downstream().onComplete();
	}

	void serOnError(Throwable e) {
		if (isCancelled() || isTerminated()) {
			return;
		}

		synchronized (serGuard()) {
			if (isCancelled() || isTerminated()) {
				return;
			}

			serSetDone(true);
			serSetError(e);

			if (serIsEmitting()) {
				serSetMissed(true);
				return;
			}
		}

		downstream().onError(e);
	}

	void serOnNext(T t) {
		if (isCancelled() || isTerminated()) {
			return;
		}

		synchronized (serGuard()) {
			if (isCancelled() || isTerminated()) {
				return;
			}

			if (serIsEmitting()) {
				serAdd(t);
				serSetMissed(true);
				return;
			}

			serSetEmitting(true);
		}

		Subscriber<? super T> actual = downstream();

		actual.onNext(t);

		serDrainLoop(actual);
	}

	void serSetEmitting(boolean emitting) {
		this.emitting = emitting;
	}

	boolean serIsMissed() {
		return missed;
	}

	void serSetMissed(boolean missed) {
		this.missed = missed;
	}

	@Override
	public boolean isCancelled() {
		return cancelled;
	}

	@Override
	public boolean isTerminated() {
		return done;
	}

	void serSetDone(boolean done) {
		this.done = done;
	}

	@Override
	public Throwable getError() {
		return error;
	}

	void serSetError(Throwable error) {
		this.error = error;
	}

	LinkedArrayNode<T> serGetHead() {
		return head;
	}

	void serSetHead(LinkedArrayNode<T> node) {
		head = node;
	}

	LinkedArrayNode<T> serGetTail() {
		return tail;
	}

	void serSetTail(LinkedArrayNode<T> node) {
		tail = node;
	}

	@Override
	public boolean isStarted() {
		return s != null || !cancelled;
	}

	@Override
	public Subscription upstream() {
		return s;
	}

	@Override
	public long getPending() {
		LinkedArrayNode<T> node = serGetTail();
		if (node != null) {
			return node.count;
		}
		return 0;
	}

	@Override
	public int getMode() {
		return 0;
	}


	@Override
	public long getCapacity() {
		return LinkedArrayNode.DEFAULT_CAPACITY;
	}

	/**
	 * Node in a linked array list that is only appended.
	 *
	 * @param <T> the value type
	 */
	static final class LinkedArrayNode<T> {

		static final int DEFAULT_CAPACITY = 16;

		final T[] array;
		int count;

		LinkedArrayNode<T> next;

		@SuppressWarnings("unchecked")
		public LinkedArrayNode(T value) {
			array = (T[]) new Object[DEFAULT_CAPACITY];
			array[0] = value;
			count = 1;
		}
	}
}
