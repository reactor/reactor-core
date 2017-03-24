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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

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
final class SerializedSubscriber<T> implements InnerOperator<T, T> {

	final Subscriber<? super T> actual;

	boolean emitting;

	boolean missed;

	volatile boolean done;

	volatile boolean cancelled;

	LinkedArrayNode<T> head;

	LinkedArrayNode<T> tail;

	Throwable error;

	Subscription s;

	SerializedSubscriber(Subscriber<? super T> actual) {
		this.actual = actual;
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (Operators.validate(this.s, s)) {
			this.s = s;

			actual.onSubscribe(this);
		}
	}

	@Override
	public void onNext(T t) {
		if (cancelled || done) {
			return;
		}

		synchronized (this) {
			if (cancelled || done) {
				return;
			}

			if (emitting) {
				serAdd(t);
				missed = true;
				return;
			}

			emitting = true;
		}

		actual.onNext(t);

		serDrainLoop(actual);
	}

	@Override
	public void onError(Throwable t) {
		if (cancelled || done) {
			return;
		}

		synchronized (this) {
			if (cancelled || done) {
				return;
			}

			done = true;
			error = t;

			if (emitting) {
				missed = true;
				return;
			}
		}

		actual.onError(t);
	}

	@Override
	public void onComplete() {
		if (cancelled || done) {
			return;
		}

		synchronized (this) {
			if (cancelled || done) {
				return;
			}

			done = true;

			if (emitting) {
				missed = true;
				return;
			}
		}

		actual.onComplete();
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
		LinkedArrayNode<T> t = tail;

		if (t == null) {
			t = new LinkedArrayNode<>(value);

			head = t;
			tail = t;
		}
		else {
			if (t.count == LinkedArrayNode.DEFAULT_CAPACITY) {
				LinkedArrayNode<T> n = new LinkedArrayNode<>(value);

				t.next = n;
				tail = n ;
			}
			else {
				t.array[t.count++] = value;
			}
		}
	}

	void serDrainLoop(Subscriber<? super T> actual) {
		for (; ; ) {

			if (cancelled) {
				return;
			}

			boolean d;
			Throwable e;
			LinkedArrayNode<T> n;

			synchronized (this) {
				if (cancelled) {
					return;
				}

				if (!missed) {
					emitting = false;
					return;
				}

				missed = false;

				d = done;
				e = error;
				n = head;

				head = null;
				tail = null;
			}

			while (n != null) {

				T[] arr = n.array;
				int c = n.count;

				for (int i = 0; i < c; i++) {

					if (cancelled) {
						return;
					}

					actual.onNext(arr[i]);
				}

				n = n.next;
			}

			if (cancelled) {
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
	public Subscriber<? super T> actual() {
		return actual;
	}

	@Override
	public Object scan(Attr key) {
		switch (key){
			case PARENT:
				return s;
			case ERROR:
				return error;
			case BUFFERED:
				return producerCapacity();
			case CAPACITY:
				return LinkedArrayNode.DEFAULT_CAPACITY;
			case CANCELLED:
				return cancelled;
			case TERMINATED:
				return done;
		}
		return InnerOperator.super.scan(key);
	}

	long producerCapacity() {
		LinkedArrayNode<T> node = tail;
		if(node != null){
			return node.count;
		}
		return 0;
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
		LinkedArrayNode(T value) {
			array = (T[]) new Object[DEFAULT_CAPACITY];
			array[0] = value;
			count = 1;
		}
	}
}
