/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;

/**
 * Abstract base class for subscribers that hold another subscriber, a queue
 * and requires queue-drain behavior.
 *
 * @param <T> the source type to which this subscriber will be subscribed
 * @param <U> the value type in the queue
 * @param <V> the value type the child subscriber accepts
 *
 * @author Simon Baslé
 * @author David Karnok
 */
abstract class QueueDrainSubscriber<T, U, V> extends QueueDrainSubscriberPad4
		implements InnerOperator<T, V> {

	final CoreSubscriber<? super V> actual;
	final Queue<U>              queue;

	volatile boolean cancelled;

	volatile boolean done;
	Throwable error;

	QueueDrainSubscriber(CoreSubscriber<? super V> actual, Queue<U> queue) {
		this.actual = actual;
		this.queue = queue;
	}

	@Override
	public CoreSubscriber<? super V> actual() {
		return actual;
	}

	public final boolean cancelled() {
		return cancelled;
	}

	public final boolean done() {
		return done;
	}

	public final boolean enter() {
		return wip.getAndIncrement() == 0;
	}

	public final boolean fastEnter() {
		return wip.get() == 0 && wip.compareAndSet(0, 1);
	}

	protected final void fastPathEmitMax(U value, boolean delayError, Disposable dispose) {
		final Subscriber<? super V> s = actual;
		final Queue<U> q = queue;

		if (wip.get() == 0 && wip.compareAndSet(0, 1)) {
			long r = requested;
			if (r != 0L) {
				if (accept(s, value)) {
					if (r != Long.MAX_VALUE) {
						produced(1);
					}
				}
				if (leave(-1) == 0) {
					return;
				}
			} else {
				dispose.dispose();
				s.onError(Exceptions.failWithOverflow("Could not emit buffer due to lack of requests"));
				return;
			}
		} else {
			q.offer(value);
			if (!enter()) {
				return;
			}
		}
		drainMaxLoop(q, s, delayError, dispose, this);
	}

	protected final void fastPathOrderedEmitMax(U value, boolean delayError, Disposable dispose) {
		final Subscriber<? super V> s = actual;
		final Queue<U> q = queue;

		if (wip.get() == 0 && wip.compareAndSet(0, 1)) {
			long r = requested;
			if (r != 0L) {
				if (q.isEmpty()) {
					if (accept(s, value)) {
						if (r != Long.MAX_VALUE) {
							produced(1);
						}
					}
					if (leave(-1) == 0) {
						return;
					}
				} else {
					q.offer(value);
				}
			} else {
				cancelled = true;
				dispose.dispose();
				s.onError(Exceptions.failWithOverflow("Could not emit buffer due to lack of requests"));
				return;
			}
		} else {
			q.offer(value);
			if (!enter()) {
				return;
			}
		}
		drainMaxLoop(q, s, delayError, dispose, this);
	}

	/**
	 * Accept the value and return true if forwarded.
	 * @param a the subscriber
	 * @param v the value
	 * @return true if the value was delivered
	 */
	public boolean accept(Subscriber<? super V> a, U v) {
		return false;
	}

	public final Throwable error() {
		return error;
	}

	/**
	 * Adds m to the wip counter.
	 * @param m the value to add
	 * @return the current value after adding m
	 */
	public final int leave(int m) {
		return wip.addAndGet(m);
	}

	public final long requested() {
		return requested;
	}

	public final long produced(long n) {
		return REQUESTED.addAndGet(this, -n);
	}

	public final void requested(long n) {
		if (Operators.validate(n)) {
			Operators.addCap(REQUESTED, this, n);
		}
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED) return done;
		if (key == Attr.CANCELLED) return cancelled;
		if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
		if (key == Attr.ERROR) return error;

		return InnerOperator.super.scanUnsafe(key);
	}

	/**
	 * Drain the queue but give up with an error if there aren't enough requests.
	 * @param <Q> the queue value type
	 * @param <S> the emission value type
	 * @param q the queue
	 * @param a the subscriber
	 * @param delayError true if errors should be delayed after all normal items
	 * @param dispose the disposable to call when termination happens and cleanup is necessary
	 * @param qd the QueueDrain instance that gives status information to the drain logic
	 */
	static <Q, S> void drainMaxLoop(Queue<Q> q, Subscriber<? super S> a, boolean delayError,
			Disposable dispose, QueueDrainSubscriber<?, Q, S> qd) {
		int missed = 1;

		for (;;) {
			for (;;) {
				boolean d = qd.done();

				Q v = q.poll();

				boolean empty = v == null;

				if (checkTerminated(d, empty, a, delayError, q, qd)) {
					if (dispose != null) {
						dispose.dispose();
					}
					return;
				}

				if (empty) {
					break;
				}

				long r = qd.requested();
				if (r != 0L) {
					if (qd.accept(a, v)) {
						if (r != Long.MAX_VALUE) {
							qd.produced(1);
						}
					}
				} else {
					q.clear();
					if (dispose != null) {
						dispose.dispose();
					}
					a.onError(Exceptions.failWithOverflow("Could not emit value due to lack of requests."));
					return;
				}
			}

			missed = qd.leave(-missed);
			if (missed == 0) {
				break;
			}
		}
	}

	static <Q, S> boolean checkTerminated(boolean d, boolean empty,
			Subscriber<?> s, boolean delayError, Queue<?> q, QueueDrainSubscriber<?, Q, S> qd) {
		if (qd.cancelled()) {
			q.clear();
			return true;
		}

		if (d) {
			if (delayError) {
				if (empty) {
					Throwable err = qd.error();
					if (err != null) {
						s.onError(err);
					} else {
						s.onComplete();
					}
					return true;
				}
			} else {
				Throwable err = qd.error();
				if (err != null) {
					q.clear();
					s.onError(err);
					return true;
				} else
				if (empty) {
					s.onComplete();
					return true;
				}
			}
		}

		return false;
	}

}

// -------------------------------------------------------------------
// Padding superclasses
//-------------------------------------------------------------------

/** Pads the header away from other fields. */
class QueueDrainSubscriberPad0 {
	volatile long p1, p2, p3, p4, p5, p6, p7;
	volatile long p8, p9, p10, p11, p12, p13, p14, p15;
}

/** The WIP counter. */
class QueueDrainSubscriberWip extends QueueDrainSubscriberPad0 {
	final AtomicInteger wip = new AtomicInteger();
}

/** Pads away the wip from the other fields. */
class QueueDrainSubscriberPad2 extends QueueDrainSubscriberWip {
	volatile long p1a, p2a, p3a, p4a, p5a, p6a, p7a;
	volatile long p8a, p9a, p10a, p11a, p12a, p13a, p14a, p15a;
}

/** Contains the requested field. */
class QueueDrainSubscriberPad3 extends QueueDrainSubscriberPad2 {

	static final AtomicLongFieldUpdater<QueueDrainSubscriberPad3> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(QueueDrainSubscriberPad3.class, "requested");

	volatile long requested;
}

/** Pads away the requested from the other fields. */
class QueueDrainSubscriberPad4 extends QueueDrainSubscriberPad3 {
	volatile long q1, q2, q3, q4, q5, q6, q7;
	volatile long q8, q9, q10, q11, q12, q13, q14, q15;
}