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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.util.concurrent.QueueSupplier;

/**
 * A Processor implementation that takes a custom queue and allows
 * only a single subscriber.
 * 
 * <p>
 * The implementation keeps the order of signals.
 *
 * @param <T> the input and output type
 */
public final class UnicastProcessor<T>
		extends FluxProcessor<T, T>
		implements Fuseable.QueueSubscription<T>, Fuseable, Producer, Receiver {

	/**
	 * Create a unicast {@link FluxProcessor} that will buffer on a given queue in an
	 * unbounded fashion.
	 *
	 * @param <T> the relayed type
	 * @return a unicast {@link FluxProcessor}
	 */
	public static <T> UnicastProcessor<T> create() {
		return create(QueueSupplier.<T>unbounded().get());
	}

	/**
	 * Create a unicast {@link FluxProcessor} that will buffer on a given queue in an
	 * unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param <T> the relayed type
	 * @return a unicast {@link FluxProcessor}
	 */
	public static <T> UnicastProcessor<T> create(Queue<T> queue) {
		return new UnicastProcessor<>(queue);
	}

	/**
	 * Create a unicast {@link FluxProcessor} that will buffer on a given queue in an
	 * unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param endcallback called on any terminal signal
	 * @param <T> the relayed type
	 * @return a unicast {@link FluxProcessor}
	 */
	public static <T> UnicastProcessor<T> create(Queue<T> queue, Disposable endcallback) {
		return new UnicastProcessor<>(queue, endcallback);
	}

	/**
	 * Create a unicast {@link FluxProcessor} that will buffer on a given queue in an
	 * unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param endcallback called on any terminal signal
	 * @param onOverflow called when queue.offer return false and unicastProcessor is
	 * about to emit onError.
	 * @param <T> the relayed type
	 *
	 * @return a unicast {@link FluxProcessor}
	 */
	public static <T> UnicastProcessor<T> create(Queue<T> queue,
			Consumer<? super T> onOverflow,
			Disposable endcallback) {
		return new UnicastProcessor<>(queue, onOverflow, endcallback);
	}

	final Queue<T>            queue;
	final Consumer<? super T> onOverflow;

	volatile Disposable onTerminate;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<UnicastProcessor, Disposable> ON_TERMINATE =
			AtomicReferenceFieldUpdater.newUpdater(UnicastProcessor.class, Disposable.class, "onTerminate");

	volatile boolean done;
	Throwable error;

	volatile Subscriber<? super T> actual;

	volatile boolean cancelled;

	volatile int once;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<UnicastProcessor> ONCE =
			AtomicIntegerFieldUpdater.newUpdater(UnicastProcessor.class, "once");

	volatile int wip;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<UnicastProcessor> WIP =
			AtomicIntegerFieldUpdater.newUpdater(UnicastProcessor.class, "wip");

	volatile long requested;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<UnicastProcessor> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(UnicastProcessor.class, "requested");

	volatile boolean outputFused;

	public UnicastProcessor(Queue<T> queue) {
		this.queue = Objects.requireNonNull(queue, "queue");
		this.onTerminate = null;
		this.onOverflow = null;
	}

	public UnicastProcessor(Queue<T> queue, Disposable onTerminate) {
		this.queue = Objects.requireNonNull(queue, "queue");
		this.onTerminate = Objects.requireNonNull(onTerminate, "onTerminate");
		this.onOverflow = null;
	}

	public UnicastProcessor(Queue<T> queue,
			Consumer<? super T> onOverflow,
			Disposable onTerminate) {
		this.queue = Objects.requireNonNull(queue, "queue");
		this.onOverflow = Objects.requireNonNull(onOverflow, "onOverflow");
		this.onTerminate = Objects.requireNonNull(onTerminate, "onTerminate");
	}

	void doTerminate() {
		Disposable r = onTerminate;
		if (r != null && ON_TERMINATE.compareAndSet(this, r, null)) {
			r.dispose();
		}
	}

	void drainRegular(Subscriber<? super T> a) {
		int missed = 1;

		final Queue<T> q = queue;

		for (;;) {

			long r = requested;
			long e = 0L;

			while (r != e) {
				boolean d = done;

				T t = q.poll();
				boolean empty = t == null;

				if (checkTerminated(d, empty, a, q)) {
					return;
				}

				if (empty) {
					break;
				}

				a.onNext(t);

				e++;
			}

			if (r == e) {
				if (checkTerminated(done, q.isEmpty(), a, q)) {
					return;
				}
			}

			if (e != 0 && r != Long.MAX_VALUE) {
				REQUESTED.addAndGet(this, -e);
			}

			missed = WIP.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	void drainFused(Subscriber<? super T> a) {
		int missed = 1;

		final Queue<T> q = queue;

		for (;;) {

			if (cancelled) {
				q.clear();
				actual = null;
				return;
			}

			boolean d = done;

			a.onNext(null);

			if (d) {
				actual = null;

				Throwable ex = error;
				if (ex != null) {
					a.onError(ex);
				} else {
					a.onComplete();
				}
				return;
			}

			missed = WIP.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	void drain() {
		if (WIP.getAndIncrement(this) != 0) {
			return;
		}

		int missed = 1;

		for (;;) {
			Subscriber<? super T> a = actual;
			if (a != null) {

				if (outputFused) {
					drainFused(a);
				} else {
					drainRegular(a);
				}
				return;
			}

			missed = WIP.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a, Queue<T> q) {
		if (cancelled) {
			q.clear();
			actual = null;
			return true;
		}
		if (d && empty) {
			Throwable e = error;
			actual = null;
			if (e != null) {
				a.onError(e);
			} else {
				a.onComplete();
			}
			return true;
		}

		return false;
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (done || cancelled) {
			s.cancel();
		} else {
			s.request(Long.MAX_VALUE);
		}
	}

	@Override
	public long getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void onNext(T t) {
		if (done || cancelled) {
			//Exceptions.onNextDropped(t);
			return;
		}

		if (!queue.offer(t)) {
			Throwable ex = Operators.onOperatorError(null,
					Exceptions.failWithOverflow(), t);
			if(onOverflow != null) {
				try {
					onOverflow.accept(t);
				}
				catch (Throwable e) {
					Exceptions.throwIfFatal(e);
					ex.initCause(e);
				}
			}
			onError(Operators.onOperatorError(null, ex, t));
			return;
		}
		drain();
	}

	@Override
	public void onError(Throwable t) {
		if (done || cancelled) {
			Operators.onErrorDropped(t);
			return;
		}

		error = t;
		done = true;

		doTerminate();

		drain();
	}

	@Override
	public void onComplete() {
		if (done || cancelled) {
			return;
		}

		done = true;

		doTerminate();

		drain();
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {

			s.onSubscribe(this);
			actual = s;
			if (cancelled) {
				actual = null;
			} else {
				drain();
			}
		} else {
			Operators.error(s, new IllegalStateException("UnicastProcessor " +
					"allows only a single Subscriber"));
		}
	}

	@Override
	public void request(long n) {
		if (Operators.validate(n)) {
			Operators.getAndAddCap(REQUESTED, this, n);
			drain();
		}
	}

	@Override
	public void cancel() {
		if (cancelled) {
			return;
		}
		cancelled = true;

		doTerminate();

		if (!outputFused) {
			if (WIP.getAndIncrement(this) == 0) {
				queue.clear();
			}
		}
	}

	@Override
	public T poll() {
		return queue.poll();
	}

	@Override
	public int size() {
		return queue.size();
	}

	@Override
	public boolean isEmpty() {
		return queue.isEmpty();
	}

	@Override
	public void clear() {
		queue.clear();
	}

	@Override
	public int requestFusion(int requestedMode) {
		if ((requestedMode & Fuseable.ASYNC) != 0) {
			outputFused = true;
			return Fuseable.ASYNC;
		}
		return Fuseable.NONE;
	}

	@Override
	public boolean isCancelled() {
		return cancelled;
	}

	@Override
	public boolean isStarted() {
		return once == 1 && !done && !cancelled;
	}

	@Override
	public boolean isTerminated() {
		return done;
	}

	@Override
	public Throwable getError() {
		return error;
	}

	@Override
	public Object downstream() {
		return actual;
	}

	@Override
	public Object upstream() {
		return onTerminate;
	}

	@Override
	public long getCapacity() {
		return Integer.MAX_VALUE;
	}

	@Override
	public long requestedFromDownstream() {
		return requested;
	}
}