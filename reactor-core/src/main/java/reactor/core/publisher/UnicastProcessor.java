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
import reactor.util.concurrent.QueueSupplier;
import reactor.util.context.Context;
import javax.annotation.Nullable;

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
		implements Fuseable.QueueSubscription<T>, Fuseable, InnerOperator<T, T> {

	/**
	 * {@link UnicastProcessor} builder that can be used to create new
	 * processors. Instantiate it through the {@link UnicastProcessor#builder()} static
	 * method:
	 * <p>
	 * {@code UnicastProcessor<String> processor = UnicastProcessor.<String>builder().build()}
	 *
	 * @param <T> Type of dispatched signal
	 */
	public final static class Builder<T> {

		static final Disposable NOOP_DISPOSABLE = () -> {};

		Queue<T> queue;
		Disposable onTerminate;
		Consumer<? super T> onOverflow;

		Builder() {
			this.onTerminate = NOOP_DISPOSABLE;
			this.onOverflow = t -> {};
		}

		/**
		 * Configures queue for this builder. Unbounded queue is used by default.
		 * If the provided <code>queue</code> is null, default unbounded queue is used.
		 * @param queue the buffering queue
		 * @return builder with provided queue
		 */
		public Builder<T> queue(@Nullable Queue<T> queue) {
			this.queue = queue;
			return this;
		}

		/**
		 * Configures onTerminate callback for this builder.
		 * @param onTerminate called on any terminal signal
		 * @return builder with provided onTerminate callback
		 */
		public Builder<T> onTerminate(Disposable onTerminate) {
			this.onTerminate = onTerminate;
			return this;
		}

		/**
		 * Configures onOverflow callback for this builder.
		 * @param onOverflow called when queue.offer return false and unicastProcessor is about to emit onError.
		 * @return builder with provided onOverflow callback
		 */
		public Builder<T> onOverflow(Consumer<? super T> onOverflow) {
			this.onOverflow = onOverflow;
			return this;
		}

		/**
		 * Creates a new {@link UnicastProcessor} using the properties
		 * of this builder.
		 * @return a fresh processor
		 */
		public UnicastProcessor<T>  build() {
			Queue<T> queue = this.queue != null ? this.queue : QueueSupplier.<T>unbounded().get();
			return new UnicastProcessor<T>(queue, onOverflow, onTerminate);
		}
	}

	/**
	 * Create a new {@link UnicastProcessor} {@link Builder} with default properties.
	 * @return new UnicastProcessor builder
	 */
	public static <E> Builder<E> builder()  {
		return new Builder<>();
	}

	/**
	 * Create a unicast {@link FluxProcessor} that will buffer on a given queue in an
	 * unbounded fashion.
	 *
	 * @param <E> the relayed type
	 * @return a unicast {@link FluxProcessor}
	 */
	public static <E> UnicastProcessor<E> create() {
		return UnicastProcessor.<E>builder().build();
	}

	/**
	 * Create a unicast {@link FluxProcessor} that will buffer on a given queue in an
	 * unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param <E> the relayed type
	 * @return a unicast {@link FluxProcessor}
	 * @deprecated use the Builder ({@link #builder()} and its {@link Builder#build()} method)
	 */
	@Deprecated
	public static <E> UnicastProcessor<E> create(Queue<E> queue) {
		return UnicastProcessor.<E>builder().queue(queue).build();
	}

	/**
	 * Create a unicast {@link FluxProcessor} that will buffer on a given queue in an
	 * unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param endcallback called on any terminal signal
	 * @param <E> the relayed type
	 * @return a unicast {@link FluxProcessor}
	 * @deprecated use {@link Builder#build()}
	 */
	@Deprecated
	public static <E> UnicastProcessor<E> create(Queue<E> queue, Disposable endcallback) {
		return UnicastProcessor.<E>builder().queue(queue).onTerminate(endcallback).build();
	}

	/**
	 * Create a unicast {@link FluxProcessor} that will buffer on a given queue in an
	 * unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param endcallback called on any terminal signal
	 * @param onOverflow called when queue.offer return false and unicastProcessor is
	 * about to emit onError.
	 * @param <E> the relayed type
	 *
	 * @return a unicast {@link FluxProcessor}
	 * @deprecated use {@link Builder#build()}
	 */
	@Deprecated
	public static <E> UnicastProcessor<E> create(Queue<E> queue,
			Consumer<? super E> onOverflow,
			Disposable endcallback) {
		return UnicastProcessor.<E>builder().queue(queue).onOverflow(onOverflow).onTerminate(endcallback).build();
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
	public int getPrefetch() {
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
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		//noinspection ConstantConditions
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
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
	@Nullable
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
	public boolean isDisposed() {
		return cancelled || done;
	}

	@Override
	public boolean isTerminated() {
		return done;
	}

	@Override
	@Nullable
	public Throwable getError() {
		return error;
	}

	@Override
	public Subscriber<? super T> actual() {
		return actual;
	}

	@Override
	public long downstreamCount() {
		return hasDownstreams() ? 1L : 0L;
	}

	@Override
	public boolean hasDownstreams() {
		return actual != null;
	}
}
