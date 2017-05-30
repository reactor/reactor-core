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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.concurrent.QueueSupplier;

import static reactor.core.publisher.FluxPublish.PublishSubscriber.EMPTY;
import static reactor.core.publisher.FluxPublish.PublishSubscriber.TERMINATED;

/**
 * * An implementation of a RingBuffer backed message-passing Processor implementing
 * publish-subscribe with synchronous (thread-stealing and happen-before interactions)
 * drain loops.
 * <p>
 * The default {@link #create} factories will only produce the new elements observed in
 * the parent sequence after a given {@link Subscriber} is subscribed.
 * <p>
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.0.M1/src/docs/marble/emitter.png"
 * alt="">
 * <p>
 *
 * @param <T> the input and output value type
 *
 * @author Stephane Maldini
 */
public final class EmitterProcessor<T> extends FluxProcessor<T, T> {

	/**
	 * {@link EmitterProcessor} builder that can be used to create new
	 * processors.
	 *
	 * @param <T> Type of dispatched signal
	 */
	public final static class Builder<T> {
		boolean autoCancel;
		int bufferSize;

		/**
		 * Creates a new {@link EmitterProcessor} builder with default properties.
		 * @return new EmitterProcessor builder
		 */
		public static <T> Builder<T> create()  {
			return new Builder<T>();
		}

		Builder() {
			this.autoCancel = true;
			this.bufferSize = QueueSupplier.SMALL_BUFFER_SIZE;
		}

		/**
		 * Configures buffer size for this builder. Default value is {@link QueueSupplier#SMALL_BUFFER_SIZE}.
		 * @param bufferSize the internal buffer size to hold signals
		 * @return builder with provided buffer size
		 */
		public Builder<T> bufferSize(int bufferSize) {
			if (bufferSize < 1){
				throw new IllegalArgumentException("bufferSize must be strictly positive, " +
						"was: "+bufferSize);
			}
			this.bufferSize = bufferSize;
			return this;
		}

		/**
		 * Configures auto-cancel for this builder. Default value is true.
		 * @param autoCancel automatically cancel
		 * @return builder with provided auto-cancel
		 */
		public Builder<T> autoCancel(boolean autoCancel) {
			this.autoCancel = autoCancel;
			return this;
		}

		/**
		 * Creates a new {@link EmitterProcessor} using the properties
		 * of this builder.
		 * @return a fresh processor
		 */
		public EmitterProcessor<T> build() {
			return new EmitterProcessor<>(autoCancel, bufferSize);
		}
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE}
	 * backlog size, blockingWait Strategy and auto-cancel.
	 *
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create() {
		return Builder.<E>create().build();
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE}
	 * backlog size, blockingWait Strategy and auto-cancel.
	 *
	 * @param <E> Type of processed signals
	 * @param autoCancel automatically cancel
	 *
	 * @return a fresh processor
	 * @deprecated use {@link Builder#build()}
	 */
	@Deprecated
	public static <E> EmitterProcessor<E> create(boolean autoCancel) {
		return Builder.<E>create().autoCancel(autoCancel).build();
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE}
	 * backlog size, blockingWait Strategy and auto-cancel.
	 *
	 * @param <E> Type of processed signals
	 * @param bufferSize the internal buffer size to hold signals
	 *
	 * @return a fresh processor
	 * @deprecated use {@link Builder#build()}
	 */
	@Deprecated
	public static <E> EmitterProcessor<E> create(int bufferSize) {
		return Builder.<E>create().bufferSize(bufferSize).build();
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE}
	 * backlog size, blockingWait Strategy and auto-cancel.
	 *
	 * @param <E> Type of processed signals
	 * @param bufferSize the internal buffer size to hold signals
	 * @param autoCancel automatically cancel
	 *
	 * @return a fresh processor
	 * @deprecated use {@link Builder#build()}
	 */
	@Deprecated
	public static <E> EmitterProcessor<E> create(int bufferSize, boolean autoCancel) {
		return Builder.<E>create().bufferSize(bufferSize).autoCancel(autoCancel).build();
	}

	final int prefetch;

	final boolean autoCancel;

	volatile Subscription s;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(EmitterProcessor.class,
					Subscription.class,
					"s");

	volatile FluxPublish.PubSubInner<T>[] subscribers;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, FluxPublish.PubSubInner[]>
			SUBSCRIBERS = AtomicReferenceFieldUpdater.newUpdater(EmitterProcessor.class,
			FluxPublish.PubSubInner[].class,
			"subscribers");

	@SuppressWarnings("unused")
	volatile int wip;

	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<EmitterProcessor> WIP =
			AtomicIntegerFieldUpdater.newUpdater(EmitterProcessor.class, "wip");

	volatile Queue<T> queue;

	int sourceMode;

	volatile boolean done;

	volatile Throwable error;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, Throwable> ERROR =
			AtomicReferenceFieldUpdater.newUpdater(EmitterProcessor.class,
					Throwable.class,
					"error");

	EmitterProcessor(boolean autoCancel, int prefetch) {
		if (prefetch < 1) {
			throw new IllegalArgumentException("bufferSize must be strictly positive, " + "was: " + prefetch);
		}
		this.autoCancel = autoCancel;
		this.prefetch = prefetch;
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
		EmitterInner<T> inner = new EmitterInner<>(s, this);
		s.onSubscribe(inner);

		if (inner.isCancelled()) {
			return;
		}

		if (add(inner)) {
			if (inner.isCancelled()) {
				remove(inner);
			}
			drain();
		}
		else {
			Throwable e = error;
			if (e != null) {
				inner.actual.onError(e);
			}
			else {
				inner.actual.onComplete();
			}
		}
	}

	/**
	 * Return the number of parked elements in the emitter backlog.
	 *
	 * @return the number of parked elements in the emitter backlog.
	 */
	public int getPending() {
		Queue<T> q = queue;
		return q != null ? q.size() : 0;
	}

	@Override
	public void onSubscribe(final Subscription s) {
		if (Operators.setOnce(S, this, s)) {
			if (s instanceof Fuseable.QueueSubscription) {
				@SuppressWarnings("unchecked") Fuseable.QueueSubscription<T> f =
						(Fuseable.QueueSubscription<T>) s;

				int m = f.requestFusion(Fuseable.ANY);
				if (m == Fuseable.SYNC) {
					sourceMode = m;
					queue = f;
					done = true;
					drain();
					return;
				}
				else if (m == Fuseable.ASYNC) {
					sourceMode = m;
					queue = f;
					s.request(prefetch);
					return;
				}
			}

			queue = QueueSupplier.<T>get(prefetch).get();

			s.request(prefetch);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onNext(T t) {
		if (t == null && sourceMode == Fuseable.NONE) {
			throw Exceptions.argumentIsNullException();
		}

		if (done) {
			return;
		}
		if (sourceMode == Fuseable.ASYNC) {
			drain();
			return;
		}

		Queue<T> q = queue;

		if (q == null) {
			if (Operators.setOnce(S, this, Operators.emptySubscription())) {
				q = QueueSupplier.<T>get(prefetch).get();
				queue = q;
			}
			else {
				for (; ; ) {
					if (isDisposed()) {
						return;
					}
					q = queue;
					if (q != null) {
						break;
					}
				}
			}
		}

		while (!q.offer(t)) {
			LockSupport.parkNanos(10);
		}
		drain();
	}

	@Override
	public void onError(Throwable t) {
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}
		if (done) {
			Operators.onErrorDropped(t);
			return;
		}
		if (Exceptions.addThrowable(ERROR, this, t)) {
			done = true;
			drain();
		}
		else {
			Operators.onErrorDropped(t);
		}
	}

	@Override
	public void onComplete() {
		if (done) {
			return;
		}
		done = true;
		drain();
	}

	@Override
	public Throwable getError() {
		return error;
	}

	/**
	 * Return whether has all subcribers cancelled and {@link #create(boolean)} auto
	 * shutdown argument
	 * been set to true.
	 *
	 * @return whether has all subcribers cancelled and {@link #create(boolean)} auto
	 * shutdown argument been set to true.
	 */
	public boolean isCancelled() {
		return Operators.cancelledSubscription() == s;
	}

	@Override
	final public int getBufferSize() {
		return prefetch;
	}

	@Override
	public boolean isTerminated() {
		return done && getPending() == 0;
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == ScannableAttr.PARENT) return s;
		if (key == IntAttr.BUFFERED) return getPending();
		if (key == BooleanAttr.CANCELLED) return isCancelled();

		return super.scanUnsafe(key);
	}

	final void drain() {
		if (WIP.getAndIncrement(this) != 0) {
			return;
		}

		int missed = 1;

		for (; ; ) {

			boolean d = done;

			Queue<T> q = queue;

			boolean empty = q == null || q.isEmpty();

			if (checkTerminated(d, empty)) {
				return;
			}

			if (!empty) {
				FluxPublish.PubSubInner<T>[] a = subscribers;
				long maxRequested = Long.MAX_VALUE;

				int len = a.length;
				int cancel = 0;

				for (FluxPublish.PubSubInner<T> inner : a) {
					long r = inner.requested;
					if (r >= 0L) {
						maxRequested = Math.min(maxRequested, r);
					}
					else { //Long.MIN == PublishInner.CANCEL_REQUEST
						cancel++;
					}
				}

				if (len == cancel) {
					T v;

					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						Exceptions.addThrowable(ERROR,
								this,
								Operators.onOperatorError(s, ex));
						d = true;
						v = null;
					}
					if (checkTerminated(d, v == null)) {
						return;
					}
					if (sourceMode != Fuseable.SYNC) {
						s.request(1);
					}
					continue;
				}

				int e = 0;

				while (e < maxRequested && cancel != Integer.MIN_VALUE) {
					d = done;
					T v;

					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						Exceptions.addThrowable(ERROR,
								this,
								Operators.onOperatorError(s, ex));
						d = true;
						v = null;
					}

					empty = v == null;

					if (checkTerminated(d, empty)) {
						return;
					}

					if (empty) {
						break;
					}

					for (FluxPublish.PubSubInner<T> inner : a) {
						inner.actual.onNext(v);
						if (FluxPublish.PubSubInner.produced(inner,
								1) == FluxPublish.PublishInner.CANCEL_REQUEST) {
							cancel = Integer.MIN_VALUE;
						}
					}

					e++;
				}

				if (e != 0 && sourceMode != Fuseable.SYNC) {
					s.request(e);
				}

				if (maxRequested != 0L && !empty) {
					continue;
				}
			}

			missed = WIP.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	@SuppressWarnings("unchecked")
	FluxPublish.PubSubInner<T>[] terminate() {
		return SUBSCRIBERS.getAndSet(this, TERMINATED);
	}

	boolean checkTerminated(boolean d, boolean empty) {
		if (s == Operators.cancelledSubscription()) {
			if (autoCancel) {
				terminate();
				Queue<T> q = queue;
				if (q != null) {
					q.clear();
				}
			}
			return true;
		}
		if (d) {
			Throwable e = error;
			if (e != null && e != Exceptions.TERMINATED) {
				Queue<T> q = queue;
				if (q != null) {
					q.clear();
				}
				for (FluxPublish.PubSubInner<T> inner : terminate()) {
					inner.actual.onError(e);
				}
				return true;
			}
			else if (empty) {
				for (FluxPublish.PubSubInner<T> inner : terminate()) {
					inner.actual.onComplete();
				}
				return true;
			}
		}
		return false;
	}

	final boolean add(EmitterInner<T> inner) {
		for (; ; ) {
			FluxPublish.PubSubInner<T>[] a = subscribers;
			if (a == TERMINATED) {
				return false;
			}
			int n = a.length;
			FluxPublish.PubSubInner<?>[] b = new FluxPublish.PubSubInner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = inner;
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return true;
			}
		}
	}

	final void remove(FluxPublish.PubSubInner<T> inner) {
		for (; ; ) {
			FluxPublish.PubSubInner<T>[] a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			int n = a.length;
			int j = -1;
			for (int i = 0; i < n; i++) {
				if (a[i] == inner) {
					j = i;
					break;
				}
			}

			if (j < 0) {
				return;
			}

			FluxPublish.PubSubInner<?>[] b;
			if (n == 1) {
				b = EMPTY;
			}
			else {
				b = new FluxPublish.PubSubInner<?>[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				if (autoCancel && b == EMPTY && Operators.terminate(S, this)) {
					if (WIP.getAndIncrement(this) != 0) {
						return;
					}
					terminate();
					Queue<T> q = queue;
					if (q != null) {
						q.clear();
					}
				}
			}
			return;
		}
	}

	@Override
	public long downstreamCount() {
		return subscribers.length;
	}

	static final class EmitterInner<T> extends FluxPublish.PubSubInner<T> {

		final EmitterProcessor<T> parent;

		EmitterInner(Subscriber<? super T> actual, EmitterProcessor<T> parent) {
			super(actual);
			this.parent = parent;
		}

		@Override
		void drainParent() {
			parent.drain();
		}

		@Override
		void removeAndDrainParent() {
			parent.remove(this);
			parent.drain();
		}
	}


}
