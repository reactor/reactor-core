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

package reactor.core.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static reactor.core.publisher.FluxPublish.PublishSubscriber.TERMINATED;

/**
 * An implementation of a message-passing Processor implementing
 * publish-subscribe with synchronous (thread-stealing and happen-before interactions)
 * drain loops.
 * <p>
 * The default {@link #create} factories will only produce the new elements observed in
 * the parent sequence after a given {@link Subscriber} is subscribed.
 * <p>
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/emitter.png"
 * alt="">
 * <p>
 *
 * @param <T> the input and output value type
 *
 * @author Stephane Maldini
 * @deprecated To be removed in 3.5. Prefer clear cut usage of {@link Sinks} through
 * variations of {@link Sinks.MulticastSpec#onBackpressureBuffer() Sinks.many().multicast().onBackpressureBuffer()}.
 * This processor was blocking in {@link EmitterProcessor#onNext(Object)}.
 * This behaviour can be implemented with the {@link Sinks} API by calling
 * {@link Sinks.Many#tryEmitNext(Object)} and retrying, e.g.:
 * <pre>{@code while (sink.tryEmitNext(v).hasFailed()) {
 *     LockSupport.parkNanos(10);
 * }
 * }</pre>
 */
@Deprecated
public final class EmitterProcessor<T> extends FluxProcessor<T, T> implements InternalManySink<T> {

	@SuppressWarnings("rawtypes")
	static final FluxPublish.PubSubInner[] EMPTY = new FluxPublish.PublishInner[0];

	/**
	 * Create a new {@link EmitterProcessor} using {@link Queues#SMALL_BUFFER_SIZE}
	 * backlog size and auto-cancel.
	 *
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 * @deprecated use {@link Sinks.MulticastSpec#onBackpressureBuffer() Sinks.many().multicast().onBackpressureBuffer()}
	 * (or the unsafe variant if you're sure about external synchronization). To be removed in 3.5.
	 */
	@Deprecated
	public static <E> EmitterProcessor<E> create() {
		return create(Queues.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link Queues#SMALL_BUFFER_SIZE}
	 * backlog size and the provided auto-cancel.
	 *
	 * @param <E> Type of processed signals
	 * @param autoCancel automatically cancel
	 *
	 * @return a fresh processor
	 * @deprecated use {@link Sinks.MulticastSpec#onBackpressureBuffer(int, boolean) Sinks.many().multicast().onBackpressureBuffer(bufferSize, boolean)}
	 * using the old default of {@link Queues#SMALL_BUFFER_SIZE} for the {@code bufferSize}
	 * (or the unsafe variant if you're sure about external synchronization). To be removed in 3.5.
	 */
	@Deprecated
	public static <E> EmitterProcessor<E> create(boolean autoCancel) {
		return create(Queues.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link EmitterProcessor} using the provided backlog size, with auto-cancel.
	 *
	 * @param <E> Type of processed signals
	 * @param bufferSize the internal buffer size to hold signals
	 *
	 * @return a fresh processor
	 * @deprecated use {@link Sinks.MulticastSpec#onBackpressureBuffer(int) Sinks.many().multicast().onBackpressureBuffer(bufferSize)}
	 * (or the unsafe variant if you're sure about external synchronization). To be removed in 3.5.
	 */
	@Deprecated
	public static <E> EmitterProcessor<E> create(int bufferSize) {
		return create(bufferSize, true);
	}

	/**
	 * Create a new {@link EmitterProcessor} using the provided backlog size and auto-cancellation.
	 *
	 * @param <E> Type of processed signals
	 * @param bufferSize the internal buffer size to hold signals
	 * @param autoCancel automatically cancel
	 *
	 * @return a fresh processor
	 * @deprecated use {@link Sinks.MulticastSpec#onBackpressureBuffer(int, boolean) Sinks.many().multicast().onBackpressureBuffer(bufferSize, autoCancel)}
	 * (or the unsafe variant if you're sure about external synchronization). To be removed in 3.5.
	 */
	@Deprecated
	public static <E> EmitterProcessor<E> create(int bufferSize, boolean autoCancel) {
		return new EmitterProcessor<>(autoCancel, bufferSize);
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
		//doesn't use INIT/CANCELLED distinction, contrary to FluxPublish)
		//see remove()
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	@Override
	public Context currentContext() {
		return Operators.multiSubscribersContext(subscribers);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Objects.requireNonNull(actual, "subscribe");
		EmitterInner<T> inner = new EmitterInner<>(actual, this);
		actual.onSubscribe(inner);

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

	@Override
	public void onComplete() {
		//no particular error condition handling for onComplete
		@SuppressWarnings("unused") EmitResult emitResult = tryEmitComplete();
	}

	@Override
	public EmitResult tryEmitComplete() {
		if (done) {
			return EmitResult.FAIL_TERMINATED;
		}
		done = true;
		drain();
		return EmitResult.OK;
	}

	@Override
	public void onError(Throwable throwable) {
		emitError(throwable, Sinks.EmitFailureHandler.FAIL_FAST);
	}

	@Override
	public EmitResult tryEmitError(Throwable t) {
		Objects.requireNonNull(t, "onError");
		if (done) {
			return EmitResult.FAIL_TERMINATED;
		}
		if (Exceptions.addThrowable(ERROR, this, t)) {
			done = true;
			drain();
			return EmitResult.OK;
		}
		else {
			return Sinks.EmitResult.FAIL_TERMINATED;
		}
	}

	@Override
	public void onNext(T t) {
		if (sourceMode == Fuseable.ASYNC) {
			drain();
			return;
		}
		emitNext(t, Sinks.EmitFailureHandler.FAIL_FAST);
	}

	@Override
	public EmitResult tryEmitNext(T t) {
		if (done) {
			return Sinks.EmitResult.FAIL_TERMINATED;
		}

		Objects.requireNonNull(t, "onNext");

		Queue<T> q = queue;

		if (q == null) {
			if (Operators.setOnce(S, this, Operators.emptySubscription())) {
				q = Queues.<T>get(prefetch).get();
				queue = q;
			}
			else {
				for (; ; ) {
					if (isCancelled()) {
						return EmitResult.FAIL_CANCELLED;
					}
					q = queue;
					if (q != null) {
						break;
					}
				}
			}
		}

		if (!q.offer(t)) {
			return subscribers == EMPTY ? EmitResult.FAIL_ZERO_SUBSCRIBER : EmitResult.FAIL_OVERFLOW;
		}
		drain();
		return EmitResult.OK;
	}

	@Override
	public int currentSubscriberCount() {
		return subscribers.length;
	}

	@Override
	public Flux<T> asFlux() {
		return this;
	}

	@Override
	protected boolean isIdentityProcessor() {
		return true;
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
	public boolean isDisposed() {
		return isTerminated() || isCancelled();
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
					drain();
					return;
				}
				else if (m == Fuseable.ASYNC) {
					sourceMode = m;
					queue = f;
					s.request(Operators.unboundedOrPrefetch(prefetch));
					return;
				}
			}

			queue = Queues.<T>get(prefetch).get();

			s.request(Operators.unboundedOrPrefetch(prefetch));
		}
	}

	@Override
	@Nullable
	public Throwable getError() {
		return error;
	}

	/**
	 * @return true if all subscribers have actually been cancelled and the processor auto shut down
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
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return s;
		if (key == Attr.BUFFERED) return getPending();
		if (key == Attr.CANCELLED) return isCancelled();
		if (key == Attr.PREFETCH) return getPrefetch();

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

			FluxPublish.PubSubInner<T>[] a = subscribers;

			if (a != EMPTY && !empty) {
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
								this, Operators.onOperatorError(s, ex, currentContext()));
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
								this, Operators.onOperatorError(s, ex, currentContext()));
						d = true;
						v = null;
					}

					empty = v == null;

					if (checkTerminated(d, empty)) {
						return;
					}

					if (empty) {
						//async mode only needs to break but SYNC mode needs to perform terminal cleanup here...
						if (sourceMode == Fuseable.SYNC) {
							//the q is empty
							done = true;
							checkTerminated(true, true);
						}
						break;
					}

					for (FluxPublish.PubSubInner<T> inner : a) {
						inner.actual.onNext(v);
						if (Operators.producedCancellable(FluxPublish
										.PublishInner.REQUESTED, inner,
								1) == Long.MIN_VALUE) {
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
			else if ( sourceMode == Fuseable.SYNC ) {
				done = true;
				if (checkTerminated(true, empty)) { //empty can be true if no subscriber
					break;
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
				//contrary to FluxPublish, there is a possibility of auto-cancel, which
				//happens when the removed inner makes the subscribers array EMPTY
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

		EmitterInner(CoreSubscriber<? super T> actual, EmitterProcessor<T> parent) {
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
