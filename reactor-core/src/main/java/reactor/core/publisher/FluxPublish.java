/*
 * Copyright (c) 2016-2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A connectable publisher which shares an underlying source and dispatches source values
 * to subscribers in a backpressure-aware manner.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxPublish<T> extends ConnectableFlux<T> implements Scannable {

	/**
	 * The source observable.
	 */
	final Flux<? extends T> source;

	/**
	 * The size of the prefetch buffer.
	 */
	final int prefetch;

	final Supplier<? extends Queue<T>> queueSupplier;

	/**
	 * Whether to prepare for a reconnect after the source terminates.
	 */
	final boolean resetUponSourceTermination;

	volatile PublishSubscriber<T> connection;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<FluxPublish, PublishSubscriber> CONNECTION =
			AtomicReferenceFieldUpdater.newUpdater(FluxPublish.class,
					PublishSubscriber.class,
					"connection");

	FluxPublish(Flux<? extends T> source,
			int prefetch,
			Supplier<? extends Queue<T>> queueSupplier,
			boolean resetUponSourceTermination) {
		if (prefetch <= 0) {
			throw new IllegalArgumentException("bufferSize > 0 required but it was " + prefetch);
		}
		this.source = Flux.from(Objects.requireNonNull(source, "source"));
		this.prefetch = prefetch;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.resetUponSourceTermination = resetUponSourceTermination;
	}

	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		boolean doConnect;
		PublishSubscriber<T> s;
		for (; ; ) {
			s = connection;
			if (s == null || s.isTerminated()) {
				PublishSubscriber<T> u = new PublishSubscriber<>(prefetch, this);

				if (!CONNECTION.compareAndSet(this, s, u)) {
					continue;
				}

				s = u;
			}

 			doConnect = s.tryConnect();
			break;
		}

		cancelSupport.accept(s);
		if (doConnect) {
			source.subscribe(s);
		}
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		PublishInner<T> inner = new PublishInner<>(actual);
		actual.onSubscribe(inner);
		for (; ; ) {
			if (inner.isCancelled()) {
				break;
			}

			PublishSubscriber<T> c = connection;
			if (c == null || (this.resetUponSourceTermination && c.isTerminated())) {
				PublishSubscriber<T> u = new PublishSubscriber<>(prefetch, this);
				if (!CONNECTION.compareAndSet(this, c, u)) {
					continue;
				}

				c = u;
			}

			if (c.add(inner)) {
				if (inner.isCancelled()) {
					c.remove(inner);
				}
				else {
					inner.parent = c;
				}

				c.drainFromInner();
				break;
			}
			else if (!this.resetUponSourceTermination) {
				if (c.error != null) {
					inner.actual.onError(c.error);
				} else {
					inner.actual.onComplete();
				}
				break;
			}
		}
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.PARENT) return source;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		if (key == InternalProducerAttr.INSTANCE) return true;

		return null;
	}

	static final class PublishSubscriber<T>
			implements InnerConsumer<T>, Disposable {

		final int prefetch;

		final FluxPublish<T> parent;

		Subscription s;

		volatile PubSubInner<T>[] subscribers;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<PublishSubscriber, PubSubInner[]> SUBSCRIBERS =
				AtomicReferenceFieldUpdater.newUpdater(PublishSubscriber.class,
						PubSubInner[].class,
						"subscribers");

		volatile long state;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PublishSubscriber> STATE =
				AtomicLongFieldUpdater.newUpdater(PublishSubscriber.class, "state");

		//notes: FluxPublish needs to distinguish INIT from CANCELLED in order to correctly
		//drop values in case of an early connect() without any subscribers.
		@SuppressWarnings("rawtypes")
		static final PubSubInner[] INIT       = new PublishInner[0];
		@SuppressWarnings("rawtypes")
		static final PubSubInner[] CANCELLED  = new PublishInner[0];
		@SuppressWarnings("rawtypes")
		static final PubSubInner[] TERMINATED = new PublishInner[0];

		Queue<T> queue;

		int sourceMode;

		boolean   done;

		volatile Throwable error;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<PublishSubscriber, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(PublishSubscriber.class,
						Throwable.class,
						"error");

		PublishSubscriber(int prefetch, FluxPublish<T> parent) {
			this.prefetch = prefetch;
			this.parent = parent;
			SUBSCRIBERS.lazySet(this, INIT);
		}

		boolean isTerminated(){
			return subscribers == TERMINATED;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof Fuseable.QueueSubscription) {
					@SuppressWarnings("unchecked") Fuseable.QueueSubscription<T> f =
							(Fuseable.QueueSubscription<T>) s;

					int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);
					if (m == Fuseable.SYNC) {
						sourceMode = m;
						queue = f;
						long previousState = markSubscriptionSetAndAddWork(this);
						if (isCancelled(previousState)) {
							s.cancel();
							return;
						}

						if (hasWorkInProgress(previousState)) {
							return;
						}

						drain(previousState | SUBSCRIPTION_SET_FLAG | 1);
						return;
					}

					if (m == Fuseable.ASYNC) {
						sourceMode = m;
						queue = f;
						long previousState = markSubscriptionSet(this);
						if (isCancelled(previousState)) {
							s.cancel();
						}
						else {
							s.request(Operators.unboundedOrPrefetch(prefetch));
						}
						return;
					}
				}

				queue = parent.queueSupplier.get();
				long previousState = markSubscriptionSet(this);
				if (isCancelled(previousState)) {
					s.cancel();
				}
				else {
					s.request(Operators.unboundedOrPrefetch(prefetch));
				}
			}
		}

		@Override
		public void onNext(@Nullable T t) {
			if (done) {
				if (t != null) {
					Operators.onNextDropped(t, currentContext());
				}
				return;
			}
			boolean isAsyncMode = sourceMode == Fuseable.ASYNC;
			if (!isAsyncMode && !queue.offer(t)) {
				Throwable ex = Operators.onOperatorError(s,
						Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
						t,
						currentContext());
				if (!Exceptions.addThrowable(ERROR, this, ex)) {
					Operators.onErrorDroppedMulticast(ex, subscribers);
					return;
				}
				done = true;
			}

			long previousState = addWork(this);

			if (isFinalized(previousState)) {
				clear();
				return;
			}

			if (isTerminated(previousState) || isCancelled(previousState)) {
				return;
			}

			if (hasWorkInProgress(previousState)) {
				return;
			}

			drain(previousState + 1);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDroppedMulticast(t, subscribers);
				return;
			}

			if (!Exceptions.addThrowable(ERROR, this, t)) {
				Operators.onErrorDroppedMulticast(t, subscribers);
				return;
			}

			done = true;

			long previousState = markTerminated(this);
			if (isTerminated(previousState) || isCancelled(previousState)) {
				return;
			}

			if (hasWorkInProgress(previousState)) {
				return;
			}

			drain((previousState | TERMINATED_FLAG) + 1);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			long previousState = markTerminated(this);
			if (isTerminated(previousState) || isCancelled(previousState)) {
				return;
			}

			if (hasWorkInProgress(previousState)) {
				return;
			}

			drain((previousState | TERMINATED_FLAG) + 1);
		}

		@Override
		public void dispose() {
			if (SUBSCRIBERS.get(this) == TERMINATED) {
				return;
			}
			if (CONNECTION.compareAndSet(parent, this, null)) {
				long previousState = markCancelled(this);
				if (isTerminated(previousState) || isCancelled(previousState)) {
					return;
				}

				if (hasWorkInProgress(previousState)) {
					return;
				}

				disconnectAction(previousState);
			}
		}

		void clear() {
			if (sourceMode == Fuseable.NONE) {
				T t;
				while ((t = queue.poll()) != null) {
					Operators.onDiscard(t, currentContext());
				}
			}
			else {
				queue.clear();
			}
		}

		void disconnectAction(long previousState) {
			if (isSubscriptionSet(previousState)) {
				this.s.cancel();
				clear();
			}

			@SuppressWarnings("unchecked")
			PubSubInner<T>[] inners = SUBSCRIBERS.getAndSet(this, CANCELLED);
			if (inners.length > 0) {
				CancellationException ex = new CancellationException("Disconnected");

				for (PubSubInner<T> inner : inners) {
					inner.actual.onError(ex);
				}
			}
		}

		boolean add(PublishInner<T> inner) {
			for (; ; ) {
				FluxPublish.PubSubInner<T>[] a = subscribers;
				if (a == TERMINATED) {
					return false;
				}
				int n = a.length;
				PubSubInner<?>[] b = new PubSubInner[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = inner;
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return true;
				}
			}
		}

		public void remove(PubSubInner<T> inner) {
			for (; ; ) {
				PubSubInner<T>[] a = subscribers;
				if (a == TERMINATED || a == CANCELLED) {
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
					//inner was not found
					return;
				}

				PubSubInner<?>[] b;
				if (n == 1) {
					b = CANCELLED;
				}
				else {
					b = new PubSubInner<?>[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					//we don't assume autoCancel semantics, which will rather depend from
					//downstream operators like autoConnect vs refCount, so we don't disconnect here
					return;
				}
			}
		}

		@SuppressWarnings("unchecked")
		PubSubInner<T>[] terminate() {
			return SUBSCRIBERS.getAndSet(this, TERMINATED);
		}

		boolean tryConnect() {
			long previousState = markConnected(this);

			return !isConnected(previousState);
		}

		final void drainFromInner() {
			long previousState = addWorkIfSubscribed(this);

			if (!isSubscriptionSet(previousState)) {
				return;
			}

			if (hasWorkInProgress(previousState)) {
				return;
			}

			drain(previousState + 1);
		}

		final void drain(long expectedState) {
			for (; ; ) {

				boolean d = done;

				Queue<T> q = queue;
				int mode = sourceMode;

				boolean empty = q == null || q.isEmpty();

				if (checkTerminated(d, empty, null)) {
					return;
				}

				PubSubInner<T>[] a = subscribers;

				if (a != CANCELLED && !empty) {
					long maxRequested = Long.MAX_VALUE;

					int len = a.length;
					int cancel = 0;

					for (PubSubInner<T> inner : a) {
						long r = inner.requested;
						if (r >= 0L) {
							maxRequested = Math.min(maxRequested, r);
						}
						else { //Long.MIN
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
									Operators.onOperatorError(s, ex, currentContext()));
							d = true;
							v = null;
						}
						if (checkTerminated(d, v == null, v)) {
							return;
						}
						if (mode != Fuseable.SYNC) {
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
									Operators.onOperatorError(s, ex, currentContext()));
							d = true;
							v = null;
						}

						empty = v == null;

						if (checkTerminated(d, empty, v)) {
							return;
						}

						if (empty) {
							//async mode only needs to break but SYNC mode needs to perform terminal cleanup here...
							if (mode == Fuseable.SYNC) {
								done = true;
								checkTerminated(true, true, null);
							}
							break;
						}

						for (PubSubInner<T> inner : a) {
							inner.actual.onNext(v);
							if(Operators.producedCancellable(PubSubInner.REQUESTED,
									inner,1) ==
									Long.MIN_VALUE){
								cancel = Integer.MIN_VALUE;
							}
						}

						e++;
					}

					if (e != 0 && mode != Fuseable.SYNC) {
						s.request(e);
					}

					if (maxRequested != 0L && !empty) {
						continue;
					}
				}
				else if (q != null && mode == Fuseable.SYNC) {
					done = true;
					if (checkTerminated(true, empty, null)) {
						break;
					}
				}

				expectedState = markWorkDone(this, expectedState);
				if (isCancelled(expectedState)) {
					clearAndFinalize(this);
					return;
				}

				if (!hasWorkInProgress(expectedState)) {
					return;
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, @Nullable T t) {
			long state = this.state;
			if (isCancelled(state)) {
				Operators.onDiscard(t, currentContext());
				disconnectAction(state);
				return true;
			}
			if (d) {
				Throwable e = error;
				if (e != null && e != Exceptions.TERMINATED) {
					if (parent.resetUponSourceTermination) {
						CONNECTION.compareAndSet(parent, this, null);
						e = Exceptions.terminate(ERROR, this);
					}
					queue.clear();
					for (PubSubInner<T> inner : terminate()) {
						inner.actual.onError(e);
					}
					return true;
				}
				else if (empty) {
					if (parent.resetUponSourceTermination) {
						CONNECTION.compareAndSet(parent, this, null);
					}
					for (PubSubInner<T> inner : terminate()) {
						inner.actual.onComplete();
					}
					return true;
				}
			}
			return false;
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.ERROR) return error;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.TERMINATED) return isTerminated();
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public boolean isDisposed() {
			long state = this.state;
			return isTerminated(state) || isCancelled(state);
		}

		static void clearAndFinalize(PublishSubscriber<?> instance) {
			for (; ; ) {
				final long state = instance.state;

				if (isFinalized(state)) {
					instance.clear();
					return;
				}

				if (isSubscriptionSet(state)) {
					instance.clear();
				}

				if (STATE.compareAndSet(
						instance, state,
						(state & ~WORK_IN_PROGRESS_MASK) | FINALIZED_FLAG)) {
					break;
				}
			}
		}

		static long addWork(PublishSubscriber<?> instance) {
			for (;;) {
				long state = instance.state;

				if (STATE.compareAndSet(instance, state, addWork(state))) {
					return state;
				}
			}
		}

		static long addWorkIfSubscribed(PublishSubscriber<?> instance) {
			for (;;) {
				long state = instance.state;

				if (!isSubscriptionSet(state)) {
					return state;
				}

				if (STATE.compareAndSet(instance, state, addWork(state))) {
					return state;
				}
			}
		}

		static long addWork(long state) {
			if ((state & WORK_IN_PROGRESS_MASK) == WORK_IN_PROGRESS_MASK) {
				return (state &~ WORK_IN_PROGRESS_MASK) | 1;
			}
			else {
				return state + 1;
			}
		}

		static long markTerminated(PublishSubscriber<?> instance) {
			for (;;) {
				long state = instance.state;

				if (isCancelled(state) || isTerminated(state)) {
					return state;
				}

				long nextState = addWork(state);
				if (STATE.compareAndSet(instance, state, nextState | TERMINATED_FLAG)) {
					return state;
				}
			}
		}

		static long markConnected(PublishSubscriber<?> instance) {
			for (;;) {
				long state = instance.state;

				if (isConnected(state)) {
					return state;
				}

				if (STATE.compareAndSet(instance, state, state | CONNECTED_FLAG)) {
					return state;
				}
			}
		}

		static long markSubscriptionSet(PublishSubscriber<?> instance) {
			for (;;) {
				long state = instance.state;

				if (isCancelled(state)) {
					return state;
				}

				if (STATE.compareAndSet(instance, state, state | SUBSCRIPTION_SET_FLAG)) {
					return state;
				}
			}
		}

		static long markSubscriptionSetAndAddWork(PublishSubscriber<?> instance) {
			for (;;) {
				long state = instance.state;

				if (isCancelled(state)) {
					return state;
				}

				long nextState = addWork(state);
				if (STATE.compareAndSet(instance, state, nextState | SUBSCRIPTION_SET_FLAG)) {
					return state;
				}
			}
		}

		static long markCancelled(PublishSubscriber<?> instance) {
			for (;;) {
				long state = instance.state;

				if (isCancelled(state)) {
					return state;
				}

				long nextState = addWork(state);
				if (STATE.compareAndSet(instance, state, nextState | CANCELLED_FLAG)) {
					return state;
				}
			}
		}

		static long markWorkDone(PublishSubscriber<?> instance, long expectedState) {
			for (;;) {
				long state = instance.state;

				if (expectedState != state) {
					return state;
				}

				long nextState = state & ~WORK_IN_PROGRESS_MASK;
				if (STATE.compareAndSet(instance, state, nextState)) {
					return nextState;
				}
			}
		}

		static boolean isConnected(long state) {
			return (state & CONNECTED_FLAG) == CONNECTED_FLAG;
		}

		static boolean isFinalized(long state) {
			return (state & FINALIZED_FLAG) == FINALIZED_FLAG;
		}

		static boolean isCancelled(long state) {
			return (state & CANCELLED_FLAG) == CANCELLED_FLAG;
		}

		static boolean isTerminated(long state) {
			return (state & TERMINATED_FLAG) == TERMINATED_FLAG;
		}

		static boolean isSubscriptionSet(long state) {
			return (state & SUBSCRIPTION_SET_FLAG) == SUBSCRIPTION_SET_FLAG;
		}

		static boolean hasWorkInProgress(long state) {
			return (state & WORK_IN_PROGRESS_MASK) > 0;
		}

		static final long FINALIZED_FLAG =
				0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;

		static final long CANCELLED_FLAG =
				0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;

		static final long TERMINATED_FLAG =
				0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;

		static final long SUBSCRIPTION_SET_FLAG =
				0b0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;

		static final long CONNECTED_FLAG =
				0b0000_0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;

		static final long WORK_IN_PROGRESS_MASK =
				0b0000_0000_0000_0000_0000_0000_0000_0000_1111_1111_1111_1111_1111_1111_1111_1111L;
	}

	static abstract class PubSubInner<T> implements InnerProducer<T> {

		final CoreSubscriber<? super T> actual;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PubSubInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PubSubInner.class, "requested");

		PubSubInner(CoreSubscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public final void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCapCancellable(REQUESTED, this, n);
				drainParent();
			}
		}

		@Override
		public final void cancel() {
			long r = requested;
			if (r != Long.MIN_VALUE) {
				r = REQUESTED.getAndSet(this, Long.MIN_VALUE);
				if (r != Long.MIN_VALUE) {
					removeAndDrainParent();
				}
			}
		}

		final boolean isCancelled() {
			return requested == Long.MIN_VALUE;
		}

		@Override
		public final CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return isCancelled();
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return isCancelled() ? 0L : requested;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerProducer.super.scanUnsafe(key);
		}

		abstract void drainParent();
		abstract void removeAndDrainParent();
	}

	static final class PublishInner<T> extends PubSubInner<T> {
		PublishSubscriber<T> parent;

		PublishInner(CoreSubscriber<? super T> actual) {
			super(actual);
		}

		@Override
		void drainParent() {
			PublishSubscriber<T> p = parent;
			if (p != null) {
				p.drainFromInner();
			}
		}

		@Override
		void removeAndDrainParent() {
			PublishSubscriber<T> p = parent;
			if (p != null) {
				p.remove(this);
				p.drainFromInner();
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.TERMINATED) return parent != null && parent.isTerminated();
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
		}
	}
}
