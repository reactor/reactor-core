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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.ReactiveState;
import reactor.core.support.internal.PlatformDependent;
import reactor.core.support.rb.disruptor.RingBuffer;
import reactor.core.support.rb.disruptor.Sequence;
import reactor.core.support.rb.disruptor.Sequencer;
import reactor.fn.Function;
import reactor.fn.Supplier;

/**
 * A merge operator that transforms input T to Publisher of V using the assigned map function. Once transformed,
 * publishers are then subscribed and requested with the following demand rules: - request eagerly up to bufferSize -
 * downstream consume buffer once all inner subscribers have read - when more than half of the demand has been
 * fulfilled, request more - request to upstream represent the maximum concurrent merge possible
 * <p>
 * <p>
 * https://github.com/reactor/reactive-streams-commons
 *
 * @author David Karnok
 * @author Stephane Maldini
 * @since 2.5
 */
public final class FluxFlatMap<T, V> extends Flux.FluxBarrier<T, V> {

	/**
	 * @param <T>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> Function<Publisher<? extends T>, Publisher<? extends T>> identity() {
		return (Function<Publisher<? extends T>, Publisher<? extends T>>) P2P_FUNCTION;
	}

	/**
	 * Internals
	 */

	private static final PublisherToPublisherFunction<?> P2P_FUNCTION = new PublisherToPublisherFunction<>();

	final Function<? super T, ? extends Publisher<? extends V>> mapper;
	final int                                                   maxConcurrency;
	final int                                                   bufferSize;

	@SuppressWarnings("unchecked")
	public FluxFlatMap(Publisher<T> source, int maxConcurrency, int bufferSize) {
		this(source, (Function<? super T, ? extends Publisher<? extends V>>) P2P_FUNCTION, maxConcurrency, bufferSize);
	}

	public FluxFlatMap(Publisher<T> source,
			Function<? super T, ? extends Publisher<? extends V>> mapper,
			int maxConcurrency,
			int bufferSize) {
		super(source);
		this.mapper = mapper;
		this.maxConcurrency = maxConcurrency;
		this.bufferSize = bufferSize;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super V> s) {
		if (s == null) {
			throw Exceptions.spec_2_13_exception();
		}
		if (source instanceof Supplier) {
			try {
				T v = ((Supplier<T>) source).get();
				if (v != null) {
					mapper.apply(v)
					      .subscribe(s);
					return;
				}
			}
			catch (Throwable e) {
				EmptySubscription.error(s, e);
				return;
			}
		}
		source.subscribe(new MergeBarrier<>(s, mapper, maxConcurrency, bufferSize));
	}

	static final class MergeBarrier<T, V> extends SubscriberWithDemand<T, V>
			implements ReactiveState.LinkedUpstreams, ReactiveState.ActiveDownstream, ReactiveState.Buffering,
			           ReactiveState.FailState {

		final Function<? super T, ? extends Publisher<? extends V>> mapper;
		final int                                                   maxConcurrency;
		final int                                                   bufferSize;
		final int                                                   limit;

		private          Sequence                       pollCursor;
		private volatile RingBuffer<RingBuffer.Slot<V>> emitBuffer;

		private volatile Throwable error;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeBarrier, Throwable> ERROR =
				PlatformDependent.newAtomicReferenceFieldUpdater(MergeBarrier.class, "error");

		volatile BufferSubscriber<?, ?>[] subscribers;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeBarrier, BufferSubscriber[]> SUBSCRIBERS =
				PlatformDependent.newAtomicReferenceFieldUpdater(MergeBarrier.class, "subscribers");

		@SuppressWarnings("unused")
		private volatile int running;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MergeBarrier> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(MergeBarrier.class, "running");

		static final BufferSubscriber<?, ?>[] EMPTY = new BufferSubscriber<?, ?>[0];

		static final BufferSubscriber<?, ?>[] CANCELLED = new BufferSubscriber<?, ?>[0];

		long lastRequest;
		long uniqueId;
		long lastId;
		int  lastIndex;

		public MergeBarrier(Subscriber<? super V> actual,
				Function<? super T, ? extends Publisher<? extends V>> mapper,
				int maxConcurrency,
				int bufferSize) {
			super(actual);
			this.mapper = mapper;
			this.maxConcurrency = maxConcurrency;
			this.bufferSize = bufferSize;
			this.limit = Math.max(1, maxConcurrency / 2);
			SUBSCRIBERS.lazySet(this, EMPTY);
		}

		@Override
		protected void doOnSubscribe(Subscription s) {
			subscriber.onSubscribe(this);
			if (!isCancelled()) {
				if (maxConcurrency == Integer.MAX_VALUE) {
					subscription.request(Long.MAX_VALUE);
				}
				else {
					subscription.request(maxConcurrency);
				}
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		protected void doNext(T t) {
			// safeguard against misbehaving sources
			final Publisher<? extends V> p = mapper.apply(t);

			if (p instanceof Supplier) {
				V v = ((Supplier<? extends V>) p).get();
				if (v != null) {
					tryEmit(v);
					return;
				}
			}

			BufferSubscriber<T, V> inner = new BufferSubscriber<>(this, uniqueId++);
			addInner(inner);
			p.subscribe(inner);
		}

		@Override
		public long pending() {
			return emitBuffer != null ? emitBuffer.pending() : -1;
		}

		void addInner(BufferSubscriber<T, V> inner) {
			for (; ; ) {
				BufferSubscriber<?, ?>[] a = subscribers;
				if (a == CANCELLED) {
					inner.cancel();
					return;
				}
				int n = a.length;
				BufferSubscriber<?, ?>[] b = new BufferSubscriber[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = inner;
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return;
				}
			}
		}

		@Override
		public long getCapacity() {
			return bufferSize;
		}

		void removeInner(BufferSubscriber<T, V> inner) {
			for (; ; ) {
				BufferSubscriber<?, ?>[] a = subscribers;
				if (a == CANCELLED || a == EMPTY) {
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
				BufferSubscriber<?, ?>[] b;
				if (n == 1) {
					b = EMPTY;
				}
				else {
					b = new BufferSubscriber<?, ?>[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return;
				}
			}
		}

		RingBuffer<RingBuffer.Slot<V>> getMainQueue() {
			RingBuffer<RingBuffer.Slot<V>> q = emitBuffer;
			if (q == null) {
				q = RingBuffer.createSingleProducer(maxConcurrency == Integer.MAX_VALUE ? bufferSize : maxConcurrency);
				q.addGatingSequence(pollCursor = Sequencer.newSequence(-1L));
				emitBuffer = q;
			}
			return q;
		}

		void tryEmit(V value) {
			if (RUNNING.get(this) == 0 && RUNNING.compareAndSet(this, 0, 1)) {
				long r = requestedFromDownstream();
				if (r != 0L) {
					if (null != value) {
						subscriber.onNext(value);
					}
					if (r != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}
					if (maxConcurrency != Integer.MAX_VALUE && !isCancelled() && ++lastRequest == limit) {
						lastRequest = 0;
						subscription.request(limit);
					}
				}
				else {
					RingBuffer<RingBuffer.Slot<V>> q = getMainQueue();
					long seq = q.tryNext();
					q.get(seq).value = value;
					q.publish(seq);

				}
				if (RUNNING.decrementAndGet(this) == 0) {
					return;
				}
			}
			else {
				RingBuffer<RingBuffer.Slot<V>> q = getMainQueue();
				long seq = q.tryNext();
				q.get(seq).value = value;
				q.publish(seq);
				if (RUNNING.getAndIncrement(this) != 0) {
					return;
				}
			}
			drainLoop();
		}

		RingBuffer<RingBuffer.Slot<V>> getInnerQueue(BufferSubscriber<T, V> inner) {
			RingBuffer<RingBuffer.Slot<V>> q = inner.queue;
			if (q == null) {
				q = RingBuffer.createSingleProducer(bufferSize);
				q.addGatingSequence(inner.pollCursor = Sequencer.newSequence(-1L));
				inner.queue = q;
			}
			return q;
		}

		void tryEmit(V value, BufferSubscriber<T, V> inner) {
			if (RUNNING.get(this) == 0 && RUNNING.compareAndSet(this, 0, 1)) {
				long r = requestedFromDownstream();
				if (r != 0L) {
					subscriber.onNext(value);
					if (r != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}
					inner.requestMore(1);
				}
				else {
					RingBuffer<RingBuffer.Slot<V>> q = getInnerQueue(inner);
					long seq = q.tryNext();
					q.get(seq).value = value;
					q.publish(seq);
				}
				if (RUNNING.decrementAndGet(this) == 0) {
					return;
				}
			}
			else {
				RingBuffer<RingBuffer.Slot<V>> q = getInnerQueue(inner);
				long seq = q.tryNext();
				q.get(seq).value = value;
				q.publish(seq);
				if (RUNNING.getAndIncrement(this) != 0) {
					return;
				}
			}
			drainLoop();
		}

		@Override
		protected void checkedError(Throwable t) {
			reportError(t);
			drain();
		}

		@Override
		protected void checkedComplete() {
			drain();
		}

		@Override
		protected void doRequested(long b, long n) {
			drain();
		}

		@Override
		protected void doCancel() {
			int terminated = TERMINATED.get(this);
			if (terminated != TERMINATED_WITH_CANCEL && TERMINATED.compareAndSet(this,
					terminated,
					TERMINATED_WITH_CANCEL)) {
				if (RUNNING.getAndIncrement(this) == 0) {
					subscription.cancel();
					unsubscribe();
				}
			}
		}

		@Override
		public Iterator<?> upstreams() {
			return Arrays.asList(subscribers)
			             .iterator();
		}

		@Override
		public boolean isTerminated() {
			return super.isTerminated() && subscribers.length == 0;
		}

		@Override
		public long upstreamsCount() {
			return subscribers.length;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		void reportError(Throwable t) {
			if (!ERROR.compareAndSet(this, null, t)) {
				throw ReactorFatalException.create(t);
			}
		}

		void drain() {
			if (RUNNING.getAndIncrement(this) == 0) {
				drainLoop();
			}
		}

		void drainLoop() {
			final Subscriber<? super V> child = this.subscriber;
			int missed = 1;
			for (; ; ) {
				if (checkTerminate()) {
					return;
				}
				RingBuffer<RingBuffer.Slot<V>> svq = emitBuffer;

				long r = requestedFromDownstream();
				boolean unbounded = r == Long.MAX_VALUE;

				long replenishMain = 0;

				if (svq != null) {
					for (; ; ) {
						long scalarEmission = 0;
						RingBuffer.Slot<V> o;
						V oo = null;
						while (r != 0L) {
							long cursor = pollCursor.get() + 1;

							if (svq.getCursor() >= cursor) {
								o = svq.get(cursor);
								oo = o.value;
							}
							else {
								o = null;
								oo = null;
							}

							if (checkTerminate()) {
								return;
							}
							if (oo == null) {
								break;
							}

							o.value = null;
							pollCursor.set(cursor);
							child.onNext(oo);

							replenishMain++;
							scalarEmission++;
							r--;
						}
						if (scalarEmission != 0L) {
							if (unbounded) {
								r = Long.MAX_VALUE;
							}
							else {
								r = REQUESTED.addAndGet(this, -scalarEmission);
							}
						}
						if (r == 0L || oo == null) {
							break;
						}
					}
				}

				boolean d = super.isTerminated();
				svq = emitBuffer;
				BufferSubscriber<?, ?>[] inner = subscribers;
				int n = inner.length;

				if (d && !isCancelled() && (svq == null || svq.pending() == 0) && n == 0) {
					Throwable e = error;
					if (e == null) {
						child.onComplete();
					}
					else {
						subscriber.onError(e);
					}
					return;
				}

				boolean innerCompleted = false;
				if (n != 0) {
					long startId = lastId;
					int index = lastIndex;

					if (n <= index || inner[index].id != startId) {
						if (n <= index) {
							index = 0;
						}
						int j = index;
						for (int i = 0; i < n; i++) {
							if (inner[j].id == startId) {
								break;
							}
							j++;
							if (j == n) {
								j = 0;
							}
						}
						index = j;
						lastIndex = j;
						lastId = inner[j].id;
					}

					int j = index;
					for (int i = 0; i < n; i++) {
						if (checkTerminate()) {
							return;
						}
						@SuppressWarnings("unchecked") BufferSubscriber<T, V> is = (BufferSubscriber<T, V>) inner[j];

						RingBuffer.Slot<V> o;
						V oo = null;
						for (; ; ) {
							long produced = 0;
							while (r != 0L) {
								if (checkTerminate()) {
									return;
								}
								RingBuffer<RingBuffer.Slot<V>> q = is.queue;
								if (q == null) {
									break;
								}
								long cursor = is.pollCursor.get() + 1;

								if (q.getCursor() >= cursor) {
									o = q.get(cursor);
									oo = o.value;
								}
								else {
									o = null;
									oo = null;
								}

								if (oo == null) {
									break;
								}

								o.value = null;
								is.pollCursor.set(cursor);
								child.onNext(oo);

								r--;
								produced++;
							}
							if (produced != 0L) {
								if (!unbounded) {
									r = REQUESTED.addAndGet(this, -produced);
								}
								else {
									r = Long.MAX_VALUE;
								}
								is.requestMore(produced);
							}
							if (r == 0 || oo == null) {
								break;
							}
						}
						boolean innerDone = is.done;
						RingBuffer<RingBuffer.Slot<V>> innerQueue = is.queue;
						if (innerDone && (innerQueue == null || innerQueue.pending() == 0)) {
							removeInner(is);
							if (checkTerminate()) {
								return;
							}
							replenishMain++;
							innerCompleted = true;
						}
						if (r == 0L) {
							break;
						}

						j++;
						if (j == n) {
							j = 0;
						}
					}
					lastIndex = j;
					lastId = inner[j].id;
				}

				if (replenishMain != 0L && !isCancelled()) {
					subscription.request(maxConcurrency == 1 ? 1 : replenishMain);
				}
				if (innerCompleted) {
					continue;
				}
				missed = RUNNING.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminate() {
			if (isCancelled()) {
				subscription.cancel();
				unsubscribe();
				return true;
			}
			Throwable e = error;
			if (e != null) {
				try {
					subscriber.onError(error);
				}
				finally {
					unsubscribe();
				}
				return true;
			}
			return false;
		}

		void unsubscribe() {
			BufferSubscriber<?, ?>[] a = subscribers;
			if (a != CANCELLED) {
				a = SUBSCRIBERS.getAndSet(this, CANCELLED);
				if (a != CANCELLED) {
					for (BufferSubscriber<?, ?> inner : a) {
						inner.cancel();
					}
				}
			}
		}

	}

	static final class BufferSubscriber<T, V> extends BaseSubscriber<V>
			implements ReactiveState.Bounded, ReactiveState.Upstream, ReactiveState.Downstream, ReactiveState.Buffering,
			           ReactiveState.ActiveDownstream, ReactiveState.ActiveUpstream, ReactiveState.UpstreamDemand,
			           ReactiveState.UpstreamPrefetch, ReactiveState.Inner {

		final long               id;
		final MergeBarrier<T, V> parent;
		final int                limit;
		final int                bufferSize;

		@SuppressWarnings("unused")
		volatile Subscription subscription;
		final static AtomicReferenceFieldUpdater<BufferSubscriber, Subscription> SUBSCRIPTION =
				PlatformDependent.newAtomicReferenceFieldUpdater(BufferSubscriber.class, "subscription");

		Sequence pollCursor;

		volatile boolean                        done;
		volatile RingBuffer<RingBuffer.Slot<V>> queue;
		int outstanding;

		public BufferSubscriber(MergeBarrier<T, V> parent, long id) {
			this.id = id;
			this.parent = parent;
			this.bufferSize = parent.bufferSize;
			this.limit = bufferSize >> 2;
		}

		@Override
		public void onSubscribe(Subscription s) {
			super.onSubscribe(s);

//Should it try here a last time to check before requesting
//			if(parent.isCancelled()){
//				s.cancel();
//				return;
//			}

			if (!SUBSCRIPTION.compareAndSet(this, null, s)) {
				s.cancel();
				return;
			}
			outstanding = bufferSize;
			s.request(outstanding);
		}

		@Override
		public void onNext(V t) {
			super.onNext(t);
			parent.tryEmit(t, this);
		}

		@Override
		public void onError(Throwable t) {
			super.onError(t);
			parent.reportError(t);
			done = true;
			parent.drain();
		}

		@Override
		public void onComplete() {
			done = true;
			parent.drain();
		}

		void requestMore(long n) {
			int r = outstanding - (int) n;
			if (r > limit) {
				outstanding = r;
				return;
			}
			outstanding = bufferSize;
			int k = bufferSize - r;
			if (k > 0) {
				SUBSCRIPTION.get(this)
				            .request(k);
			}
		}

		@Override
		public Subscriber downstream() {
			return parent;
		}

		public void cancel() {
			Subscription s = SUBSCRIPTION.get(this);
			if (s != EmptySubscription.INSTANCE) {
				s = SUBSCRIPTION.getAndSet(this, EmptySubscription.INSTANCE);
				if (s != EmptySubscription.INSTANCE && s != null) {
					s.cancel();
				}
			}
		}

		@Override
		public long limit() {
			return limit;
		}

		@Override
		public long expectedFromUpstream() {
			return outstanding;
		}

		@Override
		public Object upstream() {
			return subscription;
		}

		@Override
		public long getCapacity() {
			return bufferSize;
		}

		@Override
		public long pending() {
			return !done && queue != null ? queue.pending() : -1L;
		}

		@Override
		public boolean isCancelled() {
			return parent.isCancelled();
		}

		@Override
		public boolean isStarted() {
			return parent.isStarted();
		}

		@Override
		public boolean isTerminated() {
			return done && (queue == null || queue.pending() == 0L);
		}
	}

	static final class PublisherToPublisherFunction<I>
			implements Function<Publisher<? extends I>, Publisher<? extends I>> {

		@Override
		public Publisher<? extends I> apply(Publisher<? extends I> publisher) {
			return publisher;
		}
	}
}