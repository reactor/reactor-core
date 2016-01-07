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

package reactor.core.processor;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.core.error.CancelException;
import reactor.core.error.InsufficientCapacityException;
import reactor.core.error.ReactorFatalException;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.core.support.internal.PlatformDependent;
import reactor.core.support.rb.disruptor.RingBuffer;
import reactor.core.support.rb.disruptor.Sequence;
import reactor.core.support.rb.disruptor.Sequencer;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public final class EmitterProcessor<T> extends FluxProcessor<T, T>
		implements ReactiveState.LinkedDownstreams,
		           ReactiveState.ActiveUpstream,
		           ReactiveState.ActiveDownstream,
		           ReactiveState.UpstreamDemand,
		           ReactiveState.UpstreamPrefetch,
		           ReactiveState.Buffering,
		           ReactiveState.FailState{

	final int maxConcurrency;
	final int bufferSize;
	final int limit;
	final int replay;
	final boolean autoCancel;

	private volatile RingBuffer<RingBuffer.Slot<T>> emitBuffer;

	private volatile boolean done;

	static final EmitterSubscriber<?>[] EMPTY = new EmitterSubscriber<?>[0];

	static final EmitterSubscriber<?>[] CANCELLED = new EmitterSubscriber<?>[0];

	@SuppressWarnings("unused")
	private volatile Throwable error;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, Throwable> ERROR =
			PlatformDependent.newAtomicReferenceFieldUpdater(EmitterProcessor.class, "error");

	volatile EmitterSubscriber<?>[] subscribers;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, EmitterSubscriber[]> SUBSCRIBERS =
			PlatformDependent.newAtomicReferenceFieldUpdater(EmitterProcessor.class, "subscribers");

	@SuppressWarnings("unused")
	private volatile int running;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<EmitterProcessor> RUNNING =
			AtomicIntegerFieldUpdater.newUpdater(EmitterProcessor.class, "running");

	@SuppressWarnings("unused")
	private volatile int outstanding;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<EmitterProcessor> OUTSTANDING =
			AtomicIntegerFieldUpdater.newUpdater(EmitterProcessor.class, "outstanding");

	long uniqueId;
	long lastId;
	int  lastIndex;
	boolean firstDrain = true;

	public EmitterProcessor(){
		this(true, Integer.MAX_VALUE, SMALL_BUFFER_SIZE, -1);
	}

	public EmitterProcessor(boolean autoCancel, int maxConcurrency, int bufferSize, int replayLastN) {
		this.autoCancel = autoCancel;
		this.maxConcurrency = maxConcurrency;
		this.bufferSize = bufferSize;
		this.limit = Math.max(1, bufferSize / 2);
		this.replay = Math.min(replayLastN, bufferSize);
		OUTSTANDING.lazySet(this, bufferSize);
		SUBSCRIBERS.lazySet(this, EMPTY);
		if (replayLastN > 0) {
			getMainQueue();
		}
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		super.subscribe(s);
		EmitterSubscriber<T> inner = new EmitterSubscriber<T>(this, s, uniqueId++);
		try {
			addInner(inner);
			if (upstreamSubscription != null) {
				inner.start();
			}
		}
		catch (CancelException c) {
			//IGNORE
		}
		catch (Throwable t) {
			removeInner(inner, EMPTY);
			EmptySubscription.error(s, t);
		}
	}

	@Override
	public long pending() {
		return (emitBuffer == null ? -1L : emitBuffer.pending());
	}

	@Override
	protected void doOnSubscribe(Subscription s) {
		EmitterSubscriber<?>[] innerSubscribers = subscribers;
		if (innerSubscribers != CANCELLED && innerSubscribers.length != 0) {
			for (int i = 0; i < innerSubscribers.length; i++) {
				innerSubscribers[i].start();
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onNext(T t) {
		super.onNext(t);

		EmitterSubscriber<?>[] inner = subscribers;
		if (autoCancel && inner == CANCELLED) {
			//FIXME should entorse to the spec and throw CancelException
			return;
		}

		int n = inner.length;
		if (n != 0) {

			long seq = -1L;

			int outstanding;
			if (upstreamSubscription != EmptySubscription.INSTANCE) {

				outstanding = this.outstanding;
				if (outstanding != 0) {
					OUTSTANDING.decrementAndGet(this);
				}
				else {
					buffer(t);
					drain();
					return;
				}
			}

			int j = n == 1 ? 0 : getLastIndex(n, inner);

			for (int i = 0; i < n; i++) {

				EmitterSubscriber<T> is = (EmitterSubscriber<T>) inner[j];

				if (is.done) {
					removeInner(is, autoCancel ? CANCELLED : EMPTY);

					if (autoCancel && subscribers == CANCELLED) {
						if (RUNNING.compareAndSet(this, 0, 1)) {
							cancel();
						}
						return;
					}
					j++;
					if (j == n) {
						j = 0;
					}
					continue;
				}

				if (is.unbounded && replay == -1) {
					is.actual.onNext(t);
				}
				else {
					long r = is.requested;
					is.unbounded = r == Long.MAX_VALUE;
					Sequence poll = is.unbounded && replay == -1 ? null : is.pollCursor;

					//no tracking and remaining demand positive
					if (r > 0L && poll == null) {
						if (r != Long.MAX_VALUE) {
							EmitterSubscriber.REQUESTED.decrementAndGet(is);
						}
						is.actual.onNext(t);
					}
					//if failed, we buffer if not previously buffered and we assign a tracking cursor to that slot
					else {
						if (seq == -1L) {
							seq = buffer(t);
							if (i > 0) {
								startAllTrackers(inner, seq, j, i - 1, n);
							}

						}
						is.startTracking(seq);
					}
				}

				j++;
				if (j == n) {
					j = 0;
				}
			}
			lastIndex = j;
			lastId = inner[j].id;

			if (RUNNING.getAndIncrement(this) != 0) {
				return;
			}

			drainLoop();

		}
		else {
			buffer(t);
		}
	}

	@Override
	public void onError(Throwable t) {
		super.onError(t);
		if (autoCancel && done) {
			throw ReactorFatalException.create(t);
		}
		reportError(t);
		done = true;
		drain();
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

	@Override
	public boolean isCancelled() {
		return autoCancel && subscribers == CANCELLED;
	}

	@Override
	final public long getCapacity() {
		return bufferSize;
	}

	@Override
	public boolean isStarted() {
		return upstreamSubscription != null;
	}

	@Override
	public boolean isTerminated() {
		return done && (emitBuffer == null || emitBuffer.pending() == 0L);
	}

	@Override
	public long limit() {
		return limit;
	}

	@Override
	public long expectedFromUpstream() {
		return outstanding;
	}

	RingBuffer<RingBuffer.Slot<T>> getMainQueue() {
		RingBuffer<RingBuffer.Slot<T>> q = emitBuffer;
		if (q == null) {
			q = RingBuffer.createSingleProducer(bufferSize);
			emitBuffer = q;
		}
		return q;
	}

	final long buffer(T value) {
		RingBuffer<RingBuffer.Slot<T>> q = getMainQueue();

		long seq = q.next();

		q.get(seq).value = value;
		q.publish(seq);
		return seq;
	}

	final void drain() {
		if (RUNNING.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	final void drainLoop() {
		int missed = 1;
		RingBuffer<RingBuffer.Slot<T>> q = null;
		for (; ; ) {
			EmitterSubscriber<?>[] inner = subscribers;
			if (inner == CANCELLED) {
				cancel();
				return;
			}
			boolean d = done;

			if (d && replay == -1 && inner == EMPTY) {
				return;
			}

			int n = inner.length;

			if (n != 0) {
				int j = getLastIndex(n, inner);
				Sequence innerSequence;
				long _r;

				for (int i = 0; i < n; i++) {
					@SuppressWarnings("unchecked") EmitterSubscriber<T> is = (EmitterSubscriber<T>) inner[j];

					long r = is.requested;

					if (is.done) {
						removeInner(is, autoCancel ? CANCELLED : EMPTY);
						if (autoCancel && subscribers == CANCELLED) {
							cancel();
							return;
						}
						continue;
					}

					if (q == null) {
						q = emitBuffer;
					}
					innerSequence = is.pollCursor;

					if (innerSequence != null && r > 0) {
						_r = r;

						boolean unbounded = _r == Long.MAX_VALUE;
						T oo;

						long cursor = innerSequence.get();
						while (_r != 0L) {
							cursor++;
							if (q.getCursor() >= cursor) {
								oo = q.get(cursor).value;
							}
							else {
								break;
							}

							innerSequence.set(cursor);
							is.actual.onNext(oo);

							if (!unbounded) {
								_r--;
							}
						}

						if (r > _r) {
							EmitterSubscriber.REQUESTED.addAndGet(is, _r - r);
						}

					}
					else {
						_r = 0L;
					}

					if (d) {
						checkTerminal(is, innerSequence, _r);
					}

					j++;
					if (j == n) {
						j = 0;
					}
				}
				lastIndex = j;
				lastId = inner[j].id;

				if (!done && firstDrain) {
					Subscription s = upstreamSubscription;
					if (s != null) {
						firstDrain = false;
						s.request(bufferSize);
					}
				}
				else {
					if (q != null) {
						requestMore((int) q.pending());
					}
					else {
						requestMore(0);
					}
				}
			}

			missed = RUNNING.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	final void checkTerminal(EmitterSubscriber<T> is, Sequence innerSequence, long r) {
		Throwable e = error;
		if ((e != null && r == 0) || innerSequence == null || innerSequence.get() >= emitBuffer.getCursor()) {
			removeInner(is, EMPTY);
			if (!is.done) {
				if (e == null) {
					is.actual.onComplete();
				}
				else {
					is.actual.onError(e);
				}
			}
		}
	}

	final void startAllTrackers(EmitterSubscriber<?>[] inner, long seq, int startIndex, int times, int size) {
		int k = startIndex;
		Sequence poll;
		for (int l = times; l > 0; l--) {
			k--;
			if (k == -1) {
				k = size - 1;
			}

			if (k == startIndex) {
				continue;
			}

			poll = inner[k].pollCursor;
			if (poll == null) {
				inner[k].startTracking(seq);
			}
		}
	}

	final void reportError(Throwable t) {
		ERROR.compareAndSet(this, null, t);
	}

	final void addInner(EmitterSubscriber<T> inner) {
		for (; ; ) {
			EmitterSubscriber<?>[] a = subscribers;
			if (a == CANCELLED) {
				Flux.<T>empty().subscribe(inner.actual);
			}
			int n = a.length;
			if (n + 1 > maxConcurrency) {
				throw InsufficientCapacityException.get();
			}
			EmitterSubscriber<?>[] b = new EmitterSubscriber[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = inner;
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	final int getLastIndex(int n, EmitterSubscriber<?>[] inner) {
		int index = lastIndex;
		long startId = lastId;
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
		return index;
	}

	final void removeInner(EmitterSubscriber<?> inner, EmitterSubscriber<?>[] lastRemoved) {
		for (; ; ) {
			EmitterSubscriber<?>[] a = subscribers;
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
			EmitterSubscriber<?>[] b;
			if (n == 1) {
				b = lastRemoved;
			}
			else {
				b = new EmitterSubscriber<?>[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				Sequence poll = inner.pollCursor;
				if (poll != null) {
					getMainQueue().removeGatingSequence(poll);
				}
				return;
			}
		}
	}

	final void requestMore(int buffered) {
		Subscription subscription = upstreamSubscription;
		if (subscription == EmptySubscription.INSTANCE) {
			return;
		}

		if (buffered < bufferSize) {
			int r = outstanding;
			if (r > limit) {
				return;
			}
			int toRequest = (bufferSize - r) - buffered;
			if (toRequest > 0 && subscription != null) {
				OUTSTANDING.addAndGet(this, toRequest);
				subscription.request(toRequest);
			}
		}
	}

	final void cancel() {
		if (!done) {
			Subscription s = upstreamSubscription;
			if (s != null) {
				upstreamSubscription = null;
				s.cancel();
			}
		}
	}

	@Override
	public Iterator<?> downstreams() {
		return Arrays.asList(subscribers).iterator();
	}

	@Override
	public long downstreamsCount() {
		return subscribers.length;
	}

	@Override
	public String toString() {
		return "{" +
				"done: " + done +
				(error != null ? ", error: '" + error.getMessage() + "', " : "") +
				", outstanding: " + outstanding +
				", pending: " + emitBuffer +
				'}';
	}

	static final class EmitterSubscriber<T>
			implements Subscription, Inner, ActiveUpstream, ActiveDownstream, Buffering, Bounded, Upstream,
			           DownstreamDemand, Downstream {

		final long                  id;
		final EmitterProcessor<T>   parent;
		final Subscriber<? super T> actual;

		volatile boolean done;

		boolean unbounded = false;

		@SuppressWarnings("unused")
		private volatile long requested = -1L;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<EmitterSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(EmitterSubscriber.class, "requested");

		volatile Sequence pollCursor;

		static final AtomicReferenceFieldUpdater<EmitterSubscriber, Sequence> CURSOR =
				PlatformDependent.newAtomicReferenceFieldUpdater(EmitterSubscriber.class, "pollCursor");

		public EmitterSubscriber(EmitterProcessor<T> parent, final Subscriber<? super T> actual, long id) {
			this.id = id;
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.checkRequest(n, actual)) {
				BackpressureUtils.getAndAdd(REQUESTED, this, n);
				if (EmitterProcessor.RUNNING.getAndIncrement(parent) == 0) {
					parent.drainLoop();
				}
			}
		}

		public void cancel() {
			done = true;
			parent.drain();
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public long getCapacity() {
			return parent.bufferSize;
		}

		void startTracking(long seq) {
			Sequence pollSequence = Sequencer.newSequence(seq - 1L);
			if (CURSOR.compareAndSet(this, null, pollSequence)) {
				parent.emitBuffer.addGatingSequence(pollSequence);
			}
		}

		void start() {
			if (REQUESTED.compareAndSet(this, -1L, 0)) {
				RingBuffer<RingBuffer.Slot<T>> ringBuffer = parent.emitBuffer;
				if (ringBuffer != null) {
					if (parent.replay > 0) {
						long cursor = ringBuffer.getCursor();
						startTracking(Math.max(0L,
								cursor - Math.min(parent.replay, cursor % ringBuffer.getBufferSize())));
					}
					else {
						startTracking(Math.max(0L, ringBuffer.getMinimumGatingSequence()));
					}
				}

				actual.onSubscribe(this);
			}
		}

		@Override
		public boolean isCancelled() {
			return done;
		}

		@Override
		public long pending() {
			return pollCursor == null || done ? -1L : parent.emitBuffer.getCursor() - pollCursor
					.get();
		}

		@Override
		public boolean isStarted() {
			return parent.isStarted();
		}

		@Override
		public boolean isTerminated() {
			return parent.isTerminated();
		}

		@Override
		public Object upstream() {
			return parent;
		}

		@Override
		public Subscriber<? super T> downstream() {
			return actual;
		}
	}


}
