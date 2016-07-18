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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.MultiProducer;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.Exceptions;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.concurrent.RingBuffer;
import reactor.util.concurrent.Sequence;

/**
 ** An implementation of a RingBuffer backed message-passing Processor implementing publish-subscribe with
 * synchronous (thread-stealing and happen-before interactions) drain loops.
 * <p>
 *     The default {@link #create} factories will only produce the new elements observed in
 *     the
 *     parent sequence after a given {@link Subscriber} is subscribed.
 *
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/emitter.png" alt="">
 * <p>
 *
 * @author Stephane Maldini
 *
 * @param <T> the input and output value type
 */
public final class EmitterProcessor<T> extends FluxProcessor<T, T>
		implements MultiProducer, Receiver {

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 *
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create() {
		return create(true);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 * @param <E> Type of processed signals
     * @param autoCancel automatically cancel
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(boolean autoCancel) {
		return create(QueueSupplier.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 * @param <E> Type of processed signals
     * @param bufferSize the internal buffer size to hold signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize) {
		return create(bufferSize, Integer.MAX_VALUE);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 * @param <E> Type of processed signals
     * @param bufferSize the internal buffer size to hold signals
     * @param concurrency the concurrency level of the emission
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize, int concurrency) {
		return create(bufferSize, concurrency, true);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. 
	 * @param <E> Type of processed signals
     * @param bufferSize the internal buffer size to hold signals
     * @param autoCancel automatically cancel
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize, boolean autoCancel) {
		return create(bufferSize, Integer.MAX_VALUE, autoCancel);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. 
	 * @param <E> Type of processed signals
	 * @param bufferSize the internal buffer size to hold signals
	 * @param concurrency the concurrency level of the emission
	 * @param autoCancel automatically cancel
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize, int concurrency, boolean autoCancel) {
		return new EmitterProcessor<>(autoCancel, concurrency, bufferSize);
	}

	/**
	 * Create a {@link FluxProcessor} from hot {@link EmitterProcessor#create EmitterProcessor}  safely gated by a serializing {@link Subscriber}.
	 * It will not propagate cancel upstream if {@link Subscription} has been set. Serialization uses thread-stealing
	 * and a potentially unbounded queue that might starve a calling thread if races are too important and
	 * {@link Subscriber} is slower.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/serialize.png" alt="">
	 *
	 * @param <T> the relayed type
	 * @return a serializing {@link FluxProcessor}
	 */
	public static <T> FluxProcessor<T, T> serialize() {
		Processor<T, T> processor = create();
		return new DelegateProcessor<>(processor, Operators.serialize(processor));
	}

	final int maxConcurrency;
	final int bufferSize;
	final int limit;
	final boolean autoCancel;
	Subscription upstreamSubscription;

	private volatile RingBuffer<EventLoopProcessor.Slot<T>> emitBuffer;

	private volatile boolean done;

	@Override
	public Subscription upstream() {
		return upstreamSubscription;
	}

	static final EmitterSubscriber<?>[] EMPTY = new EmitterSubscriber<?>[0];

	static final EmitterSubscriber<?>[] CANCELLED = new EmitterSubscriber<?>[0];

	private volatile Throwable error;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, Throwable> ERROR =
			AtomicReferenceFieldUpdater.newUpdater(EmitterProcessor.class,
					Throwable.class,
					"error");

	volatile EmitterSubscriber<?>[] subscribers;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, EmitterSubscriber[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(EmitterProcessor.class,
					EmitterSubscriber[].class,
					"subscribers");
	@SuppressWarnings("unused")
	private volatile int running;

	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<EmitterProcessor> RUNNING =
			AtomicIntegerFieldUpdater.newUpdater(EmitterProcessor.class, "running");

	private volatile int outstanding;

	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<EmitterProcessor> OUTSTANDING =
			AtomicIntegerFieldUpdater.newUpdater(EmitterProcessor.class, "outstanding");
	boolean firstDrain = true;

	EmitterProcessor(boolean autoCancel, int maxConcurrency, int bufferSize) {
		this.autoCancel = autoCancel;
		this.maxConcurrency = maxConcurrency;
		this.bufferSize = bufferSize;
		this.limit = Math.max(1, bufferSize / 2);
		OUTSTANDING.lazySet(this, bufferSize);
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		super.subscribe(s);
		EmitterSubscriber<T> inner = new EmitterSubscriber<>(this, s);
		try {
			addInner(inner);
			if (upstreamSubscription != null) {
				inner.start();
			}
		}
		catch (Throwable t) {
			if(Exceptions.isCancel(t)){
				return;
			}
			removeInner(inner, EMPTY);
			Operators.error(s, t);
		}
	}

	@Override
	public EmitterProcessor<T> connect() {
		onSubscribe(Operators.emptySubscription());
		return this;
	}

	@Override
	public long getPending() {
		return (emitBuffer == null ? -1L :
				emitBuffer.getPending());
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onNext(T t) {
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}

		EmitterSubscriber<?>[] inner = subscribers;
		if (autoCancel && inner == CANCELLED) {
			//FIXME should entorse to the spec and throw Exceptions.failWithCancel
			return;
		}

		int n = inner.length;
		if (n != 0) {

			long seq = -1L;

			int outstanding;
			if (upstreamSubscription != Operators.emptySubscription()) {

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

			for (int i = 0; i < n; i++) {

				EmitterSubscriber<T> is = (EmitterSubscriber<T>) inner[i];

				if (is.done) {
					removeInner(is, autoCancel ? CANCELLED : EMPTY);

					if (autoCancel && subscribers == CANCELLED) {
						if (RUNNING.compareAndSet(this, 0, 1)) {
							cancel();
						}
						return;
					}
					continue;
				}
					long r = is.requested;
					is.unbounded = r == Long.MAX_VALUE;
					Sequence poll = is.unbounded ? null : is.pollCursor;

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
							startAllTrackers(inner, seq, i + 1);
						}
						else if(poll == null){
							is.startTracking(seq);
						}
					}

			}

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
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}
		if (autoCancel && done) {
			Exceptions.onErrorDropped(t);
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
	public void onSubscribe(final Subscription s) {
		if (Operators.validate(upstreamSubscription, s)) {
			this.upstreamSubscription = s;
			try {
				EmitterSubscriber<?>[] innerSubscribers = subscribers;
				if (innerSubscribers != CANCELLED && innerSubscribers.length != 0) {
					for (int i = 0; i < innerSubscribers.length; i++) {
						innerSubscribers[i].start();
					}
				}
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				s.cancel();
				onError(t);
			}
		}
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
		return done && (emitBuffer == null || emitBuffer.getPending() == 0);
	}

	@Override
	public long limit() {
		return limit;
	}

	@Override
	public long expectedFromUpstream() {
		return outstanding;
	}

	RingBuffer<EventLoopProcessor.Slot<T>> getMainQueue() {
		RingBuffer<EventLoopProcessor.Slot<T>> q = emitBuffer;
		if (q == null) {
			q = EventLoopProcessor.createSingleProducer(bufferSize);
			emitBuffer = q;
		}
		return q;
	}

	final long buffer(T value) {
		RingBuffer<EventLoopProcessor.Slot<T>> q = getMainQueue();

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
		RingBuffer<EventLoopProcessor.Slot<T>> q = null;
		for (; ; ) {
			EmitterSubscriber<?>[] inner = subscribers;
			if (inner == CANCELLED) {
				cancel();
				return;
			}
			boolean d = done;

			if (d && inner == EMPTY) {
				return;
			}

			int n = inner.length;

			if (n != 0) {
				Sequence innerSequence;
				long _r;

				for (int i = 0; i < n; i++) {
					@SuppressWarnings("unchecked") EmitterSubscriber<T> is = (EmitterSubscriber<T>) inner[i];

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
					if (!is.unbounded && innerSequence != null && r > 0) {
						_r = r;

						boolean unbounded = _r == Long.MAX_VALUE;
						T oo;

						long cursor = innerSequence.getAsLong();
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

						if (!unbounded && r > _r) {
							EmitterSubscriber.REQUESTED.addAndGet(is, _r - r);
						}

					}
					else {
						_r = 0L;
					}

					if (d) {
						checkTerminal(is, innerSequence, _r);
					}
				}

				if (!done && firstDrain) {
					Subscription s = upstreamSubscription;
					if (s != null) {
						firstDrain = false;
						s.request(bufferSize);
					}
				}
				else {
					if (q != null) {
						requestMore(q.getPending());
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
		if ((e != null && r == 0) || innerSequence == null || is.unbounded || innerSequence.getAsLong() >= emitBuffer.getCursor()) {
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

	final void startAllTrackers(EmitterSubscriber<?>[] inner, long seq, int size) {
		Sequence poll;
		for (int i = 0; i < size - 1; i++) {
			poll = inner[i].pollCursor;
			if (poll == null) {
				inner[i].startTracking(seq + 1);
			}
		}
		inner[size - 1].startTracking(seq);
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
				throw Exceptions.failWithOverflow();
			}
			EmitterSubscriber<?>[] b = new EmitterSubscriber[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = inner;
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
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
		if (subscription == Operators.emptySubscription()) {
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
	public long downstreamCount() {
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
			implements Subscription, Trackable, Receiver, Producer {

		public static final long MASK_NOT_SUBSCRIBED = Long.MIN_VALUE;
		final EmitterProcessor<T>   parent;
		final Subscriber<? super T> actual;

		volatile boolean done;

		boolean unbounded = false;

		private volatile long requested = MASK_NOT_SUBSCRIBED;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<EmitterSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(EmitterSubscriber.class, "requested");

		volatile Sequence pollCursor;

		@SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<EmitterSubscriber, Sequence> CURSOR =
				AtomicReferenceFieldUpdater.newUpdater(EmitterSubscriber.class,
						Sequence.class,
						"pollCursor");

		public EmitterSubscriber(EmitterProcessor<T> parent, final Subscriber<? super T> actual) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (Operators.checkRequest(n, actual)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				if (EmitterProcessor.RUNNING.getAndIncrement(parent) == 0) {
					parent.drainLoop();
				}
			}
		}

		@Override
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
			Sequence pollSequence = RingBuffer.newSequence(seq - 1L);
			if (CURSOR.compareAndSet(this, null, pollSequence)) {
				parent.emitBuffer.addGatingSequence(pollSequence);
			}
		}

		void start() {
			if (REQUESTED.compareAndSet(this, MASK_NOT_SUBSCRIBED, 0)) {
				RingBuffer<EventLoopProcessor.Slot<T>> ringBuffer = parent.emitBuffer;
				if (ringBuffer != null) {
					startTracking(Math.max(0L, ringBuffer.getMinimumGatingSequence() + 1L));
				}

				actual.onSubscribe(this);
			}
		}

		@Override
		public boolean isCancelled() {
			return done;
		}

		@Override
		public long getPending() {
			return pollCursor == null || done ? -1L : parent.emitBuffer.getCursor() - pollCursor.getAsLong();
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
