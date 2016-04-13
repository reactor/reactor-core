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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.MultiProducer;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.queue.RingBuffer;
import reactor.core.queue.Slot;
import reactor.core.scheduler.Scheduler;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.state.Completable;
import reactor.core.state.Introspectable;
import reactor.core.state.Prefetchable;
import reactor.core.state.Requestable;

import reactor.core.subscriber.Subscribers;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.PlatformDependent;
import reactor.core.util.Sequence;

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
 *     The RingBuffer nature gives this processor a natural ability to replay data to late subscribers that can be
 *     set with {@link #replay}.
 * <img width="640" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/emitterreplay.png" alt="">
 * <p>
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public final class EmitterProcessor<T> extends FluxProcessor<T, T>
		implements MultiProducer, Completable, Cancellable, Prefetchable, Backpressurable {

	/**
	 * Create an asynchronously {@link Flux#publishOn(Scheduler) dispatched} {@link FluxProcessor} multicast/topic
	 * relay.
	 * Like {@link Flux#publishOn(Scheduler)} the worker resources will be cleaned accordingly to the {@link Runnable}
	 * {@literal null} protocol.
	 * Unlike {@link TopicProcessor} or {@link WorkQueueProcessor}, the threading resources are not dedicated nor
	 * mandatory.
	 *
	 * @param scheduler a checked {@link reactor.core.scheduler.Scheduler.Worker} factory
	 * @param <IN> The relayed data type
	 *
	 * @return a new asynchronous {@link FluxProcessor}
	 */
	public static <IN> FluxProcessor<IN, IN> async(final Scheduler scheduler) {
		FluxProcessor<IN, IN> emitter = create();
		return create(emitter, emitter.publishOn(scheduler));
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 *
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create() {
		return create(true);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(boolean autoCancel) {
		return create(PlatformDependent.SMALL_BUFFER_SIZE, autoCancel);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize) {
		return create(bufferSize, Integer.MAX_VALUE);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize, int concurrency) {
		return create(bufferSize, concurrency, true);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. 
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize, boolean autoCancel) {
		return create(bufferSize, Integer.MAX_VALUE, autoCancel);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. 
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> create(int bufferSize, int concurrency, boolean autoCancel) {
		return new EmitterProcessor<>(autoCancel, concurrency, bufferSize, -1);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. 
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> replay() {
		return replay(PlatformDependent.SMALL_BUFFER_SIZE);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. 
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> replay(int historySize) {
		return replay(historySize, Integer.MAX_VALUE);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. 
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> replay(int historySize, int concurrency) {
		return replay(historySize, concurrency, false);
	}

	/**
	 * Create a new {@link EmitterProcessor} using {@link PlatformDependent#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel. 
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> EmitterProcessor<E> replay(int historySize, int concurrency, boolean autoCancel) {
		return new EmitterProcessor<>(autoCancel, concurrency, historySize, historySize);
	}

	/**
	 * Create a {@link EmitterProcessor} from hot-cold {@link EmitterProcessor#replay EmitterProcessor}  that will not 
	 * propagate 
	 * cancel upstream if {@link Subscription} has been set. The last emitted item will be replayable to late {@link Subscriber}
	 * (buffer and history size of 1).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/replaylast.png" alt="">
	 *
	 * @param <T>  the relayed type
	 *
	 * @return a non interruptable last item cached pub-sub {@link EmitterProcessor}
	 */
	public static <T> EmitterProcessor<T> replayLast() {
		return replayLastOrDefault(null);
	}

	/**
	 * Create a {@link EmitterProcessor} from hot-cold {@link EmitterProcessor#replay EmitterProcessor}  that will not 
	 * propagate 
	 * cancel upstream if {@link Subscription} has been set. The last emitted item will be replayable to late {@link Subscriber} (buffer and history size of 1).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/replaylastd.png" alt="">
	 *
	 * @param value a default value to start the sequence with
	 * @param <T> the relayed type
	 *
	 * @return a non interruptable last item cached pub-sub {@link EmitterProcessor}
	 */
	public static <T> EmitterProcessor<T> replayLastOrDefault(T value) {
		EmitterProcessor<T> b = replay(1);
		if(value != null){
			b.onNext(value);
		}
		return b;
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
		return new DelegateProcessor<>(processor, Subscribers.serialize(processor));
	}

	final int maxConcurrency;
	final int bufferSize;
	final int limit;
	final int replay;
	final boolean autoCancel;

	private volatile RingBuffer<Slot<T>> emitBuffer;

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

	boolean firstDrain = true;

	EmitterProcessor(boolean autoCancel, int maxConcurrency, int bufferSize, int replayLastN) {
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
		EmitterSubscriber<T> inner = new EmitterSubscriber<T>(this, s);
		try {
			addInner(inner);
			if (upstreamSubscription != null) {
				inner.start();
			}
		}
		catch (Exceptions.CancelException c) {
			//IGNORE
		}
		catch (Throwable t) {
			removeInner(inner, EMPTY);
			EmptySubscription.error(s, t);
		}
	}

	@Override
	public long getPending() {
		return (emitBuffer == null ? -1L : emitBuffer.getPending());
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
	public EmitterProcessor<T> connect() {
		onSubscribe(EmptySubscription.INSTANCE);
		return this;
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

				if (is.unbounded && replay == -1) {
					is.actual.onNext(t);
				}
				else {
					long r = is.requested;
					is.unbounded = r == Long.MAX_VALUE && replay == -1 ;
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
		super.onError(t);
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
		return done && (emitBuffer == null || emitBuffer.getPending() == 0L);
	}

	@Override
	public long limit() {
		return limit;
	}

	@Override
	public long expectedFromUpstream() {
		return outstanding;
	}

	RingBuffer<Slot<T>> getMainQueue() {
		RingBuffer<Slot<T>> q = emitBuffer;
		if (q == null) {
			q = RingBuffer.createSingleProducer(bufferSize);
			emitBuffer = q;
		}
		return q;
	}

	final long buffer(T value) {
		RingBuffer<Slot<T>> q = getMainQueue();

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
		RingBuffer<Slot<T>> q = null;
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
						requestMore((int) q.getPending());
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
			implements Subscription, Introspectable, Completable, Cancellable, Backpressurable, Receiver,
			           Requestable, Producer {

		public static final long MASK_NOT_SUBSCRIBED = Long.MIN_VALUE;
		final EmitterProcessor<T>   parent;
		final Subscriber<? super T> actual;

		volatile boolean done;

		boolean unbounded = false;

		@SuppressWarnings("unused")
		private volatile long requested = MASK_NOT_SUBSCRIBED;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<EmitterSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(EmitterSubscriber.class, "requested");

		volatile Sequence pollCursor;

		static final AtomicReferenceFieldUpdater<EmitterSubscriber, Sequence> CURSOR =
				PlatformDependent.newAtomicReferenceFieldUpdater(EmitterSubscriber.class, "pollCursor");

		public EmitterSubscriber(EmitterProcessor<T> parent, final Subscriber<? super T> actual) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.checkRequest(n, actual)) {
				BackpressureUtils.getAndAddCap(REQUESTED, this, n);
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
			Sequence pollSequence = RingBuffer.newSequence(seq - 1L);
			if (CURSOR.compareAndSet(this, null, pollSequence)) {
				parent.emitBuffer.addGatingSequence(pollSequence);
			}
		}

		void start() {
			if (REQUESTED.compareAndSet(this, MASK_NOT_SUBSCRIBED, 0)) {
				RingBuffer<Slot<T>> ringBuffer = parent.emitBuffer;
				if (ringBuffer != null) {
					if (parent.replay > 0) {
						long cursor = ringBuffer.getCursor();
						startTracking(Math.max(0L,
								cursor - Math.min(parent.replay, cursor % ringBuffer.getCapacity())));
					}
					else {
						startTracking(Math.max(0L, ringBuffer.getMinimumGatingSequence() + 1L));
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
		public int getMode() {
			return INNER;
		}

		@Override
		public String getName() {
			return EmitterSubscriber.class.getSimpleName();
		}

		@Override
		public Subscriber<? super T> downstream() {
			return actual;
		}
	}


}
