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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.QueueSupplier;

import static reactor.core.publisher.FluxReplay.ReplaySubscriber.EMPTY;
import static reactor.core.publisher.FluxReplay.ReplaySubscriber.TERMINATED;

/**
 * Replays all or the last N items to Subscribers.
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.0.M1/src/docs/marble/emitterreplay.png"
 * alt="">
 * <p>
 *
 * @param <T> the value type
 */
public final class ReplayProcessor<T> extends FluxProcessor<T, T>
		implements Fuseable {

	/**
	 * Create a {@link ReplayProcessor} from hot-cold {@link ReplayProcessor#create
	 * ReplayProcessor}  that will not propagate cancel upstream if {@link Subscription}
	 * has been set. The last emitted item will be replayable to late {@link Subscriber}
	 * (buffer and history size of 1).
	 * <p>
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.0.M1/src/docs/marble/replaylast.png"
	 * alt="">
	 *
	 * @param <T> the relayed type
	 *
	 * @return a non interruptable last item cached pub-sub {@link ReplayProcessor}
	 */
	public static <T> ReplayProcessor<T> cacheLast() {
		return cacheLastOrDefault(null);
	}

	/**
	 * Create a {@link ReplayProcessor} from hot-cold {@link ReplayProcessor#create
	 * ReplayProcessor}  that will not propagate cancel upstream if {@link Subscription}
	 * has been set. The last emitted item will be replayable to late {@link Subscriber}
	 * (buffer and history size of 1).
	 * <p>
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.0.M1/src/docs/marble/replaylastd.png"
	 * alt="">
	 *
	 * @param value a default value to start the sequence with
	 * @param <T> the relayed type
	 *
	 * @return a non interruptable last item cached pub-sub {@link ReplayProcessor}
	 */
	public static <T> ReplayProcessor<T> cacheLastOrDefault(T value) {
		ReplayProcessor<T> b = create(1);
		if (value != null) {
			b.onNext(value);
		}
		return b;
	}

	/**
	 * Create a new {@link ReplayProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE}
	 * backlog size, blockingWait Strategy and auto-cancel.
	 *
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> ReplayProcessor<E> create() {
		return create(QueueSupplier.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link ReplayProcessor} using a provided backlog size, blockingWait
	 * Strategy and auto-cancel.
	 *
	 * @param historySize the backlog size, ie. maximum items retained
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> ReplayProcessor<E> create(int historySize) {
		return create(historySize, false);
	}

	/**
	 * Create a new {@link ReplayProcessor} using a provided backlog size, blockingWait
	 * Strategy and auto-cancel.
	 *
	 * @param historySize maximum items retained if bounded, or link size if unbounded
	 * @param unbounded true if "unlimited" data store must be supplied
	 * @param <E> Type of processed signals
	 *
	 * @return a fresh processor
	 */
	public static <E> ReplayProcessor<E> create(int historySize, boolean unbounded) {
		FluxReplay.ReplayBuffer<E> buffer;
		if (unbounded) {
			buffer = new FluxReplay.UnboundedReplayBuffer<>(historySize);
		}
		else {
			buffer = new FluxReplay.SizeBoundReplayBuffer<>(historySize);
		}
		return new ReplayProcessor<>(buffer);
	}

	/**
	 * Creates a time-bounded replay processor.
	 * <p>
	 * In this setting, the {@code ReplayProcessor} internally tags each observed item
	 * with a timestamp value supplied by the {@link Schedulers#parallel()} and keeps only
	 * those whose age is less than the supplied time value converted to milliseconds. For
	 * example, an item arrives at T=0 and the max age is set to 5; at T&gt;=5 this first
	 * item is then evicted by any subsequent item or termination signal, leaving the
	 * buffer empty.
	 * <p>
	 * Once the processor is terminated, subscribers subscribing to it will receive items
	 * that remained in the buffer after the terminal signal, regardless of their age.
	 * <p>
	 * If an subscriber subscribes while the {@code ReplayProcessor} is active, it will
	 * observe only those items from within the buffer that have an age less than the
	 * specified time, and each item observed thereafter, even if the buffer evicts items
	 * due to the time constraint in the mean time. In other words, once an subscriber
	 * subscribes, it observes items without gaps in the sequence except for any outdated
	 * items at the beginning of the sequence.
	 * <p>
	 * Note that terminal signals ({@code onError} and {@code onComplete}) trigger
	 * eviction as well. For example, with a max age of 5, the first item is observed at
	 * T=0, then an {@code onComplete} signal arrives at T=10. If an subscriber subscribes
	 * at T=11, it will find an empty {@code ReplayProcessor} with just an {@code
	 * onCompleted} signal.
	 *
	 * @param <T> the type of items observed and emitted by the Processor
	 * @param maxAge the maximum age of the contained items
	 *
	 * @return a new {@link ReplayProcessor}
	 */
	public static <T> ReplayProcessor<T> createTimeout(Duration maxAge) {
		return createTimeout(maxAge, Schedulers.parallel());
	}

	/**
	 * Creates a time-bounded replay processor.
	 * <p>
	 * In this setting, the {@code ReplayProcessor} internally tags each observed item
	 * with a timestamp value supplied by the {@link Scheduler} and keeps only
	 * those whose age is less than the supplied time value converted to milliseconds. For
	 * example, an item arrives at T=0 and the max age is set to 5; at T&gt;=5 this first
	 * item is then evicted by any subsequent item or termination signal, leaving the
	 * buffer empty.
	 * <p>
	 * Once the processor is terminated, subscribers subscribing to it will receive items
	 * that remained in the buffer after the terminal signal, regardless of their age.
	 * <p>
	 * If an subscriber subscribes while the {@code ReplayProcessor} is active, it will
	 * observe only those items from within the buffer that have an age less than the
	 * specified time, and each item observed thereafter, even if the buffer evicts items
	 * due to the time constraint in the mean time. In other words, once an subscriber
	 * subscribes, it observes items without gaps in the sequence except for any outdated
	 * items at the beginning of the sequence.
	 * <p>
	 * Note that terminal signals ({@code onError} and {@code onComplete}) trigger
	 * eviction as well. For example, with a max age of 5, the first item is observed at
	 * T=0, then an {@code onComplete} signal arrives at T=10. If an subscriber subscribes
	 * at T=11, it will find an empty {@code ReplayProcessor} with just an {@code
	 * onCompleted} signal.
	 *
	 * @param <T> the type of items observed and emitted by the Processor
	 * @param maxAge the maximum age of the contained items
	 *
	 * @return a new {@link ReplayProcessor}
	 */
	public static <T> ReplayProcessor<T> createTimeout(Duration maxAge, Scheduler scheduler) {
		return createSizeAndTimeout(Integer.MAX_VALUE, maxAge, scheduler);
	}

	/**
	 * Creates a time- and size-bounded replay processor.
	 * <p>
	 * In this setting, the {@code ReplayProcessor} internally tags each received item
	 * with a timestamp value supplied by the {@link Schedulers#parallel()} and holds at
	 * most
	 * {@code size} items in its internal buffer. It evicts items from the start of the
	 * buffer if their age becomes less-than or equal to the supplied age in milliseconds
	 * or the buffer reaches its {@code size} limit.
	 * <p>
	 * When subscribers subscribe to a terminated {@code ReplayProcessor}, they observe
	 * the items that remained in the buffer after the terminal signal, regardless of
	 * their age, but at most {@code size} items.
	 * <p>
	 * If an subscriber subscribes while the {@code ReplayProcessor} is active, it will
	 * observe only those items from within the buffer that have age less than the
	 * specified time and each subsequent item, even if the buffer evicts items due to the
	 * time constraint in the mean time. In other words, once an subscriber subscribes, it
	 * observes items without gaps in the sequence except for the outdated items at the
	 * beginning of the sequence.
	 * <p>
	 * Note that terminal signals ({@code onError} and {@code onComplete}) trigger
	 * eviction as well. For example, with a max age of 5, the first item is observed at
	 * T=0, then an {@code onComplete} signal arrives at T=10. If an Subscriber subscribes
	 * at T=11, it will find an empty {@code ReplayProcessor} with just an {@code
	 * onCompleted} signal.
	 *
	 * @param <T> the type of items observed and emitted by the Processor
	 * @param maxAge the maximum age of the contained items
	 * @param size the maximum number of buffered items
	 *
	 * @return a new {@link ReplayProcessor}
	 */
	public static <T> ReplayProcessor<T> createSizeAndTimeout(int size, Duration maxAge) {
		return createSizeAndTimeout(size, maxAge, Schedulers.parallel());
	}

	/**
	 * Creates a time- and size-bounded replay processor.
	 * <p>
	 * In this setting, the {@code ReplayProcessor} internally tags each received item
	 * with a timestamp value supplied by the {@link Scheduler} and holds at most
	 * {@code size} items in its internal buffer. It evicts items from the start of the
	 * buffer if their age becomes less-than or equal to the supplied age in milliseconds
	 * or the buffer reaches its {@code size} limit.
	 * <p>
	 * When subscribers subscribe to a terminated {@code ReplayProcessor}, they observe
	 * the items that remained in the buffer after the terminal signal, regardless of
	 * their age, but at most {@code size} items.
	 * <p>
	 * If an subscriber subscribes while the {@code ReplayProcessor} is active, it will
	 * observe only those items from within the buffer that have age less than the
	 * specified time and each subsequent item, even if the buffer evicts items due to the
	 * time constraint in the mean time. In other words, once an subscriber subscribes, it
	 * observes items without gaps in the sequence except for the outdated items at the
	 * beginning of the sequence.
	 * <p>
	 * Note that terminal signals ({@code onError} and {@code onComplete}) trigger
	 * eviction as well. For example, with a max age of 5, the first item is observed at
	 * T=0, then an {@code onComplete} signal arrives at T=10. If an Subscriber subscribes
	 * at T=11, it will find an empty {@code ReplayProcessor} with just an {@code
	 * onCompleted} signal.
	 *
	 * @param <T> the type of items observed and emitted by the Processor
	 * @param maxAge the maximum age of the contained items in milliseconds
	 * @param size the maximum number of buffered items
	 * @param scheduler the {@link Scheduler} that provides the current time
	 *
	 * @return a new {@link ReplayProcessor}
	 */
	public static <T> ReplayProcessor<T> createSizeAndTimeout(int size,
			Duration maxAge,
			Scheduler scheduler) {
		Objects.requireNonNull(scheduler, "scheduler is null");
		if (size <= 0) {
			throw new IllegalArgumentException("size > 0 required but it was " + size);
		}
		return new ReplayProcessor<>(new FluxReplay.SizeAndTimeBoundReplayBuffer<>(size,
				maxAge.toMillis(),
				scheduler));
	}

	final FluxReplay.ReplayBuffer<T> buffer;

	Subscription subscription;

	volatile FluxReplay.ReplaySubscription<T>[] subscribers;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<ReplayProcessor, FluxReplay.ReplaySubscription[]>
			SUBSCRIBERS = AtomicReferenceFieldUpdater.newUpdater(ReplayProcessor.class,
			FluxReplay.ReplaySubscription[].class,
			"subscribers");

	ReplayProcessor(FluxReplay.ReplayBuffer<T> buffer) {
		this.buffer = buffer;
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
		FluxReplay.ReplaySubscription<T> rs = new ReplayInner<>(s, this);
		s.onSubscribe(rs);

		if (add(rs)) {
			if (rs.isCancelled()) {
				remove(rs);
				return;
			}
		}
		buffer.replay(rs);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == ScannableAttr.PARENT){
			return subscription;
		}
		return super.scanUnsafe(key);
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	@Override
	public long downstreamCount() {
		return subscribers.length;
	}

	@Override
	public boolean isTerminated() {
		return buffer.isDone();
	}

	boolean add(FluxReplay.ReplaySubscription<T> rs) {
		for (; ; ) {
			FluxReplay.ReplaySubscription<T>[] a = subscribers;
			if (a == TERMINATED) {
				return false;
			}
			int n = a.length;

			@SuppressWarnings("unchecked") FluxReplay.ReplaySubscription<T>[] b =
					new ReplayInner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = rs;
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return true;
			}
		}
	}

	@SuppressWarnings("unchecked")
	void remove(FluxReplay.ReplaySubscription<T> rs) {
		outer:
		for (; ; ) {
			FluxReplay.ReplaySubscription<T>[] a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			int n = a.length;

			for (int i = 0; i < n; i++) {
				if (a[i] == rs) {
					FluxReplay.ReplaySubscription<T>[] b;

					if (n == 1) {
						b = EMPTY;
					}
					else {
						b = new ReplayInner[n - 1];
						System.arraycopy(a, 0, b, 0, i);
						System.arraycopy(a, i + 1, b, i, n - i - 1);
					}

					if (SUBSCRIBERS.compareAndSet(this, a, b)) {
						return;
					}

					continue outer;
				}
			}

			break;
		}
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (buffer.isDone()) {
			s.cancel();
		}
		else if (Operators.validate(subscription, s)) {
			subscription = s;
			s.request(Long.MAX_VALUE);
		}
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void onNext(T t) {
		FluxReplay.ReplayBuffer<T> b = buffer;
		if (b.isDone()) {
			Operators.onNextDropped(t);
		}
		else {
			b.add(t);
			for (FluxReplay.ReplaySubscription<T> rs : subscribers) {
				b.replay(rs);
			}
		}
	}

	@Override
	public void onError(Throwable t) {
		FluxReplay.ReplayBuffer<T> b = buffer;
		if (b.isDone()) {
			Operators.onErrorDropped(t);
		}
		else {
			b.onError(t);

			@SuppressWarnings("unchecked") FluxReplay.ReplaySubscription<T>[] a =
					SUBSCRIBERS.getAndSet(this, TERMINATED);

			for (FluxReplay.ReplaySubscription<T> rs : a) {
				b.replay(rs);
			}
		}
	}

	@Override
	public void onComplete() {
		FluxReplay.ReplayBuffer<T> b = buffer;
		if (!b.isDone()) {
			b.onComplete();

			@SuppressWarnings("unchecked") FluxReplay.ReplaySubscription<T>[] a =
					SUBSCRIBERS.getAndSet(this, TERMINATED);

			for (FluxReplay.ReplaySubscription<T> rs : a) {
				b.replay(rs);
			}
		}
	}

	static final class ReplayInner<T>
			implements FluxReplay.ReplaySubscription<T> {

		final Subscriber<? super T> actual;

		final ReplayProcessor<T> parent;

		final FluxReplay.ReplayBuffer<T> buffer;

		int index;

		int tailIndex;

		Object node;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplayInner> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ReplayInner.class,
						"wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ReplayInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ReplayInner.class,
						"requested");

		volatile boolean cancelled;

		int fusionMode;

		ReplayInner(Subscriber<? super T> actual,
				ReplayProcessor<T> parent) {
			this.actual = actual;
			this.parent = parent;
			this.buffer = parent.buffer;
		}

		@Override
		public long requested() {
			return requested;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0) {
				fusionMode = ASYNC;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public T poll() {
			return buffer.poll(this);
		}

		@Override
		public void clear() {
			buffer.clear(this);
		}

		@Override
		public boolean isEmpty() {
			return buffer.isEmpty(this);
		}

		@Override
		public int size() {
			return buffer.size(this);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (fusionMode() == NONE) {
					Operators.getAndAddCap(REQUESTED, this, n);
				}
				buffer.replay(this);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				parent.remove(this);

				if (enter()) {
					node = null;
				}
			}
		}

		@Override
		public void node(Object node) {
			this.node = node;
		}

		@Override
		public int fusionMode() {
			return fusionMode;
		}

		@Override
		public Object node() {
			return node;
		}

		@Override
		public int index() {
			return index;
		}

		@Override
		public void index(int index) {
			this.index = index;
		}

		@Override
		public int tailIndex() {
			return tailIndex;
		}

		@Override
		public void tailIndex(int tailIndex) {
			this.tailIndex = tailIndex;
		}

		@Override
		public boolean enter() {
			return WIP.getAndIncrement(this) == 0;
		}

		@Override
		public int leave(int missed) {
			return WIP.addAndGet(this, -missed);
		}

		@Override
		public void produced(long n) {
			REQUESTED.addAndGet(this, -n);
		}
	}
}