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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
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

/**
 * Replays all or the last N items to Subscribers.
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/emitterreplay.png"
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/replaylast.png"
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
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/replaylastd.png"
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
		ReplayBuffer<E> buffer;
		if (unbounded) {
			buffer = new UnboundedReplayBuffer<>(historySize);
		}
		else {
			buffer = new SizeBoundReplayBuffer<>(historySize);
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
		return new ReplayProcessor<>(new SizeAndTimeBoundReplayBuffer<>(size,
				maxAge.toMillis(),
				scheduler));
	}

	final ReplayBuffer<T> buffer;

	Subscription subscription;

	volatile ReplaySubscription<T>[] subscribers;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<ReplayProcessor, ReplaySubscription[]>
			SUBSCRIBERS = AtomicReferenceFieldUpdater.newUpdater(ReplayProcessor.class,
			ReplaySubscription[].class,
			"subscribers");

	@SuppressWarnings("rawtypes")
	static final ReplaySubscription[] EMPTY      = new ReplaySubscription[0];
	@SuppressWarnings("rawtypes")
	static final ReplaySubscription[] TERMINATED = new ReplaySubscription[0];

	ReplayProcessor(ReplayBuffer<T> buffer) {
		this.buffer = buffer;
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
		ReplaySubscription<T> rs = new ReplayInner<>(s, this);
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
	public Object scan(Attr key) {
		if(key == Attr.PARENT){
			return subscription;
		}
		return super.scan(key);
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
	public int getBufferSize() {
		return buffer.capacity();
	}

	@Override
	public boolean isTerminated() {
		return buffer.isDone();
	}

	@Override
	public boolean isStarted() {
		return subscription != null;
	}

	boolean add(ReplaySubscription<T> rs) {
		for (; ; ) {
			ReplaySubscription<T>[] a = subscribers;
			if (a == TERMINATED) {
				return false;
			}
			int n = a.length;

			@SuppressWarnings("unchecked") ReplaySubscription<T>[] b =
					new ReplayInner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = rs;
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return true;
			}
		}
	}

	@SuppressWarnings("unchecked")
	void remove(ReplaySubscription<T> rs) {
		outer:
		for (; ; ) {
			ReplaySubscription<T>[] a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			int n = a.length;

			for (int i = 0; i < n; i++) {
				if (a[i] == rs) {
					ReplaySubscription<T>[] b;

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
	public long getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void onNext(T t) {
		ReplayBuffer<T> b = buffer;
		if (b.isDone()) {
			Operators.onNextDropped(t);
		}
		else {
			b.add(t);
			for (ReplaySubscription<T> rs : subscribers) {
				b.replay(rs);
			}
		}
	}

	@Override
	public void onError(Throwable t) {
		ReplayBuffer<T> b = buffer;
		if (b.isDone()) {
			Operators.onErrorDropped(t);
		}
		else {
			b.onError(t);

			@SuppressWarnings("unchecked") ReplaySubscription<T>[] a =
					SUBSCRIBERS.getAndSet(this, TERMINATED);

			for (ReplaySubscription<T> rs : a) {
				b.replay(rs);
			}
		}
	}

	@Override
	public void onComplete() {
		ReplayBuffer<T> b = buffer;
		if (!b.isDone()) {
			b.onComplete();

			@SuppressWarnings("unchecked") ReplaySubscription<T>[] a =
					SUBSCRIBERS.getAndSet(this, TERMINATED);

			for (ReplaySubscription<T> rs : a) {
				b.replay(rs);
			}
		}
	}

	@Override
	public ReplayProcessor<T> connect() {
		onSubscribe(Operators.emptySubscription());
		return this;
	}

	interface ReplayBuffer<T> {

		void add(T value);

		void onError(Throwable ex);

		Throwable getError();

		void onComplete();

		void replay(ReplaySubscription<T> rs);

		boolean isDone();

		T poll(ReplaySubscription<T> rs);

		void clear(ReplaySubscription<T> rs);

		boolean isEmpty(ReplaySubscription<T> rs);

		int size(ReplaySubscription<T> rs);

		int size();

		int capacity();
	}

	static final class UnboundedReplayBuffer<T> implements ReplayBuffer<T> {

		final int batchSize;

		volatile int size;

		final Object[] head;

		Object[] tail;

		int tailIndex;

		volatile boolean done;
		Throwable error;

		UnboundedReplayBuffer(int batchSize) {
			this.batchSize = batchSize;
			Object[] n = new Object[batchSize + 1];
			this.tail = n;
			this.head = n;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public int capacity() {
			return Integer.MAX_VALUE;
		}

		@Override
		public void add(T value) {
			int i = tailIndex;
			Object[] a = tail;
			if (i == a.length - 1) {
				Object[] b = new Object[a.length];
				b[0] = value;
				tailIndex = 1;
				a[i] = b;
				tail = b;
			}
			else {
				a[i] = value;
				tailIndex = i + 1;
			}
			size++;
		}

		@Override
		public void onError(Throwable ex) {
			error = ex;
			done = true;
		}

		@Override
		public void onComplete() {
			done = true;
		}

		void replayNormal(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();
			final int n = batchSize;

			for (; ; ) {

				long r = rs.requested();
				long e = 0L;

				Object[] node = (Object[]) rs.node();
				if (node == null) {
					node = head;
				}
				int tailIndex = rs.tailIndex();
				int index = rs.index();

				while (e != r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					boolean empty = index == size;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}

					if (empty) {
						break;
					}

					if (tailIndex == n) {
						node = (Object[]) node[tailIndex];
						tailIndex = 0;
					}

					@SuppressWarnings("unchecked") T v = (T) node[tailIndex];

					a.onNext(v);

					e++;
					tailIndex++;
					index++;
				}

				if (e == r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					boolean empty = index == size;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}
				}

				if (e != 0L) {
					if (r != Long.MAX_VALUE) {
						rs.produced(e);
					}
				}

				rs.index(index);
				rs.tailIndex(tailIndex);
				rs.node(node);

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void replayFused(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {

				if (rs.isCancelled()) {
					rs.node(null);
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void replay(ReplaySubscription<T> rs) {
			if (!rs.enter()) {
				return;
			}

			if (rs.fusionMode() == NONE) {
				replayNormal(rs);
			}
			else {
				replayFused(rs);
			}
		}

		@Override
		public boolean isDone() {
			return done;
		}

		@Override
		public T poll(ReplaySubscription<T> rs) {
			int index = rs.index();
			if (index == size) {
				return null;
			}
			Object[] node = (Object[]) rs.node();
			if (node == null) {
				node = head;
				rs.node(node);
			}
			int tailIndex = rs.tailIndex();
			if (tailIndex == batchSize) {
				node = (Object[]) node[tailIndex];
				tailIndex = 0;
				rs.node(node);
			}
			@SuppressWarnings("unchecked") T v = (T) node[tailIndex];
			rs.index(index + 1);
			rs.tailIndex(tailIndex + 1);
			return v;
		}

		@Override
		public void clear(ReplaySubscription<T> rs) {
			rs.node(null);
		}

		@Override
		public boolean isEmpty(ReplaySubscription<T> rs) {
			return rs.index() == size;
		}

		@Override
		public int size(ReplaySubscription<T> rs) {
			return size - rs.index();
		}

		@Override
		public int size() {
			return size;
		}

	}

	static final class SizeBoundReplayBuffer<T> implements ReplayBuffer<T> {

		final int limit;

		volatile Node<T> head;

		Node<T> tail;

		int size;

		volatile boolean done;
		Throwable error;

		SizeBoundReplayBuffer(int limit) {
			if(limit < 0){
				throw new IllegalArgumentException("Limit cannot be negative");
			}
			this.limit = limit;
			Node<T> n = new Node<>(null);
			this.tail = n;
			this.head = n;
		}

		@Override
		public int capacity() {
			return limit;
		}

		@Override
		public void add(T value) {
			Node<T> n = new Node<>(value);
			tail.set(n);
			tail = n;
			int s = size;
			if (s == limit) {
				head = head.get();
			}
			else {
				size = s + 1;
			}
		}

		@Override
		public void onError(Throwable ex) {
			error = ex;
			done = true;
		}

		@Override
		public void onComplete() {
			done = true;
		}

		void replayNormal(ReplaySubscription<T> rs) {
			final Subscriber<? super T> a = rs.actual();

			int missed = 1;

			for (; ; ) {

				long r = rs.requested();
				long e = 0L;

				@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
				if (node == null) {
					node = head;
				}

				while (e != r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					Node<T> next = node.get();
					boolean empty = next == null;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(next.value);

					e++;
					node = next;
				}

				if (e == r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					boolean empty = node.get() == null;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}
				}

				if (e != 0L) {
					if (r != Long.MAX_VALUE) {
						rs.produced(e);
					}
				}

				rs.node(node);

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void replayFused(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {

				if (rs.isCancelled()) {
					rs.node(null);
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void replay(ReplaySubscription<T> rs) {
			if (!rs.enter()) {
				return;
			}

			if (rs.fusionMode() == NONE) {
				replayNormal(rs);
			}
			else {
				replayFused(rs);
			}
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public boolean isDone() {
			return done;
		}

		static final class Node<T> extends AtomicReference<Node<T>> {

			/** */
			private static final long serialVersionUID = 3713592843205853725L;

			final T value;

			Node(T value) {
				this.value = value;
			}
		}

		@Override
		public T poll(ReplaySubscription<T> rs) {
			@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
			if (node == null) {
				node = head;
				rs.node(node);
			}

			Node<T> next = node.get();
			if (next == null) {
				return null;
			}
			rs.node(next);

			return next.value;
		}

		@Override
		public void clear(ReplaySubscription<T> rs) {
			rs.node(null);
		}

		@Override
		public boolean isEmpty(ReplaySubscription<T> rs) {
			@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
			if (node == null) {
				node = head;
				rs.node(node);
			}
			return node.get() == null;
		}

		@Override
		public int size(ReplaySubscription<T> rs) {
			@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
			if (node == null) {
				node = head;
			}
			int count = 0;

			Node<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}

		@Override
		public int size() {
			Node<T> node = head;
			int count = 0;

			Node<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}
	}

	static final class SizeAndTimeBoundReplayBuffer<T> implements ReplayBuffer<T> {

		static final class TimedNode<T> extends AtomicReference<TimedNode<T>> {

			final T    value;
			final long time;

			TimedNode(T value, long time) {
				this.value = value;
				this.time = time;
			}
		}

		final int            limit;
		final long           maxAge;
		final Scheduler scheduler;
		int size;

		volatile TimedNode<T> head;

		TimedNode<T> tail;

		Throwable error;
		volatile boolean done;

		SizeAndTimeBoundReplayBuffer(int limit,
				long maxAge,
				Scheduler scheduler) {
			this.limit = limit;
			this.maxAge = maxAge;
			this.scheduler = scheduler;
			TimedNode<T> h = new TimedNode<>(null, 0L);
			this.tail = h;
			this.head = h;
		}

		@SuppressWarnings("unchecked")
		void replayNormal(ReplaySubscription<T> rs) {
			int missed = 1;
			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {
				@SuppressWarnings("unchecked") TimedNode<T> node =
						(TimedNode<T>) rs.node();
				if (node == null) {
					node = head;
					if (!done) {
						// skip old entries
						long limit = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;
						TimedNode<T> next = node;
						while (next != null) {
							long ts = next.time;
							if (ts > limit) {
								break;
							}
							node = next;
							next = node.get();
						}
					}
				}

				long r = rs.requested();
				long e = 0L;

				while (e != r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					TimedNode<T> next = node.get();
					boolean empty = next == null;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(next.value);

					e++;
					node = next;
				}

				if (e == r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					boolean empty = node.get() == null;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}
				}

				if (e != 0L) {
					if (r != Long.MAX_VALUE) {
						rs.produced(e);
					}
				}

				rs.node(node);

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void replayFused(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {

				if (rs.isCancelled()) {
					rs.node(null);
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void onError(Throwable ex) {
			done = true;
			error = ex;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public void onComplete() {
			done = true;
		}

		@Override
		public boolean isDone() {
			return done;
		}

		@SuppressWarnings("unchecked")
		TimedNode<T> latestHead(ReplaySubscription<T> rs) {
			long now = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;

			TimedNode<T> h = (TimedNode<T>) rs.node();
			if (h == null) {
				h = head;
			}
			TimedNode<T> n;
			while ((n = h.get()) != null) {
				if (n.time > now) {
					break;
				}
				h = n;
			}
			return h;
		}

		@Override
		public T poll(ReplaySubscription<T> rs) {
			TimedNode<T> node = latestHead(rs);
			TimedNode<T> next;
			long now = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;
			while ((next = node.get()) != null) {
				if (next.time > now) {
					node = next;
					break;
				}
				node = next;
			}
			if (next == null) {
				return null;
			}
			rs.node(next);

			return node.value;
		}

		@Override
		public void clear(ReplaySubscription<T> rs) {
			rs.node(null);
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean isEmpty(ReplaySubscription<T> rs) {
			TimedNode<T> node = latestHead(rs);
			return node.get() == null;
		}

		@Override
		public int size(ReplaySubscription<T> rs) {
			TimedNode<T> node = latestHead(rs);
			int count = 0;

			TimedNode<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}

		@Override
		public int size() {
			TimedNode<T> node = head;
			int count = 0;

			TimedNode<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}

		@Override
		public int capacity() {
			return limit;
		}

		@Override
		public void add(T value) {
			TimedNode<T> n = new TimedNode<>(value, scheduler.now(TimeUnit.MILLISECONDS));
			tail.set(n);
			tail = n;
			int s = size;
			if (s == limit) {
				head = head.get();
			}
			else {
				size = s + 1;
			}
			long limit = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;

			TimedNode<T> h = head;
			TimedNode<T> next;
			int removed = 0;
			for (; ; ) {
				next = h.get();
				if (next == null) {
					break;
				}

				if (next.time > limit) {
					if (removed != 0) {
						size = size - removed;
						head = h;
					}
					break;
				}

				h = next;
				removed++;
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		public void replay(ReplaySubscription<T> rs) {
			if (!rs.enter()) {
				return;
			}

			if (rs.fusionMode() == NONE) {
				replayNormal(rs);
			}
			else {
				replayFused(rs);
			}
		}
	}

	interface ReplaySubscription<T> extends QueueSubscription<T>, InnerProducer<T> {

		Subscriber<? super T> actual();

		boolean enter();

		int leave(int missed);

		void produced(long n);

		void node(Object node);

		Object node();

		int tailIndex();

		void tailIndex(int tailIndex);

		int index();

		void index(int index);

		int fusionMode();

		boolean isCancelled();

		long requested();
	}

	static final class ReplayInner<T>
			implements ReplaySubscription<T> {

		final Subscriber<? super T> actual;

		final ReplayProcessor<T> parent;

		final ReplayBuffer<T> buffer;

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