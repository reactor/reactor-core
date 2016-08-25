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

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.MultiProducer;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.util.concurrent.QueueSupplier;

/**
 * Replays all or the last N items to Subscribers.
 *
 * <img width="640" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/emitterreplay.png" alt="">
 * <p>
 * 
 * @param <T> the value type
 */
public final class ReplayProcessor<T> 
extends FluxProcessor<T, T> implements Fuseable, MultiProducer, Receiver {

	/**
	 * Create a {@link ReplayProcessor} from hot-cold {@link ReplayProcessor#create ReplayProcessor}  that will not
	 * propagate
	 * cancel upstream if {@link Subscription} has been set. The last emitted item will be replayable to late {@link Subscriber}
	 * (buffer and history size of 1).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/replaylast.png" alt="">
	 *
	 * @param <T>  the relayed type
	 *
	 * @return a non interruptable last item cached pub-sub {@link ReplayProcessor}
	 */
	public static <T> ReplayProcessor<T> cacheLast() {
		return cacheLastOrDefault(null);
	}

	/**
	 * Create a {@link ReplayProcessor} from hot-cold {@link ReplayProcessor#create ReplayProcessor}  that will not
	 * propagate
	 * cancel upstream if {@link Subscription} has been set. The last emitted item will be replayable to late {@link Subscriber} (buffer and history size of 1).
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/replaylastd.png" alt="">
	 *
	 * @param value a default value to start the sequence with
	 * @param <T> the relayed type
	 *
	 * @return a non interruptable last item cached pub-sub {@link ReplayProcessor}
	 */
	public static <T> ReplayProcessor<T> cacheLastOrDefault(T value) {
		ReplayProcessor<T> b = create(1);
		if(value != null){
			b.onNext(value);
		}
		return b;
	}

	/**
	 * Create a new {@link ReplayProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ReplayProcessor<E> create() {
		return create(QueueSupplier.SMALL_BUFFER_SIZE, true);
	}

	/**
	 * Create a new {@link ReplayProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 *
	 * @param historySize
	 *
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> ReplayProcessor<E> create(int historySize) {
		return create(historySize, false);
	}

	/**
	 * Create a new {@link ReplayProcessor} using {@link QueueSupplier#SMALL_BUFFER_SIZE} backlog size, blockingWait
	 * Strategy and auto-cancel.
	 *
	 * @param historySize maximum items retained if bounded, or link size if unbounded
	 * @param  unbounded true if "unlimited" data store must be supplied
	 *
	 * @param <E> Type of processed signals
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
	 * with
	 * a timestamp value supplied by the {@link Schedulers#timer()} and keeps only those whose
	 * age
	 * is less than the supplied time value converted to milliseconds. For example, an
	 * item arrives at T=0 and the max age is set to 5; at T&gt;=5 this first item is then
	 * evicted by any subsequent item or termination signal, leaving the buffer empty.
	 * <p>
	 * Once the processor is terminated, subscribers subscribing to it will receive items that
	 * remained in the buffer after the terminal signal, regardless of their age.
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
	 * T=0, then an {@code onComplete} signal arrives at T=10. If an subscriber
	 * subscribes at T=11, it will find an empty {@code ReplayProcessor} with just an {@code
	 * onCompleted} signal.
	 *
	 * @param <T> the type of items observed and emitted by the Processor
	 * @param maxAge the maximum age of the contained items
	 *
	 * @return a new {@link ReplayProcessor}
	 */
	public static <T> ReplayProcessor<T> createTimeout(Duration maxAge) {
		return createTimeoutMillis(maxAge.toMillis(), Schedulers.timer());
	}

	/**
	 * Creates a time-bounded replay processor.
	 * <p>
	 * In this setting, the {@code ReplayProcessor} internally tags each observed item
	 * with a timestamp value supplied by the {@link TimedScheduler} and keeps only those
	 * whose age is less than the supplied time value converted to milliseconds. For
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
	 * @param maxAge the maximum age of the contained items in milliseconds
	 * @param scheduler the {@link TimedScheduler} that provides the current time
	 *
	 * @return a new {@link ReplayProcessor}
	 */
	public static <T> ReplayProcessor<T> createTimeoutMillis(long maxAge,
			TimedScheduler scheduler) {
		return createSizeAndTimeoutMillis(Integer.MAX_VALUE, maxAge, scheduler);
	}

	/**
	 * Creates a time- and size-bounded replay processor.
	 * <p>
	 * In this setting, the {@code ReplayProcessor} internally tags each received item
	 * with a timestamp value supplied by the {@link Schedulers#timer()} and holds at most
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
		return createSizeAndTimeoutMillis(size, maxAge.toMillis(), Schedulers.timer());
	}

	/**
	 * Creates a time- and size-bounded replay processor.
	 * <p>
	 * In this setting, the {@code ReplayProcessor} internally tags each received item
	 * with a timestamp value supplied by the {@link TimedScheduler} and holds at most
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
	 * @param scheduler the {@link TimedScheduler} that provides the current time
	 *
	 * @return a new {@link ReplayProcessor}
	 */
	public static <T> ReplayProcessor<T> createSizeAndTimeoutMillis(int size,
			long maxAge,
			TimedScheduler scheduler) {
		Objects.requireNonNull(scheduler, "scheduler is null");
		if (size <= 0) {
			throw new IllegalArgumentException("size > 0 required but it was " + size);
		}
		return new ReplayProcessor<>(new SizeAndTimeBoundReplayBuffer<>(size,
				maxAge, TimeUnit.MILLISECONDS, scheduler));
	}

	final ReplayBuffer<T> buffer;

	Subscription subscription;

	volatile ReplaySubscription<T>[] subscribers;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<ReplayProcessor, ReplaySubscription[]>
			SUBSCRIBERS = AtomicReferenceFieldUpdater.newUpdater(ReplayProcessor.class, ReplaySubscription[].class, "subscribers");
	
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

		ReplaySubscription<T> rp = new ReplaySubscription<>(s, this);
		s.onSubscribe(rp);

		if (add(rp)) {
			if (rp.cancelled) {
				remove(rp);
				return;
			}
		}
		buffer.replay(rp);
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
	public long getCapacity() {
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

	@Override
	public Object upstream() {
		return subscription;
	}

	boolean add(ReplaySubscription<T> rp) {
		for (;;) {
			ReplaySubscription<T>[] a = subscribers;
			if (a == TERMINATED) {
				return false;
			}
			int n = a.length;
			
			@SuppressWarnings("unchecked")
			ReplaySubscription<T>[] b = new ReplaySubscription[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = rp;
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return true;
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	void remove(ReplaySubscription<T> rp) {
		outer:
		for (;;) {
			ReplaySubscription<T>[] a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			int n = a.length;
			
			for (int i = 0; i < n; i++) {
				if (a[i] == rp) {
					ReplaySubscription<T>[] b;
					
					if (n == 1) {
						b = EMPTY;
					} else {
						b = new ReplaySubscription[n - 1];
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
		} else {
			if(!Operators.validate(subscription, s)) {
				s.cancel();
				return;
			}
			subscription = s;
			s.request(Long.MAX_VALUE);
		}
	}

	@Override
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}

	@Override
	public void onNext(T t) {
		ReplayBuffer<T> b = buffer;
		if (b.isDone()) {
			Operators.onNextDropped(t);
		}
		else {
			b.add(t);
			for (ReplaySubscription<T> rp : subscribers) {
				b.replay(rp);
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
			
			for (ReplaySubscription<T> rp : a) {
				b.replay(rp);
			}
		}
	}

	@Override
	public void onComplete() {
		ReplayBuffer<T> b = buffer;
		if (!b.isDone()) {
			b.onComplete();
			
			@SuppressWarnings("unchecked")
			ReplaySubscription<T>[] a = SUBSCRIBERS.getAndSet(this, TERMINATED);

			for (ReplaySubscription<T> rp : a) {
				b.replay(rp);
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
		
		void onComplete();

		void replay(ReplaySubscription<T> rp);
		
		boolean isDone();

		T poll(ReplaySubscription<T> rp);

		void clear(ReplaySubscription<T> rp);
		
		boolean isEmpty(ReplaySubscription<T> rp);

		int size(ReplaySubscription<T> rp);

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

		public UnboundedReplayBuffer(int batchSize) {
			this.batchSize = batchSize;
			Object[] n = new Object[batchSize + 1];
			this.tail = n;
			this.head = n;
		}

		@Override
		public int capacity() {
			return batchSize;
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
			} else {
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

		void replayNormal(ReplaySubscription<T> rp) {
			int missed = 1;
			
			final Subscriber<? super T> a = rp.actual;
			final int n = batchSize;

			for (; ; ) {

				long r = rp.requested;
				long e = 0L;

				Object[] node = (Object[]) rp.node;
				if (node == null) {
					node = head;
				}
				int tailIndex = rp.tailIndex;
				int index = rp.index;
				
				while (e != r) {
					if (rp.cancelled) {
						rp.node = null;
						return;
					}
					
					boolean d = done;
					boolean empty = index == size;

					if (d && empty) {
						rp.node = null;
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						} else {
							a.onComplete();
						}
						return;
					}
					
					if (empty) {
						break;
					}
					
					if (tailIndex == n) {
						node = (Object[])node[tailIndex];
						tailIndex = 0;
					}
					
					@SuppressWarnings("unchecked")
					T v = (T)node[tailIndex];
					
					a.onNext(v);
					
					e++;
					tailIndex++;
					index++;
				}
				
				if (e == r) {
					if (rp.cancelled) {
						rp.node = null;
						return;
					}

					boolean d = done;
					boolean empty = index == size;

					if (d && empty) {
						rp.node = null;
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
						rp.produced(e);
					}
				}

				rp.index = index;
				rp.tailIndex = tailIndex;
				rp.node = node;

				missed = rp.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}
		
		void replayFused(ReplaySubscription<T> rp) {
			int missed = 1;
			
			final Subscriber<? super T> a = rp.actual;
			
			for (;;) {
				
				if (rp.cancelled) {
					rp.node = null;
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

				missed = rp.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void replay(ReplaySubscription<T> rp) {
			if (!rp.enter()) {
				return;
			}

			if (rp.fusionMode == NONE) {
				replayNormal(rp);
			} else {
				replayFused(rp);
			}
		}

		@Override
		public boolean isDone() {
			return done;
		}

		@Override
		public T poll(ReplaySubscription<T> rp) {
			int index = rp.index;
			if (index == size) {
				return null;
			}
			Object[] node = (Object[]) rp.node;
			if (node == null) {
				node = head;
				rp.node = node;
			}
			int tailIndex = rp.tailIndex;
			if (tailIndex == batchSize) {
				node = (Object[]) node[tailIndex];
				tailIndex = 0;
			}
			@SuppressWarnings("unchecked")
			T v = (T)node[tailIndex];
			rp.index = index + 1;
			rp.tailIndex = tailIndex + 1;
			return v;
		}

		@Override
		public void clear(ReplaySubscription<T> rp) {
			rp.node = null;
		}

		@Override
		public boolean isEmpty(ReplaySubscription<T> rp) {
			return rp.index == size;
		}

		@Override
		public int size(ReplaySubscription<T> rp) {
			return size - rp.index;
		}

	}

	static final class SizeBoundReplayBuffer<T> implements ReplayBuffer<T> {

		final int limit;
		
		volatile Node<T> head;

		Node<T> tail;

		int size;

		volatile boolean done;
		Throwable error;

		public SizeBoundReplayBuffer(int limit) {
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

		void replayNormal(ReplaySubscription<T> rp) {
			final Subscriber<? super T> a = rp.actual;

			int missed = 1;

			for (;;) {

				long r = rp.requested;
				long e = 0L;

				@SuppressWarnings("unchecked")
				Node<T> node = (Node<T>)rp.node;
				if (node == null) {
					node = head;
				}

				while (e != r) {
					if (rp.cancelled) {
						rp.node = null;
						return;
					}

					boolean d = done;
					Node<T> next = node.get();
					boolean empty = next == null;

					if (d && empty) {
						rp.node = null;
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						} else {
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
					if (rp.cancelled) {
						rp.node = null;
						return;
					}

					boolean d = done;
					boolean empty = node.get() == null;

					if (d && empty) {
						rp.node = null;
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						} else {
							a.onComplete();
						}
						return;
					}
				}

				if (e != 0L) {
					if (r != Long.MAX_VALUE) {
						rp.produced(e);
					}
				}

				rp.node = node;

				missed = rp.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void replayFused(ReplaySubscription<T> rp) {
			int missed = 1;
			
			final Subscriber<? super T> a = rp.actual;
			
			for (;;) {
				
				if (rp.cancelled) {
					rp.node = null;
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

				missed = rp.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void replay(ReplaySubscription<T> rp) {
			if (!rp.enter()) {
				return;
			}
			
			if (rp.fusionMode == NONE) {
				replayNormal(rp);
			} else {
				replayFused(rp);
			}
		}

		@Override
		public boolean isDone() {
			return done;
		}
		
		static final class Node<T> extends AtomicReference<Node<T>> {
			/** */
			private static final long serialVersionUID = 3713592843205853725L;
			
			final T value;
			
			public Node(T value) {
				this.value = value;
			}
		}

		@Override
		public T poll(ReplaySubscription<T> rp) {
			@SuppressWarnings("unchecked")
			Node<T> node = (Node<T>)rp.node;
			if (node == null) {
				node = head;
				rp.node = node;
			}
			
			Node<T> next = node.get();
			if (next == null) {
				return null;
			}
			rp.node = next;
			
			return next.value;
		}

		@Override
		public void clear(ReplaySubscription<T> rp) {
			rp.node = null;
		}

		@Override
		public boolean isEmpty(ReplaySubscription<T> rp) {
			@SuppressWarnings("unchecked")
			Node<T> node = (Node<T>)rp.node;
			if (node == null) {
				node = head;
				rp.node = node;
			}
			return node.get() == null;
		}

		@Override
		public int size(ReplaySubscription<T> rp) {
			@SuppressWarnings("unchecked")
			Node<T> node = (Node<T>)rp.node;
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
	}

	static final class SizeAndTimeBoundReplayBuffer<T> implements ReplayBuffer<T> {

		static final class TimedNode<T> extends AtomicReference<TimedNode<T>> {

			final T    value;
			final long time;

			public TimedNode(T value, long time) {
				this.value = value;
				this.time = time;
			}
		}

		final int            limit;
		final long           maxAge;
		final TimeUnit       unit;
		final TimedScheduler scheduler;
		int size;

		volatile TimedNode<T> head;

		TimedNode<T> tail;

		Throwable error;
		volatile boolean done;

		public SizeAndTimeBoundReplayBuffer(int limit,
				long maxAge,
				TimeUnit unit,
				TimedScheduler scheduler) {
			this.limit = limit;
			this.maxAge = maxAge;
			this.unit = unit;
			this.scheduler = scheduler;
			TimedNode<T> h = new TimedNode<>(null, 0L);
			this.tail = h;
			this.head = h;
		}

		@SuppressWarnings("unchecked")
		void replayNormal(ReplaySubscription<T> rs) {
			int missed = 1;
			final Subscriber<? super T> a = rs.actual;

			for (; ; ) {
				@SuppressWarnings("unchecked") TimedNode<T> node = (TimedNode<T>) rs.node;
				if (node == null) {
					node = head;
					if (!done) {
						// skip old entries
						long limit = scheduler.now(unit) - maxAge;
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

				long r = rs.requested;
				long e = 0L;

				while (e != r) {
					if (rs.cancelled) {
						rs.node = null;
						return;
					}

					boolean d = done;
					TimedNode<T> next = node.get();
					boolean empty = next == null;

					if (d && empty) {
						rs.node = null;
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
					if (rs.cancelled) {
						rs.node = null;
						return;
					}

					boolean d = done;
					boolean empty = node.get() == null;

					if (d && empty) {
						rs.node = null;
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

				rs.node = node;

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void replayFused(ReplaySubscription<T> rp) {
			int missed = 1;

			final Subscriber<? super T> a = rp.actual;

			for (; ; ) {

				if (rp.cancelled) {
					rp.node = null;
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

				missed = rp.leave(missed);
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
		public void onComplete() {
			done = true;
		}

		@Override
		public boolean isDone() {
			return done;
		}

		@SuppressWarnings("unchecked")
		TimedNode<T> latestHead(ReplaySubscription<T> rp) {
			long now = scheduler.now(unit) - maxAge;

			TimedNode<T> h = (TimedNode<T>)rp.node;
			if(h == null){
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
		public T poll(ReplaySubscription<T> rp) {
			TimedNode<T> node = latestHead(rp);
			TimedNode<T> next;
			long now = scheduler.now(unit) - maxAge;
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
			rp.node = next;

			return node.value;
		}

		@Override
		public void clear(ReplaySubscription<T> rp) {
			rp.node = null;
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean isEmpty(ReplaySubscription<T> rp) {
			TimedNode<T> node = latestHead(rp);
			return node.get() == null;
		}

		@Override
		public int size(ReplaySubscription<T> rp) {
			TimedNode<T> node = latestHead(rp);
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
			TimedNode<T> n = new TimedNode<>(value, scheduler.now(unit));
			tail.set(n);
			tail = n;
			int s = size;
			if (s == limit) {
				head = head.get();
			}
			else {
				size = s + 1;
			}
			long limit = scheduler.now(unit) - maxAge;

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
		public void replay(ReplaySubscription<T> rp) {
			if (!rp.enter()) {
				return;
			}

			if (rp.fusionMode == NONE) {
				replayNormal(rp);
			}
			else {
				replayFused(rp);
			}
		}
	}

	static final class ReplaySubscription<T> implements QueueSubscription<T>, Producer,
	                                                    Trackable, Receiver {

		final Subscriber<? super T> actual;
		
		final ReplayProcessor<T> parent;

		final ReplayBuffer<T> buffer;

		int index;

		int tailIndex;

		Object node;
		
		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplaySubscription> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ReplaySubscription.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ReplaySubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ReplaySubscription.class, "requested");
		
		volatile boolean cancelled;
		
		int fusionMode;

		public ReplaySubscription(Subscriber<? super T> actual, ReplayProcessor<T> parent) {
			this.actual = actual;
			this.parent = parent;
			this.buffer = parent.buffer;
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return parent;
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
				if (fusionMode == NONE) {
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
		
		boolean enter() {
			return WIP.getAndIncrement(this) == 0;
		}
		
		int leave(int missed) {
			return WIP.addAndGet(this, -missed);
		}
		
		void produced(long n) {
			REQUESTED.addAndGet(this, -n);
		}
	}
}