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
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.MultiReceiver;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.subscriber.SubscriberState;
import reactor.core.subscriber.SubscriptionHelper;
import reactor.util.Exceptions;

/**
 * Combines the latest values from multiple sources through a function.
 *
 * @param <T> the value type of the sources
 * @param <R> the result type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxCombineLatest<T, R> 
extends Flux<R>
		implements MultiReceiver, Fuseable {

	final Publisher<? extends T>[] array;

	final Iterable<? extends Publisher<? extends T>> iterable;

	final Function<Object[], R> combiner;
	
	final Supplier<? extends Queue<SourceAndArray>> queueSupplier;
	
	final int bufferSize;

	public FluxCombineLatest(Publisher<? extends T>[] array,
			Function<Object[], R> combiner, Supplier<? extends Queue<SourceAndArray>> queueSupplier,
					int bufferSize) {
		if (bufferSize <= 0) {
			throw new IllegalArgumentException("BUFFER_SIZE > 0 required but it was " + bufferSize);
		}

		this.array = Objects.requireNonNull(array, "array");
		this.iterable = null;
		this.combiner = Objects.requireNonNull(combiner, "combiner");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.bufferSize = bufferSize;
	}
	
	public FluxCombineLatest(Iterable<? extends Publisher<? extends T>> iterable,
			Function<Object[], R> combiner, Supplier<? extends Queue<SourceAndArray>> queueSupplier,
					int bufferSize) {
		if (bufferSize <= 0) {
			throw new IllegalArgumentException("BUFFER_SIZE > 0 required but it was " + bufferSize);
		}
		
		this.array = null;
		this.iterable = Objects.requireNonNull(iterable, "iterable");
		this.combiner = Objects.requireNonNull(combiner, "combiner");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.bufferSize = bufferSize;
	}

	@Override
	public Iterator<?> upstreams() {
		return iterable != null ? iterable.iterator() : Arrays.asList(array).iterator();
	}

	@Override
	public long upstreamCount() {
		return array != null ? array.length : -1L;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(Subscriber<? super R> s) {
		Publisher<? extends T>[] a = array;
		int n;
		if (a == null) {
			n = 0;
			a = new Publisher[8];

			Iterator<? extends Publisher<? extends T>> it;

			try {
				it = iterable.iterator();
			} catch (Throwable e) {
				SubscriptionHelper.error(s, e);
				return;
			}

			if (it == null) {
				SubscriptionHelper.error(s, new NullPointerException("The iterator returned is null"));
				return;
			}

			for (;;) {

				boolean b;

				try {
					b = it.hasNext();
				} catch (Throwable e) {
					SubscriptionHelper.error(s, e);
					return;
				}

				if (!b) {
					break;
				}

				Publisher<? extends T> p;

				try {
					p = it.next();
				} catch (Throwable e) {
					SubscriptionHelper.error(s, e);
					return;
				}

				if (p == null) {
					SubscriptionHelper.error(s, new NullPointerException("The Publisher returned by the iterator is " +
					  "null"));
					return;
				}

				if (n == a.length) {
					Publisher<? extends T>[] c = new Publisher[n + (n >> 2)];
					System.arraycopy(a, 0, c, 0, n);
					a = c;
				}
				a[n++] = p;
			}

		} else {
			n = a.length;
		}

		if (n == 0) {
			SubscriptionHelper.complete(s);
			return;
		}
		if (n == 1) {
			new FluxMap<>(a[0], new Function<T, R>() {
				@Override
				public R apply(T t) {
					return combiner.apply(new Object[] { t });
				}
			}).subscribe(s);
			return;
		}
		
		Queue<SourceAndArray> queue;
		
		try {
			queue = queueSupplier.get();
		} catch (Throwable e) {
			SubscriptionHelper.error(s, e);
			return;
		}
		
		if (queue == null) {
			SubscriptionHelper.error(s, new NullPointerException("The queueSupplier returned a null queue"));
			return;
		}
		
		CombineLatestCoordinator<T, R> coordinator = 
				new CombineLatestCoordinator<>(s, combiner, n, queue, bufferSize);
		
		s.onSubscribe(coordinator);
		
		coordinator.subscribe(a, n);
	}
	
	static final class CombineLatestCoordinator<T, R>
			implements QueueSubscription<R>, MultiReceiver, SubscriberState {

		final Subscriber<? super R> actual;
		
		final Function<Object[], R> combiner;
		
		final CombineLatestInner<T>[] subscribers;
		
		final Queue<SourceAndArray> queue;
		
		final Object[] latest;
		
		boolean outputFused;

		int nonEmptySources;
		
		int completedSources;
		
		volatile boolean cancelled;
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<CombineLatestCoordinator> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(CombineLatestCoordinator.class, "requested");
		
		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<CombineLatestCoordinator> WIP =
				AtomicIntegerFieldUpdater.newUpdater(CombineLatestCoordinator.class, "wip");
		
		volatile boolean done;
		
		volatile Throwable error;
		
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CombineLatestCoordinator, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(CombineLatestCoordinator.class, Throwable.class, "error");
		
		public CombineLatestCoordinator(Subscriber<? super R> actual, 
				Function<Object[], R> combiner, int n, Queue<SourceAndArray> queue,
				int bufferSize) {
			this.actual = actual;
			this.combiner = combiner;
			@SuppressWarnings("unchecked")
			CombineLatestInner<T>[] a = new CombineLatestInner[n];
			for (int i = 0; i < n; i++) {
				a[i] = new CombineLatestInner<>(this, i, bufferSize);
			}
			this.subscribers = a;
			this.latest = new Object[n];
			this.queue = queue;
		}

		@Override
		public void request(long n) {
			if (SubscriptionHelper.validate(n)) {
				SubscriptionHelper.getAndAddCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			cancelled = true;
			cancelAll();

			if (WIP.getAndIncrement(this) == 0) {
				queue.clear();
			}
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public Iterator<?> upstreams() {
			return Arrays.asList(subscribers).iterator();
		}

		@Override
		public long upstreamCount() {
			return subscribers.length;
		}

		void subscribe(Publisher<? extends T>[] sources, int n) {
			CombineLatestInner<T>[] a = subscribers;
			
			for (int i = 0; i < n; i++) {
				if (done || cancelled) {
					return;
				}
				sources[i].subscribe(a[i]);
			}
		}
		
		void innerValue(int index, T value) {
			
			boolean replenishInsteadOfDrain;
			
			synchronized (this) {
				Object[] os = latest;

				int localNonEmptySources = nonEmptySources;
				
				if (os[index] == null) {
					localNonEmptySources++;
					nonEmptySources = localNonEmptySources;
				}
				
				os[index] = value;

				if (os.length == localNonEmptySources) {
					SourceAndArray sa = new SourceAndArray(subscribers[index], os.clone());
					
					queue.offer(sa);
					
					replenishInsteadOfDrain = false;
				} else {
					replenishInsteadOfDrain = true;
				}
			}
			
			if (replenishInsteadOfDrain) {
				subscribers[index].requestOne();
			} else {
				drain();
			}
		}
		
		void innerComplete(int index) {
			synchronized (this) {
				Object[] os = latest;
				
				if (os[index] != null) {
					int localCompletedSources = completedSources + 1;
					
					if (localCompletedSources == os.length) {
						done = true;
					} else {
						completedSources = localCompletedSources;
						return;
					}
				} else {
					done = true;
				}
			}
			drain();
		}
		
		void innerError(Throwable e) {
			
			if (Exceptions.addThrowable(ERROR, this, e)) {
				done = true;
				drain();
			} else {
				Exceptions.onErrorDropped(e);
			}
		}
		
		void drainOutput() {
			final Subscriber<? super R> a = actual;
			final Queue<SourceAndArray> q = queue;
			
			int missed = 1;
			
			for (;;) {
				
				if (cancelled) {
					q.clear();
					return;
				}
				
				Throwable ex = error;
				if (ex != null) {
					q.clear();
					
					a.onError(ex);
					return;
				}
				
				boolean d = done;
				
				boolean empty = q.isEmpty();
				
				if (!empty) {
					a.onNext(null);
				}
				
				if (d && empty) {
					a.onComplete();
					return;
				}
				
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
		
		void drainAsync() {
			final Subscriber<? super R> a = actual;
			final Queue<SourceAndArray> q = queue;
			
			int missed = 1;
			
			for (;;) {
				
				long r = requested;
				long e = 0L;
				
				while (e != r) {
					boolean d = done;
					
					SourceAndArray v = q.poll();
					
					boolean empty = v == null;
					
					if (checkTerminated(d, empty, a, q)) {
						return;
					}
					
					if (empty) {
						break;
					}
					
					R w;
					
					try {
						w = combiner.apply(v.array);
					} catch (Throwable ex) {
						innerError(Exceptions.unwrap(ex));
						Exceptions.throwIfFatal(ex);
						continue;
					}
					
					if (w == null) {
						innerError(new NullPointerException("The combiner returned a null value"));
						continue;
					}
					
					a.onNext(w);
					
					v.source.requestOne();
					
					e++;
				}
				
				if (e == r) {
					if (checkTerminated(done, q.isEmpty(), a, q)) {
						return;
					}
				}
				
				if (e != 0L && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}
				
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
		
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			
			if (outputFused) {
				drainOutput();
			} else {
				drainAsync();
			}
		}
		
		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled) {
				cancelAll();
				q.clear();
				return true;
			}
			
			if (d) {
				Throwable e = Exceptions.terminate(ERROR, this);
				
				if (e != null && e != Exceptions.TERMINATED) {
					cancelAll();
					q.clear();
					a.onError(e);
					return true;
				} else
				if (empty) {
					cancelAll();

					a.onComplete();
					return true;
				}
			}
			return false;
		}
		
		void cancelAll() {
			for (CombineLatestInner<T> inner : subscribers) {
				inner.cancel();
			}
		}
		
		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & THREAD_BARRIER) != 0) {
				return NONE;
			}
			int m = requestedMode & ASYNC;
			outputFused = m != 0;
			return m;
		}
		
		@Override
		public R poll() {
			SourceAndArray e = queue.poll();
			if (e == null) {
				return null;
			}
			R r = combiner.apply(e.array);
			e.source.requestOne();
			return r;
		}
		
		@Override
		public void clear() {
			queue.clear();
		}
		
		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}
		
		@Override
		public int size() {
			return queue.size();
		}
	}
	
	static final class CombineLatestInner<T>
			implements Subscriber<T>, Receiver, Producer, SubscriberState {

		final CombineLatestCoordinator<T, ?> parent;

		final int index;

		final int prefetch;
		
		final int limit;
		
		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CombineLatestInner, Subscription> S =
		  AtomicReferenceFieldUpdater.newUpdater(CombineLatestInner.class, Subscription.class, "s");

		int produced;
		
		
		public CombineLatestInner(CombineLatestCoordinator<T, ?> parent, int index, int prefetch) {
			this.parent = parent;
			this.index = index;
			this.prefetch = prefetch;
			this.limit = prefetch - (prefetch >> 2);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (SubscriptionHelper.setOnce(S, this, s)) {
				s.request(prefetch);
			}
		}

		@Override
		public void onNext(T t) {
			parent.innerValue(index, t);
		}

		@Override
		public void onError(Throwable t) {
			parent.innerError(t);
		}

		@Override
		public void onComplete() {
			parent.innerComplete(index);
		}
		
		public void cancel() {
			SubscriptionHelper.terminate(S, this);
		}
		
		public void requestOne() {
			
			int p = produced + 1;
			if (p == limit) {
				produced = 0;
				s.request(p);
			} else {
				produced = p;
			}
			
		}

		@Override
		public Object downstream() {
			return parent;
		}

		@Override
		public long requestedFromDownstream() {
			return produced;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public long limit() {
			return limit;
		}

		@Override
		public long expectedFromUpstream() {
			return limit - produced;
		}
	}
	
	/**
	 * The queue element type for internal use with FluxCombineLatest.
	 */
	public static final class SourceAndArray {
		final CombineLatestInner<?> source;
		final Object[] array;
		
		SourceAndArray(CombineLatestInner<?> source, Object[] array) {
			this.source = source;
			this.array = array;
		}
	}
}
