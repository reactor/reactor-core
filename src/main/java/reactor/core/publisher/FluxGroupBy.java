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

import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.MultiProducer;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.Exceptions;

/**
 * Groups upstream items into their own Publisher sequence based on a key selector.
 *
 * @param <T> the source value type
 * @param <K> the key value type
 * @param <V> the group item value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxGroupBy<T, K, V> extends FluxSource<T, GroupedFlux<K, V>>
		implements Fuseable {

	final Function<? super T, ? extends K> keySelector;

	final Function<? super T, ? extends V> valueSelector;

	final Supplier<? extends Queue<V>> groupQueueSupplier;

	final Supplier<? extends Queue<GroupedFlux<K, V>>> mainQueueSupplier;

	final int prefetch;

	FluxGroupBy(
			Publisher<? extends T> source, 
			Function<? super T, ? extends K> keySelector,
			Function<? super T, ? extends V> valueSelector,
			Supplier<? extends Queue<GroupedFlux<K, V>>> mainQueueSupplier,
			Supplier<? extends Queue<V>> groupQueueSupplier, 
			int prefetch) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.keySelector = Objects.requireNonNull(keySelector, "keySelector");
		this.valueSelector = Objects.requireNonNull(valueSelector, "valueSelector");
		this.mainQueueSupplier = Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
		this.groupQueueSupplier = Objects.requireNonNull(groupQueueSupplier, "groupQueueSupplier");
		this.prefetch = prefetch;
	}
	
	@Override
	public void subscribe(Subscriber<? super GroupedFlux<K, V>> s) {
		source.subscribe(new GroupByMain<>(s, mainQueueSupplier.get(), groupQueueSupplier, prefetch, keySelector, valueSelector));
	}

	@Override
	public long getPrefetch() {
		return prefetch;
	}
	
	static final class GroupByMain<T, K, V> implements Subscriber<T>,
	                                                   QueueSubscription<GroupedFlux<K, V>>,
	                                                   MultiProducer, Producer, Trackable, Receiver {

		final Function<? super T, ? extends K> keySelector;
		
		final Function<? super T, ? extends V> valueSelector;
		
		final Subscriber<? super GroupedFlux<K, V>> actual;

		final Queue<GroupedFlux<K, V>> queue;
		
		final Supplier<? extends Queue<V>> groupQueueSupplier;

		final int prefetch;

		final ConcurrentMap<K, UnicastGroupedFlux<K, V>> groupMap;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<GroupByMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(GroupByMain.class, "wip");
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<GroupByMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(GroupByMain.class, "requested");
		
		volatile boolean done;
		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<GroupByMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(GroupByMain.class, Throwable.class, "error");
		
		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<GroupByMain> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(GroupByMain.class, "cancelled");
		
		volatile int groupCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<GroupByMain> GROUP_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(GroupByMain.class, "groupCount");

		Subscription s;
		
		volatile boolean enableAsyncFusion;
		
		public GroupByMain(
				Subscriber<? super GroupedFlux<K, V>> actual,
				Queue<GroupedFlux<K, V>> queue,
				Supplier<? extends Queue<V>> groupQueueSupplier, 
				int prefetch,
				Function<? super T, ? extends K> keySelector,
				Function<? super T, ? extends V> valueSelector
				) {
			this.actual = actual;
			this.queue = queue;
			this.groupQueueSupplier = groupQueueSupplier;
			this.prefetch = prefetch;
			this.groupMap = new ConcurrentHashMap<>();
			this.keySelector = keySelector;
			this.valueSelector = valueSelector;
			GROUP_COUNT.lazySet(this, 1);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
				if (prefetch == Integer.MAX_VALUE) {
					s.request(Long.MAX_VALUE);
				}
				else {
					s.request(prefetch);
				}
			}
		}
		
		@Override
		public void onNext(T t) {
			K key;
			V value;
			
			try {
				key = keySelector.apply(t);
				value = valueSelector.apply(t);
			} catch (Throwable ex) {
				onError(Operators.onOperatorError(s, ex, t));
				return;
			}
			if (key == null) {
				onError(Operators.onOperatorError(s, new NullPointerException("The " +
						"keySelector returned a null value"), t));
				return;
			}
			if (value == null) {
				onError(Operators.onOperatorError(s, new NullPointerException("The " +
						"valueSelector returned a null value"), t));
				return;
			}
			
			UnicastGroupedFlux<K, V> g = groupMap.get(key);
			
			if (g == null) {
				// if the main is cancelled, don't create new groups
				if (cancelled == 0) {
					Queue<V> q = groupQueueSupplier.get();

					GROUP_COUNT.getAndIncrement(this);
					g = new UnicastGroupedFlux<>(key, q, this, prefetch);
					g.onNext(value);
					groupMap.put(key, g);
					
					queue.offer(g);
					drain();
				}
			} else {
				g.onNext(value);
			}
		}
		
		@Override
		public void onError(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			} else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			for (UnicastGroupedFlux<K, V> g : groupMap.values()) {
				g.onComplete();
			}
			groupMap.clear();
			GROUP_COUNT.decrementAndGet(this);
			done = true;
			drain();
		}

		@Override
		public long getCapacity() {
			return prefetch;
		}

		@Override
		public long getPending() {
			return queue.size();
		}

		@Override
		public boolean isStarted() {
			return s != null && cancelled != 1 && !done;
		}

		@Override
		public boolean isCancelled() {
			return cancelled == 1;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public Iterator<?> downstreams() {
			return groupMap.values().iterator();
		}

		@Override
		public long downstreamCount() {
			return GROUP_COUNT.get(this);
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		void signalAsyncError() {
			Throwable e = Exceptions.terminate(ERROR, this);
			groupCount = 0;
			for (UnicastGroupedFlux<K, V> g : groupMap.values()) {
				g.onError(e);
			}
			actual.onError(e);
			groupMap.clear();
		}
		
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				drain();
			}
		}
		
		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				if (GROUP_COUNT.decrementAndGet(this) == 0) {
					s.cancel();
				} else {
					if (!enableAsyncFusion) {
						if (WIP.getAndIncrement(this) == 0) {
							// remove queued up but unobservable groups from the mapping
							GroupedFlux<K, V> g;
							while ((g = queue.poll()) != null) {
								((UnicastGroupedFlux<K, V>)g).cancel();
							}
							
							if (WIP.decrementAndGet(this) == 0) {
								return;
							}
							
							drainLoop();
						}
					}
				}
			}
		}
		
		void groupTerminated(K key) {
			if (groupCount == 0) {
				return;
			}
			groupMap.remove(key);
			if (GROUP_COUNT.decrementAndGet(this) == 0) {
				s.cancel();
			}
		}
		
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			if (enableAsyncFusion) {
				drainFused();
			}
			else {
				drainLoop();
			}
		}

		void drainFused() {
			int missed = 1;

			final Subscriber<? super GroupedFlux<K, V>> a = actual;
			final Queue<GroupedFlux<K, V>> q = queue;

			for (; ; ) {

				if (cancelled != 0) {
					q.clear();
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						signalAsyncError();
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drainLoop() {
			
			int missed = 1;
			
			Subscriber<? super GroupedFlux<K, V>> a = actual;
			Queue<GroupedFlux<K, V>> q = queue;
			
			for (;;) {
				
				long r = requested;
				long e = 0L;
				
				while (e != r) {
					boolean d = done;
					GroupedFlux<K, V> v = q.poll();
					boolean empty = v == null;
					
					if (checkTerminated(d, empty, a, q)) {
						return;
					}
					
					if (empty) {
						break;
					}
					
					a.onNext(v);
					
					e++;
				}
				
				if (e == r) {
					if (checkTerminated(done, q.isEmpty(), a, q)) {
						return;
					}
				}
				
				if (e != 0L) {
					
					s.request(e);
					
					if (r != Long.MAX_VALUE) {
						REQUESTED.addAndGet(this, -e);
					}
				}
				
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
		
		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<GroupedFlux<K, V>> q) {
			if (d) {
				Throwable e = error;
				if (e != null && e != Exceptions.TERMINATED) {
					queue.clear();
					signalAsyncError();
					return true;
				} else
				if (empty) {
					a.onComplete();
					return true;
				}
			}
			
			return false;
		}

		@Override
		public GroupedFlux<K, V> poll() {
			return queue.poll();
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public void clear() {
			queue.clear();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if (requestedMode == Fuseable.ANY || requestedMode == Fuseable.ASYNC) {
				enableAsyncFusion = true;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}
		
		void requestInner(long n) {
			s.request(n);
		}
	}

	static final class UnicastGroupedFlux<K, V> extends GroupedFlux<K, V>
			implements Fuseable, QueueSubscription<V>, Producer, Receiver, Trackable {
		final K key;
		
		final int limit;

		@Override
		public K key() {
			return key;
		}
		
		final Queue<V> queue;
		
		volatile GroupByMain<?, K, V> parent;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<UnicastGroupedFlux, GroupByMain> PARENT =
				AtomicReferenceFieldUpdater.newUpdater(UnicastGroupedFlux.class, GroupByMain.class, "parent");
		
		volatile boolean done;
		Throwable error;
		
		volatile Subscriber<? super V> actual;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<UnicastGroupedFlux, Subscriber> ACTUAL =
				AtomicReferenceFieldUpdater.newUpdater(UnicastGroupedFlux.class, Subscriber.class, "actual");
		
		volatile boolean cancelled;
		
		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<UnicastGroupedFlux> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(UnicastGroupedFlux.class, "once");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<UnicastGroupedFlux> WIP =
				AtomicIntegerFieldUpdater.newUpdater(UnicastGroupedFlux.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<UnicastGroupedFlux> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(UnicastGroupedFlux.class, "requested");
		
		volatile boolean enableOperatorFusion;

		int produced;
		
		public UnicastGroupedFlux(K key, Queue<V> queue, GroupByMain<?, K, V> parent, int prefetch) {
			this.key = key;
			this.queue = queue;
			this.parent = parent;
			this.limit = prefetch - (prefetch >> 2);
		}
		
		void doTerminate() {
			GroupByMain<?, K, V> r = parent;
			if (r != null && PARENT.compareAndSet(this, r, null)) {
				r.groupTerminated(key);
			}
		}

		void drainRegular(Subscriber<? super V> a) {
			int missed = 1;

			final Queue<V> q = queue;

			for (;;) {

				long r = requested;
				long e = 0L;

				while (r != e) {
					boolean d = done;

					V t = q.poll();
					boolean empty = t == null;

					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(t);

					e++;
				}

				if (r == e) {
					if (checkTerminated(done, q.isEmpty(), a, q)) {
						return;
					}
				}

				if (e != 0) {
					GroupByMain<?, K, V> main = parent;
					if (main != null) {
						main.requestInner(e);
					}
					if (r != Long.MAX_VALUE) {
						REQUESTED.addAndGet(this, -e);
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drainFused(Subscriber<? super V> a) {
			int missed = 1;

			final Queue<V> q = queue;

			for (;;) {

				if (cancelled) {
					q.clear();
					actual = null;
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					actual = null;

					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					} else {
						a.onComplete();
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void drain() {
			Subscriber<? super V> a = actual;
			if (a != null) {
				if (WIP.getAndIncrement(this) != 0) {
					return;
				}

				if (enableOperatorFusion) {
					drainFused(a);
				} else {
					drainRegular(a);
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled) {
				q.clear();
				actual = null;
				return true;
			}
			if (d && empty) {
				Throwable e = error;
				actual = null;
				if (e != null) {
					a.onError(e);
				} else {
					a.onComplete();
				}
				return true;
			}

			return false;
		}

		public void onNext(V t) {
			if (done || cancelled) {
				return;
			}

			Subscriber<? super V> a = actual;

			if (!queue.offer(t)) {
				onError(Exceptions.failWithOverflow("The queue is full"));
				return;
			}
			if (enableOperatorFusion) {
				if (a != null) {
					a.onNext(null); // in op-fusion, onNext(null) is the indicator of more data
				}
			} else {
				drain();
			}
		}

		public void onError(Throwable t) {
			if (done || cancelled) {
				return;
			}

			error = t;
			done = true;

			doTerminate();

			drain();
		}

		public void onComplete() {
			if (done || cancelled) {
				return;
			}

			done = true;

			doTerminate();

			drain();
		}

		@Override
		public void subscribe(Subscriber<? super V> s) {
			if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {

				s.onSubscribe(this);
				actual = s;
				if (cancelled) {
					actual = null;
				} else {
					drain();
				}
			} else {
				s.onError(new IllegalStateException("This processor allows only a single Subscriber"));
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				drain();
			}
		}
		
		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			cancelled = true;

			doTerminate();

			if (!enableOperatorFusion) {
				if (WIP.getAndIncrement(this) == 0) {
					queue.clear();
				}
			}
		}
		
		@Override
		public V poll() {
			V v = queue.poll();
			if (v != null) {
				produced++;
			} else {
				int p = produced;
				if (p != 0) {
					produced = 0;
					GroupByMain<?, K, V> main = parent;
					if (main != null) {
						main.requestInner(p);
					}
				}
			}
			return v;
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public void clear() {
			queue.clear();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.ASYNC) != 0) {
				enableOperatorFusion = true;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}
		
		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return once == 1 && !done && !cancelled;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Throwable getError() {
			return error;
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
		public long getCapacity() {
			GroupByMain<?, ?, ?> parent = this.parent;
			return parent != null ? parent.prefetch : -1L;
		}

		@Override
		public long getPending() {
			return queue == null || done ? -1L : queue.size();
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public long expectedFromUpstream() {
			return produced;
		}

		@Override
		public long limit() {
			return limit;
		}

	}
}
