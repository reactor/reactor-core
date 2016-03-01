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

import java.util.Collection;
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
import reactor.core.flow.Fuseable;
import reactor.core.flow.MultiProducer;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.state.Completable;
import reactor.core.state.Failurable;
import reactor.core.state.Prefetchable;
import reactor.core.state.Requestable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;

/**
 * Groups upstream items into their own Publisher sequence based on a key selector.
 *
 * @param <T> the source value type
 * @param <K> the key value type
 * @param <V> the group item value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxGroupBy<T, K, V> extends FluxSource<T, GroupedFlux<K, V>>
implements Fuseable, Backpressurable  {

	final Function<? super T, ? extends K> keySelector;

	final Function<? super T, ? extends V> valueSelector;

	final Supplier<? extends Queue<V>> groupQueueSupplier;

	final Supplier<? extends Queue<GroupedFlux<K, V>>> mainQueueSupplier;

	final int prefetch;

	public FluxGroupBy(
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
		Queue<GroupedFlux<K, V>> q;
		
		try {
			q = mainQueueSupplier.get();
		} catch (Throwable ex) {
			Exceptions.throwIfFatal(ex);
			EmptySubscription.error(s, ex);
			return;
		}
		
		if (q == null) {
			EmptySubscription.error(s, new NullPointerException("The mainQueueSupplier returned a null queue"));
			return;
		}
		
		source.subscribe(new GroupByMain<>(s, q, groupQueueSupplier, prefetch, keySelector, valueSelector));
	}
	
	static final class GroupByMain<T, K, V> implements Subscriber<T>,
	                                                   QueueSubscription<GroupedFlux<K, V>>, MultiProducer, Backpressurable, Producer, Requestable,
	                                                   Failurable, Cancellable, Completable, Receiver {

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
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
				s.request(prefetch);
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
				Exceptions.throwIfFatal(ex);
				s.cancel();
				
				Exceptions.addThrowable(ERROR, this, ex);
				done = true;
				if (enableAsyncFusion) {
					signalAsyncError();
				} else {
					drain();
				}
				return;
			}
			if (key == null) {
				NullPointerException ex = new NullPointerException("The keySelector returned a null value");
				s.cancel();
				Exceptions.addThrowable(ERROR, this, ex);
				done = true;
				if (enableAsyncFusion) {
					signalAsyncError();
				} else {
					drain();
				}
				return;
			}
			if (value == null) {
				NullPointerException ex = new NullPointerException("The valueSelector returned a null value");
				s.cancel();
				Exceptions.addThrowable(ERROR, this, ex);
				done = true;
				if (enableAsyncFusion) {
					signalAsyncError();
				} else {
					drain();
				}
				return;
			}
			
			UnicastGroupedFlux<K, V> g = groupMap.get(key);
			
			if (g == null) {
				// if the main is cancelled, don't create new groups
				if (cancelled == 0) {
					Queue<V> q;
					
					try {
						q = groupQueueSupplier.get();
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						s.cancel();
						
						Exceptions.addThrowable(ERROR, this, ex);
						done = true;
						if (enableAsyncFusion) {
							signalAsyncError();
						} else {
							drain();
						}
						return;
					}
					
					GROUP_COUNT.getAndIncrement(this);
					g = new UnicastGroupedFlux<>(key, q, this, prefetch);
					g.onNext(value);
					groupMap.put(key, g);
					
					queue.offer(g);
					if (enableAsyncFusion) {
						actual.onNext(null);
					} else {
						drain();
					}
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
				Exceptions.onErrorDropped(t);
			}
		}
		
		@Override
		public void onComplete() {
			done = true;
			if (enableAsyncFusion) {
				signalAsyncComplete();
			} else {
				drain();
			}
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

		void signalAsyncComplete() {
			groupCount = 0;
			for (UnicastGroupedFlux<K, V> g : groupMap.values()) {
				g.onComplete();
			}
			actual.onComplete();
			groupMap.clear();
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
			if (BackpressureUtils.validate(n)) {
				if (enableAsyncFusion) {
					actual.onNext(null);
				} else {
					BackpressureUtils.addAndGet(REQUESTED, this, n);
					drain();
				}
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
			drainLoop();
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
					signalAsyncComplete();
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
		public GroupedFlux<K, V> peek() {
			return queue.peek();
		}

		@Override
		public boolean add(GroupedFlux<K, V> t) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean offer(GroupedFlux<K, V> t) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public GroupedFlux<K, V> remove() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public GroupedFlux<K, V> element() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public boolean contains(Object o) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public Iterator<GroupedFlux<K, V>> iterator() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public Object[] toArray() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public <T1> T1[] toArray(T1[] a) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean remove(Object o) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean addAll(Collection<? extends GroupedFlux<K, V>> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
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
		
		@Override
		public void drop() {
			queue.poll();
		}
		
		void requestInner(long n) {
			s.request(n);
		}
	}
	
	static final class UnicastGroupedFlux<K, V> extends GroupedFlux<K, V>
	implements Fuseable, QueueSubscription<V>,
			   Producer, Receiver, Failurable, Completable, Prefetchable, Cancellable, Requestable, Backpressurable {
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
		
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			
			int missed = 1;
			
			final Queue<V> q = queue;
			Subscriber<? super V> a = actual;
			
			
			for (;;) {

				if (a != null) {
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
				}
				
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
				
				if (a == null) {
					a = actual;
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
				IllegalStateException ex = new IllegalStateException("The queue is full");
				error = ex;
				done = true;

				doTerminate();

				if (enableOperatorFusion) {
					a.onError(ex);
				} else {
					drain();
				}
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
			
			if (enableOperatorFusion) {
				Subscriber<? super V> a = actual;
				if (a != null) {
					a.onError(t);
				}
			} else {
				drain();
			}
		}
		
		public void onComplete() {
			if (done || cancelled) {
				return;
			}
			
			done = true;

			doTerminate();
			
			if (enableOperatorFusion) {
				Subscriber<? super V> a = actual;
				if (a != null) {
					a.onComplete();
				}
			} else {
				drain();
			}
		}
		
		@Override
		public void subscribe(Subscriber<? super V> s) {
			if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
				
				s.onSubscribe(this);
				actual = s;
				if (cancelled) {
					actual = null;
				} else {
					if (enableOperatorFusion) {
						if (done) {
							Throwable e = error;
							if (e != null) {
								s.onError(e);
							} else {
								s.onComplete();
							}
						} else {
							s.onNext(null);
						}
					} else {
						drain();
					}
				}
			} else {
				s.onError(new IllegalStateException("This processor allows only a single Subscriber"));
			}
		}

		@Override
		public int getMode() {
			return INNER;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				if (enableOperatorFusion) {
					Subscriber<? super V> a = actual;
					if (a != null) {
						a.onNext(null); // in op-fusion, onNext(null) is the indicator of more data
					}
				} else {
					BackpressureUtils.addAndGet(REQUESTED, this, n);
					drain();
				}
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
		public V peek() {
			return queue.peek();
		}

		@Override
		public boolean add(V t) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean offer(V t) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public V remove() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public V element() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public boolean contains(Object o) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public Iterator<V> iterator() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public Object[] toArray() {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public <T1> T1[] toArray(T1[] a) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean remove(Object o) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean addAll(Collection<? extends V> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			throw new UnsupportedOperationException("Operators should not use this method!");
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
		public void drop() {
			queue.poll();
			int p = produced + 1;
			if (p == limit) {
				produced = 0;
				GroupByMain<?, K, V> main = parent;
				if (main != null) {
					main.requestInner(p);
				}
			} else {
				produced = p;
			}
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
