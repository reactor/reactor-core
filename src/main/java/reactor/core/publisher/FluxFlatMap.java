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
import java.util.concurrent.Callable;
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
import reactor.core.Trackable;
import reactor.core.Exceptions;

/**
 * Maps a sequence of values each into a Publisher and flattens them 
 * back into a single sequence, interleaving events from the various inner Publishers.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxFlatMap<T, R> extends FluxSource<T, R> {

	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	final boolean delayError;

	final int maxConcurrency;

	final Supplier<? extends Queue<R>> mainQueueSupplier;

	final int prefetch;

	final Supplier<? extends Queue<R>> innerQueueSupplier;

	public FluxFlatMap(Publisher<? extends T> source, Function<? super T, ? extends Publisher<? extends R>> mapper,
			boolean delayError, int maxConcurrency, Supplier<? extends Queue<R>> mainQueueSupplier, int prefetch, Supplier<? extends Queue<R>> innerQueueSupplier) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		if (maxConcurrency <= 0) {
			throw new IllegalArgumentException("maxConcurrency > 0 required but it was " + maxConcurrency);
		}
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.delayError = delayError;
		this.prefetch = prefetch;
		this.maxConcurrency = maxConcurrency;
		this.mainQueueSupplier = Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
		this.innerQueueSupplier = Objects.requireNonNull(innerQueueSupplier, "innerQueueSupplier");
	}

	@Override
	public long getPrefetch() {
		return prefetch;
	}

	public static <T, R> Subscriber<T> subscriber(Subscriber<? super R> s,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			boolean delayError,
			int maxConcurrency, Supplier<? extends Queue<R>> mainQueueSupplier, 
			int prefetch, Supplier<? extends Queue<R>> innerQueueSupplier) {
		return new FlatMapMain<>(s, mapper, delayError, maxConcurrency, mainQueueSupplier, prefetch, innerQueueSupplier);
	}
	
	@Override
	public void subscribe(Subscriber<? super R> s) {
		
		if (trySubscribeScalarMap(source, s, mapper, false)) {
			return;
		}

		source.subscribe(new FlatMapMain<>(s,
				mapper,
				delayError,
				maxConcurrency,
				mainQueueSupplier,
				prefetch,
				innerQueueSupplier));
	}

	/**
	 * Checks if the source is a Supplier and if the mapper's publisher output is also
	 * a supplier, thus avoiding subscribing to any of them.
	 *
	 * @param source the source publisher
	 * @param s the end consumer
	 * @param mapper the mapper function
	 * @param fuseableExpected if true, the parent class was marked Fuseable thus the mapping
	 * output has to signal onSubscribe with a QueueSubscription
	 * @return true if the optimization worked
	 */
	@SuppressWarnings("unchecked")
	static <T, R> boolean trySubscribeScalarMap(
			Publisher<? extends T> source,
			Subscriber<? super R> s,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			boolean fuseableExpected) {
		if (source instanceof Callable) {
			T t;

			try {
				t = ((Callable<? extends T>)source).call();
			} catch (Throwable e) {
				Operators.error(s, Exceptions.mapOperatorError(null, e));
				return true;
			}

			if (t == null) {
				Operators.complete(s);
				return true;
			}

			Publisher<? extends R> p;

			try {
				p = mapper.apply(t);
			}
			catch (Throwable e) {
				Operators.error(s, Exceptions.mapOperatorError(null, e));
				return true;
			}

			if (p == null) {
				Operators.error(s, new NullPointerException("The mapper returned a null Publisher"));
				return true;
			}

			if (p instanceof Callable) {
				R v;

				try {
					v = ((Callable<R>)p).call();
				}
				catch (Throwable e) {
					Operators.error(s, Exceptions.mapOperatorError(null, e));
					return true;
				}

				if (v != null) {
					s.onSubscribe(new Operators.ScalarSubscription<>(s, v));
				} else {
					Operators.complete(s);
				}
			} else {
				if (!fuseableExpected || p instanceof Fuseable) {
					p.subscribe(s);
				} else {
					p.subscribe(new SuppressFuseableSubscriber<>(s));
				}
			}

			return true;
		}

		return false;
	}

	static final class FlatMapMain<T, R> extends SpscFreeListTracker<FlatMapInner<R>>
			implements Subscriber<T>, Subscription, Receiver, MultiReceiver, Producer,
			           Trackable {
		
		final Subscriber<? super R> actual;

		final Function<? super T, ? extends Publisher<? extends R>> mapper;

		final boolean delayError;
		
		final int maxConcurrency;
		
		final Supplier<? extends Queue<R>> mainQueueSupplier;

		final int prefetch;

		final Supplier<? extends Queue<R>> innerQueueSupplier;

		final int limit;
		
		volatile Queue<R> scalarQueue;
		
		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FlatMapMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(FlatMapMain.class, Throwable.class, "error");

		volatile boolean done;
		
		volatile boolean cancelled;
		
		Subscription s;
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<FlatMapMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(FlatMapMain.class, "requested");
		
		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<FlatMapMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(FlatMapMain.class, "wip");
		
		@SuppressWarnings("rawtypes")
		static final FlatMapInner[] EMPTY = new FlatMapInner[0];
		
		@SuppressWarnings("rawtypes")
		static final FlatMapInner[] TERMINATED = new FlatMapInner[0];
		
		int lastIndex;
		
		int produced;
		
		public FlatMapMain(Subscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError, int maxConcurrency,
				Supplier<? extends Queue<R>> mainQueueSupplier, int prefetch, Supplier<? extends Queue<R>> innerQueueSupplier) {
			this.actual = actual;
			this.mapper = mapper;
			this.delayError = delayError;
			this.maxConcurrency = maxConcurrency;
			this.mainQueueSupplier = mainQueueSupplier;
			this.prefetch = prefetch;
			this.innerQueueSupplier = innerQueueSupplier;
			this.limit = maxConcurrency - (maxConcurrency >> 2);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected FlatMapInner<R>[] empty() {
			return EMPTY;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		protected FlatMapInner<R>[] terminated() {
			return TERMINATED;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		protected FlatMapInner<R>[] newArray(int size) {
			return new FlatMapInner[size];
		}
		
		@Override
		protected void setIndex(FlatMapInner<R> entry, int index) {
			entry.index = index;
		}
		
		@Override
		protected void unsubscribeEntry(FlatMapInner<R> entry) {
			entry.cancel();
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
			if (!cancelled) {
				cancelled = true;
				
				if (WIP.getAndIncrement(this) == 0) {
					scalarQueue = null;
					s.cancel();
					unsubscribe();
				}
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				
				actual.onSubscribe(this);
				
				if (maxConcurrency == Integer.MAX_VALUE) {
					s.request(Long.MAX_VALUE);
				} else {
					s.request(maxConcurrency);
				}
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}
			
			Publisher<? extends R> p;
			
			try {
				p = mapper.apply(t);
			} catch (Throwable e) {
				onError(Exceptions.mapOperatorError(s, e));
				return;
			}
			
			if (p == null) {
				s.cancel();

				onError(new NullPointerException("The mapper returned a null Publisher"));
				return;
			}
			
			if (p instanceof Callable) {
				R v;
				try {
					v = ((Callable<R>)p).call();
				} catch (Throwable e) {
					onError(Exceptions.mapOperatorError(s, e));
					return;
				}
				emitScalar(v);
			} else {
				FlatMapInner<R> inner = new FlatMapInner<>(this, prefetch);
				if (add(inner)) {
					
					p.subscribe(inner);
				}
			}
			
		}
		
		void emitScalar(R v) {
			if (v == null) {
				if (maxConcurrency != Integer.MAX_VALUE) {
					int p = produced + 1;
					if (p == limit) {
						produced = 0;
						s.request(p);
					} else {
						produced = p;
					}
				}
				return;
			}
			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				long r = requested;
				
				if (r != 0L) {
					actual.onNext(v);
					
					if (r != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}
					
					if (maxConcurrency != Integer.MAX_VALUE) {
						int p = produced + 1;
						if (p == limit) {
							produced = 0;
							s.request(p);
						} else {
							produced = p;
						}
					}
				} else {
					Queue<R> q;
					
					try {
						q = getOrCreateScalarQueue();
					} catch (Throwable ex) {
						ex = Exceptions.mapOperatorError(s, ex);

						if (Exceptions.addThrowable(ERROR, this, ex)) {
							done = true;
						}
						else {
							Exceptions.onErrorDropped(ex);
						}
						
						drainLoop();
						return;
					}
					
					if (!q.offer(v)) {
						s.cancel();
						
						Throwable e = new IllegalStateException("Scalar queue full?!");
						
						if (Exceptions.addThrowable(ERROR, this, e)) {
							done = true;
						} else {
							Exceptions.onErrorDropped(e);
						}
						drainLoop();
						return;
					}
				}
				if (WIP.decrementAndGet(this) == 0) {
					return;
				}

				drainLoop();
			} else {
				Queue<R> q;
				
				try {
					q = getOrCreateScalarQueue();
				}
				catch (Throwable ex) {
					ex = Exceptions.mapOperatorError(s, ex);
					
					if (Exceptions.addThrowable(ERROR, this, ex)) {
						done = true;
					} else {
						Exceptions.onErrorDropped(ex);
					}
					
					drain();
					return;
				}
				
				if (!q.offer(v)) {
					s.cancel();
					
					Throwable e = new IllegalStateException("Scalar queue full?!");
					
					if (Exceptions.addThrowable(ERROR, this, e)) {
						done = true;
					} else {
						Exceptions.onErrorDropped(e);
					}
				}
				drain();
			}
		}
		
		Queue<R> getOrCreateScalarQueue() {
			Queue<R> q = scalarQueue;
			if (q == null) {
				q = mainQueueSupplier.get();
				scalarQueue = q;
			}
			return q;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			} else {
				Exceptions.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			
			done = true;
			drain();
		}
		
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			drainLoop();
		}
		
		void drainLoop() {
			int missed = 1;
			
			final Subscriber<? super R> a = actual;
			
			for (;;) {

				boolean d = done;

				FlatMapInner<R>[] as = get();

				int n = as.length;

				Queue<R> sq = scalarQueue;
				
				boolean noSources = isEmpty();
				
				if (checkTerminated(d, noSources && (sq == null || sq.isEmpty()), a)) {
					return;
				}
				
				boolean again = false;

				long r = requested;
				long e = 0L;
				long replenishMain = 0L;
				
				if (r != 0L) {
					sq = scalarQueue;
					if (sq != null) {
						
						while (e != r) {
							d = done;
							
							R v = sq.poll();
							
							boolean empty = v == null;

							if (checkTerminated(d, false, a)) {
								return;
							}

							if (empty) {
								break;
							}

							a.onNext(v);
							
							e++;
						}
						
						if (e != 0L) {
							replenishMain += e;
							if (r != Long.MAX_VALUE) {
								r = REQUESTED.addAndGet(this, -e);
							}
							e = 0L;
							again = true;
						}

					}
				}
				if (r != 0L && !noSources) {
					
					int j = lastIndex;
					if (j >= n) {
						j = 0;
					}
					
					for (int i = 0; i < n; i++) {
						if (cancelled) {
							scalarQueue = null;
							s.cancel();
							unsubscribe();
							return;
						}
						
						FlatMapInner<R> inner = as[j];
						if (inner != null) {
							d = inner.done;
							Queue<R> q = inner.queue;
							if (d && q == null) {
								remove(inner.index);
								again = true;
								replenishMain++;
							}
							else if (q != null) {
								while (e != r) {
									d = inner.done;

									R v;

									try {
										v = q.poll();
									}
									catch (Throwable ex) {
										ex = Exceptions.mapOperatorError(inner, ex);
										if (!Exceptions.addThrowable(ERROR, this, ex)) {
											Exceptions.onErrorDropped(ex);
										}
										v = null;
										d = true;
									}
									
									boolean empty = v == null;
									
									if (checkTerminated(d, false, a)) {
										return;
									}
	
									if (d && empty) {
										remove(inner.index);
										again = true;
										replenishMain++;
										break;
									}

									if (empty) {
										break;
									}

									a.onNext(v);
									
									e++;
								}
								
								if (e == r) {
									d = inner.done;
									boolean empty;

									try {
										empty = q.isEmpty();
									}
									catch (Throwable ex) {
										ex = Exceptions.mapOperatorError(inner, ex);
										if (!Exceptions.addThrowable(ERROR, this, ex)) {
											Exceptions.onErrorDropped(ex);
										}
										empty = true;
										d = true;
									}
									
									if (d && empty) {
										remove(inner.index);
										again = true;
										replenishMain++;
									}
								}
								
								if (e != 0L) {
									if (!inner.done) {
										inner.request(e);
									}
									if (r != Long.MAX_VALUE) {
										r = REQUESTED.addAndGet(this, -e);
										if (r == 0L) {
											break; // 0 .. n - 1
										}
									}
									e = 0L;
								}
							}
						}
						
						if (r == 0L) {
							break;
						}

						if (++j == n) {
							j = 0;
						}
					}
					
					lastIndex = j;
				}
				
				if (r == 0L && !noSources) {
					as = get();
					n = as.length;
					
					for (int i = 0; i < n; i++) {
						if (cancelled) {
							scalarQueue = null;
							s.cancel();
							unsubscribe();
							return;
						}
						
						FlatMapInner<R> inner = as[i];
						if (inner == null) {
							continue;
						}
						
						d = inner.done;
						Queue<R> q = inner.queue;
						boolean empty = (q == null || q.isEmpty());
						
						// if we have a non-empty source then quit the cleanup
						if (!empty) {
							break;
						}

						if (d && empty) {
							remove(inner.index);
							again = true;
							replenishMain++;
						}
					}
				}
				
				if (replenishMain != 0L && !done && !cancelled) {
					s.request(replenishMain);
				}
				
				if (again) {
					continue;
				}
				
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
		
		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
			if (cancelled) {
				scalarQueue = null;
				s.cancel();
				unsubscribe();
				
				return true;
			}
			
			if (delayError) {
				if (d && empty) {
					Throwable e = error;
					if (e != null && e != Exceptions.TERMINATED) {
						e = Exceptions.terminate(ERROR, this);
						a.onError(e);
					} else {
						a.onComplete();
					}
					
					return true;
				}
			} else {
				if (d) {
					Throwable e = error;
					if (e != null && e != Exceptions.TERMINATED) {
						e = Exceptions.terminate(ERROR, this);
						scalarQueue = null;
						s.cancel();
						unsubscribe();

						a.onError(e);
						return true;
					}
					else if (empty) {
						a.onComplete();
						return true;
					}
				}
			}
			
			return false;
		}
		
		void innerError(FlatMapInner<R> inner, Throwable e) {
			if (Exceptions.addThrowable(ERROR, this, e)) {
				inner.done = true;
				if (!delayError) {
					done = true;
				}
				drain();
			} else {
				Exceptions.onErrorDropped(e);
			}
		}
		
		void innerNext(FlatMapInner<R> inner, R v) {
			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				long r = requested;
				
				if (r != 0L) {
					actual.onNext(v);
					
					if (r != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}

					inner.request(1);
				} else {
					Queue<R> q;
					
					try {
						q = getOrCreateScalarQueue(inner);
					} catch (Throwable ex) {
						ex = Exceptions.mapOperatorError(inner, ex);
						if (Exceptions.addThrowable(ERROR, this, ex)) {
							inner.done = true;
						} else {
							Exceptions.onErrorDropped(ex);
						}
						drainLoop();
						return;
					}

					if (!q.offer(v)) {
						inner.cancel();
						
						Throwable e = new IllegalStateException("Scalar queue full?!");
						
						if (Exceptions.addThrowable(ERROR, this, e)) {
							inner.done = true;
						} else {
							Exceptions.onErrorDropped(e);
						}
						drainLoop();
						return;
					}
				}
				if (WIP.decrementAndGet(this) == 0) {
					return;
				}
				
				drainLoop();
			} else {
				Queue<R> q;
				
				try {
					q = getOrCreateScalarQueue(inner);
				} catch (Throwable ex) {
					ex = Exceptions.mapOperatorError(inner, ex);
					if (Exceptions.addThrowable(ERROR, this, ex)) {
						inner.done = true;
					} else {
						Exceptions.onErrorDropped(ex);
					}
					drain();
					return;
				}
				
				if (!q.offer(v)) {
					inner.cancel();
					
					Throwable e = new IllegalStateException("Scalar queue full?!");
					
					if (Exceptions.addThrowable(ERROR, this, e)) {
						inner.done = true;
					} else {
						Exceptions.onErrorDropped(e);
					}
				}
				drain();
			}
		}
		
		Queue<R> getOrCreateScalarQueue(FlatMapInner<R> inner) {
			Queue<R> q = inner.queue;
			if (q == null) {
				q = innerQueueSupplier.get();
				inner.queue = q;
			}
			return q;
		}

		@Override
		public long getCapacity() {
			return maxConcurrency;
		}

		@Override
		public long getPending() {
			return done || scalarQueue == null ? -1L : scalarQueue.size();
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return s != null && !isTerminated() && !isCancelled();
		}

		@Override
		public boolean isTerminated() {
			return done && get().length == 0;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public Iterator<?> upstreams() {
			return Arrays.asList(get()).iterator();
		}

		@Override
		public long upstreamCount() {
			return get().length;
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public Object downstream() {
			return actual;
		}
	}

	static final class FlatMapInner<R>
			implements Subscriber<R>, Subscription, Producer, Receiver, Trackable {

		final FlatMapMain<?, R> parent;
		
		final int prefetch;
		
		final int limit;
		
		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FlatMapInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(FlatMapInner.class, Subscription.class, "s");
		
		long produced;

		volatile Queue<R> queue;
		
		volatile boolean done;
		
		/** Represents the optimization mode of this inner subscriber. */
		int sourceMode;
		
		/** Running with regular, arbitrary source. */
		static final int NORMAL = 0;
		/** Running with a source that implements SynchronousSource. */
		static final int SYNC = 1;
		/** Running with a source that implements AsynchronousSource. */
		static final int ASYNC = 2;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<FlatMapInner> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(FlatMapInner.class, "once");

		int index;
		
		public FlatMapInner(FlatMapMain<?, R> parent, int prefetch) {
			this.parent = parent;
			this.prefetch = prefetch;
			this.limit = prefetch - (prefetch >> 2);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				if (s instanceof Fuseable.QueueSubscription) {
					@SuppressWarnings("unchecked") Fuseable.QueueSubscription<R> f = (Fuseable.QueueSubscription<R>)s;
					int m = f.requestFusion(Fuseable.ANY);
					if (m == Fuseable.SYNC){
						sourceMode = SYNC;
						queue = f;
						done = true;
						parent.drain();
						return;
					}
					else if (m == Fuseable.ASYNC) {
						sourceMode = ASYNC;
						queue = f;
					}
					// NONE is just fall-through as the queue will be created on demand
				}
				s.request(prefetch);
			}
		}

		@Override
		public void onNext(R t) {
			if (sourceMode == ASYNC) {
				parent.drain();
			} else {
				parent.innerNext(this, t);
			}
		}

		@Override
		public void onError(Throwable t) {
			// we don't want to emit the same error twice in case of subscription-race in async mode
			if (sourceMode != ASYNC || ONCE.compareAndSet(this, 0, 1)) {
				parent.innerError(this, t);
			}
		}

		@Override
		public void onComplete() {
			// onComplete is practically idempotent so there is no risk due to subscription-race in async mode
			done = true;
			parent.drain();
		}

		@Override
		public void request(long n) {
			if (sourceMode != SYNC) {
				long p = produced + n;
				if (p >= limit) {
					produced = 0L;
					s.request(p);
				} else {
					produced = p;
				}
			}
		}

		@Override
		public void cancel() {
			Operators.terminate(S, this);
		}

		@Override
		public long getCapacity() {
			return prefetch;
		}

		@Override
		public long getPending() {
			return done || queue == null ? -1L : queue.size();
		}

		@Override
		public boolean isCancelled() {
			return s == Operators.cancelledSubscription();
		}

		@Override
		public boolean isStarted() {
			return s != null && !done && !isCancelled();
		}

		@Override
		public boolean isTerminated() {
			return done && (queue == null || queue.isEmpty());
		}

		@Override
		public long expectedFromUpstream() {
			return produced;
		}

		@Override
		public long limit() {
			return limit;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public Object downstream() {
			return parent;
		}
	}
}

abstract class SpscFreeListTracker<T> {
	private volatile T[] array = empty();

	private int[] free = FREE_EMPTY;

	private long producerIndex;
	private long consumerIndex;

	volatile int size;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<SpscFreeListTracker> SIZE =
			AtomicIntegerFieldUpdater.newUpdater(SpscFreeListTracker.class, "size");

	private static final int[] FREE_EMPTY = new int[0];

	protected abstract T[] empty();

	protected abstract T[] terminated();

	protected abstract T[] newArray(int size);

	protected abstract void unsubscribeEntry(T entry);

	protected abstract void setIndex(T entry, int index);


	protected final void unsubscribe() {
		T[] a;
		T[] t = terminated();
		synchronized (this) {
			a = array;
			if (a == t) {
				return;
			}
			SIZE.lazySet(this, 0);
			free = null;
			array = t;
		}
		for (T e : a) {
			if (e != null) {
				unsubscribeEntry(e);
			}
		}
	}

	public final T[] get() {
		return array;
	}

	public final boolean add(T entry) {
		T[] a = array;
		if (a == terminated()) {
			return false;
		}
		synchronized (this) {
			a = array;
			if (a == terminated()) {
				return false;
			}

			int idx = pollFree();
			if (idx < 0) {
				int n = a.length;
				T[] b = n != 0 ? newArray(n << 1) : newArray(4);
				System.arraycopy(a, 0, b, 0, n);

				array = b;
				a = b;

				int m = b.length;
				int[] u = new int[m];
				for (int i = n + 1; i < m; i++) {
					u[i] = i;
				}
				free = u;
				consumerIndex = n + 1;
				producerIndex = m;

				idx = n;
			}
			setIndex(entry, idx);
			a[idx] = entry;
			SIZE.lazySet(this, size + 1);
			return true;
		}
	}

	public final void remove(int index) {
		synchronized (this) {
			T[] a = array;
			if (a != terminated()) {
				a[index] = null;
				offerFree(index);
				SIZE.lazySet(this, size - 1);
			}
		}
	}

	private int pollFree() {
		int[] a = free;
		int m = a.length - 1;
		long ci = consumerIndex;
		if (producerIndex == ci) {
			return -1;
		}
		int offset = (int)ci & m;
		consumerIndex = ci + 1;
		return a[offset];
	}

	private void offerFree(int index) {
		int[] a = free;
		int m = a.length - 1;
		long pi = producerIndex;
		int offset = (int)pi & m;
		a[offset] = index;
		producerIndex = pi + 1;
	}

	protected final boolean isEmpty() {
		return size == 0;
	}
}

final class SuppressFuseableSubscriber<T> implements Producer, Receiver, Subscriber<T>,
                                                     Fuseable.QueueSubscription<T> {

	final Subscriber<? super T> actual;

	Subscription s;

	public SuppressFuseableSubscriber(Subscriber<? super T> actual) {
		this.actual = actual;

	}

	@Override
	public void onSubscribe(Subscription s) {
		if (Operators.validate(this.s, s)) {
			this.s = s;

			actual.onSubscribe(this);
		}
	}

	@Override
	public void onNext(T t) {
		actual.onNext(t);
	}

	@Override
	public void onError(Throwable t) {
		actual.onError(t);
	}

	@Override
	public void onComplete() {
		actual.onComplete();
	}

	@Override
	public void request(long n) {
		s.request(n);
	}

	@Override
	public void cancel() {
		s.cancel();
	}

	@Override
	public int requestFusion(int requestedMode) {
		return Fuseable.NONE;
	}

	@Override
	public T poll() {
		return null;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public void clear() {

	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public Object downstream() {
		return actual;
	}

	@Override
	public Object upstream() {
		return s;
	}
}
