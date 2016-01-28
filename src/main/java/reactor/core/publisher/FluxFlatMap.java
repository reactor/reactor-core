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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.flow.MultiReceiver;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.state.Completable;
import reactor.core.state.Failurable;
import reactor.core.state.Introspectable;
import reactor.core.state.Prefetchable;
import reactor.core.state.Requestable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.CancelledSubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.ScalarSubscription;
import reactor.fn.Function;
import reactor.fn.Supplier;

/**
 * Maps a sequence of values each into a Publisher and flattens them 
 * back into a single sequence, interleaving events from the various inner Publishers.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
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
	public void subscribe(Subscriber<? super R> s) {
		
		if (ScalarSubscription.trySubscribeScalarMap(source, s, mapper)) {
			return;
		}
		
		source.subscribe(new FlatMapMain<>(s, mapper, delayError, maxConcurrency, mainQueueSupplier, prefetch, innerQueueSupplier));
	}

	static final class FlatMapMain<T, R> 
	implements Subscriber<T>, Subscription, Receiver, MultiReceiver, Requestable, Completable, Producer,
			   Cancellable, Backpressurable, Failurable {
		
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
		
		volatile FlatMapInner<R>[] subscribers;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FlatMapMain, FlatMapInner[]> SUBSCRIBERS =
				AtomicReferenceFieldUpdater.newUpdater(FlatMapMain.class, FlatMapInner[].class, "subscribers");
		
		@SuppressWarnings("rawtypes")
		static final FlatMapInner[] EMPTY = new FlatMapInner[0];
		
		@SuppressWarnings("rawtypes")
		static final FlatMapInner[] TERMINATED = new FlatMapInner[0];
		
		int last;
		
		int produced;
		
		public FlatMapMain(Subscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError, int maxConcurrency,
				Supplier<? extends Queue<R>> mainQueueSupplier, int prefetch, Supplier<? extends Queue<R>> innerQueueSupplier) {
			this.actual = actual;
			this.mapper = mapper;
			this.delayError = delayError;
			this.maxConcurrency = maxConcurrency;
			this.mainQueueSupplier = mainQueueSupplier;
			int pf = Math.min(maxConcurrency, prefetch);
			this.prefetch = pf;
			this.innerQueueSupplier = innerQueueSupplier;
			this.limit = pf - (pf >> 2);
			SUBSCRIBERS.lazySet(this, EMPTY);
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.addAndGet(REQUESTED, this, n);
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
					cancelAllInner();
				}
			}
		}

		@SuppressWarnings("unchecked")
		void cancelAllInner() {
			FlatMapInner<R>[] a = subscribers;
			if (a != TERMINATED) {
				a = SUBSCRIBERS.getAndSet(this, TERMINATED);
				if (a != TERMINATED) {
					for (FlatMapInner<R> e : a) {
						e.cancel();
					}
				}
			}
		}
		
		boolean add(FlatMapInner<R> inner) {
			for (;;) {
				FlatMapInner<R>[] a = subscribers;
				if (a == TERMINATED) {
					return false;
				}
				
				int n = a.length;
				
				@SuppressWarnings("unchecked")
				FlatMapInner<R>[] b = new FlatMapInner[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = inner;
				
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return true;
				}
			}
		}
		
		@SuppressWarnings("unchecked")
		void remove(FlatMapInner<R> inner) {
			for (;;) {
				FlatMapInner<R>[] a = subscribers;
				if (a == TERMINATED || a == EMPTY) {
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
				
				FlatMapInner<R>[] b;
				if (n == 1) {
					b = EMPTY;
				} else {
					b = new FlatMapInner[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return;
				}
			}
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
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
				s.cancel();
				Exceptions.throwIfFatal(e);
				onError(e);
				return;
			}
			
			if (p == null) {
				s.cancel();

				onError(new NullPointerException("The mapper returned a null Publisher"));
				return;
			}
			
			if (p instanceof Supplier) {
				R v;
				try {
					v = ((Supplier<R>)p).get();
				} catch (Throwable e) {
					s.cancel();
					onError(Exceptions.unwrap(e));
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
					
					if (WIP.decrementAndGet(this) == 0) {
						return;
					}
				} else {
					Queue<R> q;
					
					try {
						q = getOrCreateScalarQueue();
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						
						s.cancel();
						
						if (Exceptions.addThrowable(ERROR, this, ex)) {
							done = true;
						} else {
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
					}
				}
				drainLoop();
			} else {
				Queue<R> q;
				
				try {
					q = getOrCreateScalarQueue();
				} catch (Throwable ex) {
					Exceptions.throwIfFatal(ex);
					
					s.cancel();
					
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
				
				FlatMapInner<R>[] as = subscribers;
				
				int n = as.length;

				boolean d = done;
				Queue<R> sq = scalarQueue;
				
				if (checkTerminated(d, n == 0 && (sq == null || sq.isEmpty()), a)) {
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

					if (r != 0L) {
						
						int j = last;
						if (j >= n) {
							j = n - 1;
						}
						
						for (int i = 0; i < n; i++) {
							
							FlatMapInner<R> inner = as[j];
							
							d = inner.done;
							Queue<R> q = inner.queue;
							if (d && q == null) {
								remove(inner);
								again = true;
								replenishMain++;
							} else 
							if (q != null) {
								while (e != r) {
									d = inner.done;
									
									R v;
									
									try {
										v = q.poll();
									} catch (Throwable ex) {
										Exceptions.throwIfFatal(ex);
										inner.cancel();
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
										remove(inner);
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
									if (d && q.isEmpty()) {
										remove(inner);
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
											last = j;
											break; // 0 .. n - 1
										}
									}
									e = 0L;
								}
							}
							if (++j == n) {
								j = 0;
							}
						}
					}
				}
				
				if (r == 0L) {
					for (int i = 0; i < n; i++) {
						
						FlatMapInner<R> inner = as[i];
						
						d = inner.done;
						Queue<R> q = inner.queue;
						if (d && (q == null || q.isEmpty())) {
							remove(inner);
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
				cancelAllInner();
				
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
						cancelAllInner();
						
						a.onError(e);
						return true;
					} else 
					if (empty) {
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
					
					if (WIP.decrementAndGet(this) == 0) {
						return;
					}
				} else {
					Queue<R> q;
					
					try {
						q = getOrCreateScalarQueue(inner);
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						inner.cancel();
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
					}
				}
				drainLoop();
			} else {
				Queue<R> q;
				
				try {
					q = getOrCreateScalarQueue(inner);
				} catch (Throwable ex) {
					Exceptions.throwIfFatal(ex);
					inner.cancel();
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
			return done && subscribers.length == 0;
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
			return Arrays.asList(subscribers).iterator();
		}

		@Override
		public long upstreamCount() {
			return subscribers.length;
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
	implements Subscriber<R>, Subscription, Producer, Receiver,
			   Backpressurable,
			   Cancellable,
			   Completable,
			   Prefetchable,
			   Introspectable {

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
		
		public FlatMapInner(FlatMapMain<?, R> parent, int prefetch) {
			this.parent = parent;
			this.prefetch = prefetch;
			this.limit = prefetch - (prefetch >> 2);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.setOnce(S, this, s)) {
				if (s instanceof Fuseable.FusionSubscription) {
					@SuppressWarnings("unchecked") Fuseable.FusionSubscription<R> f = (Fuseable.FusionSubscription<R>)s;
					queue = f.queue();
					if (f.requestSyncFusion()){
						sourceMode = SYNC;
						done = true;
						parent.drain();
						return;
					} else {
						sourceMode = ASYNC;
					}
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
			BackpressureUtils.terminate(S, this);
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
			return s == CancelledSubscription.INSTANCE;
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
		public int getMode() {
			return INNER;
		}

		@Override
		public String getName() {
			return FlatMapInner.class.getSimpleName();
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
