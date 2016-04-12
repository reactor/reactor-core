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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.flow.Fuseable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;

/**
 * Concatenates values from Iterable sequences generated via a mapper function.
 * @param <T> the input value type
 * @param <R> the value type of the iterables and the result type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxConcatMapIterable<T, R> extends FluxSource<T, R> implements Fuseable {

	final Function<? super T, ? extends Iterable<? extends R>> mapper;
	
	final int prefetch;
	
	final Supplier<Queue<T>> queueSupplier;
	
	public FluxConcatMapIterable(Publisher<? extends T> source,
			Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch,
			Supplier<Queue<T>> queueSupplier) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.prefetch = prefetch;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(Subscriber<? super R> s) {
		if (source instanceof Supplier) {
			T v;
			
			try {
				v = ((Supplier<T>)source).get();
			} catch (Throwable ex) {
				Exceptions.throwIfFatal(ex);
				EmptySubscription.error(s, ex);
				return;
			}
			
			if (v == null) {
				EmptySubscription.complete(s);
				return;
			}
			
			Iterator<? extends R> it;
			
			try {
				Iterable<? extends R> iter = mapper.apply(v);
				
				it = iter.iterator();
			} catch (Throwable ex) {
				Exceptions.throwIfFatal(ex);
				EmptySubscription.error(s, ex);
				return;
			}
			
			FluxIterable.subscribe(s, it);
			
			return;
		}
		source.subscribe(new ConcatMapIterableSubscriber<>(s, mapper, prefetch, queueSupplier));
	}
	
	static final class ConcatMapIterableSubscriber<T, R> 
	implements Subscriber<T>, QueueSubscription<R> {
		
		final Subscriber<? super R> actual;
		
		final Function<? super T, ? extends Iterable<? extends R>> mapper;
		
		final int prefetch;
		
		final int limit;
		
		final Supplier<Queue<T>> queueSupplier;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ConcatMapIterableSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ConcatMapIterableSubscriber.class, "wip");
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ConcatMapIterableSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ConcatMapIterableSubscriber.class, "requested");
		
		Subscription s;
		
		Queue<T> queue;
		
		volatile boolean done;
		
		volatile boolean cancelled;

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ConcatMapIterableSubscriber, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(ConcatMapIterableSubscriber.class, Throwable.class, "error");

		Iterator<? extends R> current;
		
		int consumed;
		
		int fusionMode;
		
		public ConcatMapIterableSubscriber(Subscriber<? super R> actual,
				Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch,
				Supplier<Queue<T>> queueSupplier) {
			this.actual = actual;
			this.mapper = mapper;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			this.limit = prefetch - (prefetch >> 2);
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				
				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked")
					QueueSubscription<T> qs = (QueueSubscription<T>) s;
					
					int m = qs.requestFusion(Fuseable.ANY);
					
					if (m == Fuseable.SYNC) {
						fusionMode = m;
						this.queue = qs;
						done = true;
						
						actual.onSubscribe(this);
						
						return;
					} else
					if (m == Fuseable.ASYNC) {
						fusionMode = m;
						this.queue = qs;
						
						actual.onSubscribe(this);
						
						s.request(prefetch);
						return;
					}
				}
				
				try {
					queue = queueSupplier.get();
				} catch (Throwable ex) {
					Exceptions.throwIfFatal(ex);
					s.cancel();
					EmptySubscription.error(actual, ex);
					return;
				}

				actual.onSubscribe(this);
				
				s.request(prefetch);
			}
		}
		
		@Override
		public void onNext(T t) {
			if (fusionMode != Fuseable.ASYNC) {
				if (!queue.offer(t)) {
					onError(new IllegalStateException("Queue is full?!"));
					return;
				}
			}
			drain();
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
			drain();
		}
		
		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.getAndAddCap(REQUESTED, this, n);
				drain();
			}
		}
		
		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				
				if (WIP.getAndIncrement(this) == 0) {
					queue.clear();
				}
			}
		}
		
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			
			final Subscriber<? super R> a = actual;
			final Queue<T> q = queue;
			final boolean replenish = fusionMode != Fuseable.SYNC;
			
			int missed = 1;
			
			Iterator<? extends R> it = current;

			for (;;) {

				if (it == null) {
					
					boolean d = done;
					
					T t;
					
					t = q.poll();
					
					boolean empty = t == null;
					
					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (t != null) {
						Iterable<? extends R> iterable;
						
						boolean b;
						
						try {
							iterable = mapper.apply(t);
	
							it = iterable.iterator();
							
							b = it.hasNext();
						} catch (Throwable ex) {
							Exceptions.throwIfFatal(ex);
							onError(ex);
							it = null;
							continue;
						}
						
						if (!b) {
							it = null;
							consumedOne(replenish);
							continue;
						}
						
						current = it;
					}
				}
				
				if (it != null) {
					long r = requested;
					long e = 0L;
					
					while (e != r) {
						if (checkTerminated(done, false, a, q)) {
							return;
						}

						R v;
						
						try {
							v = it.next();
						} catch (Throwable ex) {
							Exceptions.throwIfFatal(ex);
							onError(ex);
							continue;
						}
						
						a.onNext(v);

						if (checkTerminated(done, false, a, q)) {
							return;
						}

						e++;
						
						boolean b;
						
						try {
							b = it.hasNext();
						} catch (Throwable ex) {
							Exceptions.throwIfFatal(ex);
							onError(ex);
							continue;
						}
						
						if (!b) {
							consumedOne(replenish);
							it = null;
							current = null;
							break;
						}
					}
					
					if (e == r) {
						boolean d = done;
						boolean empty;
						
						try {
							empty = q.isEmpty() && it == null;
						} catch (Throwable ex) {
							Exceptions.throwIfFatal(ex);
							onError(ex);
							empty = true;
						}
						
						if (checkTerminated(d, empty, a, q)) {
							return;
						}
					}
					
					if (e != 0L) {
						if (r != Long.MAX_VALUE) {
							REQUESTED.addAndGet(this, -e);
						}
					}
					
					if (it == null) {
						continue;
					}
				}
				
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
		
		void consumedOne(boolean enabled) {
			if (enabled) {
				int c = consumed + 1;
				if (c == limit) {
					consumed = 0;
					s.request(c);
				} else {
					consumed = c;
				}
			}
		}
		
		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled) {
				current = null;
				q.clear();
				return true;
			}
			if (d) {
				if (error != null) {
					Throwable e = Exceptions.terminate(ERROR, this);
					
					current = null;
					q.clear();

					a.onError(e);
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
		public void clear() {
			current = null;
			queue.clear();
		}
		
		@Override
		public boolean isEmpty() {
			Iterator<? extends R> it = current;
			if (it != null) {
				return it.hasNext();
			}
			return queue.isEmpty(); // estimate
		}
		
		@Override
		public R poll() {
			Iterator<? extends R> it = current;
			for (;;) {
				if (it == null) {
					T v = queue.poll();
					if (v == null) {
						return null;
					}
					
					it = mapper.apply(v).iterator();
					
					if (!it.hasNext()) {
						continue;
					}
					current = it;
				}
				
				R r = it.next();
				
				if (!it.hasNext()) {
					current = null;
				}
				
				return r;
			}
		}
		
		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & SYNC) != 0 && fusionMode == SYNC) {
				return SYNC;
			}
			return NONE;
		}
		
		@Override
		public int size() {
			return queue.size(); // estimate
		}
	}
}
