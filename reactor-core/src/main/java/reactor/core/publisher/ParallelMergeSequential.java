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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;


/**
 * Merges the individual 'rails' of the source ParallelFlux, unordered,
 * into a single regular Publisher sequence (exposed as reactor.core.publisher.Flux).
 *
 * @param <T> the value type
 */
final class ParallelMergeSequential<T> extends Flux<T> implements Scannable {
	final ParallelFlux<? extends T> source;
	final int prefetch;
	final Supplier<Queue<T>> queueSupplier;
	
	ParallelMergeSequential(ParallelFlux<? extends T> source, int prefetch, Supplier<Queue<T>> queueSupplier) {
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.source = source;
		this.prefetch = prefetch;
		this.queueSupplier = queueSupplier;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == ScannableAttr.PARENT) return source;
		if (key == IntAttr.PREFETCH) return getPrefetch();

		return null;
	}
	
	@Override
	public void subscribe(Subscriber<? super T> s) {
		MergeSequentialMain<T> parent = new MergeSequentialMain<>(s, source
				.parallelism(), prefetch, queueSupplier);
		s.onSubscribe(parent);
		source.subscribe(parent.subscribers);
	}
	
	static final class MergeSequentialMain<T>
			implements InnerProducer<T> {
		final MergeSequentialInner<T>[] subscribers;
		
		final Supplier<Queue<T>>    queueSupplier;
		final Subscriber<? super T> actual;

		volatile Throwable error;

		@Override
		public final Subscriber<? super T> actual() {
			return actual;
		}

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeSequentialMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(MergeSequentialMain.class, Throwable.class, "error");
		
		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MergeSequentialMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(MergeSequentialMain.class, "wip");
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<MergeSequentialMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(MergeSequentialMain.class, "requested");
		
		volatile boolean cancelled;

		volatile int done;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MergeSequentialMain> DONE =
				AtomicIntegerFieldUpdater.newUpdater(MergeSequentialMain.class, "done");

		MergeSequentialMain(Subscriber<? super T> actual, int n, int
				prefetch,
				Supplier<Queue<T>> queueSupplier) {
			this.actual = actual;
			this.queueSupplier = queueSupplier;
			@SuppressWarnings("unchecked")
			MergeSequentialInner<T>[] a = new MergeSequentialInner[n];
			
			for (int i = 0; i < n; i++) {
				a[i] = new MergeSequentialInner<>(this, prefetch);
			}
			
			this.subscribers = a;
			DONE.lazySet(this, n);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == BooleanAttr.TERMINATED) return done == 0;
			if (key == ThrowableAttr.ERROR) return error;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
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
				
				cancelAll();
				
				if (WIP.getAndIncrement(this) == 0) {
					cleanup();
				}
			}
		}
		
		void cancelAll() {
			for (MergeSequentialInner<T> s : subscribers) {
				s.cancel();
			}
		}
		
		void cleanup() {
			for (MergeSequentialInner<T> s : subscribers) {
				s.queue = null; 
			}
		}
		
		void onNext(MergeSequentialInner<T> inner, T value) {
			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				if (requested != 0) {
					actual.onNext(value);
					if (requested != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}
					inner.request(1);
				} else {
					Queue<T> q = inner.getQueue(queueSupplier);

					if(!q.offer(value)){
						onError(Operators.onOperatorError(this, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), value));
						return;
					}
				}
				if (WIP.decrementAndGet(this) == 0) {
					return;
				}
			} else {
				Queue<T> q = inner.getQueue(queueSupplier);

				if(!q.offer(value)){
					onError(Operators.onOperatorError(this, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), value));
					return;
				}

				if (WIP.getAndIncrement(this) != 0) {
					return;
				}
			}
			
			drainLoop();
		}
		
		void onError(Throwable ex) {
			if(ERROR.compareAndSet(this, null, ex)){
				cancelAll();
				drain();
			}
			else if(error != ex) {
				Operators.onErrorDropped(ex);
			}
		}
		
		void onComplete() {
			if(DONE.decrementAndGet(this) < 0){
				return;
			}
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
			
			MergeSequentialInner<T>[] s = this.subscribers;
			int n = s.length;
			Subscriber<? super T> a = this.actual;
			
			for (;;) {
				
				long r = requested;
				long e = 0;
				
				middle:
				while (e != r) {
					if (cancelled) {
						cleanup();
						return;
					}
					
					Throwable ex = error;
					if (ex != null) {
						cleanup();
						a.onError(ex);
						return;
					}
					
					boolean d = done == 0;
					
					boolean empty = true;
					
					for (int i = 0; i < n; i++) {
						MergeSequentialInner<T> inner = s[i];
						
						Queue<T> q = inner.queue;
						if (q != null) {
							T v = q.poll();
							
							if (v != null) {
								empty = false;
								a.onNext(v);
								inner.requestOne();
								if (++e == r) {
									break middle;
								}
							}
						}
					}
					
					if (d && empty) {
						a.onComplete();
						return;
					}
					
					if (empty) {
						break;
					}
				}
				
				if (e == r) {
					if (cancelled) {
						cleanup();
						return;
					}
					
					Throwable ex = error;
					if (ex != null) {
						cleanup();
						a.onError(ex);
						return;
					}
					
					boolean d = done == 0;
					
					boolean empty = true;
					
					for (int i = 0; i < n; i++) {
						MergeSequentialInner<T> inner = s[i];
						
						Queue<T> q = inner.queue;
						if (q != null && !q.isEmpty()) {
							empty = false;
							break;
						}
					}
					
					if (d && empty) {
						a.onComplete();
						return;
					}
				}
				
				if (e != 0 && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}
				
				int w = wip;
				if (w == missed) {
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				} else {
					missed = w;
				}
			}
		}
	}
	
	static final class MergeSequentialInner<T> implements InnerConsumer<T> {
		
		final MergeSequentialMain<T> parent;
		
		final int prefetch;
		
		final int limit;
		
		long produced;
		
		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeSequentialInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(MergeSequentialInner.class, Subscription.class, "s");
		
		volatile Queue<T> queue;
		
		volatile boolean done;
		
		MergeSequentialInner(MergeSequentialMain<T> parent, int prefetch) {
			this.parent = parent;
			this.prefetch = prefetch ;
			this.limit = prefetch - (prefetch >> 2);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == ScannableAttr.PARENT) return s;
			if (key == ScannableAttr.ACTUAL) return parent;
			if (key == IntAttr.PREFETCH) return prefetch;
			if (key == IntAttr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == BooleanAttr.TERMINATED) return done;

			return null;
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(prefetch);
			}
		}
		
		@Override
		public void onNext(T t) {
			parent.onNext(this, t);
		}
		
		@Override
		public void onError(Throwable t) {
			parent.onError(t);
		}
		
		@Override
		public void onComplete() {
			parent.onComplete();
		}
		
		void requestOne() {
			long p = produced + 1;
			if (p == limit) {
				produced = 0;
				s.request(p);
			} else {
				produced = p;
			}
		}

		public void request(long n) {
			long p = produced + n;
			if (p >= limit) {
				produced = 0;
				s.request(p);
			} else {
				produced = p;
			}
		}

		public void cancel() {
			Operators.terminate(S, this);
		}
		
		Queue<T> getQueue(Supplier<Queue<T>> queueSupplier) {
			Queue<T> q = queue;
			if (q == null) {
				q = queueSupplier.get();
				this.queue = q;
			}
			return q;
		}
	}
}
