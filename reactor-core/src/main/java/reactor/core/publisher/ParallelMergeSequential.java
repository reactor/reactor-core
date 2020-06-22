/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

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
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		MergeSequentialMain<T> parent = new MergeSequentialMain<>(actual, source
				.parallelism(), prefetch, queueSupplier);
		actual.onSubscribe(parent);
		source.subscribe(parent.subscribers);
	}
	
	static final class MergeSequentialMain<T> implements InnerProducer<T> {

		final MergeSequentialInner<T>[] subscribers;
		
		final Supplier<Queue<T>>    queueSupplier;
		final CoreSubscriber<? super T> actual;

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
		volatile Throwable error;

		MergeSequentialMain(CoreSubscriber<? super T> actual, int n, int
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
		public final CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.TERMINATED) return done == 0 ;
			if (key == Attr.ERROR) return error;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
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
					inner.requestOne();
				} else {
					Queue<T> q = inner.getQueue(queueSupplier);

					if(!q.offer(value)){
						onError(Operators.onOperatorError(this, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), value,
								actual.currentContext()));
						return;
					}
				}
				if (WIP.decrementAndGet(this) == 0) {
					return;
				}
			} else {
				Queue<T> q = inner.getQueue(queueSupplier);

				if(!q.offer(value)){
					onError(Operators.onOperatorError(this, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), value,
							actual.currentContext()));
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
				Operators.onErrorDropped(ex, actual.currentContext());
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
			this.limit = Operators.unboundedOrLimit(prefetch);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public Context currentContext() {
			return parent.actual.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Operators.unboundedOrPrefetch(prefetch));
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
