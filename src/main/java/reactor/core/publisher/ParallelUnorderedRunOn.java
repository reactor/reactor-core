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

import java.util.Queue;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

import org.reactivestreams.*;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.core.subscriber.SubscriptionHelper;
import reactor.util.*;

/**
 * Ensures each 'rail' from upstream runs on a Worker from a Scheduler.
 *
 * @param <T> the value type
 */
final class ParallelUnorderedRunOn<T> extends ParallelFlux<T> {
	final ParallelFlux<? extends T> source;
	
	final Scheduler scheduler;

	final int prefetch;

	final Supplier<Queue<T>> queueSupplier;
	
	public ParallelUnorderedRunOn(ParallelFlux<? extends T> parent,
			Scheduler scheduler, int prefetch, Supplier<Queue<T>> queueSupplier) {
		this.source = parent;
		this.scheduler = scheduler;
		this.prefetch = prefetch;
		this.queueSupplier = queueSupplier;
	}
	
	@Override
	public void subscribe(Subscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		
		@SuppressWarnings("unchecked")
		Subscriber<T>[] parents = new Subscriber[n];
		
		for (int i = 0; i < n; i++) {
			Subscriber<? super T> a = subscribers[i];
			
			Worker w = scheduler.createWorker();
			Queue<T> q = queueSupplier.get();
			
			RunOnSubscriber<T> parent = new RunOnSubscriber<>(a, prefetch, q, w);
			parents[i] = parent;
		}
		
		source.subscribe(parents);
	}


	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	public boolean isOrdered() {
		return source.isOrdered();
	}

	static final class RunOnSubscriber<T> implements Subscriber<T>, Subscription, Runnable {
		
		final Subscriber<? super T> actual;
		
		final int prefetch;
		
		final int limit;
		
		final Queue<T> queue;
		
		final Worker worker;
		
		Subscription s;
		
		volatile boolean done;
		
		Throwable error;
		
		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RunOnSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(RunOnSubscriber.class, "wip");
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<RunOnSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(RunOnSubscriber.class, "requested");
		
		volatile boolean cancelled;
		
		int consumed;

		public RunOnSubscriber(Subscriber<? super T> actual, int prefetch, Queue<T> queue, Worker worker) {
			this.actual = actual;
			this.prefetch = prefetch;
			this.queue = queue;
			this.limit = prefetch - (prefetch >> 2);
			this.worker = worker;
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (SubscriptionHelper.validate(this.s, s)) {
				this.s = s;
				
				actual.onSubscribe(this);
				
				s.request(prefetch);
			}
		}
		
		@Override
		public void onNext(T t) {
			if (done) {
				return;
			}
			if (!queue.offer(t)) {
				onError(new IllegalStateException("Queue is full?!"));
				return;
			}
			schedule();
		}
		
		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			error = t;
			done = true;
			schedule();
		}
		
		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			schedule();
		}
		
		@Override
		public void request(long n) {
			if (SubscriptionHelper.validate(n)) {
				SubscriptionHelper.getAndAddCap(REQUESTED, this, n);
				schedule();
			}
		}
		
		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				s.cancel();
				worker.shutdown();
				
				if (WIP.getAndIncrement(this) == 0) {
					queue.clear();
				}
			}
		}
		
		void schedule() {
			if (WIP.getAndIncrement(this) == 0) {
				worker.schedule(this);
			}
		}
		
		@Override
		public void run() {
			int missed = 1;
			int c = consumed;
			Queue<T> q = queue;
			Subscriber<? super T> a = actual;
			int lim = limit;
			
			for (;;) {
				
				long r = requested;
				long e = 0L;
				
				while (e != r) {
					if (cancelled) {
						q.clear();
						return;
					}
					
					boolean d = done;
					
					if (d) {
						Throwable ex = error;
						if (ex != null) {
							q.clear();
							
							a.onError(ex);
							
							worker.shutdown();
							return;
						}
					}
					
					T v = q.poll();
					
					boolean empty = v == null;
					
					if (d && empty) {
						a.onComplete();
						
						worker.shutdown();
						return;
					}
					
					if (empty) {
						break;
					}
					
					a.onNext(v);
					
					e++;
					
					int p = ++c;
					if (p == lim) {
						c = 0;
						s.request(p);
					}
				}
				
				if (e == r) {
					if (cancelled) {
						q.clear();
						return;
					}
					
					if (done) {
						Throwable ex = error;
						if (ex != null) {
							q.clear();
							
							a.onError(ex);
							
							worker.shutdown();
							return;
						}
						if (q.isEmpty()) {
							a.onComplete();
							
							worker.shutdown();
							return;
						}
					}
				}
				
				if (e != 0L && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}
				
				int w = wip;
				if (w == missed) {
					consumed = c;
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
}
