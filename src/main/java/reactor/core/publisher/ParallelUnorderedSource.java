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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.core.Exceptions;

/**
 * Dispatches the values from upstream in a round robin fashion to subscribers which are
 * ready to consume elements. A value from upstream is sent to only one of the subscribers.
 *
 * @param <T> the value type
 */
final class ParallelUnorderedSource<T> extends ParallelFlux<T> {
	final Publisher<? extends T> source;
	
	final int parallelism;
	
	final int prefetch;
	
	final Supplier<Queue<T>> queueSupplier;

	public ParallelUnorderedSource(Publisher<? extends T> source, int parallelism, int prefetch, Supplier<Queue<T>> queueSupplier) {
		this.source = source;
		this.parallelism = parallelism;
		this.prefetch = prefetch;
		this.queueSupplier = queueSupplier;
	}
	
	@Override
	public int parallelism() {
		return parallelism;
	}
	
	@Override
	public boolean isOrdered() {
		return false;
	}
	
	@Override
	public void subscribe(Subscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		source.subscribe(new ParallelDispatcher<>(subscribers, prefetch, queueSupplier));
	}
	
	static final class ParallelDispatcher<T> implements Subscriber<T> {

		final Subscriber<? super T>[] subscribers;
		
		final AtomicLongArray requests;

		final long[] emissions;

		final int prefetch;
		
		final int limit;

		final Supplier<Queue<T>> queueSupplier;

		Subscription s;
		
		Queue<T> queue;
		
		Throwable error;
		
		volatile boolean done;
		
		int index;
		
		volatile boolean cancelled;
		
		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ParallelDispatcher> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ParallelDispatcher.class, "wip");
		
		/** 
		 * Counts how many subscribers were setup to delay triggering the
		 * drain of upstream until all of them have been setup.
		 */
		volatile int subscriberCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ParallelDispatcher> SUBSCRIBER_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(ParallelDispatcher.class, "subscriberCount");
		
		int produced;
		
		int sourceMode;

		public ParallelDispatcher(Subscriber<? super T>[] subscribers, int prefetch, Supplier<Queue<T>> queueSupplier) {
			this.subscribers = subscribers;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			this.limit = prefetch - (prefetch >> 2);
			this.requests = new AtomicLongArray(subscribers.length);
			this.emissions = new long[subscribers.length];
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof Fuseable.QueueSubscription) {
					@SuppressWarnings("unchecked")
					Fuseable.QueueSubscription<T> qs = (Fuseable.QueueSubscription<T>) s;
					
					int m = qs.requestFusion(Fuseable.ANY);
					
					if (m == Fuseable.SYNC) {
						sourceMode = m;
						queue = qs;
						done = true;
						setupSubscribers();
						drain();
						return;
					} else
					if (m == Fuseable.ASYNC) {
						sourceMode = m;
						queue = qs;
						
						setupSubscribers();
						
						s.request(prefetch);
						
						return;
					}
				}
				
				queue = queueSupplier.get();
				
				setupSubscribers();
				
				s.request(prefetch);
			}
		}
		
		void setupSubscribers() {
			int m = subscribers.length;
			
			for (int i = 0; i < m; i++) {
				if (cancelled) {
					return;
				}
				int j = i;

				SUBSCRIBER_COUNT.lazySet(this, i + 1);
				
				subscribers[i].onSubscribe(new Subscription() {
					@Override
					public void request(long n) {
						if (Operators.validate(n)) {
							AtomicLongArray ra = requests;
							for (;;) {
								long r = ra.get(j);
								if (r == Long.MAX_VALUE) {
									return;
								}
								long u = Operators.addCap(r, n);
								if (ra.compareAndSet(j, r, u)) {
									break;
								}
							}
							if (subscriberCount == m) {
								drain();
							}
						}
					}
					
					@Override
					public void cancel() {
						ParallelDispatcher.this.cancel();
					}
				});
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == Fuseable.NONE) {
				if (!queue.offer(t)) {
					cancel();
					onError(new IllegalStateException("Queue is full?"));
					return;
				}
			}
			drain();
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;
			drain();
		}

		@Override
		public void onComplete() {
			done = true;
			drain();
		}
		
		void cancel() {
			if (!cancelled) {
				cancelled = true;
				this.s.cancel();
				
				if (WIP.getAndIncrement(this) == 0) {
					queue.clear();
				}
			}
		}
		
		void drainAsync() {
			int missed = 1;
			
			Queue<T> q = queue;
			Subscriber<? super T>[] a = this.subscribers;
			AtomicLongArray r = this.requests;
			long[] e = this.emissions;
			int n = e.length;
			int idx = index;
			int consumed = produced;
			
			for (;;) {

				int notReady = 0;
				
				for (;;) {
					if (cancelled) {
						q.clear();
						return;
					}
					
					boolean d = done;
					if (d) {
						Throwable ex = error;
						if (ex != null) {
							q.clear();
							for (Subscriber<? super T> s : a) {
								s.onError(ex);
							}
							return;
						}
					}

					boolean empty = q.isEmpty();
					
					if (d && empty) {
						for (Subscriber<? super T> s : a) {
							s.onComplete();
						}
						return;
					}

					if (empty) {
						break;
					}
					
					long ridx = r.get(idx);
					long eidx = e[idx];
					if (ridx != eidx) {

						T v;
						
						try {
							v = q.poll();
						} catch (Throwable ex) {
							ex = Exceptions.mapOperatorError(s, ex);
							for (Subscriber<? super T> s : a) {
								s.onError(ex);
							}
							return;
						}
						
						if (v == null) {
							break;
						}
						
						a[idx].onNext(v);
						
						e[idx] = eidx + 1;
						
						int c = ++consumed;
						if (c == limit) {
							consumed = 0;
							s.request(c);
						}
						notReady = 0;
					} else {
						notReady++;
					}
					
					idx++;
					if (idx == n) {
						idx = 0;
					}
					
					if (notReady == n) {
						break;
					}
				}
				
				int w = wip;
				if (w == missed) {
					index = idx;
					produced = consumed;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				} else {
					missed = w;
				}
			}
		}
		
		void drainSync() {
			int missed = 1;
			
			Queue<T> q = queue;
			Subscriber<? super T>[] a = this.subscribers;
			AtomicLongArray r = this.requests;
			long[] e = this.emissions;
			int n = e.length;
			int idx = index;
			
			for (;;) {

				int notReady = 0;
				
				for (;;) {
					if (cancelled) {
						q.clear();
						return;
					}
					
					boolean empty;
					
					try {
						empty = q.isEmpty();
					} catch (Throwable ex) {
						ex = Exceptions.mapOperatorError(s, ex);
						for (Subscriber<? super T> s : a) {
							s.onError(ex);
						}
						return;
					}
					
					if (empty) {
						for (Subscriber<? super T> s : a) {
							s.onComplete();
						}
						return;
					}

					long ridx = r.get(idx);
					long eidx = e[idx];
					if (ridx != eidx) {

						T v;
						
						try {
							v = q.poll();
						} catch (Throwable ex) {
							ex = Exceptions.mapOperatorError(s, ex);
							for (Subscriber<? super T> s : a) {
								s.onError(ex);
							}
							return;
						}
						
						if (v == null) {
							for (Subscriber<? super T> s : a) {
								s.onComplete();
							}
							return;
						}
						
						a[idx].onNext(v);
						
						e[idx] = eidx + 1;
						
						notReady = 0;
					} else {
						notReady++;
					}
					
					idx++;
					if (idx == n) {
						idx = 0;
					}
					
					if (notReady == n) {
						break;
					}
				}
				
				int w = wip;
				if (w == missed) {
					index = idx;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				} else {
					missed = w;
				}
			}
		}
		
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			
			if (sourceMode == Fuseable.SYNC) {
				drainSync();
			} else {
				drainAsync();
			}
		}
	}
}
