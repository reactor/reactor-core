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
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.flow.Loopback;
import reactor.core.flow.MultiProducer;
import reactor.core.flow.Receiver;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.state.Completable;
import reactor.core.state.Introspectable;
import reactor.core.state.Requestable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;

/**
 * A connectable {@link Flux} which shares an underlying source and dispatches source values to subscribers in a
 * backpressure-aware manner.
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxPublish<T> extends ConnectableFlux<T>
		implements Receiver, Loopback, Backpressurable {
	/** The source publisher. */
	final Publisher<? extends T> source;
	
	/** The size of the prefetch buffer. */
	final int prefetch;

	final Supplier<? extends Queue<T>> queueSupplier;
	
	volatile State<T> connection;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<FluxPublish, State> CONNECTION =
			AtomicReferenceFieldUpdater.newUpdater(FluxPublish.class, State.class, "connection");
	
	public FluxPublish(Publisher<? extends T> source, int prefetch, Supplier<? extends Queue<T>> queueSupplier) {
		if (prefetch <= 0) {
			throw new IllegalArgumentException("bufferSize > 0 required but it was " + prefetch);
		}
		this.source = Objects.requireNonNull(source, "source");
		this.prefetch = prefetch;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public void connect(Consumer<? super Runnable> cancelSupport) {
		boolean doConnect;
		State<T> s;
		for (;;) {
			s = connection;
			if (s == null || s.isTerminated()) {
				State<T> u = new State<>(prefetch, this);
				
				if (!CONNECTION.compareAndSet(this, s, u)) {
					continue;
				}
				
				s = u;
			}

			doConnect = s.tryConnect();
			break;
		}
		
		cancelSupport.accept(s);
		if (doConnect) {
			source.subscribe(s);
		}
	}
	
	@Override
	public void subscribe(Subscriber<? super T> s) {
		InnerSubscription<T> inner = new InnerSubscription<>(s);
		s.onSubscribe(inner);
		for (;;) {
			if (inner.isCancelled()) {
				break;
			}
			
			State<T> c = connection;
			if (c == null || c.isTerminated()) {
				State<T> u = new State<>(prefetch, this);
				if (!CONNECTION.compareAndSet(this, c, u)) {
					continue;
				}
				
				c = u;
			}
			
			if (c.trySubscribe(inner)) {
				break;
			}
		}
	}

	@Override
	public long getCapacity() {
		return prefetch;
	}

	@Override
	public Object connectedOutput() {
		return connection;
	}

	@Override
	public Object upstream() {
		return source;
	}
	
	static final class State<T> implements Subscriber<T>, Runnable, Receiver, MultiProducer, Backpressurable,
										   Completable, Cancellable, Introspectable {

		final int prefetch;
		
		final FluxPublish<T> parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<State, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(State.class, Subscription.class, "s");
		
		volatile InnerSubscription<T>[] subscribers;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<State> WIP =
				AtomicIntegerFieldUpdater.newUpdater(State.class, "wip");

		volatile int connected;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<State> CONNECTED =
				AtomicIntegerFieldUpdater.newUpdater(State.class, "connected");

		@SuppressWarnings("rawtypes")
		static final InnerSubscription[] EMPTY = new InnerSubscription[0];
		@SuppressWarnings("rawtypes")
		static final InnerSubscription[] TERMINATED = new InnerSubscription[0];
		
		volatile Queue<T> queue;
		
		int sourceMode;
		
		volatile boolean done;
		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<State, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(State.class, Throwable.class, "error");
		
		volatile boolean cancelled;
		
		@SuppressWarnings("unchecked")
		public State(int prefetch, FluxPublish<T> parent) {
			this.prefetch = prefetch;
			this.parent = parent;
			this.subscribers = EMPTY;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.setOnce(S, this, s)) {
				if (s instanceof Fuseable.QueueSubscription) {
					@SuppressWarnings("unchecked")
					Fuseable.QueueSubscription<T> f = (Fuseable.QueueSubscription<T>) s;
					
					int m = f.requestFusion(Fuseable.ANY);
					if (m == Fuseable.SYNC) {
						sourceMode = m;
						queue = f;
						done = true;
						drain();
						return;
					} else
					if (m == Fuseable.ASYNC) {
						sourceMode = m;
						queue = f;
						s.request(prefetch);
						return;
					}
				}
				
				try {
					queue = parent.queueSupplier.get(); 
				} catch (Throwable ex) {
					Exceptions.throwIfFatal(ex);
					s.cancel();
					
					error = ex;
					done = true;
					drain();
					return;
				}
				
				s.request(prefetch);
			}
		}
		
		@Override
		public void onNext(T t) {
			if (done) {
				if(t != null) {
					Exceptions.onNextDropped(t);
				}
				return;
			}
			if (sourceMode == Fuseable.ASYNC) {
				drain();
				return;
			}
			
			if (!queue.offer(t)) {
				Throwable ex = new IllegalStateException("Queue full?!");
				if (!Exceptions.addThrowable(ERROR, this, ex)) {
					throw Exceptions.wrapUpstream(ex);
				}
				done = true;
			}
			drain();
		}
		
		@Override
		public void onError(Throwable t) {
			if (done) {
				throw Exceptions.wrapUpstream(t);
			}
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			} else {
				throw Exceptions.wrapUpstream(t);
			}
		}
		
		@Override
		public void onComplete() {
			done = true;
			drain();
		}
		
		@Override
		public void run() {
			if (cancelled) {
				return;
			}
			if (BackpressureUtils.terminate(S, this)) {
				cancelled = true;
				if (WIP.getAndIncrement(this) != 0) {
					return;
				}
				disconnectAction();
			}
		}
		
		void disconnectAction() {
			queue.clear();
			for (InnerSubscription<T> inner : terminate()) {
				inner.actual.onError(Exceptions.CancelException.INSTANCE);
			}
		}
		
		boolean add(InnerSubscription<T> inner) {
			if (subscribers == TERMINATED) {
				return false;
			}
			synchronized (this) {
				InnerSubscription<T>[] a = subscribers;
				if (a == TERMINATED) {
					return false;
				}
				int n = a.length;
				
				@SuppressWarnings("unchecked")
				InnerSubscription<T>[] b = new InnerSubscription[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = inner;
				
				subscribers = b;
				return true;
			}
		}
		
		@SuppressWarnings("unchecked")
		void remove(InnerSubscription<T> inner) {
			InnerSubscription<T>[] a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			synchronized (this) {
				a = subscribers;
				if (a == TERMINATED || a == EMPTY) {
					return;
				}
				
				int j = -1;
				int n = a.length;
				for (int i = 0; i < n; i++) {
					if (a[i] == inner) {
						j = i;
						break;
					}
				}
				if (j < 0) {
					return;
				}
				
				InnerSubscription<T>[] b;
				if (n == 1) {
					b = EMPTY;
				} else {
					b = new InnerSubscription[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}
				
				subscribers = b;
			}
		}

		@SuppressWarnings("unchecked")
		InnerSubscription<T>[] terminate() {
			InnerSubscription<T>[] a = subscribers;
			if (a == TERMINATED) {
				return a;
			}
			synchronized (this) {
				a = subscribers;
				if (a != TERMINATED) {
					subscribers = TERMINATED;
				}
				return a;
			}
		}

		@Override
		public boolean isTerminated() {
			return subscribers == TERMINATED;
		}
		
		boolean tryConnect() {
			return connected == 0 && CONNECTED.compareAndSet(this, 0, 1);
		}
		
		boolean trySubscribe(InnerSubscription<T> inner) {
			if (add(inner)) {
				if (inner.isCancelled()) {
					remove(inner);
				} else {
					inner.parent = this;
					drain();
				}
				return true;
			}
			return false;
		}
		
		void replenish(long n) {
			if (sourceMode != Fuseable.SYNC) {
				s.request(n);
			}
		}
		
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			
			Queue<T> q = queue;
			int missed = 1;
			
			for (;;) {

				if (q != null) {
					InnerSubscription<T>[] a = subscribers;
					long r = Long.MAX_VALUE;
					
					for (InnerSubscription<T> inner : a) {
						r = Math.min(r, inner.requested);
					}
	
					if (a.length != 0 && r != 0) {
						long e = 0L;
						
						while (e != r) {
							boolean d = done;
							T v;
							
							try {
								v = q.poll();
							} catch (Throwable ex) {
								Exceptions.throwIfFatal(ex);
								
								Exceptions.addThrowable(ERROR, this, ex);
								d = true;
								v = null;
							}
							
							boolean empty = v == null;
							
							if (checkTerminated(d, empty)) {
								return;
							}
							
							if (empty) {
								break;
							}
							
							for (InnerSubscription<T> inner : a) {
								inner.actual.onNext(v);
							}
							
							e++;
						}
						
						if (e == r) {
							boolean d = done;
							boolean empty;
							try {
								empty = q.isEmpty();
							} catch (Throwable ex) {
								Exceptions.throwIfFatal(ex);
								
								Exceptions.addThrowable(ERROR, this, ex);
								d = true;
								empty = true;
							}
							if (checkTerminated(d, empty)) {
								return;
							}
						}
						
						if (e != 0) {
							replenish(e);
							if (r != Long.MAX_VALUE) {
								for (InnerSubscription<T> inner : a) {
									inner.produced(e);
								}
							}
						}
					}
				}
				
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
				
				if (q == null) {
					q = queue;
				}
			}
		}
		
		boolean checkTerminated(boolean d, boolean empty) {
			if (cancelled) {
				disconnectAction();
				return true;
			}
			if (d) {
				Throwable e = error;
				if (e != null && e != Exceptions.TERMINATED) {
					e = Exceptions.terminate(ERROR, this);
					queue.clear();
					for (InnerSubscription<T> inner : terminate()) {
						inner.actual.onError(e);
					}
					return true;
				} else 
				if (empty) {
					for (InnerSubscription<T> inner : terminate()) {
						inner.actual.onComplete();
					}
					return true;
				}
			}
			return false;
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
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return !cancelled && !done && s != null;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public Iterator<?> downstreams() {
			return Arrays.asList(subscribers).iterator();
		}

		@Override
		public long downstreamCount() {
			return subscribers.length;
		}

		@Override
		public Object upstream() {
			return s;
		}
	}
	
	static final class InnerSubscription<T> implements Subscription, Introspectable, Receiver, Requestable, Cancellable {
		
		final Subscriber<? super T> actual;
		
		State<T> parent;
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<InnerSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(InnerSubscription.class, "requested");

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<InnerSubscription> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(InnerSubscription.class, "cancelled");
		
		public InnerSubscription(Subscriber<? super T> actual) {
			this.actual = actual;
		}
		
		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.addAndGet(REQUESTED, this, n);
				State<T> p = parent;
				if (p != null) {
					p.drain();
				}
			}
		}
		
		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				State<T> p = parent;
				if (p != null) {
					p.remove(this);
				}
			}
		}

		@Override
		public boolean isCancelled() {
			return cancelled != 0;
		}

		@Override
		public int getMode() {
			return INNER;
		}

		@Override
		public String getName() {
			return InnerSubscription.class.getSimpleName();
		}

		@Override
		public Object upstream() {
			return parent;
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		void produced(long n) {
			REQUESTED.addAndGet(this, -n);
		}
	}
}