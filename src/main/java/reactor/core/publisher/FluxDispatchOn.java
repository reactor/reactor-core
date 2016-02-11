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

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.flow.Loopback;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.publisher.FluxPublishOn.ScheduledEmptySubscriptionEager;
import reactor.core.publisher.FluxPublishOn.ScheduledSubscriptionEagerCancel;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.state.Completable;
import reactor.core.state.Failurable;
import reactor.core.state.Prefetchable;
import reactor.core.state.Requestable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.fn.Consumer;
import reactor.fn.Supplier;

/**
 * Emits events on a different thread specified by a scheduler callback.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxDispatchOn<T> extends FluxSource<T, T> implements Loopback {

	final Callable<? extends Consumer<Runnable>> schedulerFactory;
	
	final boolean delayError;
	
	final Supplier<? extends Queue<T>> queueSupplier;
	
	final int prefetch;
	
	public FluxDispatchOn(
			Publisher<? extends T> source, 
			Callable<? extends Consumer<Runnable>> schedulerFactory, 
			boolean delayError,
			int prefetch,
			Supplier<? extends Queue<T>> queueSupplier) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.schedulerFactory = Objects.requireNonNull(schedulerFactory, "schedulerFactory");
		this.delayError = delayError;
		this.prefetch = prefetch;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		
		Consumer<Runnable> scheduler;
		
		try {
			scheduler = schedulerFactory.call();
		} catch (Throwable e) {
			Exceptions.throwIfFatal(e);
			EmptySubscription.error(s, e);
			return;
		}
		
		if (scheduler == null) {
			EmptySubscription.error(s, new NullPointerException("The schedulerFactory returned a null Function"));
			return;
		}
		
		if (source instanceof Supplier) {
			@SuppressWarnings("unchecked")
			Supplier<T> supplier = (Supplier<T>) source;
			
			T v = supplier.get();
			
			if (v == null) {
				ScheduledEmptySubscriptionEager parent = new ScheduledEmptySubscriptionEager(s, scheduler);
				s.onSubscribe(parent);
				scheduler.accept(parent);
			} else {
				s.onSubscribe(new ScheduledSubscriptionEagerCancel<>(s, v, scheduler));
			}
			return;
		}
		
		if (s instanceof Fuseable.ConditionalSubscriber) {
			@SuppressWarnings("unchecked")
			Fuseable.ConditionalSubscriber<? super T> cs = (Fuseable.ConditionalSubscriber<? super T>) s;
			source.subscribe(new DispatchOnConditionalSubscriber<>(cs, scheduler, delayError, prefetch, queueSupplier));
			return;
		}
		source.subscribe(new DispatchOnSubscriber<>(s, scheduler, delayError, prefetch, queueSupplier));
	}

	@Override
	public Object connectedInput() {
		return null;
	}

	@Override
	public Object connectedOutput() {
		return schedulerFactory;
	}



	static final class DispatchOnSubscriber<T>
	implements Subscriber<T>, Subscription, Runnable,
			   Producer, Loopback, Backpressurable, Prefetchable, Receiver, Cancellable, Failurable,
			   Requestable, Completable {
		
		final Subscriber<? super T> actual;
		
		final Consumer<Runnable> scheduler;
		
		final boolean delayError;
		
		final int prefetch;
		
		final int limit;
		
		final Supplier<? extends Queue<T>> queueSupplier;
		
		Subscription s;
		
		Queue<T> queue;
		
		volatile boolean cancelled;
		
		volatile boolean done;
		
		Throwable error;
		
		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<DispatchOnSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(DispatchOnSubscriber.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<DispatchOnSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(DispatchOnSubscriber.class, "requested");

		int sourceMode;
		
		long produced;
		
		public DispatchOnSubscriber(
				Subscriber<? super T> actual,
				Consumer<Runnable> scheduler,
				boolean delayError,
				int prefetch,
				Supplier<? extends Queue<T>> queueSupplier) {
			this.actual = actual;
			this.scheduler = scheduler;
			this.delayError = delayError;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			if (prefetch != Integer.MAX_VALUE) {
				this.limit = prefetch - (prefetch >> 2);
			} else {
				this.limit = Integer.MAX_VALUE;
			}
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				
				if (s instanceof Fuseable.QueueSubscription) {
					@SuppressWarnings("unchecked")
					Fuseable.QueueSubscription<T> f = (Fuseable.QueueSubscription<T>) s;

					int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);
					
					if (m == Fuseable.SYNC) {
						sourceMode = Fuseable.SYNC;
						queue = f;
						done = true;
						
						actual.onSubscribe(this);
						return;
					} else
					if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						queue = f;
					} else {
						try {
							queue = queueSupplier.get();
						} catch (Throwable e) {
							Exceptions.throwIfFatal(e);
							s.cancel();
							scheduler.accept(null);
							
							EmptySubscription.error(actual, e);
							return;
						}
					}
				} else {
					try {
						queue = queueSupplier.get();
					} catch (Throwable e) {
						Exceptions.throwIfFatal(e);
						s.cancel();
						scheduler.accept(null);
						
						EmptySubscription.error(actual, e);
						return;
					}
				}
				
				actual.onSubscribe(this);
				
				if (prefetch == Integer.MAX_VALUE) {
					s.request(Long.MAX_VALUE);
				} else {
					s.request(prefetch);
				}
			}
		}
		
		@Override
		public void onNext(T t) {
			if (sourceMode == Fuseable.ASYNC) {
				trySchedule();
				return;
			}
			if (!queue.offer(t)) {
				s.cancel();
				
				error = new IllegalStateException("Queue is full?!");
				done = true;
			}
			trySchedule();
		}
		
		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;
			trySchedule();
		}
		
		@Override
		public void onComplete() {
			done = true;
			trySchedule();
		}
		
		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.addAndGet(REQUESTED, this, n);
				trySchedule();
			}
		}
		
		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			
			cancelled = true;
			scheduler.accept(null);
			
			if (WIP.getAndIncrement(this) == 0) {
				s.cancel();
				queue.clear();
			}
		}

		void trySchedule() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			scheduler.accept(this);
		}

		void runSync() {
			int missed = 1;

			final Subscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long e = produced;

			for (;;) {

				long r = requested;

				while (e != r) {
					T v;

					try {
						v = q.poll();
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						scheduler.accept(null);

						a.onError(ex);
						return;
					}

					if (cancelled) {
						scheduler.accept(null);
						return;
					}
					if (v == null) {
						scheduler.accept(null);
						a.onComplete();
						return;
					}

					a.onNext(v);

					e++;
				}

				if (e == r) {
					if (cancelled) {
						scheduler.accept(null);
						return;
					}

					boolean empty;

					try {
						empty = q.isEmpty();
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						scheduler.accept(null);

						a.onError(ex);
						return;
					}

					if (empty) {
						scheduler.accept(null);
						a.onComplete();
						return;
					}
				}

				int w = wip;
				if (missed == w) {
					produced = e;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				} else {
					missed = w;
				}
			}
		}

		void runAsync() {
			int missed = 1;

			final Subscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long e = produced;

			for (;;) {

				long r = requested;

				while (e != r) {
					boolean d = done;
					T v;

					try {
						v = q.poll();
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);

						s.cancel();
						scheduler.accept(null);
						q.clear();

						a.onError(ex);
						return;
					}

					boolean empty = v == null;

					if (checkTerminated(d, empty, a)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(v);

					e++;
					if (e == limit) {
						if (r != Long.MAX_VALUE) {
							r = REQUESTED.addAndGet(this, -e);
						}
						s.request(e);
						e = 0L;
					}
				}

				if (e == r) {
					boolean d = done;
					boolean empty;
					try {
						empty = q.isEmpty();
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);

						s.cancel();
						scheduler.accept(null);
						q.clear();

						a.onError(ex);
						return;
					}

					if (checkTerminated(d, empty, a)) {
						return;
					}
				}

				int w = wip;
				if (missed == w) {
					produced = e;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				} else {
					missed = w;
				}
			}
		}

		@Override
		public void run() {
			if (sourceMode == Fuseable.SYNC) {
				runSync();
			} else {
				runAsync();
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
			if (cancelled) {
				s.cancel();
				scheduler.accept(null);
				queue.clear();
				return true;
			}
			if (d) {
				if (delayError) {
					if (empty) {
						scheduler.accept(null);
						Throwable e = error;
						if (e != null) {
							a.onError(e);
						} else {
							a.onComplete();
						}
						return true;
					}
				} else {
					Throwable e = error;
					if (e != null) {
						scheduler.accept(null);
						queue.clear();
						a.onError(e);
						return true;
					}
					else
					if (empty) {
						scheduler.accept(null);
						a.onComplete();
						return true;
					}
				}
			}

			return false;
		}

		@Override
		public long requestedFromDownstream() {
			return queue == null ? prefetch : (prefetch - queue.size());
		}

		@Override
		public long getCapacity() {
			return prefetch;
		}

		@Override
		public long getPending() {
			return queue != null ? queue.size() : -1L;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return s != null && !cancelled && !done;
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
		public Object connectedInput() {
			return null;
		}

		@Override
		public Object connectedOutput() {
			return scheduler;
		}

		@Override
		public long expectedFromUpstream() {
			return queue == null ? prefetch : (prefetch - queue.size());
		}

		@Override
		public long limit() {
			return limit;
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

	static final class DispatchOnConditionalSubscriber<T>
	implements Subscriber<T>, Subscription, Runnable,
			   Producer, Loopback, Backpressurable, Prefetchable, Receiver, Cancellable, Failurable, Completable, Requestable {
		
		final Fuseable.ConditionalSubscriber<? super T> actual;
		
		final Consumer<Runnable> scheduler;
		
		final boolean delayError;
		
		final int prefetch;
		
		final int limit;

		final Supplier<? extends Queue<T>> queueSupplier;
		
		Subscription s;
		
		Queue<T> queue;
		
		volatile boolean cancelled;
		
		volatile boolean done;
		
		Throwable error;
		
		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<DispatchOnConditionalSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(DispatchOnConditionalSubscriber.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<DispatchOnConditionalSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(DispatchOnConditionalSubscriber.class, "requested");

		int sourceMode;
		
		long produced;
		
		long consumed;
		
		public DispatchOnConditionalSubscriber(
				Fuseable.ConditionalSubscriber<? super T> actual,
				Consumer<Runnable> scheduler,
				boolean delayError,
				int prefetch,
				Supplier<? extends Queue<T>> queueSupplier) {
			this.actual = actual;
			this.scheduler = scheduler;
			this.delayError = delayError;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			if (prefetch != Integer.MAX_VALUE) {
				this.limit = prefetch - (prefetch >> 2);
			} else {
				this.limit = Integer.MAX_VALUE;
			}
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				
				if (s instanceof Fuseable.QueueSubscription) {
					@SuppressWarnings("unchecked")
					Fuseable.QueueSubscription<T> f = (Fuseable.QueueSubscription<T>) s;

					int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);
					
					if (m == Fuseable.SYNC) {
						sourceMode = Fuseable.SYNC;
						queue = f;
						done = true;
						
						actual.onSubscribe(this);
						return;
					} else
					if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						queue = f;
					} else {
						try {
							queue = queueSupplier.get();
						} catch (Throwable e) {
							Exceptions.throwIfFatal(e);
							s.cancel();
							scheduler.accept(null);
							
							EmptySubscription.error(actual, e);
							return;
						}
					}
				} else {
					try {
						queue = queueSupplier.get();
					} catch (Throwable e) {
						Exceptions.throwIfFatal(e);
						s.cancel();
						scheduler.accept(null);
						
						EmptySubscription.error(actual, e);
						return;
					}
				}
				
				actual.onSubscribe(this);
				
				if (prefetch == Integer.MAX_VALUE) {
					s.request(Long.MAX_VALUE);
				} else {
					s.request(prefetch);
				}
			}
		}
		
		@Override
		public void onNext(T t) {
			if (sourceMode == Fuseable.ASYNC) {
				trySchedule();
				return;
			}
			if (!queue.offer(t)) {
				s.cancel();
				
				error = new IllegalStateException("Queue is full?!");
				done = true;
			}
			trySchedule();
		}
		
		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;
			trySchedule();
		}
		
		@Override
		public void onComplete() {
			done = true;
			trySchedule();
		}
		
		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.addAndGet(REQUESTED, this, n);
				trySchedule();
			}
		}
		
		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			
			cancelled = true;
			scheduler.accept(null);
			
			if (WIP.getAndIncrement(this) == 0) {
				s.cancel();
				queue.clear();
			}
		}
		
		void trySchedule() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			
			scheduler.accept(this);
		}
		
		void runSync() {
			int missed = 1;
			
			final Fuseable.ConditionalSubscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long e = produced;

			for (;;) {
				
				long r = requested;
				
				while (e != r) {
					T v;
					try {
						v = q.poll();
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						scheduler.accept(null);
						
						a.onError(ex);
						return;
					}

					if (cancelled) {
						scheduler.accept(null);
						return;
					}
					if (v == null) {
						scheduler.accept(null);
						a.onComplete();
						return;
					}
					
					if (a.tryOnNext(v)) {
						e++;
					}
				}
				
				if (e == r) {
					if (cancelled) {
						scheduler.accept(null);
						return;
					}
					
					boolean empty;
					
					try {
						empty = q.isEmpty();
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						scheduler.accept(null);
						
						a.onError(ex);
						return;
					}
					
					if (empty) {
						scheduler.accept(null);
						a.onComplete();
						return;
					}
				}

				int w = wip;
				if (missed == w) {
					produced = e;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				} else {
					missed = w;
				}
			}
		}
		
		void runAsync() {
			int missed = 1;
			
			final Fuseable.ConditionalSubscriber<? super T> a = actual;
			final Queue<T> q = queue;
			
			long emitted = produced;
			long polled = consumed;
			
			for (;;) {
				
				long r = requested;
				
				while (emitted != r) {
					boolean d = done;
					T v;
					try {
						v = q.poll();
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);

						s.cancel();
						scheduler.accept(null);
						q.clear();
						
						a.onError(ex);
						return;
					}
					boolean empty = v == null;
					
					if (checkTerminated(d, empty, a)) {
						return;
					}
					
					if (empty) {
						break;
					}

					if (a.tryOnNext(v)) {
						emitted++;
					}
					
					polled++;
					
					if (polled == limit) {
						s.request(polled);
						polled = 0L;
					}
				}
				
				if (emitted == r) {
					boolean d = done;
					boolean empty;
					try {
						empty = q.isEmpty();
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);

						s.cancel();
						scheduler.accept(null);
						q.clear();
						
						a.onError(ex);
						return;
					}

					if (checkTerminated(d, empty, a)) {
						return;
					}
				}
				
				int w = wip;
				if (missed == w) {
					produced = emitted;
					consumed = polled;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				} else {
					missed = w;
				}
			}

		}
		
		@Override
		public void run() {
			if (sourceMode == Fuseable.SYNC) {
				runSync();
			} else {
				runAsync();
			}
		}

		@Override
		public long getCapacity() {
			return prefetch;
		}

		@Override
		public long getPending() {
			return queue != null ? queue.size() : -1;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return s != null && !cancelled && !done;
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
		public Object connectedInput() {
			return null;
		}

		@Override
		public Object connectedOutput() {
			return scheduler;
		}

		@Override
		public long expectedFromUpstream() {
			return queue == null ? prefetch : (prefetch - queue.size());
		}

		@Override
		public long limit() {
			return limit;
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
			return queue == null ? requested : (requested - queue.size());
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
			if (cancelled) {
				s.cancel();
				queue.clear();
				scheduler.accept(null);
				return true;
			}
			if (d) {
				if (delayError) {
					if (empty) {
						scheduler.accept(null);
						Throwable e = error;
						if (e != null) {
							a.onError(e);
						} else {
							a.onComplete();
						}
						return true;
					}
				} else {
					Throwable e = error;
					if (e != null) {
						scheduler.accept(null);
						queue.clear();
						a.onError(e);
						return true;
					} else 
					if (empty) {
						scheduler.accept(null);
						a.onComplete();
						return true;
					}
				}
			}
			
			return false;
		}
	}
}