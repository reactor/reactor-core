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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;

/**
 * Emits events on a different thread specified by a scheduler callback.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxPublishOn<T> extends FluxSource<T, T> implements Loopback, Fuseable {

	private static final Cancellation NULL_CANCELLATION = () -> {};

	final Scheduler scheduler;
	
	final boolean delayError;
	
	final Supplier<? extends Queue<T>> queueSupplier;
	
	final int prefetch;
	
	public FluxPublishOn(
			Publisher<? extends T> source, 
			Scheduler scheduler, 
			boolean delayError,
			int prefetch,
			Supplier<? extends Queue<T>> queueSupplier) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
		this.delayError = delayError;
		this.prefetch = prefetch;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public long getPrefetch() {
		return prefetch;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s) {

		if (FluxSubscribeOnValue.scalarScheduleOn(source, s, scheduler)) {
			return;
		}

		Worker worker;
		
		try {
			worker = scheduler.createWorker();
		} catch (Throwable e) {
			Operators.error(s, Operators.onOperatorError(e));
			return;
		}
		
		if (worker == null) {
			Operators.error(s,
					new NullPointerException("The scheduler returned a null Function"));
			return;
		}

		if (s instanceof ConditionalSubscriber) {
			ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) s;
			source.subscribe(new PublishOnConditionalSubscriber<>(cs,
					worker,
					delayError,
					prefetch,
					queueSupplier));
			return;
		}
		source.subscribe(new PublishOnSubscriber<>(s,
				worker,
				delayError,
				prefetch,
				queueSupplier));
	}


	@Override
	public Object connectedOutput() {
		return scheduler;
	}

	static final class PublishOnSubscriber<T>
			implements Subscriber<T>, QueueSubscription<T>, Runnable, Producer, Loopback,
			           Receiver, Trackable {
		
		final Subscriber<? super T> actual;
		
		final Worker worker;
		
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
		static final AtomicIntegerFieldUpdater<PublishOnSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PublishOnSubscriber.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PublishOnSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PublishOnSubscriber.class, "requested");

		int sourceMode;
		
		long produced;

		boolean outputFused;

		public PublishOnSubscriber(
				Subscriber<? super T> actual,
				Worker worker,
				boolean delayError,
				int prefetch,
				Supplier<? extends Queue<T>> queueSupplier) {
			this.actual = actual;
			this.worker = worker;
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
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked") QueueSubscription<T> f =
							(QueueSubscription<T>) s;
					
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

						actual.onSubscribe(this);

						initialRequest();

						return;
					}
				}

				try {
					queue = queueSupplier.get();
				}
				catch (Throwable e) {
					try {
						Operators.error(actual, Operators.onOperatorError(s, e));
					}
					finally {
						worker.shutdown();
					}
					return;
				}

				actual.onSubscribe(this);

				initialRequest();
			}
		}

		void initialRequest() {
			if (prefetch == Integer.MAX_VALUE) {
				s.request(Long.MAX_VALUE);
			}
			else {
				s.request(prefetch);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			if (sourceMode == Fuseable.ASYNC) {
				if (trySchedule() == Scheduler.REJECTED) {
					throw Operators.onRejectedExecution(this, null, t);
				}
				return;
			}
			if (!queue.offer(t)) {
				error = Operators.onOperatorError(s, Exceptions.failWithOverflow("Queue is full?!"), t);
				done = true;
			}
			if (trySchedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution(this, null, t);
			}
		}
		
		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			error = t;
			done = true;
			if (trySchedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution(null, t, null);
			}
		}
		
		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			if (trySchedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution();
			}
		}
		
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				//Do not check REJECTED in request flow and silently drop requests on shutdown scheduler
				trySchedule();
			}
		}
		
		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}

			cancelled = true;
			s.cancel();
			worker.shutdown();

			if (WIP.getAndIncrement(this) == 0) {
				queue.clear();
			}
		}

		Cancellation trySchedule() {
			if (WIP.getAndIncrement(this) != 0) {
				return NULL_CANCELLATION;
			}

			return worker.schedule(this);
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
						doError(a, Operators.onOperatorError(s, ex));
						return;
					}

					if (cancelled) {
						return;
					}
					if (v == null) {
						doComplete(a);
						return;
					}

					a.onNext(v);

					e++;
				}

				if (cancelled) {
					return;
				}

				boolean empty;

				try {
					empty = q.isEmpty();
				} catch (Throwable ex) {
					doError(a, Operators.onOperatorError(s, ex));
					return;
				}

				if (empty) {
					doComplete(a);
					return;
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
						q.clear();

						doError(a, Operators.onOperatorError(ex));
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
						q.clear();

						doError(a, Operators.onOperatorError(ex));
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

		void runBackfused() {
			int missed = 1;

			for (; ; ) {

				if (cancelled) {
					return;
				}

				boolean d = done;

				actual.onNext(null);

				if (d) {
					Throwable e = error;
					if (e != null) {
						doError(actual, e);
					}
					else {
						doComplete(actual);
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void doComplete(Subscriber<?> a) {
			try {
				a.onComplete();
			} finally {
				worker.shutdown();
			}
		}
		
		void doError(Subscriber<?> a, Throwable e) {
			try {
				a.onError(e);
			} finally {
				worker.shutdown();
			}
		}
		
		@Override
		public void run() {
			if (outputFused) {
				runBackfused();
			}
			else
			if (sourceMode == Fuseable.SYNC) {
				runSync();
			} else {
				runAsync();
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
			if (cancelled) {
				queue.clear();
				return true;
			}
			if (d) {
				if (delayError) {
					if (empty) {
						Throwable e = error;
						if (e != null) {
							doError(a, e);
						} else {
							doComplete(a);
						}
						return true;
					}
				} else {
					Throwable e = error;
					if (e != null) {
						queue.clear();
						doError(a, e);
						return true;
					} else
					if (empty) {
						doComplete(a);
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
			return worker;
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
		public void clear() {
			queue.clear();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public T poll() {
			T v = queue.poll();
			if (v != null && sourceMode != SYNC) {
				long p = produced + 1;
				if (p == limit) {
					produced = 0;
					s.request(p);
				}
				else {
					produced = p;
				}
			}
			return v;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0) {
				outputFused = true;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public int size() {
			return queue.size();
		}
	}

	static final class PublishOnConditionalSubscriber<T>
			implements Subscriber<T>, QueueSubscription<T>, Runnable, Producer, Loopback,
			           Receiver, Trackable {

		final ConditionalSubscriber<? super T> actual;
		
		final Worker worker;
		
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
		static final AtomicIntegerFieldUpdater<PublishOnConditionalSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PublishOnConditionalSubscriber.class,
						"wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PublishOnConditionalSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PublishOnConditionalSubscriber.class,
						"requested");

		int sourceMode;
		
		long produced;
		
		long consumed;

		boolean outputFused;

		public PublishOnConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				Worker worker,
				boolean delayError,
				int prefetch,
				Supplier<? extends Queue<T>> queueSupplier) {
			this.actual = actual;
			this.worker = worker;
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
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked") QueueSubscription<T> f =
							(QueueSubscription<T>) s;
					
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

						actual.onSubscribe(this);

						initialRequest();

						return;
					}
				}
				try {
					queue = queueSupplier.get();
				}
				catch (Throwable e) {
					try {
						Operators.error(actual, Operators.onOperatorError(s, e));
					}
					finally {
						worker.shutdown();
					}

					return;
				}
				
				actual.onSubscribe(this);

				initialRequest();
			}
		}

		void initialRequest() {
			if (prefetch == Integer.MAX_VALUE) {
				s.request(Long.MAX_VALUE);
			}
			else {
				s.request(prefetch);
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == Fuseable.ASYNC) {
				if (trySchedule() == Scheduler.REJECTED) {
					throw Operators.onRejectedExecution(this, null, t);
				}
				return;
			}
			if (!queue.offer(t)) {
				s.cancel();
				
				error = Exceptions.failWithOverflow("Queue is full?!");
				done = true;
			}
			if (trySchedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution(this, null, t);
			}
		}
		
		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;
			if (trySchedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution(null, t, null);
			}
		}
		
		@Override
		public void onComplete() {
			done = true;
			if (trySchedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution();
			}
		}
		
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				//Do not check REJECTED in request flow and silently drop requests on shutdown scheduler
				trySchedule();
			}
		}
		
		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			
			cancelled = true;
			s.cancel();
			worker.shutdown();
			
			if (WIP.getAndIncrement(this) == 0) {
				queue.clear();
			}
		}
		
		Cancellation trySchedule() {
			if (WIP.getAndIncrement(this) != 0) {
				return NULL_CANCELLATION;
			}

			return worker.schedule(this);
		}
		
		void runSync() {
			int missed = 1;

			final ConditionalSubscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long e = produced;

			for (;;) {
				
				long r = requested;
				
				while (e != r) {
					T v;
					try {
						v = q.poll();
					} catch (Throwable ex) {
						doError(a, Operators.onOperatorError(s, ex));
						return;
					}

					if (cancelled) {
						return;
					}
					if (v == null) {
						doComplete(a);
						return;
					}
					
					if (a.tryOnNext(v)) {
						e++;
					}
				}

				if (cancelled) {
					return;
				}

				boolean empty;

				try {
					empty = q.isEmpty();
				} catch (Throwable ex) {
					doError(a, Operators.onOperatorError(s, ex));
					return;
				}

				if (empty) {
					doComplete(a);
					return;
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

			final ConditionalSubscriber<? super T> a = actual;
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
						q.clear();
						
						doError(a, Operators.onOperatorError(ex));
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
						q.clear();
						
						doError(a, Operators.onOperatorError(ex));
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

		void runBackfused() {
			int missed = 1;

			for (; ; ) {

				if (cancelled) {
					return;
				}

				boolean d = done;

				actual.onNext(null);

				if (d) {
					Throwable e = error;
					if (e != null) {
						doError(actual, e);
					}
					else {
						doComplete(actual);
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void run() {
			if (outputFused) {
				runBackfused();
			}
			else
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
			return worker;
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

		void doComplete(Subscriber<?> a) {
			try {
				a.onComplete();
			} finally {
				worker.shutdown();
			}
		}
		
		void doError(Subscriber<?> a, Throwable e) {
			try {
				a.onError(e);
			} finally {
				worker.shutdown();
			}
		}
		
		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
			if (cancelled) {
				queue.clear();
				return true;
			}
			if (d) {
				if (delayError) {
					if (empty) {
						Throwable e = error;
						if (e != null) {
							doError(a, e);
						} else {
							doComplete(a);
						}
						return true;
					}
				} else {
					Throwable e = error;
					if (e != null) {
						queue.clear();
						doError(a, e);
						return true;
					} else 
					if (empty) {
						doComplete(a);
						return true;
					}
				}
			}
			
			return false;
		}

		@Override
		public void clear() {
			queue.clear();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public T poll() {
			T v = queue.poll();
			if (v != null && sourceMode != SYNC) {
				long p = consumed + 1;
				if (p == limit) {
					consumed = 0;
					s.request(p);
				}
				else {
					consumed = p;
				}
			}
			return v;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0) {
				outputFused = true;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public int size() {
			return queue.size();
		}
	}
}
