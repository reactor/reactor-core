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
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

import org.reactivestreams.*;

import reactor.core.flow.*;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.core.util.*;

/**
 * Subscribes to the source Publisher asynchronously through a scheduler function or
 * ExecutorService.
 * 
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxSubscribeOn<T> extends FluxSource<T, T> implements Loopback {

	final Scheduler scheduler;
	
	public FluxSubscribeOn(
			Publisher<? extends T> source, 
			Scheduler scheduler) {
		super(source);
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	public static <T> void scalarScheduleOn(Publisher<? extends T> source, Subscriber<? super T> s, Scheduler scheduler) {
		@SuppressWarnings("unchecked")
		Supplier<T> supplier = (Supplier<T>) source;
		
		T v = supplier.get();
		
		if (v == null) {
			ScheduledEmpty parent = new ScheduledEmpty(s);
			s.onSubscribe(parent);
			Cancellation f = scheduler.schedule(parent);
			parent.setFuture(f);
		} else {
			s.onSubscribe(new ScheduledScalar<>(s, v, scheduler));
		}
	}
	
	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (source instanceof Supplier) {
			scalarScheduleOn(source, s, scheduler);
			return;
		}
		
		Worker worker;
		
		try {
			worker = scheduler.createWorker();
		} catch (Throwable e) {
			Exceptions.throwIfFatal(e);
			EmptySubscription.error(s, e);
			return;
		}
		
		if (worker == null) {
			EmptySubscription.error(s, new NullPointerException("The scheduler returned a null Function"));
			return;
		}

		SubscribeOnPipeline<T> parent = new SubscribeOnPipeline<>(s, worker);
		s.onSubscribe(parent);
		
		worker.schedule(new SourceSubscribeTask<>(parent, source));
	}

	@Override
	public Object connectedInput() {
		return scheduler;
	}

	@Override
	public Object connectedOutput() {
		return null;
	}

	static final class SubscribeOnPipeline<T>
			extends DeferredSubscription implements Subscriber<T>, Producer, Loopback, Runnable {
		final Subscriber<? super T> actual;

		final Worker worker;

		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SubscribeOnPipeline> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(SubscribeOnPipeline.class, "requested");

		volatile int wip;

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SubscribeOnPipeline> WIP =
				AtomicIntegerFieldUpdater.newUpdater(SubscribeOnPipeline.class, "wip");

		public SubscribeOnPipeline(Subscriber<? super T> actual, Worker worker) {
			this.actual = actual;
			this.worker = worker;
		}

		@Override
		public void onSubscribe(Subscription s) {
			set(s);
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			try {
				actual.onError(t);
			} finally {
				worker.shutdown();
			}
		}

		@Override
		public void onComplete() {
			try {
				actual.onComplete();
			} finally {
				worker.shutdown();
			}
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.getAndAddCap(REQUESTED, this, n);
				if(WIP.getAndIncrement(this) == 0){
					worker.schedule(this);
				}
			}
		}

		@Override
		public void run() {
			long r;
			int missed = 1;
			for(;;){
				r = REQUESTED.getAndSet(this, 0L);

				if(r != 0L) {
					super.request(r);
				}

				if(r == Long.MAX_VALUE){
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if(missed == 0){
					break;
				}
			}
		}

		@Override
		public void cancel() {
			super.cancel();
			worker.shutdown();
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object connectedOutput() {
			return worker;
		}

		@Override
		public Object connectedInput() {
			return null;
		}
	}
	
	static final class ScheduledScalar<T>
			implements Subscription, Runnable, Producer, Loopback {

		final Subscriber<? super T> actual;
		
		final T value;
		
		final Scheduler scheduler;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ScheduledScalar> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(ScheduledScalar.class, "once");
		
		volatile Cancellation future;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ScheduledScalar, Cancellation> FUTURE =
				AtomicReferenceFieldUpdater.newUpdater(ScheduledScalar.class, Cancellation.class, "future");
		
		static final Cancellation CANCELLED = () -> { };

		static final Cancellation FINISHED = () -> { };

		public ScheduledScalar(Subscriber<? super T> actual, T value, Scheduler scheduler) {
			this.actual = actual;
			this.value = value;
			this.scheduler = scheduler;
		}
		
		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				if (ONCE.compareAndSet(this, 0, 1)) {
					Cancellation f = scheduler.schedule(this);
					if (!FUTURE.compareAndSet(this, null, f)) {
						if (future != FINISHED && future != CANCELLED) {
							f.dispose();
						}
					}
				}
			}
		}
		
		@Override
		public void cancel() {
			ONCE.lazySet(this, 1);
			Cancellation f = future;
			if (f != CANCELLED && future != FINISHED) {
				f = FUTURE.getAndSet(this, CANCELLED);
				if (f != null && f != CANCELLED && f != FINISHED) {
					f.dispose();
				}
			}
		}
		
		@Override
		public void run() {
			try {
				actual.onNext(value);
				actual.onComplete();
			} finally {
				FUTURE.lazySet(this, FINISHED);
			}
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object connectedInput() {
			return scheduler;
		}

		@Override
		public Object connectedOutput() {
			return value;
		}
	}

	static final class ScheduledEmpty implements Subscription, Runnable, Producer, Loopback {
		final Subscriber<?> actual;

		volatile Cancellation future;
		static final AtomicReferenceFieldUpdater<ScheduledEmpty, Cancellation> FUTURE =
				AtomicReferenceFieldUpdater.newUpdater(ScheduledEmpty.class, Cancellation.class, "future");
		
		static final Cancellation CANCELLED = () -> { };
		
		static final Cancellation FINISHED = () -> { };

		public ScheduledEmpty(Subscriber<?> actual) {
			this.actual = actual;
		}

		@Override
		public void request(long n) {
			BackpressureUtils.validate(n);
		}

		@Override
		public void cancel() {
			Cancellation f = future;
			if (f != CANCELLED && f != FINISHED) {
				f = FUTURE.getAndSet(this, CANCELLED);
				if (f != null && f != CANCELLED && f != FINISHED) {
					f.dispose();
				}
			}
		}

		@Override
		public void run() {
			try {
				actual.onComplete();
			} finally {
				FUTURE.lazySet(this, FINISHED);
			}
		}

		void setFuture(Cancellation f) {
			if (!FUTURE.compareAndSet(this, null, f)) {
				Cancellation a = future;
				if (a != FINISHED && a != CANCELLED) {
					f.dispose();
				}
			}
		}
		
		@Override
		public Object connectedInput() {
			return null; // FIXME value?
		}

		@Override
		public Object downstream() {
			return actual;
		}
	}

	static final class SourceSubscribeTask<T> implements Runnable {

		final Subscriber<? super T> actual;
		
		final Publisher<? extends T> source;

		public SourceSubscribeTask(Subscriber<? super T> s, Publisher<? extends T> source) {
			this.actual = s;
			this.source = source;
		}

		@Override
		public void run() {
			source.subscribe(actual);
		}
	}

}
