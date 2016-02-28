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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Loopback;
import reactor.core.flow.Producer;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.DeferredSubscription;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;

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
final class FluxPublishOn<T> extends FluxSource<T, T> implements Loopback {

	final Callable<? extends Consumer<Runnable>> schedulerFactory;
	
	public FluxPublishOn(
			Publisher<? extends T> source, 
			Callable<? extends Consumer<Runnable>> schedulerFactory) {
		super(source);
		this.schedulerFactory = Objects.requireNonNull(schedulerFactory, "schedulerFactory");
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

		PublishOnPipeline<T> parent = new PublishOnPipeline<>(s, scheduler);
		//FluxPublishOnPipeline<T> parent = new FluxPublishOnPipeline<>(s, scheduler);
		s.onSubscribe(parent);
		
		scheduler.accept(new SourceSubscribeTask<>(parent, source));
	}

	@Override
	public Object connectedInput() {
		return schedulerFactory;
	}

	static final class PublishOnPipeline<T>
			extends DeferredSubscription implements Subscriber<T>, Producer, Loopback, Runnable {
		final Subscriber<? super T> actual;

		final Consumer<Runnable> scheduler;

		volatile long requested;

		static final AtomicLongFieldUpdater<PublishOnPipeline> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(PublishOnPipeline.class, "requested");

		volatile int wip;

		static final AtomicIntegerFieldUpdater<PublishOnPipeline> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PublishOnPipeline.class, "wip");

		public PublishOnPipeline(Subscriber<? super T> actual, Consumer<Runnable> scheduler) {
			this.actual = actual;
			this.scheduler = scheduler;
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
			scheduler.accept(null);
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			scheduler.accept(null);
			actual.onComplete();
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.addAndGet(REQUESTED, this, n);
				if(WIP.getAndIncrement(this) == 0){
					scheduler.accept(this);
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
			scheduler.accept(null);
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object connectedOutput() {
			return scheduler;
		}

		@Override
		public Object connectedInput() {
			return null;
		}
	}

	static final class ScheduledSubscriptionEagerCancel<T>
			implements Subscription, Runnable, Producer, Loopback {

		final Subscriber<? super T> actual;
		
		final T value;
		
		final Consumer<Runnable> scheduler;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ScheduledSubscriptionEagerCancel> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(ScheduledSubscriptionEagerCancel.class, "once");

		public ScheduledSubscriptionEagerCancel(Subscriber<? super T> actual, T value, Consumer<Runnable> scheduler) {
			this.actual = actual;
			this.value = value;
			this.scheduler = scheduler;
		}
		
		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				if (ONCE.compareAndSet(this, 0, 1)) {
					scheduler.accept(this);
				}
			}
		}
		
		@Override
		public void cancel() {
			ONCE.lazySet(this, 1);
			scheduler.accept(null);
		}
		
		@Override
		public void run() {
			actual.onNext(value);
			scheduler.accept(null);
			actual.onComplete();
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

	static final class ScheduledEmptySubscriptionEager implements Subscription, Runnable, Producer, Loopback {
		final Subscriber<?> actual;

		final Consumer<Runnable> scheduler;

		public ScheduledEmptySubscriptionEager(Subscriber<?> actual, Consumer<Runnable> scheduler) {
			this.actual = actual;
			this.scheduler = scheduler;
		}

		@Override
		public void request(long n) {
			BackpressureUtils.validate(n);
		}

		@Override
		public void cancel() {
			scheduler.accept(null);
		}

		@Override
		public void run() {
			scheduler.accept(null);
			actual.onComplete();
		}

		@Override
		public Object connectedInput() {
			return scheduler;
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
