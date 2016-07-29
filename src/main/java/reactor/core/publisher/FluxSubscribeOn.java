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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.core.Exceptions;

/**
 * Subscribes to the source Publisher asynchronously through a scheduler function or
 * ExecutorService.
 * 
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSubscribeOn<T> extends FluxSource<T, T> implements Loopback, Fuseable {

	final Scheduler scheduler;
	
	public FluxSubscribeOn(
			Publisher<? extends T> source, 
			Scheduler scheduler) {
		super(source);
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (source instanceof ScalarCallable) {
			FluxSubscribeOnValue.singleScheduleOn(source, s, scheduler);
			return;
		}
		
		Worker worker;
		
		try {
			worker = scheduler.createWorker();
		} catch (Throwable e) {
			Operators.error(s, Exceptions.mapOperatorError(null, e));
			return;
		}
		
		if (worker == null) {
			Operators.error(s, Exceptions.mapOperatorError(null, new
					NullPointerException("The scheduler returned a null Function")));
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

	static final class SubscribeOnPipeline<T> extends Operators.DeferredSubscription
			implements Subscriber<T>, Producer, Loopback, Runnable, QueueSubscription<T> {

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
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
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

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public T poll() {
			return null;
		}

		@Override
		public void clear() {

		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public int size() {
			return 0;
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
