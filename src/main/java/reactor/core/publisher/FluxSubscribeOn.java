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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;

import static reactor.core.publisher.FluxSubscribeOnValue.scalarScheduleOn;

/**
 * Subscribes to the source Publisher asynchronously through a scheduler function or
 * ExecutorService.
 * 
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSubscribeOn<T> extends FluxSource<T, T> implements Loopback {

	final Scheduler scheduler;
	
	public FluxSubscribeOn(
			Publisher<? extends T> source, 
			Scheduler scheduler) {
		super(source);
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s) {
		if (source instanceof Fuseable.ScalarCallable) {
			if (!scalarScheduleOn(source, s, scheduler)) {
				MonoSubscribeOnCallable.subscribe((Callable<T>) source, s, scheduler);
			}
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
			Operators.error(s, Operators.onOperatorError(new
					NullPointerException("The scheduler returned a null Function")));
			return;
		}

		SubscribeOnSubscriber<T> parent = new SubscribeOnSubscriber<>(s, worker);
		s.onSubscribe(parent);
		
		if (worker.schedule(new SourceSubscribeTask<>(parent, source)) == Scheduler.REJECTED) {
			throw Operators.onRejectedExecution(parent, null, null);
		}
	}

	@Override
	public Object connectedInput() {
		return scheduler;
	}

	@Override
	public Object connectedOutput() {
		return null;
	}

	static final class SubscribeOnSubscriber<T>
			implements Subscription, Subscriber<T>, Producer, Loopback {

		final Subscriber<? super T> actual;

		final Worker worker;

		volatile Subscription s;
		static final AtomicReferenceFieldUpdater<SubscribeOnSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(SubscribeOnSubscriber.class,
						Subscription.class,
						"s");


		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SubscribeOnSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(SubscribeOnSubscriber.class,
						"requested");

		volatile Thread thread;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SubscribeOnSubscriber, Thread> THREAD =
				AtomicReferenceFieldUpdater.newUpdater(SubscribeOnSubscriber.class,
						Thread.class,
						"thread");

		public SubscribeOnSubscriber(Subscriber<? super T> actual, Worker worker) {
			this.actual = actual;
			this.worker = worker;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				long r = REQUESTED.getAndSet(this, 0L);
				if (r != 0L) {
					requestUpstream(r, s);
				}
			}
		}

		void requestUpstream(final long n, final Subscription s) {
			if (Thread.currentThread() == THREAD.get(this)) {
				s.request(n);
			}
			else {
				//Do not check REJECTED in request flow and silently drop requests on shutdown scheduler
				worker.schedule(() -> s.request(n));
			}
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
				Subscription s = S.get(this);
				if (s != null) {
					requestUpstream(n, s);
				}
				else {
					Operators.addAndGet(REQUESTED, this, n);
					s = S.get(this);
					if (s != null) {
						long r = REQUESTED.getAndSet(this, 0L);
						if (r != 0L) {
							requestUpstream(r, s);
						}
					}

				}
			}
		}

		@Override
		public void cancel() {
			Subscription a = s;
			if (a != Operators.cancelledSubscription()) {
				a = S.getAndSet(this, Operators.cancelledSubscription());
				if (a != null && a != Operators.cancelledSubscription()) {
					a.cancel();
				}
			}
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

	static final class SourceSubscribeTask<T> implements Runnable {

		final SubscribeOnSubscriber<T> actual;
		
		final Publisher<? extends T> source;

		public SourceSubscribeTask(SubscribeOnSubscriber<T> s,
				Publisher<? extends T> source) {
			this.actual = s;
			this.source = source;
		}

		@Override
		public void run() {
			SubscribeOnSubscriber.THREAD.lazySet(actual, Thread.currentThread());
			source.subscribe(actual);
		}
	}

}
