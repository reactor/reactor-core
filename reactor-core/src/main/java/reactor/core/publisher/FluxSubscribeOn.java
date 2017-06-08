/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Subscribes to the source Publisher asynchronously through a scheduler function or
 * ExecutorService.
 * 
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSubscribeOn<T> extends FluxOperator<T, T> {

	final Scheduler scheduler;
	
	FluxSubscribeOn(
			Flux<? extends T> source,
			Scheduler scheduler) {
		super(source);
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		Worker worker;
		
		try {
			worker = Objects.requireNonNull(scheduler.createWorker(),
					"The scheduler returned a null Function");
		} catch (Throwable e) {
			Operators.error(s, Operators.onOperatorError(e));
			return;
		}

		SubscribeOnSubscriber<T> parent = new SubscribeOnSubscriber<>(source, s, worker);
		s.onSubscribe(parent);
		
		if (worker.schedule(parent) == Scheduler.REJECTED && !worker.isDisposed()) {
			s.onError(Operators.onRejectedExecution(parent, null, null));
		}
	}

	static final class SubscribeOnSubscriber<T>
			implements InnerOperator<T, T>, Runnable {

		final Subscriber<? super T> actual;

		final Publisher<? extends T> source;

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

		SubscribeOnSubscriber(Publisher<? extends T> source, Subscriber<? super
				T> actual, Worker worker) {
			this.actual = actual;
			this.worker = worker;
			this.source = source;
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
				if(worker.schedule(() -> s.request(n)) == Scheduler.REJECTED &&
						!worker.isDisposed()){
					throw Operators.onRejectedExecution(this, null, null);
				}
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
				worker.dispose();
			}
		}

		@Override
		public void onComplete() {
			try {
				actual.onComplete();
			} finally {
				worker.dispose();
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
		public void run() {
			THREAD.lazySet(this, Thread.currentThread());
			source.subscribe(this);
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
			worker.dispose();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

	}

}
