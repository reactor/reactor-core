/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.annotation.Nullable;

/**
 * Subscribes to the upstream Mono on the specified Scheduler and makes sure
 * any request from downstream is issued on the same worker where the subscription
 * happened.
 *
 * @param <T> the value type
 */
final class MonoSubscribeOn<T> extends InternalMonoOperator<T, T> {

	final Scheduler scheduler;

	MonoSubscribeOn(Mono<? extends T> source, Scheduler scheduler) {
		super(source);
		this.scheduler = scheduler;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		Scheduler.Worker worker = scheduler.createWorker();

		SubscribeOnSubscriber<T> parent = new SubscribeOnSubscriber<>(source,
				actual, worker);
		actual.onSubscribe(parent);

		try {
			worker.schedule(parent);
		}
		catch (RejectedExecutionException ree) {
			if (parent.s != Operators.cancelledSubscription()) {
				actual.onError(Operators.onRejectedExecution(ree, parent, null, null,
						actual.currentContext()));
			}
		}
		return null;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return scheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return super.scanUnsafe(key);
	}

	static final class SubscribeOnSubscriber<T>
			implements InnerOperator<T, T>, Runnable {

		final CoreSubscriber<? super T> actual;

		final Publisher<? extends T> parent;

		final Scheduler.Worker worker;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
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

		SubscribeOnSubscriber(Publisher<? extends T> parent,
				CoreSubscriber<? super T> actual,
				Worker worker) {
			this.actual = actual;
			this.parent = parent;
			this.worker = worker;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return s;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.RUN_ON) return worker;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void run() {
			THREAD.lazySet(this, Thread.currentThread());
			parent.subscribe(this);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				long r = REQUESTED.getAndSet(this, 0L);
				if (r != 0L) {
					trySchedule(r, s);
				}
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			try{
				actual.onError(t);
			}
			finally {
				worker.dispose();
				THREAD.lazySet(this,null);
			}
		}

		@Override
		public void onComplete() {
			actual.onComplete();
			worker.dispose();
			THREAD.lazySet(this,null);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Subscription a = s;
				if (a != null) {
					trySchedule(n, a);
				}
				else {
					Operators.addCap(REQUESTED, this, n);
					a = s;
					if (a != null) {
						long r = REQUESTED.getAndSet(this, 0L);
						if (r != 0L) {
							trySchedule(n, a);
						}
					}
				}
			}
		}

		void trySchedule(long n, Subscription s) {
			if (Thread.currentThread() == THREAD.get(this)) {
				s.request(n);
			}
			else {
				try {
					worker.schedule(() -> s.request(n));

				}
				catch (RejectedExecutionException ree) {
					if (!worker.isDisposed()) {
						actual.onError(Operators.onRejectedExecution(ree,
								this,
								null,
								null,
								actual.currentContext()));
					}
				}
			}
		}

		@Override
		public void cancel() {
			Operators.terminate(S, this);
			worker.dispose();
		}
	}
}
