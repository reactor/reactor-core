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
 * Subscribes to the upstream Mono on the specified Scheduler and makes sure
 * any request from downstream is issued on the same worker where the subscription
 * happened.
 *
 * @param <T> the value type
 */
final class MonoSubscribeOn<T> extends MonoOperator<T, T> {

	final Scheduler scheduler;

	MonoSubscribeOn(Mono<? extends T> source, Scheduler scheduler) {
		super(source);
		this.scheduler = scheduler;
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		Scheduler.Worker worker = scheduler.createWorker();

		SubscribeOnSubscriber<T> parent = new SubscribeOnSubscriber<>(source, s, worker);
		s.onSubscribe(parent);

		if (worker.schedule(parent) == Scheduler.REJECTED && !worker.isDisposed()) {
			s.onError(Operators.onRejectedExecution(parent, null, null));
		}
	}

	static final class SubscribeOnSubscriber<T>
			implements InnerOperator<T, T>, Runnable {

		final Subscriber<? super T> actual;

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

		SubscribeOnSubscriber(Publisher<? extends T> parent,
				Subscriber<? super T> actual,
				Worker worker) {
			this.actual = actual;
			this.parent = parent;
			this.worker = worker;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == ScannableAttr.PARENT) return s;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void run() {
			parent.subscribe(this);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!Operators.setOnce(S, this, s)) {
				s.cancel();
			}
			else {
				long r = REQUESTED.getAndSet(this, 0L);
				if (r != 0L) {
					if (worker.schedule(() -> requestMore(r)) == Scheduler.REJECTED && !worker.isDisposed()) {
						actual.onError(Operators.onRejectedExecution(this, null, null));
					}
				}
			}
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			try {
				actual.onError(t);
			}
			finally {
				worker.dispose();
			}
		}

		@Override
		public void onComplete() {
			try {
				actual.onComplete();
			}
			finally {
				worker.dispose();
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (worker.schedule(() -> requestMore(n)) == Scheduler.REJECTED && !worker.isDisposed()) {
					actual.onError(Operators.onRejectedExecution(this, null, null));
				}
			}
		}

		@Override
		public void cancel() {
			if (Operators.terminate(S, this)) {
				worker.dispose();
			}
		}

		void requestMore(long n) {
			Subscription a = s;
			if (a != null) {
				a.request(n);
			}
			else {
				Operators.getAndAddCap(REQUESTED, this, n);
				a = s;
				if (a != null) {
					long r = REQUESTED.getAndSet(this, 0L);
					if (r != 0L) {
						a.request(r);
					}
				}
			}
		}
	}
}
