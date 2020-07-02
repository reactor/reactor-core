/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

final class FluxCancelOn<T> extends InternalFluxOperator<T, T> {

	final Scheduler scheduler;

	public FluxCancelOn(Flux<T> source, Scheduler scheduler) {
		super(source);
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new CancelSubscriber<T>(actual, scheduler);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return scheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return super.scanUnsafe(key);
	}

	static final class CancelSubscriber<T>
			implements InnerOperator<T, T>, Runnable {

		final CoreSubscriber<? super T> actual;
		final Scheduler             scheduler;

		Subscription s;

		volatile int cancelled = 0;
		static final AtomicIntegerFieldUpdater<CancelSubscriber> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(CancelSubscriber.class, "cancelled");

		CancelSubscriber(CoreSubscriber<? super T> actual, Scheduler scheduler) {
			this.actual = actual;
			this.scheduler = scheduler;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return cancelled == 1;
			if (key == Attr.RUN_ON) return scheduler;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void run() {
			s.cancel();
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				try {
					scheduler.schedule(this);
				}
				catch (RejectedExecutionException ree) {
					throw Operators.onRejectedExecution(ree, actual.currentContext());
				}
			}
		}
	}
}
