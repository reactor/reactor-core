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

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

/**
 * Schedules the emission of the value or completion of the wrapped Mono via
 * the given Scheduler.
 *
 * @param <T> the value type
 */
final class MonoPublishOn<T> extends InternalMonoOperator<T, T> {

	final Scheduler scheduler;

	MonoPublishOn(Mono<? extends T> source, Scheduler scheduler) {
		super(source);
		this.scheduler = scheduler;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new PublishOnSubscriber<T>(actual, scheduler);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return scheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return super.scanUnsafe(key);
	}

	static final class PublishOnSubscriber<T>
			implements InnerOperator<T, T>, Runnable {

		final CoreSubscriber<? super T> actual;

		final Scheduler scheduler;

		Subscription s;

		volatile Disposable future;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<PublishOnSubscriber, Disposable>
				FUTURE =
				AtomicReferenceFieldUpdater.newUpdater(PublishOnSubscriber.class,
						Disposable.class,
						"future");


		volatile T         value;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<PublishOnSubscriber, Object>
				VALUE =
				AtomicReferenceFieldUpdater.newUpdater(PublishOnSubscriber.class,
						Object.class,
						"value");

		volatile Throwable error;

		PublishOnSubscriber(CoreSubscriber<? super T> actual,
				Scheduler scheduler) {
			this.actual = actual;
			this.scheduler = scheduler;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return future == OperatorDisposables.DISPOSED;
			if (key == Attr.PARENT) return s;
			if (key == Attr.ERROR) return error;
			if (key == Attr.RUN_ON) return scheduler;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			value = t;
			trySchedule(this, null, t);
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			trySchedule(null, t, null);
		}

		@Override
		public void onComplete() {
			if (value == null) {
				trySchedule(null, null, null);
			}
		}

		void trySchedule(
				@Nullable Subscription subscription,
				@Nullable Throwable suppressed,
				@Nullable Object dataSignal) {

				if(future != null){
					return;
				}

				try {
					future = this.scheduler.schedule(this);
				}
				catch (RejectedExecutionException ree) {
					actual.onError(Operators.onRejectedExecution(ree, subscription,
							suppressed,	dataSignal, actual.currentContext()));
				}
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			Disposable c = future;
			if (c != OperatorDisposables.DISPOSED) {
				c = FUTURE.getAndSet(this, OperatorDisposables.DISPOSED);
				if (c != null && !OperatorDisposables.isDisposed(c)) {
					c.dispose();
				}
				value = null;
			}
			s.cancel();
		}

		@Override
		@SuppressWarnings("unchecked")
		public void run() {
			if (OperatorDisposables.isDisposed(future)) {
				return;
			}
			T v = (T)VALUE.getAndSet(this, null);

			if (v != null) {
				actual.onNext(v);
				actual.onComplete();
			}
			else {
				Throwable e = error;
				if (e != null) {
					actual.onError(e);
				}
				else {
					actual.onComplete();
				}
			}
		}
	}
}
