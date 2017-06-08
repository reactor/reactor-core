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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Schedules the emission of the value or completion of the wrapped Mono via
 * the given Scheduler.
 *
 * @param <T> the value type
 */
final class MonoPublishOn<T> extends MonoOperator<T, T> {

	final Scheduler scheduler;

	MonoPublishOn(Mono<? extends T> source, Scheduler scheduler) {
		super(source);
		this.scheduler = scheduler;
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		source.subscribe(new PublishOnSubscriber<T>(s, scheduler), ctx);
	}

	static final class PublishOnSubscriber<T>
			implements InnerOperator<T, T>, Runnable {

		final Subscriber<? super T> actual;

		final Scheduler scheduler;

		Subscription s;

		volatile Disposable future;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<PublishOnSubscriber, Disposable>
				FUTURE =
				AtomicReferenceFieldUpdater.newUpdater(PublishOnSubscriber.class,
						Disposable.class,
						"future");

		T         value;
		Throwable error;

		PublishOnSubscriber(Subscriber<? super T> actual,
				Scheduler scheduler) {
			this.actual = actual;
			this.scheduler = scheduler;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.CANCELLED) return future == Disposables.DISPOSED;
			if (key == ScannableAttr.PARENT) return s;
			if (key == ThrowableAttr.ERROR) return error;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Subscriber<? super T> actual() {
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
			if(schedule() == Scheduler.REJECTED){
				throw Operators.onRejectedExecution(this, null, t);
			}
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			if (schedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution(null, t, null);
			}
		}

		@Override
		public void onComplete() {
			if (value == null) {
				if (schedule() == Scheduler.REJECTED && future != Disposables.DISPOSED) {
					throw Operators.onRejectedExecution();
				}
			}
		}

		@Nullable
		Disposable schedule() {
			if (future == null) {
				Disposable c = scheduler.schedule(this);
				if (!FUTURE.compareAndSet(this, null, c)) {
					c.dispose();
				}
				return c;
			}
			return null;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			Disposable c = future;
			if (c != Disposables.DISPOSED) {
				c = FUTURE.getAndSet(this, Disposables.DISPOSED);
				if (c != null && c != Disposables.DISPOSED) {
					c.dispose();
				}
			}
			s.cancel();
		}

		@Override
		public void run() {
			if (Disposables.isDisposed(future)) {
				return;
			}
			T v = value;
			value = null;
			if (v != null) {
				actual.onNext(v);
			}

			if (Disposables.isDisposed(future)) {
				return;
			}
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
