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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import javax.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Publisher indicating a scalar/empty source that subscribes on the specified scheduler.
 * 
 * @param <T>
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class FluxSubscribeOnValue<T> extends Flux<T> implements Fuseable {

	final T value;
	
	final Scheduler scheduler;

	FluxSubscribeOnValue(@Nullable T value,
			Scheduler scheduler) {
		this.value = value;
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context context) {
		T v = value;
		if (v == null) {
			ScheduledEmpty parent = new ScheduledEmpty(s);
			s.onSubscribe(parent);
			Disposable f = scheduler.schedule(parent);
			if (f == Scheduler.REJECTED) {
				if (parent.future != Disposables.DISPOSED) {
					s.onError(Operators.onRejectedExecution());
				}
			}
			else {
				parent.setFuture(f);
			}
		} else {
			s.onSubscribe(new ScheduledScalar<>(s, v, scheduler));
		}
	}

	static final class ScheduledScalar<T>
			implements QueueSubscription<T>, InnerProducer<T>, Runnable {

		final Subscriber<? super T> actual;

		final T value;

		final Scheduler scheduler;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ScheduledScalar> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(ScheduledScalar.class, "once");

		volatile Disposable future;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ScheduledScalar, Disposable> FUTURE =
				AtomicReferenceFieldUpdater.newUpdater(ScheduledScalar.class,
						Disposable.class,
						"future");

		static final Disposable FINISHED = () -> {
		};

		int fusionState;

		static final int NO_VALUE  = 1;
		static final int HAS_VALUE = 2;
		static final int COMPLETE  = 3;

		ScheduledScalar(Subscriber<? super T> actual,
				T value,
				Scheduler scheduler) {
			this.actual = actual;
			this.value = value;
			this.scheduler = scheduler;
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Scannable.Attr key) {
			if (key == BooleanAttr.CANCELLED) return future == Disposables.DISPOSED;
			if (key == BooleanAttr.TERMINATED) return future == FINISHED;
			if (key == IntAttr.BUFFERED) return 1;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (ONCE.compareAndSet(this, 0, 1)) {
					Disposable f = scheduler.schedule(this);
					if(f == Scheduler.REJECTED && future != FINISHED && future !=
							Disposables.DISPOSED) {
						actual.onError(Operators.onRejectedExecution(this, null, null));
					}
					else if (!FUTURE.compareAndSet(this, null, f)) {
						if (future != FINISHED && future != Disposables.DISPOSED) {
							f.dispose();
						}
					}
				}
			}
		}

		@Override
		public void cancel() {
			ONCE.lazySet(this, 1);
			Disposable f = future;
			if (f != Disposables.DISPOSED && future != FINISHED) {
				f = FUTURE.getAndSet(this, Disposables.DISPOSED);
				if (f != null && f != Disposables.DISPOSED && f != FINISHED) {
					f.dispose();
				}
			}
		}

		@Override
		public void run() {
			try {
				if (fusionState == NO_VALUE) {
					fusionState = HAS_VALUE;
				}
				actual.onNext(value);
				actual.onComplete();
			}
			finally {
				FUTURE.lazySet(this, FINISHED);
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.ASYNC) != 0) {
				fusionState = NO_VALUE;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}

		@Override
		@Nullable
		public T poll() {
			if (fusionState == HAS_VALUE) {
				fusionState = COMPLETE;
				return value;
			}
			return null;
		}

		@Override
		public boolean isEmpty() {
			return fusionState != HAS_VALUE;
		}

		@Override
		public int size() {
			return isEmpty() ? 0 : 1;
		}

		@Override
		public void clear() {
			fusionState = COMPLETE;
		}
	}

	static final class ScheduledEmpty
			implements QueueSubscription<Void>, Runnable {

		final Subscriber<?> actual;

		volatile Disposable future;
		static final AtomicReferenceFieldUpdater<ScheduledEmpty, Disposable> FUTURE =
				AtomicReferenceFieldUpdater.newUpdater(ScheduledEmpty.class,
						Disposable.class,
						"future");

		static final Disposable FINISHED = () -> {
		};

		ScheduledEmpty(Subscriber<?> actual) {
			this.actual = actual;
		}

		@Override
		public void request(long n) {
			Operators.validate(n);
		}

		@Override
		public void cancel() {
			Disposable f = future;
			if (f != Disposables.DISPOSED && f != FINISHED) {
				f = FUTURE.getAndSet(this, Disposables.DISPOSED);
				if (f != null && f != Disposables.DISPOSED && f != FINISHED) {
					f.dispose();
				}
			}
		}

		@Override
		public void run() {
			try {
				actual.onComplete();
			}
			finally {
				FUTURE.lazySet(this, FINISHED);
			}
		}

		void setFuture(Disposable f) {
			if (!FUTURE.compareAndSet(this, null, f)) {
				Disposable a = future;
				if (a != FINISHED && a != Disposables.DISPOSED) {
					f.dispose();
				}
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			return requestedMode & Fuseable.ASYNC;
		}

		@Override
		@Nullable
		public Void poll() {
			return null;
		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public void clear() {
			// nothing to do
		}
	}

}
