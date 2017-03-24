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
import reactor.core.Cancellation;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;


/**
 * Publisher indicating a scalar/empty source that subscribes on the specified scheduler.
 * 
 * @param <T>
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class FluxSubscribeOnValue<T> extends Flux<T> implements Fuseable {

	final T value;
	
	final Scheduler scheduler;

	FluxSubscribeOnValue(T value,
			Scheduler scheduler) {
		this.value = value;
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		T v = value;
		if (v == null) {
			ScheduledEmpty parent = new ScheduledEmpty(s);
			s.onSubscribe(parent);
			Cancellation f = scheduler.schedule(parent);
			if (f == Scheduler.REJECTED) {
				if (parent.future != Flux.CANCELLED) {
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

		volatile Cancellation future;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ScheduledScalar, Cancellation> FUTURE =
				AtomicReferenceFieldUpdater.newUpdater(ScheduledScalar.class,
						Cancellation.class,
						"future");

		static final Cancellation FINISHED = () -> {
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
		public Object scan(Scannable.Attr key) {
			switch (key){
				case CANCELLED:
					return future == CANCELLED;
				case TERMINATED:
					return future == FINISHED;
				case BUFFERED:
					return value != null ? 1 : 0;
			}
			return InnerProducer.super.scan(key);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (ONCE.compareAndSet(this, 0, 1)) {
					Cancellation f = scheduler.schedule(this);
					if(f == Scheduler.REJECTED && future != FINISHED && future !=
							CANCELLED) {
						actual.onError(Operators.onRejectedExecution(this, null, null));
					}
					else if (!FUTURE.compareAndSet(this, null, f)) {
						if (future != FINISHED && future != CANCELLED) {
							f.dispose();
						}
					}
				}
			}
		}

		@Override
		public void cancel() {
			ONCE.lazySet(this, 1);
			Cancellation f = future;
			if (f != CANCELLED && future != FINISHED) {
				f = FUTURE.getAndSet(this, CANCELLED);
				if (f != null && f != CANCELLED && f != FINISHED) {
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

		volatile Cancellation future;
		static final AtomicReferenceFieldUpdater<ScheduledEmpty, Cancellation> FUTURE =
				AtomicReferenceFieldUpdater.newUpdater(ScheduledEmpty.class,
						Cancellation.class,
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
			Cancellation f = future;
			if (f != CANCELLED && f != FINISHED) {
				f = FUTURE.getAndSet(this, CANCELLED);
				if (f != null && f != CANCELLED && f != FINISHED) {
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

		void setFuture(Cancellation f) {
			if (!FUTURE.compareAndSet(this, null, f)) {
				Cancellation a = future;
				if (a != FINISHED && a != CANCELLED) {
					f.dispose();
				}
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			return requestedMode & Fuseable.ASYNC;
		}

		@Override
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
