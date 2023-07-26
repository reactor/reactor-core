/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

/**
 * Emits the first value emitted by a given source publisher, delayed by some time amount
 * with the help of a ScheduledExecutorService instance or a generic function callback that
 * wraps other form of async-delayed execution of tasks.
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @author Simon Baslé
 */
final class MonoDelayElement<T> extends InternalMonoOperator<T, T> {

	final Scheduler timedScheduler;

	final long delay;

	final TimeUnit unit;

	MonoDelayElement(Mono<? extends T> source, long delay, TimeUnit unit, Scheduler timedScheduler) {
		super(source);
		this.delay = delay;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.timedScheduler = Objects.requireNonNull(timedScheduler, "timedScheduler");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new DelayElementSubscriber<>(actual, timedScheduler, delay, unit);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return timedScheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return super.scanUnsafe(key);
	}

	static final class DelayElementSubscriber<T> implements InnerOperator<T, T>,
	                                                        Fuseable,
	                                                        Fuseable.QueueSubscription<T>,
	                                                        Runnable {

		static final Disposable CANCELLED  = Disposables.disposed();
		static final Disposable TERMINATED = Disposables.disposed();

		final CoreSubscriber<? super T> actual;
		final long delay;
		final Scheduler scheduler;
		final TimeUnit unit;

		Subscription s;
		T value;
		boolean done;

		volatile Disposable task;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DelayElementSubscriber, Disposable> TASK =
				AtomicReferenceFieldUpdater.newUpdater(DelayElementSubscriber.class, Disposable.class, "task");

		DelayElementSubscriber(
			CoreSubscriber<? super T> actual,
			Scheduler scheduler,
			long delay,
			TimeUnit unit
		) {
			this.actual = actual;
			this.scheduler = scheduler;
			this.delay = delay;
			this.unit = unit;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) {
				final Disposable task = this.task;
				return done && (task == TERMINATED || (task == null && value == null));
			}
			if (key == Attr.CANCELLED) return task == CANCELLED;
			if (key == Attr.PREFETCH) return 0;
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_ON) return scheduler;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
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
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			this.done = true;
			this.value = t;

			try {
				Disposable currentTask = this.task;
				if (currentTask == CANCELLED) {
					this.value = null;
					Operators.onDiscard(t, actual.currentContext());
					return;
				}

				final Disposable nextTask = scheduler.schedule(this, delay, unit);

				for (;;) {
					currentTask = this.task;

					if (currentTask == CANCELLED) {
						nextTask.dispose();
						Operators.onDiscard(t, actual.currentContext());
						return;
					}

					if (currentTask == TERMINATED) {
						// scheduled task completion happened before this
						// just return and do nothing
						return;
					}

					if (TASK.compareAndSet(this, null, nextTask)) {
						return;
					}
				}
			}
			catch (RejectedExecutionException ree) {
				this.value = null;
				Operators.onDiscard(t, actual.currentContext());
				actual.onError(Operators.onRejectedExecution(ree, this, null, t, actual.currentContext()));
			}
		}

		@Override
		public void run() {
			for (;;) {
				final Disposable currentTask = this.task;

				if (currentTask == CANCELLED) {
					return;
				}

				if (TASK.compareAndSet(this, currentTask, TERMINATED)) {
					break;
				}
				// we may to repeat since this may race with CAS in the onNext method
			}

			final T value = this.value;
			this.value = null;

			this.actual.onNext(value);
			this.actual.onComplete();
		}

		@Override
		public void cancel() {
			for (;;) {
				final Disposable task = this.task;
				if (task == CANCELLED || task == TERMINATED) {
					return;
				}

				if (TASK.compareAndSet(this, task, CANCELLED)) {
					if (task != null) {
						task.dispose();

						final T value = this.value;
						this.value = null;

						Operators.onDiscard(value, actual.currentContext());
						return;
					}
					break;
				}
			}

			s.cancel();
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			this.done = true;

			actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			this.done = true;

			actual.onError(t);
		}

		@Override
		public T poll() {
			return null;
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public void clear() {

		}
	}
}
