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
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

/**
 * Emits the first value emitted by a given source publisher, delayed by some time amount
 * with the help of a ScheduledExecutorService instance or a generic function callback that
 * wraps other form of async-delayed execution of tasks.
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @author Simon Baslé
 * TODO : Review impl
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

		return super.scanUnsafe(key);
	}

	static final class DelayElementSubscriber<T> extends Operators.MonoSubscriber<T,T> {

		final long delay;
		final Scheduler scheduler;
		final TimeUnit unit;

		Subscription s;

		volatile Disposable task;
		boolean done;

		DelayElementSubscriber(CoreSubscriber<? super T> actual, Scheduler scheduler,
				long delay, TimeUnit unit) {
			super(actual);
			this.scheduler = scheduler;
			this.delay = delay;
			this.unit = unit;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_ON) return scheduler;

			return super.scanUnsafe(key);
		}

		@Override
		public void cancel() {
			super.cancel();
			if (task != null) {
				task.dispose();
			}
			if (s != Operators.cancelledSubscription()) {
				s.cancel();
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			this.done = true;
			try {
				this.task = scheduler.schedule(() -> complete(t), delay, unit);
			}
			catch (RejectedExecutionException ree) {
				throw Operators.onRejectedExecution(ree, this, null, t,
						actual.currentContext());
			}
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
	}
}
