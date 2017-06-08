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
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import javax.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Emits the first value emitted by a given source publisher, delayed by some time amount
 * with the help of a ScheduledExecutorService instance or a generic function callback that
 * wraps other form of async-delayed execution of tasks.
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @author Simon Baslé
 * TODO : Review impl
 */
final class MonoDelayElement<T> extends MonoOperator<T, T> {

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
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		source.subscribe(new DelayElementSubscriber<>(s, timedScheduler, delay, unit),
				ctx);
	}

	static final class DelayElementSubscriber<T> extends Operators.MonoSubscriber<T,T> {

		final long delay;
		final Scheduler scheduler;
		final TimeUnit unit;

		Subscription s;

		volatile Disposable task;
		volatile boolean done;

		DelayElementSubscriber(Subscriber<? super T> actual, Scheduler scheduler,
				long delay, TimeUnit unit) {
			super(actual);
			this.scheduler = scheduler;
			this.delay = delay;
			this.unit = unit;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == ScannableAttr.PARENT) return s;

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
				s.request(1);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			this.done = true;
			Disposable task = scheduler.schedule(() -> complete(t), delay, unit);
			if (task == Scheduler.REJECTED) {
					throw Operators.onRejectedExecution(this, null, t);
			}
			else {
				this.task = task;
				Subscription actualS = s;
				s = Operators.cancelledSubscription();
				actualS.cancel();
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
				Operators.onErrorDropped(t);
				return;
			}
			this.done = true;
			actual.onError(t);
		}
	}
}
