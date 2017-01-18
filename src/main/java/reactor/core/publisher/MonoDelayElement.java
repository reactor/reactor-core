/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.TimedScheduler;

/**
 * Emits the first value emitted by a given source publisher, delayed by some time amount
 * with the help of a ScheduledExecutorService instance or a generic function callback that
 * wraps other form of async-delayed execution of tasks.
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @author Simon Baslé
 */
final class MonoDelayElement<T> extends MonoSource<T, T> {

	final TimedScheduler timedScheduler;

	final long delay;

	final TimeUnit unit;

	public MonoDelayElement(Publisher<? extends T> source, long delay, TimeUnit unit, TimedScheduler timedScheduler) {
		super(source);
		this.delay = delay;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.timedScheduler = Objects.requireNonNull(timedScheduler, "timedScheduler");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		MonoDelayElementSubscriber r = new MonoDelayElementSubscriber<>(s, timedScheduler, delay, unit);
		source.subscribe(r);
	}

	static final class MonoDelayElementSubscriber<T> extends Operators.MonoSubscriber<T,T>
			implements Subscription  {

		final long delay;
		final TimedScheduler scheduler;
		final TimeUnit unit;

		private Cancellation task;
		private boolean done;

		public MonoDelayElementSubscriber(Subscriber<? super T> actual, TimedScheduler scheduler,
				long delay, TimeUnit unit) {
			super(actual);
			this.scheduler = scheduler;
			this.delay = delay;
			this.unit = unit;
		}
		Subscription s;

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
			if (task != null) {
				task.dispose();
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
			s.cancel();
			//FIXME handle REJECTED
			this.task = scheduler.schedule(() -> complete(t), delay, unit);
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

		@Override
		public Object upstream() {
			return s;
		}
	}
}
