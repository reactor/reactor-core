/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.TimedScheduler;

/**
 * Emits a single 0L value delayed by some time amount with a help of
 * a ScheduledExecutorService instance or a generic function callback that
 * wraps other form of async-delayed execution of tasks.
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoDelay extends Mono<Long> {

	final TimedScheduler timedScheduler;

	final long delay;

	final TimeUnit unit;

	MonoDelay(long delay, TimeUnit unit, TimedScheduler timedScheduler) {
		this.delay = delay;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.timedScheduler = Objects.requireNonNull(timedScheduler, "timedScheduler");
	}

	@Override
	public void subscribe(Subscriber<? super Long> s) {
		MonoDelayRunnable r = new MonoDelayRunnable(s);

		s.onSubscribe(r);

		Cancellation f = timedScheduler.schedule(r, delay, unit);
		if (f == Scheduler.REJECTED) {
			if(r.cancel != Flux.CANCELLED &&
					r.cancel != MonoDelayRunnable.FINISHED) {
				s.onError(Operators.onRejectedExecution(r, null, null));
			}
		}
		else {
			r.setCancel(f);
		}
	}

	static final class MonoDelayRunnable implements Runnable, Subscription {
		final Subscriber<? super Long> s;

		volatile Cancellation cancel;
		static final AtomicReferenceFieldUpdater<MonoDelayRunnable, Cancellation> CANCEL =
				AtomicReferenceFieldUpdater.newUpdater(MonoDelayRunnable.class,
						Cancellation.class,
						"cancel");

		volatile boolean requested;

		static final Disposable FINISHED = () -> { };

		public MonoDelayRunnable(Subscriber<? super Long> s) {
			this.s = s;
		}

		public void setCancel(Cancellation cancel) {
			if (!CANCEL.compareAndSet(this, null, cancel)) {
				cancel.dispose();
			}
		}

		@Override
		public void run() {
			if (requested) {
				try {
				if (CANCEL.getAndSet(this, FINISHED) != Flux.CANCELLED) {
					s.onNext(0L);
				}
				if (cancel != Flux.CANCELLED) {
					s.onComplete();
				}
				}
				catch (Throwable t){
					s.onError(Operators.onOperatorError(t));
				}
			} else {
				s.onError(Exceptions.failWithOverflow("Could not emit value due to lack of requests"));
			}
		}

		@Override
		public void cancel() {
			Cancellation c = cancel;
			if (c != Flux.CANCELLED && c != FINISHED) {
				c =  CANCEL.getAndSet(this, Flux.CANCELLED);
				if (c != null && c != Flux.CANCELLED && c != FINISHED) {
					c.dispose();
				}
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				requested = true;
			}
		}
	}
}
