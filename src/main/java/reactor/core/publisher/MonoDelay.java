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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;


/**
 * Emits a single 0L value delayed by some time amount with a help of
 * a ScheduledExecutorService instance or a generic function callback that
 * wraps other form of async-delayed execution of tasks.
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoDelay extends Mono<Long> {

	final Scheduler timedScheduler;

	final long delay;

	final TimeUnit unit;

	MonoDelay(long delay, TimeUnit unit, Scheduler timedScheduler) {
		this.delay = delay;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.timedScheduler = Objects.requireNonNull(timedScheduler, "timedScheduler");
	}

	@Override
	public void subscribe(Subscriber<? super Long> s) {
		MonoDelayRunnable r = new MonoDelayRunnable(s);

		s.onSubscribe(r);

		Disposable f = timedScheduler.schedule(r, delay, unit);
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

	static final class MonoDelayRunnable implements Runnable, InnerProducer<Long> {
		final Subscriber<? super Long> actual;

		volatile Disposable cancel;
		static final AtomicReferenceFieldUpdater<MonoDelayRunnable, Disposable> CANCEL =
				AtomicReferenceFieldUpdater.newUpdater(MonoDelayRunnable.class,
						Disposable.class,
						"cancel");

		volatile boolean requested;

		static final Disposable FINISHED = () -> { };

		MonoDelayRunnable(Subscriber<? super Long> actual) {
			this.actual = actual;
		}

		public void setCancel(Disposable cancel) {
			if (!CANCEL.compareAndSet(this, null, cancel)) {
				cancel.dispose();
			}
		}

		@Override
		public Subscriber<? super Long> actual() {
			return actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.TERMINATED) return cancel == FINISHED;
			if (key == BooleanAttr.CANCELLED) return cancel == Flux.CANCELLED;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public void run() {
			if (requested) {
				try {
					if (CANCEL.getAndSet(this, FINISHED) != Flux.CANCELLED) {
						actual.onNext(0L);
						actual.onComplete();
					}
				}
				catch (Throwable t){
					actual.onError(Operators.onOperatorError(t));
				}
			} else {
				actual.onError(Exceptions.failWithOverflow("Could not emit value due to lack of requests"));
			}
		}

		@Override
		public void cancel() {
			Disposable c = cancel;
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
