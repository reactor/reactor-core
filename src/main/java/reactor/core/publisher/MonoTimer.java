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

import org.reactivestreams.*;

import reactor.core.flow.Cancellation;
import reactor.core.scheduler.TimedScheduler;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;

/**
 * Emits a single 0L value delayed by some time amount with a help of
 * a ScheduledExecutorService instance or a generic function callback that
 * wraps other form of async-delayed execution of tasks.
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
final class MonoTimer extends Mono<Long> {

	final TimedScheduler timedScheduler;
	
	final long delay;
	
	final TimeUnit unit;
	
	public MonoTimer(long delay, TimeUnit unit, TimedScheduler timedScheduler) {
		this.delay = delay;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.timedScheduler = Objects.requireNonNull(timedScheduler, "timedScheduler");
	}
	
	@Override
	public void subscribe(Subscriber<? super Long> s) {
		TimerRunnable r = new TimerRunnable(s);
		
		s.onSubscribe(r);
		
		r.setCancel(timedScheduler.schedule(r, delay, unit));
	}

	@Override
	public long getPeriod() {
		return delay;
	}

	static final class TimerRunnable implements Runnable, Subscription {
		final Subscriber<? super Long> s;
		
		volatile Cancellation cancel;
		static final AtomicReferenceFieldUpdater<TimerRunnable, Cancellation> CANCEL =
				AtomicReferenceFieldUpdater.newUpdater(TimerRunnable.class, Cancellation.class, "cancel");
		
		volatile boolean requested;
		
		static final Cancellation CANCELLED = () -> { };

		public TimerRunnable(Subscriber<? super Long> s) {
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
					if (cancel != CANCELLED) {
						s.onNext(0L);
					}
					if (cancel != CANCELLED) {
						s.onComplete();
					}
				}
				catch (Throwable t){
					Exceptions.throwIfFatal(t);
					s.onError(t);
				}
			} else {
				s.onError(new IllegalStateException("Could not emit value due to lack of requests"));
			}
		}
		
		@Override
		public void cancel() {
			Cancellation c = cancel;
			if (c != CANCELLED) {
				c =  CANCEL.getAndSet(this, CANCELLED);
				if (c != null && c != CANCELLED) {
					c.dispose();
				}
			}
		}
		
		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				requested = true;
			}
		}
	}
}
