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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.scheduler.TimedScheduler;
import reactor.core.scheduler.TimedScheduler.TimedWorker;

/**
 * Periodically emits an ever increasing long value either via a ScheduledExecutorService
 * or a custom async callback function
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxInterval extends Flux<Long> {

	final TimedScheduler timedScheduler;
	
	final long initialDelay;
	
	final long period;
	
	final TimeUnit unit;

	public FluxInterval(
			long initialDelay, 
			long period, 
			TimeUnit unit, 
			TimedScheduler timedScheduler) {
		if (period < 0L) {
			throw new IllegalArgumentException("period >= 0 required but it was " + period);
		}
		this.initialDelay = initialDelay;
		this.period = period;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.timedScheduler = Objects.requireNonNull(timedScheduler, "timedScheduler");
	}
	
	@Override
	public void subscribe(Subscriber<? super Long> s) {
		
		TimedWorker w = timedScheduler.createWorker();

		IntervalRunnable r = new IntervalRunnable(s, w);

		s.onSubscribe(r);

		w.schedulePeriodically(r, initialDelay, period, unit);
	}

	static final class IntervalRunnable implements Runnable, Subscription {
		final Subscriber<? super Long> s;
		
		final TimedWorker worker;
		
		volatile long requested;
		static final AtomicLongFieldUpdater<IntervalRunnable> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(IntervalRunnable.class, "requested");
		
		long count;
		
		volatile boolean cancelled;

		public IntervalRunnable(Subscriber<? super Long> s, TimedWorker worker) {
			this.s = s;
			this.worker = worker;
		}
		
		@Override
		public void run() {
			if (!cancelled) {
				if (requested != 0L) {
					s.onNext(count++);
					if (requested != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}
				} else {
					cancel();
					
					s.onError(new IllegalStateException("Could not emit value " + count + " due to lack of requests"));
				}
			}
		}
		
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
			}
		}
		
		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				worker.shutdown();
			}
		}
	}
}
