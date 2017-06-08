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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.context.Context;

import javax.annotation.Nullable;

/**
 * Periodically emits an ever increasing long value either via a ScheduledExecutorService
 * or a custom async callback function
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxInterval extends Flux<Long> {

	final Scheduler timedScheduler;
	
	final long initialDelay;
	
	final long period;
	
	final TimeUnit unit;

	FluxInterval(
			long initialDelay, 
			long period, 
			TimeUnit unit, 
			Scheduler timedScheduler) {
		if (period < 0L) {
			throw new IllegalArgumentException("period >= 0 required but it was " + period);
		}
		this.initialDelay = initialDelay;
		this.period = period;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.timedScheduler = Objects.requireNonNull(timedScheduler, "timedScheduler");
	}
	
	@Override
	public void subscribe(Subscriber<? super Long> s, Context context) {
		Worker w = timedScheduler.createWorker();

		IntervalRunnable r = new IntervalRunnable(s, w);

		s.onSubscribe(r);

		w.schedulePeriodically(r, initialDelay, period, unit);
	}

	static final class IntervalRunnable implements Runnable, Subscription,
	                                               InnerProducer<Long> {
		final Subscriber<? super Long> actual;
		
		final Worker worker;
		
		volatile long requested;
		static final AtomicLongFieldUpdater<IntervalRunnable> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(IntervalRunnable.class, "requested");
		
		long count;
		
		volatile boolean cancelled;

		IntervalRunnable(Subscriber<? super Long> actual, Worker worker) {
			this.actual = actual;
			this.worker = worker;
		}

		@Override
		public Subscriber<? super Long> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.CANCELLED) return cancelled;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public void run() {
			if (!cancelled) {
				if (requested != 0L) {
					actual.onNext(count++);
					if (requested != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}
				} else {
					cancel();
					
					actual.onError(Exceptions.failWithOverflow("Could not emit value " + count + " due to lack of requests"));
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
				worker.dispose();
			}
		}
	}
}
