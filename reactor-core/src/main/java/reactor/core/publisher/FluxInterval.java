/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.annotation.Nullable;

/**
 * Periodically emits an ever increasing long value either via a ScheduledExecutorService
 * or a custom async callback function
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxInterval extends Flux<Long> implements SourceProducer<Long> {

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
	public void subscribe(CoreSubscriber<? super Long> actual) {
		Worker w = timedScheduler.createWorker();

		IntervalRunnable r = new IntervalRunnable(actual, w);

		actual.onSubscribe(r);

		try {
			w.schedulePeriodically(r, initialDelay, period, unit);
		}
		catch (RejectedExecutionException ree) {
			if (!r.cancelled) {
				actual.onError(Operators.onRejectedExecution(ree, r, null, null,
						actual.currentContext()));
			}
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return timedScheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return null;
	}

	static final class IntervalRunnable implements Runnable, Subscription,
	                                               InnerProducer<Long> {
		final CoreSubscriber<? super Long> actual;

		final Worker worker;

		volatile long requested;
		static final AtomicLongFieldUpdater<IntervalRunnable> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(IntervalRunnable.class, "requested");

		long count;

		volatile boolean cancelled;

		IntervalRunnable(CoreSubscriber<? super Long> actual, Worker worker) {
			this.actual = actual;
			this.worker = worker;
		}

		@Override
		public CoreSubscriber<? super Long> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.RUN_ON) return worker;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

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
					
					actual.onError(Exceptions.failWithOverflow("Could not emit tick " + count + " due to lack of requests" +
							" (interval doesn't support small downstream requests that replenish slower than the ticks)"));
				}
			}
		}
		
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
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
