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

package reactor.core.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import reactor.core.Disposable;

/**
 * Scheduler that works with a single-threaded ScheduledExecutorService and is suited for
 * same-thread work (like an event dispatch thread). This scheduler is time-capable (can
 * schedule with delay / periodically).
 */
final class SingleScheduler implements Scheduler, Supplier<ScheduledExecutorService> {

	static final AtomicLong COUNTER       = new AtomicLong();

	final ThreadFactory factory;

	volatile ScheduledExecutorService executor;
	static final AtomicReferenceFieldUpdater<SingleScheduler, ScheduledExecutorService> EXECUTORS =
			AtomicReferenceFieldUpdater.newUpdater(SingleScheduler.class,
					ScheduledExecutorService.class,
					"executor");

	static final ScheduledExecutorService TERMINATED;

	static {
		TERMINATED = Executors.newSingleThreadScheduledExecutor();
		TERMINATED.shutdownNow();
	}

	SingleScheduler(ThreadFactory factory) {
		this.factory = factory;
		init();
	}

	/**
	 * Instantiates the default {@link ScheduledExecutorService} for the SingleScheduler
	 * ({@code Executors.newSingleThreadExecutor}).
	 */
	@Override
	public ScheduledExecutorService get() {
		ScheduledExecutorService e = Executors.newScheduledThreadPool(1, this.factory);
		((ScheduledThreadPoolExecutor) e).setRemoveOnCancelPolicy(true);
		return e;
	}

	private void init() {
		EXECUTORS.lazySet(this,
				Schedulers.decorateExecutorService(Schedulers.SINGLE, this));
	}

	@Override
	public boolean isDisposed() {
		return executor == TERMINATED;
	}

	@Override
	public void start() {
		//TODO SingleTimedScheduler didn't implement start, check if any particular reason?
		ScheduledExecutorService b = null;
		for (; ; ) {
			ScheduledExecutorService a = executor;
			if (a != TERMINATED) {
				if (b != null) {
					b.shutdownNow();
				}
				return;
			}

			if (b == null) {
				b = Schedulers.decorateExecutorService(Schedulers.SINGLE, this);
			}

			if (EXECUTORS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	@Override
	public void dispose() {
		ScheduledExecutorService a = executor;
		if (a != TERMINATED) {
			a = EXECUTORS.getAndSet(this, TERMINATED);
			if (a != TERMINATED) {
				a.shutdownNow();
			}
		}
	}

	@Override
	public Disposable schedule(Runnable task) {
		return Schedulers.directSchedule(executor, task, 0L, TimeUnit.MILLISECONDS);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		return Schedulers.directSchedule(executor, task, delay, unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		return Schedulers.directSchedulePeriodically(executor,
				task,
				initialDelay,
				period,
				unit);
	}

	@Override
	public Worker createWorker() {
		return new ExecutorServiceWorker(executor);
	}

}
