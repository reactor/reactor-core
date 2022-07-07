/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.scheduler;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Scheduler that works with a single-threaded ScheduledExecutorService and is suited for
 * same-thread work (like an event dispatch thread). This scheduler is time-capable (can
 * schedule with delay / periodically).
 */
final class SingleScheduler implements Scheduler, Supplier<ScheduledExecutorService>,
                                       Scannable {

	static final AtomicLong COUNTER       = new AtomicLong();

	final ThreadFactory factory;

	volatile SchedulerState state;
	private static final AtomicReferenceFieldUpdater<SingleScheduler, SchedulerState> STATE =
			AtomicReferenceFieldUpdater.newUpdater(
					SingleScheduler.class, SchedulerState.class, "state"
			);

	SingleScheduler(ThreadFactory factory) {
		this.factory = factory;
	}

	/**
	 * Instantiates the default {@link ScheduledExecutorService} for the SingleScheduler
	 * ({@code Executors.newScheduledThreadPoolExecutor} with core and max pool size of 1).
	 */
	@Override
	public ScheduledExecutorService get() {
		ScheduledThreadPoolExecutor e = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, this.factory);
		e.setRemoveOnCancelPolicy(true);
		e.setMaximumPoolSize(1);
		return e;
	}

	@Override
	public boolean isDisposed() {
		// we only consider disposed as actually shutdown
		SchedulerState current = STATE.get(this);
		return current != null && current.executor == SchedulerState.TERMINATED;
	}

	@Override
	public void start() {
		//TODO SingleTimedScheduler didn't implement start, check if any particular reason?
		SchedulerState b = null;
		for (; ; ) {
			SchedulerState a = STATE.get(this);
			if (a != null) {
				if (a.executor != SchedulerState.TERMINATED) {
					if (b != null) {
						b.executor.shutdownNow();
					}
					return;
				}
			}

			if (b == null) {
				b = SchedulerState.fresh(
						Schedulers.decorateExecutorService(this, this.get())
				);
			}

			if (STATE.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	@Override
	public void dispose() {
		SchedulerState a = STATE.get(this);
		if (a != null && a.executor != SchedulerState.TERMINATED) {
			a = STATE.getAndUpdate(this, SchedulerState::terminated);
			if (a != null && a.executor != SchedulerState.TERMINATED) {
				a.executor.shutdownNow();
			}
		}
	}

	@Override
	public Mono<Void> disposeGracefully(Duration gracePeriod) {
		return Mono.defer(() -> {
			SchedulerState current = STATE.getAndUpdate(this, previous -> {
				if (previous.executor != SchedulerState.TERMINATED) {
					return SchedulerState.terminated(previous);
				}
				return previous;
			});
			if (current == null) {
				return Mono.empty();
			} else if (current.executor == SchedulerState.TERMINATED) {
				return current.onDispose;
			}
			current.executor.shutdown();
			return current.onDispose;
		}).timeout(gracePeriod);
	}

	@Override
	public Disposable schedule(Runnable task) {
		return Schedulers.directSchedule(STATE.get(this).executor, task, null, 0L,
				TimeUnit.MILLISECONDS);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		return Schedulers.directSchedule(STATE.get(this).executor, task, null, delay, unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		return Schedulers.directSchedulePeriodically(STATE.get(this).executor,
				task,
				initialDelay,
				period,
				unit);
	}

	@Override
	public String toString() {
		StringBuilder ts = new StringBuilder(Schedulers.SINGLE)
				.append('(');
		if (factory instanceof ReactorThreadFactory) {
			ts.append('\"').append(((ReactorThreadFactory) factory).get()).append('\"');
		}
		return ts.append(')').toString();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
		if (key == Attr.NAME) return this.toString();
		if (key == Attr.CAPACITY || key == Attr.BUFFERED) return 1; //BUFFERED: number of workers doesn't vary

		return Schedulers.scanExecutor(STATE.get(this).executor, key);
	}

	@Override
	public Worker createWorker() {
		return new ExecutorServiceWorker(STATE.get(this).executor);
	}

	static final class SchedulerState {

		static final ScheduledExecutorService TERMINATED;

		static {
			TERMINATED = Executors.newSingleThreadScheduledExecutor();
			TERMINATED.shutdownNow();
		}

		final ScheduledExecutorService executor;
		final Mono<Void>               onDispose;

		private SchedulerState(ScheduledExecutorService executor, Mono<Void> onDispose) {
			this.executor = executor;
			this.onDispose = onDispose;
		}

		static SchedulerState fresh(final ScheduledExecutorService executor) {
			return new SchedulerState(
					executor,
					Flux.<Void>create(sink -> {
						Thread backgroundThread = new Thread(() -> {
							while (!Thread.currentThread().isInterrupted()) {
								try {
									if (executor.awaitTermination(1, TimeUnit.SECONDS)) {
										sink.complete();
										return;
									}
								} catch (InterruptedException e) {
									Thread.currentThread().interrupt();
									return;
								}
							}
						});
						sink.onCancel(backgroundThread::interrupt);
						backgroundThread.start();
					}).publish().refCount().next()
			);
		}

		static SchedulerState terminated(SchedulerState base) {
			return new SchedulerState(TERMINATED, base.onDispose);
		}
	}
}
