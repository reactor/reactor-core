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
import reactor.core.publisher.Mono;

/**
 * Scheduler that works with a single-threaded ScheduledExecutorService and is suited for
 * same-thread work (like an event dispatch thread). This scheduler is time-capable (can
 * schedule with delay / periodically).
 */
final class SingleScheduler implements Scheduler, Supplier<ScheduledExecutorService>,
                                       Scannable, SchedulerState.DisposeAwaiter<ScheduledExecutorService> {

	static final AtomicLong COUNTER       = new AtomicLong();
	static final ScheduledExecutorService TERMINATED;

	static {
		TERMINATED = Executors.newSingleThreadScheduledExecutor();
		TERMINATED.shutdownNow();
	}

	final ThreadFactory factory;

	volatile SchedulerState<ScheduledExecutorService> state;
	@SuppressWarnings("rawtypes")
	private static final AtomicReferenceFieldUpdater<SingleScheduler, SchedulerState> STATE =
			AtomicReferenceFieldUpdater.newUpdater(
					SingleScheduler.class, SchedulerState.class, "state"
			);

	private static final SchedulerState<ScheduledExecutorService> INIT =
			SchedulerState.init(TERMINATED);

	SingleScheduler(ThreadFactory factory) {
		this.factory = factory;
		STATE.lazySet(this, INIT);
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
		SchedulerState<ScheduledExecutorService> current = state;
		return current != INIT && current.currentResource == TERMINATED;
	}

	@Override
	public void init() {
		SchedulerState<ScheduledExecutorService> a = this.state;
		if (a != INIT) {
			if (a.currentResource == TERMINATED) {
				throw new IllegalStateException(
						"Initializing a disposed scheduler is not permitted"
				);
			}
			// return early - scheduler already initialized
			return;
		}

		SchedulerState<ScheduledExecutorService> b = SchedulerState.init(
				Schedulers.decorateExecutorService(this, this.get())
		);

		if (!STATE.compareAndSet(this, INIT, b)) {
			b.currentResource.shutdownNow();
			// Currently, isDisposed() is true for non-initialized state, but that will
			// be fixed in 3.5.0. At this stage we know however that the state is no
			// longer INIT, so isDisposed() actually means disposed state.
			if (isDisposed()) {
				throw new IllegalStateException(
						"Initializing a disposed scheduler is not permitted"
				);
			}
		}
	}

	@Override
	public void start() {
		//TODO SingleTimedScheduler didn't implement start, check if any particular reason?
		SchedulerState<ScheduledExecutorService> a = this.state;
		if (a.currentResource != TERMINATED) {
			return;
		}

		SchedulerState<ScheduledExecutorService> b = SchedulerState.init(
				Schedulers.decorateExecutorService(this, this.get())
		);

		if (STATE.compareAndSet(this, a, b)) {
			return;
		}

		// someone else shutdown or started successfully, free the resource
		b.currentResource.shutdownNow();
	}

	@Override
	public boolean await(ScheduledExecutorService resource, long timeout, TimeUnit timeUnit) throws InterruptedException {
		return resource.awaitTermination(timeout, timeUnit);
	}

	@Override
	public void dispose() {
		SchedulerState<ScheduledExecutorService> previous = state;

		if (previous.currentResource == TERMINATED) {
			// In case a graceful shutdown called shutdown and is waiting, but we want to force shutdown,
			// we need access to the original ScheduledExecutorService.
			assert previous.initialResource != null;
			previous.initialResource.shutdownNow();
			return;
		}

		SchedulerState<ScheduledExecutorService> terminated =
				SchedulerState.transition(previous.currentResource, TERMINATED, this);

		STATE.compareAndSet(this, previous, terminated);

		// If unsuccessful - either another thread disposed or restarted - no issue,
		// we only care about the one stored in terminated.
		assert terminated.initialResource != null;
		terminated.initialResource.shutdownNow();
	}

	@Override
	public Mono<Void> disposeGracefully() {
		return Mono.defer(() -> {
			SchedulerState<ScheduledExecutorService> previous = state;

			if (previous.currentResource == TERMINATED) {
				return previous.onDispose;
			}

			SchedulerState<ScheduledExecutorService> terminated =
					SchedulerState.transition(previous.currentResource, TERMINATED, this);

			STATE.compareAndSet(this, previous, terminated);

			// If unsuccessful - either another thread disposed or restarted - no issue,
			// we only care about the one stored in terminated.
			assert terminated.initialResource != null;
			terminated.initialResource.shutdown();
			return terminated.onDispose;
		});
	}

	@Override
	public Disposable schedule(Runnable task) {
		ScheduledExecutorService executor = state.currentResource;
		return Schedulers.directSchedule(executor, task, null, 0L,
				TimeUnit.MILLISECONDS);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		return Schedulers.directSchedule(state.currentResource, task, null, delay, unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		return Schedulers.directSchedulePeriodically(state.currentResource,
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

		return Schedulers.scanExecutor(state.currentResource, key);
	}

	@Override
	public Worker createWorker() {
		return new ExecutorServiceWorker(state.currentResource);
	}
}
