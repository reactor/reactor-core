/*
 * Copyright (c) 2017-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

import static reactor.core.scheduler.SchedulerState.TERMINATED;

/**
 * A simple {@link Scheduler} which uses a backing {@link ExecutorService} to schedule
 * Runnables for async operators. This scheduler is time-capable (can schedule with a
 * delay and/or periodically) if the backing executor is a {@link ScheduledExecutorService}.
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class DelegateServiceScheduler implements Scheduler, Scannable {

	final String executorName;
	final ScheduledExecutorService original;

	@Nullable
	volatile SchedulerState state;
	static final AtomicReferenceFieldUpdater<DelegateServiceScheduler, SchedulerState> STATE =
			AtomicReferenceFieldUpdater.newUpdater(DelegateServiceScheduler.class,
					SchedulerState.class, "state");

	DelegateServiceScheduler(String executorName, ExecutorService executorService) {
			this.executorName = executorName;
			this.original = convert(executorService);
	}

	ScheduledExecutorService getOrCreate() {
		SchedulerState s = state;
		if (s == null) {
			start();
			s = state;
			if (s == null) {
				throw new IllegalStateException("executor is null after implicit start()");
			}
		}
		return s.executor;
	}

	@Override
	public Worker createWorker() {
		return new ExecutorServiceWorker(getOrCreate());
	}

	@Override
	public Disposable schedule(Runnable task) {
		return Schedulers.directSchedule(getOrCreate(), task, null, 0L, TimeUnit.MILLISECONDS);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		return Schedulers.directSchedule(getOrCreate(), task, null, delay, unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		return Schedulers.directSchedulePeriodically(getOrCreate(),
				task,
				initialDelay,
				period,
				unit);
	}

	@Override
	public void start() {
		STATE.compareAndSet(this, null,
				SchedulerState.fresh(Schedulers.decorateExecutorService(this, original)));
	}

	@Override
	public boolean isDisposed() {
		SchedulerState current = state;
		return current != null && current.executor == SchedulerState.TERMINATED;
	}

	@Override
	public void dispose() {
		for (;;) {
			SchedulerState previous = state;

			if (previous != null && previous.executor == TERMINATED) {
				return;
			}

			if (STATE.compareAndSet(this, previous, SchedulerState.terminated(previous))) {
				if (previous != null) {
					previous.executor.shutdownNow();
				}
				return;
			}
		}
	}

	@Override
	public Mono<Void> disposeGracefully(Duration gracePeriod) {
		return Mono.defer(() -> {
			for (;;) {
				SchedulerState previous = state;

				if (previous != null && previous.executor == TERMINATED) {
					return previous.onDispose;
				}

				SchedulerState next = SchedulerState.terminated(previous);
				if (STATE.compareAndSet(this, previous, next)) {
					if (previous != null) {
						previous.executor.shutdown();
					}
					return next.onDispose;
				}
			}
		}).timeout(gracePeriod);
	}

	@SuppressWarnings("unchecked")
	static ScheduledExecutorService convert(ExecutorService executor) {
		if (executor instanceof ScheduledExecutorService) {
			return (ScheduledExecutorService) executor;
		}
		return new UnsupportedScheduledExecutorService(executor);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
		if (key == Attr.NAME) return toString();

		SchedulerState s = state;
		if (s != null) {
			return Schedulers.scanExecutor(s.executor, key);
		}
		return null;
	}

	@Override
	public String toString() {
		return Schedulers.FROM_EXECUTOR_SERVICE + '(' + executorName + ')';
	}

	static final class UnsupportedScheduledExecutorService
			implements ScheduledExecutorService, Supplier<ExecutorService> {

		final ExecutorService exec;

		UnsupportedScheduledExecutorService(ExecutorService exec) {
			this.exec = exec;
		}

		@Override
		public ExecutorService get() {
			return exec;
		}

		@Override
		public void shutdown() {
			exec.shutdown();
		}

		@NonNull
		@Override
		public List<Runnable> shutdownNow() {
			return exec.shutdownNow();
		}

		@Override
		public boolean isShutdown() {
			return exec.isShutdown();
		}

		@Override
		public boolean isTerminated() {
			return exec.isTerminated();
		}

		@Override
		public boolean awaitTermination(long timeout, @NonNull TimeUnit unit)
				throws InterruptedException {
			return exec.awaitTermination(timeout, unit);
		}

		@NonNull
		@Override
		public <T> Future<T> submit(@NonNull Callable<T> task) {
			return exec.submit(task);
		}

		@NonNull
		@Override
		public <T> Future<T> submit(@NonNull Runnable task, T result) {
			return exec.submit(task, result);
		}

		@NonNull
		@Override
		public Future<?> submit(@NonNull Runnable task) {
			return exec.submit(task);
		}

		@NonNull
		@Override
		public <T> List<Future<T>> invokeAll(@NonNull Collection<? extends Callable<T>> tasks)
				throws InterruptedException {
			return exec.invokeAll(tasks);
		}

		@NonNull
		@Override
		public <T> List<Future<T>> invokeAll(@NonNull Collection<? extends Callable<T>> tasks,
				long timeout,
				@NonNull TimeUnit unit) throws InterruptedException {
			return exec.invokeAll(tasks, timeout, unit);
		}

		@NonNull
		@Override
		public <T> T invokeAny(@NonNull Collection<? extends Callable<T>> tasks)
				throws InterruptedException, ExecutionException {
			return exec.invokeAny(tasks);
		}

		@Override
		public <T> T invokeAny(@NonNull Collection<? extends Callable<T>> tasks,
				long timeout,
				@NonNull TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			return exec.invokeAny(tasks, timeout, unit);
		}

		@Override
		public void execute(@NonNull Runnable command) {
			exec.execute(command);
		}

		@NonNull
		@Override
		public ScheduledFuture<?> schedule(@NonNull Runnable command,
				long delay,
				@NonNull TimeUnit unit) {
			throw Exceptions.failWithRejectedNotTimeCapable();
		}

		@NonNull
		@Override
		public <V> ScheduledFuture<V> schedule(@NonNull Callable<V> callable,
				long delay,
				@NonNull TimeUnit unit) {
			throw Exceptions.failWithRejectedNotTimeCapable();
		}

		@NonNull
		@Override
		public ScheduledFuture<?> scheduleAtFixedRate(@NonNull Runnable command,
				long initialDelay,
				long period,
				@NonNull TimeUnit unit) {
			throw Exceptions.failWithRejectedNotTimeCapable();
		}

		@NonNull
		@Override
		public ScheduledFuture<?> scheduleWithFixedDelay(@NonNull Runnable command,
				long initialDelay,
				long delay,
				@NonNull TimeUnit unit) {
			throw Exceptions.failWithRejectedNotTimeCapable();
		}

		@Override
		public String toString() {
			return exec.toString();
		}
	}

}
