/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

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
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

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
	final ScheduledExecutorService executor;

	DelegateServiceScheduler(String executorName, ExecutorService executorService) {
			this.executorName = executorName;
			ScheduledExecutorService exec = convert(executorService);
			this.executor = Schedulers.decorateExecutorService(this, exec);
	}

	@Override
	public Worker createWorker() {
		return new ExecutorServiceWorker(executor);
	}

	@Override
	public Disposable schedule(Runnable task) {
		return Schedulers.directSchedule(executor, task, null, 0L, TimeUnit.MILLISECONDS);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		return Schedulers.directSchedule(executor, task, null, delay, unit);
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
	public boolean isDisposed() {
		return executor.isShutdown();
	}

	@Override
	public void dispose() {
		executor.shutdownNow();
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

		return Schedulers.scanExecutor(executor, key);
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

		@NotNull
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
		public boolean awaitTermination(long timeout, @NotNull TimeUnit unit)
				throws InterruptedException {
			return exec.awaitTermination(timeout, unit);
		}

		@NotNull
		@Override
		public <T> Future<T> submit(@NotNull Callable<T> task) {
			return exec.submit(task);
		}

		@NotNull
		@Override
		public <T> Future<T> submit(@NotNull Runnable task, T result) {
			return exec.submit(task, result);
		}

		@NotNull
		@Override
		public Future<?> submit(@NotNull Runnable task) {
			return exec.submit(task);
		}

		@NotNull
		@Override
		public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks)
				throws InterruptedException {
			return exec.invokeAll(tasks);
		}

		@NotNull
		@Override
		public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks,
				long timeout,
				@NotNull TimeUnit unit) throws InterruptedException {
			return exec.invokeAll(tasks, timeout, unit);
		}

		@NotNull
		@Override
		public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks)
				throws InterruptedException, ExecutionException {
			return exec.invokeAny(tasks);
		}

		@Override
		public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks,
				long timeout,
				@NotNull TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			return exec.invokeAny(tasks, timeout, unit);
		}

		@Override
		public void execute(@NotNull Runnable command) {
			exec.execute(command);
		}

		@NotNull
		@Override
		public ScheduledFuture<?> schedule(@NotNull Runnable command,
				long delay,
				@NotNull TimeUnit unit) {
			throw Exceptions.failWithRejectedNotTimeCapable();
		}

		@NotNull
		@Override
		public <V> ScheduledFuture<V> schedule(@NotNull Callable<V> callable,
				long delay,
				@NotNull TimeUnit unit) {
			throw Exceptions.failWithRejectedNotTimeCapable();
		}

		@NotNull
		@Override
		public ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command,
				long initialDelay,
				long period,
				@NotNull TimeUnit unit) {
			throw Exceptions.failWithRejectedNotTimeCapable();
		}

		@NotNull
		@Override
		public ScheduledFuture<?> scheduleWithFixedDelay(@NotNull Runnable command,
				long initialDelay,
				long delay,
				@NotNull TimeUnit unit) {
			throw Exceptions.failWithRejectedNotTimeCapable();
		}

		@Override
		public String toString() {
			return exec.toString();
		}
	}

}
