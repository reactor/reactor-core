/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

abstract class TaskWrappingScheduledExecutorService
		implements ScheduledExecutorService {

	final ScheduledExecutorService delegate;

	protected TaskWrappingScheduledExecutorService(ScheduledExecutorService delegate) {
		this.delegate = delegate;
	}

	protected abstract Runnable wrap(Runnable runnable);

	protected abstract <V> Callable<V> wrap(Callable<V> callable);

	@NotNull
	@Override
	public ScheduledFuture<?> schedule(@NotNull Runnable command,
			long delay,
			@NotNull TimeUnit unit) {
		command = wrap(command);
		return delegate.schedule(command, delay, unit);
	}

	@NotNull
	@Override
	public <V> ScheduledFuture<V> schedule(@NotNull Callable<V> callable,
			long delay,
			@NotNull TimeUnit unit) {
		callable = wrap(callable);
		return delegate.schedule(callable, delay, unit);
	}

	@NotNull
	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command,
			long initialDelay,
			long period,
			@NotNull TimeUnit unit) {
		command = wrap(command);
		return delegate.scheduleAtFixedRate(command, initialDelay, period, unit);
	}

	@NotNull
	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(@NotNull Runnable command,
			long initialDelay,
			long delay,
			@NotNull TimeUnit unit) {
		command = wrap(command);
		return delegate.scheduleWithFixedDelay(command, initialDelay, delay, unit);
	}

	@Override
	public void shutdown() {
		delegate.shutdown();
	}

	@NotNull
	@Override
	public List<Runnable> shutdownNow() {
		return delegate.shutdownNow();
	}

	@Override
	public boolean isShutdown() {
		return delegate.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return delegate.isTerminated();
	}

	@Override
	public boolean awaitTermination(long timeout, @NotNull TimeUnit unit)
			throws InterruptedException {
		return delegate.awaitTermination(timeout, unit);
	}

	@NotNull
	@Override
	public <T> Future<T> submit(@NotNull Callable<T> task) {
		task = wrap(task);
		return delegate.submit(task);
	}

	@NotNull
	@Override
	public <T> Future<T> submit(@NotNull Runnable task, T result) {
		task = wrap(task);
		return delegate.submit(task, result);
	}

	@NotNull
	@Override
	public Future<?> submit(@NotNull Runnable task) {
		task = wrap(task);
		return delegate.submit(task);
	}

	@NotNull
	@Override
	public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks)
			throws InterruptedException {
		tasks = tasks.stream().map(this::wrap).collect(Collectors.toList());
		return delegate.invokeAll(tasks);
	}

	@NotNull
	@Override
	public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks,
			long timeout,
			@NotNull TimeUnit unit) throws InterruptedException {
		tasks = tasks.stream()
		             .map(this::wrap)
		             .collect(Collectors.toList());
		return delegate.invokeAll(tasks, timeout, unit);
	}

	@NotNull
	@Override
	public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks)
			throws InterruptedException, ExecutionException {
		tasks = tasks.stream()
		             .map(this::wrap)
		             .collect(Collectors.toList());
		return delegate.invokeAny(tasks);
	}

	@Override
	public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks,
			long timeout,
			@NotNull TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		tasks = tasks.stream()
		             .map(this::wrap)
		             .collect(Collectors.toList());
		return delegate.invokeAny(tasks, timeout, unit);
	}

	@Override
	public void execute(@NotNull Runnable command) {
		command = wrap(command);
		delegate.execute(command);
	}
}
