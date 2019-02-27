/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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

import org.jetbrains.annotations.NotNull;

class HookableScheduledExecutorService implements ScheduledExecutorService {

	final ScheduledExecutorService delegate;

	protected HookableScheduledExecutorService(ScheduledExecutorService delegate) {
		this.delegate = delegate;
	}

	protected Runnable wrap(Runnable runnable) {
		return Hooks.onScheduleHook.apply(runnable);
	}

	protected <V> Runnable wrap(Callable<V> callable) {
		// We assume that every Callable in Reactor implements Runnable
		return Hooks.onScheduleHook.apply((Runnable) callable);
	}

	@NotNull
	@Override
	public ScheduledFuture<?> schedule(@NotNull Runnable command,
			long delay,
			@NotNull TimeUnit unit) {
		return delegate.schedule(wrap(command), delay, unit);
	}

	@NotNull
	@Override
	public <V> ScheduledFuture<V> schedule(@NotNull Callable<V> callable,
			long delay,
			@NotNull TimeUnit unit) {
		return (ScheduledFuture<V>) delegate.schedule(wrap(callable), delay, unit);
	}

	@NotNull
	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command,
			long initialDelay,
			long period,
			@NotNull TimeUnit unit) {
		return delegate.scheduleAtFixedRate(wrap(command), initialDelay, period, unit);
	}

	@NotNull
	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(@NotNull Runnable command,
			long initialDelay,
			long delay,
			@NotNull TimeUnit unit) {
		return delegate.scheduleWithFixedDelay(wrap(command), initialDelay, delay, unit);
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
		return (Future<T>) delegate.submit(wrap(task));
	}

	@NotNull
	@Override
	public <T> Future<T> submit(@NotNull Runnable task, T result) {
		return delegate.submit(wrap(task), result);
	}

	@NotNull
	@Override
	public Future<?> submit(@NotNull Runnable task) {
		return delegate.submit(wrap(task));
	}

	@Override
	public void execute(@NotNull Runnable command) {
		delegate.execute(wrap(command));
	}

	@NotNull
	@Override
	public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks)
			throws InterruptedException {
		throw new UnsupportedOperationException("invokeAll is not implemented");
	}

	@NotNull
	@Override
	public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks,
			long timeout,
			@NotNull TimeUnit unit) throws InterruptedException {
		throw new UnsupportedOperationException("invokeAll is not implemented");
	}

	@NotNull
	@Override
	public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks)
			throws InterruptedException, ExecutionException {
		throw new UnsupportedOperationException("invokeAny is not implemented");
	}

	@Override
	public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks,
			long timeout,
			@NotNull TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		throw new UnsupportedOperationException("invokeAny is not implemented");
	}
}