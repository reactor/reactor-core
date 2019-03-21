/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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

	@Override
	public ScheduledFuture<?> schedule(Runnable command,
			long delay,
			TimeUnit unit) {
		return delegate.schedule(wrap(command), delay, unit);
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable,
			long delay,
			TimeUnit unit) {
		return (ScheduledFuture<V>) delegate.schedule(wrap(callable), delay, unit); //FIXME
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
			long initialDelay,
			long period,
			TimeUnit unit) {
		return delegate.scheduleAtFixedRate(wrap(command), initialDelay, period, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
			long initialDelay,
			long delay,
			TimeUnit unit) {
		return delegate.scheduleWithFixedDelay(wrap(command), initialDelay, delay, unit);
	}

	@Override
	public void shutdown() {
		delegate.shutdown();
	}

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
	public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		return delegate.awaitTermination(timeout, unit);
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return (Future<T>) delegate.submit(wrap(task)); //FIXME
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return delegate.submit(wrap(task), result);
	}

	@Override
	public Future<?> submit(Runnable task) {
		return delegate.submit(wrap(task));
	}

	@Override
	public void execute(Runnable command) {
		delegate.execute(wrap(command));
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
		throw new UnsupportedOperationException("invokeAll is not implemented");
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
			long timeout,
			TimeUnit unit) {
		throw new UnsupportedOperationException("invokeAll is not implemented");
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
		throw new UnsupportedOperationException("invokeAny is not implemented");
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
			long timeout,
			TimeUnit unit) {
		throw new UnsupportedOperationException("invokeAny is not implemented");
	}
}