/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.support;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Stephane Maldini
 */
public final class SingleUseExecutor implements ExecutorService {

	final private ExecutorService delegate;

	public static ExecutorService create(String name) {
		return create(name, null);
	}

	public static ExecutorService create(String name, ClassLoader contextClassLoader) {
		return new SingleUseExecutor(Executors.newCachedThreadPool(
		  new NamedDaemonThreadFactory(name, contextClassLoader, null, false)));
	}

	public static ExecutorService wrap(ExecutorService delegate) {
		return new SingleUseExecutor(delegate);
	}

	private SingleUseExecutor(ExecutorService delegate) {
		this.delegate = delegate;
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
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return delegate.awaitTermination(timeout, unit);
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return delegate.submit(task);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return delegate.submit(task, result);
	}

	@Override
	public Future<?> submit(Runnable task) {
		return delegate.submit(task);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return delegate.invokeAll(tasks);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws
	  InterruptedException {
		return delegate.invokeAll(tasks, timeout, unit);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return delegate.invokeAny(tasks);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws
	  InterruptedException, ExecutionException, TimeoutException {
		return delegate.invokeAny(tasks, timeout, unit);
	}

	@Override
	public void execute(Runnable command) {
		delegate.execute(command);
	}
}
