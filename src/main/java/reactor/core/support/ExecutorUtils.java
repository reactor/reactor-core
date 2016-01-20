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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Stephane Maldini
 */
public enum ExecutorUtils {
	;

	/**
	 * @param name
	 *
	 * @return
	 */
	public static ThreadFactory newNamedFactory(String name) {
		return ExecutorUtils.newNamedFactory(name);
	}

	/**
	 * @param name
	 * @param cl
	 *
	 * @return
	 */
	public static ThreadFactory newNamedFactory(String name, ClassLoader cl) {
		return ExecutorUtils.newNamedFactory(name, cl);
	}

	/**
	 * @param name
	 * @param cl
	 * @param uncaughtExceptionHandler
	 * @param daemon
	 *
	 * @return
	 */
	public static ThreadFactory newNamedFactory(String name,
			ClassLoader cl,
			Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
			boolean daemon) {
		return ExecutorUtils.newNamedFactory(name, cl, uncaughtExceptionHandler, daemon);
	}

	/**
	 * @param executor
	 */
	public static void shutdownIfSingleUse(ExecutorService executor) {
		if (executor.getClass() == ExecutorUtils.SingleUseExecutor.class) {
			executor.shutdown();
		}
	}

	/**
	 * @param name
	 *
	 * @return
	 */
	public static ExecutorService singleUse(String name) {
		return singleUse(name, null);
	}

	/**
	 * @param name
	 * @param contextClassLoader
	 *
	 * @return
	 */
	public static ExecutorService singleUse(String name, ClassLoader contextClassLoader) {
		return new SingleUseExecutor(Executors.newCachedThreadPool(ExecutorUtils.newNamedFactory(name,
				contextClassLoader,
				null,
				false)));
	}

	/**
	 * @param delegate
	 *
	 * @return
	 */
	public static ExecutorService wrapSingleUse(ExecutorService delegate) {
		return new SingleUseExecutor(delegate);
	}

	/**
	 */
	static final class SingleUseExecutor implements ExecutorService {

		final private ExecutorService delegate;

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
		public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
				throws InterruptedException {
			return delegate.invokeAll(tasks, timeout, unit);
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
				throws InterruptedException, ExecutionException {
			return delegate.invokeAny(tasks);
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			return delegate.invokeAny(tasks, timeout, unit);
		}

		@Override
		public void execute(Runnable command) {
			delegate.execute(command);
		}
	}

	/**
	 * A thread factory that creates named daemon threads. Each thread created by this class will have a different name
	 * due to a count of the number of threads  created thus far being included in the name of each thread. This count
	 * is held statically to ensure different thread names across different instances of this class.
	 *
	 * @author Stephane Maldini
	 * @see Thread#setDaemon(boolean)
	 * @see Thread#setName(String)
	 */
	final static class NamedDaemonThreadFactory extends AtomicInteger implements ThreadFactory {

		private final String                          prefix;
		private final boolean                         daemon;
		private final ClassLoader                     contextClassLoader;
		private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

		/**
		 * Creates a new thread factory that will name its threads &lt;prefix&gt;-&lt;n&gt;, where &lt;prefix&gt; is the
		 * given {@code prefix} and &lt;n&gt; is the count of threads created thus far by this class.
		 *
		 * @param prefix The thread name prefix
		 */
		NamedDaemonThreadFactory(String prefix) {
			this(prefix,
					new ClassLoader(Thread.currentThread()
					                      .getContextClassLoader()) {
					});
		}

		/**
		 * Creates a new thread factory that will name its threads &lt;prefix&gt;-&lt;n&gt;, where &lt;prefix&gt; is the
		 * given {@code prefix} and &lt;n&gt; is the count of threads created thus far by this class. If the
		 * contextClassLoader parameter is not null it will assign it to the forged Thread
		 *
		 * @param prefix The thread name prefix
		 * @param contextClassLoader An optional classLoader to assign to thread
		 */
		NamedDaemonThreadFactory(String prefix, ClassLoader contextClassLoader) {
			this(prefix, contextClassLoader, null, true);
		}

		NamedDaemonThreadFactory(String prefix,
				ClassLoader contextClassLoader,
				Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
				boolean daemon) {
			this.prefix = prefix;
			this.daemon = daemon;
			this.contextClassLoader = contextClassLoader;
			this.uncaughtExceptionHandler = uncaughtExceptionHandler;
			set(0);
		}

		@Override
		public Thread newThread(Runnable runnable) {
			Thread t = new Thread(runnable);
			t.setName(prefix + "-" + incrementAndGet());
			t.setDaemon(daemon);
			if (contextClassLoader != null) {
				t.setContextClassLoader(contextClassLoader);
			}
			if (null != uncaughtExceptionHandler) {
				t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
			}
			return t;
		}

	}
}
