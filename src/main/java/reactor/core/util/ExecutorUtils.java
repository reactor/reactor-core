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

package reactor.core.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Stephane Maldini
 */
public enum ExecutorUtils {
	;

	/**
	 * Create a named {@link ThreadFactory} that will set the created threads as daemon with an incrementing derived
	 * name.
	 *
	 * @param name The prefix given to created threads
	 *
	 * @return a new {@link ThreadFactory}
	 */
	public static ThreadFactory newNamedFactory(String name) {
		return new NamedDaemonThreadFactory(name);
	}

	/**
	 * @param name The prefix given to created threads
	 * @param cl An arbitrary classloader to assign to the created threads
	 *
	 * @return a new {@link ThreadFactory}
	 */
	public static ThreadFactory newNamedFactory(String name, ClassLoader cl) {
		return new NamedDaemonThreadFactory(name, cl);
	}

	/**
	 * @param name The prefix given to created threads
	 * @param cl An arbitrary classloader to assign to the created threads
	 * @param uncaughtExceptionHandler an uncaught exception fatal callback
	 * @param daemon manually set if the created threads should prevent JVM shutdown (non-daemon) or not (daemon).
	 *
	 * @return a new {@link ThreadFactory}
	 */
	public static ThreadFactory newNamedFactory(String name,
			ClassLoader cl,
			Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
			boolean daemon) {
		return new NamedDaemonThreadFactory(name, cl, uncaughtExceptionHandler, daemon);
	}

	/**
	 * A thread factory that creates named daemon threads. Each thread created by this class will have a different name
	 * due to a count of the number of threads  created thus far being included in the name of each thread. This count
	 * is held statically to ensure different thread names across different instances of this class.
	 *
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
