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

package reactor.util;

import java.io.PrintStream;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.regex.Matcher;

import reactor.util.annotation.Nullable;


/**
 * Expose static methods to get a logger depending on the environment. If SL4J is on the
 * classpath, it will be used. Otherwise, there are two possible fallbacks: Console or
 * {@link java.util.logging.Logger java.util.logging.Logger}). By default, the Console
 * fallback is used. To use the JDK loggers, set the {@value #FALLBACK_PROPERTY}
 * {@link System#setProperty(String, String) System property} to "{@code JDK}".
 * <p>
 * One can also force the implementation by using the "useXXX" static methods:
 * {@link #useConsoleLoggers()}, {@link #useVerboseConsoleLoggers()}, {@link #useJdkLoggers()}
 * and {@link #useSl4jLoggers()} (which may throw an Exception if the library isn't on the
 * classpath). Note that the system property method above is preferred, as no cleanup of
 * the logger factory initialized at startup is attempted by the useXXX methods.
 */
public abstract class Loggers {

	/**
	 * The system property that determines which fallback implementation to use for loggers
	 * when SLF4J isn't available. Use {@code JDK} for the JDK-backed logging and anything
	 * else for Console-based (the default).
	 */
	public static final String FALLBACK_PROPERTY = "reactor.logging.fallback";

	private static Function<String, ? extends Logger> LOGGER_FACTORY;

	static {
		resetLoggerFactory();
	}

	/**
	 * Attempt to activate the best {@link Logger reactor Logger} factory, by first attempting
	 * to use the SLF4J one, then falling back to either Console logging or
	 * {@link java.util.logging.Logger java.util.logging.Logger}). By default, the Console
	 * fallback is used. To fallback to the JDK loggers, set the {@value #FALLBACK_PROPERTY}
	 * {@link System#setProperty(String, String) System property} to "{@code JDK}".
	 *
	 * @see #useJdkLoggers()
	 * @see #useConsoleLoggers()
	 * @see #useVerboseConsoleLoggers()
	 */
	public static void resetLoggerFactory() {
		try {
			useSl4jLoggers();
		}
		catch (Throwable t) {
			if (isFallbackToJdk()) {
				useJdkLoggers();
			}
			else {
				useConsoleLoggers();
			}
		}
	}

	/**
	 * Return true if {@link #resetLoggerFactory()} would fallback to java.util.logging
	 * rather than console-based logging, as defined by the {@link #FALLBACK_PROPERTY}
	 * System property.
	 *
	 * @return true if falling back to JDK, false for Console.
	 */
	static boolean isFallbackToJdk() {
		return "JDK".equalsIgnoreCase(System.getProperty(FALLBACK_PROPERTY));
	}

	/**
	 * Force the usage of Console-based {@link Logger Loggers}, even if SLF4J is available
	 * on the classpath. Console loggers will output {@link Logger#error(String) ERROR} and
	 * {@link Logger#warn(String) WARN} levels to {@link System#err} and levels below to
	 * {@link System#out}. All levels <strong>except TRACE and DEBUG</strong> are
	 * considered enabled. TRACE and DEBUG are omitted.
	 * <p>
	 * The previously active logger factory is simply replaced without
	 * any particular clean-up.
	 */
	public static void useConsoleLoggers() {
		String name = Loggers.class.getName();
		Function<String, Logger> loggerFactory = new ConsoleLoggerFactory(false);
		LOGGER_FACTORY = loggerFactory;
		loggerFactory.apply(name).debug("Using Console logging");
	}

	/**
	 * Force the usage of Console-based {@link Logger Loggers}, even if SLF4J is available
	 * on the classpath. Console loggers will output {@link Logger#error(String) ERROR} and
	 * {@link Logger#warn(String) WARN} levels to {@link System#err} and levels below to
	 * {@link System#out}. All levels (including TRACE and DEBUG) are considered enabled.
	 * <p>
	 * The previously active logger factory is simply replaced without
	 * any particular clean-up.
	 */
	public static void useVerboseConsoleLoggers() {
		String name = Loggers.class.getName();
		Function<String, Logger> loggerFactory = new ConsoleLoggerFactory(true);
		LOGGER_FACTORY = loggerFactory;
		loggerFactory.apply(name).debug("Using Verbose Console logging");
	}

	/**
	 * Use a custom type of {@link Logger} created through the provided {@link Function},
	 * which takes a logger name as input.
	 * <p>
	 * The previously active logger factory is simply replaced without
	 * any particular clean-up.
	 *
	 * <h4>Thread-safety</h4>
	 *
	 * Given logger acquisition function <em>must</em> be thread-safe.
	 * It means that it is user responsibility to ensure that any internal state and cache
	 * used by the provided function is properly synchronized.
	 *
	 * @param loggerFactory the {@link Function} that provides a (possibly cached) {@link Logger}
	 * given a name.
	 */
	public static void useCustomLoggers(final Function<String, ? extends Logger> loggerFactory) {
		String name = Loggers.class.getName();
		LOGGER_FACTORY = loggerFactory;
		loggerFactory.apply(name).debug("Using custom logging");
	}

	/**
	 * Force the usage of JDK-based {@link Logger Loggers}, even if SLF4J is available
	 * on the classpath.
	 * <p>
	 * The previously active logger factory is simply replaced without
	 * any particular clean-up.
	 */
	public static void useJdkLoggers() {
		String name = Loggers.class.getName();
		Function<String, Logger> loggerFactory = new JdkLoggerFactory();
		LOGGER_FACTORY = loggerFactory;
		loggerFactory.apply(name).debug("Using JDK logging framework");
	}

	/**
	 * Force the usage of SL4J-based {@link Logger Loggers}, throwing an exception if
	 * SLF4J isn't available on the classpath. Prefer using {@link #resetLoggerFactory()}
	 * as it will fallback in the later case.
	 * <p>
	 * The previously active logger factory is simply replaced without
	 * any particular clean-up.
	 */
	public static void useSl4jLoggers() {
		String name = Loggers.class.getName();
		Function<String, Logger> loggerFactory = new Slf4JLoggerFactory();
		LOGGER_FACTORY = loggerFactory;
		loggerFactory.apply(name).debug("Using Slf4j logging framework");
	}

	/**
	 * Get a {@link Logger}.
	 * <p>
	 * For a notion of how the backing implementation is chosen, see
	 * {@link #resetLoggerFactory()} (or call one of the {@link #useConsoleLoggers() useXxxLoggers}
	 * methods).
	 *
	 * @param name the category or logger name to use
	 *
	 * @return a new {@link Logger} instance
	 */
	public static Logger getLogger(String name) {
		return LOGGER_FACTORY.apply(name);
	}

	/**
	 * Get a {@link Logger}, backed by SLF4J if present on the classpath or falling back
	 * to {@link java.util.logging.Logger java.util.logging.Logger}.
	 *
	 * @param cls the source {@link Class} to derive the logger name from.
	 *
	 * @return a new {@link Logger} instance
	 */
	public static Logger getLogger(Class<?> cls) {
		return LOGGER_FACTORY.apply(cls.getName());
	}

	private static class Slf4JLoggerFactory implements Function<String, Logger> {

		@Override
		public Logger apply(String name) {
			return new Slf4JLogger(org.slf4j.LoggerFactory.getLogger(name));
		}
	}

	private static class Slf4JLogger implements Logger {

		private final org.slf4j.Logger logger;

		public Slf4JLogger(org.slf4j.Logger logger) {
			this.logger = logger;
		}

		@Override
		public String getName() {
			return logger.getName();
		}

		@Override
		public boolean isTraceEnabled() {
			return logger.isTraceEnabled();
		}

		@Override
		public void trace(String msg) {
			logger.trace(msg);
		}

		@Override
		public void trace(String format, Object... arguments) {
			logger.trace(format, arguments);
		}

		@Override
		public void trace(String msg, Throwable t) {
			logger.trace(msg, t);
		}

		@Override
		public boolean isDebugEnabled() {
			return logger.isDebugEnabled();
		}

		@Override
		public void debug(String msg) {
			logger.debug(msg);
		}

		@Override
		public void debug(String format, Object... arguments) {
			logger.debug(format, arguments);
		}

		@Override
		public void debug(String msg, Throwable t) {
			logger.debug(msg, t);
		}

		@Override
		public boolean isInfoEnabled() {
			return logger.isInfoEnabled();
		}

		@Override
		public void info(String msg) {
			logger.info(msg);
		}

		@Override
		public void info(String format, Object... arguments) {
			logger.info(format, arguments);
		}

		@Override
		public void info(String msg, Throwable t) {
			logger.info(msg, t);
		}

		@Override
		public boolean isWarnEnabled() {
			return logger.isWarnEnabled();
		}

		@Override
		public void warn(String msg) {
			logger.warn(msg);
		}

		@Override
		public void warn(String format, Object... arguments) {
			logger.warn(format, arguments);
		}

		@Override
		public void warn(String msg, Throwable t) {
			logger.warn(msg, t);
		}

		@Override
		public boolean isErrorEnabled() {
			return logger.isErrorEnabled();
		}

		@Override
		public void error(String msg) {
			logger.error(msg);
		}

		@Override
		public void error(String format, Object... arguments) {
			logger.error(format, arguments);
		}

		@Override
		public void error(String msg, Throwable t) {
			logger.error(msg, t);
		}
	}

	/**
	 * Wrapper over JDK logger
	 */
	static final class JdkLogger implements Logger {

		private final java.util.logging.Logger logger;

		public JdkLogger(java.util.logging.Logger logger) {
			this.logger = logger;
		}

		@Override
		public String getName() {
			return logger.getName();
		}

		@Override
		public boolean isTraceEnabled() {
			return logger.isLoggable(Level.FINEST);
		}

		@Override
		public void trace(String msg) {
			logger.log(Level.FINEST, msg);
		}

		@Override
		public void trace(String format, Object... arguments) {
			logger.log(Level.FINEST, format(format, arguments));
		}

		@Override
		public void trace(String msg, Throwable t) {
			logger.log(Level.FINEST, msg, t);
		}

		@Override
		public boolean isDebugEnabled() {
			return logger.isLoggable(Level.FINE);
		}

		@Override
		public void debug(String msg) {
			logger.log(Level.FINE, msg);
		}

		@Override
		public void debug(String format, Object... arguments) {
			logger.log(Level.FINE, format(format, arguments));
		}

		@Override
		public void debug(String msg, Throwable t) {
			logger.log(Level.FINE, msg, t);
		}

		@Override
		public boolean isInfoEnabled() {
			return logger.isLoggable(Level.INFO);
		}

		@Override
		public void info(String msg) {
			logger.log(Level.INFO, msg);
		}

		@Override
		public void info(String format, Object... arguments) {
			logger.log(Level.INFO, format(format, arguments));
		}

		@Override
		public void info(String msg, Throwable t) {
			logger.log(Level.INFO, msg, t);
		}

		@Override
		public boolean isWarnEnabled() {
			return logger.isLoggable(Level.WARNING);
		}

		@Override
		public void warn(String msg) {
			logger.log(Level.WARNING, msg);
		}

		@Override
		public void warn(String format, Object... arguments) {
			logger.log(Level.WARNING, format(format, arguments));
		}

		@Override
		public void warn(String msg, Throwable t) {
			logger.log(Level.WARNING, msg, t);
		}

		@Override
		public boolean isErrorEnabled() {
			return logger.isLoggable(Level.SEVERE);
		}

		@Override
		public void error(String msg) {
			logger.log(Level.SEVERE, msg);
		}

		@Override
		public void error(String format, Object... arguments) {
			logger.log(Level.SEVERE, format(format, arguments));
		}

		@Override
		public void error(String msg, Throwable t) {
			logger.log(Level.SEVERE, msg, t);
		}

		@Nullable
		final String format(@Nullable String from, @Nullable Object... arguments){
			if(from != null) {
				String computed = from;
				if (arguments != null && arguments.length != 0) {
					for (Object argument : arguments) {
						computed = computed.replaceFirst("\\{\\}", Matcher.quoteReplacement(String.valueOf(argument)));
					}
				}
				return computed;
			}
			return null;
		}
	}

	private static class JdkLoggerFactory implements Function<String, Logger> {

		@Override
		public Logger apply(String name) {
			return new JdkLogger(java.util.logging.Logger.getLogger(name));
		}
	}

	/**
	 * A {@link Logger} that has all levels enabled. error and warn log to System.err
	 * while all other levels log to System.out (printstreams can be changed via constructor).
	 */
	static final class ConsoleLogger implements Logger {

		private final ConsoleLoggerKey identifier;
		private final PrintStream err;
		private final PrintStream log;

		ConsoleLogger(ConsoleLoggerKey identifier, PrintStream log, PrintStream err) {
			this.identifier = identifier;
			this.log = log;
			this.err = err;
		}

		ConsoleLogger(String name, PrintStream log, PrintStream err, boolean verbose) {
			this(new ConsoleLoggerKey(name, verbose), log, err);
		}

		ConsoleLogger(ConsoleLoggerKey identifier) {
			this(identifier, System.out, System.err);
		}

		@Override
		public String getName() {
			return identifier.name;
		}

		@Nullable
		final String format(@Nullable String from, @Nullable Object... arguments){
			if(from != null) {
				String computed = from;
				if (arguments != null && arguments.length != 0) {
					for (Object argument : arguments) {
						computed = computed.replaceFirst("\\{\\}", Matcher.quoteReplacement(String.valueOf(argument)));
					}
				}
				return computed;
			}
			return null;
		}

		@Override
		public boolean isTraceEnabled() {
			return identifier.verbose;
		}

		@Override
		public synchronized void trace(String msg) {
			if (!identifier.verbose) {
				return;
			}
			this.log.format("[TRACE] (%s) %s\n", Thread.currentThread().getName(), msg);
		}

		@Override
		public synchronized void trace(String format, Object... arguments) {
			if (!identifier.verbose) {
				return;
			}
			this.log.format("[TRACE] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
		}
		@Override
		public synchronized void trace(String msg, Throwable t) {
			if (!identifier.verbose) {
				return;
			}
			this.log.format("[TRACE] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
			t.printStackTrace(this.log);
		}

		@Override
		public boolean isDebugEnabled() {
			return identifier.verbose;
		}

		@Override
		public synchronized void debug(String msg) {
			if (!identifier.verbose) {
				return;
			}
			this.log.format("[DEBUG] (%s) %s\n", Thread.currentThread().getName(), msg);
		}

		@Override
		public synchronized void debug(String format, Object... arguments) {
			if (!identifier.verbose) {
				return;
			}
			this.log.format("[DEBUG] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
		}

		@Override
		public synchronized void debug(String msg, Throwable t) {
			if (!identifier.verbose) {
				return;
			}
			this.log.format("[DEBUG] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
			t.printStackTrace(this.log);
		}

		@Override
		public boolean isInfoEnabled() {
			return true;
		}

		@Override
		public synchronized void info(String msg) {
			this.log.format("[ INFO] (%s) %s\n", Thread.currentThread().getName(), msg);
		}

		@Override
		public synchronized void info(String format, Object... arguments) {
			this.log.format("[ INFO] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
		}

		@Override
		public synchronized void info(String msg, Throwable t) {
			this.log.format("[ INFO] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
			t.printStackTrace(this.log);
		}

		@Override
		public boolean isWarnEnabled() {
			return true;
		}

		@Override
		public synchronized void warn(String msg) {
			this.err.format("[ WARN] (%s) %s\n", Thread.currentThread().getName(), msg);
		}

		@Override
		public synchronized void warn(String format, Object... arguments) {
			this.err.format("[ WARN] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
		}

		@Override
		public synchronized void warn(String msg, Throwable t) {
			this.err.format("[ WARN] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
			t.printStackTrace(this.err);
		}

		@Override
		public boolean isErrorEnabled() {
			return true;
		}

		@Override
		public synchronized void error(String msg) {
			this.err.format("[ERROR] (%s) %s\n", Thread.currentThread().getName(), msg);
		}

		@Override
		public synchronized void error(String format, Object... arguments) {
			this.err.format("[ERROR] (%s) %s\n", Thread.currentThread().getName(), format(format, arguments));
		}

		@Override
		public synchronized void error(String msg, Throwable t) {
			this.err.format("[ERROR] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
			t.printStackTrace(this.err);
		}

		@Override
		public String toString() {
			return "ConsoleLogger[name="+getName()+", verbose="+identifier.verbose+"]";
		}
	}

	/**
	 * A key object to serve a dual purpose:
	 * <ul>
	 *     <li>Allow consistent identification of cached console loggers using not
	 *     only its name, but also its verbosity level</li>
	 *     <li>Provide an object eligible to cache eviction. Contrary to a logger or
	 *     a string (logger name) object, this is a good candidate for weak reference key,
	 *     because it should be held only internally by the attached logger and by the
	 *     logger cache (as evictable key).</li>
	 * </ul>
	 */
	private static final class ConsoleLoggerKey {

		private final String name;
		private final boolean verbose;

		private ConsoleLoggerKey(String name, boolean verbose) {
			this.name = name;
			this.verbose = verbose;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ConsoleLoggerKey key = (ConsoleLoggerKey) o;
			return verbose == key.verbose && Objects.equals(name, key.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, verbose);
		}
	}

	static final class ConsoleLoggerFactory implements Function<String, Logger> {

		private static final Map<ConsoleLoggerKey, WeakReference<Logger>> consoleLoggers =
				new WeakHashMap<>();

		final boolean verbose;

		ConsoleLoggerFactory(boolean verbose) {
			this.verbose = verbose;
		}

		@Override
		public Logger apply(String name) {
			final ConsoleLoggerKey key = new ConsoleLoggerKey(name, verbose);
			synchronized (consoleLoggers) {
				final WeakReference<Logger> ref = consoleLoggers.get(key);
				Logger cached = ref == null ? null : ref.get();
				if (cached == null) {
					cached = new ConsoleLogger(key);
					consoleLoggers.put(key, new WeakReference<>(cached));
				}

				return cached;
			}
		}
	}

	Loggers(){}
}
