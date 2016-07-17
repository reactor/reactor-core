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
package reactor.core;

import java.util.logging.Level;
import java.util.regex.Matcher;

/**
 * Expose common Reactor runtime properties, methods and internal logger.
 */
public abstract class Reactor {

	public static final long DEFAULT_TIMEOUT =
			Long.parseLong(System.getProperty("reactor.await.defaultTimeout", "30000"));

	public static boolean TRACE_OPERATOR_STACKTRACE =
			Boolean.parseBoolean(System.getProperty("reactor.trace.operatorStacktrace", "false"));

	/**
	 * Default number of processors available to the runtime on init (min 4)
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE =
			Math.max(Runtime.getRuntime().availableProcessors(), 4);

	private final static LoggerFactory LOGGER_FACTORY;

	static {
		LoggerFactory loggerFactory;
		String name = LoggerFactory.class.getName();
		try {
			loggerFactory = new Slf4JLoggerFactory();
			loggerFactory.getLogger(name).debug("Using Slf4j logging framework");
		}
		catch (Throwable t) {
			loggerFactory = new JdkLoggerFactory();
			loggerFactory.getLogger(name).debug("Using JDK logging framework");
		}
		LOGGER_FACTORY = loggerFactory;
	}


	/**
	 * Try getting an appropriate
	 * {@link Logger} whether SLF4J is not present on the classpath or fallback to {@link java.util.logging.Logger}.
	 *
	 * @param name the category or logger name to assign
	 *
	 * @return a new {@link Logger} instance
	 */
	public static Logger getLogger(String name) {
		return LOGGER_FACTORY.getLogger(name);
	}

	/**
	 * Try getting an appropriate
	 * {@link Logger} whether SLF4J is not present on the classpath or fallback to {@link java.util.logging.Logger}.
	 *
	 * @param cls the source {@link Class} to derive the name from.
	 *
	 * @return a new {@link Logger} instance
	 */
	public static Logger getLogger(Class<?> cls) {
		return LOGGER_FACTORY.getLogger(cls.getName());
	}

	/**
	 * When enabled, producer declaration stacks are recorded via an intercepting
	 * "assembly tracker" operator and added as Suppressed
	 * Exception if the source producer fails.
	 *
	 * @return a true if assembly tracking is enabled
	 */
	public static boolean isOperatorStacktraceEnabled() {
		return TRACE_OPERATOR_STACKTRACE;
	}

	/**
	 * Enable operator stack recorder. When a producer is declared, an "assembly
	 * tracker" operator is automatically added to capture declaration
	 * stack. Errors are observed and enriched with a Suppressed
	 * Exception detailing the original stack. Must be called before producers
	 * (e.g. Flux.map, Mono.fromCallable) are actually called to intercept the right
	 * stack information.
	 */
	public static void enableOperatorStacktrace() {
		TRACE_OPERATOR_STACKTRACE = true;
	}

	/**
	 * Disable operator stack recorder.
	 */
	public static void disableOperatorStacktrace() {
		TRACE_OPERATOR_STACKTRACE = false;
	}

	/**
	 * Logger interface designed for internal Reactor usage.
	 */
	public interface Logger {

		int SUBSCRIBE     = 0b010000000;
		int ON_SUBSCRIBE  = 0b001000000;
		int ON_NEXT       = 0b000100000;
		int ON_ERROR      = 0b000010000;
		int ON_COMPLETE   = 0b000001000;
		int REQUEST       = 0b000000100;
		int CANCEL        = 0b000000010;
		int TERMINAL      = CANCEL | ON_COMPLETE | ON_ERROR;
		int ALL           = TERMINAL | REQUEST | ON_SUBSCRIBE | ON_NEXT | SUBSCRIBE;

		/**
		 * Return the name of this <code>Logger</code> instance.
		 * @return name of this logger instance
		 */
		String getName();

		/**
		 * Is the logger instance enabled for the TRACE level?
		 *
		 * @return True if this Logger is enabled for the TRACE level,
		 *         false otherwise.
		 */
		boolean isTraceEnabled();

		/**
		 * Log a message at the TRACE level.
		 *
		 * @param msg the message string to be logged
		 */
		void trace(String msg);

		/**
		 * Log a message at the TRACE level according to the specified format
		 * and arguments.
		 * <p/>
		 * <p>This form avoids superfluous string concatenation when the logger
		 * is disabled for the TRACE level. However, this variant incurs the hidden
		 * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
		 * even if this logger is disabled for TRACE.</p>
		 *
		 * @param format    the format string
		 * @param arguments a list of 3 or more arguments
		 */
		void trace(String format, Object... arguments);

		/**
		 * Log an exception (throwable) at the TRACE level with an
		 * accompanying message.
		 *
		 * @param msg the message accompanying the exception
		 * @param t   the exception (throwable) to log
		 */
		void trace(String msg, Throwable t);

		/**
		 * Is the logger instance enabled for the DEBUG level?
		 *
		 * @return True if this Logger is enabled for the DEBUG level,
		 *         false otherwise.
		 */
		boolean isDebugEnabled();

		/**
		 * Log a message at the DEBUG level.
		 *
		 * @param msg the message string to be logged
		 */
		void debug(String msg);

		/**
		 * Log a message at the DEBUG level according to the specified format
		 * and arguments.
		 * <p/>
		 * <p>This form avoids superfluous string concatenation when the logger
		 * is disabled for the DEBUG level. However, this variant incurs the hidden
		 * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
		 * even if this logger is disabled for DEBUG. </p>
		 *
		 * @param format    the format string
		 * @param arguments a list of 3 or more arguments
		 */
		void debug(String format, Object... arguments);

		/**
		 * Log an exception (throwable) at the DEBUG level with an
		 * accompanying message.
		 *
		 * @param msg the message accompanying the exception
		 * @param t   the exception (throwable) to log
		 */
		void debug(String msg, Throwable t);

		/**
		 * Is the logger instance enabled for the INFO level?
		 *
		 * @return True if this Logger is enabled for the INFO level,
		 *         false otherwise.
		 */
		boolean isInfoEnabled();

		/**
		 * Log a message at the INFO level.
		 *
		 * @param msg the message string to be logged
		 */
		void info(String msg);

		/**
		 * Log a message at the INFO level according to the specified format
		 * and arguments.
		 * <p/>
		 * <p>This form avoids superfluous string concatenation when the logger
		 * is disabled for the INFO level. However, this variant incurs the hidden
		 * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
		 * even if this logger is disabled for INFO. </p>
		 *
		 * @param format    the format string
		 * @param arguments a list of 3 or more arguments
		 */
		void info(String format, Object... arguments);

		/**
		 * Log an exception (throwable) at the INFO level with an
		 * accompanying message.
		 *
		 * @param msg the message accompanying the exception
		 * @param t   the exception (throwable) to log
		 */
		void info(String msg, Throwable t);

		/**
		 * Is the logger instance enabled for the WARN level?
		 *
		 * @return True if this Logger is enabled for the WARN level,
		 *         false otherwise.
		 */
		boolean isWarnEnabled();

		/**
		 * Log a message at the WARN level.
		 *
		 * @param msg the message string to be logged
		 */
		void warn(String msg);

		/**
		 * Log a message at the WARN level according to the specified format
		 * and arguments.
		 * <p/>
		 * <p>This form avoids superfluous string concatenation when the logger
		 * is disabled for the WARN level. However, this variant incurs the hidden
		 * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
		 * even if this logger is disabled for WARN. </p>
		 *
		 * @param format    the format string
		 * @param arguments a list of 3 or more arguments
		 */
		void warn(String format, Object... arguments);

		/**
		 * Log an exception (throwable) at the WARN level with an
		 * accompanying message.
		 *
		 * @param msg the message accompanying the exception
		 * @param t   the exception (throwable) to log
		 */
		void warn(String msg, Throwable t);

		/**
		 * Is the logger instance enabled for the ERROR level?
		 *
		 * @return True if this Logger is enabled for the ERROR level,
		 *         false otherwise.
		 */
		boolean isErrorEnabled();

		/**
		 * Log a message at the ERROR level.
		 *
		 * @param msg the message string to be logged
		 */
		void error(String msg);

		/**
		 * Log a message at the ERROR level according to the specified format
		 * and arguments.
		 * <p/>
		 * <p>This form avoids superfluous string concatenation when the logger
		 * is disabled for the ERROR level. However, this variant incurs the hidden
		 * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
		 * even if this logger is disabled for ERROR. </p>
		 *
		 * @param format    the format string
		 * @param arguments a list of 3 or more arguments
		 */
		void error(String format, Object... arguments);

		/**
		 * Log an exception (throwable) at the ERROR level with an
		 * accompanying message.
		 *
		 * @param msg the message accompanying the exception
		 * @param t   the exception (throwable) to log
		 */
		void error(String msg, Throwable t);

	}

	private interface LoggerFactory {
		Logger getLogger(String name);
	}

	private static class Slf4JLoggerFactory implements LoggerFactory {

		@Override
		public Logger getLogger(String name) {
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
	private static class JdkLogger implements Logger {

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

		private String format(String from, Object... arguments){
			if(from != null) {
				String computed = from;
				if (arguments != null && arguments.length != 0) {
					for (Object argument : arguments) {
						computed = computed.replaceFirst("\\{\\}", Matcher.quoteReplacement(argument.toString()));
					}
				}
				return computed;
			}
			return null;
		}
	}

	private static class JdkLoggerFactory implements LoggerFactory {
		@Override
		public Logger getLogger(String name) {
			return new JdkLogger(java.util.logging.Logger.getLogger(name));
		}
	}

	Reactor(){}
}
