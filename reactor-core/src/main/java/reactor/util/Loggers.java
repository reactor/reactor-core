/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
package reactor.util;

import java.util.logging.Level;
import java.util.regex.Matcher;

import javax.annotation.Nullable;

/**
 * Expose static methods to get a logger depending on the environment (SLF4J or
 * {@link java.util.logging.Logger java.util.logging.Logger}).
 */
public abstract class Loggers {

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
	 * Get a {@link Logger}, backed by SLF4J if present on the classpath or falling back
	 * to {@link java.util.logging.Logger java.util.logging.Logger}.
	 *
	 * @param name the category or logger name to use
	 *
	 * @return a new {@link Logger} instance
	 */
	public static Logger getLogger(String name) {
		return LOGGER_FACTORY.getLogger(name);
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
		return LOGGER_FACTORY.getLogger(cls.getName());
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

		@Nullable
		private String format(@Nullable String from, @Nullable Object... arguments){
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

	Loggers(){}
}
