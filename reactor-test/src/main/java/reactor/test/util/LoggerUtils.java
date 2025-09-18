/*
 * Copyright (c) 2020-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.test.util;

import java.lang.reflect.Field;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import reactor.core.Disposable;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * This class eases testing interested in what reactor classes emit using {@link Logger loggers}.
 *
 * @author Eric Bottard
 */
public final class LoggerUtils {

	static @Nullable CapturingFactory currentCapturingFactory;

	private LoggerUtils() {
	}

	/**
	 * Sets a {@link Loggers#useCustomLoggers(Function) logger factory} that will return loggers that not only use the
	 * original logging framework used by reactor, but also use the logger set via {@link #enableCaptureWith(Logger)}, irrespective
	 * of its name or how it was obtained. The expectation here is that tests that want to assess that something is
	 * logged by reactor will pass a {@link TestLogger} instance to {@link #enableCaptureWith(Logger)}, trigger the operation
	 * under scrutiny, assert the logger contents and reset state by calling {@link #disableCapture()}.
	 *
	 * <p>This method should be called very early in the application/tests lifecycle, before reactor classes have created
	 * their loggers.</p>
	 *
	 * @return a disposable that re-installs the original factory when disposed
	 */
	public static Disposable useCurrentLoggersWithCapture() throws IllegalStateException {
		try {
			Field lfField = Loggers.class.getDeclaredField("LOGGER_FACTORY");
			lfField.setAccessible(true);
			Object originalFactoryInstance = lfField.get(Loggers.class);
			if (originalFactoryInstance instanceof CapturingFactory) {
				return (Disposable) originalFactoryInstance;
			}
			@SuppressWarnings("unchecked")
			final Function<String, ? extends Logger> originalFactory =
				(Function<String, ? extends Logger>) originalFactoryInstance;
			CapturingFactory capturingFactory = new CapturingFactory(originalFactory);
			currentCapturingFactory = capturingFactory;
			Loggers.useCustomLoggers(capturingFactory);
			return capturingFactory;
		}
		catch (NoSuchFieldException | IllegalAccessException e) {
			throw new IllegalStateException("Could not install custom logger", e);
		}
	}

	/**
	 * Set the logger used for capturing.
	 *
	 * @param testLogger the {@link Logger} in which to copy logs
	 * @throws IllegalStateException if no capturing factory is installed or a previous logger has been set but not
	 * cleared via {@link #disableCapture()}
	 */
	public static void enableCaptureWith(Logger testLogger) {
		CapturingFactory f = currentCapturingFactory;
		if (f == null) {
			throw new IllegalStateException("LoggerUtils#useCurrentLoggerWithCapture() hasn't been called");
		}
		f.enableRedirection(testLogger, true); //throws ISE also
	}

	/**
	 * Set the logger used for capturing, an optionally suppress log messages from original logger.
	 *
	 * @param testLogger the {@link TestLogger} in which to copy or redirect logs
	 * @param redirectToOriginal whether log messages should also go to the original logging infrastructure
	 * @throws IllegalStateException if no capturing factory is installed or a previous logger has been set but not
	 * cleared via {@link #disableCapture()}
	 */
	public static void enableCaptureWith(Logger testLogger, boolean redirectToOriginal) {
		CapturingFactory f = currentCapturingFactory;
		if (f == null) {
			throw new IllegalStateException("LoggerUtils#useCurrentLoggerWithCapture() hasn't been called");
		}
		f.enableRedirection(testLogger, redirectToOriginal); //throws ISE also
	}

	/**
	 * Disable capturing, forgetting about the logger set via {@link #enableCaptureWith(Logger)}.
	 */
	public static void disableCapture() {
		CapturingFactory f = currentCapturingFactory;
		if (f == null) {
			throw new IllegalStateException("LoggerUtils#useCurrentLoggerWithCapture() hasn't been called");
		}
		f.disableRedirection();
	}


	static final class CapturingFactory implements Function<String, Logger>, Disposable {

		final Function<String, ? extends Logger> originalFactory;

		@Nullable Logger capturingLogger;

		boolean redirectToOriginal;

		CapturingFactory(Function<String, ? extends Logger> originalFactory) {
			this.originalFactory = originalFactory;
			disableRedirection();
		}

		void disableRedirection() {
			this.redirectToOriginal = true;
			this.capturingLogger = null;
		}

		void enableRedirection(Logger captureLogger, boolean redirectToOriginal) {
			if (this.capturingLogger != null) {
				throw new IllegalStateException("A logger was already set, maybe from a previous run. Don't forget to call disableCapture()");
			}
			this.redirectToOriginal = redirectToOriginal;
			this.capturingLogger = captureLogger;
		}

		@Nullable Logger getCapturingLogger() {
			return this.capturingLogger;
		}

		boolean isRedirectToOriginal() {
			return this.redirectToOriginal;
		}

		@Override
		public Logger apply(String category) {
			Logger original = originalFactory.apply(category);
			return new DivertingLogger(original, this);
		}

		@Override
		public void dispose() {
			if (LoggerUtils.currentCapturingFactory == this) {
				LoggerUtils.currentCapturingFactory = null;
			}
			try {
				Field lfField = Loggers.class.getDeclaredField("LOGGER_FACTORY");
				lfField.setAccessible(true);
				Object o = lfField.get(Loggers.class);

				if (!(Loggers.getLogger(LoggerUtils.class) instanceof DivertingLogger)) {
					throw new IllegalStateException("Expected the current factory to be " + this + ", found " + o + " instead");
				}
				lfField.set(Loggers.class, originalFactory);
			}
			catch (NoSuchFieldException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * A Logger that behaves like its {@link #delegate} but also logs to its parent {@link CapturingFactory}
	 * {@link CapturingFactory#getCapturingLogger() capturing logger} if it is set.
	 */
	static class DivertingLogger implements reactor.util.Logger {

		private final reactor.util.Logger delegate;
		private final CapturingFactory parent;

		DivertingLogger(Logger delegate, CapturingFactory parent) {
			this.delegate = delegate;
			this.parent = parent;
		}

		@Override
		public String getName() {
			return delegate.getName();
		}

		@Override
		public boolean isTraceEnabled() {
			Logger logger = parent.getCapturingLogger();
			return delegate.isTraceEnabled() || (logger != null && logger.isTraceEnabled());
		}

		@Override
		public void trace(String msg) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.trace(msg);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.trace(msg);
			}
		}

		@Override
		public void trace(String format, Object... arguments) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.trace(format, arguments);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.trace(format, arguments);
			}
		}

		@Override
		public void trace(String msg, Throwable t) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.trace(msg, t);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.trace(msg, t);
			}
		}

		@Override
		public boolean isDebugEnabled() {
			Logger logger = parent.getCapturingLogger();
			return delegate.isDebugEnabled() || (logger != null && logger.isDebugEnabled());
		}

		@Override
		public void debug(String msg) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.debug(msg);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.debug(msg);
			}
		}

		@Override
		public void debug(String format, Object... arguments) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.debug(format, arguments);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.debug(format, arguments);
			}
		}

		@Override
		public void debug(String msg, Throwable t) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.debug(msg, t);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.debug(msg, t);
			}
		}

		@Override
		public boolean isInfoEnabled() {
			Logger logger = parent.getCapturingLogger();
			return delegate.isInfoEnabled() || (logger != null && logger.isInfoEnabled());
		}

		@Override
		public void info(String msg) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.info(msg);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.info(msg);
			}
		}

		@Override
		public void info(String format, Object... arguments) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.info(format, arguments);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.info(format, arguments);
			}
		}

		@Override
		public void info(String msg, Throwable t) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.info(msg, t);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.info(msg, t);
			}
		}

		@Override
		public boolean isWarnEnabled() {
			Logger logger = parent.getCapturingLogger();
			return delegate.isWarnEnabled() || (logger != null && logger.isWarnEnabled());
		}

		@Override
		public void warn(String msg) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.warn(msg);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.warn(msg);
			}
		}

		@Override
		public void warn(String format, Object... arguments) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.warn(format, arguments);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.warn(format, arguments);
			}
		}

		@Override
		public void warn(String msg, Throwable t) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.warn(msg, t);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.warn(msg, t);
			}
		}

		@Override
		public boolean isErrorEnabled() {
			Logger logger = parent.getCapturingLogger();
			return delegate.isErrorEnabled() || (logger != null && logger.isErrorEnabled());
		}

		@Override
		public void error(String msg) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.error(msg);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.error(msg);
			}
		}

		@Override
		public void error(String format, Object... arguments) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.error(format, arguments);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.error(format, arguments);
			}
		}

		@Override
		public void error(String msg, Throwable t) {
			Logger logger = parent.getCapturingLogger();
			if (logger != null) {
				logger.error(msg, t);
			}
			if (parent.isRedirectToOriginal()) {
				delegate.error(msg, t);
			}
		}
	}
}
