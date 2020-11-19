package reactor.test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Function;

import reactor.test.util.TestLogger;
import reactor.util.Logger;
import reactor.util.Loggers;

public class LoggerUtils {
	private static Logger testLogger;

	public static void installAdditionalLogger(Logger testLogger) {
		LoggerUtils.testLogger = testLogger;
	}

	public static void resetAdditionalLogger() {
		LoggerUtils.testLogger = null;
	}

	/**
	 * Sets a {@link Loggers#useCustomLoggers(Function) logger factory} that will return loggers that not only use the
	 * original logging framework used by reactor, but also use the logger set via {@link #installAdditionalLogger(Logger)}, irrespective
	 * of its name or how it was obtained. The expectation here is that tests that want to assess that something is
	 * logged by reactor will pass a {@link TestLogger} instance to {@link #installAdditionalLogger(Logger)}, trigger the operation
	 * under scrutiny, assert the logger contents and reset state by calling {@link #resetAdditionalLogger()}.
	 */
	public static void setupDivertingLoggerFactory() {
		try {
			Field lfField = Loggers.class.getDeclaredField("LOGGER_FACTORY");
			lfField.setAccessible(true);
			Object factoryObject = lfField.get(Loggers.class);
			Method factoryMethod = factoryObject.getClass().getMethod("getLogger", String.class);
			factoryMethod.setAccessible(true);

			Loggers.useCustomLoggers(category -> {
				try {
					Logger original = (Logger) factoryMethod.invoke(factoryObject, category);
					return new DivertingLogger(original);
				}
				catch (IllegalAccessException | InvocationTargetException e) {
					throw new RuntimeException(e);
				}
			});
		}
		catch (NoSuchFieldException | IllegalAccessException | NoSuchMethodException e) {
			throw new RuntimeException("Could not install custom logger", e);
		}
	}

	/**
	 * A Logger that behaves like its {@link #delegate} but also logs to {@link LoggerUtils#testLogger} if it is set.
	 */
	private static class DivertingLogger implements reactor.util.Logger {

		private final reactor.util.Logger delegate;

		private DivertingLogger(Logger delegate) {
			this.delegate = delegate;
		}

		@Override
		public String getName() {
			return delegate.getName();
		}

		@Override
		public boolean isTraceEnabled() {
			return delegate.isTraceEnabled() || (LoggerUtils.testLogger != null && LoggerUtils.testLogger.isTraceEnabled());
		}

		@Override
		public void trace(String msg) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.trace(msg);
			}
			delegate.trace(msg);
		}

		@Override
		public void trace(String format, Object... arguments) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.trace(format, arguments);
			}
			delegate.trace(format, arguments);
		}

		@Override
		public void trace(String msg, Throwable t) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.trace(msg, t);
			}
			delegate.trace(msg, t);
		}

		@Override
		public boolean isDebugEnabled() {
			return delegate.isDebugEnabled() || (LoggerUtils.testLogger != null && LoggerUtils.testLogger.isDebugEnabled());
		}

		@Override
		public void debug(String msg) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.debug(msg);
			}
			delegate.debug(msg);
		}

		@Override
		public void debug(String format, Object... arguments) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.debug(format, arguments);
			}
			delegate.debug(format, arguments);
		}

		@Override
		public void debug(String msg, Throwable t) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.debug(msg, t);
			}
			delegate.debug(msg, t);
		}

		@Override
		public boolean isInfoEnabled() {
			return delegate.isInfoEnabled() || (LoggerUtils.testLogger != null && LoggerUtils.testLogger.isInfoEnabled());
		}

		@Override
		public void info(String msg) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.info(msg);
			}
			delegate.info(msg);
		}

		@Override
		public void info(String format, Object... arguments) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.info(format, arguments);
			}
			delegate.info(format, arguments);
		}

		@Override
		public void info(String msg, Throwable t) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.info(msg, t);
			}
			delegate.info(msg, t);
		}

		@Override
		public boolean isWarnEnabled() {
			return delegate.isWarnEnabled() || (LoggerUtils.testLogger != null && LoggerUtils.testLogger.isWarnEnabled());
		}

		@Override
		public void warn(String msg) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.warn(msg);
			}
			delegate.warn(msg);
		}

		@Override
		public void warn(String format, Object... arguments) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.warn(format, arguments);
			}
			delegate.warn(format, arguments);
		}

		@Override
		public void warn(String msg, Throwable t) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.warn(msg, t);
			}
			delegate.warn(msg, t);
		}

		@Override
		public boolean isErrorEnabled() {
			return delegate.isErrorEnabled() || (LoggerUtils.testLogger != null && LoggerUtils.testLogger.isErrorEnabled());
		}

		@Override
		public void error(String msg) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.error(msg);
			}
			delegate.error(msg);
		}

		@Override
		public void error(String format, Object... arguments) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.error(format, arguments);
			}
			delegate.error(format, arguments);
		}

		@Override
		public void error(String msg, Throwable t) {
			if (LoggerUtils.testLogger != null) {
				LoggerUtils.testLogger.error(msg, t);
			}
			delegate.error(msg, t);
		}
	}
}
