package reactor.test.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Function;

import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * This class eases testing interested in what reactor classes emit using {@link Logger loggers}.
 *
 * @author Eric Bottard
 */
public class LoggerUtils {
	private static Logger testLogger;

	/**
	 * Sets a {@link Loggers#useCustomLoggers(Function) logger factory} that will return loggers that not only use the
	 * original logging framework used by reactor, but also use the logger set via {@link #installAdditionalLogger(Logger)}, irrespective
	 * of its name or how it was obtained. The expectation here is that tests that want to assess that something is
	 * logged by reactor will pass a {@link TestLogger} instance to {@link #installAdditionalLogger(Logger)}, trigger the operation
	 * under scrutiny, assert the logger contents and reset state by calling {@link #resetAdditionalLogger()}.
	 *
	 * <p>This method should be called very early in the application/tests lifecycle, before reactor classes have created
	 * their loggers.</p>
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

	public static void installAdditionalLogger(Logger testLogger) {
		if (LoggerUtils.testLogger != null) {
			throw new IllegalStateException("A logger was already set, maybe from a previous run. Don't forget to call resetAdditionalLogger()");
		}
		LoggerUtils.testLogger = testLogger;
	}

	public static void resetAdditionalLogger() {
		LoggerUtils.testLogger = null;
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
			Logger logger = LoggerUtils.testLogger;
			return delegate.isTraceEnabled() || (logger != null && logger.isTraceEnabled());
		}

		@Override
		public void trace(String msg) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.trace(msg);
			}
			delegate.trace(msg);
		}

		@Override
		public void trace(String format, Object... arguments) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.trace(format, arguments);
			}
			delegate.trace(format, arguments);
		}

		@Override
		public void trace(String msg, Throwable t) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.trace(msg, t);
			}
			delegate.trace(msg, t);
		}

		@Override
		public boolean isDebugEnabled() {
			Logger logger = LoggerUtils.testLogger;
			return delegate.isDebugEnabled() || (logger != null && logger.isDebugEnabled());
		}

		@Override
		public void debug(String msg) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.debug(msg);
			}
			delegate.debug(msg);
		}

		@Override
		public void debug(String format, Object... arguments) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.debug(format, arguments);
			}
			delegate.debug(format, arguments);
		}

		@Override
		public void debug(String msg, Throwable t) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.debug(msg, t);
			}
			delegate.debug(msg, t);
		}

		@Override
		public boolean isInfoEnabled() {
			Logger logger = LoggerUtils.testLogger;
			return delegate.isInfoEnabled() || (logger != null && logger.isInfoEnabled());
		}

		@Override
		public void info(String msg) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.info(msg);
			}
			delegate.info(msg);
		}

		@Override
		public void info(String format, Object... arguments) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.info(format, arguments);
			}
			delegate.info(format, arguments);
		}

		@Override
		public void info(String msg, Throwable t) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.info(msg, t);
			}
			delegate.info(msg, t);
		}

		@Override
		public boolean isWarnEnabled() {
			Logger logger = LoggerUtils.testLogger;
			return delegate.isWarnEnabled() || (logger != null && logger.isWarnEnabled());
		}

		@Override
		public void warn(String msg) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.warn(msg);
			}
			delegate.warn(msg);
		}

		@Override
		public void warn(String format, Object... arguments) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.warn(format, arguments);
			}
			delegate.warn(format, arguments);
		}

		@Override
		public void warn(String msg, Throwable t) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.warn(msg, t);
			}
			delegate.warn(msg, t);
		}

		@Override
		public boolean isErrorEnabled() {
			Logger logger = LoggerUtils.testLogger;
			return delegate.isErrorEnabled() || (logger != null && logger.isErrorEnabled());
		}

		@Override
		public void error(String msg) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.error(msg);
			}
			delegate.error(msg);
		}

		@Override
		public void error(String format, Object... arguments) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.error(format, arguments);
			}
			delegate.error(format, arguments);
		}

		@Override
		public void error(String msg, Throwable t) {
			Logger logger = LoggerUtils.testLogger;
			if (logger != null) {
				logger.error(msg, t);
			}
			delegate.error(msg, t);
		}
	}
}
