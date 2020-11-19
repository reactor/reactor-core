package reactor.test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.pattern.ThrowableProxyConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import reactor.test.util.TestLogger;
import reactor.util.Logger;
import reactor.util.Loggers;

public class LoggerUtils {
	private static TestLogger testLogger;

	public static void addAppender(TestLogger testLogger, Class<?> classWithLogger) {
		LoggerUtils.testLogger = testLogger;
	}

	public static void resetAppender(Class<?> classWithLogger) {
		LoggerUtils.testLogger = null;
	}

	public static void installQueryableLogger() {
		Loggers.resetLoggerFactory();
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
			return delegate.isTraceEnabled();
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
			return delegate.isDebugEnabled();
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
			return delegate.isInfoEnabled();
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
			return delegate.isWarnEnabled();
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
			return delegate.isErrorEnabled();
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
