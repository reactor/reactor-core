package reactor.test.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Function;

import reactor.core.Disposable;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

/**
 * This class eases testing interested in what reactor classes emit using {@link Logger loggers}.
 *
 * @author Eric Bottard
 */
public final class LoggerUtils {

	@Nullable
	private static Logger testLogger;

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
			Object originalFactory = lfField.get(Loggers.class);
			if (originalFactory instanceof CapturingFactory) {
				return (Disposable) originalFactory;
			}
			Method originalFactoryMethod = originalFactory.getClass().getMethod("apply", String.class);
			originalFactoryMethod.setAccessible(true);

			CapturingFactory capturingFactory = new CapturingFactory(originalFactory, originalFactoryMethod);
			Loggers.useCustomLoggers(capturingFactory);
			return capturingFactory;
		}
		catch (NoSuchFieldException | IllegalAccessException | NoSuchMethodException e) {
			throw new IllegalStateException("Could not install custom logger", e);
		}
	}

	/**
	 * Set the logger used for capturing.
	 *
	 * @throws IllegalStateException if a previous logger has been set but not cleared via {@link #disableCapture()}
	 */
	public static void enableCaptureWith(Logger testLogger) {
		if (LoggerUtils.testLogger != null) {
			throw new IllegalStateException("A logger was already set, maybe from a previous run. Don't forget to call disableCapture()");
		}
		LoggerUtils.testLogger = testLogger;
	}

	/**
	 * Disable capturing, forgetting about the logger set via {@link #enableCaptureWith(Logger)}.
	 */
	public static void disableCapture() {
		LoggerUtils.testLogger = null;
	}


	private static class CapturingFactory implements Function<String, Logger>, Disposable {

		private final Object originalFactory;
		private final Method originalFactoryMethod;

		private CapturingFactory(Object factory, Method method) {
			originalFactory = factory;
			originalFactoryMethod = method;
		}

		@Override
		public Logger apply(String category) {
			try {
				Logger original = (Logger) originalFactoryMethod.invoke(originalFactory, category);
				return new DivertingLogger(original);
			}
			catch (IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void dispose() {
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
