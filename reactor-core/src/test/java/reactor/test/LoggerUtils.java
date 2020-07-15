package reactor.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.pattern.ThrowableProxyConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import org.slf4j.LoggerFactory;
import reactor.test.util.TestLogger;

public class LoggerUtils {
	static final String TEST_APPENDER_NAME = "TestAppender";

	public static void addAppender(TestLogger testLogger, Class<?> classWithLogger) {
		org.slf4j.Logger slf4jLogger = LoggerFactory.getLogger(classWithLogger);
		if (slf4jLogger instanceof Logger) {
			Logger logbackLogger = (Logger) slf4jLogger;
			TestAppender appender = new TestAppender(testLogger);
			appender.start();
			logbackLogger.addAppender(appender);
		}
	}

	public static void resetAppender(Class<?> classWithLogger) {
		org.slf4j.Logger slf4jLogger = LoggerFactory.getLogger(classWithLogger);
		if (slf4jLogger instanceof Logger) {
			Logger logbackLogger = (Logger) slf4jLogger;
			logbackLogger.detachAppender(TEST_APPENDER_NAME);
		}
	}

	static class TestAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

		private final TestLogger testLogger;
		private final ThrowableProxyConverter throwableProxyConverter = new ThrowableProxyConverter();

		TestAppender(TestLogger testLogger) {
			this.testLogger = testLogger;
		}

		@Override
		protected void append(ILoggingEvent eventObject) {
			if (eventObject.getLevel() == Level.ERROR) {
				testLogger.error(eventObject.getFormattedMessage()
				                            .concat("\n")
				                            .concat(throwableProxyConverter.convert(eventObject)));

			} else {
				testLogger.info(eventObject.getFormattedMessage());
			}
		}

		@Override
		public String getName() {
			return TEST_APPENDER_NAME;
		}

	}
}
