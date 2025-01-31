/*
 * Copyright (c) 2017-2025 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.logging.Level;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import reactor.ReactorLauncherSessionListener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class LoggersTest {

	@AfterAll
	static void resetLoggerFactory() {
		//delegate to ReactorLauncherSessionListener to centralize the logic
		ReactorLauncherSessionListener.resetLoggersFactory();
	}

	@Test
	void dontFallbackToJdk() throws Exception {
		String oldValue = System.getProperty(Loggers.FALLBACK_PROPERTY);

		System.setProperty(Loggers.FALLBACK_PROPERTY, "something");
		try {
			assertThat(Loggers.isFallbackToJdk()).isFalse();
		}
		finally {
			if (oldValue == null) System.clearProperty(Loggers.FALLBACK_PROPERTY);
			else System.setProperty(Loggers.FALLBACK_PROPERTY, oldValue);
		}
	}

	@Test
	void fallbackToJdk() throws Exception {
		String oldValue = System.getProperty(Loggers.FALLBACK_PROPERTY);

		System.setProperty(Loggers.FALLBACK_PROPERTY, "JdK");
		try {
			assertThat(Loggers.isFallbackToJdk()).isTrue();
		}
		finally {
			if (oldValue == null) System.clearProperty(Loggers.FALLBACK_PROPERTY);
			else System.setProperty(Loggers.FALLBACK_PROPERTY, oldValue);
		}
	}

	@Test
	void useConsoleLoggers() throws Exception {
		try {
			Loggers.useConsoleLoggers();
			Logger l = Loggers.getLogger("test");

			assertThat(l.getClass().getSimpleName()).isEqualTo("ConsoleLogger");
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	@Test
	void useVerboseConsoleLoggers() throws Exception {
		try {
			Loggers.useVerboseConsoleLoggers();
			Logger l = Loggers.getLogger("test");

			assertThat(l.getClass().getSimpleName()).isEqualTo("ConsoleLogger");
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	@Test
	void useJdkLoggers() throws Exception {
		try {
			Loggers.useJdkLoggers();
			Logger l = Loggers.getLogger("test");

			assertThat(l.getClass().getSimpleName()).isEqualTo("JdkLogger");
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	@Test
	void useSl4jLoggers() throws Exception {
		try {
			Loggers.useSl4jLoggers();
			Logger l = Loggers.getLogger("test");

			assertThat(l.getClass().getSimpleName()).isEqualTo("Slf4JLogger");
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	@Test
	void logWhenUsingJdkLoggers() {
		java.util.logging.Logger jdkLogger = mock(java.util.logging.Logger.class);
		Loggers.JdkLogger log = new Loggers.JdkLogger(jdkLogger);

		log.warn("message: {}, {}", "foo", "bar");

		verify(jdkLogger, times(1))
				.log(
						Level.WARNING,
						"message: foo, bar"
				);
	}

	@Test
	void logWithThrowableWhenUsingJdkLoggers() {
		java.util.logging.Logger jdkLogger = mock(java.util.logging.Logger.class);
		Loggers.JdkLogger log = new Loggers.JdkLogger(jdkLogger);

		Throwable t = new IllegalAccessError();
		log.warn("message: {}, {}", "foo", "bar", t);

		verify(jdkLogger, times(1))
				.log(
						Level.WARNING,
						"message: foo, bar",
						t
				);
	}

	@Test
	void logWarnWhenUsingConsoleLoggers() {
		PrintStream logConsoleLogger = mock(PrintStream.class);
		PrintStream errorConsoleLogger = mock(PrintStream.class);
		Loggers.ConsoleLogger log = new Loggers.ConsoleLogger(
				"test",
				logConsoleLogger,
				errorConsoleLogger,
				true
		);

		log.warn("message: {}, {}", "foo", "bar");

		verify(errorConsoleLogger, times(1))
				.format(
						"[%s] (%s) %s\n",
						" WARN",
						Thread.currentThread().getName(),
						"message: foo, bar"
				);
	}

	@Test
	void logWarnWithThrowableWhenUsingConsoleLoggers() {
		PrintStream logConsoleLogger = mock(PrintStream.class);
		PrintStream errorConsoleLogger = mock(PrintStream.class);
		Loggers.ConsoleLogger log = new Loggers.ConsoleLogger(
				"test",
				logConsoleLogger,
				errorConsoleLogger,
				true
		);

		Throwable t = mock(IllegalAccessError.class);
		log.warn("message: {}, {}", "foo", "bar", t);

		verify(logConsoleLogger, never())
				.format(
						anyString(),
						anyString(),
						anyString(),
						anyString()
				);

		verify(errorConsoleLogger, times(1))
				.format(
						"[%s] (%s) %s\n",
						" WARN",
						Thread.currentThread().getName(),
						"message: foo, bar"
				);

		verify(t, times(1))
				.printStackTrace(errorConsoleLogger);
	}

	@Test
	void logInfoWhenUsingConsoleLoggers() {
		PrintStream logConsoleLogger = mock(PrintStream.class);
		PrintStream errorConsoleLogger = mock(PrintStream.class);
		Loggers.ConsoleLogger log = new Loggers.ConsoleLogger(
				"test",
				logConsoleLogger,
				errorConsoleLogger,
				true
		);

		log.info("message: {}, {}", "foo", "bar");

		verify(logConsoleLogger, times(1))
				.format(
						"[%s] (%s) %s\n",
						" INFO",
						Thread.currentThread().getName(),
						"message: foo, bar"
				);
	}

	@Test
	void logInfoWithThrowableWhenUsingConsoleLoggers() {
		PrintStream logConsoleLogger = mock(PrintStream.class);
		PrintStream errorConsoleLogger = mock(PrintStream.class);
		Loggers.ConsoleLogger log = new Loggers.ConsoleLogger(
				"test",
				logConsoleLogger,
				errorConsoleLogger,
				true
		);

		Throwable t = mock(IllegalAccessError.class);
		log.info("message: {}, {}", "foo", "bar", t);

		verify(errorConsoleLogger, never())
				.format(
						anyString(),
						anyString(),
						anyString(),
						anyString()
				);

		verify(logConsoleLogger, times(1))
				.format(
						"[%s] (%s) %s\n",
						" INFO",
						Thread.currentThread().getName(),
						"message: foo, bar"
				);

		verify(t, times(1))
				.printStackTrace(logConsoleLogger);
	}
}
