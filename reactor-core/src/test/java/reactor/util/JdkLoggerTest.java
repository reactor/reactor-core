/*
 * Copyright (c) 2018-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JdkLoggerTest {

	@Test
	public void formatNullFormat() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.FINE);

		jdkLogger.debug(null, (Object[]) null);

		assertThat(log.toString()).isEqualTo("null");
	}

	@Test
	public void nullFormatIsAcceptedByUnderlyingLogger() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.FINEST);

		jdkLogger.trace(null, null, null);

		assertThat(log.toString()).isEqualTo("null");
	}

	@Test
	public void formatNullVararg() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.INFO);

		jdkLogger.info("test {} is {}", (Object[]) null);

		assertThat(log.toString()).isEqualTo("test {} is {}");
	}

	@Test
	public void formatNullParamInVararg() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.FINEST);

		jdkLogger.trace("test {} is {}", null, null);

		assertThat(log.toString()).isEqualTo("test null is null");
	}

	@Test
	public void trace() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.FINEST);

		jdkLogger.trace("test {} is {}", "foo", "bar");

		assertThat(log.toString()).isEqualTo("test foo is bar");
	}

	@Test
	public void traceWithThrowable() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.FINEST);

		Throwable t = new IllegalAccessError();
		jdkLogger.trace("test {} is {}", "foo", "bar", t);

		assertThat(log.toString()).startsWith("test foo is bar"
				+ "\njava.lang.IllegalAccessError\n"
				+ "\tat reactor.util.JdkLoggerTest.traceWithThrowable");
	}

	@Test
	public void debug() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.FINE);

		jdkLogger.debug("test {} is {}", "foo", "bar");

		assertThat(log.toString()).isEqualTo("test foo is bar");
	}

	@Test
	public void debugWithThrowable() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.FINE);

		Throwable t = new IllegalAccessError();
		jdkLogger.debug("test {} is {}", "foo", "bar", t);

		assertThat(log.toString()).startsWith("test foo is bar"
				+ "\njava.lang.IllegalAccessError\n"
				+ "\tat reactor.util.JdkLoggerTest.debugWithThrowable");
	}

	@Test
	public void info() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.FINE);

		jdkLogger.info("test {} is {}", "foo", "bar");

		assertThat(log.toString()).isEqualTo("test foo is bar");
	}

	@Test
	public void infoWithThrowable() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.FINE);

		Throwable t = new IllegalAccessError();
		jdkLogger.info("test {} is {}", "foo", "bar", t);

		assertThat(log.toString()).startsWith("test foo is bar"
				+ "\njava.lang.IllegalAccessError\n"
				+ "\tat reactor.util.JdkLoggerTest.infoWithThrowable");
	}

	@Test
	public void warn() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.WARNING);

		jdkLogger.warn("test {} is {}", "foo", "bar");

		assertThat(log.toString()).isEqualTo("test foo is bar");
	}

	@Test
	public void warnWithThrowable() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.WARNING);

		Throwable t = new IllegalAccessError();
		jdkLogger.warn("test {} is {}", "foo", "bar", t);

		assertThat(log.toString()).startsWith("test foo is bar"
				+ "\njava.lang.IllegalAccessError\n"
				+ "\tat reactor.util.JdkLoggerTest.warnWithThrowable");
	}

	@Test
	public void error() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.SEVERE);

		jdkLogger.error("test {} is {}", "foo", "bar");

		assertThat(log.toString()).isEqualTo("test foo is bar");
	}

	@Test
	public void errorWithThrowable() {
		StringBuilder log = new StringBuilder();
		Loggers.JdkLogger jdkLogger = getJdkLogger(log, Level.SEVERE);

		Throwable t = new IllegalAccessError();
		jdkLogger.error("test {} is {}", "foo", "bar", t);

		assertThat(log.toString()).startsWith("test foo is bar"
				+ "\njava.lang.IllegalAccessError\n"
				+ "\tat reactor.util.JdkLoggerTest.errorWithThrowable");
	}

	private Loggers.JdkLogger getJdkLogger(StringBuilder log, Level level) {
		Logger underlyingLogger = Logger.getAnonymousLogger();
		underlyingLogger.setLevel(level);
		underlyingLogger.addHandler(
				new Handler() {
					@Override
					public void publish(LogRecord record) {
						log.append(record.getMessage());

						if (record.getThrown() != null) {
							log.append("\n").append(record.getThrown().toString());
							for (StackTraceElement element : record.getThrown().getStackTrace()) {
								log.append("\n\tat ").append(element.toString());
							}
						}
					}

					@Override
					public void flush() { }

					@Override
					public void close() throws SecurityException { }
				});

		return new Loggers.JdkLogger(underlyingLogger);
	}
}
