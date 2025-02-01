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
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class JdkLoggerTest {

	@Test
	void formatNullFormat() {
		Logger logger = Mockito.mock(Logger.class);
		Loggers.JdkLogger jdkLogger = new Loggers.JdkLogger(logger);

		jdkLogger.debug(null, (Object[]) null);

		verify(logger, times(1))
				.log(Level.FINE, (String) null);
	}

	@Test
	void nullFormatIsAcceptedByUnderlyingLogger() {
		StringBuilder log = new StringBuilder();
		Logger underlyingLogger = Logger.getAnonymousLogger();
		underlyingLogger.setLevel(Level.FINEST);
		underlyingLogger.addHandler(
				new Handler() {
					@Override
					public void publish(LogRecord record) {
						log.append(record.getMessage());
					}

					@Override
					public void flush() {
					}

					@Override
					public void close() throws SecurityException {
					}
				});

		Loggers.JdkLogger jdkLogger = new Loggers.JdkLogger(underlyingLogger);

		jdkLogger.trace(null, null, null);

		assertThat(log.toString()).isEqualTo("null");
	}

	@Test
	void formatNullVararg() {
		Logger logger = Mockito.mock(Logger.class);
		Loggers.JdkLogger jdkLogger = new Loggers.JdkLogger(logger);

		jdkLogger.info("test {} is {}", (Object[]) null);

		verify(logger, times(1))
				.log(Level.INFO, "test {} is {}");
	}

	@Test
	void formatNullParamInVararg() {
		Logger logger = Mockito.mock(Logger.class);
		Loggers.JdkLogger jdkLogger = new Loggers.JdkLogger(logger);

		jdkLogger.trace("test {} is {}", null, null);

		verify(logger, times(1))
				.log(Level.FINEST, "test null is null");
	}

	@Test
	void logWarnLevel() {
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
	void logWithThrowable() {
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
}
