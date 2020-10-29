/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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

public class JdkLoggerTest {

	@Test
	public void formatNullFormat() {
		Loggers.JdkLogger jdkLogger = new Loggers.JdkLogger(Mockito.mock(java.util.logging.Logger.class));

		assertThat(jdkLogger.format(null, (Object[]) null))
				.as("null format should be interpreted as null")
				.isNull();
	}

	@Test
	public void nullFormatIsAcceptedByUnderlyingLogger() {
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
					public void flush() { }

					@Override
					public void close() throws SecurityException { }
				});

		Loggers.JdkLogger jdkLogger = new Loggers.JdkLogger(underlyingLogger);

		jdkLogger.trace(null, null, null);

		assertThat(log.toString()).isEqualTo("null");
	}

	@Test
	public void formatNullVararg() {
		Loggers.JdkLogger jdkLogger= new Loggers.JdkLogger(Mockito.mock(java.util.logging.Logger.class));

		assertThat(jdkLogger.format("test {} is {}", (Object[]) null))
				.as("format should be returned as is")
				.isEqualTo("test {} is {}");
	}

	@Test
	public void formatNullParamInVararg() {
		Loggers.JdkLogger jdkLogger= new Loggers.JdkLogger(Mockito.mock(java.util.logging.Logger.class));

		assertThat(jdkLogger.format("test {} is {}", null, null))
				.as("placeholders should be replaced by null")
				.isEqualTo("test null is null");
	}

}
