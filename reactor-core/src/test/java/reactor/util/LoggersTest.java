/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import reactor.ReactorTestExecutionListener;

import static org.assertj.core.api.Assertions.assertThat;

class LoggersTest {

	@AfterAll
	static void resetLoggerFactory() {
		//delegate to ReactorTestExecutionListener to centralize the logic
		ReactorTestExecutionListener.resetLoggersFactory();
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

}
