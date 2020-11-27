/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.test.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import reactor.core.Disposable;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.*;

class LoggerUtilsTest {

	@Test
	void installsFactory() {
		Disposable disposable = LoggerUtils.useCurrentLoggersWithCapture();
		TestLogger testLogger = new TestLogger();
		try {
			Logger frameworkLogger = Loggers.getLogger("category"); // simulates an early creation of a logger

			LoggerUtils.enableCaptureWith(testLogger);
			frameworkLogger.debug("Look ma!, I'm debugging!");
			assertThat(testLogger.getOutContent()).contains("Look ma!, I'm debugging!");
			LoggerUtils.disableCapture();
			frameworkLogger.debug("This won't be captured");
			assertThat(testLogger.getOutContent()).doesNotContain("This won't be captured");
		} finally {
			disposable.dispose();

			// The following tests that once disposed, capturing is no longer in effect
			LoggerUtils.enableCaptureWith(testLogger);
			Logger otherLogger = Loggers.getLogger("another");
			otherLogger.debug("This won't be captured either");
			assertThat(testLogger.getOutContent()).doesNotContain("This won't be captured either");
		}
	}

	@Test
	void disposeOnlyUninstallsItself() {
		Disposable disposable = LoggerUtils.useCurrentLoggersWithCapture();
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> {
			Loggers.resetLoggerFactory(); // Overwrites our custom logger
			disposable.dispose();
		})
		.withMessageContaining("Expected the current factory to be " + LoggerUtils.class.getName() + "$");
	}

	@Test
	void continuouslyInstallingFactoryDoesntCauseStackOverflow() {
		final int LOOPS = 2000;
		List<Disposable> disposables = new ArrayList<>(LOOPS);
		for (int i = 0; i < LOOPS; i++) {
			disposables.add(LoggerUtils.useCurrentLoggersWithCapture());
		}

		final Logger test = Loggers.getLogger("test");

		Loggers.resetLoggerFactory();

		assertThatCode(() -> test.error("expected error message")).doesNotThrowAnyException();
	}

}