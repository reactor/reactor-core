/*
 * Copyright (c) 2020-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.test.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.*;

@Isolated
class LoggerUtilsTest {

	/**
	 * Disposable added to this list will be disposed at end of test, ignoring exceptions
	 */
	static final List<Disposable> SAFE_DISPOSE = new ArrayList<>();

	static Disposable disposeAfterTest(Disposable d) {
		SAFE_DISPOSE.add(d);
		return d;
	}

	@AfterEach
	void safelyDispose() {
		for (Disposable disposable : SAFE_DISPOSE) {
			try {
				disposable.dispose();
			}
			catch (Exception e) {
				//NO-OP
			}
		}
		SAFE_DISPOSE.clear();
	}

	@Test
	void installsFactory() {
		Disposable disposable = disposeAfterTest(LoggerUtils.useCurrentLoggersWithCapture());
		TestLogger testLogger = new TestLogger();
		Logger frameworkLogger = Loggers.getLogger("category"); // simulates an early creation of a logger

		LoggerUtils.enableCaptureWith(testLogger);
		frameworkLogger.debug("Look ma!, I'm debugging!");
		assertThat(testLogger.getOutContent()).contains("Look ma!, I'm debugging!");

		LoggerUtils.disableCapture();
		frameworkLogger.debug("This won't be captured");
		assertThat(testLogger.getOutContent()).doesNotContain("This won't be captured");

		LoggerUtils.enableCaptureWith(testLogger);
		frameworkLogger.debug("I've reactivated redirection");
		assertThat(testLogger.getOutContent()).contains("I've reactivated redirection");

		disposable.dispose();

		// The following tests that once disposed, capturing is no longer in effect
		Logger otherLogger = Loggers.getLogger("another");
		otherLogger.debug("This won't be captured either");
		assertThat(testLogger.getOutContent()).doesNotContain("This won't be captured either");
	}

	@Test
	void disposeUninstallsFromLoggersAndLoggerUtils() {
		Disposable d = disposeAfterTest(LoggerUtils.useCurrentLoggersWithCapture());

		assertThat(Loggers.getLogger("foo")).as("when factory installed").isInstanceOf(LoggerUtils.DivertingLogger.class);
		assertThat(LoggerUtils.currentCapturingFactory).as("when currentCapturingFactory").isSameAs(d);

		d.dispose();

		assertThat(Loggers.getLogger("foo")).as("after uninstall").isNotInstanceOf(LoggerUtils.DivertingLogger.class);
		assertThat(LoggerUtils.currentCapturingFactory).as("not currentCapturingFactory").isNull();
	}

	@Test
	void disposeOnlyUninstallsItselfFromLoggersFactory() {
		Disposable disposable = disposeAfterTest(LoggerUtils.useCurrentLoggersWithCapture());
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
			disposables.add(disposeAfterTest(LoggerUtils.useCurrentLoggersWithCapture()));
		}

		final Logger test = Loggers.getLogger("test");

		Loggers.resetLoggerFactory();

		assertThatCode(() -> test.error("expected error message")).doesNotThrowAnyException();
	}

	@Test
	void enableCaptureWithThrowsIfNotCapturing() {
		assertThatIllegalStateException()
			.isThrownBy(() -> LoggerUtils.enableCaptureWith(new TestLogger()))
			.withMessage("LoggerUtils#useCurrentLoggerWithCapture() hasn't been called");
	}

	@Test
	void enableCaptureWithNoRedirectThrowsIfNotCapturing() {
		assertThatIllegalStateException()
			.isThrownBy(() -> LoggerUtils.enableCaptureWith(new TestLogger(), false))
			.withMessage("LoggerUtils#useCurrentLoggerWithCapture() hasn't been called");
	}

	@Test
	void enableCaptureWithThrowsIfPreviouslyEnabledCapture() {
		disposeAfterTest(LoggerUtils.useCurrentLoggersWithCapture());

		assertThatCode(() -> LoggerUtils.enableCaptureWith(new TestLogger()))
			.as("first enableCapture")
			.doesNotThrowAnyException();

		assertThatIllegalStateException()
			.as("redundant enableCapture")
			.isThrownBy(() -> LoggerUtils.enableCaptureWith(new TestLogger()))
			.withMessage("A logger was already set, maybe from a previous run. Don't forget to call disableCapture()");
	}

	@Test
	void enableCaptureWithNoRedirectThrowsIfPreviouslyEnabledCapture() {
		disposeAfterTest(LoggerUtils.useCurrentLoggersWithCapture());

		assertThatCode(() -> LoggerUtils.enableCaptureWith(new TestLogger()))
			.as("first enableCapture")
			.doesNotThrowAnyException();

		assertThatIllegalStateException()
			.as("redundant enableCapture")
			.isThrownBy(() -> LoggerUtils.enableCaptureWith(new TestLogger(), false))
			.withMessage("A logger was already set, maybe from a previous run. Don't forget to call disableCapture()");
	}

	@Nested
	class DivertingLoggerTest {

		private final TestLogger INACTIVE_TEST_LOGGER = new TestLogger(false) {
			@Override
			public boolean isInfoEnabled() {
				return false;
			}

			@Override
			public boolean isTraceEnabled() {
				return false;
			}

			@Override
			public boolean isDebugEnabled() {
				return false;
			}

			@Override
			public boolean isWarnEnabled() {
				return false;
			}

			@Override
			public boolean isErrorEnabled() {
				return false;
			}
		};

		@ParameterizedTest
		@ValueSource(booleans = { true, false })
		void allLogMethods(boolean redirect) {
			final TestLogger underlyingTestLogger = new TestLogger(false);
			Function<String, Logger> fakeFactory = s -> {
				throw new UnsupportedOperationException("test shouldn't trigger the original factory");
			};
			final TestLogger capturingTestLogger = new TestLogger(false);
			LoggerUtils.CapturingFactory capturingFactory = new LoggerUtils.CapturingFactory(fakeFactory);
			LoggerUtils.DivertingLogger divertingLogger = new LoggerUtils.DivertingLogger(underlyingTestLogger, capturingFactory);

			//optionally activate the redirection to our capturingTestLogger, with suppression of original logs
			capturingFactory.enableRedirection(capturingTestLogger, redirect);

			divertingLogger.info("info1");
			divertingLogger.info("info{}", 2);
			divertingLogger.info("info3", Exceptions.TERMINATED); //TERMINATED has no stacktrace

			divertingLogger.warn("warn1");
			divertingLogger.warn("warn{}", 2);
			divertingLogger.warn("warn3", Exceptions.TERMINATED); //TERMINATED has no stacktrace

			divertingLogger.error("error1");
			divertingLogger.error("error{}", 2);
			divertingLogger.error("error3", Exceptions.TERMINATED); //TERMINATED has no stacktrace

			divertingLogger.debug("debug1");
			divertingLogger.debug("debug{}", 2);
			divertingLogger.debug("debug3", Exceptions.TERMINATED); //TERMINATED has no stacktrace

			divertingLogger.trace("trace1");
			divertingLogger.trace("trace{}", 2);
			divertingLogger.trace("trace3", Exceptions.TERMINATED); //TERMINATED has no stacktrace

			String[] expectedOut = new String[] {
				"[ INFO] info1",
				"[ INFO] info2",
				"[ INFO] info3 - reactor.core.Exceptions$StaticThrowable: Operator has been terminated",
				"[DEBUG] debug1",
				"[DEBUG] debug2",
				"[DEBUG] debug3 - reactor.core.Exceptions$StaticThrowable: Operator has been terminated",
				"[TRACE] trace1",
				"[TRACE] trace2",
				"[TRACE] trace3 - reactor.core.Exceptions$StaticThrowable: Operator has been terminated"
			};

			String[] expectedErrInAnyOrder = new String[] {
				"[ WARN] warn1",
				"[ WARN] warn2",
				"[ WARN] warn3 - reactor.core.Exceptions$StaticThrowable: Operator has been terminated",
				"[ERROR] error1",
				"[ERROR] error2",
				"[ERROR] error3 - reactor.core.Exceptions$StaticThrowable: Operator has been terminated",
				//all logging methods with Throwable also print the stacktrace to err logger
				"reactor.core.Exceptions$StaticThrowable: Operator has been terminated",
				"reactor.core.Exceptions$StaticThrowable: Operator has been terminated",
				"reactor.core.Exceptions$StaticThrowable: Operator has been terminated",
				"reactor.core.Exceptions$StaticThrowable: Operator has been terminated",
				"reactor.core.Exceptions$StaticThrowable: Operator has been terminated"
			};

			assertThat(capturingTestLogger.getOutContent().split(System.lineSeparator()))
				.as("captured out")
				.containsExactly(expectedOut);

			assertThat(capturingTestLogger.getErrContent().split(System.lineSeparator()))
				.as("captured err")
				.containsExactlyInAnyOrder(expectedErrInAnyOrder);

			if (redirect) {
				assertThat(underlyingTestLogger.getOutContent().split(System.lineSeparator()))
					.as("redirected to original out")
					.containsExactly(expectedOut);
				assertThat(underlyingTestLogger.getErrContent().split(System.lineSeparator()))
					.as("redirected to original err")
					.containsExactlyInAnyOrder(expectedErrInAnyOrder);
			}
			else {
				assertThat(underlyingTestLogger.getErrContent()).as("suppressed original err").isEmpty();
				assertThat(underlyingTestLogger.getOutContent()).as("suppressed original out").isEmpty();
			}
		}

		@Test
		void categoriesAreActiveIfActiveInOriginal() {
			TestLogger original = new TestLogger(false);
			TestLogger redirect = INACTIVE_TEST_LOGGER;
			LoggerUtils.CapturingFactory capturingFactory = new LoggerUtils.CapturingFactory(s -> original);
			capturingFactory.enableRedirection(redirect, true);

			LoggerUtils.DivertingLogger divertingLogger = new LoggerUtils.DivertingLogger(original, capturingFactory);
			
			assertThat(divertingLogger.isInfoEnabled()).as("isInfoEnabled").isTrue();
			assertThat(divertingLogger.isWarnEnabled()).as("isWarnEnabled").isTrue();
			assertThat(divertingLogger.isErrorEnabled()).as("isErrorEnabled").isTrue();
			assertThat(divertingLogger.isDebugEnabled()).as("isDebugEnabled").isTrue();
			assertThat(divertingLogger.isTraceEnabled()).as("isTraceEnabled").isTrue();
		}

		@Test
		void categoriesAreActiveIfActiveInRedirect() {
			TestLogger original = INACTIVE_TEST_LOGGER;
			TestLogger redirect = new TestLogger(false);
			LoggerUtils.CapturingFactory capturingFactory = new LoggerUtils.CapturingFactory(s -> original);
			capturingFactory.enableRedirection(redirect, true);

			LoggerUtils.DivertingLogger divertingLogger = new LoggerUtils.DivertingLogger(original, capturingFactory);

			assertThat(divertingLogger.isInfoEnabled()).as("isInfoEnabled").isTrue();
			assertThat(divertingLogger.isWarnEnabled()).as("isWarnEnabled").isTrue();
			assertThat(divertingLogger.isErrorEnabled()).as("isErrorEnabled").isTrue();
			assertThat(divertingLogger.isDebugEnabled()).as("isDebugEnabled").isTrue();
			assertThat(divertingLogger.isTraceEnabled()).as("isTraceEnabled").isTrue();
		}

		@Test
		void categoriesAreInactiveIfInactiveInOriginalAndNoRedirect() {
			TestLogger original = INACTIVE_TEST_LOGGER;
			LoggerUtils.CapturingFactory capturingFactory = new LoggerUtils.CapturingFactory(s -> original);

			LoggerUtils.DivertingLogger divertingLogger = new LoggerUtils.DivertingLogger(original, capturingFactory);

			assertThat(divertingLogger.isInfoEnabled()).as("isInfoEnabled").isFalse();
			assertThat(divertingLogger.isWarnEnabled()).as("isWarnEnabled").isFalse();
			assertThat(divertingLogger.isErrorEnabled()).as("isErrorEnabled").isFalse();
			assertThat(divertingLogger.isDebugEnabled()).as("isDebugEnabled").isFalse();
			assertThat(divertingLogger.isTraceEnabled()).as("isTraceEnabled").isFalse();
		}

		@Test
		void categoriesAreInactiveIfInactiveInOriginalAndRedirect() {
			TestLogger original = INACTIVE_TEST_LOGGER;
			TestLogger redirect = INACTIVE_TEST_LOGGER;
			LoggerUtils.CapturingFactory capturingFactory = new LoggerUtils.CapturingFactory(s -> original);
			capturingFactory.enableRedirection(redirect, true);

			LoggerUtils.DivertingLogger divertingLogger = new LoggerUtils.DivertingLogger(original, capturingFactory);

			assertThat(divertingLogger.isInfoEnabled()).as("isInfoEnabled").isFalse();
			assertThat(divertingLogger.isWarnEnabled()).as("isWarnEnabled").isFalse();
			assertThat(divertingLogger.isErrorEnabled()).as("isErrorEnabled").isFalse();
			assertThat(divertingLogger.isDebugEnabled()).as("isDebugEnabled").isFalse();
			assertThat(divertingLogger.isTraceEnabled()).as("isTraceEnabled").isFalse();
		}
	}
}