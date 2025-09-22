/*
 * Copyright (c) 2021-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.junit.jupiter.api.Test;

import reactor.core.Exceptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestLoggerTest {

	@Test
	void returnMessageWithoutThreadNameWhenLogCurrentThreadNameIsFalse() {
		TestLogger testLogger = new TestLogger(false);

		assertThat(testLogger.isLogCurrentThreadName()).as("isLogCurrentThreadName").isFalse();
		assertEquals("[ERROR] TestMessage\n", testLogger.logContent("ERROR",
				"TestMessage"));
	}

	@Test
	void returnMessageWithoutThreadNameWhenLogCurrentThreadNameIsTrue() {
		TestLogger testLogger = new TestLogger(true);

		assertThat(testLogger.isLogCurrentThreadName()).as("isLogCurrentThreadName").isTrue();
		assertEquals(String.format("[ERROR] (%s) TestMessage\n", Thread.currentThread().getName()),
				testLogger.logContent("ERROR", "TestMessage"));
	}

	@Test
	void getName() {
		assertThat(new TestLogger().getName()).isEqualTo("TestLogger");
	}

	@Test
	void resetContents() {
		final TestLogger testLogger = new TestLogger(false);
		testLogger.info("info");
		testLogger.error("error");

		assertThat(testLogger.getOutContent())
			.as("out before reset")
			.isEqualToIgnoringNewLines("[ INFO] info");
		assertThat(testLogger.getErrContent())
			.as("err before reset")
			.isEqualToIgnoringNewLines("[ERROR] error");

		testLogger.reset();

		assertThat(testLogger.getOutContent())
			.as("after reset")
			.isEqualTo(testLogger.getErrContent())
			.isEmpty();
	}

	@Test
	void allModesAreConsideredEnabled() {
		TestLogger testLogger = new TestLogger();

		assertThat(testLogger.isInfoEnabled()).as("isInfoEnabled").isTrue();
		assertThat(testLogger.isDebugEnabled()).as("isDebugEnabled").isTrue();
		assertThat(testLogger.isTraceEnabled()).as("isTraceEnabled").isTrue();
		assertThat(testLogger.isWarnEnabled()).as("isWarnEnabled").isTrue();
		assertThat(testLogger.isErrorEnabled()).as("isErrorEnabled").isTrue();
	}

	@Test
	void infoNoThrowableLogsToOutContent() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.info("msg1");
		testLogger.info("msg{}", 2);

		assertThat(testLogger.getOutContent()).as("out")
			.isEqualToIgnoringNewLines("[ INFO] msg1[ INFO] msg2");
		assertThat(testLogger.getErrContent()).as("err").isEmpty();
	}

	@Test
	void infoThrowableLogsToOutContentPrintStackTraceToErrContent() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.info("msg", Exceptions.TERMINATED);

		assertThat(testLogger.getOutContent()).as("out")
			.isEqualToIgnoringNewLines("[ INFO] msg - reactor.core.Exceptions$StaticThrowable: Operator has been terminated");
		assertThat(testLogger.getErrContent()).as("err")
			.isEqualToIgnoringNewLines("reactor.core.Exceptions$StaticThrowable: Operator has been terminated");
	}

	@Test
	void debugNoThrowableLogsToOutContent() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.debug("msg1");
		testLogger.debug("msg{}", 2);

		assertThat(testLogger.getOutContent()).as("out")
			.isEqualToIgnoringNewLines("[DEBUG] msg1[DEBUG] msg2");
		assertThat(testLogger.getErrContent()).as("err").isEmpty();
	}

	@Test
	void debugThrowableLogsToOutContentPrintStackTraceToErrContent() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.debug("msg", Exceptions.TERMINATED);

		assertThat(testLogger.getOutContent()).as("out")
			.isEqualToIgnoringNewLines("[DEBUG] msg - reactor.core.Exceptions$StaticThrowable: Operator has been terminated");
		assertThat(testLogger.getErrContent()).as("err")
			.isEqualToIgnoringNewLines("reactor.core.Exceptions$StaticThrowable: Operator has been terminated");
	}

	@Test
	void traceNoThrowableLogsToOutContent() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.trace("msg1");
		testLogger.trace("msg{}", 2);

		assertThat(testLogger.getOutContent()).as("out")
			.isEqualToIgnoringNewLines("[TRACE] msg1[TRACE] msg2");
		assertThat(testLogger.getErrContent()).as("err").isEmpty();
	}

	@Test
	void traceThrowableLogsToOutContentPrintStackTraceToErrContent() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.trace("msg", Exceptions.TERMINATED);

		assertThat(testLogger.getOutContent()).as("out")
			.isEqualToIgnoringNewLines("[TRACE] msg - reactor.core.Exceptions$StaticThrowable: Operator has been terminated");
		assertThat(testLogger.getErrContent()).as("err")
			.isEqualToIgnoringNewLines("reactor.core.Exceptions$StaticThrowable: Operator has been terminated");
	}

	@Test
	void warnLogsAndPrintsThrowableStackTraceToErrContent() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.warn("msg1");
		testLogger.warn("msg{}", 2);
		testLogger.warn("msg", Exceptions.TERMINATED);

		assertThat(testLogger.getOutContent()).as("out").isEmpty();
		assertThat(testLogger.getErrContent().split(System.lineSeparator()))
			.as("err")
			.containsExactly(
				"[ WARN] msg1",
				"[ WARN] msg2",
				"[ WARN] msg - reactor.core.Exceptions$StaticThrowable: Operator has been terminated",
				"reactor.core.Exceptions$StaticThrowable: Operator has been terminated");
	}

	@Test
	void errorLogsAndPrintsThrowableStackTraceToErrContent() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.error("msg1");
		testLogger.error("msg{}", 2);
		testLogger.error("msg", Exceptions.TERMINATED);

		assertThat(testLogger.getOutContent()).as("out").isEmpty();
		assertThat(testLogger.getErrContent().split(System.lineSeparator()))
			.as("err")
			.containsExactly(
				"[ERROR] msg1",
				"[ERROR] msg2",
				"[ERROR] msg - reactor.core.Exceptions$StaticThrowable: Operator has been terminated",
				"reactor.core.Exceptions$StaticThrowable: Operator has been terminated");
	}

	@Test
	void traceLogsNullArgument() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.trace("msg {}", (Object) null);
		testLogger.trace("msg {} {}", 1, null);
		testLogger.trace("msg {} {} {}", 1, null, 3);
		assertThat(testLogger.getErrContent()).as("err").isEmpty();
		assertThat(testLogger.getOutContent().split(System.lineSeparator()))
				.as("out")
				.containsExactly(
						"[TRACE] msg null",
						"[TRACE] msg 1 null",
						"[TRACE] msg 1 null 3");
	}

	@Test
	void debugLogsNullArgument() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.debug("msg {}", (Object) null);
		testLogger.debug("msg {} {}", 1, null);
		testLogger.debug("msg {} {} {}", 1, null, 3);
		assertThat(testLogger.getErrContent()).as("err").isEmpty();
		assertThat(testLogger.getOutContent().split(System.lineSeparator()))
				.as("out")
				.containsExactly(
						"[DEBUG] msg null",
						"[DEBUG] msg 1 null",
						"[DEBUG] msg 1 null 3");
	}

	@Test
	void infoLogsNullArgument() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.info("msg {}", (Object) null);
		testLogger.info("msg {} {}", 1, null);
		testLogger.info("msg {} {} {}", 1, null, 3);
		assertThat(testLogger.getErrContent()).as("err").isEmpty();
		assertThat(testLogger.getOutContent().split(System.lineSeparator()))
				.as("out")
				.containsExactly(
						"[ INFO] msg null",
						"[ INFO] msg 1 null",
						"[ INFO] msg 1 null 3");
	}

	@Test
	void warnLogsNullArgument() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.warn("msg {}", (Object) null);
		testLogger.warn("msg {} {}", 1, null);
		testLogger.warn("msg {} {} {}", 1, null, 3);
		assertThat(testLogger.getOutContent()).as("out").isEmpty();
		assertThat(testLogger.getErrContent().split(System.lineSeparator()))
				.as("err")
				.containsExactly(
						"[ WARN] msg null",
						"[ WARN] msg 1 null",
						"[ WARN] msg 1 null 3");
	}

	@Test
	void errorLogsNullArgument() {
		TestLogger testLogger = new TestLogger(false);
		testLogger.error("msg {}", (Object) null);
		testLogger.error("msg {} {}", 1, null);
		testLogger.error("msg {} {} {}", 1, null, 3);
		assertThat(testLogger.getOutContent()).as("out").isEmpty();
		assertThat(testLogger.getErrContent().split(System.lineSeparator()))
				.as("err")
				.containsExactly(
						"[ERROR] msg null",
						"[ERROR] msg 1 null",
						"[ERROR] msg 1 null 3");
	}
}
