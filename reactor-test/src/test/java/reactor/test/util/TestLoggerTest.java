/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestLoggerTest {

	@Test
	void returnMessageWithoutThreadNameWhenLogCurrentThreadNameIsFalse() {
		TestLogger testLogger = new TestLogger(false);

		assertEquals("[ERROR] TestMessage\n", testLogger.logContent("ERROR",
				"TestMessage"));
	}

	@Test
	void returnMessageWithoutThreadNameWhenLogCurrentThreadNameIsTrue() {
		TestLogger testLogger = new TestLogger(true);

		assertEquals(String.format("[ERROR] (%s) TestMessage\n", Thread.currentThread().getName()),
				testLogger.logContent("ERROR", "TestMessage"));
	}
}
