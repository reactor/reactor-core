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
