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

package reactor.util;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Simon Baslé
 */
class LoggerTest {

	private Logger mockVerbose() {
		Logger mockLogger = Mockito.mock(Logger.class);

		when(mockLogger.isInfoEnabled()).thenReturn(true);
		when(mockLogger.isWarnEnabled()).thenReturn(true);
		when(mockLogger.isErrorEnabled()).thenReturn(true);
		when(mockLogger.isDebugEnabled()).thenReturn(true);

		doCallRealMethod().when(mockLogger).infoOrDebug(any());
		doCallRealMethod().when(mockLogger).infoOrDebug(any(), any());
		doCallRealMethod().when(mockLogger).warnOrDebug(any());
		doCallRealMethod().when(mockLogger).warnOrDebug(any(), any());
		doCallRealMethod().when(mockLogger).errorOrDebug(any());
		doCallRealMethod().when(mockLogger).errorOrDebug(any(), any());

		return mockLogger;
	}

	private Logger mockTerse() {
		Logger mockLogger = Mockito.mock(Logger.class);

		when(mockLogger.isInfoEnabled()).thenReturn(true);
		when(mockLogger.isWarnEnabled()).thenReturn(true);
		when(mockLogger.isErrorEnabled()).thenReturn(true);
		when(mockLogger.isDebugEnabled()).thenReturn(false);

		doCallRealMethod().when(mockLogger).infoOrDebug(any());
		doCallRealMethod().when(mockLogger).infoOrDebug(any(), any());
		doCallRealMethod().when(mockLogger).warnOrDebug(any());
		doCallRealMethod().when(mockLogger).warnOrDebug(any(), any());
		doCallRealMethod().when(mockLogger).errorOrDebug(any());
		doCallRealMethod().when(mockLogger).errorOrDebug(any(), any());

		return mockLogger;
	}

	private Logger mockSilent() {
		Logger mockLogger = Mockito.mock(Logger.class);

		when(mockLogger.isInfoEnabled()).thenReturn(false);
		when(mockLogger.isWarnEnabled()).thenReturn(false);
		when(mockLogger.isErrorEnabled()).thenReturn(false);
		when(mockLogger.isDebugEnabled()).thenReturn(false);

		doCallRealMethod().when(mockLogger).infoOrDebug(any());
		doCallRealMethod().when(mockLogger).infoOrDebug(any(), any());
		doCallRealMethod().when(mockLogger).warnOrDebug(any());
		doCallRealMethod().when(mockLogger).warnOrDebug(any(), any());
		doCallRealMethod().when(mockLogger).errorOrDebug(any());
		doCallRealMethod().when(mockLogger).errorOrDebug(any(), any());

		return mockLogger;
	}

	private void verifyIsXxxEnabledXxxOrDebugAndNoMore(Logger mockLogger) {
		Mockito.verify(mockLogger, atMostOnce()).isDebugEnabled();
		Mockito.verify(mockLogger, atMostOnce()).isInfoEnabled();
		Mockito.verify(mockLogger, atMostOnce()).isWarnEnabled();
		Mockito.verify(mockLogger, atMostOnce()).isErrorEnabled();

		Mockito.verify(mockLogger, atMostOnce()).infoOrDebug(any());
		Mockito.verify(mockLogger, atMostOnce()).infoOrDebug(any(), any());

		Mockito.verify(mockLogger, atMostOnce()).warnOrDebug(any());
		Mockito.verify(mockLogger, atMostOnce()).warnOrDebug(any(), any());

		Mockito.verify(mockLogger, atMostOnce()).errorOrDebug(any());
		Mockito.verify(mockLogger, atMostOnce()).errorOrDebug(any(), any());

		Mockito.verifyNoMoreInteractions(mockLogger);
	}

	@Test
	void infoOrDebug_debugEnabled() {
		Logger mockLogger = mockVerbose();

		mockLogger.infoOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE");
		Mockito.verify(mockLogger).debug("VERBOSE");

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void infoOrDebug_infoEnabled() {
		Logger mockLogger = mockTerse();

		mockLogger.infoOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE");
		Mockito.verify(mockLogger).info("TERSE");

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void infoOrDebug_noneEnabled() {
		Logger mockLogger = mockSilent();

		mockLogger.infoOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE");

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void infoOrDebugCause_debugEnabled() {
		Throwable cause = new IllegalStateException("expected");
		Logger mockLogger = mockVerbose();

		mockLogger.infoOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE", cause);
		Mockito.verify(mockLogger).debug("VERBOSE", cause);

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void infoOrDebugCause_infoEnabled() {
		Throwable cause = new IllegalStateException("expected");
		Logger mockLogger = mockTerse();

		mockLogger.infoOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE", cause);
		Mockito.verify(mockLogger).info("TERSE", cause);

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void infoOrDebugCause_noneEnabled() {
		Throwable cause = new IllegalStateException("expected");
		Logger mockLogger = mockSilent();

		mockLogger.infoOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE", cause);

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void warnOrDebug_debugEnabled() {
		Logger mockLogger = mockVerbose();

		mockLogger.warnOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE");
		Mockito.verify(mockLogger).debug("VERBOSE");

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void warnOrDebug_infoEnabled() {
		Logger mockLogger = mockTerse();

		mockLogger.warnOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE");
		Mockito.verify(mockLogger).warn("TERSE");

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void warnOrDebug_noneEnabled() {
		Logger mockLogger = mockSilent();

		mockLogger.warnOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE");

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void warnOrDebugCause_debugEnabled() {
		Throwable cause = new IllegalStateException("expected");
		Logger mockLogger = mockVerbose();

		mockLogger.warnOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE", cause);
		Mockito.verify(mockLogger).debug("VERBOSE", cause);

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void warnOrDebugCause_infoEnabled() {
		Throwable cause = new IllegalStateException("expected");
		Logger mockLogger = mockTerse();

		mockLogger.warnOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE", cause);
		Mockito.verify(mockLogger).warn("TERSE", cause);

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void warnOrDebugCause_noneEnabled() {
		Throwable cause = new IllegalStateException("expected");
		Logger mockLogger = mockSilent();

		mockLogger.warnOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE", cause);

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void errorOrDebug_debugEnabled() {
		Logger mockLogger = mockVerbose();

		mockLogger.errorOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE");
		Mockito.verify(mockLogger).debug("VERBOSE");

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void errorOrDebug_infoEnabled() {
		Logger mockLogger = mockTerse();

		mockLogger.errorOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE");
		Mockito.verify(mockLogger).error("TERSE");

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void errorOrDebug_noneEnabled() {
		Logger mockLogger = mockSilent();

		mockLogger.errorOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE");

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void errorOrDebugCause_debugEnabled() {
		Throwable cause = new IllegalStateException("expected");
		Logger mockLogger = mockVerbose();

		mockLogger.errorOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE", cause);
		Mockito.verify(mockLogger).debug("VERBOSE", cause);

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void errorOrDebugCause_infoEnabled() {
		Throwable cause = new IllegalStateException("expected");
		Logger mockLogger = mockTerse();

		mockLogger.errorOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE", cause);
		Mockito.verify(mockLogger).error("TERSE", cause);

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

	@Test
	void errorOrDebugCause_noneEnabled() {
		Throwable cause = new IllegalStateException("expected");
		Logger mockLogger = mockSilent();

		mockLogger.errorOrDebug(isVerbose -> isVerbose ? "VERBOSE" : "TERSE", cause);

		verifyIsXxxEnabledXxxOrDebugAndNoMore(mockLogger);
	}

}