/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import reactor.test.util.LoggerUtils;
import reactor.test.util.TestLogger;
import reactor.util.Logger;
import reactor.util.annotation.Nullable;

/**
 * A JUnit5 extension that installs a {@link TestLogger} as the capturing instance via {@link LoggerUtils#enableCaptureWith(Logger)},
 * {@link LoggerUtils#disableCapture() disable capture} at the end of the test and injects the {@link TestLogger}
 * into the test case (by implementing {@link ParameterResolver}).
 *
 * @author Simon Baslé
 */
public class TestLoggerExtension implements ParameterResolver, AfterEachCallback, BeforeEachCallback {

	/**
	 * Set up a default {@link TestLoggerExtension}, unless @{@link Redirect} annotation is also present.
	 * By default loggers will route the log messages to both the original logger and the injected
	 * {@link TestLogger}, and in the latter there won't be automatic inclusion of thread names.
	 *
	 * @see Redirect
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE, ElementType.METHOD })
	@ExtendWith(TestLoggerExtension.class)
	public @interface Capture { }

	/**
	 * Set up a {@link TestLoggerExtension} that routes log messages only to the injected {@link TestLogger},
	 * suppressing the logs from the original logger. Messages don't include thread names.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE, ElementType.METHOD })
	@ExtendWith(TestLoggerExtension.class)
	public @interface Redirect { }

	TestLogger logger;

	@Override
	public void beforeEach(ExtensionContext context) throws Exception {
		if (context.getElement().isPresent()) {
			boolean suppressOriginal = context.getElement().get().isAnnotationPresent(Redirect.class);
			this.logger = new TestLogger(false);
			LoggerUtils.enableCaptureWith(logger, !suppressOriginal);
		}
	}

	@Override
	public void afterEach(ExtensionContext context) throws Exception {
		LoggerUtils.disableCapture();
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
		throws ParameterResolutionException {
		return parameterContext.getParameter().getType() == TestLogger.class;
	}

	@Override
	@Nullable
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
		throws ParameterResolutionException {
		if (parameterContext.getParameter().getType() == TestLogger.class) {
			return this.logger;
		}
		return null;
	}
}
