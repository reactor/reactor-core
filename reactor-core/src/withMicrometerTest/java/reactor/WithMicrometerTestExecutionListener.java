/*
 * Copyright (c) 2020-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor;

import io.micrometer.context.ContextRegistry;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

/**
 * A custom TestExecutionListener that helps with testing Micrometer integration in reactor:<ul>
 *     <li>defines a {@link ThreadLocal} and installs corresponding {@link io.micrometer.context.ThreadLocalAccessor}</li>
 * </ul>
 */
public class WithMicrometerTestExecutionListener implements TestExecutionListener {

	public static final String STRING_TL_KEY = "reactor.test.withMicrometer.threadLocal.string";

	public static final ThreadLocal<String> STRING_THREAD_LOCAL = new ThreadLocal<>();

	@Override
	public void executionStarted(TestIdentifier testIdentifier) {
		STRING_THREAD_LOCAL.set(testIdentifier.getDisplayName());
	}

	@Override
	public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
		STRING_THREAD_LOCAL.remove();
	}

	@Override
	public void testPlanExecutionStarted(TestPlan testPlan) {
		ContextRegistry.getInstance().registerThreadLocalAccessor(STRING_TL_KEY, STRING_THREAD_LOCAL);
	}
}
