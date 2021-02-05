/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
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

package reactor;

import org.assertj.core.presentation.Representation;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Schedulers;
import reactor.test.AssertionsUtils;
import reactor.test.util.LoggerUtils;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * A custom TestExecutionListener that helps with tests in reactor:<ul>
 *     <li>resets {@link Hooks} once a test is finished, making sure no dirty state remains,</li>
 *     <li>resets {@link Schedulers} related infrastructure, making sure no dirty state remains,</li>
 *     <li>installs custom assertJ {@link Representation} for some of reactor types,</li>
 *     <li>installs a custom {@link Logger} factory <strong>very</strong> early in the suite lifecycle, so that loggers
 *     in reactor (which are typically static members initialized early) can be diverted and asserted in tests.</li>
 * </ul>
 */
public class ReactorTestExecutionListener implements TestExecutionListener {

	private static void resetHooksAndSchedulers() {
		Hooks.resetOnOperatorDebug();

		Hooks.resetOnEachOperator();
		Hooks.resetOnLastOperator();

		Hooks.resetOnErrorDropped();
		Hooks.resetOnNextDropped();

		Hooks.resetOnNextError();
		Hooks.resetOnOperatorError();

		Hooks.removeQueueWrappers();

		Schedulers.resetOnHandleError();
		Schedulers.resetFactory();
		Schedulers.resetOnScheduleHooks();

		// TODO capture non-default schedulers and shutdown them
	}

	/**
	 * Reset the {@link Loggers} factory to defaults suitable for reactor-core tests.
	 * Notably, it installs an indirection via {@link LoggerUtils#useCurrentLoggersWithCapture()}.
	 */
	public static void resetLoggersFactory() {
		Loggers.resetLoggerFactory();
		LoggerUtils.useCurrentLoggersWithCapture();
	}

	@Override
	public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
		resetHooksAndSchedulers();
	}

	@Override
	public void testPlanExecutionStarted(TestPlan testPlan) {
		AssertionsUtils.installAssertJTestRepresentation();
		resetLoggersFactory();
	}
}
