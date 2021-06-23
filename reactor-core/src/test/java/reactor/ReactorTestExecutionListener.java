/*
 * Copyright (c) 2011-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Schedulers;
import reactor.test.AssertionsUtils;

public class ReactorTestExecutionListener implements TestExecutionListener {

	public static void reset() {
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

	@Override
	public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
		reset();
	}

	@Override
	public void testPlanExecutionStarted(TestPlan testPlan) {
		AssertionsUtils.installAssertJTestRepresentation();
	}
}
