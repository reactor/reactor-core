/*
 * Copyright (c) 2023-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.scheduler;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Assertions;

class GenericBoundedElasticThreadPerTaskSchedulerTest extends AbstractSchedulerTest {


	static boolean SUPPORTED;

	static {
		try {
			new VirtualThreadFactory("threadPerTaskBoundedElasticSchedulerTest",
					false,
					Schedulers::defaultUncaughtException);
			SUPPORTED = true;
		}
		catch (Throwable t) {
			SUPPORTED = false;
		}
	}

	@Override
	protected BoundedElasticThreadPerTaskScheduler scheduler() {
		BoundedElasticThreadPerTaskScheduler test = freshScheduler();
		test.init();
		return test;
	}

	@Override
	protected BoundedElasticThreadPerTaskScheduler freshScheduler() {
		Assumptions.assumeThat(SUPPORTED).isTrue();
		return new BoundedElasticThreadPerTaskScheduler(4,
				Integer.MAX_VALUE,
				new VirtualThreadFactory(
						"threadPerTaskBoundedElasticSchedulerTest", false,
						Schedulers::defaultUncaughtException));
	}

	@Override
	protected boolean shouldCheckInterrupted() {
		return true;
	}

	@Override
	protected boolean shouldCheckMultipleDisposeGracefully() {
		return true;
	}

	@Override
	protected boolean shouldCheckSupportRestart() {
		return false;
	}
}